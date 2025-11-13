from __future__ import annotations

import inspect
import math
import os
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, TypeVar

import httpx

from .client import AccountServiceClient
from .config import ClientConfig

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


class AccountServiceError(RuntimeError):
    pass


def account_rate_limit(
    *,
    crawler_type: str,
    request_count: Optional[int] = None,
    calculate_request_count: bool = False,
    records_per_page_env: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator that reserves quota with Account Service before executing the wrapped crawler function.

    Args:
        crawler_type: Identifier for the crawler (e.g. "google").
        request_count: Fixed request count to reserve. Defaults to 1 if not provided.
        calculate_request_count: When True, derives request count from `num_results` argument and
            records-per-page configuration.
        records_per_page_env: Optional environment variable name to resolve records per page.
            Defaults to `<CRAWLER_TYPE>_RECORDS_PER_PAGE` if omitted.
    """

    def decorator(func: F) -> F:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("account_rate_limit decorator requires an async function")

        signature = inspect.signature(func)
        env_key = records_per_page_env or "RECORDS_PER_PAGE"

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            account_override = _resolve_account_override(bound.arguments)
            resolved_request_count = _resolve_request_count(
                bound.arguments,
                request_count=request_count,
                calculate_request_count=calculate_request_count,
                records_per_page_env=env_key,
            )

            config = ClientConfig.from_env()

            try:
                async with AccountServiceClient(config.base_url, config.timeout) as client:
                    reserve_payload: dict[str, Any] = {"request_count": resolved_request_count}
                    try:
                        reserve_response = await client.reserve_account(
                            crawler_type=crawler_type,
                            payload=reserve_payload,
                            account_id=account_override,
                        )
                        account_payload = reserve_response.get("account") or {}
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            account_payload = await client.get_account(
                                crawler_type=crawler_type,
                                account_id=account_override,
                            )
                            account_id = account_payload.get("account_id")
                            if not account_id:
                                raise AccountServiceError("Account Service response missing account_id")
                            await client.update_rate_limit(
                                account_id=account_id,
                                crawler_type=crawler_type,
                                increment=resolved_request_count,
                            )
                            reserve_response = {
                                "account": account_payload,
                                "account_id": account_id,
                                "request_count": resolved_request_count,
                            }
                        else:
                            raise
            except httpx.HTTPStatusError as exc:
                raise AccountServiceError(
                    f"Account Service request failed ({exc.response.status_code}): {exc.response.text}"
                ) from exc
            except httpx.HTTPError as exc:
                raise AccountServiceError(f"Account Service request error: {exc}") from exc

            account_payload = reserve_response.get("account") or account_payload or {}
            account_id = reserve_response.get("account_id")
            if not account_id:
                raise AccountServiceError("Account Service response missing account_id")
            resolved_request_count = reserve_response.get("request_count", resolved_request_count)

            injected = {
                "account": account_payload,
                "account_id": account_id,
                "request_count": resolved_request_count,
            }

            call_kwargs = dict(bound.kwargs)
            call_kwargs.update(injected)
            return await func(*bound.args, **call_kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def _resolve_request_count(
    arguments: dict[str, Any],
    *,
    request_count: Optional[int],
    calculate_request_count: bool,
    records_per_page_env: str,
) -> int:
    if request_count is not None:
        return max(int(request_count), 1)

    if not calculate_request_count:
        return 1

    num_results = arguments.get("num_results")
    if num_results is None:
        return 1

    try:
        num_results_int = int(num_results)
    except (TypeError, ValueError):
        return 1

    records_per_page = _resolve_records_per_page(records_per_page_env)
    if records_per_page <= 0:
        records_per_page = 1

    return max(math.ceil(num_results_int / records_per_page), 1)


def _resolve_records_per_page(env_key: str) -> int:
    value = os.getenv(env_key, "1")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 1


def _resolve_account_override(arguments: dict[str, Any]) -> Optional[str]:
    for key in ("account_id", "account_override", "user_account_id"):
        value = arguments.get(key)
        if value:
            return str(value)
    return None



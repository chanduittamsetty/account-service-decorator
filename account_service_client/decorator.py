from __future__ import annotations

import inspect
import math
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
    type: str,
    request_count: Optional[int] = None,
    calculate_request_count: bool = False,
) -> Callable[[F], F]:
    """
    Decorator that reserves quota with Account Service before executing the wrapped crawler function.

    Args:
        type: Identifier for the crawler (e.g. "google_account").
        request_count: Fixed request count to reserve. Defaults to 1 if not provided.
        calculate_request_count: When True, derives request count from `num_results` and
            the function arguments (`records_per_page` / `page_size`).
    """

    def decorator(func: F) -> F:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("account_rate_limit decorator requires an async function")

        signature = inspect.signature(func)
        account_type = type

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            account_override = _resolve_account_override(bound.arguments)
            resolved_request_count = _resolve_request_count(
                bound.arguments,
                request_count=request_count,
                calculate_request_count=calculate_request_count,
            )

            config = ClientConfig.from_env()

            try:
                async with AccountServiceClient(config.base_url, config.timeout) as client:
                    reserve_payload: dict[str, Any] = {"request_count": resolved_request_count}
                    try:
                        reserve_response = await client.reserve_account(
                            type=account_type,
                            payload=reserve_payload,
                            account_id=account_override,
                        )
                        account_payload = reserve_response.get("account") or {}
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            # Fallback: use get_account + update_rate_limit for older API versions
                            get_response = await client.get_account(
                                type=account_type,
                                account_id=account_override,
                            )
                            account_id = get_response.get("account_id")
                            if not account_id:
                                raise AccountServiceError("Account Service response missing account_id")
                            await client.update_rate_limit(
                                account_id=account_id,
                                type=account_type,
                                increment=resolved_request_count,
                            )
                            # Extract the nested account document from the response
                            account_payload = get_response.get("account") or {}
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

    records_per_page = _resolve_records_per_page_from_args(arguments)
    if records_per_page <= 0:
        records_per_page = 1

    return max(math.ceil(num_results_int / records_per_page), 1)


def _resolve_records_per_page_from_args(arguments: dict[str, Any]) -> int:
    for key in ("records_per_page", "page_size", "per_page"):
        value = arguments.get(key)
        if value is None:
            continue
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            continue
    return 1


def _resolve_account_override(arguments: dict[str, Any]) -> Optional[str]:
    for key in ("account_id", "account_override", "user_account_id", "x_user_id"):
        value = arguments.get(key)
        if value:
            return str(value)
    return None



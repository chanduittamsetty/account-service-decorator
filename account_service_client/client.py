from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


class AccountServiceClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "AccountServiceClient":
        if not self._client:
            self._client = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("AccountServiceClient must be used within an async context manager")
        return self._client

    async def get_account(
        self,
        crawler_type: str,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        headers: Dict[str, str] = {}
        if account_id:
            headers["x-user-id"] = account_id

        response = await self.client.get(f"/api/v1/accounts/{crawler_type}/get", headers=headers)
        response.raise_for_status()
        return response.json()

    async def update_rate_limit(
        self,
        account_id: str,
        crawler_type: str,
        increment: int,
    ) -> Dict[str, Any]:
        payload = {
            "crawler_type": crawler_type,
            "increment": increment,
        }
        response = await self.client.post(
            f"/api/v1/accounts/{account_id}/rate-limit/update",
            json=payload,
        )
        response.raise_for_status()
        return response.json()



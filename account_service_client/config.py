from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ClientConfig:
    base_url: str
    timeout: float

    @classmethod
    def from_env(cls) -> "ClientConfig":
        base_url = os.getenv("ACCOUNT_SERVICE_BASE_URL", "http://localhost:8000")
        timeout = float(os.getenv("ACCOUNT_SERVICE_TIMEOUT", "10"))
        return cls(base_url=base_url.rstrip("/"), timeout=timeout)



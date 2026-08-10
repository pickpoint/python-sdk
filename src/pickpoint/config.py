from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

DEFAULT_BASE_URL = "https://api.pickpoint.io"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE = 1.0  # seconds
MIN_RETRY_BASE = 0.2
DEFAULT_TIMEOUT = 30.0
MAX_CONCURRENCY = 20
DEFAULT_CONCURRENCY = 20
CLIENT_AUTH_REFRESH_AT = 0.5


@dataclass
class ClientAuth:
    """Pair from POST /v2/client-tokens. expires_at is unix epoch milliseconds."""

    access_token: str
    refresh_token: str
    expires_at: int


@dataclass
class Config:
    """Public-api client config. Provide exactly one of api_key / client_auth / access_token."""

    api_key: str | None = None
    client_auth: ClientAuth | None = None
    access_token: str | None = None
    base_url: str | None = None
    max_retries: int | None = None
    retry_base: float | None = None
    timeout: float | None = None
    concurrency: int | None = None
    http_client: Any | None = None  # optional httpx.AsyncClient

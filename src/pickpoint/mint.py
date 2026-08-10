from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import DEFAULT_BASE_URL, DEFAULT_TIMEOUT, Config
from .errors import APIError, InvalidConfigError
from .transport import trim_slash


@dataclass
class TokenPair:
    access_token: str
    refresh_token: str
    expires_at: int
    expires_in: int = 0
    scopes: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TokenPair:
        return cls(
            access_token=str(d.get("accessToken") or ""),
            refresh_token=str(d.get("refreshToken") or ""),
            expires_at=int(d.get("expiresAt") or 0),
            expires_in=int(d.get("expiresIn") or 0),
            scopes=list(d.get("scopes") or []),
        )


async def mint_client_tokens(
    cfg: Config,
    scopes: list[str] | None = None,
    ttl_sec: int | None = None,
) -> TokenPair:
    """Mint a client-token pair with a secret API key (server-side)."""
    if not cfg.api_key:
        raise InvalidConfigError("mint_client_tokens requires api_key")
    base = trim_slash(cfg.base_url or DEFAULT_BASE_URL)
    timeout = cfg.timeout if cfg.timeout and cfg.timeout > 0 else DEFAULT_TIMEOUT
    payload: dict[str, Any] = {"scopes": scopes or []}
    if ttl_sec and ttl_sec > 0:
        payload["ttlSec"] = ttl_sec

    own_client = cfg.http_client is None
    http = cfg.http_client or httpx.AsyncClient(timeout=timeout)
    try:
        res = await http.post(
            f"{base}/v2/client-tokens",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-api-key": cfg.api_key,
            },
            json=payload,
        )
    except httpx.HTTPError as e:
        raise APIError(code="NETWORK", message=f"mint client tokens network error: {e}") from e
    finally:
        if own_client:
            await http.aclose()

    raw = res.content
    if res.status_code < 200 or res.status_code >= 300:
        raise APIError(
            status=res.status_code,
            code="CLIENT_ERROR",
            message=f"mint client tokens failed ({res.status_code})",
            body=raw,
        )
    return TokenPair.from_dict(res.json())

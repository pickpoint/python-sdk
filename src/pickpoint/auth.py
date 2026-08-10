from __future__ import annotations

import asyncio
import time
from typing import Any, Protocol

import httpx

from .config import CLIENT_AUTH_REFRESH_AT, ClientAuth, Config
from .errors import APIError, InvalidConfigError


class TokenSession(Protocol):
    async def token(self) -> str: ...

    async def refresh_after_unauthorized(self) -> bool: ...


class AuthState:
    def __init__(self, *, api_key: str | None = None, session: TokenSession | None = None) -> None:
        self.api_key = api_key
        self.session = session

    @property
    def is_bearer(self) -> bool:
        return self.session is not None

    async def apply(self, headers: dict[str, str]) -> None:
        headers["Accept"] = "application/json"
        if self.api_key is not None:
            headers["x-api-key"] = self.api_key
            return
        assert self.session is not None
        tok = await self.session.token()
        headers["Authorization"] = f"Bearer {tok}"

    async def refresh_after_unauthorized(self) -> bool:
        if self.session is None:
            return False
        return await self.session.refresh_after_unauthorized()


class StaticSession:
    def __init__(self, access_token: str) -> None:
        self._token = access_token

    async def token(self) -> str:
        return self._token

    async def refresh_after_unauthorized(self) -> bool:
        return False


class ClientAuthSession:
    def __init__(self, initial: ClientAuth, base_url: str, http: httpx.AsyncClient) -> None:
        if not initial.access_token or not initial.refresh_token or not initial.expires_at:
            raise InvalidConfigError(
                "client_auth requires access_token, refresh_token, and expires_at (unix ms)"
            )
        self._access = initial.access_token
        self._refresh = initial.refresh_token
        self._expires_at = initial.expires_at
        self._issued_at = time.time()
        self._base_url = base_url
        self._http = http
        self._lock = asyncio.Lock()
        self._refreshing = False
        self._waiters: list[asyncio.Future[None]] = []

    def _needs_proactive_refresh(self) -> bool:
        now_ms = int(time.time() * 1000)
        issued_ms = int(self._issued_at * 1000)
        ttl_ms = self._expires_at - issued_ms
        if ttl_ms <= 0:
            return now_ms >= self._expires_at - 30_000
        refresh_after = ttl_ms * CLIENT_AUTH_REFRESH_AT
        return (time.time() - self._issued_at) >= (refresh_after / 1000.0)

    async def token(self) -> str:
        if self._needs_proactive_refresh():
            await self.refresh()
        return self._access

    async def refresh_after_unauthorized(self) -> bool:
        try:
            await self.refresh()
            return True
        except Exception:
            return False

    async def refresh(self) -> None:
        async with self._lock:
            if self._refreshing:
                fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                self._waiters.append(fut)
                waiters = True
            else:
                self._refreshing = True
                waiters = False
                refresh_tok = self._refresh

        if waiters:
            await fut
            return

        err: Exception | None = None
        try:
            await self._do_refresh(refresh_tok)
        except Exception as e:
            err = e

        async with self._lock:
            self._refreshing = False
            for w in self._waiters:
                if err is None:
                    w.set_result(None)
                else:
                    w.set_exception(err)
            self._waiters.clear()

        if err is not None:
            raise err

    async def _do_refresh(self, refresh_tok: str) -> None:
        try:
            res = await self._http.post(
                f"{self._base_url}/v2/client-tokens/refresh",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                json={"refreshToken": refresh_tok},
            )
        except httpx.HTTPError as e:
            raise APIError(
                code="REFRESH_FAILED",
                message=f"client token refresh network error: {e}",
            ) from e
        raw = res.content
        if res.status_code < 200 or res.status_code >= 300:
            raise APIError(
                status=res.status_code,
                code="REFRESH_FAILED",
                message=f"client token refresh failed ({res.status_code})",
                body=raw,
            )
        try:
            pair = res.json()
        except Exception as e:
            raise APIError(code="INVALID_TOKEN", message=f"refresh returned invalid JSON: {e}", body=raw) from e
        if not pair.get("accessToken") or not pair.get("refreshToken") or not pair.get("expiresAt"):
            raise APIError(code="INVALID_TOKEN", message="refresh returned invalid clientAuth pair", body=raw)
        async with self._lock:
            self._access = pair["accessToken"]
            self._refresh = pair["refreshToken"]
            self._expires_at = int(pair["expiresAt"])
            self._issued_at = time.time()


def resolve_auth(cfg: Config, base_url: str, http: httpx.AsyncClient) -> AuthState:
    n = 0
    if cfg.api_key:
        n += 1
    if cfg.client_auth is not None:
        n += 1
    if cfg.access_token:
        n += 1
    if n > 1:
        raise InvalidConfigError("provide only one of: api_key | client_auth | access_token")
    if n == 0:
        raise InvalidConfigError("auth required: api_key, client_auth, or access_token")
    if cfg.api_key:
        return AuthState(api_key=cfg.api_key)
    if cfg.client_auth is not None:
        return AuthState(session=ClientAuthSession(cfg.client_auth, base_url, http))
    assert cfg.access_token
    return AuthState(session=StaticSession(cfg.access_token))

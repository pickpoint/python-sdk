from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import urlencode

import httpx

from .auth import AuthState
from .config import DEFAULT_RETRY_BASE, MIN_RETRY_BASE
from .errors import APIError


class OnClientError(Enum):
    THROW = "throw"
    EMPTY = "empty"


@dataclass
class RequestOpts:
    method: str
    path: str
    query: dict[str, str] | None = None
    body: Any | None = None
    on_client_error: OnClientError = OnClientError.THROW
    empty: bytes | None = None


class Transport:
    def __init__(
        self,
        *,
        base_url: str,
        http: httpx.AsyncClient,
        auth: AuthState,
        max_retries: int,
        retry_base: float,
    ) -> None:
        self.base_url = base_url
        self.http = http
        self.auth = auth
        self.max_retries = max_retries
        self.retry_base = retry_base

    async def do(self, opts: RequestOpts) -> bytes:
        attempt = 0
        auth_retried = False
        while True:
            url = f"{self.base_url}{opts.path}"
            if opts.query:
                q = {k: v for k, v in opts.query.items() if v}
                if q:
                    url = f"{url}?{urlencode(q)}"

            headers: dict[str, str] = {}
            await self.auth.apply(headers)
            if opts.body is not None:
                headers["Content-Type"] = "application/json"

            try:
                res = await self.http.request(
                    opts.method,
                    url,
                    headers=headers,
                    content=None if opts.body is None else json.dumps(opts.body).encode(),
                )
            except httpx.HTTPError as e:
                if attempt >= self.max_retries:
                    raise APIError(code="NETWORK", message=f"network error: {e}") from e
                await _sleep_backoff(self.retry_base, attempt)
                attempt += 1
                continue

            status = res.status_code
            raw = res.content

            if status == 401:
                if not auth_retried and self.auth.is_bearer and await self.auth.refresh_after_unauthorized():
                    auth_retried = True
                    continue
                raise APIError(status=status, code="API_AUTH", message="auth failed (401)", body=raw)

            if status in (402, 403):
                raise APIError(status=status, code="API_AUTH", message="auth failed", body=raw)

            if status == 204:
                return b""

            if status == 409:
                raise APIError(
                    status=409,
                    code="CONFLICT",
                    message=_message_from_body(raw, 409),
                    body=raw,
                )

            if status == 400 or 404 <= status < 500:
                if opts.on_client_error is OnClientError.EMPTY:
                    return opts.empty or b""
                code = "NOT_FOUND" if status == 404 else "CLIENT_ERROR"
                raise APIError(
                    status=status,
                    code=code,
                    message=_message_from_body(raw, status),
                    body=raw,
                )

            if status >= 500:
                if attempt >= self.max_retries:
                    raise APIError(
                        status=status,
                        code="SERVER_ERROR",
                        message="server error after retries",
                        body=raw,
                    )
                await _sleep_backoff(self.retry_base, attempt)
                attempt += 1
                continue

            if 200 <= status < 300:
                return raw

            if 400 <= status < 500 and opts.on_client_error is OnClientError.EMPTY:
                return opts.empty or b""
            raise APIError(
                status=status,
                code="CLIENT_ERROR",
                message=_message_from_body(raw, status),
                body=raw,
            )


def _message_from_body(raw: bytes, status: int) -> str:
    try:
        m = json.loads(raw.decode())
        if isinstance(m, dict):
            if m.get("message"):
                return str(m["message"])
            if m.get("error"):
                return str(m["error"])
    except Exception:
        pass
    return httpx.codes.get_reason_phrase(status) or "unknown"


async def _sleep_backoff(base: float, attempt: int) -> None:
    if base <= 0:
        base = DEFAULT_RETRY_BASE
    base = max(base, MIN_RETRY_BASE)
    max_delay = base * (2**min(attempt, 16))
    await asyncio.sleep(random.uniform(0, max_delay))


def trim_slash(s: str) -> str:
    return s.rstrip("/")

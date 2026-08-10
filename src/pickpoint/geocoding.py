from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, TypeVar

from .errors import APIError
from .transport import OnClientError, RequestOpts, Transport

Query = dict[str, str]
T = TypeVar("T")


class GeocodingService:
    def __init__(self, transport: Transport, concurrency: int) -> None:
        self._t = transport
        self._concurrency = concurrency

    async def forward(self, q: Query) -> list[Any]:
        raw = await self._t.do(
            RequestOpts(
                method="GET",
                path="/v2/geocode/forward",
                query=q,
                on_client_error=OnClientError.EMPTY,
                empty=b"[]",
            )
        )
        return _decode_json_array(raw)

    async def reverse(self, q: Query) -> dict[str, Any] | None:
        raw = await self._t.do(
            RequestOpts(
                method="GET",
                path="/v2/geocode/reverse",
                query=q,
                on_client_error=OnClientError.EMPTY,
                empty=b"null",
            )
        )
        if not raw or raw == b"null":
            return None
        return json.loads(raw)

    async def lookup(self, q: Query) -> list[Any]:
        raw = await self._t.do(
            RequestOpts(
                method="GET",
                path="/v2/address/lookup",
                query=q,
                on_client_error=OnClientError.EMPTY,
                empty=b"[]",
            )
        )
        return _decode_json_array(raw)

    async def forward_batch(self, qs: list[Query]) -> list[list[Any]]:
        return await _run_batch(self._concurrency, qs, self.forward)

    async def reverse_batch(self, qs: list[Query]) -> list[dict[str, Any] | None]:
        return await _run_batch(self._concurrency, qs, self.reverse)

    async def lookup_batch(self, qs: list[Query]) -> list[list[Any]]:
        return await _run_batch(self._concurrency, qs, self.lookup)


def _decode_json_array(raw: bytes) -> list[Any]:
    if not raw:
        return []
    try:
        out = json.loads(raw)
        if isinstance(out, list):
            return out
        if out is None:
            return []
        return [out]
    except json.JSONDecodeError as e:
        raise APIError(code="INVALID_JSON", message=str(e), body=raw) from e


async def _run_batch(
    concurrency: int,
    inputs: list[Query],
    fn: Callable[[Query], Awaitable[T]],
) -> list[T]:
    concurrency = max(1, concurrency)
    n = len(inputs)
    if n == 0:
        return []

    out: list[T | None] = [None] * n
    first_err: BaseException | None = None
    sem = asyncio.Semaphore(concurrency)
    cancel = asyncio.Event()

    async def worker(i: int, q: Query) -> None:
        nonlocal first_err
        if cancel.is_set():
            return
        async with sem:
            if cancel.is_set():
                return
            try:
                out[i] = await fn(q)
            except BaseException as e:
                if first_err is None:
                    first_err = e
                    cancel.set()

    tasks = [asyncio.create_task(worker(i, q)) for i, q in enumerate(inputs)]
    await asyncio.gather(*tasks, return_exceptions=True)
    if first_err is not None:
        raise first_err
    return [out[i] for i in range(n)]  # type: ignore[misc]

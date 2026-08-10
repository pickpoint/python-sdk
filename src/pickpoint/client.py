from __future__ import annotations

from typing import Any

import httpx

from .address import AddressService
from .auth import resolve_auth
from .config import (
    DEFAULT_BASE_URL,
    DEFAULT_CONCURRENCY,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BASE,
    DEFAULT_TIMEOUT,
    MAX_CONCURRENCY,
    MIN_RETRY_BASE,
    Config,
)
from .devices import (
    Device,
    DeviceCommandResult,
    DeviceInput,
    DeviceListQuery,
    DeviceListResult,
    DevicesService,
)
from .geocoding import GeocodingService, Query
from .routing import RoutingService
from .transport import Transport, trim_slash


class Client:
    """Unified public-api client (geocoding, address, routing, devices).

    Tracking lives in ``pickpoint.tracking``.
    """

    def __init__(self, cfg: Config) -> None:
        base = trim_slash(cfg.base_url or DEFAULT_BASE_URL)
        timeout = cfg.timeout if cfg.timeout and cfg.timeout > 0 else DEFAULT_TIMEOUT
        self._owns_http = cfg.http_client is None
        self._http = cfg.http_client or httpx.AsyncClient(timeout=timeout)

        max_retries = cfg.max_retries if cfg.max_retries and cfg.max_retries > 0 else DEFAULT_MAX_RETRIES
        retry_base = cfg.retry_base if cfg.retry_base and cfg.retry_base > 0 else DEFAULT_RETRY_BASE
        retry_base = max(retry_base, MIN_RETRY_BASE)
        concurrency = cfg.concurrency if cfg.concurrency and cfg.concurrency > 0 else DEFAULT_CONCURRENCY
        concurrency = min(concurrency, MAX_CONCURRENCY)

        auth = resolve_auth(cfg, base, self._http)
        self._transport = Transport(
            base_url=base,
            http=self._http,
            auth=auth,
            max_retries=max_retries,
            retry_base=retry_base,
        )
        self.concurrency = concurrency
        self.geocoding = GeocodingService(self._transport, concurrency)
        self.address = AddressService(self._transport)
        self.routing = RoutingService(self._transport)
        self.devices = DevicesService(self._transport)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    # Flat shortcuts
    async def forward(self, q: Query) -> list[Any]:
        return await self.geocoding.forward(q)

    async def reverse(self, q: Query) -> dict[str, Any] | None:
        return await self.geocoding.reverse(q)

    async def lookup(self, q: Query) -> list[Any]:
        return await self.geocoding.lookup(q)

    async def forward_batch(self, qs: list[Query]) -> list[list[Any]]:
        return await self.geocoding.forward_batch(qs)

    async def reverse_batch(self, qs: list[Query]) -> list[dict[str, Any] | None]:
        return await self.geocoding.reverse_batch(qs)

    async def lookup_batch(self, qs: list[Query]) -> list[list[Any]]:
        return await self.geocoding.lookup_batch(qs)

    async def search(self, q: Query) -> dict[str, Any]:
        return await self.address.search(q)

    async def route(self, body: Any) -> Any:
        return await self.routing.route(body)

    async def optimized_route(self, body: Any) -> Any:
        return await self.routing.optimized(body)

    async def matrix(self, body: Any) -> Any:
        return await self.routing.matrix(body)

    async def locate(self, body: Any) -> Any:
        return await self.routing.locate(body)

    async def elevation(self, body: Any) -> Any:
        return await self.routing.elevation(body)

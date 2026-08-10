from __future__ import annotations

import json
from typing import Any

from .transport import RequestOpts, Transport


class RoutingService:
    def __init__(self, transport: Transport) -> None:
        self._t = transport

    async def _post(self, path: str, body: Any) -> Any:
        raw = await self._t.do(RequestOpts(method="POST", path=path, body=body))
        if not raw:
            return None
        return json.loads(raw)

    async def route(self, body: Any) -> Any:
        return await self._post("/v2/route", body)

    async def optimized(self, body: Any) -> Any:
        return await self._post("/v2/route/optimized", body)

    async def matrix(self, body: Any) -> Any:
        return await self._post("/v2/route/matrix", body)

    async def locate(self, body: Any) -> Any:
        return await self._post("/v2/route/locate", body)

    async def elevation(self, body: Any) -> Any:
        return await self._post("/v2/route/elevation", body)

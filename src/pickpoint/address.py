from __future__ import annotations

import json
from typing import Any

from .geocoding import Query
from .transport import RequestOpts, Transport


class AddressService:
    def __init__(self, transport: Transport) -> None:
        self._t = transport

    async def search(self, q: Query) -> dict[str, Any]:
        raw = await self._t.do(RequestOpts(method="GET", path="/v2/address/search", query=q))
        return json.loads(raw)

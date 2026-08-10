from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from .transport import RequestOpts, Transport


@dataclass
class Device:
    uid: str
    id: int = 0
    name: str = ""
    status: str = ""
    description: str | None = None
    tracks_count: int = 0
    type: str = ""
    secret: str = ""
    metadata: str | None = None
    created_at: str = ""
    updated_at: str = ""
    last_location: Any | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Device:
        return cls(
            id=int(d.get("id") or 0),
            uid=str(d.get("uid") or ""),
            name=str(d.get("name") or ""),
            status=str(d.get("status") or ""),
            description=d.get("description"),
            tracks_count=int(d.get("tracksCount") or 0),
            type=str(d.get("type") or ""),
            secret=str(d.get("secret") or ""),
            metadata=d.get("metadata"),
            created_at=str(d.get("createdAt") or ""),
            updated_at=str(d.get("updatedAt") or ""),
            last_location=d.get("lastLocation"),
        )


@dataclass
class DeviceInput:
    name: str
    type: str
    description: str | None = None
    metadata: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name, "type": self.type}
        if self.description is not None:
            out["description"] = self.description
        if self.metadata is not None:
            out["metadata"] = self.metadata
        return out


@dataclass
class DeviceListResult:
    data: list[Device]
    total: int


@dataclass
class DeviceListQuery:
    skip: int | None = None
    take: int | None = None
    search: str | None = None
    idle: bool = False


@dataclass
class DeviceCommandResult:
    delivered: int = 0


class DevicesService:
    def __init__(self, transport: Transport) -> None:
        self._t = transport

    async def list(self, q: DeviceListQuery | None = None) -> DeviceListResult:
        q = q or DeviceListQuery()
        query: dict[str, str] = {}
        if q.skip and q.skip > 0:
            query["skip"] = str(q.skip)
        if q.take and q.take > 0:
            query["take"] = str(q.take)
        if q.search:
            query["search"] = q.search
        if q.idle:
            query["idle"] = "1"
        raw = await self._t.do(RequestOpts(method="GET", path="/v2/devices", query=query))
        body = json.loads(raw)
        return DeviceListResult(
            data=[Device.from_dict(x) for x in body.get("data") or []],
            total=int(body.get("total") or 0),
        )

    async def get(self, uid: str) -> Device:
        path = f"/v2/devices/{quote(uid, safe='')}"
        raw = await self._t.do(RequestOpts(method="GET", path=path))
        return Device.from_dict(json.loads(raw))

    async def create(self, input: DeviceInput) -> Device:
        raw = await self._t.do(
            RequestOpts(method="POST", path="/v2/devices", body=input.to_dict())
        )
        return Device.from_dict(json.loads(raw))

    async def update(self, uid: str, input: DeviceInput) -> Device:
        path = f"/v2/devices/{quote(uid, safe='')}"
        raw = await self._t.do(RequestOpts(method="PATCH", path=path, body=input.to_dict()))
        return Device.from_dict(json.loads(raw))

    async def delete(self, uid: str) -> None:
        path = f"/v2/devices/{quote(uid, safe='')}"
        await self._t.do(RequestOpts(method="DELETE", path=path))

    async def command(self, uid: str, payload: bytes | bytearray) -> DeviceCommandResult:
        path = f"/v2/devices/{quote(uid, safe='')}/command"
        raw = await self._t.do(
            RequestOpts(
                method="POST",
                path=path,
                body={"payload": base64.b64encode(bytes(payload)).decode("ascii")},
            )
        )
        body = json.loads(raw)
        return DeviceCommandResult(delivered=int(body.get("delivered") or 0))

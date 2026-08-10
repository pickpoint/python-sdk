from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from websockets.asyncio.server import serve
from websockets.server import ServerConnection

from pickpoint.tracking import SUBPROTOCOL
from pickpoint.tracking.v2 import (
    ClientMsg,
    Error,
    Hello,
    LocationAdded,
    Relocate,
    ResumeOk,
    ServerMsg,
    Subscribed,
    TrackStarted,
    TrackStopped,
)


@dataclass
class MockConn:
    ws: ServerConnection
    messages: list[ClientMsg] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, msg: ServerMsg) -> None:
        await self.ws.send(msg.SerializeToString())

    async def close(self) -> None:
        await self.ws.close()


@dataclass
class MockOpts:
    auto: bool = True
    on_msg: Callable[[ClientMsg, MockConn], None] | None = None
    before_hello: Callable[[int, MockConn], None] | None = None
    relocate_on_connect: Relocate | None = None


class MockServer:
    def __init__(self) -> None:
        self.url = ""
        self.connections: list[MockConn] = []
        self.opts = MockOpts()
        self._lock = asyncio.Lock()
        self._server = None
        self._task: asyncio.Task[None] | None = None

    @property
    def conn_count(self) -> int:
        return len(self.connections)

    async def wait_conn(self, timeout: float = 2.0) -> MockConn:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if self.connections:
                return self.connections[0]
            await asyncio.sleep(0.005)
        raise TimeoutError("wait_conn timeout")

    async def wait_msg(
        self, pred: Callable[[ClientMsg], bool], timeout: float = 2.0
    ) -> ClientMsg:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            for c in list(self.connections):
                for m in list(c.messages):
                    if pred(m):
                        return m
            await asyncio.sleep(0.005)
        raise TimeoutError("wait_msg timeout")

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._task is not None:
            self._task.cancel()


def process_request(connection: ServerConnection, request):  # type: ignore[no-untyped-def]
    """Accept tracking.v2.proto subprotocol."""
    return None


async def start_mock(
    auto: bool = True,
    on_msg: Callable[[ClientMsg, MockConn], None] | None = None,
    *,
    before_hello: Callable[[int, MockConn], None] | None = None,
    relocate_on_connect: Relocate | None = None,
) -> MockServer:
    ms = MockServer()
    ms.opts = MockOpts(
        auto=auto,
        on_msg=on_msg,
        before_hello=before_hello,
        relocate_on_connect=relocate_on_connect,
    )

    async def handler(ws: ServerConnection) -> None:
        c = MockConn(ws=ws)
        async with ms._lock:
            ms.connections.append(c)
            idx = len(ms.connections)

        if ms.opts.before_hello is not None:
            ms.opts.before_hello(idx, c)

        if ms.opts.relocate_on_connect is not None and idx == 1:
            msg = ServerMsg()
            msg.relocate.CopyFrom(ms.opts.relocate_on_connect)
            await c.send(msg)
        else:
            msg = ServerMsg()
            msg.hello.CopyFrom(Hello(node_id="mock-1"))
            await c.send(msg)

        try:
            async for raw in ws:
                if isinstance(raw, str):
                    continue
                msg = ClientMsg()
                msg.ParseFromString(raw)
                async with c._lock:
                    clone = ClientMsg()
                    clone.CopyFrom(msg)
                    c.messages.append(clone)
                if ms.opts.on_msg is not None:
                    ms.opts.on_msg(msg, c)
                if not ms.opts.auto:
                    continue
                kind = msg.WhichOneof("body")
                if kind == "track_start":
                    out = ServerMsg()
                    out.track_started.CopyFrom(TrackStarted(track_uid="track-mock-1"))
                    await c.send(out)
                elif kind == "track_stop":
                    out = ServerMsg()
                    out.track_stopped.CopyFrom(TrackStopped(track_uid=msg.track_stop.track_uid))
                    await c.send(out)
                elif kind == "resume":
                    out = ServerMsg()
                    out.resume_ok.CopyFrom(
                        ResumeOk(track_uid=msg.resume.track_uid, last_acked_seq=0)
                    )
                    await c.send(out)
                elif kind == "location_add":
                    out = ServerMsg()
                    la = LocationAdded(
                        track_uid=msg.location_add.track_uid,
                        client_seq=msg.location_add.client_seq,
                        device_uid="dev-1",
                    )
                    la.point.CopyFrom(msg.location_add.point)
                    out.location_added.CopyFrom(la)
                    await c.send(out)
                elif kind == "location_batch":
                    out = ServerMsg()
                    out.location_added.CopyFrom(
                        LocationAdded(
                            track_uid=msg.location_batch.track_uid,
                            client_seq=msg.location_batch.client_seq,
                            device_uid="dev-1",
                        )
                    )
                    await c.send(out)
                elif kind == "subscribe":
                    out = ServerMsg()
                    out.subscribed.CopyFrom(
                        Subscribed(device_uid=msg.subscribe.device_uid, track_uid="track-mock-1")
                    )
                    await c.send(out)
                elif kind == "ping":
                    out = ServerMsg()
                    out.pong.SetInParent()
                    await c.send(out)
        except Exception:
            pass

    server = await serve(
        handler,
        "127.0.0.1",
        0,
        subprotocols=[SUBPROTOCOL],
        process_request=process_request,
    )
    ms._server = server
    sock = server.sockets[0]
    port = sock.getsockname()[1]
    ms.url = f"ws://127.0.0.1:{port}"
    return ms


def server_error(code: int, message: str) -> ServerMsg:
    msg = ServerMsg()
    msg.error.CopyFrom(Error(code=code, message=message))
    return msg


async def wait_for(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.015)
    raise TimeoutError("wait_for timeout")

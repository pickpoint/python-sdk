from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from websockets.asyncio.server import serve
from websockets.server import ServerConnection

from pickpoint.tracking import SUBPROTOCOL
from pickpoint.tracking.codec import decode_client_msg, encode_server_msg
from pickpoint.tracking.types import (
    PROTOCOL_VERSION,
    Ack,
    ClientMsg,
    ErrorCode,
    Hello,
    Relocate,
    ResumeOk,
    ServerMsg,
    Subscribed,
    TrackStarted,
    TrackStopped,
    WireError,
)

MOCK_TRACK_UID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MOCK_DEVICE_UID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
MOCK_NODE_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


@dataclass
class MockConn:
    ws: ServerConnection
    messages: list[ClientMsg] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, msg: ServerMsg) -> None:
        await self.ws.send(encode_server_msg(msg))

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
        self._next_sub = 1

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
            await c.send(ServerMsg(relocate=ms.opts.relocate_on_connect))
        else:
            await c.send(
                ServerMsg(hello=Hello(version=PROTOCOL_VERSION, node_id=MOCK_NODE_ID))
            )

        try:
            async for raw in ws:
                if isinstance(raw, str):
                    continue
                try:
                    msg = decode_client_msg(raw)
                except Exception:
                    continue
                async with c._lock:
                    c.messages.append(msg)
                if ms.opts.on_msg is not None:
                    ms.opts.on_msg(msg, c)
                if not ms.opts.auto:
                    continue
                if msg.track_start is not None:
                    await c.send(ServerMsg(track_started=TrackStarted(track_uid=MOCK_TRACK_UID)))
                elif msg.track_stop is not None:
                    await c.send(ServerMsg(track_stopped=TrackStopped(track_uid=MOCK_TRACK_UID)))
                elif msg.resume is not None:
                    await c.send(
                        ServerMsg(
                            resume_ok=ResumeOk(track_uid=msg.resume.track_uid, last_acked=0)
                        )
                    )
                elif msg.loc is not None:
                    await c.send(ServerMsg(ack=Ack(seq=msg.loc.seq)))
                elif msg.subscribe is not None:
                    sub = ms._next_sub
                    ms._next_sub += 1
                    await c.send(
                        ServerMsg(
                            subscribed=Subscribed(
                                sub=sub,
                                device_uid=msg.subscribe.device_uid,
                                track_uid=MOCK_TRACK_UID,
                                online=True,
                            )
                        )
                    )
        except Exception:
            pass

    server = await serve(
        handler,
        "127.0.0.1",
        0,
        subprotocols=[SUBPROTOCOL],
    )
    ms._server = server
    sock = server.sockets[0]
    port = sock.getsockname()[1]
    ms.url = f"ws://127.0.0.1:{port}"
    return ms


def server_error(code: int, message: str) -> ServerMsg:
    return ServerMsg(error=WireError(code=code, message=message))


async def wait_for(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.015)
    raise TimeoutError("wait_for timeout")

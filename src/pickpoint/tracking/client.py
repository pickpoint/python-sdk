"""Realtime tracking client (binary WebSocket by default, gRPC supported)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import grpc
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from .backoff import new_backoff, next_delay, reset_backoff
from .codec import clone_lat_lng, decode_server_msg, encode_client_msg, stamp_lat_lng, stamp_lat_lngs
from .errors import Error, error_from_wire, is_auth_error, is_fatal_resume_error, new_error
from .queue import OfflineQueue
from .rate import can_accept_publish, next_publish_allowed_at
from .types import Config, ConnectionState, DeviceAuth, ListenerAuth, Transport
from .url import build_ws_url
from .v2 import (
    ClientMsg,
    Command,
    CommandAck,
    Event,
    ErrorCode,
    LatLng,
    LocationAdd,
    LocationBatch,
    Resume,
    ServerMsg,
    Subscribe,
    TrackStart,
    TrackStop,
    TrackingStub,
)

SUBPROTOCOL = "tracking.v2.proto"
MAX_PUBLISH_HZ = 50
MIN_PUBLISH_INTERVAL = 1.0 / MAX_PUBLISH_HZ
MAX_EVENT_BYTES = 4 * 1024
MAX_EVENT_HZ = 1
MIN_EVENT_INTERVAL = 1.0 / MAX_EVENT_HZ


class Client:
    """Tracking session (device publisher or listener)."""

    def __init__(self, cfg: Config) -> None:
        hello = cfg.hello_timeout if cfg.hello_timeout and cfg.hello_timeout > 0 else 10.0
        self.cfg = Config(
            endpoint=cfg.endpoint,
            transport=cfg.transport,
            device=cfg.device,
            listener=cfg.listener,
            ws_path=cfg.ws_path,
            disable_reconnect=cfg.disable_reconnect,
            reconnect_min_delay=cfg.reconnect_min_delay,
            reconnect_max_delay=cfg.reconnect_max_delay,
            reconnect_max_attempts=cfg.reconnect_max_attempts,
            refresh_auth=cfg.refresh_auth,
            max_queue_size=cfg.max_queue_size or 10_000,
            hello_timeout=hello,
        )
        self._lock = asyncio.Lock()
        self._state = ConnectionState.CONNECTING
        self._track_uid = ""
        self._client_seq = 0
        self._last_acked_seq = 0
        self._queue = OfflineQueue(self.cfg.max_queue_size)
        self._backoff = new_backoff(
            self.cfg.reconnect_min_delay,
            self.cfg.reconnect_max_delay,
            self.cfg.reconnect_max_attempts,
        )
        self._next_publish_at = 0.0
        self._next_event_at = 0.0
        self._subscriptions: set[str] = set()
        self._intentional = False
        self._dial_gen = 0
        self._ws: Any | None = None
        self._grpc_channel: grpc.aio.Channel | None = None
        self._grpc_call: Any | None = None
        self._send_q: asyncio.Queue[ClientMsg | None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._recv_q: asyncio.Queue[ServerMsg] = asyncio.Queue(maxsize=64)
        self._cmd_q: asyncio.Queue[Command] = asyncio.Queue(maxsize=16)
        self._start_fut: asyncio.Future[str] | None = None
        self._stop_fut: asyncio.Future[None] | None = None
        self._resume_fut: asyncio.Future[int] | None = None

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def track_uid(self) -> str:
        return self._track_uid

    @property
    def client_seq(self) -> int:
        return self._client_seq

    @property
    def last_acked_seq(self) -> int:
        return self._last_acked_seq

    async def _set_state(self, s: ConnectionState) -> None:
        self._state = s

    async def dial(self, *, send_resume: bool = False) -> None:
        async with self._lock:
            await self._clear_reconnect_locked()
            self._dial_gen += 1
            gen = self._dial_gen
            if self._state in (ConnectionState.OPEN, ConnectionState.RECONNECTING):
                await self._set_state(ConnectionState.RECONNECTING)
            else:
                await self._set_state(ConnectionState.CONNECTING)
            cfg = self.cfg

        url = build_ws_url(cfg)
        ws = await ws_connect(url, subprotocols=[SUBPROTOCOL], open_timeout=cfg.hello_timeout)
        if ws.subprotocol != SUBPROTOCOL:
            await ws.close()
            raise Error(ErrorCode.ERROR_CODE_INVALID, f"server did not accept {SUBPROTOCOL}")

        async with self._lock:
            if gen != self._dial_gen or self._intentional:
                await ws.close()
                raise Error(ErrorCode.ERROR_CODE_INVALID, "dial superseded")
            await self._close_transport_locked()
            self._ws = ws
            self._send_q = asyncio.Queue()
            self._writer_task = asyncio.create_task(self._write_loop_ws(ws, self._send_q))

        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=cfg.hello_timeout)
        except TimeoutError as e:
            await ws.close()
            raise Error(ErrorCode.ERROR_CODE_INVALID, "hello timeout") from e
        if isinstance(raw, str):
            await ws.close()
            raise Error(ErrorCode.ERROR_CODE_INVALID, "expected binary hello")
        msg = decode_server_msg(raw)
        kind = msg.WhichOneof("body")
        if kind == "relocate":
            await ws.close()
            await self._handle_relocate(msg.relocate, send_resume=send_resume)
            return
        if kind == "error":
            await ws.close()
            raise error_from_wire(msg.error)
        if kind != "hello":
            await ws.close()
            raise Error(ErrorCode.ERROR_CODE_INVALID, f"expected hello, got {kind}")

        async with self._lock:
            if gen != self._dial_gen or self._intentional:
                await ws.close()
                raise Error(ErrorCode.ERROR_CODE_INVALID, "dial superseded")
            await self._set_state(ConnectionState.OPEN)
            reset_backoff(self._backoff)
            self._reader_task = asyncio.create_task(self._read_loop_ws(ws, gen))

        if send_resume:
            await self._send_resume_and_wait()
        await self._resubscribe()

    async def _connect_grpc(self) -> None:
        target = self.cfg.endpoint
        channel = grpc.aio.insecure_channel(target)
        md: list[tuple[str, str]] = []
        if self.cfg.device is not None:
            md.append(("x-client-id", self.cfg.device.client_id))
            md.append(("x-client-secret", self.cfg.device.client_secret))
        elif self.cfg.listener is not None:
            md.append(("authorization", f"Bearer {self.cfg.listener.access_token}"))
        stub = TrackingStub(channel)
        call = stub.Session(metadata=md)
        async with self._lock:
            self._grpc_channel = channel
            self._grpc_call = call
            self._send_q = asyncio.Queue()
            self._writer_task = asyncio.create_task(self._write_loop_grpc(call, self._send_q))
            self._reader_task = asyncio.create_task(self._read_loop_grpc(call))
            await self._set_state(ConnectionState.OPEN)

    async def _write_loop_ws(self, ws: Any, q: asyncio.Queue[ClientMsg | None]) -> None:
        try:
            while True:
                msg = await q.get()
                if msg is None:
                    break
                await ws.send(encode_client_msg(msg))
        except Exception:
            pass

    async def _write_loop_grpc(self, call: Any, q: asyncio.Queue[ClientMsg | None]) -> None:
        try:
            while True:
                msg = await q.get()
                if msg is None:
                    break
                await call.write(msg)
        except Exception:
            pass

    async def _read_loop_ws(self, ws: Any, gen: int) -> None:
        try:
            async for raw in ws:
                if isinstance(raw, str):
                    continue
                try:
                    msg = decode_server_msg(raw)
                except Exception:
                    continue
                await self._dispatch(msg)
        except ConnectionClosed:
            pass
        finally:
            await self._on_socket_closed(gen)

    async def _read_loop_grpc(self, call: Any) -> None:
        try:
            async for msg in call:
                await self._dispatch(msg)
        except Exception:
            pass

    async def _dispatch(self, msg: ServerMsg) -> None:
        kind = msg.WhichOneof("body")
        if kind == "relocate":
            asyncio.create_task(self._handle_relocate(msg.relocate, send_resume=True))
            return
        if kind == "resume_ok":
            async with self._lock:
                if msg.resume_ok.track_uid:
                    self._track_uid = msg.resume_ok.track_uid
                self._last_acked_seq = msg.resume_ok.last_acked_seq
                if self._client_seq < self._last_acked_seq:
                    self._client_seq = self._last_acked_seq
                self._queue.ack_through(self._last_acked_seq)
                fut = self._resume_fut
                self._resume_fut = None
            await self._flush_queue()
            if fut is not None and not fut.done():
                fut.set_result(msg.resume_ok.last_acked_seq)
            await self._push_recv(msg)
            return
        if kind == "track_started":
            async with self._lock:
                self._track_uid = msg.track_started.track_uid
                self._client_seq = 0
                self._last_acked_seq = 0
                self._queue.clear()
                fut = self._start_fut
                self._start_fut = None
            if fut is not None and not fut.done():
                fut.set_result(msg.track_started.track_uid)
            await self._push_recv(msg)
            return
        if kind == "track_stopped":
            async with self._lock:
                if self._track_uid == msg.track_stopped.track_uid:
                    self._track_uid = ""
                    self._queue.clear()
                fut = self._stop_fut
                self._stop_fut = None
            if fut is not None and not fut.done():
                fut.set_result(None)
            await self._push_recv(msg)
            return
        if kind == "location_added":
            async with self._lock:
                if msg.location_added.client_seq > self._last_acked_seq:
                    self._last_acked_seq = msg.location_added.client_seq
                self._queue.ack_through(msg.location_added.client_seq)
            await self._push_recv(msg)
            return
        if kind == "command":
            try:
                self._cmd_q.put_nowait(msg.command)
            except asyncio.QueueFull:
                pass
            return
        if kind == "error":
            err = error_from_wire(msg.error)
            async with self._lock:
                if self._resume_fut is not None:
                    fut = self._resume_fut
                    self._resume_fut = None
                    if is_fatal_resume_error(err.code):
                        self._track_uid = ""
                        self._queue.clear()
                    if not fut.done():
                        fut.set_exception(err)
                elif self._start_fut is not None:
                    fut = self._start_fut
                    self._start_fut = None
                    if not fut.done():
                        fut.set_exception(err)
                elif self._stop_fut is not None:
                    fut = self._stop_fut
                    self._stop_fut = None
                    if not fut.done():
                        fut.set_exception(err)
            if is_auth_error(err.code):
                asyncio.create_task(self._handle_auth_error())
            await self._push_recv(msg)
            return
        await self._push_recv(msg)

    async def _push_recv(self, msg: ServerMsg) -> None:
        try:
            self._recv_q.put_nowait(msg)
        except asyncio.QueueFull:
            pass

    async def _handle_relocate(self, rel: Any, *, send_resume: bool) -> None:
        if rel.endpoint:
            async with self._lock:
                self.cfg.endpoint = rel.endpoint
        delay_ms = rel.retry_after_ms
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)
        async with self._lock:
            if self._track_uid:
                send_resume = True
            intentional = self._intentional
        if intentional:
            raise Error(ErrorCode.ERROR_CODE_INVALID, "closed")
        await self.dial(send_resume=send_resume)

    async def _handle_auth_error(self) -> None:
        refresh = self.cfg.refresh_auth
        if refresh is None:
            async with self._lock:
                self._intentional = True
                await self._clear_reconnect_locked()
                await self._set_state(ConnectionState.CLOSED)
                await self._close_transport_locked()
            return
        try:
            device, listener = await asyncio.wait_for(refresh(), timeout=15.0)
        except Exception:
            async with self._lock:
                self._intentional = True
                await self._set_state(ConnectionState.CLOSED)
            return
        async with self._lock:
            if device is not None:
                self.cfg.device = device
                self.cfg.listener = None
            if listener is not None:
                self.cfg.listener = listener
                self.cfg.device = None
            send_resume = bool(self._track_uid)
            intentional = self._intentional
            self._dial_gen += 1
            await self._clear_reconnect_locked()
            await self._close_transport_locked()
        if intentional:
            return
        try:
            await self.dial(send_resume=send_resume)
        except Exception:
            async with self._lock:
                if not self._intentional:
                    await self._schedule_reconnect_locked()

    async def _on_socket_closed(self, gen: int) -> None:
        async with self._lock:
            if gen != self._dial_gen:
                return
            self._ws = None
            self._send_q = None
            if self._intentional:
                await self._set_state(ConnectionState.CLOSED)
                return
            if self.cfg.disable_reconnect or self.cfg.transport == Transport.GRPC:
                await self._set_state(ConnectionState.CLOSED)
                self._reject_pending_locked(Error(ErrorCode.ERROR_CODE_INVALID, "connection closed"))
                return
            await self._schedule_reconnect_locked()

    async def _schedule_reconnect_locked(self) -> None:
        await self._set_state(ConnectionState.RECONNECTING)
        delay = next_delay(self._backoff)
        if delay is None:
            await self._set_state(ConnectionState.CLOSED)
            self._reject_pending_locked(
                Error(ErrorCode.ERROR_CODE_INVALID, "reconnect attempts exhausted")
            )
            return
        send_resume = bool(self._track_uid)
        await self._clear_reconnect_locked()

        async def _run() -> None:
            await asyncio.sleep(delay)
            async with self._lock:
                self._reconnect_task = None
                intentional = self._intentional
            if intentional:
                return
            try:
                await self.dial(send_resume=send_resume)
            except Exception:
                async with self._lock:
                    if self._intentional or self._state == ConnectionState.OPEN:
                        return
                    await self._schedule_reconnect_locked()

        self._reconnect_task = asyncio.create_task(_run())

    def _reject_pending_locked(self, err: Exception) -> None:
        for attr in ("_start_fut", "_stop_fut", "_resume_fut"):
            fut = getattr(self, attr)
            if fut is not None and not fut.done():
                fut.set_exception(err)
            setattr(self, attr, None)

    async def _clear_reconnect_locked(self) -> None:
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            self._reconnect_task = None

    async def _close_transport_locked(self) -> None:
        if self._send_q is not None:
            try:
                self._send_q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        if self._writer_task is not None:
            self._writer_task.cancel()
            self._writer_task = None
        if self._reader_task is not None:
            self._reader_task.cancel()
            self._reader_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._grpc_call is not None:
            try:
                await self._grpc_call.done_writing()
            except Exception:
                pass
            self._grpc_call = None
        if self._grpc_channel is not None:
            await self._grpc_channel.close()
            self._grpc_channel = None
        self._send_q = None

    async def _resubscribe(self) -> None:
        async with self._lock:
            subs = list(self._subscriptions)
        for d in subs:
            msg = ClientMsg()
            msg.subscribe.CopyFrom(Subscribe(device_uid=d))
            try:
                await self.send(msg)
            except Exception:
                pass

    async def _send_resume_and_wait(self) -> None:
        async with self._lock:
            uid = self._track_uid
            seq = self._client_seq
            if not uid:
                return
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[int] = loop.create_future()
            self._resume_fut = fut
        msg = ClientMsg()
        msg.resume.CopyFrom(Resume(track_uid=uid, last_client_seq=seq))
        try:
            await self.send(msg)
        except Exception:
            async with self._lock:
                self._resume_fut = None
            raise
        await fut

    async def _flush_queue(self) -> None:
        async with self._lock:
            uid = self._track_uid
            pending = self._queue.peek_all()
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
        if not uid or not open_ or not pending:
            return
        points = [p.point for p in pending]
        last = pending[-1].seq
        msg = ClientMsg()
        batch = LocationBatch(track_uid=uid, client_seq=last)
        batch.points.extend(stamp_lat_lngs([clone_lat_lng(p) for p in points if p is not None]))  # type: ignore[misc]
        msg.location_batch.CopyFrom(batch)
        try:
            await self.send(msg)
        except Exception:
            pass

    async def recv(self) -> ServerMsg:
        return await self._recv_q.get()

    def commands(self) -> AsyncIterator[Command]:
        async def _iter() -> AsyncIterator[Command]:
            while True:
                yield await self._cmd_q.get()

        return _iter()

    async def recv_command(self) -> Command:
        return await self._cmd_q.get()

    async def ack_command(
        self,
        command_id: str,
        status: int,
        message: str = "",
    ) -> None:
        ack = CommandAck(command_id=command_id, status=status)
        if message:
            ack.message = message
        msg = ClientMsg()
        msg.command_ack.CopyFrom(ack)
        await self.send(msg)

    async def send(self, msg: ClientMsg) -> None:
        async with self._lock:
            if self._state == ConnectionState.CLOSED and self._intentional:
                raise Error(ErrorCode.ERROR_CODE_INVALID, "closed")
            q = self._send_q
            if q is None:
                raise Error(ErrorCode.ERROR_CODE_INVALID, "socket not open")
            await q.put(msg)

    async def start_track(
        self,
        loc: LatLng | None = None,
        route: list[LatLng] | None = None,
        metadata: bytes | None = None,
    ) -> str:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        async with self._lock:
            self._start_fut = fut
        start = TrackStart()
        if loc is not None:
            start.location.CopyFrom(stamp_lat_lng(clone_lat_lng(loc)) or LatLng())
        if route:
            start.route.extend(stamp_lat_lngs([clone_lat_lng(p) for p in route if p]))  # type: ignore[misc]
        if metadata:
            start.metadata = metadata
        msg = ClientMsg()
        msg.track_start.CopyFrom(start)
        try:
            await self.send(msg)
        except Exception:
            async with self._lock:
                self._start_fut = None
            raise
        return await fut

    async def resume(self, track_uid: str, last_client_seq: int) -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        async with self._lock:
            self._track_uid = track_uid
            self._client_seq = last_client_seq
            self._resume_fut = fut
        msg = ClientMsg()
        msg.resume.CopyFrom(Resume(track_uid=track_uid, last_client_seq=last_client_seq))
        try:
            await self.send(msg)
        except Exception:
            async with self._lock:
                self._resume_fut = None
            raise
        return await fut

    async def publish(self, point: LatLng) -> tuple[int, bool]:
        async with self._lock:
            if not self._track_uid:
                return 0, False
            now = time.monotonic()
            if not can_accept_publish(self._next_publish_at, now, 1):
                return self._client_seq, False
            self._next_publish_at = next_publish_allowed_at(self._next_publish_at, now, 1)
            self._client_seq += 1
            seq = self._client_seq
            uid = self._track_uid
            pt = stamp_lat_lng(clone_lat_lng(point))
            assert pt is not None
            self._queue.enqueue(seq, pt)
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
        if open_:
            msg = ClientMsg()
            msg.location_add.CopyFrom(LocationAdd(track_uid=uid, client_seq=seq, point=pt))
            try:
                await self.send(msg)
            except Exception:
                pass
        return seq, True

    async def stop_track(self, track_uid: str | None = None) -> None:
        uid = track_uid or self._track_uid
        if not uid:
            raise new_error(ErrorCode.ERROR_CODE_INVALID, "no active track")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        async with self._lock:
            self._stop_fut = fut
        msg = ClientMsg()
        msg.track_stop.CopyFrom(TrackStop(track_uid=uid))
        try:
            await self.send(msg)
        except Exception:
            async with self._lock:
                self._stop_fut = None
            raise
        await fut

    async def send_event(self, payload: bytes) -> bool:
        if len(payload) > MAX_EVENT_BYTES:
            raise new_error(ErrorCode.ERROR_CODE_INVALID, "event payload exceeds 4 KiB")
        async with self._lock:
            uid = self._track_uid
            if not uid:
                raise new_error(ErrorCode.ERROR_CODE_INVALID, "startTrack() before sendEvent()")
            now = time.monotonic()
            if self._next_event_at and now < self._next_event_at:
                return False
            self._next_event_at = now + MIN_EVENT_INTERVAL
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
        if not open_:
            return True
        ev = Event(track_uid=uid, payload=payload)
        ev.timestamp_ms = int(time.time() * 1000)
        msg = ClientMsg()
        msg.event.CopyFrom(ev)
        await self.send(msg)
        return True

    async def subscribe(self, device_uid: str) -> None:
        async with self._lock:
            self._subscriptions.add(device_uid)
        msg = ClientMsg()
        msg.subscribe.CopyFrom(Subscribe(device_uid=device_uid))
        await self.send(msg)

    async def close(self) -> None:
        async with self._lock:
            self._intentional = True
            await self._clear_reconnect_locked()
            await self._set_state(ConnectionState.CLOSED)
            self._reject_pending_locked(new_error(ErrorCode.ERROR_CODE_INVALID, "client closed"))
            await self._close_transport_locked()


async def connect(cfg: Config) -> Client:
    if not cfg.endpoint:
        raise Error(ErrorCode.ERROR_CODE_INVALID, "Endpoint is required")
    if cfg.device is None and cfg.listener is None:
        raise Error(ErrorCode.ERROR_CODE_INVALID, "Device or Listener auth is required")
    client = Client(cfg)
    if cfg.transport == Transport.GRPC:
        await client._connect_grpc()
        return client
    await client.dial(send_resume=False)
    return client

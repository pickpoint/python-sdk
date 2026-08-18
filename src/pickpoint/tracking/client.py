"""Realtime tracking client (binary WebSocket, tracking.v2)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from .backoff import new_backoff, next_delay, reset_backoff
from .codec import (
    clone_lat_lng,
    decode_server_msg,
    encode_client_msg,
    encode_inflight_frames,
    stamp_lat_lng,
    strip_live_time,
)
from .errors import Error, error_from_wire, is_auth_error, is_fatal_resume_error, is_retry_resume_error, new_error
from .filter import NoiseFilter
from .queue import OfflineQueue, QueuedPoint
from .rate import can_accept_publish, next_publish_allowed_at
from .types import (
    MAX_IN_FLIGHT_FRAMES,
    PROTOCOL_VERSION,
    SUBPROTOCOL,
    ClientMsg,
    Command,
    CommandAck,
    Config,
    ConnectionState,
    ErrorCode,
    Event,
    LatLng,
    Resume,
    ServerMsg,
    Subscribe,
    TrackStart,
    TrackStop,
    Unsubscribe,
)
from .url import build_ws_url

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
            subscribe=list(cfg.subscribe or []),
        )
        self._lock = asyncio.Lock()
        self._state = ConnectionState.CONNECTING
        self._track_uid = ""
        self._client_seq = 0
        self._last_acked_seq = 0
        self._queue = OfflineQueue(self.cfg.max_queue_size)
        self._filter = NoiseFilter()
        self._unacked_frames = 0
        self._backoff = new_backoff(
            self.cfg.reconnect_min_delay,
            self.cfg.reconnect_max_delay,
            self.cfg.reconnect_max_attempts,
        )
        self._next_publish_at = 0.0
        self._next_event_at = 0.0
        self._subscriptions: dict[str, dict[str, Any]] = {
            uid: {"include_events": True, "min_interval": 0, "handle": 0}
            for uid in (cfg.subscribe or [])
            if uid
        }
        self._sub_by_handle: dict[int, str] = {}
        self._intentional = False
        self._dial_gen = 0
        self._ws: Any | None = None
        self._send_q: asyncio.Queue[bytes | None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._recv_q: asyncio.Queue[ServerMsg] = asyncio.Queue(maxsize=64)
        self._cmd_q: asyncio.Queue[Command] = asyncio.Queue(maxsize=16)
        self._start_fut: asyncio.Future[str] | None = None
        self._stop_fut: asyncio.Future[None] | None = None
        self._resume_fut: asyncio.Future[int] | None = None
        self._starting = False

    @property
    def state(self) -> ConnectionState | str:
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

    async def _set_state(self, s: ConnectionState | str) -> None:
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
            self._unacked_frames = 0

        url = build_ws_url(cfg)
        ws = await ws_connect(url, subprotocols=[SUBPROTOCOL], open_timeout=cfg.hello_timeout)
        if ws.subprotocol != SUBPROTOCOL:
            await ws.close()
            raise Error(ErrorCode.INVALID, f"server did not accept {SUBPROTOCOL}")

        async with self._lock:
            if gen != self._dial_gen or self._intentional:
                await ws.close()
                raise Error(ErrorCode.INVALID, "dial superseded")
            await self._close_transport_locked()
            self._ws = ws
            self._send_q = asyncio.Queue()
            self._writer_task = asyncio.create_task(self._write_loop_ws(ws, self._send_q))

        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=cfg.hello_timeout)
        except TimeoutError as e:
            await ws.close()
            raise Error(ErrorCode.INVALID, "hello timeout") from e
        if isinstance(raw, str):
            await ws.close()
            raise Error(ErrorCode.INVALID, "expected binary hello")
        msg = decode_server_msg(raw)
        if msg is None:
            await ws.close()
            raise Error(ErrorCode.INVALID, "expected hello")
        if msg.relocate is not None:
            await ws.close()
            await self._handle_relocate(msg.relocate, send_resume=send_resume)
            return
        if msg.error is not None:
            await ws.close()
            raise error_from_wire(msg.error)
        if msg.hello is None:
            await ws.close()
            raise Error(ErrorCode.INVALID, f"expected hello, got {msg.kind()}")
        if msg.hello.version != PROTOCOL_VERSION:
            await ws.close()
            raise Error(ErrorCode.INVALID, f"unsupported protocol version {msg.hello.version}")

        async with self._lock:
            if gen != self._dial_gen or self._intentional:
                await ws.close()
                raise Error(ErrorCode.INVALID, "dial superseded")
            await self._set_state(ConnectionState.OPEN)
            reset_backoff(self._backoff)
            self._reader_task = asyncio.create_task(self._read_loop_ws(ws, gen))

        if send_resume:
            await self._send_resume_and_wait()
        await self._resubscribe()

    async def _write_loop_ws(self, ws: Any, q: asyncio.Queue[bytes | None]) -> None:
        try:
            while True:
                data = await q.get()
                if data is None:
                    break
                await ws.send(data)
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
                if msg is None:
                    continue
                await self._dispatch(msg)
        except ConnectionClosed:
            pass
        finally:
            await self._on_socket_closed(gen)

    async def _dispatch(self, msg: ServerMsg) -> None:
        if msg.relocate is not None:
            asyncio.create_task(self._handle_relocate(msg.relocate, send_resume=True))
            return
        if msg.resume_ok is not None:
            async with self._lock:
                if msg.resume_ok.track_uid:
                    self._track_uid = msg.resume_ok.track_uid
                self._last_acked_seq = msg.resume_ok.last_acked
                if self._client_seq < self._last_acked_seq:
                    self._client_seq = self._last_acked_seq
                self._queue.ack_through(self._last_acked_seq)
                self._unacked_frames = 0
                fut = self._resume_fut
                self._resume_fut = None
            await self._resend_inflight()
            await self._flush_staging()
            if fut is not None and not fut.done():
                fut.set_result(msg.resume_ok.last_acked)
            await self._push_recv(msg)
            return
        if msg.track_started is not None:
            async with self._lock:
                self._track_uid = msg.track_started.track_uid
                self._client_seq = 0
                self._last_acked_seq = 0
                self._unacked_frames = 0
                self._starting = False
                fut = self._start_fut
                self._start_fut = None
            if fut is not None and not fut.done():
                fut.set_result(msg.track_started.track_uid)
            await self._flush_staging()
            await self._push_recv(msg)
            return
        if msg.track_stopped is not None:
            async with self._lock:
                if self._track_uid == msg.track_stopped.track_uid or not msg.track_stopped.track_uid:
                    self._track_uid = ""
                    self._queue.clear()
                    self._filter.reset()
                fut = self._stop_fut
                self._stop_fut = None
            if fut is not None and not fut.done():
                fut.set_result(None)
            await self._push_recv(msg)
            return
        if msg.ack is not None:
            async with self._lock:
                if msg.ack.seq > self._last_acked_seq:
                    self._last_acked_seq = msg.ack.seq
                self._queue.ack_through(msg.ack.seq)
                self._unacked_frames = 0
            await self._flush_staging()
            return
        if msg.command is not None:
            try:
                self._cmd_q.put_nowait(msg.command)
            except asyncio.QueueFull:
                pass
            return
        if msg.error is not None:
            err = error_from_wire(msg.error)
            async with self._lock:
                if self._resume_fut is not None:
                    fut = self._resume_fut
                    self._resume_fut = None
                    if is_fatal_resume_error(err.code):
                        self._track_uid = ""
                        self._queue.clear()
                        self._filter.reset()
                        self._client_seq = 0
                        self._last_acked_seq = 0
                    if not fut.done():
                        if is_retry_resume_error(err.code):
                            fut.set_exception(err)
                        else:
                            fut.set_exception(err)
                elif self._start_fut is not None:
                    fut = self._start_fut
                    self._start_fut = None
                    self._starting = False
                    if not fut.done():
                        fut.set_exception(err)
                elif self._stop_fut is not None:
                    fut = self._stop_fut
                    self._stop_fut = None
                    if not fut.done():
                        fut.set_exception(err)
                elif err.code == ErrorCode.TRACK_NOT_FOUND:
                    self._track_uid = ""
                    self._queue.clear()
                    self._filter.reset()
            if is_auth_error(err.code):
                asyncio.create_task(self._handle_auth_error())
            await self._push_recv(msg)
            return
        if msg.subscribed is not None:
            async with self._lock:
                opts = self._subscriptions.get(msg.subscribed.device_uid)
                if opts is not None:
                    opts["handle"] = msg.subscribed.sub
                    self._sub_by_handle[msg.subscribed.sub] = msg.subscribed.device_uid
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
            raise Error(ErrorCode.INVALID, "closed")
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
            self._unacked_frames = 0
            self._queue.mark_unsent()
            if self._intentional:
                await self._set_state(ConnectionState.CLOSED)
                return
            if self.cfg.disable_reconnect:
                await self._set_state(ConnectionState.CLOSED)
                self._reject_pending_locked(Error(ErrorCode.INVALID, "connection closed"))
                return
            await self._schedule_reconnect_locked()

    async def _schedule_reconnect_locked(self) -> None:
        await self._set_state(ConnectionState.RECONNECTING)
        delay = next_delay(self._backoff)
        if delay is None:
            await self._set_state(ConnectionState.CLOSED)
            self._reject_pending_locked(Error(ErrorCode.INVALID, "reconnect attempts exhausted"))
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
        self._starting = False
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
        self._send_q = None

    async def _resubscribe(self) -> None:
        async with self._lock:
            items = [(uid, dict(opts)) for uid, opts in self._subscriptions.items()]
            self._sub_by_handle.clear()
            for opts in self._subscriptions.values():
                opts["handle"] = 0
        for uid, opts in items:
            msg = ClientMsg(
                subscribe=Subscribe(
                    device_uid=uid,
                    include_events=bool(opts.get("include_events", True)),
                    min_interval_ms=int(opts.get("min_interval", 0)),
                )
            )
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
        msg = ClientMsg(resume=Resume(track_uid=uid, last_seq=seq))
        try:
            await self.send(msg)
        except Exception:
            async with self._lock:
                self._resume_fut = None
            raise
        try:
            await fut
        except Error as err:
            if is_retry_resume_error(err.code):
                delay = (err.retry_after_ms / 1000.0) if err.retry_after_ms else 0.05
                await asyncio.sleep(delay)
                await self._send_resume_and_wait()
                return
            raise

    async def _send_raw(self, data: bytes) -> None:
        async with self._lock:
            q = self._send_q
            if q is None:
                raise Error(ErrorCode.INVALID, "socket not open")
            await q.put(data)

    async def _resend_inflight(self) -> None:
        async with self._lock:
            uid = self._track_uid
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
            pts = [(p.seq, p.point) for p in self._queue.peek_all()]
        if not uid or not open_ or not pts:
            return
        frames = encode_inflight_frames(pts)
        async with self._lock:
            self._unacked_frames += len(frames)
        for f in frames:
            try:
                await self._send_raw(f)
            except Exception:
                break

    async def _flush_staging(self) -> None:
        async with self._lock:
            uid = self._track_uid
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
            window = MAX_IN_FLIGHT_FRAMES - self._unacked_frames
            if not uid or not open_ or window <= 0:
                return
            assigned = self._queue.assign_from_staging(window * 100, self._client_seq)
            if assigned:
                self._client_seq = assigned[-1].seq
            q = self._send_q
        if not assigned or q is None:
            return
        frames = encode_inflight_frames([(p.seq, p.point) for p in assigned])
        async with self._lock:
            self._unacked_frames += len(frames)
        for f in frames:
            try:
                await q.put(f)
            except Exception:
                break

    async def recv(self) -> ServerMsg:
        return await self._recv_q.get()

    def commands(self) -> AsyncIterator[Command]:
        async def _iter() -> AsyncIterator[Command]:
            while True:
                yield await self._cmd_q.get()

        return _iter()

    async def recv_command(self) -> Command:
        return await self._cmd_q.get()

    async def ack_command(self, command_id: str, status: int, message: str = "") -> None:
        await self.send(
            ClientMsg(command_ack=CommandAck(command_id=command_id, status=status, message=message))
        )

    async def send(self, msg: ClientMsg) -> None:
        data = encode_client_msg(msg)
        async with self._lock:
            if self._state == ConnectionState.CLOSED and self._intentional:
                raise Error(ErrorCode.INVALID, "closed")
            q = self._send_q
            if q is None:
                raise Error(ErrorCode.INVALID, "socket not open")
            await q.put(data)

    async def start_track(
        self,
        loc: LatLng | None = None,
        route: list[LatLng] | None = None,
        metadata: bytes | None = None,
    ) -> str:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        async with self._lock:
            self._queue.clear()
            self._filter.reset()
            self._client_seq = 0
            self._last_acked_seq = 0
            self._starting = True
            self._start_fut = fut
        start = TrackStart(
            location=clone_lat_lng(loc),
            route=[clone_lat_lng(p) for p in (route or []) if p],  # type: ignore[misc]
            metadata=metadata or b"",
        )
        try:
            await self.send(ClientMsg(track_start=start))
        except Exception:
            async with self._lock:
                self._start_fut = None
                self._starting = False
            raise
        return await fut

    async def resume(self, track_uid: str, last_client_seq: int) -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        async with self._lock:
            self._track_uid = track_uid
            self._client_seq = last_client_seq
            self._resume_fut = fut
        try:
            await self.send(ClientMsg(resume=Resume(track_uid=track_uid, last_seq=last_client_seq)))
        except Exception:
            async with self._lock:
                self._resume_fut = None
            raise
        return await fut

    async def publish(self, point: LatLng) -> tuple[int, bool]:
        start_loc: LatLng | None = None
        async with self._lock:
            if not self._track_uid and not self._starting:
                self._starting = True
                self._queue.clear()
                self._filter.reset()
                self._client_seq = 0
                self._last_acked_seq = 0
                self._filter.seed(clone_lat_lng(point))
                start_loc = clone_lat_lng(point)
        if start_loc is not None:
            try:
                await self.send(ClientMsg(track_start=TrackStart(location=start_loc)))
            except Exception:
                async with self._lock:
                    self._starting = False
                return 0, False
            return 0, True
        async with self._lock:
            now = time.monotonic()
            emitted = self._filter.push(point)
            if emitted is None:
                return self._client_seq, False
            open_ = (
                self._state == ConnectionState.OPEN
                and self._send_q is not None
                and bool(self._track_uid)
            )
            window_ok = self._unacked_frames < MAX_IN_FLIGHT_FRAMES
            if not open_ or not window_ok:
                stamp_lat_lng(emitted)
                self._queue.push_staging(emitted)
                return self._client_seq, True
            if not can_accept_publish(self._next_publish_at, now, 1):
                return self._client_seq, False
            self._next_publish_at = next_publish_allowed_at(self._next_publish_at, now, 1)
            self._client_seq += 1
            seq = self._client_seq
            live = strip_live_time(emitted)
            self._queue.push_inflight_unsent(seq, emitted)
            q = self._send_q
        if q is not None:
            try:
                frames = encode_inflight_frames([(seq, live)])
                async with self._lock:
                    self._unacked_frames += len(frames)
                    self._queue.record_frame(seq)
                for f in frames:
                    await q.put(f)
            except Exception:
                pass
        return seq, True

    async def stop_track(self, track_uid: str | None = None) -> None:
        uid = track_uid or self._track_uid
        if not uid:
            raise new_error(ErrorCode.INVALID, "no active track")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        async with self._lock:
            self._stop_fut = fut
        try:
            await self.send(ClientMsg(track_stop=TrackStop()))
        except Exception:
            async with self._lock:
                self._stop_fut = None
            raise
        await fut

    async def send_event(self, payload: bytes) -> bool:
        if len(payload) > MAX_EVENT_BYTES:
            raise new_error(ErrorCode.INVALID, "event payload exceeds 4 KiB")
        async with self._lock:
            uid = self._track_uid
            if not uid:
                raise new_error(ErrorCode.INVALID, "startTrack() before sendEvent()")
            now = time.monotonic()
            if self._next_event_at and now < self._next_event_at:
                return False
            self._next_event_at = now + MIN_EVENT_INTERVAL
            open_ = self._state == ConnectionState.OPEN and self._send_q is not None
        if not open_:
            return True
        await self.send(
            ClientMsg(event=Event(payload=payload, timestamp_ms=int(time.time() * 1000)))
        )
        return True

    async def subscribe(
        self,
        device_uid: str,
        *,
        include_events: bool = True,
        min_interval_ms: int = 0,
    ) -> None:
        async with self._lock:
            self._subscriptions[device_uid] = {
                "include_events": include_events,
                "min_interval": min_interval_ms,
                "handle": 0,
            }
        await self.send(
            ClientMsg(
                subscribe=Subscribe(
                    device_uid=device_uid,
                    include_events=include_events,
                    min_interval_ms=min_interval_ms,
                )
            )
        )

    async def unsubscribe(self, sub: int) -> None:
        async with self._lock:
            uid = self._sub_by_handle.pop(int(sub), None)
            if uid is not None:
                self._subscriptions.pop(uid, None)
        await self.send(ClientMsg(unsubscribe=Unsubscribe(sub=int(sub))))

    async def close(self) -> None:
        uid = self._track_uid
        if uid:
            try:
                await self.send(ClientMsg(track_stop=TrackStop()))
            except Exception:
                pass
        async with self._lock:
            self._intentional = True
            await self._clear_reconnect_locked()
            await self._set_state(ConnectionState.CLOSED)
            self._reject_pending_locked(new_error(ErrorCode.INVALID, "client closed"))
            await self._close_transport_locked()


async def connect(cfg: Config) -> Client:
    if not cfg.endpoint:
        raise Error(ErrorCode.INVALID, "Endpoint is required")
    if cfg.device is None and cfg.listener is None:
        raise Error(ErrorCode.INVALID, "Device or Listener auth is required")
    client = Client(cfg)
    await client.dial(send_resume=False)
    return client

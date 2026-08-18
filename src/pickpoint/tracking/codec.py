from __future__ import annotations

import struct
import time

from .types import (
    MAX_LOC_POINTS,
    MAX_STRING,
    PROTOCOL_VERSION,
    Ack,
    ClientMsg,
    Command,
    CommandAck,
    ErrorCode,
    Event,
    EventAdded,
    Hello,
    LatLng,
    Loc,
    Presence,
    Relocate,
    Resume,
    ResumeOk,
    ServerLoc,
    ServerMsg,
    Subscribe,
    Subscribed,
    TrackStart,
    TrackStarted,
    TrackStop,
    TrackStopped,
    Unsubscribe,
    WireError,
)

C_RESUME = 0x01
C_TRACK_START = 0x02
C_TRACK_STOP = 0x03
C_LOC = 0x04
C_SUBSCRIBE = 0x05
C_UNSUBSCRIBE = 0x06
C_EVENT = 0x07
C_COMMAND_ACK = 0x08

S_HELLO = 0x80
S_RELOCATE = 0x81
S_RESUME_OK = 0x82
S_TRACK_STARTED = 0x83
S_TRACK_STOPPED = 0x84
S_ACK = 0x85
S_LOC = 0x86
S_SUBSCRIBED = 0x87
S_ERROR = 0x88
S_EVENT_ADDED = 0x89
S_COMMAND = 0x8A
S_PRESENCE = 0x8B

PF_ALT = 1 << 0
PF_ACC = 1 << 1
PF_TIME = 1 << 4

LAT_MIN = -90_000_000
LAT_MAX = 90_000_000
LON_MIN = -180_000_000
LON_MAX = 180_000_000
I16_MIN = -32768
I16_MAX = 32767


class DecodeError(ValueError):
    pass


class EncodeError(ValueError):
    pass


class _R:
    def __init__(self, data: bytes) -> None:
        self.b = data

    def need(self, n: int) -> bytes:
        if len(self.b) < n:
            raise DecodeError("truncated frame")
        head, self.b = self.b[:n], self.b[n:]
        return head

    def u8(self) -> int:
        return self.need(1)[0]

    def u16(self) -> int:
        return struct.unpack_from("<H", self.need(2))[0]

    def u32(self) -> int:
        return struct.unpack_from("<I", self.need(4))[0]

    def i16(self) -> int:
        return struct.unpack_from("<h", self.need(2))[0]

    def i32(self) -> int:
        return struct.unpack_from("<i", self.need(4))[0]

    def i64(self) -> int:
        return struct.unpack_from("<q", self.need(8))[0]

    def f64(self) -> float:
        return struct.unpack_from("<d", self.need(8))[0]

    def uuid(self) -> str:
        return format_uuid(self.need(16))

    def uuid_opt(self) -> str:
        raw = self.need(16)
        if all(x == 0 for x in raw):
            return ""
        return format_uuid(raw)

    def string(self) -> str:
        n = self.u16()
        if n > MAX_STRING:
            raise DecodeError("invalid frame")
        return self.need(n).decode("utf-8")

    def bytes(self) -> bytes:
        n = self.u16()
        if n > MAX_STRING:
            raise DecodeError("invalid frame")
        return self.need(n)


def _put_u8(w: bytearray, v: int) -> None:
    w.append(v & 0xFF)


def _put_u16(w: bytearray, v: int) -> None:
    w.extend(struct.pack("<H", v & 0xFFFF))


def _put_u32(w: bytearray, v: int) -> None:
    w.extend(struct.pack("<I", v & 0xFFFFFFFF))


def _put_i16(w: bytearray, v: int) -> None:
    w.extend(struct.pack("<h", v))


def _put_i32(w: bytearray, v: int) -> None:
    w.extend(struct.pack("<i", v))


def _put_i64(w: bytearray, v: int) -> None:
    w.extend(struct.pack("<q", v))


def _put_f64(w: bytearray, v: float) -> None:
    w.extend(struct.pack("<d", v))


def parse_uuid(s: str) -> bytes:
    if not s:
        return bytes(16)
    h = s.replace("-", "").lower()
    if len(h) != 32:
        return bytes(16)
    try:
        return bytes.fromhex(h)
    except ValueError:
        return bytes(16)


def format_uuid(b: bytes) -> str:
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _put_uuid(w: bytearray, s: str) -> None:
    w.extend(parse_uuid(s))


def _put_str(w: bytearray, s: str) -> None:
    raw = s.encode("utf-8")[:MAX_STRING]
    _put_u16(w, len(raw))
    w.extend(raw)


def _put_bytes(w: bytearray, b: bytes) -> None:
    raw = b[:MAX_STRING]
    _put_u16(w, len(raw))
    w.extend(raw)


def deg_to_micro(d: float) -> int:
    return int(round(d * 1_000_000.0))


def micro_to_deg(m: int) -> float:
    return m / 1_000_000.0


def check_coord(lat: int, lon: int) -> None:
    if lat < LAT_MIN or lat > LAT_MAX or lon < LON_MIN or lon > LON_MAX:
        raise DecodeError("invalid frame")


def micro_delta_fits(prev_lat: int, prev_lon: int, lat: int, lon: int) -> bool:
    dlat = lat - prev_lat
    dlon = lon - prev_lon
    return I16_MIN <= dlat <= I16_MAX and I16_MIN <= dlon <= I16_MAX


def _write_point(w: bytearray, p: LatLng, prev: tuple[int, int] | None) -> tuple[int, int]:
    lat = deg_to_micro(p.latitude)
    lon = deg_to_micro(p.longitude)
    flags = 0
    if p.altitude is not None:
        flags |= PF_ALT
    if p.accuracy is not None:
        flags |= PF_ACC
    if p.timestamp_ms is not None:
        flags |= PF_TIME
    _put_u8(w, flags)
    if prev is not None:
        if not micro_delta_fits(prev[0], prev[1], lat, lon):
            raise EncodeError("intra-frame delta overflows i16")
        _put_i16(w, lat - prev[0])
        _put_i16(w, lon - prev[1])
    else:
        _put_i32(w, lat)
        _put_i32(w, lon)
    if p.altitude is not None:
        _put_i32(w, int(round(p.altitude * 1000.0)))
    if p.accuracy is not None:
        cm = int(round(p.accuracy * 100.0))
        cm = max(0, min(0xFFFF, cm))
        _put_u16(w, cm)
    if p.timestamp_ms is not None:
        _put_i64(w, p.timestamp_ms)
    return lat, lon


def _write_abs(w: bytearray, p: LatLng) -> None:
    _write_point(w, p, None)


def _read_point(r: _R, prev: tuple[int, int] | None) -> tuple[LatLng, int, int]:
    flags = r.u8()
    if prev is not None:
        lat = prev[0] + r.i16()
        lon = prev[1] + r.i16()
        lat = max(-2_147_483_648, min(2_147_483_647, lat))
        lon = max(-2_147_483_648, min(2_147_483_647, lon))
    else:
        lat = r.i32()
        lon = r.i32()
    check_coord(lat, lon)
    p = LatLng(latitude=micro_to_deg(lat), longitude=micro_to_deg(lon))
    if flags & PF_ALT:
        p.altitude = r.i32() / 1000.0
    if flags & PF_ACC:
        p.accuracy = r.u16() / 100.0
    if flags & PF_TIME:
        p.timestamp_ms = r.i64()
    return p, lat, lon


def _write_route_abs(w: bytearray, route: list[LatLng]) -> None:
    n = min(len(route), 0xFFFF)
    _put_u16(w, n)
    for p in route[:n]:
        _put_i32(w, deg_to_micro(p.latitude))
        _put_i32(w, deg_to_micro(p.longitude))


def _read_route_abs(r: _R) -> list[LatLng]:
    n = r.u16()
    out: list[LatLng] = []
    for _ in range(n):
        lat = r.i32()
        lon = r.i32()
        check_coord(lat, lon)
        out.append(LatLng(latitude=micro_to_deg(lat), longitude=micro_to_deg(lon)))
    return out


def encode_loc_frames(last_seq: int, points: list[LatLng]) -> list[bytes]:
    if not points:
        return []
    first_seq = last_seq + 1 - len(points)
    out: list[bytes] = []
    i = 0
    while i < len(points):
        start = i
        prev_lat = deg_to_micro(points[i].latitude)
        prev_lon = deg_to_micro(points[i].longitude)
        i += 1
        while i < len(points) and (i - start) < MAX_LOC_POINTS:
            lat = deg_to_micro(points[i].latitude)
            lon = deg_to_micro(points[i].longitude)
            if not micro_delta_fits(prev_lat, prev_lon, lat, lon):
                break
            prev_lat, prev_lon = lat, lon
            i += 1
        chunk = points[start:i]
        seq = first_seq + i - 1
        out.append(_encode_loc_frame(seq, chunk))
    return out


def encode_inflight_frames(pts: list[tuple[int, LatLng]]) -> list[bytes]:
    if not pts:
        return []
    out: list[bytes] = []
    i = 0
    while i < len(pts):
        start = i
        prev_lat = deg_to_micro(pts[i][1].latitude)
        prev_lon = deg_to_micro(pts[i][1].longitude)
        i += 1
        while i < len(pts) and (i - start) < MAX_LOC_POINTS:
            if pts[i][0] != pts[i - 1][0] + 1:
                break
            lat = deg_to_micro(pts[i][1].latitude)
            lon = deg_to_micro(pts[i][1].longitude)
            if not micro_delta_fits(prev_lat, prev_lon, lat, lon):
                break
            prev_lat, prev_lon = lat, lon
            i += 1
        chunk = [p for _, p in pts[start:i]]
        out.append(_encode_loc_frame(pts[i - 1][0], chunk))
    return out


def _encode_loc_frame(seq: int, points: list[LatLng]) -> bytes:
    w = bytearray()
    _put_u8(w, C_LOC)
    _put_u32(w, seq)
    _put_u8(w, len(points))
    prev: tuple[int, int] | None = None
    for p in points:
        prev = _write_point(w, p, prev)
    return bytes(w)


def encode_client_msg(msg: ClientMsg) -> bytes:
    w = bytearray()
    if msg.resume is not None:
        _put_u8(w, C_RESUME)
        _put_uuid(w, msg.resume.track_uid)
        _put_u32(w, msg.resume.last_seq or msg.resume.last_client_seq)
    elif msg.track_start is not None:
        _put_u8(w, C_TRACK_START)
        flags = 1 if msg.track_start.location is not None else 0
        _put_u8(w, flags)
        if msg.track_start.location is not None:
            _write_abs(w, msg.track_start.location)
        _write_route_abs(w, msg.track_start.route)
        _put_bytes(w, msg.track_start.metadata)
    elif msg.track_stop is not None:
        _put_u8(w, C_TRACK_STOP)
    elif msg.loc is not None:
        frames = encode_loc_frames(msg.loc.seq, msg.loc.points)
        if not frames:
            raise EncodeError("empty loc")
        return frames[0]
    elif msg.subscribe is not None:
        _put_u8(w, C_SUBSCRIBE)
        _put_uuid(w, msg.subscribe.device_uid)
        _put_u8(w, 1 if msg.subscribe.include_events else 0)
        _put_u16(w, min(msg.subscribe.min_interval_ms, 0xFFFF))
    elif msg.unsubscribe is not None:
        _put_u8(w, C_UNSUBSCRIBE)
        _put_u8(w, msg.unsubscribe.sub)
    elif msg.event is not None:
        _put_u8(w, C_EVENT)
        _put_bytes(w, msg.event.payload)
        _put_i64(w, msg.event.timestamp_ms)
    elif msg.command_ack is not None:
        _put_u8(w, C_COMMAND_ACK)
        _put_uuid(w, msg.command_ack.command_id)
        _put_u8(w, int(msg.command_ack.status))
        _put_str(w, msg.command_ack.message)
    else:
        raise EncodeError("empty client msg")
    return bytes(w)


def decode_client_msg(data: bytes) -> ClientMsg:
    if not data:
        raise DecodeError("truncated frame")
    r = _R(data)
    typ = r.u8()
    if typ == C_RESUME:
        return ClientMsg(resume=Resume(track_uid=r.uuid(), last_seq=r.u32()))
    if typ == C_TRACK_START:
        flags = r.u8()
        loc = _read_point(r, None)[0] if flags & 1 else None
        route = _read_route_abs(r)
        meta = r.bytes()
        return ClientMsg(track_start=TrackStart(location=loc, route=route, metadata=meta))
    if typ == C_TRACK_STOP:
        return ClientMsg(track_stop=TrackStop())
    if typ == C_LOC:
        seq = r.u32()
        count = r.u8()
        if count == 0 or count > MAX_LOC_POINTS:
            raise DecodeError("invalid frame")
        points: list[LatLng] = []
        prev: tuple[int, int] | None = None
        for _ in range(count):
            p, lat, lon = _read_point(r, prev)
            points.append(p)
            prev = (lat, lon)
        return ClientMsg(loc=Loc(seq=seq, points=points))
    if typ == C_SUBSCRIBE:
        return ClientMsg(
            subscribe=Subscribe(
                device_uid=r.uuid(),
                include_events=bool(r.u8() & 1),
                min_interval_ms=r.u16(),
            )
        )
    if typ == C_UNSUBSCRIBE:
        return ClientMsg(unsubscribe=Unsubscribe(sub=r.u8()))
    if typ == C_EVENT:
        return ClientMsg(event=Event(payload=r.bytes(), timestamp_ms=r.i64()))
    if typ == C_COMMAND_ACK:
        return ClientMsg(
            command_ack=CommandAck(
                command_id=r.uuid(),
                status=r.u8(),
                message=r.string(),
            )
        )
    if typ in (0x00, 0x7F, 0xFF):
        raise DecodeError("invalid frame")
    if 0x01 <= typ <= 0x7E:
        return ClientMsg(unknown=typ)
    raise DecodeError("invalid frame")


def encode_server_msg(msg: ServerMsg) -> bytes:
    w = bytearray()
    if msg.hello is not None:
        _put_u8(w, S_HELLO)
        _put_u8(w, msg.hello.version)
        _put_u16(w, msg.hello.shard)
        _put_uuid(w, msg.hello.node_id)
    elif msg.relocate is not None:
        _put_u8(w, S_RELOCATE)
        _put_u32(w, msg.relocate.retry_after_ms)
        _put_str(w, msg.relocate.endpoint)
    elif msg.resume_ok is not None:
        _put_u8(w, S_RESUME_OK)
        _put_uuid(w, msg.resume_ok.track_uid)
        _put_u32(w, msg.resume_ok.last_acked or msg.resume_ok.last_acked_seq)
    elif msg.track_started is not None:
        _put_u8(w, S_TRACK_STARTED)
        _put_uuid(w, msg.track_started.track_uid)
        _put_bytes(w, msg.track_started.metadata)
    elif msg.track_stopped is not None:
        _put_u8(w, S_TRACK_STOPPED)
        _put_uuid(w, msg.track_stopped.track_uid)
    elif msg.ack is not None:
        _put_u8(w, S_ACK)
        _put_u32(w, msg.ack.seq)
    elif msg.loc is not None:
        _put_u8(w, S_LOC)
        _put_u8(w, msg.loc.sub)
        _put_u32(w, msg.loc.seq)
        _write_abs(w, msg.loc.point)
    elif msg.subscribed is not None:
        s = msg.subscribed
        _put_u8(w, S_SUBSCRIBED)
        _put_u8(w, s.sub)
        _put_uuid(w, s.device_uid)
        _put_uuid(w, s.track_uid)
        _put_u8(w, 1 if s.online else 0)
        flags = 0
        if s.last_location is not None:
            flags |= 1
        if s.last_seen_ms is not None:
            flags |= 2
        if s.route:
            flags |= 4
        _put_u8(w, flags)
        if s.last_location is not None:
            _write_abs(w, s.last_location)
        if s.last_seen_ms is not None:
            _put_i64(w, s.last_seen_ms)
        if flags & 4:
            _write_route_abs(w, s.route)
        _put_f64(w, s.est_distance)
        _put_f64(w, s.est_duration)
        _put_str(w, s.start_name)
        _put_str(w, s.end_name)
        _put_bytes(w, s.metadata)
    elif msg.error is not None:
        _put_u8(w, S_ERROR)
        _put_u8(w, int(msg.error.code))
        _put_u32(w, msg.error.retry_after_ms)
        _put_uuid(w, msg.error.track_uid)
        _put_str(w, msg.error.message)
    elif msg.event_added is not None:
        _put_u8(w, S_EVENT_ADDED)
        _put_u8(w, msg.event_added.sub)
        _put_bytes(w, msg.event_added.payload)
        _put_i64(w, msg.event_added.timestamp_ms)
    elif msg.command is not None:
        _put_u8(w, S_COMMAND)
        _put_uuid(w, msg.command.command_id)
        _put_bytes(w, msg.command.payload)
        _put_i64(w, msg.command.timestamp_ms)
    elif msg.presence is not None:
        _put_u8(w, S_PRESENCE)
        _put_u8(w, msg.presence.sub)
        _put_u8(w, 1 if msg.presence.online else 0)
        _put_i64(w, msg.presence.last_seen_ms)
    else:
        raise EncodeError("empty server msg")
    return bytes(w)


def decode_server_msg(data: bytes) -> ServerMsg | None:
    if not data:
        raise DecodeError("truncated frame")
    r = _R(data)
    typ = r.u8()
    if typ == S_HELLO:
        return ServerMsg(hello=Hello(version=r.u8(), shard=r.u16(), node_id=r.uuid()))
    if typ == S_RELOCATE:
        return ServerMsg(relocate=Relocate(retry_after_ms=r.u32(), endpoint=r.string()))
    if typ == S_RESUME_OK:
        return ServerMsg(resume_ok=ResumeOk(track_uid=r.uuid(), last_acked=r.u32()))
    if typ == S_TRACK_STARTED:
        return ServerMsg(track_started=TrackStarted(track_uid=r.uuid(), metadata=r.bytes()))
    if typ == S_TRACK_STOPPED:
        return ServerMsg(track_stopped=TrackStopped(track_uid=r.uuid()))
    if typ == S_ACK:
        return ServerMsg(ack=Ack(seq=r.u32()))
    if typ == S_LOC:
        sub = r.u8()
        seq = r.u32()
        point, _, _ = _read_point(r, None)
        return ServerMsg(loc=ServerLoc(sub=sub, seq=seq, point=point))
    if typ == S_SUBSCRIBED:
        sub = r.u8()
        device_uid = r.uuid()
        track_uid = r.uuid_opt()
        online = r.u8() != 0
        flags = r.u8()
        last_loc = _read_point(r, None)[0] if flags & 1 else None
        last_seen = r.i64() if flags & 2 else None
        route = _read_route_abs(r) if flags & 4 else []
        dist = r.f64()
        dur = r.f64()
        start = r.string()
        end = r.string()
        meta = r.bytes()
        return ServerMsg(
            subscribed=Subscribed(
                sub=sub,
                device_uid=device_uid,
                track_uid=track_uid,
                online=online,
                last_location=last_loc,
                last_seen_ms=last_seen,
                route=route,
                est_distance=dist,
                est_duration=dur,
                start_name=start,
                end_name=end,
                metadata=meta,
            )
        )
    if typ == S_ERROR:
        code_u8 = r.u8()
        try:
            code = ErrorCode(code_u8)
        except ValueError as e:
            raise DecodeError("invalid frame") from e
        retry = r.u32()
        uid = r.uuid_opt()
        message = r.string()
        return ServerMsg(
            error=WireError(code=code, retry_after_ms=retry, track_uid=uid, message=message)
        )
    if typ == S_EVENT_ADDED:
        return ServerMsg(event_added=EventAdded(sub=r.u8(), payload=r.bytes(), timestamp_ms=r.i64()))
    if typ == S_COMMAND:
        return ServerMsg(command=Command(command_id=r.uuid(), payload=r.bytes(), timestamp_ms=r.i64()))
    if typ == S_PRESENCE:
        return ServerMsg(presence=Presence(sub=r.u8(), online=r.u8() != 0, last_seen_ms=r.i64()))
    if typ in (0x00, 0x7F, 0xFF, 0x8C):
        raise DecodeError("invalid frame")
    if 0x80 <= typ <= 0xFE:
        return None
    raise DecodeError("invalid frame")


def stamp_lat_lng(p: LatLng | None) -> LatLng | None:
    if p is None:
        return None
    if p.timestamp_ms is None:
        p.timestamp_ms = int(time.time() * 1000)
    return p


def stamp_lat_lngs(points: list[LatLng] | None) -> list[LatLng]:
    if not points:
        return []
    for p in points:
        stamp_lat_lng(p)
    return points


def clone_lat_lng(p: LatLng | None) -> LatLng | None:
    if p is None:
        return None
    return LatLng(
        latitude=p.latitude,
        longitude=p.longitude,
        altitude=p.altitude,
        accuracy=p.accuracy,
        heading=p.heading,
        speed=p.speed,
        timestamp_ms=p.timestamp_ms,
    )


def strip_live_time(p: LatLng) -> LatLng:
    return LatLng(
        latitude=p.latitude,
        longitude=p.longitude,
        altitude=p.altitude,
        accuracy=p.accuracy,
        heading=p.heading,
        speed=p.speed,
        timestamp_ms=None,
    )


def client_resume(track_uid: str, last_client_seq: int) -> ClientMsg:
    return ClientMsg(resume=Resume(track_uid=track_uid, last_seq=int(last_client_seq)))


def protocol_version() -> int:
    return PROTOCOL_VERSION

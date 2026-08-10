from __future__ import annotations

import time

from pickpoint.tracking import (
    MAX_PUBLISH_HZ,
    MIN_PUBLISH_INTERVAL,
    MIN_PUBLISH_INTERVAL_MS,
    Config,
    DeviceAuth,
    ListenerAuth,
    OfflineQueue,
    build_ws_url,
    can_accept_publish,
    client_resume,
    decode_server_msg,
    encode_client_msg,
    new_backoff,
    next_delay,
    next_publish_allowed_at,
    reset_backoff,
    stamp_lat_lng,
)
from pickpoint.tracking.v2 import ClientMsg, Hello, LatLng, ServerMsg


def test_backoff_full_jitter() -> None:
    state = new_backoff(0.1, 0.8, 0)
    assert next_delay(state, 0.0) == 0.0
    assert next_delay(state, 0.5) == 0.1
    assert next_delay(state, 0.999) == 0.399


def test_backoff_max_attempts() -> None:
    state = new_backoff(0.01, 0.0, 2)
    assert next_delay(state, 0.0) is not None
    assert next_delay(state, 0.0) is not None
    assert next_delay(state, 0.0) is None


def test_backoff_reset() -> None:
    state = new_backoff(0.01, 0.0, 1)
    assert next_delay(state, 0.0) is not None
    assert next_delay(state, 0.0) is None
    reset_backoff(state)
    assert next_delay(state, 0.0) is not None


def test_offline_queue_ack_through() -> None:
    q = OfflineQueue(10)
    q.enqueue(1, LatLng(latitude=1.0))
    q.enqueue(2, LatLng(latitude=2.0))
    q.enqueue(3, LatLng(latitude=3.0))
    q.ack_through(2)
    got = q.peek_all()
    assert len(got) == 1
    assert got[0].seq == 3


def test_offline_queue_drop_oldest() -> None:
    q = OfflineQueue(2)
    p = LatLng(latitude=1.0)
    assert q.enqueue(1, p) == 0
    assert q.enqueue(2, p) == 0
    assert q.enqueue(3, p) == 1
    got = q.peek_all()
    assert [x.seq for x in got] == [2, 3]


def test_publish_rate_spacing() -> None:
    assert MAX_PUBLISH_HZ == 50
    assert MIN_PUBLISH_INTERVAL_MS == 20
    now = time.monotonic()
    assert can_accept_publish(now, now, 1)
    nxt = next_publish_allowed_at(now, now, 1)
    assert abs(nxt - (now + 0.02)) < 1e-9
    assert not can_accept_publish(nxt, now + 0.019, 1)
    assert can_accept_publish(nxt, now + 0.02, 1)


def test_publish_rate_batch_slots() -> None:
    now = time.monotonic()
    nxt = next_publish_allowed_at(now, now, 50)
    want = now + MIN_PUBLISH_INTERVAL * 50
    assert abs(nxt - want) < 1e-9


def test_build_ws_url_device() -> None:
    u = build_ws_url(
        Config(
            endpoint="https://tracking.example.com",
            device=DeviceAuth(client_id="id", client_secret="sec"),
        )
    )
    assert u.startswith("wss://tracking.example.com/v2/tracking/ws?")
    assert "client-id=id" in u
    assert "client-secret=sec" in u


def test_build_ws_url_listener() -> None:
    u = build_ws_url(
        Config(endpoint="ws://localhost:1", listener=ListenerAuth(access_token="jwt"))
    )
    assert "access-token=jwt" in u


def test_stamp_lat_lng_default_timestamp() -> None:
    before = int(time.time() * 1000)
    p = stamp_lat_lng(LatLng(latitude=1.0, longitude=2.0))
    assert p is not None
    assert p.HasField("timestamp_ms")
    assert p.timestamp_ms >= before


def test_golden_resume_wire() -> None:
    msg = client_resume("track-uid-9", 42)
    b = encode_client_msg(msg)
    roundtrip = ClientMsg()
    roundtrip.ParseFromString(b)
    assert roundtrip.resume.track_uid == "track-uid-9"
    assert roundtrip.resume.last_client_seq == 42
    assert b.hex() == "0a0f0a0b747261636b2d7569642d39102a"


def test_encode_decode_hello() -> None:
    msg = ServerMsg()
    msg.hello.CopyFrom(Hello(node_id="n1"))
    raw = msg.SerializeToString()
    decoded = decode_server_msg(raw)
    assert decoded.WhichOneof("body") == "hello"
    assert decoded.hello.node_id == "n1"

from __future__ import annotations

import time

from pickpoint.tracking import (
    MAX_PUBLISH_HZ,
    MIN_PUBLISH_INTERVAL,
    MIN_PUBLISH_INTERVAL_MS,
    Config,
    DeviceAuth,
    ErrorCode,
    LatLng,
    ListenerAuth,
    OfflineQueue,
    ServerMsg,
    build_ws_url,
    can_accept_publish,
    client_resume,
    decode_client_msg,
    decode_server_msg,
    encode_client_msg,
    encode_loc_frames,
    encode_server_msg,
    is_fatal_resume_error,
    is_retry_resume_error,
    new_backoff,
    next_delay,
    next_publish_allowed_at,
    reset_backoff,
    stamp_lat_lng,
)
from pickpoint.tracking.codec import C_UNSUBSCRIBE, S_ACK, S_LOC, micro_delta_fits, deg_to_micro
from pickpoint.tracking.types import Ack, ClientMsg, Hello, Loc, ServerLoc, TrackStop, Unsubscribe


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
    assert u.startswith("wss://tracking.example.com/v2/ws?")
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
    assert p.timestamp_ms is not None
    assert p.timestamp_ms >= before


def test_golden_ack_seq1() -> None:
    b = encode_server_msg(ServerMsg(ack=Ack(seq=1)))
    assert b.hex() == "8501000000"
    msg = decode_server_msg(b)
    assert msg is not None and msg.ack is not None and msg.ack.seq == 1


def test_golden_loc_55n_37e() -> None:
    b = encode_client_msg(
        ClientMsg(loc=Loc(seq=1, points=[LatLng(latitude=55, longitude=37)]))
    )
    assert b.hex() == "04010000000100c03b470340933402"


def test_golden_resume() -> None:
    msg = client_resume("00112233-4455-6677-8899-aabbccddeeff", 45)
    b = encode_client_msg(msg)
    assert b.hex() == "0100112233445566778899aabbccddeeff2d000000"


def test_golden_track_stop() -> None:
    b = encode_client_msg(ClientMsg(track_stop=TrackStop()))
    assert b == bytes([0x03])


def test_device_ack_vs_listener_loc() -> None:
    ack = encode_server_msg(ServerMsg(ack=Ack(seq=1)))
    loc = encode_server_msg(
        ServerMsg(
            loc=ServerLoc(sub=1, seq=1, point=LatLng(latitude=55, longitude=37))
        )
    )
    assert ack[0] == S_ACK
    assert loc[0] == S_LOC
    assert ack[0] != loc[0]


def test_encode_loc_splits_on_i16_overflow() -> None:
    a = LatLng(latitude=0, longitude=0)
    b = LatLng(latitude=4, longitude=0)
    assert not micro_delta_fits(deg_to_micro(0), deg_to_micro(0), deg_to_micro(4), deg_to_micro(0))
    frames = encode_loc_frames(2, [a, b])
    assert len(frames) == 2
    m0 = decode_client_msg(frames[0])
    m1 = decode_client_msg(frames[1])
    assert m0.loc is not None and m0.loc.seq == 1
    assert m1.loc is not None and m1.loc.seq == 2
    assert m1.loc.points[0].latitude == 4


def test_unknown_server_type_ignored() -> None:
    assert decode_server_msg(bytes([0x8D])) is None


def test_unsubscribe_is_sub_handle() -> None:
    b = encode_client_msg(ClientMsg(unsubscribe=Unsubscribe(sub=7)))
    assert b == bytes([C_UNSUBSCRIBE, 0x07])


def test_fatal_resume_auth_and_track_not_found_only() -> None:
    assert is_fatal_resume_error(ErrorCode.AUTH)
    assert is_fatal_resume_error(ErrorCode.TRACK_NOT_FOUND)
    assert not is_fatal_resume_error(ErrorCode.FENCED)
    assert not is_fatal_resume_error(ErrorCode.TRY_AGAIN)
    assert not is_fatal_resume_error(ErrorCode.UNAUTHORIZED)
    assert is_retry_resume_error(ErrorCode.FENCED)
    assert is_retry_resume_error(ErrorCode.TRY_AGAIN)


def test_encode_decode_hello() -> None:
    raw = encode_server_msg(
        ServerMsg(hello=Hello(version=2, shard=7, node_id="00112233-4455-6677-8899-aabbccddeeff"))
    )
    decoded = decode_server_msg(raw)
    assert decoded is not None
    assert decoded.WhichOneof("body") == "hello"
    assert decoded.hello is not None
    assert decoded.hello.node_id == "00112233-4455-6677-8899-aabbccddeeff"
    assert decoded.hello.shard == 7
    assert decoded.hello.version == 2

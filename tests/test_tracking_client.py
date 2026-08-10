from __future__ import annotations

import asyncio

import pytest

from pickpoint import tracking
from pickpoint.tracking.v2 import ErrorCode, LatLng, LocationAdded, ServerMsg

from common.tracking_mock import MockConn, server_error, start_mock, wait_for


@pytest.mark.asyncio
async def test_publish_rate_limit() -> None:
    ms = await start_mock(True)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                device=tracking.DeviceAuth(client_id="c", client_secret="s"),
                disable_reconnect=True,
            )
        )
        try:
            await c.start_track(LatLng(latitude=1, longitude=2))
            accepted = 0
            for i in range(tracking.MAX_PUBLISH_HZ * 3):
                _, ok = await c.publish(LatLng(latitude=float(i), longitude=0))
                if ok:
                    accepted += 1
            assert accepted == 1
            assert c.client_seq == 1
            await asyncio.sleep(tracking.MIN_PUBLISH_INTERVAL + 0.005)
            seq, ok = await c.publish(LatLng(latitude=9, longitude=9))
            assert ok and seq == 2
        finally:
            await c.close()
    finally:
        await ms.close()


@pytest.mark.asyncio
async def test_send_event_limits() -> None:
    ms = await start_mock(True)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                device=tracking.DeviceAuth(client_id="c", client_secret="s"),
                disable_reconnect=True,
            )
        )
        try:
            await c.start_track()
            with pytest.raises(tracking.Error):
                await c.send_event(bytes(tracking.MAX_EVENT_BYTES + 1))
            ok = await c.send_event(b"a")
            assert ok
            ok = await c.send_event(b"b")
            assert not ok
        finally:
            await c.close()
    finally:
        await ms.close()


@pytest.mark.asyncio
async def test_resume_after_publish() -> None:
    ms = await start_mock(True)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                device=tracking.DeviceAuth(client_id="c", client_secret="s"),
                disable_reconnect=True,
            )
        )
        try:
            uid = await c.start_track(LatLng(latitude=1, longitude=1))
            ok = (await c.publish(LatLng(latitude=2, longitude=2)))[1]
            assert ok
            deadline = asyncio.get_event_loop().time() + 2
            while asyncio.get_event_loop().time() < deadline:
                msg = await asyncio.wait_for(c.recv(), timeout=2)
                if msg.WhichOneof("body") == "location_added":
                    break
            else:
                pytest.fail("no location_added")
            acked = await c.resume(uid, 1)
            assert acked == 0
            await ms.wait_msg(lambda m: m.WhichOneof("body") == "resume", 2.0)
        finally:
            await c.close()
    finally:
        await ms.close()


@pytest.mark.asyncio
async def test_listener_subscribe_and_location() -> None:
    def on_msg(msg, conn: MockConn) -> None:
        if msg.WhichOneof("body") == "subscribe":

            async def _send() -> None:
                await asyncio.sleep(0.02)
                out = ServerMsg()
                la = LocationAdded(
                    device_uid=msg.subscribe.device_uid,
                    track_uid="t1",
                    client_seq=3,
                )
                la.point.CopyFrom(LatLng(latitude=1.5, longitude=2.5))
                out.location_added.CopyFrom(la)
                await conn.send(out)

            asyncio.create_task(_send())

    ms = await start_mock(True, on_msg)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                listener=tracking.ListenerAuth(access_token="jwt"),
                disable_reconnect=True,
            )
        )
        try:
            await c.subscribe("device-1")
            deadline = asyncio.get_event_loop().time() + 3
            while asyncio.get_event_loop().time() < deadline:
                msg = await asyncio.wait_for(c.recv(), timeout=3)
                kind = msg.WhichOneof("body")
                if kind == "location_added":
                    assert msg.location_added.point.latitude == 1.5
                    return
                if kind == "subscribed":
                    continue
            pytest.fail("no location")
        finally:
            await c.close()
    finally:
        await ms.close()


@pytest.mark.asyncio
async def test_reconnect_sends_resume() -> None:
    ms = await start_mock(True)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                device=tracking.DeviceAuth(client_id="c", client_secret="s"),
                reconnect_min_delay=0.02,
                reconnect_max_delay=0.05,
            )
        )
        try:
            uid = await c.start_track()
            await c.publish(LatLng(latitude=1, longitude=2))
            await asyncio.sleep(0.025)
            await c.publish(LatLng(latitude=3, longitude=4))
            assert c.client_seq == 2
            first = await ms.wait_conn(2.0)
            await first.close()
            resume = await ms.wait_msg(lambda m: m.WhichOneof("body") == "resume", 8.0)
            assert resume.resume.track_uid == uid
            assert resume.resume.last_client_seq == 2
            starts = 0
            for conn in ms.connections:
                for m in conn.messages:
                    if m.WhichOneof("body") == "track_start":
                        starts += 1
            assert starts == 1
            await wait_for(lambda: c.state == tracking.ConnectionState.OPEN, 5.0)
        finally:
            await c.close()
    finally:
        await ms.close()


@pytest.mark.asyncio
async def test_reconnect_track_not_found_clears_cursor() -> None:
    def on_msg(msg, conn: MockConn) -> None:
        kind = msg.WhichOneof("body")

        async def _reply() -> None:
            if kind == "track_start":
                out = ServerMsg()
                out.track_started.track_uid = "t-gone"
                await conn.send(out)
            elif kind == "resume":
                await conn.send(server_error(ErrorCode.ERROR_CODE_TRACK_NOT_FOUND, "track expired"))

        asyncio.create_task(_reply())

    ms = await start_mock(False, on_msg)
    try:
        c = await tracking.connect(
            tracking.Config(
                endpoint=ms.url,
                device=tracking.DeviceAuth(client_id="c", client_secret="s"),
                reconnect_min_delay=0.02,
                reconnect_max_delay=0.04,
            )
        )
        try:
            await c.start_track()
            assert c.track_uid == "t-gone"
            conn = await ms.wait_conn(2.0)
            await conn.close()
            await ms.wait_msg(lambda m: m.WhichOneof("body") == "resume", 8.0)
            await wait_for(lambda: c.track_uid == "", 5.0)
        finally:
            await c.close()
    finally:
        await ms.close()

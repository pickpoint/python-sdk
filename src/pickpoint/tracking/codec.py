from __future__ import annotations

import time

from .v2 import ClientMsg, LatLng, Resume, ServerMsg, messages


def stamp_lat_lng(p: LatLng | None) -> LatLng | None:
    if p is None:
        return None
    if not p.HasField("timestamp_ms"):
        p.timestamp_ms = int(time.time() * 1000)
    return p


def stamp_lat_lngs(points: list[LatLng] | None) -> list[LatLng]:
    if not points:
        return []
    for p in points:
        stamp_lat_lng(p)
    return points


def encode_client_msg(msg: ClientMsg) -> bytes:
    return msg.SerializeToString()


def decode_server_msg(data: bytes) -> ServerMsg:
    msg = ServerMsg()
    msg.ParseFromString(data)
    return msg


def client_resume(track_uid: str, last_client_seq: int) -> ClientMsg:
    msg = ClientMsg()
    msg.resume.CopyFrom(Resume(track_uid=track_uid, last_client_seq=last_client_seq))
    return msg


def clone_lat_lng(p: LatLng | None) -> LatLng | None:
    if p is None:
        return None
    out = LatLng()
    out.CopyFrom(p)
    return out

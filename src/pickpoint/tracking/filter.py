from __future__ import annotations

import math
import time

from .types import LatLng

HEARTBEAT_MS = 1000
MIN_MOVE_M = 2.0
HEADING_JUMP_DEG = 25.0
MOTION_EPS_MS = 0.5
EARTH_M = 6_371_000.0


class NoiseFilter:
    """Device GPS filter (filter.md). Heading/speed stay local."""

    def __init__(self) -> None:
        self.last_emitted: LatLng | None = None
        self.candidate: LatLng | None = None
        self.last_emit_at: int | None = None

    def seed(self, point: LatLng) -> None:
        t = point.timestamp_ms or 0
        self.last_emitted = clone(point)
        self.candidate = None
        self.last_emit_at = t

    def reset(self) -> None:
        self.last_emitted = None
        self.candidate = None
        self.last_emit_at = None

    def push(self, current: LatLng, now_ms: int | None = None) -> LatLng | None:
        now = current.timestamp_ms if current.timestamp_ms is not None else (now_ms or _now_ms())
        current = clone(current)
        current.timestamp_ms = now
        if self.last_emitted is None:
            return self._emit(current, now)
        if self._should_emit(current, now):
            return self._emit(current, now)
        self.candidate = current
        return None

    def _should_emit(self, current: LatLng, now: int) -> bool:
        last = self.last_emitted
        if last is None:
            return True
        if now - (self.last_emit_at or now) >= HEARTBEAT_MS:
            return True
        acc = current.accuracy or 0.0
        if haversine_m(last, current) >= max(MIN_MOVE_M, 2.0 * acc):
            return True
        if self.candidate is not None:
            speed = current.speed or 0.0
            eps = max(MIN_MOVE_M, acc, 0.5 * speed)
            if perpendicular_m(last, current, self.candidate) >= eps:
                return True
        if last.heading is not None and current.heading is not None:
            if heading_delta_deg(last.heading, current.heading) >= HEADING_JUMP_DEG:
                return True
        if last.speed is not None and current.speed is not None:
            if (last.speed > MOTION_EPS_MS) != (current.speed > MOTION_EPS_MS):
                return True
        return False

    def _emit(self, point: LatLng, now: int) -> LatLng:
        self.last_emitted = clone(point)
        self.candidate = None
        self.last_emit_at = now
        return point


def clone(p: LatLng) -> LatLng:
    return LatLng(
        latitude=p.latitude,
        longitude=p.longitude,
        altitude=p.altitude,
        accuracy=p.accuracy,
        heading=p.heading,
        speed=p.speed,
        timestamp_ms=p.timestamp_ms,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def haversine_m(a: LatLng, b: LatLng) -> float:
    dlat = math.radians(b.latitude - a.latitude)
    dlon = math.radians(b.longitude - a.longitude)
    la1 = math.radians(a.latitude)
    la2 = math.radians(b.latitude)
    h = math.sin(dlat / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin(dlon / 2) ** 2
    return 2.0 * EARTH_M * math.asin(min(1.0, math.sqrt(h)))


def perpendicular_m(a: LatLng, b: LatLng, p: LatLng) -> float:
    ax, ay = 0.0, 0.0
    bx, by = _project_m(a, b)
    px, py = _project_m(a, p)
    dx, dy = bx - ax, by - ay
    length2 = dx * dx + dy * dy
    if length2 < 1e-6:
        return haversine_m(a, p)
    t = ((px - ax) * dx + (py - ay) * dy) / length2
    qx, qy = ax + t * dx, ay + t * dy
    return math.hypot(px - qx, py - qy)


def _project_m(origin: LatLng, p: LatLng) -> tuple[float, float]:
    m_per_deg_lat = 111_320.0
    lat0 = math.radians(origin.latitude)
    x = (p.longitude - origin.longitude) * m_per_deg_lat * math.cos(lat0)
    y = (p.latitude - origin.latitude) * m_per_deg_lat
    return x, y


def heading_delta_deg(a: float, b: float) -> float:
    d = abs(b - a) % 360.0
    return 360.0 - d if d > 180.0 else d


def collapse_one_collinear(points: list[LatLng]) -> bool:
    if len(points) < 3:
        return False
    for i in range(1, len(points) - 1):
        acc = points[i].accuracy or 0.0
        eps = max(MIN_MOVE_M, acc)
        if perpendicular_m(points[i - 1], points[i + 1], points[i]) < eps:
            del points[i]
            return True
    return False

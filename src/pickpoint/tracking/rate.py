from __future__ import annotations

import time

MAX_PUBLISH_HZ = 50
MIN_PUBLISH_INTERVAL = 1.0 / MAX_PUBLISH_HZ
MIN_PUBLISH_INTERVAL_MS = int(1000 / MAX_PUBLISH_HZ)


def can_accept_publish(next_allowed_at: float, now: float | None = None, point_count: int = 1) -> bool:
    if point_count <= 0:
        return True
    if now is None:
        now = time.monotonic()
    return now >= next_allowed_at


def next_publish_allowed_at(
    next_allowed_at: float, now: float | None = None, point_count: int = 1
) -> float:
    if now is None:
        now = time.monotonic()
    start = max(next_allowed_at, now)
    return start + MIN_PUBLISH_INTERVAL * max(0, point_count)

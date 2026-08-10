from __future__ import annotations

import math
import random
from dataclasses import dataclass


@dataclass
class BackoffState:
    attempt: int = 0
    min_delay: float = 0.5
    max_delay: float = 30.0
    max_attempts: int = 0  # 0 = unlimited


def new_backoff(min_delay: float = 0.0, max_delay: float = 0.0, max_attempts: int = 0) -> BackoffState:
    return BackoffState(
        min_delay=min_delay if min_delay > 0 else 0.5,
        max_delay=max_delay if max_delay > 0 else 30.0,
        max_attempts=max_attempts,
    )


def next_delay(state: BackoffState, rnd: float | None = None) -> float | None:
    if state.max_attempts > 0 and state.attempt >= state.max_attempts:
        return None
    if rnd is None:
        rnd = random.random()
    min_ms = state.min_delay * 1000
    max_ms = state.max_delay * 1000
    exp = min_ms * (2**state.attempt)
    if exp > max_ms:
        exp = max_ms
    state.attempt += 1
    return math.floor(max(0.0, min(1.0, rnd)) * exp) / 1000.0


def reset_backoff(state: BackoffState) -> None:
    state.attempt = 0

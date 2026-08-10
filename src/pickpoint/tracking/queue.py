from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .v2 import LatLng


@dataclass
class QueuedPoint:
    seq: int
    point: LatLng


@dataclass
class OfflineQueue:
    max_size: int = 10_000
    items: list[QueuedPoint] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.max_size <= 0:
            self.max_size = 10_000

    def size(self) -> int:
        return len(self.items)

    def enqueue(self, seq: int, point: LatLng) -> int:
        self.items.append(QueuedPoint(seq=seq, point=point))
        if len(self.items) > self.max_size:
            dropped = len(self.items) - self.max_size
            self.items = self.items[dropped:]
            return dropped
        return 0

    def ack_through(self, ack: int) -> None:
        self.items = [p for p in self.items if p.seq > ack]

    def peek_all(self) -> list[QueuedPoint]:
        return list(self.items)

    def clear(self) -> None:
        self.items.clear()

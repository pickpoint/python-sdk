from __future__ import annotations

from dataclasses import dataclass, field

from .filter import collapse_one_collinear
from .types import MAX_BUFFER_POINTS, MAX_IN_FLIGHT_FRAMES, LatLng


@dataclass
class QueuedPoint:
    seq: int
    point: LatLng
    sent: bool = True


@dataclass
class OfflineQueue:
    """Staging (no seq) + InFlight (seq, waiting for Ack)."""

    max_size: int = MAX_BUFFER_POINTS
    staging: list[LatLng] = field(default_factory=list)
    inflight: list[QueuedPoint] = field(default_factory=list)
    sent_frame_seqs: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.max_size <= 0:
            self.max_size = MAX_BUFFER_POINTS

    def size(self) -> int:
        return len(self.staging) + len(self.inflight)

    def staging_len(self) -> int:
        return len(self.staging)

    def last_assigned_seq(self) -> int:
        return self.inflight[-1].seq if self.inflight else 0

    def push_staging(self, point: LatLng) -> None:
        self.staging.append(point)
        self._enforce_cap()

    def assign_from_staging(self, n: int, next_seq: int) -> list[QueuedPoint]:
        take = min(n, len(self.staging))
        out: list[QueuedPoint] = []
        for _ in range(take):
            point = self.staging.pop(0)
            next_seq += 1
            q = QueuedPoint(seq=next_seq, point=point, sent=False)
            self.inflight.append(q)
            out.append(q)
        return out

    def enqueue(self, seq: int, point: LatLng) -> int:
        """Add an already-numbered InFlight point (tests / live assign)."""
        self.inflight.append(QueuedPoint(seq=seq, point=point, sent=True))
        before = self.size()
        self._enforce_cap()
        return min(1, before - self.size()) if before > self.size() else 0

    def push_inflight_unsent(self, seq: int, point: LatLng) -> None:
        self.inflight.append(QueuedPoint(seq=seq, point=point, sent=False))
        self._enforce_cap()

    def mark_unsent(self) -> None:
        for p in self.inflight:
            p.sent = False
        self.sent_frame_seqs.clear()

    def window_full(self) -> bool:
        return len(self.sent_frame_seqs) >= MAX_IN_FLIGHT_FRAMES

    def window_remaining(self) -> int:
        return max(0, MAX_IN_FLIGHT_FRAMES - len(self.sent_frame_seqs))

    def record_frame(self, last_seq: int) -> None:
        self.sent_frame_seqs.append(last_seq)
        for p in self.inflight:
            if p.seq <= last_seq:
                p.sent = True

    def ack_through(self, ack: int) -> None:
        self.inflight = [p for p in self.inflight if p.seq > ack]
        self.sent_frame_seqs = [s for s in self.sent_frame_seqs if s > ack]

    def peek_all(self) -> list[QueuedPoint]:
        return list(self.inflight)

    def peek_staging(self) -> list[LatLng]:
        return list(self.staging)

    def unsent_inflight(self) -> list[QueuedPoint]:
        return [p for p in self.inflight if not p.sent]

    def clear(self) -> None:
        self.staging.clear()
        self.inflight.clear()
        self.sent_frame_seqs.clear()

    def _enforce_cap(self) -> None:
        while self.size() > self.max_size:
            if collapse_one_collinear(self.staging):
                continue
            if self.staging:
                self.staging.pop(0)
                continue
            if self.inflight:
                self.inflight.pop(0)
            else:
                break

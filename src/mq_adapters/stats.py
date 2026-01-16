"""Lightweight in-process stats for rabbit_mq_client.

This module provides an optional, dependency-free stats collector that tracks counts for:
- messages published / publish failures
- messages received
- handler exceptions
- ack/nack counts
- connection lifecycle and reconnect attempts

Design goals:
- No external infrastructure required.
- Thread-safe updates (works for sync threads + async tasks).
- Snapshot access for occasional inspection.

Stats collection is intentionally lightweight and can be safely ignored by users.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import threading
from typing import Any, Dict


@dataclass
class StatsSnapshot:
    """Snapshot of in-process counters."""

    published: int = 0
    publish_failed: int = 0
    received: int = 0
    handler_exceptions: int = 0
    acked: int = 0
    nacked: int = 0
    reconnect_attempts: int = 0
    connections_opened: int = 0
    connections_closed: int = 0

    def to_dict(self) -> Dict[str, int]:
        """Convert snapshot to a plain dict of ints."""
        return asdict(self)


class Stats:
    """Thread-safe counters with snapshot access."""

    __slots__ = ("_lock", "_snapshot")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshot = StatsSnapshot()

    def inc(self, field: str, value: int = 1) -> None:
        """Increment a named counter by value."""
        with self._lock:
            if not hasattr(self._snapshot, field):
                raise AttributeError(f"Unknown stats field: {field!r}")
            setattr(self._snapshot, field, int(getattr(self._snapshot, field)) + int(value))

    def snapshot(self) -> StatsSnapshot:
        """Return a copy of the current counters."""
        with self._lock:
            # return a copy
            return StatsSnapshot(**self._snapshot.to_dict())

    def reset(self) -> None:
        """Reset all counters back to zero."""
        with self._lock:
            self._snapshot = StatsSnapshot()


def ensure_stats(stats: Any | None) -> Stats:
    """Return an existing Stats or create a new one."""
    if isinstance(stats, Stats):
        return stats
    return Stats()

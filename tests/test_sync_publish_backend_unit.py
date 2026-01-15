import time
from typing import Any

from mq_adapters.sync_publisher_backend import SyncPublishBackend


class _FakeChannel:
    def __init__(self) -> None:
        self.published = 0
        self.is_open = True

    def basic_publish(self, exchange: str, routing_key: str, body: bytes):
        self.published += 1

    def close(self):
        self.is_open = False


class _FakeConnection:
    def __init__(self) -> None:
        self.is_open = True
        self._channels: list[_FakeChannel] = []

    def channel(self) -> _FakeChannel:
        ch = _FakeChannel()
        self._channels.append(ch)
        return ch

    def process_data_events(self, time_limit: float = 0):
        return

    def close(self):
        self.is_open = False


def test_backend_does_not_exit_on_heartbeat_timeout() -> None:
    """Regression test: heartbeat timeouts must NOT terminate the backend thread."""

    conn_holder: dict[str, Any] = {"conn": _FakeConnection()}

    def factory() -> _FakeConnection:
        return conn_holder["conn"]

    backend = SyncPublishBackend(
        connection_factory=factory,
        exchange="ex",
        default_routing_key="rk",
        channels=1,
        queue_maxsize=0,
        heartbeat_interval=0.01,
    )

    backend.start()

    # Wait long enough for several heartbeat cycles where queue is empty.
    time.sleep(0.1)

    # Backend should still accept publishes after being idle.
    for _ in range(10):
        backend.publish(b"x")

    # Give it time to drain.
    time.sleep(0.1)

    backend.stop(wait=True)

    assert backend.stats_snapshot().published == 10


def test_backend_stop_sentinel_stops_thread() -> None:
    """Stop should terminate backend without publishing further messages."""

    conn_holder: dict[str, Any] = {"conn": _FakeConnection()}

    def factory() -> _FakeConnection:
        return conn_holder["conn"]

    backend = SyncPublishBackend(
        connection_factory=factory,
        exchange="ex",
        default_routing_key="rk",
        channels=1,
        queue_maxsize=0,
        heartbeat_interval=0.01,
    )

    backend.start()
    backend.stop(wait=True)

    # Publishing after stop is allowed at API level, but the worker isn't running.
    backend.publish(b"x")
    time.sleep(0.05)

    assert backend.stats_snapshot().published == 0

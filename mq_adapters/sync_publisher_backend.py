"""Sync publisher backend (pika) with shared connection and channel pooling.

This module implements the missing piece of the "connection + channel pooling" recommendation for
sync (BlockingConnection) usage.

Why this exists
--------------
`pika.BlockingConnection` and its channels are *not* safe for concurrent use from multiple threads.
A safe and high-throughput pattern is:

- One dedicated I/O thread owns the connection (+ channels).
- Other threads enqueue publish requests.
- The I/O thread drains the queue and publishes.

This allows:
- One long-lived TCP connection per pool (instead of one per worker/thread)
- A bounded number of channels
- Backpressure via a bounded queue

The public surface is intentionally tiny so `PublisherPool` can wrap it.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from mq_adapters.log import log_sending
from mq_adapters.retry_policy import RetryPolicy
from mq_adapters.stats import Stats, ensure_stats


@dataclass(frozen=True)
class PublishRequest:
    """A queued publish request consumed by the backend I/O thread."""
    body: bytes
    exchange: str
    routing_key: str


class SyncPublishBackend:
    """Owns one BlockingConnection and a pool of channels in a dedicated thread."""

    def __init__(
            self,
            *,
            connection_factory: Callable[[], Any],
            exchange: str,
            default_routing_key: str,
            channels: int = 4,
            confirm_delivery: bool = False,
            queue_maxsize: int = 0,
            heartbeat_interval: float = 1.0,
            logger: Optional[Any] = None,
            reconnect_retry_policy: Optional[RetryPolicy] = None,
            stats: Optional[Stats] = None,
    ):
        self._connection_factory = connection_factory
        self._exchange = exchange
        self._default_routing_key = default_routing_key
        self._channels = max(1, int(channels))
        self._confirm_delivery = bool(confirm_delivery)
        self._queue: "queue.Queue[PublishRequest | None]" = queue.Queue(
            maxsize=max(0, int(queue_maxsize))
        )
        self._heartbeat_interval = max(0.0, float(heartbeat_interval))
        self._logger = logger
        self._stats = ensure_stats(stats)

        self._reconnect_retry_policy = reconnect_retry_policy or RetryPolicy(
            max_attempts=None,
            base_delay=0.5,
            max_delay=10.0,
            multiplier=2.0,
            jitter=0.25,
        )

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # stats / testing hooks
        self._connections_created = 0
        self._channels_created = 0
        self._reconnect_failures = 0

    @property
    def connections_created(self) -> int:
        """Number of backend connections created (testing/diagnostics)."""
        return self._connections_created

    @property
    def channels_created(self) -> int:
        """Number of backend channels created (testing/diagnostics)."""
        return self._channels_created

    def start(self) -> None:
        """Start the backend I/O thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="sync_publish_backend", daemon=True)
        self._thread.start()

    def stop(self, *, wait: bool = True) -> None:
        """Stop the backend thread and close the connection best-effort."""
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        # Always push sentinel to wake the thread.
        try:
            self._queue.put_nowait(None)
        except Exception:
            # if full, block briefly
            try:
                self._queue.put(None, timeout=1)
            except Exception:
                pass
        if wait and self._thread:
            self._thread.join(timeout=10)

    def publish(self, body: bytes, routing_key: Optional[str] = None) -> None:
        """Enqueue a message for publishing (non-blocking for the caller)."""
        rk = routing_key or self._default_routing_key
        log_sending(self._logger, body, {"exchange_name": self._exchange, "routing_key": rk})
        self._queue.put(PublishRequest(body=body, exchange=self._exchange, routing_key=rk))

    def stats_snapshot(self):
        """Return a snapshot of in-process counters."""
        return self._stats.snapshot()

    def _run(self) -> None:
        conn: Optional[Any] = None
        channels: list[Any] = []
        ch_index = 0

        # Use a monotonic clock for scheduling.
        next_heartbeat = time.monotonic()

        def _log_exc(msg: str) -> None:
            if self._logger is not None:
                try:
                    self._logger.exception(msg)
                except Exception:
                    pass

        def _ensure_connection_and_channels() -> None:
            nonlocal conn, channels, ch_index

            if conn is None or not getattr(conn, "is_open", False):
                conn = self._connection_factory()
                self._connections_created += 1
                self._stats.inc("connections_opened")
                if self._logger is not None:
                    self._logger.info("Connection opened")
                channels = []
                ch_index = 0

            # lazily create channels up to limit
            while len(channels) < self._channels:
                ch = conn.channel()
                self._channels_created += 1
                if self._confirm_delivery:
                    try:
                        ch.confirm_delivery()
                    except Exception:
                        self._stats.inc("publish_failed")
                        if self._logger is not None:
                            self._logger.error("Publish confirm setup failed")
                        _log_exc("Failed to enable confirms on backend channel")
                channels.append(ch)

        def _pump_io_if_due() -> None:
            nonlocal next_heartbeat
            if self._heartbeat_interval <= 0:
                return
            now = time.monotonic()
            if now < next_heartbeat:
                return
            next_heartbeat = now + self._heartbeat_interval
            try:
                if conn is not None and getattr(conn, "is_open", False):
                    # time_limit=0 => non-blocking; only service heartbeats/confirm frames.
                    conn.process_data_events(time_limit=0)
            except Exception:
                _log_exc("Exception while processing data events")

        while True:
            # If stop requested, break after draining optional sentinel wakeups.
            if self._stop_event.is_set():
                break

            try:
                _ensure_connection_and_channels()

                # Reset reconnect backoff after a successful connect.
                self._reconnect_failures = 0

                # Determine how long we can block waiting for a publish request.
                if self._heartbeat_interval > 0:
                    wait_seconds = max(0.0, next_heartbeat - time.monotonic())
                else:
                    wait_seconds = None

                try:
                    if wait_seconds is None:
                        item = self._queue.get()  # block until message or sentinel
                    else:
                        item = self._queue.get(timeout=wait_seconds)
                except queue.Empty:
                    item = "__heartbeat__"  # distinguish from stop sentinel

                # Pump I/O either after getting an item or after a heartbeat timeout.
                _pump_io_if_due()

                # Heartbeat tick
                if item == "__heartbeat__":
                    continue

                # Stop sentinel
                if item is None:
                    break

                # Publish request
                ch = channels[ch_index % len(channels)]
                ch_index += 1
                try:
                    ch.basic_publish(item.exchange, routing_key=item.routing_key, body=item.body)
                    self._stats.inc("published")
                except Exception:
                    self._stats.inc("publish_failed")
                    if self._logger is not None:
                        self._logger.warning("Publish failed")
                    raise
                finally:
                    # queue accounting is important when callers use bounded queues
                    try:
                        self._queue.task_done()
                    except Exception:
                        pass

            except Exception:
                _log_exc("SyncPublishBackend encountered an exception; reconnecting")
                self._reconnect_failures += 1
                self._stats.inc("reconnect_attempts")
                delay = self._reconnect_retry_policy.delay_for_attempt(self._reconnect_failures + 1)
                if self._logger is not None:
                    self._logger.info("Reconnect attempt %d; backing off %.2fs", self._reconnect_failures, delay)
                time.sleep(delay)
                # Force reconnect on next iteration
                try:
                    if conn is not None and getattr(conn, "is_open", False):
                        conn.close()
                        self._stats.inc("connections_closed")
                        if self._logger is not None:
                            self._logger.info("Connection closed")
                except Exception:
                    pass
                conn = None
                channels = []
                # Re-align heartbeat schedule after reconnect.
                next_heartbeat = time.monotonic() + self._heartbeat_interval

        # best-effort cleanup
        try:
            for ch in channels:
                try:
                    if getattr(ch, "is_open", False):
                        ch.close()
                except Exception:
                    pass
            if conn is not None and getattr(conn, "is_open", False):
                try:
                    conn.close()
                    self._stats.inc("connections_closed")
                    if self._logger is not None:
                        self._logger.info("Connection closed")
                except Exception:
                    pass
        except Exception:
            pass

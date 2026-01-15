"""Unit tests ensuring sync Listener pumps pika events via process_data_events.

These tests use fake pika-like objects to verify that Listener explicitly calls
`BlockingConnection.process_data_events(...)` while running.
"""

from __future__ import annotations

import time

from mq_adapters.sync_adapter import Listener


class _FakeMethod:
    """Minimal object emulating pika method with delivery_tag."""

    delivery_tag = 1


class _FakeQueueDeclareOk:
    """Return type for channel.queue_declare used by Listener."""

    class method:  # noqa: D401 - mimic pika shape
        queue = "q"


class _FakeChannel:
    """Fake channel that records consumer registration."""

    is_open = True

    def __init__(self) -> None:
        self.basic_consume_called = False

    def basic_qos(self, prefetch_size: int, prefetch_count: int):
        return None

    def queue_declare(self, queue: str, exclusive: bool, auto_delete: bool):
        return _FakeQueueDeclareOk()

    def exchange_declare(self, exchange: str, exchange_type: str, durable: bool = True):
        return None

    def queue_bind(self, queue: str, exchange: str, routing_key: str):
        return None

    def basic_consume(self, queue: str, on_message_callback, auto_ack: bool):
        self.basic_consume_called = True
        return None

    def close(self):
        self.is_open = False


class _FakeConnection:
    """Fake BlockingConnection that counts process_data_events calls."""

    is_open = True

    def __init__(self) -> None:
        self.channel_obj = _FakeChannel()
        self.process_calls = 0

    def channel(self):
        return self.channel_obj

    def process_data_events(self, time_limit: float = 0):
        self.process_calls += 1
        # Simulate blocking a tiny bit
        time.sleep(min(0.01, max(0.0, float(time_limit))))

    def add_callback_threadsafe(self, cb):
        # In tests we can just invoke immediately.
        cb()

    def close(self):
        self.is_open = False


def test_listener_pumps_process_data_events_while_idle(message_type_map):
    conn = _FakeConnection()

    def factory():
        return conn

    def cb(_ch, _method, _props, _body):
        return None

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        auto_ack=True,
        io_pump_time_limit=0.01,
    )

    listener.start_listening()
    try:
        deadline = time.time() + 1.0
        while time.time() < deadline and conn.process_calls < 3:
            time.sleep(0.02)
        assert conn.process_calls >= 3
        assert conn.channel_obj.basic_consume_called is True
    finally:
        listener.stop_listening()


def test_listener_stop_exits_cleanly(message_type_map):
    conn = _FakeConnection()

    def factory():
        return conn

    def cb(_ch, _method, _props, _body: bytes):
        return None

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        io_pump_time_limit=0.01,
    )

    listener.start_listening()
    time.sleep(0.05)
    listener.stop_listening()

    # stop_listening sets stop_me and should allow the thread to exit.
    listener.join(timeout=2)
    assert not listener.is_alive()

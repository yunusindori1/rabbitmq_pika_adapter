"""Unit tests for serialization hooks (serializer=...).

These tests ensure users can override the default dict->JSON bytes path without extra dependencies.
"""

from __future__ import annotations

import time

import pytest

from mq_adapters.publisher_pool import PublisherPool


def test_sync_publisher_pool_custom_serializer_is_used(message_type_map):
    """PublisherPool should use the injected serializer instead of default JSON serialization."""
    calls = {"body": None}

    class _Ch:
        is_open = True

        def basic_publish(self, exchange, routing_key, body):
            """Record published body."""
            calls["body"] = body

        def close(self):
            """No-op close."""
            return None

    class _Conn:
        is_open = True

        def channel(self):
            """Return a new fake channel."""
            return _Ch()

        def process_data_events(self, time_limit=0):
            """No-op heartbeat processing."""
            return None

        def close(self):
            """No-op close."""
            return None

    def factory():
        """Return a fake connection."""
        return _Conn()

    def serializer(_msg):
        """Serialize every message to a constant payload."""
        return b"SER"

    pool = PublisherPool(
        message_type="mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        num_workers=1,
        serializer=serializer,
    )

    pool.start()
    try:
        pool.send({"a": 1})
        deadline = time.time() + 2
        while time.time() < deadline and calls["body"] is None:
            time.sleep(0.01)
        assert calls["body"] == b"SER"
    finally:
        pool.stop()


@pytest.mark.asyncio
async def test_async_sender_custom_serializer_is_used(message_type_map, monkeypatch):
    """AsyncSender should use the injected serializer instead of default JSON serialization."""
    from mq_adapters import async_adapter

    # Reuse the fake aio-pika objects from the existing async sender tests
    from tests.test_async_sender import FakeAioPika, FakeConnection

    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        """Return a fake async connection."""
        return conn

    def serializer(_msg):
        """Serialize every message to a constant payload."""
        return b"SER"

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        serializer=serializer,
    )

    await sender.send({"a": 1})
    assert conn.channel_obj.exchange.publishes
    assert conn.channel_obj.exchange.publishes[0].body == b"SER"

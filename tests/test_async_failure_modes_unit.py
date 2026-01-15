from __future__ import annotations

import pytest

from mq_adapters import async_adapter


class FakeMessage:
    def __init__(self, body: bytes):
        self.body = body


class FakeAioPika:
    Message = FakeMessage


class FlakyExchange:
    def __init__(self) -> None:
        self.calls = 0
        self.published: list[tuple[bytes, str]] = []

    async def publish(self, message: FakeMessage, routing_key: str) -> None:
        self.calls += 1
        # Fail the first publish to simulate a transient disconnect.
        if self.calls == 1:
            raise ConnectionError("simulated disconnect")
        self.published.append((message.body, routing_key))


class FakeChannel:
    def __init__(self, exchange: FlakyExchange) -> None:
        self.is_closed = False
        self._exchange = exchange

    async def declare_exchange(self, *, name, type, durable=True, auto_delete=False):
        return self._exchange

    async def close(self) -> None:
        self.is_closed = True


class FakeConnection:
    def __init__(self, exchange: FlakyExchange) -> None:
        self.is_closed = False
        self._channel = FakeChannel(exchange)

    async def channel(self, publisher_confirms: bool = False):
        return self._channel

    async def close(self) -> None:
        self.is_closed = True


@pytest.mark.asyncio
async def test_async_sender_reconnects_after_publish_failure(message_type_map, monkeypatch):
    """Failure-mode unit test: publish fails once, then succeeds on retry.

    This verifies the send() path resets cached connection state and retries via RetryPolicy.
    """

    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())

    exchange = FlakyExchange()

    async def factory():
        return FakeConnection(exchange)

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        retry_policy=async_adapter.RetryPolicy(max_attempts=2, base_delay=0.0, max_delay=0.0, jitter=0.0),
    )

    await sender.send({"x": 1})

    # One failure + one success.
    assert exchange.calls == 2
    assert len(exchange.published) == 1

    await sender.stop()

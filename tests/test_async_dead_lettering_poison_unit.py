from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

import pytest

from mq_adapters import async_adapter


class FakeMessage:
    def __init__(self, body: bytes, headers: Optional[Dict[str, Any]] = None, **kwargs):
        self.body = body
        self.headers = headers
        self.kwargs = kwargs


class FakeAioPika:
    Message = FakeMessage


@dataclass
class FakeIncomingMessage:
    body: bytes
    headers: Optional[Dict[str, Any]] = None
    properties: Any = None
    acked: bool = False
    nacked: bool = False
    requeue: Optional[bool] = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.nacked = True
        self.requeue = requeue


class FakeDefaultExchange:
    def __init__(self) -> None:
        self.publishes: list[tuple[FakeMessage, str]] = []

    async def publish(self, msg: FakeMessage, routing_key: str) -> None:
        self.publishes.append((msg, routing_key))


class FakeQueue:
    def __init__(self) -> None:
        self.consume_cb: Optional[Callable[[FakeIncomingMessage], Awaitable[None]]] = None

    async def bind(self, exchange: Any, routing_key: str = "") -> None:
        return

    async def consume(self, callback, no_ack: bool = True) -> str:
        self.consume_cb = callback
        return "tag"

    async def cancel(self, consumer_tag: str) -> None:
        return


class FakeExchange:
    pass


class FakeChannel:
    def __init__(self) -> None:
        self.is_closed = False
        self.default_exchange = FakeDefaultExchange()
        self.queue = FakeQueue()

    async def set_qos(self, prefetch_count: int) -> None:
        return

    async def declare_exchange(self, name: str, type: str, durable: bool = True, auto_delete: bool = False):
        return FakeExchange()

    async def declare_queue(
        self,
        name: str,
        durable: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        exclusive: bool = False,
        auto_delete: bool = False,
    ):
        return self.queue

    async def close(self) -> None:
        self.is_closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.channel_obj = FakeChannel()

    async def channel(self, publisher_confirms: bool = False):
        return self.channel_obj

    async def close(self) -> None:
        self.is_closed = True


@pytest.mark.asyncio
async def test_async_listener_poison_publishes_to_dlq_and_acks(monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())

    conn = FakeConnection()

    async def factory():
        return conn

    # Poison config: DLQ present, and max_retries=1 => attempt>=1 is poison.
    message_type_map = {
        "mt": {
            "exchange_name": "ex",
            "exchange_type": "topic",
            "routing_key": "rk",
            "predefined_queue_name": "q",
            "queue_arguments": {"on_error": "dead_letter", "dead_letter_queue": "dlq", "max_retries": 1},
        }
    }

    def handler(_ch, _method, _properties, _body: bytes):
        raise RuntimeError("boom")

    listener = async_adapter.AsyncListener(
        "mt",
        callback=handler,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        auto_ack=False,
    )

    await listener.start()

    msg = FakeIncomingMessage(body=b"x", headers=None)
    assert conn.channel_obj.queue.consume_cb is not None
    await conn.channel_obj.queue.consume_cb(msg)
    await asyncio.sleep(0)

    # Poison => published to DLQ, then acked.
    assert msg.acked is True
    assert msg.nacked is False

    publishes = conn.channel_obj.default_exchange.publishes
    assert len(publishes) == 1
    out_msg, rk = publishes[0]
    assert rk == "dlq"
    assert out_msg.body == b"x"

    await listener.stop()


@pytest.mark.asyncio
async def test_async_listener_non_poison_dead_letter_nacks_without_requeue(monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())

    conn = FakeConnection()

    async def factory():
        return conn

    # Not poison: max_retries=3, attempt=1.
    message_type_map = {
        "mt": {
            "exchange_name": "ex",
            "exchange_type": "topic",
            "routing_key": "rk",
            "predefined_queue_name": "q",
            "queue_arguments": {"on_error": "dead_letter", "dead_letter_queue": "dlq", "max_retries": 3},
        }
    }

    def handler(_ch, _method, _properties, _body: bytes):
        raise RuntimeError("boom")

    listener = async_adapter.AsyncListener(
        "mt",
        callback=handler,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        auto_ack=False,
    )

    await listener.start()

    msg = FakeIncomingMessage(body=b"x", headers=None)
    assert conn.channel_obj.queue.consume_cb is not None
    await conn.channel_obj.queue.consume_cb(msg)
    await asyncio.sleep(0)

    # Non-poison + on_error=dead_letter => nack(requeue=False)
    assert msg.acked is False
    assert msg.nacked is True
    assert msg.requeue is False

    # No DLQ publish.
    assert conn.channel_obj.default_exchange.publishes == []

    await listener.stop()

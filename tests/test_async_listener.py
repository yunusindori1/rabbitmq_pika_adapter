from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

import pytest

from mq_adapters import async_adapter


class FakeAioPika:
    class Message:  # pragma: no cover
        def __init__(self, body: bytes):
            self.body = body


@dataclass
class FakeIncomingMessage:
    body: bytes
    properties: Any = None
    acked: bool = False
    nacked: bool = False
    requeue: Optional[bool] = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = True) -> None:
        self.nacked = True
        self.requeue = requeue


class FakeQueue:
    def __init__(self) -> None:
        self.bound = []
        self.consume_cb: Optional[Callable[[FakeIncomingMessage], Awaitable[None]]] = None
        self.no_ack: Optional[bool] = None
        self.cancelled: bool = False

    async def bind(self, exchange: Any, routing_key: str = "") -> None:
        self.bound.append((exchange, routing_key))

    async def consume(self, callback, no_ack: bool = True) -> str:
        self.consume_cb = callback
        self.no_ack = no_ack
        return "tag"

    async def cancel(self, consumer_tag: str) -> None:
        self.cancelled = True


class FakeExchange:
    pass


class FakeChannel:
    def __init__(self) -> None:
        self.is_closed = False
        self.qos: Optional[int] = None
        self.exchange_declared = []
        self.queue_declared = []
        self.queue = FakeQueue()

    async def set_qos(self, prefetch_count: int) -> None:
        self.qos = prefetch_count

    async def declare_exchange(self, name: str, type: str, durable: bool = True, auto_delete: bool = False):
        self.exchange_declared.append((name, type, durable, auto_delete))
        return FakeExchange()

    async def declare_queue(
            self, name: str, durable: bool = False, arguments: Optional[Dict[str, Any]] = None,
            exclusive: bool = False, auto_delete: bool = False):
        self.queue_declared.append((name, durable, arguments, exclusive, auto_delete))
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
async def test_async_listener_prefetch_and_ack(message_type_map, monkeypatch):
    # Use monkeypatch so the fake module doesn't leak into other tests (e.g., integration tests).
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    acked = {"n": 0}

    async def on_msg(ch, method, properties, body: bytes):
        acked["n"] += 1

    listener = async_adapter.AsyncListener(
        "mt",
        callback=on_msg,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        prefetch_count=10,
        auto_ack=False,
    )

    await listener.start()

    # qos was applied
    assert conn.channel_obj.qos == 10

    # queue declare included queue arguments from message_type_map
    name, durable, arguments, *_ = conn.channel_obj.queue_declared[0]
    assert name == "q"
    assert durable is True
    assert arguments == {"x-message-ttl": 20000}

    # simulate message delivery
    msg = FakeIncomingMessage(body=b"hello")
    assert conn.channel_obj.queue.consume_cb is not None
    await conn.channel_obj.queue.consume_cb(msg)

    # handler runs in background task; yield to loop
    await asyncio.sleep(0)

    assert acked["n"] == 1
    assert msg.acked is True

    await listener.stop()
    assert conn.is_closed is True
    assert conn.channel_obj.is_closed is True
    assert conn.channel_obj.queue.cancelled is True


@pytest.mark.asyncio
async def test_async_listener_nack_on_exception(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    def on_msg(ch, method, properties, body: bytes):
        raise RuntimeError("boom")

    listener = async_adapter.AsyncListener(
        "mt",
        callback=on_msg,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        auto_ack=False,
    )

    await listener.start()

    msg = FakeIncomingMessage(body=b"hello")
    await conn.channel_obj.queue.consume_cb(msg)
    await asyncio.sleep(0)

    assert msg.nacked is True
    assert msg.requeue is True

    await listener.stop()


@pytest.mark.asyncio
async def test_async_listener_respects_max_concurrency_defaulting_to_prefetch(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    # With prefetch_count=2 and max_concurrency=None, should cap at 2.
    started = 0
    max_seen = 0
    gate = asyncio.Event()
    lock = asyncio.Lock()

    async def on_msg(ch, method, properties, body: bytes):
        nonlocal started, max_seen
        async with lock:
            started += 1
            max_seen = max(max_seen, started)
        await gate.wait()
        async with lock:
            started -= 1

    listener = async_adapter.AsyncListener(
        "mt",
        callback=on_msg,
        message_type_map=message_type_map,
        connection_factory=factory,
        predefined_queue=True,
        prefetch_count=2,
        auto_ack=False,
    )

    await listener.start()

    # Fire 5 messages quickly. Only 2 should enter concurrently.
    for _ in range(5):
        msg = FakeIncomingMessage(body=b"x")
        await conn.channel_obj.queue.consume_cb(msg)

    # Let tasks schedule
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert max_seen == 2

    gate.set()
    await listener.stop()

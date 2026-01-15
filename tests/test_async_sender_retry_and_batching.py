from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from mq_adapters import async_adapter


@dataclass
class FakeMessage:
    body: bytes


class FakeAioPika:
    Message = FakeMessage


@dataclass
class PublishCall:
    body: bytes
    routing_key: str


class FakeExchange:
    def __init__(self) -> None:
        self.publishes: List[PublishCall] = []

    async def publish(self, message: FakeMessage, routing_key: str) -> None:
        self.publishes.append(PublishCall(body=message.body, routing_key=routing_key))


class FakeChannel:
    def __init__(self) -> None:
        self.is_closed = False
        self.exchange = FakeExchange()
        self.declared: List[Tuple[str, str, bool]] = []

    async def declare_exchange(self, name: str, type: str, durable: bool = True, auto_delete: bool = False):
        self.declared.append((name, type, durable))
        return self.exchange

    async def close(self) -> None:
        self.is_closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.is_closed = False
        self.channel_calls: List[Optional[bool]] = []
        self.channel_obj = FakeChannel()

    async def channel(self, publisher_confirms: bool = False):
        self.channel_calls.append(publisher_confirms)
        return self.channel_obj

    async def close(self) -> None:
        self.is_closed = True


@pytest.mark.asyncio
async def test_async_sender_retries_on_connection_factory_failure(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())

    calls = {"n": 0}
    conn = FakeConnection()

    async def factory():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("temp")
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        retry_policy=async_adapter.RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=0.0),
    )

    await sender.start()
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_async_sender_confirm_batching_flushes(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        confirm_delivery=True,
        confirm_batch_size=3,
        confirm_flush_interval=0.0,
    )

    # three sends should trigger a flush
    await sender.send(b"1")
    await sender.send(b"2")
    await sender.send(b"3")

    # allow loop to process any pending tasks
    await asyncio.sleep(0)

    assert len(conn.channel_obj.exchange.publishes) == 3

    await sender.stop()

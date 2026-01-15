from __future__ import annotations

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
    def __init__(self, name=None) -> None:
        self.name = name
        self.publishes: List[PublishCall] = []

    async def publish(self, message: FakeMessage, routing_key: str) -> None:
        self.publishes.append(PublishCall(body=message.body, routing_key=routing_key))


class FakeQueue:
    def __init__(self, name=None) -> None:
        self.name = name


class FakeChannel:
    def __init__(self) -> None:
        self.is_closed = False
        self.declared: List[Tuple[str, str, bool]] = []
        self.exchange = FakeExchange()

    async def declare_exchange(
        self, *, name, type, durable=True, auto_delete=False
    ):
        self.declared.append((name, type, durable))
        # Return the channel's exchange so tests can inspect publishes.
        self.exchange.name = name
        return self.exchange

    async def declare_queue(
        self,
        name,
        durable=True,
        arguments=None,
        exclusive=False,
        auto_delete=False,
    ):
        return FakeQueue(name)

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
async def test_async_sender_send_dict_serializes_json_bytes(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
    )

    await sender.send({"a": 1})
    assert conn.channel_obj.exchange.publishes
    body = conn.channel_obj.exchange.publishes[0].body
    # JSON order doesn't matter
    assert body.startswith(b"{")
    assert b"\"a\"" in body


@pytest.mark.asyncio
async def test_async_sender_routing_key_override(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
    )

    await sender.send(b"x", routing_key="custom")
    assert conn.channel_obj.exchange.publishes[0].routing_key == "custom"


@pytest.mark.asyncio
async def test_async_sender_confirm_delivery_enables_confirms(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
        confirm_delivery=True,
    )
    await sender.start()
    assert conn.channel_calls == [True]


@pytest.mark.asyncio
async def test_async_sender_start_idempotent(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())

    calls = {"n": 0}
    conn = FakeConnection()

    async def factory():
        calls["n"] += 1
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
    )

    await sender.start()
    await sender.start()
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_async_sender_stop_closes_channel_and_connection(message_type_map, monkeypatch):
    monkeypatch.setattr(async_adapter, "aio_pika", FakeAioPika())
    conn = FakeConnection()

    async def factory():
        return conn

    sender = async_adapter.AsyncSender(
        "mt",
        message_type_map=message_type_map,
        connection_factory=factory,
    )

    await sender.start()
    await sender.stop()

    assert conn.is_closed is True
    assert conn.channel_obj.is_closed is True

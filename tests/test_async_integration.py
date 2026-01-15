from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from mq_adapters.async_adapter import AsyncListener, AsyncSender


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_sender_listener_roundtrip(require_rabbitmq):
    """Opt-in roundtrip test against a live RabbitMQ (no docker)."""

    # Unique run names to avoid conflicts
    suffix = uuid.uuid4().hex[:12]
    exchange_name = f"rmq_client_it_ex_{suffix}"
    queue_name = f"rmq_client_it_q_{suffix}"
    routing_key = f"rmq_client.it.{suffix}"

    message_type = "it"
    message_type_map = {
        message_type: {
            "exchange_name": exchange_name,
            "exchange_type": "topic",
            "routing_key": routing_key,
            "predefined_queue_name": queue_name,
            # Keep queue args empty to avoid mismatches; broker will create it with these args.
            "queue_arguments": {},
            # NOTE: Integration tests should avoid leaving resources behind.
            # These flags are used by AsyncSender/AsyncListener at declare-time so the broker cleans them up.
            # (Documentation-only comment; changing this comment requires no re-run.)
            "exchange_durable": False,
            "exchange_auto_delete": True,
            "queue_durable": False,
            "queue_auto_delete": True,
        }
    }

    connection_params = {
        **require_rabbitmq,
        "message_types": message_type_map,
    }

    received = []
    received_event = asyncio.Event()
    total = 20

    async def on_msg(ch, method, properties, body: bytes):
        received.append(body)
        if len(received) >= total:
            received_event.set()

    listener = AsyncListener(
        message_type,
        callback=on_msg,
        connection_params=connection_params,
        predefined_queue=True,
        prefetch_count=50,
        auto_ack=False,
    )

    sender = AsyncSender(
        message_type,
        connection_params=connection_params,
        confirm_delivery=False,
    )

    await listener.start()
    await sender.start()

    try:
        for i in range(total):
            await sender.send({"i": i, "ts": time.time()})

        await asyncio.wait_for(received_event.wait(), timeout=10)
        assert len(received) == total
    finally:
        await listener.stop()
        await sender.stop()

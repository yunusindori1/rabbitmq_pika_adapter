from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from mq_adapters.async_adapter import AsyncListener, AsyncSender


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_sender_recovers_after_forced_channel_close(require_rabbitmq):
    """Failure-mode integration test.

    We force-close the sender connection mid-run, then verify a subsequent send still succeeds.
    This avoids OS-level network toggling and is reliable on Windows.

    Note: we intentionally do *not* mutate AsyncSender private internals; we only close the
    underlying connection to trigger its normal `start()` path on the next send.
    """

    suffix = uuid.uuid4().hex[:12]
    exchange_name = f"rmq_client_it_ex_reconn_{suffix}"
    queue_name = f"rmq_client_it_q_reconn_{suffix}"
    routing_key = f"rmq_client.it.reconn.{suffix}"

    message_type = "it_reconn"
    message_type_map = {
        message_type: {
            "exchange_name": exchange_name,
            "exchange_type": "topic",
            "routing_key": routing_key,
            "predefined_queue_name": queue_name,
            "queue_arguments": {},
            "exchange_durable": False,
            "exchange_auto_delete": True,
            "queue_durable": False,
            "queue_auto_delete": True,
        }
    }

    connection_params = {**require_rabbitmq, "message_types": message_type_map}

    received = 0
    got_all = asyncio.Event()

    async def on_msg(_ch, _method, _properties, _body: bytes):
        nonlocal received
        received += 1
        if received >= 2:
            got_all.set()

    listener = AsyncListener(
        message_type,
        callback=on_msg,
        connection_params=connection_params,
        predefined_queue=True,
        auto_ack=False,
        prefetch_count=10,
    )

    sender = AsyncSender(message_type, connection_params=connection_params)

    await listener.start()
    await sender.start()

    try:
        await sender.send({"phase": 1, "ts": time.time()})

        # Force-close the connection. Next send() should detect a closed connection and reconnect.
        assert sender._conn is not None
        await sender._conn.close()

        await sender.send({"phase": 2, "ts": time.time()})

        await asyncio.wait_for(got_all.wait(), timeout=10)
        assert received == 2
    finally:
        await listener.stop()
        await sender.stop()

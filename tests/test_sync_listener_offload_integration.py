"""Opt-in integration test for sync Listener offload mode.

Requires a real RabbitMQ broker via env vars (no Docker required).
"""

from __future__ import annotations

import os
import time

import pytest

from mq_adapters.sync_adapter import Listener
from mq_adapters.publisher_pool import PublisherPool


def _env(name: str) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


@pytest.mark.integration
def test_sync_listener_offload_processes_and_acks():
    host = _env("RABBITMQ_HOST") or _env("RABBITMQ_SERVER")
    port = _env("RABBITMQ_PORT")
    vhost = _env("RABBITMQ_VHOST")
    user = _env("RABBITMQ_USER")
    pw = _env("RABBITMQ_PASSWORD")

    if not (host and port and vhost and user and pw):
        pytest.skip(
            "RabbitMQ integration env vars not set. Set RABBITMQ_HOST/RABBITMQ_PORT/RABBITMQ_VHOST/"
            "RABBITMQ_USER/RABBITMQ_PASSWORD to run integration tests."
        )

    mtm = {
        "mt": {
            "exchange_name": "amq.topic",
            "exchange_type": "topic",
            "routing_key": "rk.test.offload",
        }
    }

    params = {
        "server": host,
        "port": int(port),
        "vhost": vhost,
        "username": user,
        "password": pw,
        "message_types": mtm,
    }

    seen = []

    def cb(ch, method, props, body: bytes):
        seen.append(body)

    listener = Listener(
        "mt",
        cb,
        connection_params=params,
        message_type_map=mtm,
        predefined_queue=False,
        auto_ack=False,
        offload=True,
        max_workers=2,
        max_in_flight=2,
        prefetch_size=0,
        prefetch_count=2,
    )

    pool = PublisherPool(
        message_type="mt",
        connection_params=params,
        message_type_map=mtm,
        num_workers=1,
        queue_maxsize=100,
    )

    pool.start()
    listener.start_listening()
    try:
        time.sleep(0.25)

        pool.send(b"hello", routing_key="rk.test.offload")
        deadline = time.time() + 10
        while time.time() < deadline and not seen:
            time.sleep(0.05)
        assert seen
    finally:
        try:
            listener.stop_listening()
        except Exception:
            pass
        try:
            pool.stop()
        except Exception:
            pass

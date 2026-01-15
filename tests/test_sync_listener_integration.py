from __future__ import annotations

import os
import time

import pytest

from mq_adapters.sync_adapter import Listener


def _env(name: str) -> str | None:
    return os.environ.get(name)


@pytest.mark.integration
def test_sync_listener_stop_closes_connection():
    # Opt-in integration test: requires real RabbitMQ reachable.
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

    # Use a temporary exclusive queue by default (predefined_queue=False)
    mtm = {
        "mt": {
            "exchange_name": "amq.topic",
            "exchange_type": "topic",
            "routing_key": "#",
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

    def cb(ch, method, props, body: bytes):
        return None

    listener = Listener("mt", cb, connection_params=params, message_type_map=mtm, predefined_queue=False)
    listener.start_listening()

    # give it a moment to connect and begin consuming
    time.sleep(0.5)

    # stop should be best-effort and should close connection/channel
    listener.stop_listening()

    # give the I/O thread time to execute the callback
    time.sleep(0.5)

    # access private state for integration-level assertion
    conn = getattr(listener, "_Listener__connection", None)
    ch = getattr(listener, "_Listener__channel", None)

    # If these objects exist, they should be closed.
    if conn is not None:
        assert not getattr(conn, "is_open", False)
    if ch is not None:
        assert not getattr(ch, "is_open", False)


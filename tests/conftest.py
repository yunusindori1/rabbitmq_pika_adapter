from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import pytest


@pytest.fixture()
def connection_params() -> Dict[str, Any]:
    # Not used to connect in unit tests; only here to validate parsing.
    return {
        "server": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "vhost": "/",
    }


@pytest.fixture()
def message_type_map() -> Dict[str, Any]:
    return {
        "mt": {
            "exchange_name": "ex",
            "exchange_type": "topic",
            "routing_key": "rk",
            "predefined_queue_name": "q",
            "queue_arguments": {"x-message-ttl": 20000},
        }
    }


def assert_json_bytes(payload: bytes) -> Dict[str, Any]:
    decoded = payload.decode("utf-8")
    return json.loads(decoded)


def _getenv_any(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _strip_env(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v2 = v.strip()
    return v2 if v2 else None


@pytest.fixture(scope="session")
def rabbitmq_env_params() -> Optional[Dict[str, Any]]:
    """Return RabbitMQ connection params for integration tests.

    Integration tests are opt-in and require a live broker reachable via env vars.

    Supported env vars:
      - RABBITMQ_HOST (or RABBITMQ_SERVER)
      - RABBITMQ_PORT
      - RABBITMQ_VHOST
      - RABBITMQ_USER
      - RABBITMQ_PASSWORD

    Notes:
      - Values are stripped to avoid issues from trailing spaces in .cmd files.
    """

    host = _strip_env(_getenv_any("RABBITMQ_HOST", "RABBITMQ_SERVER"))
    port = _strip_env(os.getenv("RABBITMQ_PORT"))
    vhost = _strip_env(os.getenv("RABBITMQ_VHOST"))
    user = _strip_env(os.getenv("RABBITMQ_USER"))
    password = _strip_env(os.getenv("RABBITMQ_PASSWORD"))

    if not (host and port and vhost and user and password):
        return None

    return {
        "server": host,
        "port": int(port),
        "vhost": vhost,
        "username": user,
        "password": password,
    }


@pytest.fixture(scope="session")
def require_rabbitmq(rabbitmq_env_params):
    """Skip integration tests unless RabbitMQ env vars are configured."""
    if rabbitmq_env_params is None:
        pytest.skip(
            "RabbitMQ integration env vars not set. Set RABBITMQ_HOST/RABBITMQ_PORT/RABBITMQ_VHOST/"
            "RABBITMQ_USER/RABBITMQ_PASSWORD to run integration tests.",
            allow_module_level=True,
        )
    return rabbitmq_env_params


@pytest.fixture(autouse=True)
def _reset_async_adapter_aio_pika_between_tests():
    """Prevent unit tests from leaking FakeAioPika into integration tests.

    Several unit tests patch `mq_adapters.async_adapter.aio_pika` to a fake module. If that
    global remains patched, integration tests will fail when they need the real `aio_pika`.

    We reset it to None before/after each test so async adapters will lazy-import the real
    dependency when needed.
    """
    try:
        from mq_adapters import async_adapter
    except Exception:
        # Async extras not installed or import failed; nothing to reset.
        yield
        return

    old = getattr(async_adapter, "aio_pika", None)
    async_adapter.aio_pika = None  # type: ignore
    try:
        yield
    finally:
        async_adapter.aio_pika = None  # type: ignore

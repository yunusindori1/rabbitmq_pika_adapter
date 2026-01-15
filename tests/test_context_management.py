from __future__ import annotations

import pytest

from mq_adapters.client import RabbitMQClient
from mq_adapters.publisher_pool import PublisherPool
from mq_adapters.helper_functions import CloseError


def test_rabbitmq_client_context_manager_calls_close(message_type_map, monkeypatch):
    called = {"n": 0}

    def fake_close(self):
        called["n"] += 1

    monkeypatch.setattr(RabbitMQClient, "close", fake_close)

    with RabbitMQClient(
        connection_params={
            "server": "localhost",
            "port": 5672,
            "vhost": "/",
            "username": "guest",
            "password": "guest",
            "message_types": message_type_map,
        }
    ):
        pass

    assert called["n"] == 1


@pytest.mark.asyncio
async def test_rabbitmq_client_async_context_manager_calls_aclose(message_type_map, monkeypatch):
    called = {"n": 0}

    async def fake_aclose(self):
        called["n"] += 1

    monkeypatch.setattr(RabbitMQClient, "aclose", fake_aclose)

    async with RabbitMQClient(
        connection_params={
            "server": "localhost",
            "port": 5672,
            "vhost": "/",
            "username": "guest",
            "password": "guest",
            "message_types": message_type_map,
        }
    ):
        pass

    assert called["n"] == 1


def test_publisher_pool_context_manager_starts_and_stops(message_type_map, monkeypatch):
    started = {"n": 0}
    stopped = {"n": 0}

    def fake_start(self):
        started["n"] += 1

    def fake_stop(self, wait=True):
        stopped["n"] += 1

    monkeypatch.setattr(PublisherPool, "start", fake_start)
    monkeypatch.setattr(PublisherPool, "stop", fake_stop)

    pool = PublisherPool(
        "mt",
        message_type_map=message_type_map,
        connection_params={
            "server": "localhost",
            "port": 5672,
            "vhost": "/",
            "username": "guest",
            "password": "guest",
            "message_types": message_type_map,
        },
    )

    with pool:
        pass

    assert started["n"] == 1
    assert stopped["n"] == 1


def test_rabbitmq_client_close_raises_closeerror_on_pool_stop_failure(message_type_map, monkeypatch):
    client = RabbitMQClient(message_type_map=message_type_map, connection_params={
        "server": "localhost",
        "port": 5672,
        "vhost": "/",
        "username": "guest",
        "password": "guest",
        "message_types": message_type_map,
    })

    class BadPool:
        def stop(self):
            raise RuntimeError("boom")

    # Inject a failing pool into the cache, simulating a stop failure.
    client._sync_pools["mt"] = BadPool()  # type: ignore[assignment]

    with pytest.raises(CloseError):
        client.close()


@pytest.mark.asyncio
async def test_rabbitmq_client_aclose_raises_closeerror_on_pool_stop_failure(message_type_map):
    client = RabbitMQClient(message_type_map=message_type_map, connection_params={
        "server": "localhost",
        "port": 5672,
        "vhost": "/",
        "username": "guest",
        "password": "guest",
        "message_types": message_type_map,
    })

    class BadAsyncPool:
        async def stop(self):
            raise RuntimeError("boom")

    client._async_pools["mt"] = BadAsyncPool()  # type: ignore[assignment]

    with pytest.raises(CloseError):
        await client.aclose()

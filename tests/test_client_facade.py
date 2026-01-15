from __future__ import annotations

import pytest

from mq_adapters.client import RabbitMQClient


def test_client_creates_sync_pool_publisher(message_type_map):
    client = RabbitMQClient(message_type_map=message_type_map, connection_params={
        "server": "localhost",
        "port": 5672,
        "vhost": "/",
        "username": "guest",
        "password": "guest",
        "message_types": message_type_map,
    })

    pub = client.publisher("mt")
    # PublisherPool isn't started by default
    pub.start()
    pub.send(b"x")
    client.close()


@pytest.mark.asyncio
async def test_client_async_publisher_uses_injected_factory(message_type_map):
    # This is a pure unit test: inject a fake async connection factory so we don't touch the network.
    calls = {"n": 0}

    async def fake_async_factory():
        calls["n"] += 1

        class _Conn:
            is_closed = False

            async def channel(self, publisher_confirms: bool = False):
                class _Chan:
                    is_closed = False

                    async def declare_exchange(
                            self, name: str, type: str, durable: bool = True, auto_delete: bool = False):
                        class _Ex:
                            async def publish(self, message, routing_key: str):
                                return None

                        return _Ex()

                    async def close(self):
                        self.is_closed = True

                return _Chan()

            async def close(self):
                self.is_closed = True

        return _Conn()

    client = RabbitMQClient(
        message_type_map=message_type_map,
        connection_params={
            "server": "localhost",
            "port": 5672,
            "vhost": "/",
            "username": "guest",
            "password": "guest",
            "message_types": message_type_map,
        },
        async_connection_factory=fake_async_factory,
    )

    try:
        pub = client.async_publisher("mt")
    except RuntimeError:
        # Async extras not installed in this environment
        return

    await pub.start()
    await pub.send(b"x")
    await pub.stop()
    await client.aclose()
    assert calls["n"] == 1

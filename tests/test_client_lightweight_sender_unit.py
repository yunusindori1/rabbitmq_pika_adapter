from __future__ import annotations

import time

from mq_adapters.client import RabbitMQClient


def test_client_lightweight_sender_uses_pool_and_no_sender_threads(message_type_map):
    # Inject a fake sync connection factory; we just need to ensure we don't instantiate Sender
    # (which would create per-instance threads and a connection during __init__).
    calls = {"connections": 0, "channels": 0, "publishes": 0}

    class _Ch:
        is_open = True

        def basic_publish(self, exchange, routing_key, body):
            calls["publishes"] += 1

    class _Conn:
        is_open = True

        def channel(self):
            calls["channels"] += 1
            return _Ch()

        def process_data_events(self, time_limit=0):
            return None

    def fake_factory():
        calls["connections"] += 1
        return _Conn()

    client = RabbitMQClient(
        message_type_map=message_type_map,
        connection_factory=fake_factory,
        connection_params={"message_types": message_type_map},
    )

    pub = client.publisher("mt", backend="lightweight_sender", num_workers=2)
    pub.start()
    pub.send(b"x")
    pub.send(b"y")

    # Wait briefly for the backend thread to drain
    deadline = time.time() + 2
    while time.time() < deadline and calls["publishes"] < 2:
        time.sleep(0.01)

    client.close()

    # backend is pool-backed, so it should create just 1 connection and N channels
    assert calls["connections"] == 1
    assert calls["channels"] == 2
    assert calls["publishes"] == 2

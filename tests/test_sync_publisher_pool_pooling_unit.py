from __future__ import annotations

import time

from mq_adapters.publisher_pool import PublisherPool


def test_sync_publisher_pool_uses_single_connection_and_multiple_channels(message_type_map):
    calls = {"connections": 0, "channels": 0, "publishes": 0}

    class _Ch:
        is_open = True

        def confirm_delivery(self):
            return None

        def basic_publish(self, exchange, routing_key, body):
            calls["publishes"] += 1

        def close(self):
            self.is_open = False

    class _Conn:
        is_open = True

        def channel(self):
            calls["channels"] += 1
            return _Ch()

        def process_data_events(self, time_limit=0):
            return None

        def close(self):
            self.is_open = False

    def fake_factory():
        calls["connections"] += 1
        return _Conn()

    pool = PublisherPool(
        message_type="mt",
        num_workers=4,
        connection_factory=fake_factory,
        message_type_map=message_type_map,
    )

    pool.start()
    try:
        # Enqueue a few messages
        for _ in range(25):
            pool.send(b"x")

        # Wait briefly for backend thread to drain
        deadline = time.time() + 2
        while time.time() < deadline and calls["publishes"] < 25:
            time.sleep(0.01)

        assert calls["publishes"] == 25
        # Key assertions: one connection, multiple channels
        assert calls["connections"] == 1
        assert calls["channels"] == 4
    finally:
        pool.stop()

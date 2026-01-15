"""Unit tests for sync Listener offload mode (no RabbitMQ required)."""

from __future__ import annotations

import threading
import time

from mq_adapters.sync_adapter import Listener


class FakeConn:
    """Fake pika connection object supporting add_callback_threadsafe."""

    def __init__(self):
        self.is_open = True
        self.callbacks = []

    def add_callback_threadsafe(self, cb):
        """Record and immediately execute a callback."""
        self.callbacks.append(cb)
        cb()


class FakeCh:
    """Fake pika channel collecting ack/nack calls."""

    def __init__(self):
        self.acked = []
        self.nacked = []

    def basic_ack(self, delivery_tag):
        """Record ack."""
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=True):
        """Record nack."""
        self.nacked.append((delivery_tag, requeue))


class FakeMethod:
    """Fake pika delivery method with delivery_tag."""

    def __init__(self, tag=1):
        self.delivery_tag = tag


def test_sync_listener_offload_ack(message_type_map):
    done = threading.Event()

    def cb(ch, method, props, body):
        """Simple handler used to verify ack scheduling."""
        done.set()

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_params={"server": "x", "port": 1, "vhost": "/", "username": "u", "password": "p", "message_types": message_type_map},
        auto_ack=False,
        offload=True,
        max_workers=1,
        max_in_flight=1,
    )

    # inject fakes
    setattr(listener, "_Listener__connection", FakeConn())

    ch = FakeCh()
    method = FakeMethod(tag=123)

    listener._offload_wrapper(ch, method, None, b"x")

    assert done.wait(2)
    # allow ack callback to execute
    time.sleep(0.05)

    assert ch.acked == [123]
    assert ch.nacked == []


def test_sync_listener_offload_nack_on_exception(message_type_map):
    def cb(ch, method, props, body):
        """Handler that raises to trigger nack/requeue."""
        raise RuntimeError("boom")

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_params={"server": "x", "port": 1, "vhost": "/", "username": "u", "password": "p", "message_types": message_type_map},
        auto_ack=False,
        offload=True,
        max_workers=1,
        max_in_flight=1,
    )

    setattr(listener, "_Listener__connection", FakeConn())

    ch = FakeCh()
    method = FakeMethod(tag=1)

    listener._offload_wrapper(ch, method, None, b"x")
    time.sleep(0.1)

    assert ch.acked == []
    assert ch.nacked == [(1, True)]


def test_sync_listener_internal_wrapper_ack_on_success(message_type_map):
    def cb(ch, method, props, body):
        """Handler that returns normally to trigger ack."""
        return None

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_params={"server": "x", "port": 1, "vhost": "/", "username": "u", "password": "p", "message_types": message_type_map},
        auto_ack=False,
        offload=False,
    )

    ch = FakeCh()
    method = FakeMethod(tag=55)

    listener._internal_wrapper(ch, method, None, b"x")

    assert ch.acked == [55]
    assert ch.nacked == []


def test_sync_listener_internal_wrapper_nack_on_exception(message_type_map):
    def cb(ch, method, props, body):
        """Handler that raises to trigger nack/requeue."""
        raise RuntimeError("boom")

    listener = Listener(
        "mt",
        cb,
        message_type_map=message_type_map,
        connection_params={"server": "x", "port": 1, "vhost": "/", "username": "u", "password": "p", "message_types": message_type_map},
        auto_ack=False,
        offload=False,
    )

    ch = FakeCh()
    method = FakeMethod(tag=1)

    listener._internal_wrapper(ch, method, None, b"x")

    assert ch.acked == []
    assert ch.nacked == [(1, True)]

"""
Examples showing correct usage of Listener and PublisherPool from mq_adapters.
Adjust `connection_params` or provide a `connection_factory` and ensure your MQ is reachable.

This file contains small examples:
  - basic_publisher_pool_example(): demonstrates using PublisherPool to publish many messages while limiting
    connections on the machine (useful when you will run hundreds of logical producers)
  - listener_example(): shows creating a Listener with auto_ack=True and with manual ack (auto_ack=False)
  - async_example(): shows AsyncSender and AsyncListener using aio-pika (install with `rabbit-mq-client[async]`)
  - client_facade_example(): demonstrates the RabbitMQClient facade (recommended)

Run the examples from the command line or import the helpers in your own scripts.
"""
import asyncio
import json
import logging
import os
import signal
import threading
import time
from typing import Dict

from mq_adapters import RabbitMQClient
from mq_adapters.publisher_pool import PublisherPool
from mq_adapters.sync_adapter import Listener

try:
    from mq_adapters.async_adapter import AsyncSender, AsyncListener
except Exception:
    AsyncSender = None  # type: ignore
    AsyncListener = None  # type: ignore

# Replace with a valid message_type key from your message_type_map
MESSAGE_TYPE = "all_market_events"


def _load_dotenv_if_present() -> None:
    """
    Load environment variables from a local .env file if it exists.
    No-op if python-dotenv isn't installed or no .env is found.
    """
    try:
        from dotenv import load_dotenv, find_dotenv  # type: ignore
    except Exception:
        return

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        # override=False means real environment vars win over .env values
        load_dotenv(dotenv_path)


# Example connection parameters dict (previously stored in config.yml)
_load_dotenv_if_present()
CONNECTION_PARAMS: Dict[str, object] = {
    'server': os.environ.get("USAGE_EX_RABBITMQ_HOST"),
    'port': os.environ.get("USAGE_EX_RABBITMQ_PORT"),
    'vhost': os.environ.get("USAGE_EX_RABBITMQ_VHOST"),
    'username': os.environ.get("USAGE_EX_RABBITMQ_USERNAME"),
    'password': os.environ.get("USAGE_EX_RABBITMQ_PASSWORD"),
    'message_types': {
        'all_market_events': {
            'exchange_name': 'market_data',
            'exchange_type': 'topic',
            'routing_key': 'tradier.market.events.*',
            'predefined_queue_name': 'market_data.events',
            # Queue declaration arguments must match the existing queue or RabbitMQ will reject.
            # Use exact broker args. Examples:
            # - x-message-ttl: message TTL (ms)
            # - x-dead-letter-exchange / x-dead-letter-routing-key: DLX retry routing configured on the broker
            #
            # Library-side failure handling (auto_ack=False):
            # - on_error="dead_letter" -> nack(requeue=False) so broker DLX handles retries
            # - dead_letter_queue + max_retries -> when attempt >= max_retries, publish to DLQ queue and ack
            #   (DLQ queue must already exist; the library will NOT create it)
            'queue_arguments': {
                'x-message-ttl': 20000,
                # Library-controlled behavior:
                'on_error': 'dead_letter',
                'dead_letter_queue': 'market_data.events.dlq',
                'max_retries': 1,
            },
        }
    }
}


def _print_lingering_threads(label: str = "") -> None:
    """Debug helper: list threads that can keep the process alive (non-daemon, non-main)."""
    threads = [t for t in threading.enumerate() if t.is_alive()]
    non_daemon = [t for t in threads if not t.daemon and t is not threading.current_thread()]
    if label:
        print(f"\n--- Thread dump {label} ---")
    else:
        print("\n--- Thread dump ---")
    for t in sorted(threads, key=lambda x: x.name):
        print(f"- {t.name}: alive={t.is_alive()} daemon={t.daemon}")
    if non_daemon:
        print("Non-daemon threads still alive (these can keep Python running):")
        for t in sorted(non_daemon, key=lambda x: x.name):
            print(f"  * {t.name}")


# module logger for examples
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ------------------- PublisherPool example -------------------
def basic_sender_example():
    """
    Publish many messages using PublisherPool (recommended).
    """
    pool = PublisherPool(message_type=MESSAGE_TYPE, verbose=False, connection_params=CONNECTION_PARAMS,
                         queue_maxsize=10000)
    pool.start()

    try:
        for i in range(10000):
            payload = json.dumps({"seq": i, "ts": time.time()})
            pool.send(payload)
            logger.debug("Sent message %d", i)
    finally:
        time.sleep(1)
        pool.stop()
        logger.info("PublisherPool stats: %s", pool.stats_snapshot())


# ------------------- Listener example -------------------
def listener_example():
    """Start a Listener that prints message bodies.

    The Listener callback signature must be (ch, method, properties, body).

    Acknowledgements (`auto_ack`):
      - auto_ack=True (legacy default): at-most-once delivery (no redelivery on crash/exception).
      - auto_ack=False: at-least-once delivery; the library acks after successful callback return.

    Error handling when `auto_ack=False`:
      - default is `nack(requeue=True)` on exception (can hot-loop for poison messages)
      - if your *predefined* queue has queue_arguments with `on_error="dead_letter"`, the library will instead
        `nack(requeue=False)` so RabbitMQ DLX/TTL retry queues can handle redelivery delays.
      - if `dead_letter_queue` is set and attempt >= `max_retries` (default 1), the library publishes the message
        to that DLQ queue (preserving headers/properties best-effort) and ACKs the original.

    Recommendation: for most consumers that do real work, use auto_ack=False.
    """

    def callback_auto_ack(ch, method, properties, body: bytes):
        """Fire-and-forget handler used with auto_ack=True."""
        message = body.decode("utf-8")
        logger.debug("[auto-ack] handler saw message: %s", message)

    def callback_at_least_once(ch, method, properties, body: bytes):
        """At-least-once handler used with auto_ack=False (ack handled by library)."""
        message = body.decode("utf-8")
        logger.debug("[at-least-once] handler saw message: %s", message)

        if 'fail' in message:
            raise RuntimeError("simulated handler failure")

    listener1 = Listener(
        message_type=MESSAGE_TYPE,
        callback=callback_auto_ack,
        auto_ack=True,
        verbose=False,
        connection_params=CONNECTION_PARAMS,
        predefined_queue=True,
    )
    listener1.start_listening()

    listener2 = Listener(
        message_type=MESSAGE_TYPE,
        callback=callback_at_least_once,
        auto_ack=False,
        verbose=False,
        prefetch_count=10,
        connection_params=CONNECTION_PARAMS,
        predefined_queue=True,
    )
    listener2.start_listening()

    try:
        time.sleep(5)
    finally:
        listener1.stop_listening()
        listener2.stop_listening()
        listener1.join(timeout=5)
        listener2.join(timeout=5)
        logger.info("Listener(auto_ack=True) stats: %s", listener1.stats_snapshot())
        logger.info("Listener(auto_ack=False) stats: %s", listener2.stats_snapshot())


# ------------------- Async (aio-pika) example -------------------
async def async_example(run_seconds: int = 5):
    """Demonstrate AsyncSender + AsyncListener.

    Requires:
      pip install rabbit-mq-client[async]
    """
    if AsyncSender is None or AsyncListener is None:
        raise RuntimeError("Async support not installed. Install with: pip install rabbit-mq-client[async]")

    async def on_msg(ch, method, properties, body: bytes):
        """Example async handler."""
        logger.debug("[async] Received:", body.decode("utf-8"))

    listener = AsyncListener(
        message_type=MESSAGE_TYPE,
        callback=on_msg,
        connection_params=CONNECTION_PARAMS,
        prefetch_count=50,
        auto_ack=False,
        verbose=False,
        predefined_queue=True,
    )
    sender = AsyncSender(
        message_type=MESSAGE_TYPE,
        connection_params=CONNECTION_PARAMS,
        confirm_delivery=False,
        verbose=False,
    )

    await listener.start()
    await sender.start()

    start = time.time()
    i = 0
    try:
        while time.time() - start < run_seconds:
            await sender.send({"seq": i, "ts": time.time()})
            i += 1
            await asyncio.sleep(0.1)
    finally:
        await listener.stop()
        await sender.stop()


# ------------------- High throughput publish + consume example -------------------
def high_throughput_publish_and_consume(total_messages: int = 50000, num_workers: int = 4):
    """High-throughput demo: publish N messages and consume them, then print stats.

    Notes:
      - Uses PublisherPool for efficient sync publishing.
      - Uses Listener(auto_ack=True) for max throughput (at-most-once).
      - Prints stats snapshots for both publisher and listener.
    """

    observed = 0

    def on_msg(ch, method, properties, body: bytes):
        nonlocal observed
        observed += 1

    listener = Listener(
        message_type=MESSAGE_TYPE,
        callback=on_msg,
        auto_ack=True,
        verbose=False,
        connection_params=CONNECTION_PARAMS,
        predefined_queue=True,
        prefetch_count=0,
    )

    pool = PublisherPool(
        message_type=MESSAGE_TYPE,
        verbose=False,
        connection_params=CONNECTION_PARAMS,
        num_workers=num_workers,
        queue_maxsize=max(1000, min(100000, total_messages)),
    )

    # Start consumer first so it is ready.
    listener.start_listening()
    pool.start()

    start = time.time()
    try:
        for i in range(total_messages):
            pool.send({"seq": i, "ts": time.time()})
        # Wait until consumer catches up (best-effort)
        deadline = time.time() + 30
        while observed < total_messages and time.time() < deadline:
            time.sleep(0.05)
    finally:
        # Stop in reverse order
        try:
            pool.stop()
        finally:
            listener.stop_listening()
            listener.join(timeout=5)

    elapsed = max(0.001, time.time() - start)
    logger.info("High throughput run: published=%d observed=%d elapsed=%.2fs (%.0f msg/s observed)",
                total_messages, observed, elapsed, observed / elapsed)
    logger.info("PublisherPool stats: %s", pool.stats_snapshot())
    logger.info("Listener stats: %s", listener.stats_snapshot())


# ------------------- Client facade example -------------------
def client_facade_example():
    """Demonstrate the RabbitMQClient facade (recommended)."""

    client = RabbitMQClient(connection_params=CONNECTION_PARAMS, verbose=False)

    pub = client.publisher(MESSAGE_TYPE)
    pub.start()
    pub.send({"hello": "world", "ts": time.time()})

    count = 0

    def on_msg(ch, method, properties, body: bytes):
        """Example facade handler."""
        nonlocal count
        count += 1
        logger.info("[client] handler saw body: %r", body)

    sub = client.listener(MESSAGE_TYPE, callback=on_msg, predefined_queue=True, auto_ack=True)
    sub.start()

    time.sleep(2)
    sub.stop()

    # Stats printing (now supported by facade handles)
    logger.info("Client publisher stats: %s", pub.stats_snapshot())
    logger.info("Client listener stats: %s", sub.stats_snapshot())
    logger.info("Total messages observed by handler: %d", count)

    client.close()


# ------------------- Demo runner -------------------
if __name__ == "__main__":
    # Simple CLI: choose which demo to run
    import argparse

    logging.basicConfig()
    # Let the library control per-message logs via DEBUG; examples default to INFO.
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="MQ adapters usage examples")
    parser.add_argument("action", choices=["sender", "listener", "pool", "throughput", "async", "client", "all"],
                        nargs="?",
                        default="all",
                        help="Which example to run")
    args = parser.parse_args()


    # graceful shutdown on Ctrl+C
    def _handle_sig(signum, frame):
        logger.info("signal received, exiting")
        raise KeyboardInterrupt


    signal.signal(signal.SIGINT, _handle_sig)
    try:
        if args.action in ("sender", "all"):
            print("Running sender example...")
            basic_sender_example()

        if args.action in ("listener", "all"):
            print("Running listener example...")
            listener_example()

        if args.action in ("throughput", "all"):
            print("Running high throughput publish+consume example...")
            high_throughput_publish_and_consume(total_messages=50000, num_workers=4)

        if args.action in ("async", "all"):
            print("Running async example...")
            asyncio.run(async_example(run_seconds=15))

        # if args.action in ("async-pool", "all"):
        #     print("Running async publisher pool example...")
        #     asyncio.run(async_publisher_pool_example(run_seconds=15))

        if args.action in ("client", "all"):
            print("Running client facade example...")
            client_facade_example()

    except KeyboardInterrupt:
        logger.info("Shutting down due to keyboard interrupt")
    finally:
        _print_lingering_threads("after examples")

    logger.info("Done")

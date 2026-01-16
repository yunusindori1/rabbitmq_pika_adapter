rabbitmq-pika-adapter — Usage and migration guide

This document describes how to use the current API (PublisherPool/Listener + async equivalents) with explicit configuration and
how to migrate from the previous setup that relied on `yi_config_starter` and `config.yml`.

Key concepts

- Provide connection information explicitly via:
    - `connection_factory`: a callable that returns a `pika.BlockingConnection` when called, OR
    - `connection_params`: a dict with keys `server`, `port`, `vhost`, `username`, `password`, and optionally `message_types` map.
- If neither `connection_factory` nor `connection_params` are provided, the library will attempt to read environment variables:
    - RABBITMQ_HOST (or RABBITMQ_SERVER)
    - RABBITMQ_PORT
    - RABBITMQ_VHOST
    - RABBITMQ_USER
    - RABBITMQ_PASSWORD

Acknowledgement semantics (`auto_ack`)

`Listener` and `AsyncListener` support two acknowledgement modes:

- `auto_ack=False` (default): **at-least-once** delivery.
    - The library will ack **after** your callback returns successfully.
    - If your callback raises an exception, the library will nack with `requeue=True`.
    - Your handler should be **idempotent** (messages may be delivered more than once).

- `auto_ack=True` (legacy): **at-most-once** delivery.
    - Messages are considered acknowledged on delivery.
    - If your callback raises or your process crashes during processing, RabbitMQ will **not** redeliver that message.
    - Use this only for truly fire-and-forget workloads (telemetry, best-effort notifications).

Sync `Listener` specifics:

- When `auto_ack=False` and `offload=False`, ack/nack is done directly in the internal wrapper.
- When `auto_ack=False` and `offload=True`, ack/nack is scheduled back onto pika's I/O thread via `add_callback_threadsafe`.

Async `AsyncListener` specifics:

- When `auto_ack=False`, the listener awaits your callback (if it returns a coroutine), then acks on success and nacks on exception.
- Callbacks run in background tasks with bounded concurrency (`max_concurrency`).

Recommendation: for most consumers that do real work, use `auto_ack=False`.

Examples

1) Sync publishing using `PublisherPool` (recommended)

```python
from mq_adapters.publisher_pool import PublisherPool

params = {
    'server': 'localhost',
    'port': 5672,
    'vhost': '/',
    'username': 'guest',
    'password': 'guest',
    'message_types': {
        'market_events': {'exchange_name': 'events', 'exchange_type': 'topic', 'routing_key': 'events.#'}
    }
}

pool = PublisherPool('market_events', connection_params=params, num_workers=4)
pool.start()
pool.send(b'hello')
pool.stop()

print('publisher stats:', pool.stats_snapshot())
```

2) Sync consuming using `Listener`

```python
import time

from mq_adapters.sync_adapter import Listener

params = {
    'server': 'localhost',
    'port': 5672,
    'vhost': '/',
    'username': 'guest',
    'password': 'guest',
    'message_types': {
        'market_events': {
            'exchange_name': 'events',
            'exchange_type': 'topic',
            'routing_key': 'events.#',
            'predefined_queue_name': 'market_events.q',
        }
    }
}

seen = 0

def on_msg(ch, method, properties, body: bytes):
    global seen
    seen += 1

listener = Listener('market_events', callback=on_msg, connection_params=params, predefined_queue=True, auto_ack=True)
listener.start_listening()

time.sleep(2)
listener.stop_listening()
listener.join(timeout=5)
print('listener stats:', listener.stats_snapshot())
```

3) Using the facade (`RabbitMQClient`) + stats snapshots

```python
import time

from mq_adapters import RabbitMQClient

params = {
    'server': 'localhost',
    'port': 5672,
    'vhost': '/',
    'username': 'guest',
    'password': 'guest',
    'message_types': {
        'market_events': {'exchange_name': 'events', 'exchange_type': 'topic', 'routing_key': 'events.#'}
    }
}

client = RabbitMQClient(connection_params=params)

pub = client.publisher('market_events')
pub.start()
pub.send(b'hello')

count = 0

def on_msg(ch, method, properties, body: bytes):
    global count
    count += 1

sub = client.listener('market_events', callback=on_msg, predefined_queue=True, auto_ack=True)
sub.start()

time.sleep(1)
sub.stop()

print('publisher stats:', pub.stats_snapshot())
print('listener stats:', sub.stats_snapshot())

client.close()
```

4) High-throughput demo (sync)

The repo includes an example that publishes many messages and consumes them while printing stats:

    python examples/usage.py throughput

5) Async (aio-pika) — optional

Install:

    pip install rabbitmq-pika-adapter[async]

Example:

```python
import asyncio

from mq_adapters.async_adapter import AsyncSender, AsyncListener

params = {
    'server': 'localhost',
    'port': 5672,
    'vhost': '/',
    'username': 'guest',
    'password': 'guest',
    'message_types': {
        'market_events': {'exchange_name': 'events', 'exchange_type': 'topic', 'routing_key': 'events.#'}
    }
}


async def main():
    """Example async send/receive."""
    sender = AsyncSender('market_events', connection_params=params)

    async def on_msg(ch, method, properties, body: bytes):
        """Example message handler."""
        print('got', body)

    listener = AsyncListener(
        'market_events',
        callback=on_msg,
        connection_params=params,
        prefetch_count=50,
        auto_ack=False,
    )

    await listener.start()
    await sender.start()

    for i in range(10):
        await sender.send({'i': i})

    await asyncio.sleep(2)
    await listener.stop()
    await sender.stop()


if __name__ == '__main__':
    asyncio.run(main())
```

Notes:

- For best throughput with confirms, prefer async (`AsyncSender`).
- Sync `PublisherPool` can enable confirms (`confirm_delivery=True`) but will generally incur higher per-message cost.

Migration notes

- This project no longer depends on `yi_config_starter` and does not auto-load `config.yml`.
- Legacy `Sender`/`AsyncPublisherPool` were removed to keep the library surface small.
  - Sync publishing is done via `PublisherPool`.
  - Async publishing is done via `AsyncSender`.
- If you previously stored your RabbitMQ parameters in `config.yml`, provide them via `connection_params` or use
  environment variables with the names described above.

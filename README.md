# rabbitmq-pika-adapter

Lightweight RabbitMQ client helpers built on pika (sync) and aio-pika (async).

Core API:
- Sync publish: `PublisherPool` (recommended)
- Sync consume: `Listener`
- Async publish/consume: `AsyncSender`, `AsyncListener` (optional extra)
- Facade: `RabbitMQClient` (recommended entrypoint)

## Acknowledgements (`auto_ack`)

Consumers support two acknowledgement modes:

- `auto_ack=False` (default): at-least-once delivery. The library will ack *after* your callback returns successfully, and nack/requeue on exception. Your
  handler should be idempotent because messages may be redelivered.
- `auto_ack=True` (legacy): at-most-once delivery. Messages are considered acknowledged on delivery. If your callback raises or the process
  crashes mid-processing, RabbitMQ will not redeliver that message.

Recommendation: for most "work queue" style consumers, use `auto_ack=False`.

See `docs/USAGE.md` for details and examples.

## Installation

From PyPI:

    pip install rabbitmq-pika-adapter

Async support:

    pip install rabbitmq-pika-adapter[async]

This enables `AsyncSender` and `AsyncListener`.

## Usage

Recommended entrypoint:

- `RabbitMQClient(...).publisher(...)` / `.listener(...)`

Direct usage (advanced/explicit):

- `Listener(message_type, callback, ...)` - thread-based consumer.
- `PublisherPool(message_type, ...)` - sync publisher with pooled channels.
- `AsyncListener(message_type, callback, ...)` - asyncio-based consumer.
- `AsyncSender(message_type, ...)` - asyncio-based publisher.

See `examples/usage.py` and `docs/USAGE.md`.

### Examples

- High-throughput sync publish+consume demo (prints stats for both):

    python examples/usage.py throughput

Notes:
- The repo uses a `src/` layout. If you want to run examples without installing the package, `examples/usage.py` will
  automatically add `src/` to `sys.path`.

## Testing

Install test deps:

    pip install -e .[test]

Run unit tests:

    python -m pytest -q

### Integration tests (no Docker)

Integration tests are opt-in and require a reachable RabbitMQ.
Set env vars:

- `RABBITMQ_HOST` (or `RABBITMQ_SERVER`)
- `RABBITMQ_PORT`
- `RABBITMQ_VHOST`
- `RABBITMQ_USER`
- `RABBITMQ_PASSWORD`

Then run:

    python -m pytest -m integration -q

## Publishing to PyPI

This repo publishes from GitHub Actions:

- Pushes to `develop` publish to TestPyPI.
- Pushes to `release` publish to PyPI.

Manual build/publish:

    python -m pip install build twine
    python -m build
    python -m twine check dist/*
    python -m twine upload dist/*

"""rabbitmq-pika-adapter public API.

Prefer importing from this package (or `mq_adapters`) rather than individual modules.

Note:
- The distribution/project name is `rabbitmq-pika-adapter`.
- The importable package is `mq_adapters`.
- This root-level __init__.py exists primarily for the repo layout and forwards exports.
"""

try:
    # When installed as a proper package, relative imports work.
    from .mq_adapters import RabbitMQClient, Listener, PublisherPool, AsyncSender, AsyncListener  # type: ignore
except Exception:  # pragma: no cover
    # When executed via importlib (no package context), fall back to absolute import.
    from mq_adapters import RabbitMQClient, Listener, PublisherPool, AsyncSender, AsyncListener  # type: ignore

__all__ = ["RabbitMQClient", "Listener", "PublisherPool", "AsyncSender", "AsyncListener"]

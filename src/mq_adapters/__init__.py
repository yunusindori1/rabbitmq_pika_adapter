"""Public package exports for rabbitmq-pika-adapter.

This package intentionally exposes a small API surface:
- RabbitMQClient facade (recommended)
- PublisherPool (sync publishing)
- Listener (sync consuming)
- AsyncSender/AsyncListener (async publishing/consuming; requires aio-pika)
"""

__version__ = "0.2.2"

from .sync_adapter import Listener
from .publisher_pool import PublisherPool

# Optional async API (requires aio-pika). Import errors are deferred until actually used.
try:
    from .async_adapter import AsyncSender, AsyncListener
except Exception:  # pragma: no cover
    AsyncSender = None  # type: ignore
    AsyncListener = None  # type: ignore

from .client import RabbitMQClient

__all__ = ["Listener", "PublisherPool", "AsyncSender", "AsyncListener", "RabbitMQClient"]

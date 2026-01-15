"""Client facade for rabbit-mq-client.

This module provides a *thin API surface* so users don't have to interact with the threading
and adapter internals directly.

Design goals:
- Keep a small, coherent API surface.
- Default to pooled sync publishers (PublisherPool) for efficiency.
- Provide async publishing/consuming via AsyncSender/AsyncListener (aio-pika).

The facade does not auto-start anything unless requested.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from mq_adapters.sync_adapter import Listener
from mq_adapters.helper_functions import raise_if_errors
from mq_adapters.publisher_pool import PublisherPool
from mq_adapters.retry_policy import RetryPolicy

# Optional async backends (aio-pika). Keep imports lazy/optional so sync-only installs work.
try:  # pragma: no cover
    from mq_adapters.async_adapter import AsyncSender, AsyncListener
except Exception:  # pragma: no cover
    AsyncSender = None  # type: ignore
    AsyncListener = None  # type: ignore


@dataclass
class SyncPublisher:
    """Sync publisher handle.

    Default backend is PublisherPool (recommended) to avoid per-Sender threads.
    """

    _backend: Any

    def start(self) -> None:
        """Start the underlying publisher backend (if supported)."""
        if hasattr(self._backend, "start"):
            self._backend.start()

    def send(self, message: Any, routing_key: Optional[str] = None) -> None:
        """Publish a message via the configured backend."""
        self._backend.send(message, routing_key=routing_key)

    def stop(self) -> None:
        """Stop the underlying publisher backend (if supported)."""
        if hasattr(self._backend, "stop"):
            self._backend.stop()

    def stats_snapshot(self):
        """Return a stats snapshot if the backend supports it."""
        fn = getattr(self._backend, "stats_snapshot", None)
        if callable(fn):
            return fn()
        return None


@dataclass
class SyncListenerHandle:
    """Sync listener handle."""

    _backend: Listener

    def start(self) -> None:
        """Start the listener."""
        self._backend.start_listening()

    def stop(self) -> None:
        """Stop the listener."""
        self._backend.stop_listening()

    def stats_snapshot(self):
        """Return listener stats snapshot."""
        return self._backend.stats_snapshot()


@dataclass
class AsyncPublisher:
    """Async publisher handle."""

    _backend: Any

    async def start(self) -> None:
        """Start the underlying async publisher backend."""
        if hasattr(self._backend, "start"):
            await self._backend.start()  # AsyncSender

    async def send(self, message: Any, routing_key: Optional[str] = None) -> None:
        """Publish a message via the configured async backend."""
        await self._backend.send(message, routing_key=routing_key)

    async def stop(self) -> None:
        """Stop the underlying async publisher backend."""
        if hasattr(self._backend, "stop"):
            await self._backend.stop()

    def stats_snapshot(self):
        """Return a stats snapshot if the backend supports it."""
        fn = getattr(self._backend, "stats_snapshot", None)
        if callable(fn):
            return fn()
        return None


@dataclass
class AsyncListenerHandle:
    """Async listener handle."""

    _backend: Any

    async def start(self) -> None:
        """Start the async listener."""
        await self._backend.start()

    async def stop(self) -> None:
        """Stop the async listener."""
        await self._backend.stop()

    def stats_snapshot(self):
        """Return a stats snapshot if the backend supports it."""
        fn = getattr(self._backend, "stats_snapshot", None)
        if callable(fn):
            return fn()
        return None


class RabbitMQClient:
    """Facade that creates publishers/listeners from a shared configuration.

    Provide either:
    - connection_params (dict)
    - or connection_factory and message_type_map
    """

    def __init__(
            self,
            *,
            connection_params: Optional[Dict[str, Any]] = None,
            message_type_map: Optional[Dict[str, Any]] = None,
            connection_factory: Optional[Callable[[], Any]] = None,
            async_connection_factory: Optional[Callable[[], Awaitable[Any]]] = None,
            verbose: bool = False,
    ):
        self._connection_params = connection_params
        self._message_type_map = message_type_map
        self._connection_factory = connection_factory
        self._async_connection_factory = async_connection_factory
        self._verbose = verbose

        # cache pooled publishers per message_type
        self._sync_pools: Dict[str, PublisherPool] = {}
        self._async_pools: Dict[str, Any] = {}

    def publisher(
            self, message_type: str, *, backend: str = "pool", retry_policy: Optional[RetryPolicy] = None,
            **kwargs) -> SyncPublisher:
        """Create a sync publisher.

        Args:
            retry_policy: Optional reconnect/backoff policy for pooled sync publishers (passed through to
            SyncPublishBackend).

        backend:
          - "pool" (default): PublisherPool, recommended
          - "lightweight_sender": pool-backed sender that avoids per-instance threads
        """
        if retry_policy is not None:
            kwargs.setdefault("reconnect_retry_policy", retry_policy)

        # Use PublisherPool as the unified pooling backend.
        if message_type not in self._sync_pools:
            self._sync_pools[message_type] = PublisherPool(
                message_type=message_type,
                verbose=self._verbose,
                connection_factory=self._connection_factory,
                connection_params=self._connection_params,
                message_type_map=self._message_type_map,
                **kwargs,
            )

        if backend == "pool":
            return SyncPublisher(self._sync_pools[message_type])

        if backend == "lightweight_sender":
            pool = self._sync_pools[message_type]

            class _LightweightSender:
                """Pool-backed sync sender: no per-instance threads."""

                def start(self) -> None:
                    pool.start()

                def send(self, message: Any, routing_key: Optional[str] = None) -> None:
                    pool.send(message, routing_key=routing_key)

                def stop(self) -> None:
                    # Do not stop the shared pool from a single lightweight handle.
                    return None

            return SyncPublisher(_LightweightSender())

        raise ValueError(f"Unknown sync publisher backend: {backend!r}")

    def listener(
            self, message_type: str, callback, *, connect_retry_policy: Optional[RetryPolicy] = None,
            loop_retry_policy: Optional[RetryPolicy] = None, **kwargs) -> SyncListenerHandle:
        if connect_retry_policy is not None:
            kwargs.setdefault("connect_retry_policy", connect_retry_policy)
        if loop_retry_policy is not None:
            kwargs.setdefault("loop_retry_policy", loop_retry_policy)

        listener = Listener(
            message_type=message_type,
            callback=callback,
            verbose=self._verbose,
            connection_factory=self._connection_factory,
            connection_params=self._connection_params,
            message_type_map=self._message_type_map,
            **kwargs,
        )
        return SyncListenerHandle(listener)

    def async_publisher(
            self, message_type: str, *, backend: str = "sender", retry_policy: Optional[RetryPolicy] = None,
            **kwargs) -> AsyncPublisher:
        """Create an async publisher.

        backend:
          - "sender" (default): AsyncSender
        """
        if retry_policy is not None:
            kwargs.setdefault("retry_policy", retry_policy)

        if AsyncSender is None:
            raise RuntimeError("Async support not installed. Install with: pip install rabbit-mq-client[async]")

        if backend != "sender":
            raise ValueError(f"Unknown async publisher backend: {backend!r}")

        sender = AsyncSender(
            message_type=message_type,
            verbose=self._verbose,
            connection_factory=self._async_connection_factory,
            connection_params=self._connection_params,
            message_type_map=self._message_type_map,
            **kwargs,
        )
        return AsyncPublisher(sender)

    def async_listener(self, message_type: str, callback, **kwargs) -> AsyncListenerHandle:
        if AsyncListener is None:
            raise RuntimeError("Async support not installed. Install with: pip install rabbit-mq-client[async]")

        listener = AsyncListener(
            message_type=message_type,
            callback=callback,
            verbose=self._verbose,
            connection_factory=self._async_connection_factory,
            connection_params=self._connection_params,
            message_type_map=self._message_type_map,
            **kwargs,
        )
        return AsyncListenerHandle(listener)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()
        return False

    def close(self) -> None:
        """Stop cached sync pools.

        Raises:
            CloseError: if one or more pools fail to stop.
        """
        logger = logging.getLogger(self.__class__.__name__)
        errors: list[BaseException] = []
        for pool in self._sync_pools.values():
            try:
                pool.stop()
            except Exception as e:
                logger.exception("Failed to stop sync pool")
                errors.append(e)
        self._sync_pools.clear()
        raise_if_errors(errors)

    async def aclose(self) -> None:
        """Stop cached async pools.

        Raises:
            CloseError: if one or more pools fail to stop.
        """
        logger = logging.getLogger(self.__class__.__name__)
        errors: list[BaseException] = []
        for pool in self._async_pools.values():
            try:
                await pool.stop()
            except Exception as e:
                logger.exception("Failed to stop async pool")
                errors.append(e)
        self._async_pools.clear()
        raise_if_errors(errors)


def in_asyncio_context() -> bool:
    """Return True if called with a running event loop."""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False

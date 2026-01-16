"""Async RabbitMQ adapters built on aio-pika.

This module provides asyncio-based equivalents of the threaded pika `Sender` and `Listener`.

Design goals:
- Keep the same `message_type` + `message_type_map` schema used by the sync classes.
- Be efficient for 100s of producers/consumers on the same host by using a single event loop.
- Prefer robust connections and reconnection handled by aio-pika.

`message_type_map[message_type]` keys expected (same as sync code):
- exchange_name (str)
- exchange_type (str) e.g. direct/topic/fanout/headers
- routing_key (str) (optional depending on exchange type)
- predefined_queue_name (str) (optional)

`Listener` behavior is mirrored:
- predefined_queue=True uses predefined_queue_name if provided; otherwise an exclusive auto-delete queue is created.
- auto_ack=True calls the user callback and does not ack/nack.
- auto_ack=False will ack on successful callback, and nack(requeue=True) on exception.

The user callback signature stays: (ch, method, properties, body).
For async we pass:
- ch: aio-pika channel
- method: aio-pika IncomingMessage (also contains .delivery_tag)
- properties: aio-pika message properties (IncomingMessage.properties)
- body: bytes
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, Union, TYPE_CHECKING

from mq_adapters.dead_lettering import resolve_dead_lettering_config, retry_attempt_from_headers
from mq_adapters.helper_functions import default_serializer
from mq_adapters.retry_policy import RetryPolicy, async_retry

if TYPE_CHECKING:  # pragma: no cover
    import aio_pika
    from aio_pika.abc import AbstractRobustConnection, AbstractIncomingMessage
    # Stubs for channels/exchanges return AbstractChannel/AbstractExchange; treat these as Any for baseline.
    AbstractRobustChannel = Any
    AbstractRobustExchange = Any
else:  # runtime: keep import optional
    aio_pika = None  # type: ignore
    AbstractRobustChannel = Any
    AbstractRobustConnection = Any
    AbstractIncomingMessage = Any
    AbstractRobustExchange = Any

# Type alias for an async connection factory.
AsyncConnectionFactory = Callable[[], Awaitable[Any]]

MessageBody = Union[bytes, str, Dict[str, Any]]


def _require_aio_pika() -> None:
    if aio_pika is None:
        # lazy import so the package can be imported without aio-pika installed
        try:
            import aio_pika as _aio_pika
        except Exception as e:  # pragma: no cover
            raise ImportError(
                "aio-pika is not installed. Install with: pip install rabbit-mq-client[async]"
            ) from e
        globals()["aio_pika"] = _aio_pika


def _resolve_message_type_map(
        *, message_type_map: Optional[Dict[str, Any]], connection_params: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    mtm = message_type_map or (connection_params.get("message_types") if connection_params else None)
    if not mtm:
        raise ValueError(
            'message_type_map must be provided either via message_type_map argument or connection_params['
            '"message_types"]'
        )
    return mtm


async def default_async_connection_factory(connection_params: Dict[str, Any]) -> "AbstractRobustConnection":
    """Create an aio-pika robust connection from the same connection_params dict used by sync adapters."""
    _require_aio_pika()

    required = ["server", "port", "username", "password", "vhost"]
    missing = [k for k in required if k not in connection_params]
    if missing:
        raise ValueError(f"Missing required connection params: {missing}")

    host = connection_params["server"]
    port = int(connection_params["port"])
    login = connection_params["username"]
    password = connection_params["password"]
    vhost = connection_params["vhost"]

    # Optional aio-pika kwargs we allow passing-through (safe subset)
    kwargs: Dict[str, Any] = {}
    if "heartbeat" in connection_params:
        kwargs["heartbeat"] = int(connection_params["heartbeat"])
    if "connection_timeout" in connection_params:
        kwargs["timeout"] = int(connection_params["connection_timeout"])

    return await aio_pika.connect_robust(
        host=host, port=port, login=login, password=password, virtualhost=vhost, **kwargs
    )


class AsyncSender:
    """Async publisher using aio-pika."""

    def __init__(
            self,
            message_type: str,
            *,
            verbose: bool = False,
            confirm_delivery: bool = False,
            connection_params: Optional[Dict[str, Any]] = None,
            message_type_map: Optional[Dict[str, Any]] = None,
            connection_factory: Optional[AsyncConnectionFactory] = None,
            retry_policy: Optional[RetryPolicy] = None,
            confirm_batch_size: int = 0,
            confirm_flush_interval: float = 0.0,
            serializer: Optional[Callable[[MessageBody], bytes]] = None,
    ):
        _require_aio_pika()

        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if verbose else logging.INFO)

        self._message_type = message_type
        self._message_type_map = _resolve_message_type_map(
            message_type_map=message_type_map, connection_params=connection_params
        )
        self._destination = self._message_type_map[self._message_type]

        self._connection_params = connection_params
        self._connection_factory = connection_factory

        self._confirm_delivery = confirm_delivery
        self._retry_policy = retry_policy or RetryPolicy()

        # confirm batching: if enabled, we gather publish awaitables and flush periodically
        self._confirm_batch_size = max(0, int(confirm_batch_size))
        self._confirm_flush_interval = max(0.0, float(confirm_flush_interval))
        self._pending_confirms: list[asyncio.Future] = []
        self._confirm_flush_task: Optional[asyncio.Task] = None

        self._conn: Optional["AbstractRobustConnection"] = None
        self._channel: Optional[Any] = None
        self._exchange: Optional[Any] = None

        self._serializer: Callable[[MessageBody], bytes] = serializer or default_serializer

    async def start(self) -> None:
        """Connect and declare exchange/channel."""
        if self._conn and not getattr(self._conn, "is_closed", True):
            return

        async def _do_start() -> None:
            if self._connection_factory is not None:
                self._conn = await self._connection_factory()
            else:
                if not self._connection_params:
                    raise ValueError("connection_params is required when no connection_factory is provided")
                self._conn = await default_async_connection_factory(self._connection_params)

            self._channel = await self._conn.channel(publisher_confirms=self._confirm_delivery)

            exchange_name = self._destination.get("exchange_name")
            exchange_type = self._destination.get("exchange_type")
            exchange_durable = bool(self._destination.get("exchange_durable", True))
            exchange_auto_delete = bool(self._destination.get("exchange_auto_delete", False))

            assert self._channel is not None
            self._exchange = await self._channel.declare_exchange(
                name=exchange_name,
                type=exchange_type,
                durable=exchange_durable,
                auto_delete=exchange_auto_delete,
            )

            # Start flusher if batched confirms enabled
            if self._confirm_delivery and self._confirm_flush_interval > 0 and self._confirm_batch_size > 0:
                if self._confirm_flush_task is None or self._confirm_flush_task.done():
                    self._confirm_flush_task = asyncio.create_task(self._flush_confirms_periodically())

        await async_retry(_do_start, retry_policy=self._retry_policy, logger=self._logger, operation="AsyncSender.start")

    async def _flush_confirms_periodically(self) -> None:
        while self._conn and not getattr(self._conn, "is_closed", True):
            await asyncio.sleep(self._confirm_flush_interval)
            try:
                await self._flush_confirms()
            except Exception:
                # Best-effort; the send path will also flush by size.
                self._logger.exception("Exception while flushing publisher confirms")

    async def _flush_confirms(self) -> None:
        if not self._pending_confirms:
            return
        pending = self._pending_confirms
        self._pending_confirms = []
        await asyncio.gather(*pending, return_exceptions=False)

    async def send(self, message: MessageBody, routing_key: Optional[str] = None) -> None:
        """Publish one message."""
        _require_aio_pika()

        body = self._serializer(message)

        async def _do_publish() -> None:
            if not self._channel or not self._exchange or not self._conn or getattr(self._conn, "is_closed", True):
                await self.start()

            rk = routing_key or self._destination.get("routing_key") or ""
            assert self._exchange is not None
            publish_awaitable = self._exchange.publish(aio_pika.Message(body=body), routing_key=rk)

            if self._confirm_delivery and self._confirm_batch_size > 0:
                # batch confirms: enqueue and flush when full
                fut = asyncio.ensure_future(publish_awaitable)
                self._pending_confirms.append(fut)
                if len(self._pending_confirms) >= self._confirm_batch_size:
                    await self._flush_confirms()
            else:
                await publish_awaitable

        try:
            await async_retry(_do_publish, retry_policy=self._retry_policy, logger=self._logger, operation="AsyncSender.send")
        except Exception:
            # Reset cached connection/channel so subsequent calls reconnect
            self._channel = None
            self._exchange = None
            self._conn = None
            raise

    async def stop(self) -> None:
        """Close channel and connection."""
        try:
            # flush any outstanding confirms
            try:
                await self._flush_confirms()
            except Exception:
                pass

            if self._confirm_flush_task is not None:
                self._confirm_flush_task.cancel()
                self._confirm_flush_task = None

            if self._channel and not getattr(self._channel, "is_closed", True):
                await self._channel.close()
        finally:
            self._channel = None
            self._exchange = None
            if self._conn and not getattr(self._conn, "is_closed", True):
                await self._conn.close()
            self._conn = None

    async def aclose(self) -> None:
        """Alias for stop()."""
        await self.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
        return False


class AsyncListener:
    """Async consumer using aio-pika.

    Acknowledgements (`auto_ack`):
        - `auto_ack=True` (legacy): **at-most-once** delivery. Messages are considered acked on delivery.
        - `auto_ack=False` (default): **at-least-once** delivery. The listener acks after your callback completes; on exception it nacks with requeue=True.

    For `auto_ack=False`, callbacks should be idempotent (messages can be redelivered).
    """

    def __init__(
            self,
            message_type: str,
            callback: Callable[[Any, Any, Any, bytes], Union[None, Awaitable[None]]],
            *,
            predefined_queue: bool = False,
            prefetch_count: int = -1,
            verbose: bool = False,
            auto_ack: bool = False,
            connection_params: Optional[Dict[str, Any]] = None,
            message_type_map: Optional[Dict[str, Any]] = None,
            connection_factory: Optional[AsyncConnectionFactory] = None,
            max_concurrency: Optional[int] = None,
            shutdown_timeout: float = 10.0,
    ):
        _require_aio_pika()

        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG if verbose else logging.INFO)

        self._message_type = message_type
        self._message_type_map = _resolve_message_type_map(
            message_type_map=message_type_map, connection_params=connection_params
        )
        self._destination = self._message_type_map[self._message_type]

        self._connection_params = connection_params
        self._connection_factory = connection_factory

        self._callback = callback
        self._predefined_queue = predefined_queue
        self._prefetch_count = prefetch_count
        self._auto_ack = auto_ack

        if self._auto_ack:
            self._logger.warning(
                "AsyncListener auto_ack=True => at-most-once delivery (no redelivery on crash/exception). "
                "Use auto_ack=False for at-least-once semantics."
            )

        # Concurrency control: if not specified, default to prefetch_count (when set) else 100.
        if max_concurrency is None:
            self._max_concurrency = int(prefetch_count) if (prefetch_count and prefetch_count > -1) else 100
        else:
            self._max_concurrency = max(1, int(max_concurrency))
        self._shutdown_timeout = float(shutdown_timeout)

        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight: set[asyncio.Task] = set()

        self._conn: Optional["AbstractRobustConnection"] = None
        self._channel: Optional[Any] = None
        self._queue: Any = None
        self._consumer_tag: Optional[str] = None

    async def start(self) -> None:
        """Connect, declare exchange/queue/bind, and start consuming."""
        if self._conn and not getattr(self._conn, "is_closed", True):
            return

        if self._connection_factory is not None:
            self._conn = await self._connection_factory()
        else:
            if not self._connection_params:
                raise ValueError("connection_params is required when no connection_factory is provided")
            self._conn = await default_async_connection_factory(self._connection_params)

        self._channel = await self._conn.channel()
        assert self._channel is not None

        if self._prefetch_count and self._prefetch_count > -1:
            await self._channel.set_qos(prefetch_count=int(self._prefetch_count))

        exchange_name = self._destination.get("exchange_name")
        exchange_type = self._destination.get("exchange_type")
        routing_key = self._destination.get("routing_key") or ""

        exchange_durable = bool(self._destination.get("exchange_durable", True))
        exchange_auto_delete = bool(self._destination.get("exchange_auto_delete", False))

        exchange = await self._channel.declare_exchange(
            name=exchange_name,
            type=exchange_type,
            durable=exchange_durable,
            auto_delete=exchange_auto_delete,
        )

        predefined_queue_name = self._destination.get("predefined_queue_name")
        queue_durable = bool(self._destination.get("queue_durable", True))
        queue_auto_delete = bool(self._destination.get("queue_auto_delete", False))

        if self._predefined_queue and predefined_queue_name:
            # If you redeclare an existing queue in RabbitMQ, the arguments must be equivalent.
            # Allow users to provide optional queue arguments (e.g., x-message-ttl, dead-lettering)
            # via the same message_type mapping used by the sync adapters.
            queue_arguments = (
                    self._destination.get("queue_arguments")
                    or self._destination.get("queue_args")
                    or self._destination.get("queue_declare_arguments")
            )
            q = await self._channel.declare_queue(
                predefined_queue_name,
                durable=queue_durable,
                auto_delete=queue_auto_delete,
                arguments=queue_arguments,
            )
        else:
            q = await self._channel.declare_queue("", exclusive=True, auto_delete=True)

        await q.bind(exchange, routing_key=routing_key)

        self._queue = q

        # semaphore created after we have an event loop
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        cfg = resolve_dead_lettering_config(self._destination)

        async def _run_handler(message: "AbstractIncomingMessage") -> None:
            assert self._semaphore is not None
            await self._semaphore.acquire()
            try:
                body = message.body
                result = self._callback(self._channel, message, getattr(message, "properties", None), body)
                if asyncio.iscoroutine(result):
                    await result

                if not self._auto_ack:
                    await message.ack()
            except Exception:
                self._logger.exception("Listener callback raised")
                if not self._auto_ack:
                    try:
                        headers = None
                        try:
                            headers = getattr(message, "headers", None)
                        except Exception:
                            headers = None
                        attempt = retry_attempt_from_headers(headers)

                        # poison: publish to DLQ then ack
                        if cfg.dead_letter_queue and attempt >= int(cfg.max_retries):
                            try:
                                # Preserve headers/properties best-effort.
                                body_to_dlq = message.body
                                msg_props = getattr(message, "properties", None)
                                out_headers = getattr(message, "headers", None)

                                app_id = getattr(msg_props, "app_id", None) if msg_props is not None else None
                                content_type = getattr(msg_props, "content_type", None) if msg_props is not None else None
                                content_encoding = getattr(msg_props, "content_encoding", None) if msg_props is not None else None
                                correlation_id = getattr(msg_props, "correlation_id", None) if msg_props is not None else None
                                reply_to = getattr(msg_props, "reply_to", None) if msg_props is not None else None
                                expiration = getattr(msg_props, "expiration", None) if msg_props is not None else None
                                message_id = getattr(msg_props, "message_id", None) if msg_props is not None else None
                                timestamp = getattr(msg_props, "timestamp", None) if msg_props is not None else None
                                type_ = getattr(msg_props, "type", None) if msg_props is not None else None
                                user_id = getattr(msg_props, "user_id", None) if msg_props is not None else None
                                priority = getattr(msg_props, "priority", None) if msg_props is not None else None
                                delivery_mode = getattr(msg_props, "delivery_mode", None) if msg_props is not None else None

                                # publish via default exchange to the DLQ queue name
                                assert self._channel is not None
                                await self._channel.default_exchange.publish(
                                    aio_pika.Message(
                                        body=body_to_dlq,
                                        headers=out_headers,
                                        app_id=app_id,
                                        content_type=content_type,
                                        content_encoding=content_encoding,
                                        correlation_id=correlation_id,
                                        reply_to=reply_to,
                                        expiration=expiration,
                                        message_id=message_id,
                                        timestamp=timestamp,
                                        type=type_,
                                        user_id=user_id,
                                        priority=priority,
                                        delivery_mode=delivery_mode,
                                    ),
                                    routing_key=cfg.dead_letter_queue,
                                )
                                await message.ack()
                                return
                            except Exception:
                                self._logger.exception("Failed to publish poison message to dead_letter_queue; falling back")

                        # non-poison: nack
                        if cfg.on_error == "dead_letter":
                            await message.nack(requeue=False)
                        else:
                            await message.nack(requeue=True)
                    except Exception:
                        self._logger.exception("Failed to nack message")
            finally:
                try:
                    self._semaphore.release()
                except Exception:
                    pass

        async def _on_message(message: "AbstractIncomingMessage") -> None:
            # Keep the consumer callback fast: schedule work and return.
            task = asyncio.create_task(_run_handler(message))
            self._in_flight.add(task)

            def _discard(_t: asyncio.Task) -> None:
                self._in_flight.discard(_t)

            task.add_done_callback(_discard)

        self._consumer_tag = await q.consume(_on_message, no_ack=self._auto_ack)

    async def stop(self) -> None:
        """Stop consuming and close resources."""
        try:
            if self._queue and self._consumer_tag:
                try:
                    await self._queue.cancel(self._consumer_tag)
                except Exception:
                    pass

            # Wait for in-flight callbacks to finish (best effort)
            if self._in_flight:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*list(self._in_flight), return_exceptions=True),
                        timeout=self._shutdown_timeout,
                    )
                except Exception:
                    pass
        finally:
            self._consumer_tag = None
            self._in_flight.clear()
            self._semaphore = None
            if self._channel and not getattr(self._channel, "is_closed", True):
                try:
                    await self._channel.close()
                except Exception:
                    pass
            self._channel = None
            if self._conn and not getattr(self._conn, "is_closed", True):
                try:
                    await self._conn.close()
                except Exception:
                    pass
            self._conn = None

    async def aclose(self) -> None:
        """Alias for stop()."""
        await self.stop()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
        return False


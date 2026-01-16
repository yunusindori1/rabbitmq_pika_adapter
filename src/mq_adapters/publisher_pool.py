"""
Publisher pool: provides a lightweight pool of publisher worker threads.

Each worker opens its own RabbitMQ connection + channel and reads messages from a shared queue to publish.
This reduces the number of connections when many logical producers exist on the same host.

Message payloads:
- bytes: published as-is
- str: encoded as UTF-8
- dict: JSON-serialized to UTF-8 bytes

Usage:
    pool = PublisherPool(message_type='binance_kline', num_workers=4)
    pool.start()
    pool.send(b'msg1')
    pool.stop()
"""

import logging
from typing import Any, Callable, Dict, Optional, Literal

from mq_adapters.helper_functions import (
    MessageBody,
    default_serializer,
    make_connection_factory_from_params,
    load_connection_params_from_env,
)
from mq_adapters.stats import Stats, ensure_stats
from mq_adapters.sync_publisher_backend import SyncPublishBackend


class PublisherPool:
    """A small pool of sync publisher worker threads.

    This is the recommended sync publisher for high-throughput scenarios with many producers.

    Implementation note:
        Pika BlockingConnection/channels are not safe for concurrent multi-threaded use.
        To achieve connection + channel pooling safely, this class now uses a *single dedicated I/O thread*
        that owns one connection and a small pool of channels.

        `num_workers` is therefore interpreted as the number of channels to create on that connection.
    """

    def __init__(
            self,
            message_type: str,
            num_workers: int = 4,
            verbose: bool = False,
            confirm_delivery: bool = False,
            connection_factory: Optional[Callable[[], Any]] = None,
            connection_params: Optional[Dict[str, Any]] = None,
            message_type_map: Optional[Dict[str, Any]] = None,
            queue_maxsize: int = 0,
            serializer: Optional[Callable[[MessageBody], bytes]] = None,
            reconnect_retry_policy: Optional[Any] = None,
            stats: Optional[Stats] = None,
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        if verbose:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.INFO)

        # Resolve message type mapping
        self._message_type = message_type
        self._message_type_map = message_type_map or (
            connection_params.get('message_types') if connection_params else None)
        if not self._message_type_map:
            # attempt to load from env (no message_type_map in env expected) -> raise helpful error
            raise ValueError(
                'message_type_map must be provided either via connection_params["message_types"] or message_type_map '
                'argument')
        self._destination = self._message_type_map[self._message_type]

        # Resolve connection factory
        if connection_factory and callable(connection_factory):
            self._connection_factory = connection_factory
        elif connection_params:
            self._connection_factory = make_connection_factory_from_params(connection_params)
        else:
            env_params = load_connection_params_from_env()
            if env_params:
                self._connection_factory = make_connection_factory_from_params(env_params)
            else:
                raise ValueError(
                    'No connection configuration provided. Pass connection_factory or connection_params, '
                    'or set RABBITMQ_* env vars')

        self._num_workers = max(1, int(num_workers))
        self._confirm_delivery = confirm_delivery
        self._serializer: Callable[[MessageBody], bytes] = serializer or default_serializer

        # Backend owns queue + connection/channels in a dedicated thread.
        exchange_name = self._destination.get('exchange_name')
        routing_key = self._destination.get('routing_key') or ''
        self._stats = ensure_stats(stats)

        self._backend = SyncPublishBackend(
            connection_factory=self._connection_factory,
            exchange=exchange_name,
            default_routing_key=routing_key,
            channels=self._num_workers,
            confirm_delivery=self._confirm_delivery,
            queue_maxsize=queue_maxsize,
            logger=self._logger,
            reconnect_retry_policy=reconnect_retry_policy,
            stats=self._stats,
        )

    def start(self) -> None:
        """Start publisher backend thread."""
        self._backend.start()

    def send(self, message: MessageBody, routing_key: Optional[str] = None) -> None:
        """Enqueue a message for publishing.

        Args:
            message: bytes/str/dict. dict is JSON-serialized.
            routing_key: Optional routing key override.
        """
        payload = self._serializer(message)
        self._backend.publish(payload, routing_key=routing_key)

    def stop(self, wait: bool = True) -> None:
        """Stop publisher backend."""
        self._backend.stop(wait=wait)

    def close(self, wait: bool = True) -> None:
        """Alias for stop()."""
        return self.stop(wait=wait)

    def stats_snapshot(self):
        """Return a snapshot of in-process counters for this pool."""
        return self._stats.snapshot()

    def __enter__(self) -> "PublisherPool":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        self.stop(wait=True)
        return False

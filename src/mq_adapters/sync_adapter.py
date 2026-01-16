"""
Sync RabbitMQ adapters built on pika.BlockingConnection.

This module provides a primary thread-based helper:
- Listener: consume messages for a message_type

Publishing is available via `mq_adapters.publisher_pool.PublisherPool`.
"""

# noinspection DuplicatedCode
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional, Literal

import pika

from mq_adapters.ApplicationThread import ApplicationThread
from mq_adapters.dead_lettering import (
    resolve_dead_lettering_config,
    resolve_queue_arguments,
    retry_attempt_from_headers,
)
from mq_adapters.helper_functions import (
    MessageTypeMap,
    SyncMessageCallback,
    make_connection_factory_from_params,
    load_connection_params_from_env,
)
from mq_adapters.log import log_received, maybe_repr_bytes
from mq_adapters.retry_policy import RetryPolicy, sync_retry
from mq_adapters.stats import Stats, ensure_stats


class Listener(ApplicationThread):
    """Thread-based consumer for a given message_type.

    Acknowledgements (`auto_ack`):
        - `auto_ack=True` (default, legacy): **at-most-once** delivery. Messages are considered acked on delivery.
          Callback exceptions will be logged, but they will NOT trigger redelivery.
        - `auto_ack=False`: **at-least-once** delivery. The library will ack after your callback returns successfully.
          If your callback raises, the library will nack with `requeue=True`.

    Key behavior:
    - If offload=True: callback is executed in a ThreadPoolExecutor to keep the pika consumer thread responsive.

    Args:
        message_type: Key in `message_type_map`.
        callback: User callback invoked with (ch, method, properties, body).
        predefined_queue: If True, use predefined_queue_name if present.
        prefetch_size/prefetch_count: Passed to basic_qos when both provided.
        auto_ack: Whether to auto-ack on delivery. Prefer False for at-least-once processing.
        connection_factory/connection_params: Connection configuration.
        message_type_map: Mapping from message_type to destination details.
        offload: If True, opt into callback offloading via ThreadPoolExecutor.
        max_workers/max_in_flight: Offload concurrency controls.
        shutdown_timeout: How long to wait for offloaded callbacks during __exit__.
    """

    def __init__(
            self,
            message_type: str,
            callback: SyncMessageCallback,
            predefined_queue: bool = False,
            prefetch_size: int = -1,
            prefetch_count: int = -1,
            apply_prefetch_to_all_channels: bool = False,
            verbose: bool = False,
            auto_ack: bool = True,
            connection_factory: Optional[Callable[[], Any]] = None,
            connection_params: Optional[Dict[str, Any]] = None,
            message_type_map: Optional[MessageTypeMap] = None,
            offload: bool = False,
            max_workers: Optional[int] = None,
            max_in_flight: Optional[int] = None,
            shutdown_timeout: float = 10.0,
            io_pump_time_limit: float = 0.25,
            connect_retry_policy: Optional[RetryPolicy] = None,
            loop_retry_policy: Optional[RetryPolicy] = None,
            stats: Optional[Stats] = None,
    ):
        """
        Provides convenience while reading messages from a rabbit mq queue.
        :param message_type: One of the keys in message_type_to_destination
        :param callback: The callback function is called with the params: channel, method, properties, body,
        where body is bytes.
        :param predefined_queue:
        :param auto_ack: Whether to auto-ack messages on delivery (default True to preserve backwards compatibility)
        :param connection_factory: Optional callable that returns a pika.BlockingConnection when called.
        :param connection_params: Optional dict containing required connection values (server, port, username,
        password, vhost)
        :param message_type_map: Optional dict mapping message_type -> destination details. If omitted, message_types
        key in
               connection_params will be used if present.
        """
        super(Listener, self).__init__()
        self.__logger = logging.getLogger(__name__)
        # Do not override global logger levels; respect application logging configuration.
        self.__stats = ensure_stats(stats)

        # Resolve message type mapping (prefer explicit map, then connection_params['message_types'])
        self.__message_type = message_type
        self.__message_type_map = message_type_map or (
            connection_params.get('message_types') if connection_params else None)
        if not self.__message_type_map:
            raise ValueError(
                'message_type_map must be provided either via message_type_map argument or connection_params['
                '"message_types"]')
        self.__mq_destination = self.__message_type_map[self.__message_type]

        # Resolve connection factory
        if connection_factory and callable(connection_factory):
            self.__connection_factory = connection_factory
        elif connection_params:
            self.__connection_factory = make_connection_factory_from_params(connection_params)
        else:
            env_params = load_connection_params_from_env()
            if env_params:
                self.__connection_factory = make_connection_factory_from_params(env_params)
            else:
                raise ValueError(
                    'No connection configuration provided. Pass connection_factory or connection_params, '
                    'or set RABBITMQ_* env vars')

        self.__listener = callback
        self.__connection = None
        self.__channel = None
        self.__running = False
        self.__predefined_queue = predefined_queue
        self.__prefetch_settings = None
        self.__auto_ack = auto_ack

        if self.__auto_ack:
            # One-time nudge: auto-ack implies at-most-once delivery.
            self.__logger.warning(
                "Listener auto_ack=True => at-most-once delivery (no redelivery on crash/exception). "
                "Use auto_ack=False for at-least-once semantics."
            )

        # Optional callback offload to a bounded worker pool to keep pika's consumer thread responsive.
        self.__offload = bool(offload)
        self.__shutdown_timeout = float(shutdown_timeout)
        self.__executor: Optional[ThreadPoolExecutor] = None
        self.__in_flight_sema: Optional[threading.Semaphore] = None

        if self.__offload:
            workers = max(1, int(max_workers) if max_workers is not None else 10)
            in_flight = max(1, int(max_in_flight) if max_in_flight is not None else workers)
            self.__executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mq_listener")
            self.__in_flight_sema = threading.Semaphore(in_flight)
            self.__logger.info(
                "Listener offload enabled: max_workers=%d max_in_flight=%d", workers, in_flight
            )

            # If user didn't set prefetch_count, default it to max_in_flight to avoid unbounded delivery.
            if prefetch_size <= -1 and prefetch_count <= -1:
                prefetch_size = 0
                prefetch_count = in_flight

        # Preserve prior behaviour: only apply qos if both set.
        if prefetch_size > -1 and prefetch_count > -1:
            self.__prefetch_settings = {'prefetch_size': prefetch_size, 'prefetch_count': prefetch_count,
                                        'all_channels': apply_prefetch_to_all_channels}
        self.__io_pump_time_limit = max(0.0, float(io_pump_time_limit))

        # Retry policies
        self.__connect_retry_policy = connect_retry_policy or RetryPolicy(
            max_attempts=None,
            base_delay=1.0,
            max_delay=30.0,
            multiplier=2.0,
            jitter=0.25,
        )
        # Used for transient errors in the main loop (process_data_events failures).
        self.__loop_retry_policy = loop_retry_policy or RetryPolicy(
            max_attempts=None,
            base_delay=0.5,
            max_delay=10.0,
            multiplier=2.0,
            jitter=0.25,
        )

        self.__loop_failures = 0

    def stats_snapshot(self):
        """Return a snapshot of in-process counters for this listener."""
        return self.__stats.snapshot()

    def _offload_wrapper(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """Offload the callback to worker pool; schedule ack/nack thread-safely when auto_ack=False."""
        if not self.__executor or not self.__in_flight_sema:
            return self._internal_wrapper(ch, method, properties, body)

        if self.stop_me:
            return

        # Count delivery as soon as we accept it for processing.
        self.__stats.inc("received")
        log_received(self.__logger, maybe_repr_bytes(body), self.__mq_destination)

        acquired = False
        try:
            self.__in_flight_sema.acquire()
            acquired = True

            def _run_user_callback():
                return self.__listener(ch, method, properties, body)

            fut = self.__executor.submit(_run_user_callback)

            def _on_done(_f):
                try:
                    exc = _f.exception()
                    if exc is not None:
                        self.__stats.inc("handler_exceptions")

                    if self.__auto_ack:
                        return

                    delivery_tag = getattr(method, 'delivery_tag', None)
                    if delivery_tag is None:
                        return

                    if exc is None:
                        def _ack():
                            try:
                                ch.basic_ack(delivery_tag=delivery_tag)
                                self.__stats.inc("acked")
                            except Exception:
                                self.__logger.exception('Failed to ack offloaded message')

                        _schedule = _ack
                    else:
                        cfg = resolve_dead_lettering_config(self.__mq_destination)
                        headers = None
                        try:
                            headers = getattr(properties, 'headers', None) if properties is not None else None
                        except Exception:
                            headers = None
                        attempt = retry_attempt_from_headers(headers)

                        def _handle_fail():
                            try:
                                # poison -> publish to DLQ + ack
                                if cfg.dead_letter_queue and attempt >= int(cfg.max_retries):
                                    dl_props = properties
                                    if properties is not None:
                                        try:
                                            dl_props = pika.BasicProperties(
                                                content_type=getattr(properties, 'content_type', None),
                                                content_encoding=getattr(properties, 'content_encoding', None),
                                                headers=getattr(properties, 'headers', None),
                                                delivery_mode=getattr(properties, 'delivery_mode', None),
                                                priority=getattr(properties, 'priority', None),
                                                correlation_id=getattr(properties, 'correlation_id', None),
                                                reply_to=getattr(properties, 'reply_to', None),
                                                expiration=getattr(properties, 'expiration', None),
                                                message_id=getattr(properties, 'message_id', None),
                                                timestamp=getattr(properties, 'timestamp', None),
                                                type=getattr(properties, 'type', None),
                                                user_id=getattr(properties, 'user_id', None),
                                                app_id=getattr(properties, 'app_id', None),
                                                cluster_id=getattr(properties, 'cluster_id', None),
                                            )
                                        except Exception:
                                            dl_props = properties
                                    ch.basic_publish(
                                        exchange='',
                                        routing_key=cfg.dead_letter_queue,
                                        body=body,
                                        properties=dl_props,
                                    )
                                    ch.basic_ack(delivery_tag=delivery_tag)
                                    self.__stats.inc("acked")
                                    return

                                # not poison -> nack
                                if cfg.on_error == 'dead_letter':
                                    ch.basic_nack(delivery_tag=delivery_tag, requeue=False)
                                else:
                                    ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
                                self.__stats.inc("nacked")
                            except Exception:
                                self.__logger.exception('Failed to handle offloaded message failure')

                        _schedule = _handle_fail

                    try:
                        if self.__connection and getattr(self.__connection, 'is_open', False):
                            self.__connection.add_callback_threadsafe(_schedule)
                        else:
                            _schedule()
                    except Exception:
                        self.__logger.exception('Failed to schedule ack/nack via add_callback_threadsafe')
                        try:
                            _schedule()
                        except Exception:
                            pass
                finally:
                    try:
                        self.__in_flight_sema.release()
                    except Exception:
                        pass

            fut.add_done_callback(_on_done)
        except Exception:
            self.__logger.exception('Listener offload wrapper failed; falling back to direct call')
            if acquired:
                try:
                    self.__in_flight_sema.release()
                except Exception:
                    pass
            return self._internal_wrapper(ch, method, properties, body)

    def _internal_wrapper(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """Direct (non-offloaded) wrapper for user callback; schedules ack/nack as configured."""
        if self.stop_me:
            return

        # Count delivery as soon as we accept it for processing.
        self.__stats.inc("received")
        log_received(self.__logger, maybe_repr_bytes(body), self.__mq_destination)

        try:
            self.__listener(ch, method, properties, body)
            if not self.__auto_ack:
                delivery_tag = getattr(method, 'delivery_tag', None)
                if delivery_tag is not None:
                    ch.basic_ack(delivery_tag=delivery_tag)
                    self.__stats.inc("acked")
        except Exception:
            self.__logger.exception('Error in user callback')
            if not self.__auto_ack:
                delivery_tag = getattr(method, 'delivery_tag', None)
                if delivery_tag is not None:
                    ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
                    self.__stats.inc("nacked")

    def _auto_ack_wrapper(self, ch: Any, method: Any, properties: Any, body: bytes) -> None:
        """Wrapper used for auto_ack=True mode.

        This preserves legacy at-most-once semantics (RabbitMQ considers the message acked on delivery),
        but ensures observability (received counters + debug logs) is consistent across modes.
        """
        if self.stop_me:
            return

        self.__stats.inc("received")
        log_received(self.__logger, maybe_repr_bytes(body), self.__mq_destination)
        # Preserve prior behavior: call user callback directly and let exceptions bubble.
        self.__listener(ch, method, properties, body)

    def _setup_channel_and_consumer(self) -> None:
        """(Re)declare exchange/queue/bind and register consumer callback on the current channel."""
        assert self.__connection is not None
        self.__channel = self.__connection.channel()

        # Apply QoS if provided
        if self.__prefetch_settings:
            try:
                self.__channel.basic_qos(
                    prefetch_size=self.__prefetch_settings.get('prefetch_size', 0),
                    prefetch_count=self.__prefetch_settings.get('prefetch_count', 0),
                )
            except Exception:
                self.__logger.exception('Failed to set basic_qos')

        # Declare exchange and queue/bind once per channel
        queue_arguments = resolve_queue_arguments(self.__mq_destination)

        if self.__predefined_queue and self.__mq_destination.get('predefined_queue_name'):
            queue_name = self.__mq_destination['predefined_queue_name']
            try:
                self.__channel.queue_declare(queue=queue_name, durable=True, arguments=queue_arguments or None)
            except Exception:
                self.__logger.exception('Failed to declare predefined queue')
        else:
            qd = self.__channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            queue_name = qd.method.queue

        try:
            self.__channel.exchange_declare(
                exchange=self.__mq_destination.get('exchange_name'),
                exchange_type=self.__mq_destination.get('exchange_type'),
                durable=True,
            )
        except Exception:
            self.__logger.exception('Failed to declare exchange')

        try:
            self.__channel.queue_bind(
                queue=queue_name,
                exchange=self.__mq_destination.get('exchange_name'),
                routing_key=self.__mq_destination.get('routing_key'),
            )
        except Exception:
            self.__logger.exception('Failed to bind queue')

        # Register consumer callback
        if self.__offload:
            self.__channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._offload_wrapper,
                auto_ack=self.__auto_ack,
            )
        else:
            if self.__auto_ack:
                self.__channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self._auto_ack_wrapper,
                    auto_ack=True,
                )
            else:
                self.__channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self._internal_wrapper,
                    auto_ack=False,
                )

    def run(self) -> None:
        """Run the listener thread (process_data_events loop)."""
        super(Listener, self).run()
        self.__running = True

        while not self.stop_me:
            try:
                if not self.__connection or not getattr(self.__connection, 'is_open', False):
                    self.__create_connection()

                assert self.__connection is not None

                if not self.__channel or not getattr(self.__channel, 'is_open', False):
                    self._setup_channel_and_consumer()

                try:
                    self.__connection.process_data_events(time_limit=self.__io_pump_time_limit)
                    self.__loop_failures = 0
                except Exception:
                    if not self.stop_me:
                        self.__logger.exception('process_data_events failed; reconnecting')
                        try:
                            if self.__channel is not None and getattr(self.__channel, 'is_open', False):
                                self.__channel.close()
                        except Exception:
                            pass
                        try:
                            if self.__connection is not None and getattr(self.__connection, 'is_open', False):
                                self.__connection.close()
                                self.__stats.inc('connections_closed')
                                self.__logger.info('Connection closed')
                        except Exception:
                            pass
                        self.__channel = None
                        self.__connection = None
                        self.__loop_failures += 1
                        self.__stats.inc('reconnect_attempts')
                        delay = self.__loop_retry_policy.delay_for_attempt(self.__loop_failures + 1)
                        self.__logger.info('Reconnect attempt %d; backing off %.2fs', self.__loop_failures, delay)
                        time.sleep(delay)
            except Exception:
                if not self.stop_me:
                    self.__logger.exception('Listener encountered an exception; reconnecting')
                    try:
                        if self.__channel is not None and getattr(self.__channel, 'is_open', False):
                            self.__channel.close()
                    except Exception:
                        pass
                    try:
                        if self.__connection is not None and getattr(self.__connection, 'is_open', False):
                            self.__connection.close()
                            self.__stats.inc('connections_closed')
                            self.__logger.info('Connection closed')
                    except Exception:
                        pass
                    self.__channel = None
                    self.__connection = None
                    self.__loop_failures += 1
                    self.__stats.inc('reconnect_attempts')
                    delay = self.__loop_retry_policy.delay_for_attempt(self.__loop_failures + 1)
                    self.__logger.info('Reconnect attempt %d; backing off %.2fs', self.__loop_failures, delay)
                    time.sleep(delay)

        print('Exited')

    def stop_listening(self) -> None:
        """Stop consuming and close channel/connection best-effort."""
        try:
            self.stop_me = True

            # Shutdown worker pool (best-effort). This doesn't own the pika thread.
            if self.__executor is not None:
                try:
                    self.__executor.shutdown(wait=False, cancel_futures=False)
                except Exception:
                    pass

            # Pika BlockingConnection and its channel are not thread-safe. If stop_listening() is
            # called from a different thread than the one running start_consuming(), calling
            # channel.stop_consuming() directly can race with the I/O loop and trigger transport errors
            # (e.g., "pop from an empty deque").
            if self.__connection and getattr(self.__connection, 'is_open', False):
                def _request_stop_and_close():
                    try:
                        if self.__channel and getattr(self.__channel, 'is_open', False):
                            try:
                                self.__channel.stop_consuming()
                            except Exception:
                                pass
                            try:
                                self.__channel.close()
                            except Exception:
                                pass
                        if self.__connection and getattr(self.__connection, 'is_open', False):
                            try:
                                self.__connection.close()
                                self.__stats.inc("connections_closed")
                                self.__logger.info("Connection closed")
                            except Exception:
                                pass
                    except Exception:
                        pass

                try:
                    # Schedule onto the connection's I/O thread.
                    self.__connection.add_callback_threadsafe(_request_stop_and_close)
                except Exception:
                    # Fallback: best-effort direct stop + close
                    try:
                        if self.__channel is not None and self.__channel.is_open:
                            self.__channel.stop_consuming()
                    except Exception:
                        pass
                    try:
                        if self.__channel is not None and getattr(self.__channel, 'is_open', False):
                            self.__channel.close()
                    except Exception:
                        pass
                    try:
                        if self.__connection is not None and getattr(self.__connection, 'is_open', False):
                            self.__connection.close()
                    except Exception:
                        pass
            else:
                # No usable connection; best-effort direct stop + close
                try:
                    if self.__channel is not None and self.__channel.is_open:
                        self.__channel.stop_consuming()
                except Exception:
                    pass
                try:
                    if self.__channel is not None and getattr(self.__channel, 'is_open', False):
                        self.__channel.close()
                except Exception:
                    pass
                try:
                    if self.__connection is not None and getattr(self.__connection, 'is_open', False):
                        self.__connection.close()
                except Exception:
                    pass
        except Exception as e:
            print(
                f'Exception caught during stopping listening on channel: {e}, might be ok if the channel is already '
                f'stopped')

    def __create_connection(self) -> None:
        """Create a connection with retry/backoff until stop_me is set."""

        def _connect_once() -> Any:
            self.__connection = self.__connection_factory()
            self.__stats.inc("connections_opened")
            self.__logger.info("Connection opened")
            return self.__connection

        sync_retry(
            _connect_once,
            retry_policy=self.__connect_retry_policy,
            stop_predicate=lambda: bool(self.stop_me),
            logger=self.__logger,
            operation="Listener.connect",
        )

    def close(self) -> None:
        """Alias for stop_listening() for compatibility with context management."""
        self.stop_listening()

    def __enter__(self) -> "Listener":
        self.start_listening()
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        self.stop_listening()
        # Wait briefly for offloaded callbacks to drain and then finalize executor.
        if self.__executor is not None:
            try:
                # The stdlib Executor.shutdown does not accept `timeout` in typeshed.
                self.__executor.shutdown(wait=True)
            except Exception:
                pass
        return False

    def start_listening(self) -> None:
        """Start the underlying thread if not already running."""
        if not self.__running:
            self.start()

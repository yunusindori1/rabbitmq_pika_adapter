"""Small helper utilities for rabbit-mq-client.

This module contains:
- connection factories and env var loaders for dependency injection
- small introspection helpers used for logging/debugging
- shared, lightweight type aliases for public APIs
"""

import inspect
import os
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

import pika

# Public typing helpers
ConnectionParams = Dict[str, Any]
MessageTypeMap = Dict[str, Any]
MessageBody = Union[bytes, str, Dict[str, Any]]

# Callback signature used by Listener/AsyncListener.
# We keep method/properties as Any because pika's types are not fully exposed/typed.
SyncMessageCallback = Callable[[Any, Any, Any, bytes], Any]


def default_serializer(message: MessageBody) -> bytes:
    """Serialize supported message bodies to bytes.

    Rules:
      - bytes: returned as-is
      - str: encoded as UTF-8
      - dict: JSON-serialized and UTF-8 encoded

    This is the library's default behavior; callers can inject their own serializer
    (e.g., msgpack/orjson) for performance.
    """
    if isinstance(message, bytes):
        return message
    if isinstance(message, str):
        return message.encode("utf-8")
    # dict
    return json.dumps(message).encode("utf-8")


def get_method_name_as_string(func: Callable[..., Any]) -> str:
    """Return a stable string name for a function or bound method.

    For methods the format is: "SomeClass.method".
    For module-level functions the format is: "function".

    Args:
        func: A function or bound method.

    Returns:
        The resolved function name.

    Raises:
        Exception: If `func` is not a function or method.
    """
    if inspect.ismethod(func):
        return ".".join([get_class_that_defined_method(func).__name__, func.__name__])
    elif inspect.isfunction(func):
        return func.__name__
    else:
        raise Exception(f'{func} cannot be resolved to an invokable')


def get_class_that_defined_method(meth: Callable[..., Any]) -> Any:
    """Best-effort utility to find the class/module that defines a function.

    This is used primarily for logging and debugging.

    Args:
        meth: A function or bound method.

    Returns:
        The class or module object that defined the callable, or None.
    """
    if inspect.ismethod(meth):
        print('this is a method')
        for cls in inspect.getmro(meth.__self__.__class__):
            if meth.__name__ in cls.__dict__:
                return cls
    if inspect.isfunction(meth):
        print('this is a function')
        return getattr(inspect.getmodule(meth),
                       meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0],
                       None)
    print('this is neither a function nor a method')
    return None  # not required since None would have been implicitly returned anyway


def get_rabbit_connection(vhost_name: str, server: str, port: int, username: str, password: str) -> pika.BlockingConnection:
    """Connect to RabbitMQ using pika.BlockingConnection.

    Note: This is a legacy convenience helper. Prefer using `RabbitConnectionFactory` or
    `make_connection_factory_from_params` for dependency injection and testability.

    Args:
        vhost_name: The vhost to connect to.
        server: RabbitMQ host.
        port: RabbitMQ port.
        username: Username.
        password: Password.

    Returns:
        A connected pika.BlockingConnection.
    """
    if not vhost_name:
        raise Exception("vhost name cannot be empty")
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=server, port=int(port), virtual_host=vhost_name, credentials=credentials,
                                  channel_max=100, heartbeat=60, blocked_connection_timeout=300))
    return connection


# --- New helpers: ConnectionFactory and env loader ---
class RabbitConnectionFactory:
    """Callable helper that creates a `pika.BlockingConnection` from explicit parameters.

    Example:
        factory = RabbitConnectionFactory(
            server='localhost', port=5672, username='guest', password='guest', vhost='/'
        )
        conn = factory()

    Notes:
        Any additional connection kwargs (like heartbeat, blocked_connection_timeout, ssl_options)
        can be supplied via `connection_kwargs` and will be forwarded to pika.ConnectionParameters.
    """

    def __init__(
            self, *, server: str, port: int, username: str, password: str, vhost: str,
            connection_kwargs: Optional[Dict[str, Any]] = None):
        self.server = server
        self.port = int(port)
        self.username = username
        self.password = password
        self.vhost = vhost
        self.connection_kwargs = connection_kwargs or {}

    def __call__(self) -> pika.BlockingConnection:
        credentials = pika.PlainCredentials(self.username, self.password)
        params = pika.ConnectionParameters(host=self.server, port=self.port, virtual_host=self.vhost,
                                           credentials=credentials, **self.connection_kwargs)
        return pika.BlockingConnection(params)


def make_connection_factory_from_params(params: ConnectionParams) -> Callable[[], pika.BlockingConnection]:
    """Validate a params dict and return a callable that creates a pika.BlockingConnection.

    Required keys:
        - server
        - port
        - username
        - password
        - vhost

    Additional keys are forwarded to pika.ConnectionParameters (except `message_types`).

    Args:
        params: Connection configuration.

    Returns:
        A callable that builds a pika.BlockingConnection.
    """
    required = ['server', 'port', 'username', 'password', 'vhost']
    missing = [k for k in required if k not in params]
    if missing:
        raise ValueError(f'Missing required connection params: {missing}')
    # Extract any connection kwargs (like heartbeat, channel_max, ssl_options)
    connection_kwargs = {k: v for k, v in params.items() if
                         k not in required and k != 'message_types' and k != 'exchange_name'}
    return RabbitConnectionFactory(server=params['server'], port=int(params['port']), username=params['username'],
                                   password=params['password'], vhost=params['vhost'],
                                   connection_kwargs=connection_kwargs)


def load_connection_params_from_env(prefix: str = 'RABBITMQ') -> Optional[ConnectionParams]:
    """Load connection params from environment variables.

    Recognized variables (case-sensitive):
      - {prefix}_HOST or {prefix}_SERVER
      - {prefix}_PORT
      - {prefix}_VHOST
      - {prefix}_USER
      - {prefix}_PASSWORD
      - optional: {prefix}_HEARTBEAT, {prefix}_CHANNEL_MAX

    Returns:
        A dict with keys compatible with `make_connection_factory_from_params`, or None if required
        vars are not present.
    """
    server = os.getenv(f'{prefix}_HOST') or os.getenv(f'{prefix}_SERVER')
    port = os.getenv(f'{prefix}_PORT')
    vhost = os.getenv(f'{prefix}_VHOST')
    username = os.getenv(f'{prefix}_USER')
    password = os.getenv(f'{prefix}_PASSWORD')

    if not (server and port and vhost and username and password):
        return None

    params: Dict[str, Any] = {
        'server': server,
        'port': int(port),
        'vhost': vhost,
        'username': username,
        'password': password,
    }
    hb = os.getenv(f'{prefix}_HEARTBEAT')
    if hb:
        params['heartbeat'] = int(hb)
    cm = os.getenv(f'{prefix}_CHANNEL_MAX')
    if cm:
        params['channel_max'] = int(cm)
    return params


def get_rabbit_connection_from_params(params: ConnectionParams) -> pika.BlockingConnection:
    """Create a connection directly from params dict."""
    factory = make_connection_factory_from_params(params)
    return factory()


@dataclass
class CloseError(Exception):
    """Raised when one or more resources fail to close/stop.

    This is used by higher-level facades (e.g., RabbitMQClient.close/aclose) to avoid silently
    swallowing exceptions while still attempting to stop all managed resources.
    """

    errors: list[BaseException]

    def __str__(self) -> str:  # pragma: no cover
        msgs = "; ".join(f"{type(e).__name__}: {e}" for e in self.errors)
        return f"CloseError({len(self.errors)} errors): {msgs}"


def raise_if_errors(errors: list[BaseException]) -> None:
    """Raise CloseError if the provided list is non-empty."""
    if errors:
        raise CloseError(errors)

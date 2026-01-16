"""Logging helpers for rabbit_mq_client.

Goals:
- Do not override global logging configuration.
- Keep message formats consistent across the library.

All helpers here assume callers pass a logger obtained via `logging.getLogger(__name__)`.
"""

from __future__ import annotations

from typing import Any


def destination_str(destination: Any) -> str:
    """Render a destination/source object to a stable, human-readable string for logs."""
    if destination is None:
        return "<unknown>"
    if isinstance(destination, str):
        return destination
    if isinstance(destination, dict):
        ex = destination.get("exchange_name")
        rk = destination.get("routing_key")
        q = destination.get("predefined_queue_name")
        parts = []
        if ex:
            parts.append(f"exchange={ex}")
        if rk is not None:
            parts.append(f"routing_key={rk}")
        if q:
            parts.append(f"queue={q}")
        return ",".join(parts) if parts else "<destination>"
    return str(destination)


def log_sending(logger, message: Any, destination: Any) -> None:
    """Debug-log a standardized 'Sending message ... to ...' line if debug is enabled."""
    if logger is None:
        return
    if logger.isEnabledFor(10):  # DEBUG
        logger.debug("Sending message %r to %s", message, destination_str(destination))


def log_received(logger, message: Any, source: Any) -> None:
    """Debug-log a standardized 'Received message ... from ...' line if debug is enabled."""
    if logger is None:
        return
    if logger.isEnabledFor(10):  # DEBUG
        logger.debug("Received message %r from %s", message, destination_str(source))


def maybe_repr_bytes(body: Any, max_len: int = 512) -> Any:
    """Return a safe repr of bytes payload for logs, truncating if needed."""
    if not isinstance(body, (bytes, bytearray, memoryview)):
        return body
    b = bytes(body)
    if len(b) <= max_len:
        return b
    return b[:max_len] + b"..."

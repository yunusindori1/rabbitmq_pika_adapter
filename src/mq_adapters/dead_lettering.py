"""Dead-lettering and poison-message handling helpers.

This module is dependency-free and is shared between sync (pika) and async (aio-pika) listeners.

Key concepts:
- Prefer broker-native dead-letter + retry queues (DLX + TTL) for retry delays.
- Avoid hot loops: when configured, failures use `nack(requeue=False)` to route through DLX.
- Poison handling: when retry attempts exceed a threshold, publish to a pre-existing DLQ queue
  and ACK the original message.

Configuration is read from `message_type_map[message_type]`:
- `queue_arguments` (canonical) or aliases `queue_args` / `queue_declare_arguments`

Supported keys inside queue arguments:
- `on_error`: "requeue" (default) or "dead_letter"
- `dead_letter_queue`: name of a pre-existing DLQ queue to publish poison messages to
- `max_retries`: int, default 1. Threshold is inclusive: poison when attempt >= max_retries.

Retry attempt counting:
- Uses RabbitMQ's broker-provided `x-death` header when present.
  attempt = 1 + sum(x-death[*].count)

If `x-death` is absent, attempt is treated as 1.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class DeadLetteringConfig:
    on_error: str = "requeue"  # "requeue" | "dead_letter"
    dead_letter_queue: Optional[str] = None
    max_retries: int = 1


_QUEUE_ARGS_KEYS = ("queue_arguments", "queue_args", "queue_declare_arguments")


def resolve_queue_arguments(destination: Mapping[str, Any]) -> Dict[str, Any]:
    for k in _QUEUE_ARGS_KEYS:
        v = destination.get(k)
        if isinstance(v, dict):
            return dict(v)
    return {}


def resolve_dead_lettering_config(destination: Mapping[str, Any]) -> DeadLetteringConfig:
    args = resolve_queue_arguments(destination)
    on_error = str(args.get("on_error", "requeue")).strip().lower()
    dlq = args.get("dead_letter_queue")
    if dlq is not None:
        dlq = str(dlq)
    try:
        max_retries = int(args.get("max_retries", 1))
    except Exception:
        max_retries = 1
    if max_retries < 1:
        max_retries = 1
    if on_error not in ("requeue", "dead_letter"):
        on_error = "requeue"
    return DeadLetteringConfig(on_error=on_error, dead_letter_queue=dlq, max_retries=max_retries)


def retry_attempt_from_headers(headers: Optional[Mapping[str, Any]]) -> int:
    """Compute 1-based attempt count from broker headers.

    RabbitMQ adds an `x-death` header when a message is dead-lettered.

    attempt = 1 + sum(x-death[*].count)

    If absent/unparseable, return 1.
    """

    if not headers:
        return 1

    x_death = headers.get("x-death")
    if not x_death:
        return 1

    total = 0
    try:
        if isinstance(x_death, list):
            for entry in x_death:
                if isinstance(entry, dict):
                    c = entry.get("count")
                    try:
                        total += int(c)
                    except Exception:
                        pass
    except Exception:
        return 1

    return 1 + max(0, total)


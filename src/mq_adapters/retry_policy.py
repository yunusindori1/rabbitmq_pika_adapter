"""Shared retry/backoff utilities.

The project had multiple ad-hoc retry loops (sync listener reconnect, sync publish backend
reconnect, async sender retries). This module centralizes the backoff policy so behavior is:

- consistent across sync + async
- configurable (max attempts, base/max delay)
- jittered (to avoid thundering herd reconnects)

This module is intentionally dependency-free.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional


@dataclass(frozen=True)
class RetryPolicy:
    """Retry/backoff policy.

    Args:
        max_attempts:
            Number of attempts before giving up. Use None for infinite retries.
        base_delay:
            Initial delay (seconds) before the second attempt.
        max_delay:
            Maximum delay between attempts.
        multiplier:
            Exponential backoff factor (2.0 is typical).
        jitter:
            Jitter (seconds) applied to the computed delay as +/- jitter.

    Notes:
        - attempt numbers are 1-based.
        - delay for attempt=1 is 0.0 (first call is immediate).
    """

    max_attempts: Optional[int] = 5
    base_delay: float = 0.25
    max_delay: float = 5.0
    multiplier: float = 2.0
    jitter: float = 0.1

    def delay_for_attempt(self, attempt: int) -> float:
        """Return the backoff delay (seconds) for a 1-based attempt number."""
        if attempt <= 1:
            delay = 0.0
        else:
            # attempt=2 => base_delay, attempt=3 => base_delay * multiplier, ...
            exp = attempt - 2
            delay = self.base_delay * (self.multiplier**exp)
            delay = min(self.max_delay, delay)

        if self.jitter > 0:
            delay = max(0.0, delay + random.uniform(-self.jitter, self.jitter))

        return delay


async def async_retry(
    func: Callable[[], Awaitable[Any]],
    *,
    retry_policy: RetryPolicy,
    logger: Optional[Any] = None,
    operation: str = "operation",
) -> Any:
    """Retry an async callable using the given policy."""

    attempt = 1
    while True:
        try:
            return await func()
        except Exception as e:
            max_attempts = retry_policy.max_attempts
            if max_attempts is not None and attempt >= max_attempts:
                raise

            delay = retry_policy.delay_for_attempt(attempt + 1)
            if logger is not None:
                try:
                    if max_attempts is None:
                        logger.warning(
                            f"{operation} failed (attempt {attempt}): {e}. Retrying in {delay:.2f}s"
                        )
                    else:
                        logger.warning(
                            f"{operation} failed (attempt {attempt}/{max_attempts}): {e}. Retrying in {delay:.2f}s"
                        )
                except Exception:
                    pass
            await asyncio.sleep(delay)
            attempt += 1


def sync_retry(
    func: Callable[[], Any],
    *,
    retry_policy: RetryPolicy,
    stop_predicate: Optional[Callable[[], bool]] = None,
    logger: Optional[Any] = None,
    operation: str = "operation",
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Any:
    """Retry a sync callable using the given policy.

    Args:
        stop_predicate: if provided and returns True, abort retries and raise the last exception.
        sleep_fn: injectable for testing.
    """

    attempt = 1
    last_exc: Optional[BaseException] = None

    while True:
        if stop_predicate is not None and stop_predicate():
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f"{operation} aborted")

        try:
            return func()
        except Exception as e:
            last_exc = e
            max_attempts = retry_policy.max_attempts
            if max_attempts is not None and attempt >= max_attempts:
                raise

            delay = retry_policy.delay_for_attempt(attempt + 1)
            if logger is not None:
                try:
                    if max_attempts is None:
                        logger.warning(
                            f"{operation} failed (attempt {attempt}): {e}. Retrying in {delay:.2f}s"
                        )
                    else:
                        logger.warning(
                            f"{operation} failed (attempt {attempt}/{max_attempts}): {e}. Retrying in {delay:.2f}s"
                        )
                except Exception:
                    pass

            sleep_fn(delay)
            attempt += 1


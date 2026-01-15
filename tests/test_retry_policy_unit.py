from __future__ import annotations

import pytest

from mq_adapters.retry_policy import RetryPolicy, sync_retry


def test_delay_for_attempt_monotonic_without_jitter():
    p = RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=5.0, multiplier=2.0, jitter=0.0)
    # attempt numbering is 1-based; attempt=1 is immediate
    assert p.delay_for_attempt(1) == 0.0
    assert p.delay_for_attempt(2) == 0.5
    assert p.delay_for_attempt(3) == 1.0
    assert p.delay_for_attempt(4) == 2.0
    assert p.delay_for_attempt(5) == 4.0
    # capped by max_delay
    assert p.delay_for_attempt(6) == 5.0


def test_sync_retry_stops_after_max_attempts():
    calls = {"n": 0}

    def fail():
        calls["n"] += 1
        raise RuntimeError("boom")

    p = RetryPolicy(max_attempts=3, base_delay=0.01, max_delay=0.01, multiplier=2.0, jitter=0.0)

    sleeps: list[float] = []

    def sleeper(sec: float) -> None:
        sleeps.append(sec)

    with pytest.raises(RuntimeError):
        sync_retry(fail, retry_policy=p, sleep_fn=sleeper, operation="test")

    # 3 calls (attempts) and 2 sleeps (between attempts)
    assert calls["n"] == 3
    assert sleeps == [0.01, 0.01]


from __future__ import annotations

from mq_adapters.dead_lettering import retry_attempt_from_headers, resolve_dead_lettering_config


def test_retry_attempt_from_headers_missing():
    assert retry_attempt_from_headers(None) == 1
    assert retry_attempt_from_headers({}) == 1


def test_retry_attempt_from_headers_x_death_sum_counts():
    headers = {
        "x-death": [
            {"count": 1, "queue": "q1"},
            {"count": 2, "queue": "q2"},
        ]
    }
    assert retry_attempt_from_headers(headers) == 1 + 3


def test_dead_lettering_config_defaults_and_parsing():
    destination = {"queue_arguments": {"on_error": "dead_letter", "dead_letter_queue": "dlq", "max_retries": 2}}
    cfg = resolve_dead_lettering_config(destination)
    assert cfg.on_error == "dead_letter"
    assert cfg.dead_letter_queue == "dlq"
    assert cfg.max_retries == 2

    cfg2 = resolve_dead_lettering_config({})
    assert cfg2.on_error == "requeue"
    assert cfg2.dead_letter_queue is None
    assert cfg2.max_retries == 1


"""Unit tests for the job-outcome counters ``taskq.jobs.attempt_failures``
and ``taskq.jobs.abandoned`` - the series that replaced the consumed-messages
``outcome="abandoned"`` relabelling of every retry and snooze.

Both are call-time-resolved (lazy) counters: recorded through the meter
current at the call, gated by the OTel flag, with the closed-set label
discipline every failure counter in ``obs/_otel.py`` follows.
"""

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import counter_data_points


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs.py's otel_reader fixture.
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: meter)
    otel_mod.set_otel_enabled(True)
    return reader


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[int, dict[str, object]]]:
    return [(int(p.value), dict(p.attributes or {})) for p in counter_data_points(reader, name)]


def test_attempt_failure_carries_actor_class_and_retry_decision(
    otel_reader: InMemoryMetricReader,
) -> None:
    obs_mod.record_attempt_failure("send_email", "ConnectionError", retryable=True)
    obs_mod.record_attempt_failure("send_email", "ValueError", retryable=False)
    obs_mod.record_attempt_failure("send_email", "ConnectionError", retryable=True)

    assert sorted(_points(otel_reader, "taskq.jobs.attempt_failures"), key=str) == sorted(
        [
            (2, {"actor": "send_email", "error_type": "ConnectionError", "retryable": "true"}),
            (1, {"actor": "send_email", "error_type": "ValueError", "retryable": "false"}),
        ],
        key=str,
    )


def test_attempt_failure_derives_error_type_from_the_handled_exception(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The failure-counter idiom: an omitted error_type is the class of the
    exception being handled - a closed set - never caller text."""

    class _FlakyError(RuntimeError):
        pass

    try:
        raise _FlakyError("boom")
    except _FlakyError:
        obs_mod.record_attempt_failure("send_email", retryable=True)

    assert _points(otel_reader, "taskq.jobs.attempt_failures") == [
        (1, {"actor": "send_email", "error_type": "_FlakyError", "retryable": "true"})
    ]


def test_abandoned_is_labelled_by_actor_only(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_job_abandoned("send_email")
    obs_mod.record_job_abandoned("send_email")
    obs_mod.record_job_abandoned("resize_image")

    assert sorted(_points(otel_reader, "taskq.jobs.abandoned"), key=str) == sorted(
        [(2, {"actor": "send_email"}), (1, {"actor": "resize_image"})], key=str
    )


def test_both_counters_respect_the_otel_switch(otel_reader: InMemoryMetricReader) -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_attempt_failure("send_email", "ValueError", retryable=False)
    obs_mod.record_job_abandoned("send_email")
    assert _points(otel_reader, "taskq.jobs.attempt_failures") == []
    assert _points(otel_reader, "taskq.jobs.abandoned") == []


def test_consumed_outcome_set_has_no_value_without_a_producer() -> None:
    """Every value of the closed set is something the consumer path emits:
    ``scheduled`` for a released row, and never ``abandoned`` - the
    operator-cancel outcome lives on its own counter."""
    from typing import get_args

    assert set(get_args(otel_mod.ConsumedOutcome.__value__)) == {
        "succeeded",
        "failed",
        "cancelled",
        "scheduled",
    }


def test_timeouts_counter_carries_actor_and_kind(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_job_timeout("send_email", kind="start_to_close")
    obs_mod.record_job_timeout("send_email", kind="schedule_to_close", count=3)
    assert sorted(_points(otel_reader, "taskq.jobs.timeouts"), key=str) == sorted(
        [
            (1, {"actor": "send_email", "kind": "start_to_close"}),
            (3, {"actor": "send_email", "kind": "schedule_to_close"}),
        ],
        key=str,
    )


def test_deadline_sweep_arm_feeds_the_timeouts_family(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The sweep's whole-job deadline arm counts on taskq.jobs.timeouts
    beside the handler arms, so kind="schedule_to_close" is the complete
    count however the deadline was enforced."""
    obs_mod.record_deadline_exceeded_swept("send_email", count=4)
    assert _points(otel_reader, "taskq.jobs.timeouts") == [
        (4, {"actor": "send_email", "kind": "schedule_to_close"})
    ]
    otel_mod.set_otel_enabled(False)
    obs_mod.record_deadline_exceeded_swept("send_email", count=4)
    assert _points(otel_reader, "taskq.jobs.timeouts") == [
        (4, {"actor": "send_email", "kind": "schedule_to_close"})
    ]

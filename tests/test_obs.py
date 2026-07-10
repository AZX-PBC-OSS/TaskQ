"""Unit tests for OTel safe helpers, _otel_enabled flag, set_otel_enabled, and metric instruments."""

import pytest
from opentelemetry import trace
from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics, counter_data_points, counter_value, setup_tracer


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation.

    Replaces all module-level instrument objects with fresh copies
    created on a per-test MeterProvider backed by InMemoryMetricReader.
    monkeypatch auto-restores the originals on teardown.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    _patch_instruments(monkeypatch, new_meter)
    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)

    return reader


def _patch_instruments(monkeypatch: pytest.MonkeyPatch, meter: Meter) -> None:
    """Monkeypatch all obs._otel module-level instrument singletons with test copies."""
    m = meter
    for attr, factory in [
        ("_cancellation_requested", lambda: m.create_counter("taskq.cancellation.requested")),
        ("_backpressure_errors", lambda: m.create_counter("taskq.backpressure.errors")),
        (
            "_deadline_exceeded_sweep_jobs_failed",
            lambda: m.create_counter("taskq.deadline_exceeded_sweep.jobs_failed"),
        ),
        ("_published_messages", lambda: m.create_counter("messaging.client.published.messages")),
        ("_dispatch_duration", lambda: m.create_histogram("taskq.dispatch.duration")),
        ("_consumed_messages", lambda: m.create_counter("messaging.client.consumed.messages")),
        ("_process_duration", lambda: m.create_histogram("messaging.process.duration")),
        ("_lock_expires_in_seconds", lambda: m.create_histogram("taskq.lock.expires_in_seconds")),
        ("_heartbeat_misses", lambda: m.create_counter("taskq.heartbeat.misses")),
        (
            "_queue_depth_gauge",
            lambda: m.create_observable_gauge(
                "taskq.queue.depth", callbacks=[otel_mod._observe_queue_depth]
            ),
        ),
        (
            "_reservation_slots_gauge",
            lambda: m.create_observable_gauge(
                "taskq.reservation.slots_used", callbacks=[otel_mod._observe_reservation_slots]
            ),
        ),
        ("_progress_publish_failures", lambda: m.create_counter("taskq.progress.publish_failures")),
        ("_ratelimit_refund_failures", lambda: m.create_counter("taskq.ratelimit.refund_failures")),
        ("_leader_election_attempts", lambda: m.create_counter("taskq.leader.election_attempts")),
        ("_leader_election_failures", lambda: m.create_counter("taskq.leader.election_failures")),
        (
            "_cron_consecutive_failures",
            lambda: m.create_up_down_counter("taskq.cron.consecutive_failures"),
        ),
        (
            "_disabled_schedules_gauge",
            lambda: m.create_observable_gauge(
                "taskq.cron.disabled_schedules", callbacks=[otel_mod._observe_disabled_schedules]
            ),
        ),
        ("_pruned_jobs", lambda: m.create_counter("taskq.pruned.jobs")),
    ]:
        monkeypatch.setattr(otel_mod, attr, factory())


# ── set_otel_enabled ────────────────────────────────────────────────────


def test_set_otel_enabled_false() -> None:
    otel_mod.set_otel_enabled(False)
    assert otel_mod._otel_enabled is False


def test_set_otel_enabled_true() -> None:
    otel_mod.set_otel_enabled(False)
    otel_mod.set_otel_enabled(True)
    assert otel_mod._otel_enabled is True


# ── safe_start_span: otel_enabled=False suppresses span creation ───────


def test_safe_start_span_disabled_yields_noop() -> None:
    otel_mod.set_otel_enabled(False)
    with obs_mod.safe_start_span("test.span") as span:
        assert isinstance(span, trace.NonRecordingSpan)


def test_safe_start_span_disabled_no_spans_exported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)

    otel_mod.set_otel_enabled(False)
    with obs_mod.safe_start_span("test.span"):
        pass

    assert len(exporter.spans) == 0


# ── safe_start_span: otel_enabled=True creates real spans ──────────────


def test_safe_start_span_enabled_creates_real_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)

    otel_mod.set_otel_enabled(True)
    with obs_mod.safe_start_span("test.span") as span:
        assert not isinstance(span, trace.NonRecordingSpan)

    assert len(exporter.spans) == 1


# ── safe_start_span: — OTel exceptions are caught ────────────────


def test_safe_start_span_catches_tracer_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_tracer(monkeypatch)

    otel_mod.set_otel_enabled(True)

    def _raising_start_as_current_span(*args: object, **kwargs: object) -> object:
        raise RuntimeError("simulated OTel misconfiguration")

    monkeypatch.setattr(
        otel_mod.get_tracer(),
        "start_as_current_span",
        _raising_start_as_current_span,
    )

    with obs_mod.safe_start_span("test.span") as span:
        assert isinstance(span, trace.NonRecordingSpan)


# ── safe_start_span: round-trip disabled→enabled→works ─────────────────


def test_safe_start_span_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)

    otel_mod.set_otel_enabled(False)
    with obs_mod.safe_start_span("disabled.span") as span:
        assert isinstance(span, trace.NonRecordingSpan)

    otel_mod.set_otel_enabled(True)
    with obs_mod.safe_start_span("enabled.span") as span:
        assert not isinstance(span, trace.NonRecordingSpan)

    assert len(exporter.spans) == 1


# ── safe_start_span: attributes and kind are forwarded ──────────────────


def test_safe_start_span_forwards_kind_and_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)

    otel_mod.set_otel_enabled(True)
    with obs_mod.safe_start_span(
        "test.span",
        kind=trace.SpanKind.CONSUMER,
        attributes={"messaging.operation.type": "process"},
    ) as span:
        span.set_attribute("extra", "value")

    span = exporter.span_named("test.span")
    assert span is not None
    assert span.kind == trace.SpanKind.CONSUMER
    assert span.attributes is not None
    assert span.attributes.get("messaging.operation.type") == "process"


# ── instrument 6: taskq.lock.expires_in_seconds ───────────────────────────


def test_record_lock_expires_in_seconds(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_lock_expires_in_seconds("worker-1", 30.0)

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.lock.expires_in_seconds" in names


def test_record_lock_expires_in_seconds_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_lock_expires_in_seconds("worker-1", 30.0)
    otel_mod.set_otel_enabled(True)


# ── instrument 7: taskq.heartbeat.misses ──────────────────────────────────


def test_record_heartbeat_miss(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_heartbeat_miss("worker-1")

    dps = counter_data_points(otel_reader, "taskq.heartbeat.misses")
    assert len(dps) == 1
    assert dps[0].value == 1
    assert dps[0].attributes == {"worker_id": "worker-1"}


def test_record_heartbeat_miss_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_heartbeat_miss("worker-1")
    otel_mod.set_otel_enabled(True)


# ── instrument 5: taskq.queue.depth ───────────────────────────────────────


def test_queue_depth_gauge_reads_from_cache(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.update_queue_depth_cache({"default": 5, "priority": 3})

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.queue.depth" in names


# ── instrument 8: taskq.reservation.slots_used ────────────────────────────


def test_reservation_slots_gauge_reads_from_cache(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.update_reservation_slots_cache({"bucket_a": 4, "bucket_b": 1})

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.reservation.slots_used" in names


# ── instrument 12: taskq.progress.publish_failures ────────────────────────


def test_record_progress_publish_failure(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_progress_publish_failure(channel="per_job", error_type="ConnectionError")

    assert counter_value(otel_reader, "taskq.progress.publish_failures") == 1


def test_record_progress_publish_failure_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_progress_publish_failure(channel="per_job", error_type="ConnectionError")
    otel_mod.set_otel_enabled(True)


# ── instrument 13: taskq.ratelimit.refund_failures ────────────────────────


def test_record_ratelimit_refund_failure(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_ratelimit_refund_failure("my_bucket", "redis")

    dps = counter_data_points(otel_reader, "taskq.ratelimit.refund_failures")
    assert len(dps) == 1
    assert dps[0].value == 1
    assert dps[0].attributes == {"bucket": "my_bucket", "backend": "redis"}


def test_record_ratelimit_refund_failure_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_ratelimit_refund_failure("my_bucket", "redis")
    otel_mod.set_otel_enabled(True)


# ── instruments 14-15: taskq.leader.election_attempts / election_failures ──


def test_record_election_attempt_win(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_election_attempt("worker-1", won=True)

    attempts_dps = counter_data_points(otel_reader, "taskq.leader.election_attempts")
    assert len(attempts_dps) == 1
    assert attempts_dps[0].value == 1
    assert attempts_dps[0].attributes == {"worker_id": "worker-1"}

    failure_dps = counter_data_points(otel_reader, "taskq.leader.election_failures")
    assert len(failure_dps) == 0


def test_record_election_attempt_loss(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_election_attempt("worker-1", won=False)

    attempts_dps = counter_data_points(otel_reader, "taskq.leader.election_attempts")
    assert len(attempts_dps) == 1
    assert attempts_dps[0].value == 1

    failure_dps = counter_data_points(otel_reader, "taskq.leader.election_failures")
    assert len(failure_dps) == 1
    assert failure_dps[0].value == 1


def test_record_election_attempt_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_election_attempt("worker-1", won=True)
    otel_mod.set_otel_enabled(True)


# ── instrument 16: taskq.cron.consecutive_failures ─────────────────────────


def test_record_cron_failure_increment(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_cron_failure("schedule-1", 1)

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.cron.consecutive_failures" in names


def test_record_cron_failure_reset(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_cron_failure("schedule-1", 1)
    obs_mod.record_cron_failure("schedule-1", -1)

    dps = counter_data_points(otel_reader, "taskq.cron.consecutive_failures")
    assert len(dps) == 1
    assert dps[0].value == 0
    assert dps[0].attributes == {"schedule_id": "schedule-1"}


def test_record_cron_failure_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_cron_failure("schedule-1", 1)
    otel_mod.set_otel_enabled(True)


# ── instrument 17: taskq.cron.disabled_schedules ──────────────────────────


def test_disabled_schedules_gauge_reads_from_state(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.update_disabled_schedules_count(3)

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.cron.disabled_schedules" in names


# ── instrument 18: taskq.pruned.jobs ─────────────────────────────────────


def test_record_pruned_jobs(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_pruned_jobs("my_actor", "completed", count=5)

    dps = counter_data_points(otel_reader, "taskq.pruned.jobs")
    assert len(dps) == 1
    assert dps[0].value == 5
    assert dps[0].attributes == {"actor": "my_actor", "status": "completed"}


def test_record_pruned_jobs_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_pruned_jobs("my_actor", "completed")
    otel_mod.set_otel_enabled(True)

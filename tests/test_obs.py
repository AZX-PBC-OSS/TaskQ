"""Unit tests for OTel safe helpers, _otel_enabled flag, set_otel_enabled, and metric instruments."""

import pytest
from opentelemetry import trace
from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

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
        ("_dispatch_failures", lambda: m.create_counter("taskq.dispatch.failures")),
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
        (
            "_slot_pool_acquire_failures",
            lambda: m.create_counter("taskq.worker.slot_pool.acquire_failures"),
        ),
        (
            "_slot_pool_occupancy_gauge",
            lambda: m.create_observable_gauge(
                "taskq.worker.slot_pool.connections_in_use",
                callbacks=[otel_mod._observe_slot_pool_occupancy],
            ),
        ),
        ("_leader_election_attempts", lambda: m.create_counter("taskq.leader.election_attempts")),
        ("_leader_election_failures", lambda: m.create_counter("taskq.leader.election_failures")),
        (
            "_cron_consecutive_failures",
            lambda: m.create_up_down_counter("taskq.cron.consecutive_failures"),
        ),
        (
            "_cron_budget_deferrals",
            lambda: m.create_counter("taskq.cron.budget_deferrals"),
        ),
        (
            "_disabled_schedules_gauge",
            lambda: m.create_observable_gauge(
                "taskq.cron.disabled_schedules", callbacks=[otel_mod._observe_disabled_schedules]
            ),
        ),
        (
            "_running_lease_expired_gauge",
            lambda: m.create_observable_gauge(
                "taskq.jobs.running_lease_expired",
                callbacks=[otel_mod._observe_running_lease_expired],
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


# ── safe_start_span: - OTel exceptions are caught ────────────────


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
    assert dps[0].attributes == {}


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


# ── instrument: taskq.worker.slot_pool.* ──────────────────────────────────


def test_record_slot_pool_acquire_failure_fires_on_failure_path(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The acquire-failure counter fires when the record helper is called
    from the acquire's exception branch, naming the failure class on the
    ``error_type`` dimension - the per-occurrence job id lives in the log
    event, never a metric."""
    try:
        raise TimeoutError("simulated acquire timeout")
    except TimeoutError:
        obs_mod.record_slot_pool_acquire_failure()

    assert counter_value(otel_reader, "taskq.worker.slot_pool.acquire_failures") == 1
    points = counter_data_points(otel_reader, "taskq.worker.slot_pool.acquire_failures")
    assert all(dict(p.attributes or {}) == {"error_type": "TimeoutError"} for p in points)


def test_record_slot_pool_acquire_failure_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_slot_pool_acquire_failure()
    otel_mod.set_otel_enabled(True)


def test_slot_pool_occupancy_gauge_reads_held_connections(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The occupancy gauge reports size minus idle - the in-use count a
    saturation-pin diagnosis needs (the acquire-failure counter is silent
    below the cliff)."""

    class _Pool:
        def get_size(self) -> int:
            return 9

        def get_idle_size(self) -> int:
            return 1

    obs_mod.set_slot_pool_occupancy_source(_Pool())
    try:
        assert counter_value(otel_reader, "taskq.worker.slot_pool.connections_in_use") == 8
    finally:
        # Reset the module source so later tests in this process don't
        # observe this test's pool.
        obs_mod.set_slot_pool_occupancy_source(None)


def test_slot_pool_occupancy_gauge_tolerates_a_broken_pool_source() -> None:
    """A source whose accessors raise (a pool teardown closed underneath the
    module-level source) must produce no observation - a collection read
    that raises into the SDK's export path breaks every instrument's
    export, not just this gauge's."""
    import asyncpg

    class _ClosedPool:
        def get_size(self) -> int:
            raise asyncpg.InterfaceError("pool is closed")

        def get_idle_size(self) -> int:
            return 0

    obs_mod.set_slot_pool_occupancy_source(_ClosedPool())
    try:
        # The callback itself is the unit under test (the same object
        # _patch_instruments registers on the test gauge).
        assert list(otel_mod._observe_slot_pool_occupancy(None)) == []  # pyright: ignore[reportPrivateUsage, reportArgumentType]  # Why: the callback ignores its options argument; asserting the no-raise/no-observation contract directly is what discriminates the defensive branch.
    finally:
        obs_mod.set_slot_pool_occupancy_source(None)


def test_slot_pool_occupancy_gauge_is_label_free() -> None:
    """``taskq.worker.slot_pool.connections_in_use`` carries NO dimensions -
    a single process-level number. The pool is named in the instrument and
    there is exactly one per worker process; adding a dimension here (the
    worker_id temptation especially - a fresh UUID per process on
    Kubernetes) mints unbounded time series and throttles ingestion for
    every custom metric in the subscription (see
    tests/test_obs_metric_cardinality.py)."""

    class _Pool:
        def get_size(self) -> int:
            return 9

        def get_idle_size(self) -> int:
            return 1

    obs_mod.set_slot_pool_occupancy_source(_Pool())
    try:
        observations = list(otel_mod._observe_slot_pool_occupancy(None))  # pyright: ignore[reportPrivateUsage, reportArgumentType]  # Why: the callback ignores its options argument (same direct-callback pattern as the broken-source test above).
    finally:
        obs_mod.set_slot_pool_occupancy_source(None)

    assert len(observations) == 1
    assert dict(observations[0].attributes or {}) == {}
    assert observations[0].value == 8


# ── instrument: taskq.dispatch.failures ───────────────────────────────────


def test_record_dispatch_failure_names_the_handled_exception_class(
    otel_reader: InMemoryMetricReader,
) -> None:
    """Called from an except block with no explicit value - the production
    call shape at every dispatch raise site - the counter's ``error_type``
    is the caught exception's class name."""
    try:
        raise ConnectionResetError("simulated reset mid dispatch query")
    except ConnectionResetError:
        obs_mod.record_dispatch_failure("default")

    dps = counter_data_points(otel_reader, "taskq.dispatch.failures")
    assert len(dps) == 1
    assert dps[0].value == 1
    assert dps[0].attributes == {"queue": "default", "error_type": "ConnectionResetError"}


def test_record_dispatch_failure_error_type_resolution_contract(
    otel_reader: InMemoryMetricReader,
) -> None:
    """An explicit ``error_type`` always wins; with neither an explicit
    value nor an active exception the label is the fixed ``unknown``
    value, so the dimension stays a closed class set rather than whatever
    string a caller happened to have in scope."""
    obs_mod.record_dispatch_failure("default", error_type="TimeoutError")
    obs_mod.record_dispatch_failure("default")

    dps = counter_data_points(otel_reader, "taskq.dispatch.failures")
    by_error_type = {dp.attributes["error_type"]: dp.value for dp in dps if dp.attributes}
    assert by_error_type == {"TimeoutError": 1, "unknown": 1}


def test_record_dispatch_failure_disabled() -> None:
    otel_mod.set_otel_enabled(False)
    obs_mod.record_dispatch_failure("default")
    otel_mod.set_otel_enabled(True)


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
    obs_mod.record_ratelimit_refund_failure("my_bucket", "redis", error_type="ConnectionError")

    dps = counter_data_points(otel_reader, "taskq.ratelimit.refund_failures")
    assert len(dps) == 1
    assert dps[0].value == 1
    assert dps[0].attributes == {
        "bucket": "my_bucket",
        "backend": "redis",
        "error_type": "ConnectionError",
    }


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
    assert attempts_dps[0].attributes == {}

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
    obs_mod.record_cron_failure("actor-1", 1)

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.cron.consecutive_failures" in names


def test_record_cron_failure_reset(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.record_cron_failure("actor-1", 1)
    obs_mod.record_cron_failure("actor-1", -1)

    dps = counter_data_points(otel_reader, "taskq.cron.consecutive_failures")
    assert len(dps) == 1
    assert dps[0].value == 0
    assert dps[0].attributes == {"actor": "actor-1"}


def test_record_cron_failure_is_dimensioned_per_actor(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The dimension is the actor -- schedules on one actor share a
    series. The schedule id is a per-row, runtime-minted UUID and
    identity-like, and the actor value itself comes from the schedule
    row (any string accepted at creation time), so the label is capped
    at the emitter (label-contract pins:
    tests/test_rt_worker_metric_cardinality.py). Per-schedule
    attribution rides the cron fired / cron fire failed log lines and
    the cron-fire span instead."""
    obs_mod.record_cron_failure("actor-a", 3)
    obs_mod.record_cron_failure("actor-b", 1)

    dps = counter_data_points(otel_reader, "taskq.cron.consecutive_failures")
    by_actor = {dp.attributes["actor"]: dp.value for dp in dps if dp.attributes}
    assert by_actor == {"actor-a": 3, "actor-b": 1}


def test_record_cron_failure_disabled(otel_reader: InMemoryMetricReader) -> None:
    """The disabled contract is a no-op, not a best-effort record: with
    ``_otel_enabled=False`` the emitter must leave the instrument
    untouched. The reader-backed assertion is the pin -- an earlier shape
    of this test called the emitter and asserted nothing, so a regression
    that records while disabled passed vacuously."""
    otel_mod.set_otel_enabled(False)
    obs_mod.record_cron_failure("actor-1", 1)
    otel_mod.set_otel_enabled(True)

    assert counter_data_points(otel_reader, "taskq.cron.consecutive_failures") == []


# ── instrument 17: taskq.cron.disabled_schedules ──────────────────────────


def test_disabled_schedules_gauge_reads_from_state(otel_reader: InMemoryMetricReader) -> None:
    obs_mod.update_disabled_schedules_count(3)

    metrics = collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.cron.disabled_schedules" in names


# ── instrument: taskq.cron.budget_deferrals ───────────────────────────────


def test_record_cron_budget_deferral_counts_per_actor(otel_reader: InMemoryMetricReader) -> None:
    """The monopolizer-starvation signal: one count per deferred fire,
    dimensioned by the STARVING schedule's actor (per-schedule
    attribution stays on the cron-fire-budget-deferred log line, the
    same label contract as consecutive_failures)."""
    obs_mod.record_cron_budget_deferral("actor-a")
    obs_mod.record_cron_budget_deferral("actor-a")
    obs_mod.record_cron_budget_deferral("actor-b")

    dps = counter_data_points(otel_reader, "taskq.cron.budget_deferrals")
    by_actor = {dp.attributes["actor"]: dp.value for dp in dps if dp.attributes}
    assert by_actor == {"actor-a": 2, "actor-b": 1}


def test_record_cron_budget_deferral_disabled(otel_reader: InMemoryMetricReader) -> None:
    """The disabled contract is a no-op, not a best-effort record, same
    rigor as consecutive_failures: the reader-backed assertion is the
    pin, so a regression that records while disabled cannot pass
    vacuously.  The unconditional trail for this signal is the
    cron-fire-budget-deferred log line, not the counter."""
    otel_mod.set_otel_enabled(False)
    obs_mod.record_cron_budget_deferral("actor-1")
    otel_mod.set_otel_enabled(True)

    assert counter_data_points(otel_reader, "taskq.cron.budget_deferrals") == []


# ── instrument: taskq.jobs.running_lease_expired ──────────────────────────


def test_running_lease_expired_gauge_reads_from_cache(otel_reader: InMemoryMetricReader) -> None:
    """The zombie-running gauge reads the sampler's count, with no
    dimensions: one series, the fleet total, alertable directly."""
    obs_mod.update_running_lease_expired_cache(4)

    metrics = collect_metrics(otel_reader)
    gauge = next((m for m in metrics if m.name == "taskq.jobs.running_lease_expired"), None)
    assert gauge is not None, (
        "taskq.jobs.running_lease_expired not emitted - running-with-expired-"
        "lease is invisible as a distinct shape without it"
    )
    points = [p for p in gauge.data.data_points if isinstance(p, NumberDataPoint)]
    assert len(points) == 1
    assert points[0].value == 4
    assert not points[0].attributes, (
        "the gauge must carry no dimensions - the zombie shape is a fleet "
        "total, and the per-job truth (locked_by_worker, lock_expires_at) "
        "lives on the row and the admin page, not on a label"
    )


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


# ── lazy-instrument and meter memoization (no-SDK proxy leak) ──────────


class _CountingMeter:
    """A stub meter that records every counter creation.

    Stands in for the no-SDK ``_ProxyMeter``: what this stub counts is
    exactly what the proxy meter appends to its unbounded, never-cleaned
    ``_instruments`` list, so "N creations" here is "N leaked proxy
    instruments" in the default deployment.
    """

    def __init__(self) -> None:
        self.created_counters: list[str] = []

    def create_counter(self, name: str, unit: str = "", description: str = "") -> "_StubCounter":
        self.created_counters.append(name)
        return _StubCounter()


class _StubCounter:
    """No-op counter - the pin counts creations, not recordings."""

    def add(self, value: int, attributes: dict[str, str] | None = None) -> None:
        pass


def test_get_meter_is_memoized() -> None:
    """``get_meter`` must hand back ONE meter object for the process.

    With no SDK installed (the default deployment - taskq never installs
    a MeterProvider), ``metrics.get_meter`` mints a fresh ``_ProxyMeter``
    on every call and the proxy provider appends each to a list with no
    cleanup path, so an uncached accessor grows that list forever - once
    per lazy-instrument call, which is once per denial, flush failure, and
    drain row-count.
    """
    first = otel_mod.get_meter()
    second = otel_mod.get_meter()

    assert first is second, (
        "get_meter minted a fresh proxy meter per call: the no-SDK proxy "
        "provider appends every meter to a list with no cleanup path, so "
        "each call is a permanent leak in exactly the default deployment"
    )


def test_lazy_recorder_memoizes_its_instrument_per_meter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lazy recorder called N times must mint its counter ONCE, keyed on
    the CURRENT meter.

    The lazy recorders resolve their instrument at call time (see
    ``_lazy_counter``) so a meter swap is always honored - but with no SDK
    installed, every uncached call minted a fresh proxy counter and
    appended it to the proxy meter's unbounded instruments list, growing
    it on every rate-limit denial, reservation denial, and flush failure
    during exactly the denial storms the counters exist to measure. The
    memo must rebind on a meter swap (the per-test fixtures swap
    ``get_meter``; the isolated reader must see the swapped meter's
    counts) and two different recorders must not collide on one entry.
    """
    otel_mod.set_otel_enabled(True)
    meter_a = _CountingMeter()
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter_a)

    for _ in range(5):
        obs_mod.record_reservation_denial("bucket-a", "reservation")

    assert meter_a.created_counters == ["taskq.reservation.denials"], (
        "each record_reservation_denial call minted a fresh instrument: "
        f"{meter_a.created_counters} - with no SDK installed (the default "
        "deployment) every minted proxy counter is appended to a list with "
        "no cleanup path"
    )

    meter_b = _CountingMeter()
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter_b)
    obs_mod.record_reservation_denial("bucket-b", "rate_limit")

    assert meter_b.created_counters == ["taskq.reservation.denials"], (
        "a swapped meter must get a fresh instrument: the per-test "
        "meter-swap fixtures rely on the isolated reader seeing the new "
        f"meter's counts (created: {meter_b.created_counters})"
    )
    assert meter_a.created_counters == ["taskq.reservation.denials"], (
        "the first meter must not gain instruments after the swap"
    )

    obs_mod.record_ratelimit_denial("sliding_window")
    obs_mod.record_ratelimit_denial("sliding_window")

    assert meter_b.created_counters == [
        "taskq.reservation.denials",
        "taskq.ratelimit.denials",
    ], (
        "two different recorders must not collide on the cache, and each "
        f"recorder's instrument must be minted exactly once: {meter_b.created_counters}"
    )

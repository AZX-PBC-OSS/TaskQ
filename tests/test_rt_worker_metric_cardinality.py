"""Cardinality / label-contract pins for the NEW maintenance emitters.

The repo's cardinality constitution (tests/test_obs_metric_cardinality.py):
identity-like values must never be metric dimensions. The new sweep-health
and backlog instruments carry single bounded-enum labels — ``sweep_name``
over the closed set of leader-loop sweeps, ``lock`` over the schema-
qualified lock names (a fixed purpose enum x the schema), ``status`` over
the database's status enum — and the oldest-due gauge carries none. The
pins here assert exactly that: the recorded dimensions are the documented
enum key and nothing else, so a refactor that sneaks an identity value
(worker_id, job_id) into any of these instruments fails here.
"""

from __future__ import annotations

import pytest
from opentelemetry.metrics import CallbackOptions
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.util.types import AttributeValue

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.constants import schema_lock_name

#: The closed set of sweep names the leader loops pass to the sweep
#: emitters (grep-verified against every call site at write time).
_PRODUCTION_SWEEP_NAMES = (
    "scheduled_to_pending",
    "cron",
    "expired_locks",
    "deadline_exceeded",
    "expired_results",
    "stale_workers",
    "leaked_slots",
    "stale_batches",
)

#: The maintenance-lock purposes the worker passes to
#: ``record_lock_contention`` (election, prune, archive-expiry — cron
#: keeps its own dedicated counter).
_PRODUCTION_LOCK_PURPOSES = ("maintenance_leader", "prune", "archive_expiry")


@pytest.fixture
def enum_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instruments for the new counters, enabled."""
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-enum-cardinality")
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    monkeypatch.setattr(
        otel_mod,
        "_sweep_timeouts",
        meter.create_counter("taskq.maintenance_leader.sweep_timeouts", unit="1"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_lock_contention",
        meter.create_counter("taskq.leader.lock_contention", unit="1"),
    )
    return reader


def _counter_points(
    reader: InMemoryMetricReader, name: str
) -> list[tuple[dict[str, AttributeValue], int]]:
    data = reader.get_metrics_data()
    assert data is not None
    return [
        (dict(p.attributes or {}), int(p.value))
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for p in m.data.data_points
        if isinstance(p, NumberDataPoint)
    ]


def test_sweep_timeouts_dimensions_are_the_sweep_name_enum_only(
    enum_reader: InMemoryMetricReader,
) -> None:
    """``record_sweep_timeout`` across every production sweep name: one
    series per name, dimension keys exactly {sweep_name} — no identity
    value rides along."""
    for name in _PRODUCTION_SWEEP_NAMES:
        obs_mod.record_sweep_timeout(name)

    points = _counter_points(enum_reader, "taskq.maintenance_leader.sweep_timeouts")
    assert {tuple(attrs) for attrs, _ in points} == {("sweep_name",)}
    assert {attrs["sweep_name"] for attrs, _ in points} == set(_PRODUCTION_SWEEP_NAMES)


def test_lock_contention_dimensions_are_the_qualified_lock_names_only(
    enum_reader: InMemoryMetricReader,
) -> None:
    """``record_lock_contention`` from every production lock purpose
    across several schemas: the dimension is exactly the schema-qualified
    lock name — the value set is purposes x schemas (a bounded enum
    product), and no identity value (worker_id) becomes a dimension."""
    schemas = ("taskq", "taskq_tenant_a", "taskq_tenant_b")
    for schema in schemas:
        for purpose in _PRODUCTION_LOCK_PURPOSES:
            obs_mod.record_lock_contention(schema_lock_name(purpose, schema))

    points = _counter_points(enum_reader, "taskq.leader.lock_contention")
    assert {tuple(attrs) for attrs, _ in points} == {("lock",)}
    expected = {
        schema_lock_name(purpose, schema)
        for schema in schemas
        for purpose in _PRODUCTION_LOCK_PURPOSES
    }
    assert {attrs["lock"] for attrs, _ in points} == expected


def test_backlog_gauge_dimensions_are_the_status_enum_only() -> None:
    """``taskq.jobs.by_status``: one observation per status, dimension
    exactly {status} — the database's status enum is the whole value
    space, and no queue/actor/identity key rides along."""
    obs_mod.update_jobs_by_status_cache({"scheduled": 5, "pending": 2, "running": 1})

    observations = list(otel_mod._observe_jobs_by_status(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: the callback is the only way to observe a synchronous gauge without a full SDK scrape (same pattern as the heartbeat gauge cardinality test).

    assert {tuple(dict(o.attributes or {})) for o in observations} == {("status",)}
    assert {o.value for o in observations} == {5, 2, 1}


def test_oldest_due_age_gauge_is_label_free() -> None:
    """``taskq.jobs.oldest_due_age_seconds`` carries NO dimensions — a
    single process-level number; adding any label here multiplies series
    for no query."""
    obs_mod.update_oldest_due_age_cache(12.5)

    observations = list(otel_mod._observe_oldest_due_age(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: same callback-observation pattern as above.

    assert len(observations) == 1
    assert dict(observations[0].attributes or {}) == {}
    assert observations[0].value == 12.5


def test_sweep_batch_size_gauge_dimensions_are_the_sweep_name_enum_only() -> None:
    """``taskq.maintenance_leader.sweep_batch_size``: dimension exactly
    {sweep_name}; the value is the batch size, and nothing about the
    worker identity leaks in."""
    for name in _PRODUCTION_SWEEP_NAMES:
        otel_mod.update_sweep_batch_size_cache(name, 100)

    observations = list(otel_mod._observe_sweep_batch_size(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: same callback-observation pattern as above.

    assert {tuple(dict(o.attributes or {})) for o in observations} == {("sweep_name",)}
    assert all(o.value == 100 for o in observations)
    assert {dict(o.attributes or {})["sweep_name"] for o in observations} == set(
        _PRODUCTION_SWEEP_NAMES
    )


def test_sweep_batch_size_configured_gauge_dimensions_are_the_sweep_name_enum_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``taskq.maintenance_leader.sweep_batch_size_configured``: dimension
    exactly {sweep_name} — the same single-enum contract as its used-size
    sibling, so the sweep-degraded alert's gauge-to-gauge comparison is
    always label-matched and no worker identity leaks in."""
    monkeypatch.setattr(
        otel_mod,
        "_sweep_batch_size_configured_cache",
        dict.fromkeys(_PRODUCTION_SWEEP_NAMES, 100),
    )

    observations = list(otel_mod._observe_sweep_batch_size_configured(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: same callback-observation pattern as above.

    assert {tuple(dict(o.attributes or {})) for o in observations} == {("sweep_name",)}
    assert all(o.value == 100 for o in observations)
    assert {dict(o.attributes or {})["sweep_name"] for o in observations} == set(
        _PRODUCTION_SWEEP_NAMES
    )


def test_sweep_success_gauge_dimensions_are_the_sweep_name_enum_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``taskq.maintenance_leader.sweep_last_success_seconds``: dimension
    exactly {sweep_name}; a fresh cache per test so the process-global
    stamp left by other tests cannot widen the assertion."""
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})
    for name in _PRODUCTION_SWEEP_NAMES:
        otel_mod.record_sweep_success(name)

    observations = list(otel_mod._observe_sweep_success(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: same callback-observation pattern as above.

    assert {tuple(dict(o.attributes or {})) for o in observations} == {("sweep_name",)}
    assert {dict(o.attributes or {})["sweep_name"] for o in observations} == set(
        _PRODUCTION_SWEEP_NAMES
    )
    # The observation payload is a wall-clock stamp, not an elapsed age:
    # a sane stamp is in the recent past, not the epoch or the far future.
    import time

    now = time.time()
    assert all(0 < now - float(o.value) < 60 for o in observations)


# ── The ``queue`` label cap on the job-side instruments ────────────────
#
# ``queue`` is the one caller-supplied label on the job-side instruments
# (published/consumed messages, dispatch/process duration): validated for
# charset, never bounded. The pins below hold the contract documented in
# obs/_otel.py: the first ``_MAX_QUEUE_LABEL_VALUES`` distinct names a
# process sees keep their own series; every name past the cap collapses
# onto the fixed ``_other_`` value, so the series count is hard-bounded
# at cap + 1 no matter how many distinct queues the callers mint.

_JOB_SIDE_INSTRUMENTS: tuple[tuple[str, str], ...] = (
    ("_published_messages", "messaging.client.published.messages"),
    ("_dispatch_duration", "taskq.dispatch.duration"),
    ("_consumed_messages", "messaging.client.consumed.messages"),
    ("_process_duration", "messaging.process.duration"),
)


@pytest.fixture
def job_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instruments for the four job-side emitters, enabled."""
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-queue-cardinality")
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    # The admitted-queue set is process-global admission state; a fresh set
    # per test keeps one test's queues from widening another's assertion.
    monkeypatch.setattr(otel_mod, "_queue_label_values", set())
    for attr, name, kind in (
        *(
            (attr, name, "counter")
            for attr, name in _JOB_SIDE_INSTRUMENTS
            if attr in ("_published_messages", "_consumed_messages")
        ),
        *(
            (attr, name, "histogram")
            for attr, name in _JOB_SIDE_INSTRUMENTS
            if attr in ("_dispatch_duration", "_process_duration")
        ),
    ):
        monkeypatch.setattr(
            otel_mod,
            attr,  # pyright: ignore[reportArgumentType]
            meter.create_counter(name, unit="1")
            if kind == "counter"
            else meter.create_histogram(name, unit="s"),
        )
    return reader


def _series_attributes(reader: InMemoryMetricReader, name: str) -> list[dict[str, AttributeValue]]:
    """Attribute dicts of every data point (counter OR histogram) for *name*."""
    data = reader.get_metrics_data()
    assert data is not None
    return [
        dict(point.attributes or {})
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]


def _emit_all_four(actor: str, queue: str) -> None:
    """One emission through each of the four job-side emitters."""
    obs_mod.record_published_message(actor, queue)
    obs_mod.record_dispatch_duration(queue, 0.001)
    obs_mod.record_consumed_message(actor, queue, outcome="succeeded")
    obs_mod.record_process_duration(actor, queue, 0.001)


def test_queue_label_cap_and_overflow_label_are_pinned() -> None:
    """The cap is the ~100-values-per-dimension ceiling Azure's guidance
    sets (the same number the cardinality constitution cites), and the
    overflow label is the fixed ``_other_`` string — both are contract,
    not implementation detail."""
    assert otel_mod._MAX_QUEUE_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]  # Why: the pin IS the point of the test.
    assert otel_mod._QUEUE_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]


def test_distinct_queues_beyond_the_cap_do_not_grow_series(
    job_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the cap at 3, 32 distinct queue names must produce exactly 4
    series per instrument (3 admitted + ``_other_``) — minting more queue
    names past the cap must not mint more series. Uses a small cap so the
    admission boundary is exercised surgically; the real value is pinned
    by :func:`test_queue_label_cap_and_overflow_label_are_pinned`."""
    monkeypatch.setattr(otel_mod, "_MAX_QUEUE_LABEL_VALUES", 3)  # pyright: ignore[reportPrivateUsage]  # Why: shrink the cap to reach the overflow boundary in a handful of emissions.

    for i in range(32):
        _emit_all_four("actor_0", f"queue_{i}")
    for _attr, name in _JOB_SIDE_INSTRUMENTS:
        points = _series_attributes(job_reader, name)
        assert len(points) == 4, f"{name} minted {len(points)} series for 32 queues"
        assert {attrs["queue"] for attrs in points} == {
            "queue_0",
            "queue_1",
            "queue_2",
            "_other_",
        }


def test_overflow_queues_aggregate_under_the_fixed_other_label(
    job_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Overflow emissions are not dropped, only merged: the ``_other_``
    series carries the summed count of everything past the cap, and the
    admitted queues keep their real names alongside it."""
    monkeypatch.setattr(otel_mod, "_MAX_QUEUE_LABEL_VALUES", 3)  # pyright: ignore[reportPrivateUsage]

    for i in range(12):  # 3 admitted + 9 overflow
        obs_mod.record_published_message("actor_0", f"queue_{i}")

    points = _series_attributes(job_reader, "messaging.client.published.messages")
    assert {attrs["queue"] for attrs in points} == {"queue_0", "queue_1", "queue_2", "_other_"}
    # Re-read the raw points for the aggregation assertion.
    data = job_reader.get_metrics_data()
    assert data is not None
    by_queue = {
        dict(p.attributes or {})["queue"]: int(p.value)
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "messaging.client.published.messages"
        for p in m.data.data_points
        if isinstance(p, NumberDataPoint)
    }
    assert set(by_queue) == {"queue_0", "queue_1", "queue_2", "_other_"}
    assert by_queue["queue_0"] == 1 and by_queue["queue_1"] == 1 and by_queue["queue_2"] == 1
    assert by_queue["_other_"] == 9, "overflow samples were dropped instead of merged"


def test_queues_within_the_cap_keep_their_real_names(
    job_reader: InMemoryMetricReader,
) -> None:
    """Steady-state traffic on a handful of real queues is unaffected:
    every admitted name stays its own series with its own label."""
    _emit_all_four("actor_0", "critical")
    _emit_all_four("actor_0", "default")

    for _attr, name in _JOB_SIDE_INSTRUMENTS:
        points = _series_attributes(job_reader, name)
        assert {attrs["queue"] for attrs in points} == {"critical", "default"}

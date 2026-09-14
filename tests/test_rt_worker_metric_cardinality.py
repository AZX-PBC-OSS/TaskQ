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

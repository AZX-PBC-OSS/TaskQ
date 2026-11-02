"""The runaway-fan-out DETECTION primitive, producer side.

``tick_cron``'s catch-up branch (``cron_loop._plan_fire``) SKIPS the missed
slots of a schedule whose ``next_fire_at`` fell behind
``server_now - cron_catch_up_window`` — the system ADMITTING it is losing
the race (fires it cannot even attempt). Until now that admission was
LOG-ONLY: no counter, no gauge, no per-schedule metric, so "jobs clearing
slower than the cron period" (the runaway) was invisible on the metrics
plane.

These tests pin the producer side of the runaway detection:

1. ``taskq.cron.skipped_slots`` — a Counter, labeled by ``actor`` (capped
   like the sibling cron counters), counting the schedule occurrences a
   fire attempt dropped, in FIRE UNITS (not seconds).
2. ``taskq.cron.slots_behind`` — an observable gauge, per actor, carrying
   the most recent observed skip DEPTH (how many fire-units behind the
   schedule was when it skipped), published from the tick's committed
   emission.
3. The lag rides the existing surfaces too: the ``cron missed slots
   skipped`` warning, the ``cron fired`` log line and the planning
   ``cron fire`` span all carry ``skipped_slots``.
4. The runaway shape itself: a cron schedule whose fires land slower than
   its period (the fan-out config-safety pin) — the schedule still fires,
   the dropped slots are COUNTED (never silently vanished), the durable
   ``cron_schedule_id`` stamp rides the enqueued job, and the
   budget-deferral / auto-disable machinery engages per its own design.

The clearance comparison itself (fires-vs-clearance SQL) lives in
``taskq.insights``; this module only guarantees the producer-side facts
that predicate reads.

Pure-Python, no PG required; the tick is driven against the recording
fake connection from ``tests/test_cron_loop.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import pytest
import structlog.testing
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import JobFilter
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.otel import (
    collect_metrics,
    counter_data_points,
    counter_value,
    setup_tracer,
)
from taskq.worker import cron_loop

from .test_cron_loop import (
    _NOW,
    _cron_settings,
    _FakeCronConn,
    _make_actor_config_row,
    _make_schedule_row,
    _SteppableMonotonic,
    _success_updates,
    _tick,
)

_WORKER_ID = UUID("00000000-0000-0000-0000-0000000000d4")
_SKIP_COUNTER = "taskq.cron.skipped_slots"
_SLOTS_BEHIND_GAUGE = "taskq.cron.slots_behind"


@pytest.fixture
def metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the skip counter and lag gauge.

    The skip counter resolves through :func:`taskq.obs._otel._lazy_counter`
    at call time, so patching ``get_meter`` is enough. The lag gauge is a
    module-level observable gauge bound at import time; the test re-registers
    the SAME production callback on a test-scoped meter over a fresh cache,
    the established pattern (``tests/test_queue_depth_gauge_bounded.py``).
    """
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
    )
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: meter)
    # raising=False: the gauge cache and callback do not exist before the
    # producer lands - the red run must reach each test's own assertion.
    monkeypatch.setattr(otel_mod, "_cron_slots_behind_cache", {}, raising=False)
    observe = getattr(otel_mod, "_observe_cron_slots_behind", None)
    if observe is not None:
        monkeypatch.setattr(
            otel_mod,
            "_cron_slots_behind_gauge",
            meter.create_observable_gauge(
                _SLOTS_BEHIND_GAUGE,
                callbacks=[observe],
            ),
        )
    return reader


def _overdue_row(
    *,
    actor: str = "runaway_actor",
    schedule_id: UUID | None = None,
    overdue: timedelta = timedelta(minutes=10),
    cron_expr: str = "*/5 * * * *",
    payload_factory: str | None = None,
) -> object:
    """A schedule past the one-period catch-up window.

    ``*/5 * * * *`` owed a slot 10 minutes ago; the server clock reads
    10:05 and the window is one period (300s), so the owed slot is beyond
    it: the tick recomputes to 10:10 and DROPS the occurrences at 09:55,
    10:00 and 10:05 — three skipped slots, in fire units.
    """
    return _make_schedule_row(
        actor=actor,
        cron_expr=cron_expr,
        payload_factory=payload_factory,
        next_fire_at=_NOW - overdue,
        schedule_id=schedule_id,
    )


def _runaway_settings(window_seconds: str = "300") -> WorkerSettings:
    """Cron settings with an explicit catch-up window: the default helper
    pins the 1-hour window, and the runaway shapes need it in fire-unit
    terms (one or a few periods)."""
    from tests.test_leader import _worker_settings

    return _worker_settings(
        "postgresql://x:x@localhost/x",
        CRON_CATCH_UP_WINDOW=window_seconds,
        CRON_AUTO_DISABLE_THRESHOLD="3",
    )


def _runaway_tick(
    *,
    schedule_id: UUID | None = None,
    actor: str = "runaway_actor",
    window_seconds: str = "300",
) -> tuple[_FakeCronConn, WorkerSettings, InMemoryBackend]:
    conn = _FakeCronConn(
        schedule_rows=[_overdue_row(actor=actor, schedule_id=schedule_id)],
        actor_config_rows=[_make_actor_config_row(actor=actor)],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    settings = _runaway_settings(window_seconds)
    return conn, settings, backend


def _gauge_points(reader: InMemoryMetricReader) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == _SLOTS_BEHIND_GAUGE:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def _gauge_depths(reader: InMemoryMetricReader) -> dict[str, int]:
    return {
        str(dp.attributes["actor"]): int(dp.value) for dp in _gauge_points(reader) if dp.attributes
    }


# ── piece 1: the skipped-slots counter ────────────────────────────────


async def test_a_schedule_past_the_window_counts_its_skipped_slots(
    metric_reader: InMemoryMetricReader,
) -> None:
    """next_fire_at two fire-units past a one-period window → the tick
    fires once and the counter records THREE dropped occurrences (09:55,
    10:00, 10:05) under the schedule's actor — in FIRE UNITS, not seconds."""
    conn, settings, backend = _runaway_tick()

    fired = await _tick(conn, settings, backend)

    assert fired == 1
    points = counter_data_points(metric_reader, _SKIP_COUNTER)
    assert [(p.value, dict(p.attributes or {})) for p in points] == [
        (3, {"actor": "runaway_actor"})
    ], "the three dropped occurrences (09:55, 10:00, 10:05) must land on the counter"


async def test_a_within_window_miss_moves_no_skip_counter(
    metric_reader: InMemoryMetricReader,
) -> None:
    """next_fire_at 30 minutes old against a 1-hour window — the missed
    slot is CAUGHT UP (fired late), not skipped: the counter must not
    move, the skip path is the beyond-window admission only."""
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="late_actor",
                next_fire_at=_NOW - timedelta(minutes=30),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="late_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    settings = _cron_settings()

    fired = await _tick(conn, settings, backend)

    assert fired == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0


async def test_skipped_slots_counter_accumulates_across_fire_attempts(
    metric_reader: InMemoryMetricReader,
) -> None:
    """A second runaway period on the same actor (another overdue schedule)
    adds its own dropped occurrences to the cumulative counter — the series
    an operator's rate alert reads across periods."""
    await _tick(*_runaway_tick(schedule_id=new_uuid()))

    conn2, settings2, backend2 = _runaway_tick(schedule_id=new_uuid())
    await _tick(conn2, settings2, backend2)

    assert counter_value(metric_reader, _SKIP_COUNTER) == 6, (
        "two runaway periods, three dropped slots each: the cumulative "
        "counter is the sum the alert rate reads"
    )


async def test_the_skip_counter_actor_label_is_capped(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The counter's actor label admits through the same first-100-then-
    overflow discipline as the sibling cron counters (schedule rows accept
    any actor string at creation time): past the cap, names collapse onto
    ``_other_`` instead of minting a series per tenant string."""
    monkeypatch.setattr(otel_mod, "_cron_actor_label_values", set())
    cap = otel_mod._MAX_ACTOR_LABEL_VALUES

    for i in range(cap + 5):
        conn, settings, backend = _runaway_tick(actor=f"tenant-actor-{i}")
        await _tick(conn, settings, backend)

    points = counter_data_points(metric_reader, _SKIP_COUNTER)
    labels = {str(dict(p.attributes or {})["actor"]) for p in points}
    assert len(labels) <= cap + 1
    assert "_other_" in labels, (
        "past the cap the skips collapse onto the fixed _other_ label - "
        "the same discipline as taskq.cron.budget_deferrals"
    )


# ── piece 2: the lag signal on the existing surfaces ──────────────────


async def test_the_missed_slots_warning_carries_the_skip_count(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The 'cron missed slots skipped' WARNING names how far behind the
    schedule was: skipped_slots in fire units, beside the schedule ids."""
    conn, settings, backend = _runaway_tick()

    with structlog.testing.capture_logs() as logs:
        await _tick(conn, settings, backend)

    warnings = [e for e in logs if e["event"] == "cron missed slots skipped"]
    assert len(warnings) == 1
    entry = warnings[0]
    assert entry["skipped_slots"] == 3
    assert entry["actor"] == "runaway_actor"
    assert entry["kind"] == "cron_fire"


async def test_the_cron_fired_log_line_carries_skipped_slots(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The committed 'cron fired' line carries the fire's skip depth, so a
    log-derived timeline of one schedule shows exactly where the drops were."""
    conn, settings, backend = _runaway_tick()

    with structlog.testing.capture_logs() as logs:
        await _tick(conn, settings, backend)

    fired_lines = [e for e in logs if e["event"] == "cron fired"]
    assert len(fired_lines) == 1
    assert fired_lines[0]["skipped_slots"] == 3


async def test_a_clean_fire_reports_zero_skipped_slots(
    metric_reader: InMemoryMetricReader,
) -> None:
    """A fire with nothing to skip reports skipped_slots=0 on the same
    line: the field's ABSENCE must never be the only difference between a
    clean fire and a skipping one."""
    conn = _FakeCronConn(
        schedule_rows=[_make_schedule_row(actor="punctual_actor", next_fire_at=_NOW)],
        actor_config_rows=[_make_actor_config_row(actor="punctual_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    settings = _cron_settings()

    with structlog.testing.capture_logs() as logs:
        fired = await _tick(conn, settings, backend)

    assert fired == 1
    fired_lines = [e for e in logs if e["event"] == "cron fired"]
    assert len(fired_lines) == 1
    assert fired_lines[0]["skipped_slots"] == 0


async def test_the_planning_span_carries_the_skip_depth(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 'cron fire' PRODUCER span — the per-schedule attribution surface
    where cardinality is free — carries taskq.cron.skipped_slots."""
    _, exporter = setup_tracer(monkeypatch)
    conn, settings, backend = _runaway_tick()

    await _tick(conn, settings, backend)

    span = exporter.span_named("cron fire")
    assert span is not None
    attrs = dict(span.attributes or {})
    assert attrs.get("taskq.cron.skipped_slots") == 3


async def test_the_slots_behind_gauge_reads_the_latest_observed_depth(
    metric_reader: InMemoryMetricReader,
) -> None:
    """After a skipping tick the gauge reads the actor's observed depth (3);
    after the schedule's next CLEAN fire the same series reads 0 — the
    catch-up skip advanced next_fire_at into the future, so the depth
    decays on the next observation, never stranding a stale level."""
    conn, settings, backend = _runaway_tick()
    await _tick(conn, settings, backend)
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}

    clean_conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="runaway_actor",
                cron_expr="*/5 * * * *",
                next_fire_at=_NOW + timedelta(minutes=5),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="runaway_actor")],
    )
    await _tick(clean_conn, settings, backend)

    assert _gauge_depths(metric_reader) == {"runaway_actor": 0}, (
        "the next clean fire re-observes the actor at depth 0 - a behind "
        "schedule that caught up must not keep reporting an old depth"
    )


async def test_the_gauge_actor_label_is_capped(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gauge's actor label admits through the same cap as the counter's:
    past the cap the depth collapses onto ``_other_``."""
    monkeypatch.setattr(otel_mod, "_cron_actor_label_values", set())
    cap = otel_mod._MAX_ACTOR_LABEL_VALUES

    for i in range(cap + 5):
        otel_mod.update_cron_slots_behind({f"tenant-actor-{i}": 2})

    depths = _gauge_depths(metric_reader)
    assert len(depths) <= cap + 1
    assert depths.get("_other_") == 2, (
        "past the cap the depth collapses onto the fixed _other_ label - "
        "the same discipline as taskq.cron.budget_deferrals"
    )


# ── piece 4: the runaway shape, pinned ────────────────────────────────


async def test_runaway_fan_out_shape_fires_survive_and_are_counted(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The user's exact scenario, pinned: a cron-triggered sync fanning out
    to actor jobs that take longer than the cron period (here: the leader's
    effective fire cadence for a */5 schedule fell two periods behind a
    one-period catch-up window).

    What the system does TODAY, and what this pins:
    - the schedule still FIRES (one fire per attempt — the fire plane never
      starves to zero);
    - the fire's job carries the durable cron_schedule_id stamp, so the
      clearance comparison in taskq.insights can join fires to the schedule;
    - the success UPDATE advances next_fire_at strictly into the future:
      history is dropped BY DESIGN, never queued for replay;
    - every dropped occurrence is COUNTED (the counter) and its depth is
      VISIBLE (the gauge + the log/span fields) — nothing vanishes silently;
    - no failure was recorded: the budget-deferral / auto-disable machinery
      (a different, budget-shaped lag) stays out of a healthy-factory runaway.
    """
    schedule_id = new_uuid()
    conn, settings, backend = _runaway_tick(schedule_id=schedule_id)

    with structlog.testing.capture_logs() as logs:
        fired = await _tick(conn, settings, backend)

    assert fired == 1, "the runaway schedule still fires every attempt"

    # The durable stamp: the enqueued job names its schedule, so the
    # fires-vs-clearance SQL can attribute every fan-out job.
    jobs = await backend.list_jobs(JobFilter(actor="runaway_actor"))
    assert len(jobs) == 1
    metadata = jobs[0].metadata
    assert metadata.get("cron_schedule_id") == str(schedule_id), (
        "the fan-out job must carry the durable cron_schedule_id stamp"
    )

    # History is dropped by design, never queued: next_fire_at lands in
    # the future.
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _, args = success_updates[0]
    next_fires: object = args[1]
    assert isinstance(next_fires, list)
    next_fire_arg: object = next_fires[0]
    assert isinstance(next_fire_arg, datetime)
    assert next_fire_arg > _NOW

    # The dropped slots are counted, not vanished: three dropped
    # occurrences on the counter, depth 3 on the gauge, the skip depth on
    # both log lines.
    assert counter_value(metric_reader, _SKIP_COUNTER) == 3
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}
    assert next(e for e in logs if e["event"] == "cron missed slots skipped")["skipped_slots"] == 3
    assert next(e for e in logs if e["event"] == "cron fired")["skipped_slots"] == 3

    # The budget-deferral / auto-disable machinery did NOT engage: nothing
    # failed, nothing was suppressed — the runaway here is pure lag, and
    # its only trace is the skip telemetry above.
    assert not any("consecutive_failures = f.consecutive" in sql for sql, _ in conn.execute_calls)
    assert not any("SET next_fire_at = f.next_fire" in sql for sql, _ in conn.execute_calls)


async def test_a_budget_deferred_overdue_schedule_still_counts_its_skips(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The silent-loss probe: OVERDUE (beyond-window) schedules whose fires
    are then DEFERRED for tick budget. The skips already happened in
    planning and the suppression UPDATE durably advances next_fire_at past
    the owed slots — if the counter only counted committed SUCCESSES, this
    shape would drop the slots from the metrics plane entirely. It must
    not: the deferral carries the skip count onto the counter.

    The budget consumer is the MORE overdue schedule (next_fire_at order),
    so it skips AND consumes the funded grant; the second schedule skips
    and defers with nothing left to fund its factory."""
    consumer_id, deferred_id = new_uuid(), new_uuid()
    conn = _FakeCronConn(
        schedule_rows=[
            _overdue_row(
                actor="consumer_actor",
                schedule_id=consumer_id,
                overdue=timedelta(minutes=15),
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._full_grant_unit_factory",
            ),
            _overdue_row(
                actor="deferred_runaway_actor",
                schedule_id=deferred_id,
                overdue=timedelta(minutes=10),
                cron_expr="*/5 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="consumer_actor"),
            _make_actor_config_row(actor="deferred_runaway_actor"),
        ],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    settings = _runaway_settings()

    fake_time = _SteppableMonotonic(100.0)

    async def _grant_consuming_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        """The full-grant factory: consumes exactly its granted deadline of
        the tick's elapsed budget and succeeds, leaving nothing funded for
        the schedule behind it (the ``tests/test_cron_loop.py`` idiom)."""
        fake_time.advance(timeout_s or 0.0)
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _grant_consuming_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)

    fired = await _tick(conn, settings, backend)

    assert fired == 1, "the consumer fires; the deferred schedule does not"
    points = counter_data_points(metric_reader, _SKIP_COUNTER)
    by_actor = {dict(p.attributes or {})["actor"]: p.value for p in points}
    # consumer_actor: owed 09:50 on an hourly expr, recomputed to 11:00 =
    # two dropped occurrences (09:50, 10:00). deferred_runaway_actor: owed
    # 09:55 on */5, recomputed to 10:10 = three dropped occurrences (09:55,
    # 10:00, 10:05).
    assert by_actor.get("consumer_actor") == 2
    assert by_actor.get("deferred_runaway_actor") == 3, (
        "a budget-deferred fire of an overdue schedule durably advanced "
        "next_fire_at past the owed slots - the skipped slots must be "
        "counted on the same series as any other skip, not lost"
    )


async def test_the_gauge_publishes_from_the_committed_emission(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gauge publication rides the tick's commit-gated emission, the
    same discipline as every other row-describing claim: spy
    ``update_cron_slots_behind`` and confirm a rollback cannot leave a
    depth on the gauge for a tick the database never kept."""
    published: list[dict[str, int]] = []
    real_update = otel_mod.update_cron_slots_behind

    def _spy(data: dict[str, int]) -> None:
        published.append(dict(data))
        real_update(data)

    # cron_loop binds the update function at import; patch ITS reference.
    monkeypatch.setattr(cron_loop, "update_cron_slots_behind", _spy)
    conn, settings, backend = _runaway_tick()

    await _tick(conn, settings, backend)

    assert published == [{"runaway_actor": 3}]

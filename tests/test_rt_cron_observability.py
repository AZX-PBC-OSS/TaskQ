"""Red-team attacks on the cron tick's post-write observability (real PG).

C9, at real-PG level (the fake-conn pins in ``tests/test_cron_loop.py``
cover the SQL shapes; these pin that the calls actually happen around
committed writes, with the values a real tick produces):

* ``record_cron_failure`` — ``+1`` per failed schedule, recorded under
  its actor (the metric's bounded dimension); ``-prev`` on a success
  that follows failures (the counter reset, not a bare ``-1``); and a
  mixed same-actor tick nets ``-prev`` + ``+1`` on the ONE series the
  actor's schedules share.
* ``record_published_message`` — once per fired schedule, with the
  actor and the queue from ``actor_config``.
* the ``cron fired`` / ``cron fire failed`` / ``cron schedule
  auto-disabled`` events carry the firing ``worker_id``.
* the ``cron fire`` span carries ``taskq.cron_schedule_id`` on both the
  planning-loop (success) path and the write-failure strike path — the
  per-schedule attribution channel since the metric's relabel (#157).
* the ``-> int`` return equals the number of successes — including a
  mixed batch where planning failures and successes share one tick.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.trace import SpanKind, StatusCode

import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import setup_tracer
from taskq.worker import cron_loop
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    JobIdCollisionBackend,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_obs_actor"
_QUEUE = "rt_obs_queue"
_BAD_FACTORY = "nonexistent.module.fn"


class TestObservabilityOnCommit:
    async def test_success_after_failures_resets_counter_and_publishes(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A schedule with 2 prior failures fires: the tick records
        ``record_cron_failure(actor, -2)`` (the full reset), one
        ``record_published_message(actor, queue)``, a ``cron fired`` event
        with the worker id, and the DB row shows the reset."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="recovering",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
            consecutive_failures=2,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 1
        assert cron_failure_calls == [(_ACTOR, -2)], (
            "a success after 2 failures must reset the UpDownCounter by the FULL "
            f"previous count; saw {cron_failure_calls}"
        )
        assert published == [(_ACTOR, _QUEUE)]
        fired_events = [e for e in captured if e["event"] == "cron fired"]
        assert len(fired_events) == 1
        assert fired_events[0]["worker_id"] == str(worker_id)
        assert fired_events[0]["actor"] == _ACTOR

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

    async def test_failures_count_up_and_log_worker_attribution(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two failing schedules in one tick: ``+1`` per schedule in due
        order, one ``cron fire failed`` event each with worker id and the
        raw error text, return 0."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        first_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fails-first",
            cron_expr=_HOURLY,
            next_fire_at=due - _ONE_MINUTE,
            payload_factory=_BAD_FACTORY,
        )
        second_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fails-second",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 0
        assert cron_failure_calls == [
            (_ACTOR, 1),
            (_ACTOR, 1),
        ], f"one +1 per failed schedule, in due order; saw {cron_failure_calls}"
        assert published == [], "a failed fire publishes nothing"

        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 2
        assert {e["worker_id"] for e in failed} == {str(worker_id)}
        # Per-schedule attribution lives on the log line now, not the
        # metric: both failures are one actor, and the events name the
        # exact schedules.
        assert {e["schedule_id"] for e in failed} == {str(first_id), str(second_id)}
        assert all("nonexistent" in e["error"] for e in failed), (
            "the raw resolution error must reach the log event"
        )

    async def test_mixed_batch_return_counts_successes_only(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two successes and one planning failure in one tick: return 2,
        two published messages, one +1 failure record, and the log stream
        shows two ``cron fired`` and one ``cron fire failed``."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        bad_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="mixed-bad",
            cron_expr=_HOURLY,
            next_fire_at=due - _ONE_MINUTE,
            payload_factory=_BAD_FACTORY,
        )
        for i in range(2):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"mixed-good-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
            )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 2, "the return value must equal the successes, not the batch"
        assert cron_failure_calls == [(_ACTOR, 1)]
        assert published == [(_ACTOR, _QUEUE), (_ACTOR, _QUEUE)]
        events = [e["event"] for e in captured]
        assert events.count("cron fired") == 2
        assert events.count("cron fire failed") == 1
        assert all(
            e["worker_id"] == str(worker_id)
            for e in captured
            if e["event"] in ("cron fired", "cron fire failed")
        )
        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert [e["schedule_id"] for e in failed] == [str(bad_id)], (
            "the metric lost the per-schedule label; the log event must carry it"
        )

    async def test_mixed_tick_same_actor_nets_on_one_series(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One actor, two schedules, one tick: the recovering schedule
        (2 prior failures, both emitted to THIS process's counter by
        earlier ticks) fires and records ``-2``; a fresh schedule fails
        and records ``+1``; the actor's ONE series nets to 1 — the sum
        of the actor's DB counts after the tick. The netting was only
        ever pinned via direct emitter calls before; this drives it
        through a real tick, real instrument and all."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        _FLAKY_STATE["calls"] = []
        recovering_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="recovering",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_FLAKY_FACTORY,
        )
        fresh_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fresh-failure",
            cron_expr=_HOURLY,
            # Two hours out: not selectable by any tick before _make_due
            # pulls it in, even if the wall clock crosses an hour boundary
            # mid-test (a one-hour seed leaves a sub-second window where
            # the boundary itself would make it due early).
            next_fire_at=due + timedelta(hours=2),
            payload_factory=_BAD_FACTORY,
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cron-netting")
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        cron_failure_calls: list[tuple[str, int]] = []
        real_record_cron_failure = otel_mod.record_cron_failure

        def _spy(actor: str, delta: int) -> None:
            cron_failure_calls.append((actor, delta))
            real_record_cron_failure(actor, delta)

        monkeypatch.setattr(cron_loop, "record_cron_failure", _spy)
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        async def _make_due(*schedule_ids: UUID) -> None:
            await clean_pg_conn.execute(
                f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
                f"WHERE id = ANY($1::uuid[])",
                [*schedule_ids],
                due,
            )

        # Two failing ticks build the recovering schedule's DB count (and
        # this process's counter) to 2 — the prior history a real -2 reset
        # assumes; the fresh schedule stays undelivered (next hour).
        assert await _tick() == 0
        await _make_due(recovering_id)
        assert await _tick() == 0
        assert cron_failure_calls == [(_ACTOR, 1), (_ACTOR, 1)]
        assert published == []

        # The mixed tick: the recovering schedule fires (factory call 3
        # succeeds) while the fresh schedule fails for the first time.
        await _make_due(recovering_id, fresh_id)
        cron_failure_calls.clear()
        fired = await _tick()

        assert fired == 1
        assert cron_failure_calls == [(_ACTOR, -2), (_ACTOR, +1)], (
            "the success reset (-prev) and the fresh failure (+1) must both "
            f"land under the actor, successes loop first; saw {cron_failure_calls}"
        )
        assert published == [(_ACTOR, _QUEUE)]

        data = reader.get_metrics_data()
        assert data is not None
        points = [
            (dict(p.attributes or {}), int(p.value))
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "taskq.cron.consecutive_failures"
            for p in m.data.data_points
            if isinstance(p, NumberDataPoint)
        ]
        assert points == [({"actor": _ACTOR}, 1)], (
            "the actor's schedules share ONE series and the mixed tick's "
            f"-2/+1 must net to 1 on it; saw {points}"
        )

        # The DB is the cross-check: the series value equals the actor's
        # summed schedule counts — the netting invariant, end to end.
        recovering = await schedule_row(clean_pg_conn, schema, recovering_id)
        fresh = await schedule_row(clean_pg_conn, schema, fresh_id)
        assert recovering["consecutive_failures"] == 0
        assert fresh["consecutive_failures"] == 1


class TestCronFireSpanAttribution:
    """``taskq.cron_schedule_id`` on the ``cron fire`` span — the
    per-schedule attribution channel since the metric's relabel (#157).
    Both span creation sites must stamp it: the planning loop (success
    path) and the write-failure strike path."""

    async def test_success_path_span_carries_schedule_id(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="spans-ok",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
        )

        worker_id = new_uuid()
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, worker_id
            )

        assert fired == 1
        spans = exporter.spans_named("cron fire")
        assert len(spans) == 1, f"expected one planning-loop span; saw {len(spans)}"
        span = spans[0]
        assert span.kind == SpanKind.PRODUCER
        attrs = dict(span.attributes or {})
        assert attrs["taskq.cron_schedule_id"] == str(schedule_id), (
            "the planning-loop cron fire span must carry the per-schedule "
            "attribution the metric lost"
        )
        assert attrs["cron_schedule_name"] == _ACTOR
        assert attrs["taskq.worker_id"] == str(worker_id)

    async def test_strike_path_span_carries_schedule_id(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A write-failed plan gets its OWN ``cron fire`` span from
        ``_strike_plans`` (the planning-loop span for the same schedule
        already closed OK) — that second span must carry the same
        ``taskq.cron_schedule_id`` stamp, or the write-failure path loses
        the per-schedule attribution the relabel moved onto spans."""
        _, exporter = setup_tracer(monkeypatch)
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="struck",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
        )

        worker_id = new_uuid()
        collider_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            backend = JobIdCollisionBackend(settings, collider_conn=collider_conn, schema=schema)
            async with clean_pg_conn.transaction():
                fired = await tick_cron(clean_pg_conn, settings, backend, schema, worker_id)
        finally:
            await collider_conn.close()

        assert fired == 0
        spans = exporter.spans_named("cron fire")
        struck = [s for s in spans if s.status.status_code == StatusCode.ERROR]
        planned = [s for s in spans if s.status.status_code != StatusCode.ERROR]
        assert len(struck) == 1, (
            f"the strike path must mark its own span ERROR; saw {len(struck)} "
            f"of {len(spans)} cron fire spans in ERROR"
        )
        assert len(planned) == 1, "the planning-loop span for the same schedule closed OK"
        struck_attrs = dict(struck[0].attributes or {})
        assert struck_attrs["taskq.cron_schedule_id"] == str(schedule_id), (
            "the strike-path span must carry the per-schedule attribution"
        )
        assert struck_attrs["taskq.worker_id"] == str(worker_id)
        assert dict(planned[0].attributes or {})["taskq.cron_schedule_id"] == str(schedule_id)


# ── Helpers ────────────────────────────────────────────────────────────

_ONE_MINUTE = timedelta(minutes=1)

_FLAKY_FACTORY = "tests.test_rt_cron_observability.flaky_twice_then_ok"

_FLAKY_STATE: dict[str, object] = {"calls": []}
"""Call log for :func:`flaky_twice_then_ok` — module scope because the
cron loop resolves the factory by dotted path inside the tick, so the
test and the factory can only share state through the module (the
harness ``_WEDGE_STATE`` pattern).  Reset at each using test's start."""


async def flaky_twice_then_ok() -> dict[str, object]:
    """Payload factory: fail the first two fires, then recover.

    Dotted path: ``tests.test_rt_cron_observability.flaky_twice_then_ok``.
    """
    calls = _FLAKY_STATE["calls"]
    assert isinstance(calls, list)
    calls.append(1)
    if len(calls) <= 2:
        raise RuntimeError("flaky: the first two fires fail")
    return {"recovered": True}

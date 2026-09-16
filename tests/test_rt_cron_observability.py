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


class TestAutoDisableTelemetryFollowsTheCommit:
    """An auto-disable that the COMMIT rolled back must not be announced.

    Auto-disable is the most consequential thing cron telemetry reports:
    the ``cron schedule auto-disabled`` log line and the disabled-count
    gauge tell operators a schedule has stopped firing and needs human
    attention. ``tick_cron`` runs inside the leader's transaction, so the
    COMMIT that actually flips ``enabled`` to false happens after
    ``tick_cron`` returns. When that COMMIT fails the whole transaction
    rolls back and the schedule stays enabled and keeps its old strike
    count. Announcing the auto-disable anyway sends operators to
    investigate a schedule that is still running, and moves the gauge
    away from the state the database holds. Emission must therefore be
    gated on the transaction committing, not merely on every statement
    of the tick having run.
    """

    async def test_failed_commit_announces_no_auto_disable(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A schedule one strike short of the auto-disable threshold
        fails, so the tick would disable it and announce that. The COMMIT
        then fails, rolling the disable back. The row must still be
        enabled at its old count, and no auto-disable log line, ERROR
        ``cron fire`` span or failure-metric delta may survive.
        """
        _, exporter = setup_tracer(monkeypatch)
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="auto-disable-when-commit-fails",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
            payload_factory=_BAD_FACTORY,
            # Threshold is 3, so this tick's strike is the disabling one.
            consecutive_failures=2,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )

        # DEFERRABLE INITIALLY DEFERRED: Postgres evaluates this trigger
        # only at COMMIT time, strictly after every statement the tick
        # runs (its failures UPDATE and any telemetry emission). It always
        # raises, so the COMMIT itself fails deterministically.
        await clean_pg_conn.execute(
            f'CREATE OR REPLACE FUNCTION "{schema}".commit_gate_fail_disable() '
            "RETURNS trigger AS $$ "
            "BEGIN RAISE EXCEPTION 'commit-gate: forced commit failure'; END; "
            "$$ LANGUAGE plpgsql"
        )
        await clean_pg_conn.execute(
            "CREATE CONSTRAINT TRIGGER commit_gate_fail_disable_trg "
            f'AFTER UPDATE ON "{schema}".cron_schedules '
            "DEFERRABLE INITIALLY DEFERRED "
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".commit_gate_fail_disable()'
        )

        worker_id = new_uuid()
        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(asyncpg.RaiseError, match="commit-gate"),
        ):
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )
                assert fired == 0

        # The DB rolled the whole transaction back: still enabled, still
        # at the pre-tick strike count.
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is True
        assert row["consecutive_failures"] == 2
        assert row["last_fire_error"] is None

        disabled_events = [e for e in captured if e["event"] == "cron schedule auto-disabled"]
        assert disabled_events == [], (
            "a failed COMMIT must not announce an auto-disable it rolled "
            f"back -- the schedule is still enabled; saw {disabled_events}"
        )
        failed_events = [e for e in captured if e["event"] == "cron fire failed"]
        assert failed_events == [], (
            "a failed COMMIT must not leave a strike log line behind for "
            f"the strike it rolled back; saw {failed_events}"
        )

        spans = exporter.spans_named("cron fire")
        struck_spans = [s for s in spans if s.status.status_code == StatusCode.ERROR]
        assert struck_spans == [], (
            "a failed COMMIT must not leave an ERROR-status 'cron fire' "
            f"span behind for the strike it rolled back; saw {struck_spans}"
        )

        assert cron_failure_calls == [], (
            "a failed COMMIT must not leave a record_cron_failure metric "
            f"delta behind for the strike it rolled back; saw {cron_failure_calls}"
        )


class TestNoStrikeTelemetryWithoutACommit:
    """A cron strike that never reaches durable storage must leave no
    telemetry behind either.

    The failure telemetry a tick produces (the ERROR ``cron fire`` span,
    the ``cron fire failed`` log line, the ``record_cron_failure`` metric
    delta) describes a strike the DB is supposed to have persisted on the
    schedule row. ``tick_cron`` runs inside the leader's transaction, so
    the COMMIT that makes the strike durable happens after ``tick_cron``
    returns. When that COMMIT fails, the whole transaction rolls back and
    the schedule row keeps no record of the strike. Telemetry that went
    out anyway leaves operators reading a failure count and an error span
    for something the database says never happened, which corrupts alert
    thresholds and makes the auto-disable trail unreconstructable from
    the row. Export must therefore be gated on the transaction actually
    committing, not merely on every statement of the tick having run.
    """

    async def test_failed_commit_exports_no_strike_telemetry(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failing schedule strikes, then the COMMIT that would persist
        the strike fails. The schedule row must show no strike, and the
        log, the span exporter and the failure metric must all be
        untouched by it.
        """
        _, exporter = setup_tracer(monkeypatch)
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="no-telemetry-when-commit-fails",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
            payload_factory=_BAD_FACTORY,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )

        # DEFERRABLE INITIALLY DEFERRED: Postgres evaluates this trigger
        # only at COMMIT time, strictly after every statement the tick
        # runs (its UPDATE of consecutive_failures/last_fire_error and
        # any telemetry emission). It always raises, so the COMMIT itself
        # fails deterministically.
        await clean_pg_conn.execute(
            f'CREATE OR REPLACE FUNCTION "{schema}".commit_gate_fail() '
            "RETURNS trigger AS $$ "
            "BEGIN RAISE EXCEPTION 'commit-gate: forced commit failure'; END; "
            "$$ LANGUAGE plpgsql"
        )
        await clean_pg_conn.execute(
            "CREATE CONSTRAINT TRIGGER commit_gate_fail_trg "
            f'AFTER UPDATE ON "{schema}".cron_schedules '
            "DEFERRABLE INITIALLY DEFERRED "
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".commit_gate_fail()'
        )

        worker_id = new_uuid()
        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(asyncpg.RaiseError, match="commit-gate"),
        ):
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )
                assert fired == 0

        # The DB rolled the whole transaction back: no strike persisted.
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

        failed_events = [e for e in captured if e["event"] == "cron fire failed"]
        assert failed_events == [], (
            "a failed COMMIT must not leave a 'cron fire failed' log line "
            f"behind for the strike it rolled back; saw {failed_events}"
        )

        spans = exporter.spans_named("cron fire")
        struck_spans = [s for s in spans if s.status.status_code == StatusCode.ERROR]
        assert struck_spans == [], (
            "a failed COMMIT must not leave an ERROR-status 'cron fire' "
            f"span behind for the strike it rolled back; saw {struck_spans}"
        )

        assert cron_failure_calls == [], (
            "a failed COMMIT must not leave a record_cron_failure metric "
            f"delta behind for the strike it rolled back; saw {cron_failure_calls}"
        )

    async def test_failed_commit_exports_no_success_telemetry(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The success path needs the same commit gate as the strike path.

        A schedule with prior failures fires, so the tick would emit a
        ``cron fired`` log, a ``record_published_message`` count and a
        ``record_cron_failure`` reset for the full prior count. The COMMIT
        then fails, rolling back both the advanced ``next_fire_at`` and the
        counter reset. Exporting anyway understates the actor's failure
        gauge against a DB row that still holds the old count, and claims
        a message was published for a job the enqueue rolled back too.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="success-when-commit-fails",
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

        await clean_pg_conn.execute(
            f'CREATE OR REPLACE FUNCTION "{schema}".commit_gate_fail_ok() '
            "RETURNS trigger AS $$ "
            "BEGIN RAISE EXCEPTION 'commit-gate: forced commit failure'; END; "
            "$$ LANGUAGE plpgsql"
        )
        await clean_pg_conn.execute(
            "CREATE CONSTRAINT TRIGGER commit_gate_fail_ok_trg "
            f'AFTER UPDATE ON "{schema}".cron_schedules '
            "DEFERRABLE INITIALLY DEFERRED "
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".commit_gate_fail_ok()'
        )

        worker_id = new_uuid()
        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(asyncpg.RaiseError, match="commit-gate"),
        ):
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )
                assert fired == 1

        # The DB rolled back: the prior failure count is untouched.
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 2

        fired_events = [e for e in captured if e["event"] == "cron fired"]
        assert fired_events == [], (
            "a failed COMMIT must not leave a 'cron fired' log line behind "
            f"for the fire it rolled back; saw {fired_events}"
        )
        assert published == [], (
            "a failed COMMIT must not count a published message for the "
            f"enqueue it rolled back; saw {published}"
        )
        assert cron_failure_calls == [], (
            "a failed COMMIT must not apply the failure-count reset it "
            f"rolled back; saw {cron_failure_calls}"
        )

    async def test_a_healthy_tick_after_a_rolled_back_one_still_reports(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The commit gate is armed fresh on every tick, not once per
        connection: a tick whose COMMIT fails must not leave the
        connection permanently unable to report telemetry.

        The first tick strikes a schedule and its COMMIT is forced to
        fail, so (per the sibling test above) no telemetry survives it.
        The trigger is then dropped and a second, healthy tick runs on
        the SAME connection -- the same session the commit gate's
        listener and armed-emission map are keyed by. That second tick's
        own strike must still be reported: a rolled-back tick's stale
        arming must not shadow the next tick's.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="recovers-after-rolled-back-commit",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
            payload_factory=_BAD_FACTORY,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda actor, delta: cron_failure_calls.append((actor, delta)),
        )

        await clean_pg_conn.execute(
            f'CREATE OR REPLACE FUNCTION "{schema}".commit_gate_fail_recover() '
            "RETURNS trigger AS $$ "
            "BEGIN RAISE EXCEPTION 'commit-gate: forced commit failure'; END; "
            "$$ LANGUAGE plpgsql"
        )
        await clean_pg_conn.execute(
            "CREATE CONSTRAINT TRIGGER commit_gate_fail_recover_trg "
            f'AFTER UPDATE ON "{schema}".cron_schedules '
            "DEFERRABLE INITIALLY DEFERRED "
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".commit_gate_fail_recover()'
        )

        worker_id = new_uuid()
        with pytest.raises(asyncpg.RaiseError, match="commit-gate"):
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )
                assert fired == 0
        assert cron_failure_calls == [], "the rolled-back tick's strike must not export"

        await clean_pg_conn.execute(
            f'DROP TRIGGER commit_gate_fail_recover_trg ON "{schema}".cron_schedules'
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            schedule_id,
            hour_floor(datetime.now(UTC)),
        )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, worker_id
            )
            assert fired == 0

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 1, (
            "the second tick's strike must persist in the database"
        )
        assert cron_failure_calls == [(_ACTOR, 1)], (
            "a healthy tick on the same connection as an earlier rolled-back "
            f"one must still export its own strike; saw {cron_failure_calls} "
            "-- a connection must not lose cron telemetry permanently after "
            "one bad tick"
        )


class TestFailureGaugeIsDerivedFromTheDatabase:
    """``taskq.cron.consecutive_failures`` must report the failing-schedule
    count the database holds, not a balance this process accumulated.

    A per-process running balance can only ever be right about deltas this
    process itself applied. Schedules are enabled, disabled and deleted by
    clients, by the CLI and by the admin UI — all outside the worker that
    emits the metric — and each of those actions clears or removes a
    ``cron_schedules.consecutive_failures`` value the worker once counted
    up. A worker cannot emit another process's delta, so the balance drifts
    permanently upward: an actor whose only failing schedule was deleted a
    month ago still reports a non-zero failure level forever, and an alert
    on the series becomes unreadable at exactly the moment an operator
    needs it.

    The contract: the value is derived from database state on each tick, so
    every out-of-process change self-corrects on the next tick and no
    stranded residue survives.
    """

    async def _cron_failure_points(
        self, reader: InMemoryMetricReader
    ) -> list[tuple[dict[str, object], int]]:
        data = reader.get_metrics_data()
        assert data is not None
        return [
            (dict(p.attributes or {}), int(p.value))
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "taskq.cron.consecutive_failures"
            for p in m.data.data_points
            if isinstance(p, NumberDataPoint)
        ]

    async def test_out_of_process_enable_disable_and_delete_self_correct(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two failing schedules build a count; another process then clears
        one and deletes the other. The next tick's emitted value must match
        what the database says — zero — with no residue from the strikes
        this worker counted.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        cleared_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="failing-then-re-enabled-elsewhere",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )
        deleted_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="failing-then-deleted-elsewhere",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )
        survivor_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="still-failing",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cron-failure-gauge")
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        async def _make_due(*schedule_ids: UUID) -> None:
            await clean_pg_conn.execute(
                f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
                "WHERE id = ANY($1::uuid[])",
                [*schedule_ids],
                due,
            )

        # All three strike once: the database and the metric agree at 3.
        assert await _tick() == 0
        assert await self._cron_failure_points(reader) == [({"actor": _ACTOR}, 3)], (
            "baseline: with three failing schedules on one actor the series "
            "must read 3 — if it does not, the rest of this test proves nothing"
        )

        # Another process intervenes between ticks: one schedule's strike
        # history is cleared (the shape a disable/re-enable leaves behind),
        # the other schedule is deleted outright. Neither action passes
        # through this worker, so neither can emit a compensating delta.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            "SET consecutive_failures = 0, last_fire_error = NULL, next_fire_at = $2 "
            "WHERE id = $1",
            cleared_id,
            due + timedelta(hours=2),
        )
        await clean_pg_conn.execute(
            f'DELETE FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            deleted_id,
        )

        # The survivor strikes again on the next tick; the database now
        # holds exactly 2 failures for the actor (the survivor's), and the
        # cleared schedule is not due so it contributes nothing.
        await _make_due(survivor_id)
        assert await _tick() == 0

        db_total = await clean_pg_conn.fetchval(
            f'SELECT COALESCE(SUM(consecutive_failures), 0) FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
            "WHERE actor = $1",
            _ACTOR,
        )
        assert int(db_total) == 2, "test premise: the survivor alone now carries two strikes"

        points = await self._cron_failure_points(reader)
        assert points == [({"actor": _ACTOR}, 2)], (
            "the reported failure level must equal the database's own sum for "
            f"the actor ({db_total}); saw {points}. A per-process balance "
            "cannot see the clear or the delete another process performed, so "
            "it strands their strikes permanently and the series never "
            "returns to zero even when no schedule is failing at all"
        )

    async def test_actor_whose_failing_schedule_is_deleted_with_nothing_due_self_corrects(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The stranded-actor case: an actor fails (its series reads 1),
        another process then deletes that schedule outright, and the actor
        has NOTHING due on the next tick — the tick's batch never touches
        it. The reconcile must still return the actor's series to the
        database's truth (zero), because the totals it reconciles against
        cover every actor with a failing schedule, not only the batch's.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        gone_actor = "rt_obs_gone_actor"
        await seed_actor_config(clean_pg_conn, schema, gone_actor, queue=_QUEUE)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        gone_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=gone_actor,
            name="failing-then-deleted-with-nothing-due",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )
        ticking_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="keeps-the-next-tick-nonempty",
            cron_expr=_HOURLY,
            next_fire_at=due + timedelta(hours=2),
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cron-gone-actor")
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        # The schedule strikes once: the database and the series agree at 1.
        assert await _tick() == 0
        points = {p[0].get("actor"): p[1] for p in await self._cron_failure_points(reader)}
        assert points.get(gone_actor) == 1, (
            "baseline: the failing actor must report one failure before the "
            f"rest of this test proves anything; saw {points}"
        )

        # Another process deletes the failing schedule outright. Nothing
        # for that actor is ever due again.
        await clean_pg_conn.execute(
            f'DELETE FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            gone_id,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            ticking_id,
            due,
        )

        # The next tick's batch contains only the other actor's schedule.
        assert await _tick() == 1, "the healthy schedule fires on this tick"

        points = {p[0].get("actor"): p[1] for p in await self._cron_failure_points(reader)}
        assert points.get(gone_actor) == 0, (
            f"the deleted schedule's actor must return to the database's truth "
            f"(zero) on the next tick even though no tick batch ever examined "
            f"it again; saw {points}. A reconcile scoped to the tick's own "
            "batch actors strands the actor's last reported level forever"
        )

    async def test_value_returns_to_zero_once_no_schedule_is_failing(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When every failing schedule is gone from the database, the next
        tick must report zero.

        This is the operator-facing property: "is any schedule failing right
        now" has to be answerable from the series. A residue-carrying
        balance answers "did anything ever fail on a worker that is still
        running", which is not an alertable question.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        failing_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="failing-then-gone",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )
        healthy_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="healthy",
            cron_expr=_HOURLY,
            next_fire_at=due + timedelta(hours=2),
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cron-failure-zero")
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        assert await _tick() == 0
        assert await self._cron_failure_points(reader) == [({"actor": _ACTOR}, 1)]

        # The failing schedule is deleted by another process entirely.
        await clean_pg_conn.execute(
            f'DELETE FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            failing_id,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            healthy_id,
            due,
        )

        assert await _tick() == 1, "the healthy schedule fires on this tick"

        points = await self._cron_failure_points(reader)
        assert points == [({"actor": _ACTOR}, 0)], (
            "with no failing schedule left in the database the actor's series "
            f"must read 0; saw {points}. Residue here means an operator "
            "cannot tell a currently-failing actor from one whose failures "
            "were resolved by a delete or a re-enable somewhere else"
        )


class TestReconciliationLeavesUntouchedActorsAlone:
    """An actor the database still shows failing keeps its reported level
    even when no schedule of its was due this tick.

    ``reconcile_cron_failures`` receives totals read over the whole
    ``cron_schedules`` table (``_actor_failure_totals``), not just the
    tick's own batch — so an actor with failing schedules that simply were
    not due this tick still appears in the totals with the count the
    database holds. Zeroing it would erase an outstanding failure count
    from the exported series while the database still holds it, exactly
    the kind of drift the reconciliation exists to prevent, just pointed
    the other way.
    """

    async def _cron_failure_points(
        self, reader: InMemoryMetricReader
    ) -> list[tuple[dict[str, object], int]]:
        data = reader.get_metrics_data()
        assert data is not None
        return [
            (dict(p.attributes or {}), int(p.value))
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "taskq.cron.consecutive_failures"
            for p in m.data.data_points
            if isinstance(p, NumberDataPoint)
        ]

    async def test_an_actor_absent_from_the_tick_keeps_its_reported_level(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two actors each carry a failing schedule. Once both are due and
        struck, one actor's schedule is pushed far into the future so a
        later tick never examines it. That later tick's reconciliation
        must not report the untouched actor as having zero failures --
        the database still holds its strike.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        untouched_actor = "rt_obs_untouched_actor"
        due = hour_floor(datetime.now(UTC))
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        await seed_actor_config(clean_pg_conn, schema, untouched_actor, queue=_QUEUE)
        touched_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="stays-due-every-tick",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )
        untouched_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=untouched_actor,
            name="goes-quiet-after-first-strike",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cron-reconcile-untouched")
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        # Both actors strike once: both series read 1.
        assert await _tick() == 0
        points = {p[0].get("actor"): p[1] for p in await self._cron_failure_points(reader)}
        assert points == {_ACTOR: 1, untouched_actor: 1}, (
            "baseline: both actors must show one failure before the rest of "
            f"this test proves anything; saw {points}"
        )

        # The untouched actor's schedule is pushed out of this tick's due
        # window; the other actor's stays due so the next tick's batch
        # includes it but never touches the untouched actor's schedule.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            untouched_id,
            due + timedelta(days=1),
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            touched_id,
            due,
        )

        assert await _tick() == 0
        row = await schedule_row(clean_pg_conn, schema, untouched_id)
        assert row["consecutive_failures"] == 1, (
            "test premise: the database still holds the untouched actor's strike"
        )

        points = {p[0].get("actor"): p[1] for p in await self._cron_failure_points(reader)}
        assert points.get(untouched_actor) == 1, (
            "a tick that never examined this actor must not zero its "
            f"reported failure level; saw {points}. The database still "
            "holds the strike -- reconciliation must only move actors the "
            "tick's own batch actually examined"
        )


class TestReconciliationRequiresADueSchedule:
    """``tick_cron`` returns early, before ``_actor_failure_totals`` is ever
    read, when no schedule anywhere is due (see the ``if not rows: return 0``
    guard ahead of the reconcile call). A failing schedule that is deleted
    while every remaining schedule (for every actor, fleet-wide) sits in the
    future never triggers a reconciling tick, so the deleted schedule's
    contribution to its actor's series has no opportunity to self-correct
    until some schedule, anywhere, becomes due again.
    """

    async def _cron_failure_points(
        self, reader: InMemoryMetricReader
    ) -> list[tuple[dict[str, object], int]]:
        data = reader.get_metrics_data()
        assert data is not None
        return [
            (dict(p.attributes or {}), int(p.value))
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for m in sm.metrics
            if m.name == "taskq.cron.consecutive_failures"
            for p in m.data.data_points
            if isinstance(p, NumberDataPoint)
        ]

    async def test_gauge_stays_stranded_while_no_schedule_is_due_anywhere(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failing schedule is deleted by another process, and nothing
        else in the table is due. The tick that follows returns 0 having
        never read the database's actor totals, so the series should still
        hold its pre-delete value at that point -- it is not yet residue,
        but it demonstrates the self-correction is gated on tick activity,
        not wall-clock time. A LATER tick, once something becomes due
        again, must still bring the series back to zero.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        failing_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="failing-then-gone-while-fleet-is-quiet",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter(
            "taskq-cron-reconcile-requires-due"
        )
        monkeypatch.setattr(otel_mod, "_otel_enabled", True)
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        async def _tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        assert await _tick() == 0
        assert await self._cron_failure_points(reader) == [({"actor": _ACTOR}, 1)]

        # Another process deletes the only failing schedule. Nothing else
        # in the whole table is due -- the fleet is quiet.
        await clean_pg_conn.execute(
            f'DELETE FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            failing_id,
        )
        still_due = await clean_pg_conn.fetchval(
            f'SELECT COUNT(*) FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "WHERE enabled = true AND next_fire_at <= statement_timestamp()"
        )
        assert int(still_due) == 0, "test premise: nothing in the table is due"

        # A tick while the fleet is quiet must return 0 having done no
        # reconciling work -- it never reaches the totals read.
        assert await _tick() == 0

        # Seed a fresh, healthy, due schedule so the NEXT tick has
        # something to examine and reconciliation actually runs.
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="wakes-the-fleet-back-up",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )
        assert await _tick() == 1, "the new healthy schedule fires on this tick"

        points = await self._cron_failure_points(reader)
        assert points == [({"actor": _ACTOR}, 0)], (
            "once a tick actually examines the table again the deleted "
            f"schedule's contribution must be gone; saw {points}. If this "
            "assertion fails, self-correction is not merely delayed until "
            "the next active tick -- it is broken outright"
        )


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

"""Red-team attacks on the cron tick's failure domains (real PG).

Three failure domains meet in one tick, and each owes different
observable behaviour:

* **Server-side enqueue failure** (C2): the batched INSERT itself fails
  on the caller's connection — a genuine ``UniqueViolationError`` on
  ``jobs_pkey`` driven here by colliding the first planned row's id with
  a committed row, through the REAL ``enqueue_batch``.  The failed
  INSERT aborts the tick's transaction, so the failure UPDATE the tick
  runs afterwards fails in turn and the tick raises to its caller.  This
  pins the honest observable: the caller sees
  ``InFailedSQLTransactionError``, NOTHING survives (no partial
  enqueues, no ``consecutive_failures`` bookkeeping, no auto-disable),
  and the original enqueue error survives only in the per-schedule
  span telemetry.
* **Client-side enqueue failure** (C2's contrast): the backend raises
  before any statement is sent — the transaction stays alive, so the
  per-schedule failure bookkeeping COMMITS and the tick returns 0
  instead of raising.  Both observables are correct for their domain;
  pinning them separately is what makes the boundary visible.
* **Planning failure isolation** (C8): an actor with no ``actor_config``
  row fails exactly one schedule, with the exact ``LookupError`` text,
  while other actors in the same batch still fire; three such ticks
  auto-disable the schedule (CASE + ``enabled = true`` guard).
* **The failures-UPDATE guard** (C3): a schedule disabled by another
  connection between the due SELECT and the UPDATE must be skipped —
  the rowcount shortfall warns, and the disabled row is neither
  re-enabled nor error-stamped.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import asyncpg
import pytest
import structlog.testing
from opentelemetry.trace import StatusCode

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import setup_tracer
from taskq.worker import cron_loop
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    CountingConn,
    GatedEnqueueBackend,
    JobIdCollisionBackend,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    wedge_events,
)

pytestmark = pytest.mark.integration

_MISSING_ACTOR = "rt_missing_actor"
_PRESENT_ACTOR = "rt_present_actor"
_LOOKUP_ERROR = f"Actor '{_MISSING_ACTOR}' not found in actor_config"


class TestServerSideEnqueueFailure:
    """C2: the batched INSERT genuinely fails on the caller's connection."""

    async def test_aborts_whole_tick_with_no_partial_writes(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A genuine jobs_pkey violation inside the batched enqueue → the
        tick raises InFailedSQLTransactionError (the doomed failure UPDATE
        surfacing the abort), the caller's rollback discards everything,
        and the original constraint error survives only in span telemetry."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)

        collide_id = new_uuid()
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            f"retry_kind) VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 1, 'transient')",
            collide_id,
            _PRESENT_ACTOR,
        )

        due = hour_floor(datetime.now(UTC))
        schedule_ids = [
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_PRESENT_ACTOR,
                name=f"collide-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
            )
            for i in range(3)
        ]
        before = [await schedule_row(clean_pg_conn, schema, sid) for sid in schedule_ids]

        backend = JobIdCollisionBackend(settings, collide_id=collide_id)
        _provider, exporter = setup_tracer(monkeypatch)

        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(asyncpg.InFailedSQLTransactionError),
        ):
            async with clean_pg_conn.transaction():
                await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == 1, (
            f"{jobs} jobs survived an aborted tick — only the pre-seeded collision "
            "row may exist; partial enqueues must not survive the rollback"
        )
        after = [await schedule_row(clean_pg_conn, schema, sid) for sid in schedule_ids]
        assert after == before, (
            "schedule rows changed under an aborted tick — no consecutive_failures "
            "bookkeeping and no auto-disable may survive the rollback"
        )

        tick_logs = [e["event"] for e in captured]
        assert "cron fire failed" not in tick_logs, (
            "per-schedule failure logs ran for writes that were rolled back — the "
            "tick must not claim failures it could not commit"
        )
        assert "cron schedule auto-disabled" not in tick_logs

        error_spans = [
            s for s in exporter.spans_named("cron fire") if s.status.status_code == StatusCode.ERROR
        ]
        assert len(error_spans) == 3, (
            "every planned fire must carry an errored 'cron fire' span when the "
            "batched enqueue fails — the original error is preserved there because "
            "the exception the caller sees is the transaction-abort mask"
        )
        assert all("jobs_pkey" in (s.status.description or "") for s in error_spans), (
            "the span status must carry the ORIGINAL enqueue error text, not the "
            "InFailedSQLTransactionError that masks it at the caller boundary"
        )


class TestClientSideEnqueueFailure:
    """C2 contrast: the backend raises before any statement is sent."""

    async def test_failure_bookkeeping_commits_and_tick_returns_zero(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A raise that never reaches PG leaves the transaction alive: every
        planned fire converts to a per-schedule failure that COMMITS —
        consecutive_failures=1, raw error text in last_fire_error, no jobs,
        return 0, one failure log per schedule."""
        from typing import NoReturn

        from taskq.backend._protocol import EnqueueArgs

        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        due = hour_floor(datetime.now(UTC))
        schedule_ids = [
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_PRESENT_ACTOR,
                name=f"clientside-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
            )
            for i in range(2)
        ]

        backend = make_backend(settings)

        async def _raise_before_statement(
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> NoReturn:
            raise RuntimeError("queue backend unavailable")

        monkeypatch.setattr(backend, "enqueue_batch", _raise_before_statement)

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        assert fired == 0
        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == 0
        rows = [await schedule_row(clean_pg_conn, schema, sid) for sid in schedule_ids]
        for row in rows:
            assert row["consecutive_failures"] == 1
            assert row["last_fire_error"] == "queue backend unavailable"
            assert row["enabled"] is True
            assert row["next_fire_at"] == due
        failed_logs = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed_logs) == 2


class TestFailuresUpdateGuard:
    """C3: the ``AND s.enabled = true`` guard on the batched failure UPDATE."""

    async def test_schedule_disabled_mid_tick_is_skipped_not_reenabled(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A schedule that fails planning while an operator disables it on
        another connection: the failure UPDATE's guard skips it (rowcount
        shortfall → warning), and the row keeps exactly the operator's
        state — still disabled, no error stamped, no failure count."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        due = hour_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="guard",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_harness.wedge_then_fail",
        )
        before = await schedule_row(clean_pg_conn, schema, schedule_id)

        with wedge_events() as (entered, gate):
            counting = CountingConn(clean_pg_conn)

            async def _tick() -> int:
                async with clean_pg_conn.transaction():
                    return await tick_cron(
                        counting,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper; all awaited methods typed on it.
                        settings,
                        make_backend(settings),
                        schema,
                        new_uuid(),
                    )

            task = asyncio.create_task(_tick())
            await entered.wait()

            operator = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                await operator.execute(
                    f'UPDATE "{schema}".cron_schedules SET enabled = false '  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
                    "WHERE id = $1",
                    schedule_id,
                )
            finally:
                await operator.close()
            gate.set()
            with structlog.testing.capture_logs() as captured:
                fired: int = await task

        assert fired == 0
        skipped = [e for e in captured if "UPDATE skipped" in e["event"]]
        assert len(skipped) == 1, (
            "the rowcount shortfall (guard skipped the disabled schedule) must warn: "
            f"{[e['event'] for e in captured]}"
        )
        assert skipped[0]["skipped"] == 1

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False, "the operator's disable must stand"
        assert after["last_fire_error"] is None, "the guard must skip the WHOLE update"
        assert after["consecutive_failures"] == 0
        assert after["next_fire_at"] == before["next_fire_at"]
        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == 0
        assert counting.matching("COUNT") == 0, (
            "no auto-disable ran, so the disabled-schedules count must not be re-read"
        )
        failure_updates = counting.matching("consecutive_failures = f.consecutive")
        assert failure_updates == 1, "exactly one batched failure UPDATE may run"


class TestHungPayloadFactory:
    """Own attack: a factory that never returns — the 5s ``wait_for`` in
    ``resolve_payload`` is the only bound on how long the tick's
    transaction (and the cron advisory lock) stays open."""

    async def test_hung_factory_is_cut_off_isolated_and_diagnosable(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A factory sleeping past the 5s resolution timeout becomes a
        per-schedule failure (the tick is not wedged; the healthy schedule
        in the same batch still fires) AND the failure is diagnosable:
        ``last_fire_error`` and the span status must not be the empty
        string a bare ``TimeoutError`` renders to."""
        import time

        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        due = hour_floor(datetime.now(UTC))
        hung_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="hung-factory",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_harness.hang_past_factory_timeout",
        )
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="healthy-peer",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        _provider, exporter = setup_tracer(monkeypatch)
        started = time.monotonic()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )
        elapsed = time.monotonic() - started

        assert fired == 1, "the hung schedule fails, its healthy peer in the batch fires"
        assert elapsed < 20, (
            f"the tick took {elapsed:.1f}s — a hung payload factory must be cut off "
            "by the 5s resolution timeout, not wedge the tick's transaction and the "
            "cron advisory lock open"
        )

        hung = await schedule_row(clean_pg_conn, schema, hung_id)
        assert hung["consecutive_failures"] == 1
        assert hung["last_fire_error"], (
            "last_fire_error is empty: a failure must reach the stored row "
            "with a reason an operator can act on"
        )
        assert "hang_past_factory_timeout" in hung["last_fire_error"], (
            "the timeout error must name the factory that hung — the dotted "
            "path is the only thing that distinguishes it from every other "
            f"schedule's factory; got {hung['last_fire_error']!r}"
        )
        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 1
        assert failed[0]["error"], "the failure log must carry the same non-empty reason"

        hung_span = next(
            s for s in exporter.spans_named("cron fire") if s.status.status_code == StatusCode.ERROR
        )
        assert hung_span.status.description, (
            "the exported span status is empty for the same reason — the failure "
            "reaches the telemetry backend with no diagnostic text at all"
        )
        assert "hang_past_factory_timeout" in (hung_span.status.description or "")


class TestCallerDeadlineMidTick:
    """Own attack: the leader wraps the WHOLE tick in ``asyncio.timeout`` +
    one transaction.  When the deadline fires mid-tick, the cancellation
    (a ``BaseException``) must pass through the planning loop's
    ``except Exception`` untouched — not become a per-schedule failure
    that poisons ``consecutive_failures`` toward auto-disable — and the
    rollback must release the cron advisory lock for the next tick."""

    async def test_deadline_cancel_is_not_a_schedule_failure_and_releases_the_lock(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Tick wedged inside its enqueue past the caller's deadline: the
        caller sees TimeoutError, NOTHING commits (no jobs, no phantom
        consecutive_failures), and the very next tick acquires the lock and
        fires — the transaction-scoped lock was released by the rollback."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        due = hour_floor(datetime.now(UTC))
        schedule_ids = [
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_PRESENT_ACTOR,
                name=f"deadline-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
            )
            for i in range(2)
        ]
        before = [await schedule_row(clean_pg_conn, schema, sid) for sid in schedule_ids]

        gate = asyncio.Event()
        entered = asyncio.Event()
        backend = GatedEnqueueBackend(settings, gate=gate, entered=entered)

        async def _leader_shaped_tick() -> int:
            async with asyncio.timeout(0.1):
                async with clean_pg_conn.transaction():
                    return await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())

        task = asyncio.create_task(_leader_shaped_tick())
        await entered.wait()  # the tick is wedged inside its enqueue, past BEGIN
        with pytest.raises(TimeoutError):
            await task  # the deadline fires; the gate is never opened

        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == 0, "a deadline-cancelled tick must commit nothing"
        after = [await schedule_row(clean_pg_conn, schema, sid) for sid in schedule_ids]
        assert after == before, (
            "a deadline cancellation must not increment consecutive_failures or "
            "write any failure record — the caller's clock is not a schedule defect, "
            "and poisoning the count toward auto-disable would disable healthy "
            "schedules one deadline at a time"
        )

        async with clean_pg_conn.transaction():
            next_fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )
        assert next_fired == 2, (
            "the transaction-scoped advisory lock must be released by the rollback — "
            "the next tick has to be able to fire"
        )


class TestGarbageScheduleRows:
    """Own attack: a stored row with an unusable timezone or cron expression
    (both plain ``text`` columns with no CHECK) must fail exactly itself,
    with a diagnosable error, while the rest of the batch fires."""

    async def test_bad_timezone_and_bad_expression_fail_alone_and_diagnosably(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        due = hour_floor(datetime.now(UTC))
        bad_tz_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="bad-timezone",
            cron_expr=_HOURLY,
            timezone="Not/AZone",
            next_fire_at=due,
        )
        bad_expr_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="bad-expression",
            cron_expr="not a cron expression",
            next_fire_at=due,
        )
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="healthy-peer",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        assert fired == 1, "garbage rows must not stop the batch's healthy schedule"
        jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert jobs == 1
        bad_tz = await schedule_row(clean_pg_conn, schema, bad_tz_id)
        assert bad_tz["consecutive_failures"] == 1
        assert "Not/AZone" in (bad_tz["last_fire_error"] or ""), (
            "the bad-timezone failure must name the offending zone; got "
            f"{bad_tz['last_fire_error']!r}"
        )
        bad_expr = await schedule_row(clean_pg_conn, schema, bad_expr_id)
        assert bad_expr["consecutive_failures"] == 1
        assert bad_expr["last_fire_error"], (
            "the bad-expression failure must be non-empty (croniter's validation "
            f"text); got {bad_expr['last_fire_error']!r}"
        )
        assert len([e for e in captured if e["event"] == "cron fire failed"]) == 2


class TestMissingActorIsolation:
    """C8: an actor with no actor_config row fails alone; 3 ticks disable."""

    async def test_missing_actor_fails_alone_with_exact_text_others_fire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One schedule whose actor has no config, one healthy schedule in
        the same batch: the tick fires the healthy one (isolation), records
        the exact LookupError text on the missing one, and enqueues nothing
        for it."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(
            clean_pg_conn,
            schema,
            _PRESENT_ACTOR,
            queue="rt_present_queue",
            max_attempts=7,
            retry_kind="indefinite",
        )
        due = hour_floor(datetime.now(UTC))
        missing_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="missing",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )
        present_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_PRESENT_ACTOR,
            name="present",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )
        missing_before = await schedule_row(clean_pg_conn, schema, missing_id)

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    worker_id,
                )

        assert fired == 1, "the healthy schedule in the same batch must fire"
        jobs = await clean_pg_conn.fetch(
            f"SELECT actor, queue, status::text AS status, max_attempts, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f"retry_kind::text AS retry_kind "
            f'FROM "{schema}".jobs WHERE actor = ANY($1::text[])',
            [_MISSING_ACTOR, _PRESENT_ACTOR],
        )
        assert [dict(j) for j in jobs] == [
            {
                "actor": _PRESENT_ACTOR,
                "queue": "rt_present_queue",
                "status": "pending",
                "max_attempts": 7,
                "retry_kind": "indefinite",
            }
        ], "exactly one job, for the present actor, carrying its actor_config"

        missing_after = await schedule_row(clean_pg_conn, schema, missing_id)
        assert missing_after["consecutive_failures"] == 1
        assert missing_after["last_fire_error"] == _LOOKUP_ERROR, (
            "the per-schedule failure must carry the exact actor-lookup error text"
        )
        assert missing_after["enabled"] is True
        assert missing_after["next_fire_at"] == missing_before["next_fire_at"]
        present_after = await schedule_row(clean_pg_conn, schema, present_id)
        assert present_after["last_fired_at"] is not None
        assert present_after["consecutive_failures"] == 0

        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 1
        assert failed[0]["error"] == _LOOKUP_ERROR
        assert failed[0]["worker_id"] == str(worker_id)

    async def test_missing_actor_auto_disables_after_three_ticks(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Three consecutive missing-actor ticks: the third disables the
        schedule (enabled=false, consecutive_failures=3), refreshes the
        disabled-schedules count exactly once, and a fourth tick leaves it
        alone (the due SELECT's enabled=true predicate)."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _PRESENT_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_MISSING_ACTOR,
            name="missing-3t",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
        )

        disabled_counts: list[int] = []

        def _spy_disabled_count(count: int) -> None:
            disabled_counts.append(count)

        monkeypatch.setattr(cron_loop, "update_disabled_schedules_count", _spy_disabled_count)

        worker_id = new_uuid()
        auto_disabled_events: list[dict[str, object]] = []
        for _ in range(3):
            with structlog.testing.capture_logs() as captured:
                async with clean_pg_conn.transaction():
                    fired = await tick_cron(
                        clean_pg_conn,
                        settings,
                        make_backend(settings),
                        schema,
                        worker_id,
                    )
            assert fired == 0
            auto_disabled_events.extend(
                e for e in captured if e["event"] == "cron schedule auto-disabled"
            )

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is False, "the third consecutive failure must auto-disable"
        assert row["consecutive_failures"] == 3
        assert row["last_fire_error"] == _LOOKUP_ERROR
        assert disabled_counts == [1], (
            f"the disabling tick must refresh the disabled-schedules count exactly "
            f"once with the committed count; saw {disabled_counts}"
        )
        assert len(auto_disabled_events) == 1
        assert auto_disabled_events[0]["consecutive_failures"] == 3
        assert auto_disabled_events[0]["worker_id"] == str(worker_id)

        async with clean_pg_conn.transaction():
            fourth = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, worker_id
            )
        assert fourth == 0
        after_fourth = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after_fourth["consecutive_failures"] == 3, (
            "a disabled schedule must not accrue further failures — the due SELECT "
            "must skip it entirely"
        )

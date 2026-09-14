"""Red-team attack on the concurrent singleton race mid-tick (real PG).

The tick's policy preflight and its batched INSERT are two statements in
one transaction, so a client singleton enqueue can COMMIT between them —
exactly the window the enqueue path closes with its own
``UniqueViolationError`` catch.  Here the batched INSERT carries the
stamped singleton fire, so the committed client row violates
``jobs_singleton_uniq`` from OUTSIDE the batch and aborts the whole
statement mid-tick.

The race is constructed deterministically: a gated backend holds the
tick inside its enqueue (the preflight has already run and seen no
blocker, the transaction and advisory lock are open) while a second real
connection commits the client singleton job; releasing the gate runs the
INSERT against the now-committed blocker.

The acceptable outcome — pinned here — is the honest abort shape the
server-side enqueue-failure domain already documents: the tick RAISES
(the doomed failure UPDATE surfacing the aborted transaction), NOTHING
commits (no partial enqueues, no strikes), and the NEXT tick — a fresh
transaction whose preflight runs BEFORE its enqueue — sees the committed
blocker and suppresses cleanly instead of re-aborting: self-healing in
one second, and the healthy non-singleton plan that shared the aborted
batch loses one tick, not its fire.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    GatedEnqueueBackend,
    count_jobs,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

pytestmark = pytest.mark.integration

_SINGLETON_ACTOR = "rt_race_singleton"
_HEALTHY_ACTOR = "rt_race_healthy"


class TestConcurrentSingletonRace:
    """A client singleton INSERT commits between the tick's preflight and
    its batched INSERT."""

    async def test_race_aborts_the_tick_and_the_next_tick_suppresses(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The gated race, end to end: the aborted tick raises with nothing
        committed (the healthy plan in the same batch is rolled back with
        it, still due — not lost), and the very next tick preflights FIRST,
        suppresses the singleton slot against the committed client job, and
        fires the healthy plan it lost."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_actor_config(clean_pg_conn, schema, _HEALTHY_ACTOR)
        due = hour_floor(datetime.now(UTC))
        singleton_schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="race-singleton",
            cron_expr=_HOURLY,
            next_fire_at=due,
            identity_key="race-singleton",
        )
        healthy_schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_HEALTHY_ACTOR,
            name="race-healthy",
            cron_expr=_HOURLY,
            next_fire_at=due,
            identity_key="race-healthy",
        )
        before = {
            sid: await schedule_row(clean_pg_conn, schema, sid)
            for sid in (singleton_schedule_id, healthy_schedule_id)
        }
        policies = {
            _SINGLETON_ACTOR: ActorFirePolicy(singleton=True),
            _HEALTHY_ACTOR: ActorFirePolicy(),
        }

        gate = asyncio.Event()
        entered = asyncio.Event()
        backend = GatedEnqueueBackend(settings, gate=gate, entered=entered)

        async def _racing_tick() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    clean_pg_conn,
                    settings,
                    backend,
                    schema,
                    new_uuid(),
                    actor_policies=policies,
                )

        task = asyncio.create_task(_racing_tick())
        await entered.wait()  # the tick is inside its enqueue, post-preflight

        # The client wins the race: its singleton job commits AFTER the
        # tick's preflight (which saw no blocker) and BEFORE the tick's
        # batched INSERT (which carries the stamped singleton fire).
        racer = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            client_job_id = new_uuid()
            await racer.execute(
                f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
                "(id, actor, queue, payload, max_attempts, retry_kind, status, metadata) "
                f"VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 5, 'transient', "
                f"'pending'::\"{schema}\".job_status, '{{\"singleton\": true}}'::jsonb)",
                client_job_id,
                _SINGLETON_ACTOR,
            )
        finally:
            await racer.close()

        with structlog.testing.capture_logs() as captured:
            gate.set()
            with pytest.raises(asyncpg.InFailedSQLTransactionError):
                await task

        # Nothing from the aborted tick survived its rollback: no partial
        # enqueues (the client job is the only singleton row), no strikes,
        # both schedules still due exactly as seeded.
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        assert await count_jobs(clean_pg_conn, schema, _HEALTHY_ACTOR) == 0
        for sid, row_before in before.items():
            row_after = await schedule_row(clean_pg_conn, schema, sid)
            assert row_after == row_before, (
                f"schedule {sid} changed under an aborted tick — no strike, no "
                "advance and no last_fired_at may survive the rollback"
            )
        tick_logs = [e["event"] for e in captured]
        assert "cron fire failed" not in tick_logs, (
            "per-schedule failure logs ran for writes that were rolled back — the "
            "aborted tick must not claim failures it could not commit"
        )

        # The next tick, in a fresh transaction: the preflight runs BEFORE
        # the enqueue, sees the committed client job, and suppresses the
        # singleton slot cleanly — the race does not repeat, and the healthy
        # plan the aborted batch lost fires now.
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                second = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies=policies,
                )

        assert second == 1, (
            "the next tick must suppress the singleton slot and fire the healthy "
            "plan the aborted batch lost — a re-abort here would mean the tick "
            "re-enqueued against the blocker instead of re-preflighting first"
        )
        assert await count_jobs(clean_pg_conn, schema, _HEALTHY_ACTOR) == 1
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "the suppressed singleton slot enqueued nothing"
        )
        collisions = [e for e in captured if e["event"] == "singleton-collision"]
        assert len(collisions) == 1
        assert collisions[0]["blocking_job_id"] == str(client_job_id), (
            "the post-race suppression must attribute the client job that won the race"
        )
        singleton_row = await schedule_row(clean_pg_conn, schema, singleton_schedule_id)
        assert singleton_row["next_fire_at"] > due, "the suppressed slot advanced"
        assert singleton_row["consecutive_failures"] == 0, "suppression is not a strike"
        assert singleton_row["last_fired_at"] is None
        healthy_row = await schedule_row(clean_pg_conn, schema, healthy_schedule_id)
        assert healthy_row["last_fired_at"] is not None
        assert healthy_row["consecutive_failures"] == 0

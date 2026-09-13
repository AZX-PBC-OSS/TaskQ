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

The acceptable outcome — pinned here — is per-plan attribution inside a
SAVEPOINT: the tick does NOT abort (the savepoint keeps the transaction
alive past the statement error), the racing schedule takes exactly ONE
strike with the real constraint name, and the healthy non-singleton plan
that shared the batch fires in the SAME tick.  The next tick — a fresh
transaction whose preflight runs BEFORE its enqueue — sees the committed
blocker and suppresses the singleton slot cleanly instead of re-aborting:
self-healing in one second, the strike kept (suppression is neither
amnesty nor a second strike), and no other schedule touched.
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

    async def test_race_strikes_the_racer_fires_the_peer_and_the_next_tick_suppresses(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The gated race, end to end: the racing schedule takes exactly one
        strike (attributed from the violation's ``Key (actor)=…`` detail,
        committed — the savepoint kept the transaction alive), the healthy
        plan in the same batch fires in the SAME tick, and the very next
        tick preflights FIRST, suppresses the singleton slot against the
        committed client job, and keeps the strike (no amnesty, no second
        strike)."""
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
            first: int = await task

        # The tick did NOT abort: the savepoint kept the transaction alive,
        # the racing plan was attributed from the violation's detail line,
        # and the healthy plan fired in this same tick.
        assert first == 1, (
            "the healthy plan must fire in the racing tick — one schedule "
            "losing a singleton race is not a defect of the batch"
        )
        # The racer's strike committed: consecutive_failures=1 with the real
        # constraint name, no auto-disable, no last_fired_at (nothing fired),
        # next_fire_at untouched (the failure path is not a fire).
        singleton_row = await schedule_row(clean_pg_conn, schema, singleton_schedule_id)
        assert singleton_row["consecutive_failures"] == 1, (
            "the racing schedule takes exactly one strike"
        )
        assert "jobs_singleton_uniq" in (singleton_row["last_fire_error"] or ""), (
            f"the strike must carry the real constraint name; got {singleton_row['last_fire_error']!r}"
        )
        assert singleton_row["enabled"] is True, "one race loss must not auto-disable"
        assert singleton_row["last_fired_at"] is None
        assert singleton_row["next_fire_at"] == before[singleton_schedule_id]["next_fire_at"]
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "the colliding cron fire must not be enqueued — the client job won the slot"
        )
        assert await count_jobs(clean_pg_conn, schema, _HEALTHY_ACTOR) == 1, (
            "the healthy plan's fire committed in the racing tick"
        )
        tick_logs = [e["event"] for e in captured]
        failed = [e for e in tick_logs if e == "cron fire failed"]
        assert len(failed) == 1, (
            "exactly one per-schedule failure log — the racing plan, whose "
            "bookkeeping commits because the transaction survived"
        )
        assert "cron schedule auto-disabled" not in tick_logs
        healthy_row = await schedule_row(clean_pg_conn, schema, healthy_schedule_id)
        assert healthy_row["consecutive_failures"] == 0, "the healthy plan takes no strike"
        assert healthy_row["last_fired_at"] is not None

        # The next tick, in a fresh transaction: the preflight runs BEFORE
        # the enqueue, sees the committed client job, and suppresses the
        # singleton slot cleanly — the race does not repeat.  The strike
        # from the racing tick STANDS (suppression is neither amnesty nor a
        # second strike), and the healthy plan, already fired, is not due.
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

        assert second == 0, (
            "the next tick must suppress the singleton slot (already struck, "
            "not fired) and find the healthy plan already fired"
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
        assert singleton_row["consecutive_failures"] == 1, (
            "suppression is not amnesty — the racing tick's strike stands"
        )
        assert singleton_row["last_fired_at"] is None
        healthy_row = await schedule_row(clean_pg_conn, schema, healthy_schedule_id)
        assert healthy_row["consecutive_failures"] == 0

# ruff: noqa: S608  # Why: schema name is fixture-generated and validated by the
# migration runner's _IDENT_RE; asyncpg has no parameter binding for identifiers.

"""A cron fire landing on a held singleton blocker is suppressed, not failed.

`docs/guides/sweeps.md` ("Single-flight the chain") and `docs/guides/cron.md`
("Cadence starts work; it does not pace it") claim: a fire landing while the
blocker holds is **suppressed, not failed** — no `consecutive_failures`
strike, no auto-disable; the schedule advances to the next cron slot and
re-evaluates. The failure contrast is the schedule's own fire failures, which
strike and disable after `TASKQ_CRON_AUTO_DISABLE_THRESHOLD` (pinned
elsewhere).

This pin drives the real cron tick (`tick_cron`) on a live Postgres with a
singleton actor's blocker running, and asserts what an operator observes in
the schedule and jobs tables afterwards.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=0)


@asynccontextmanager
async def _cron_world(
    pg_dsn: str,
) -> AsyncGenerator[tuple[str, WorkerSettings, WorkerDeps, PostgresBackend]]:
    schema = f"atk_cro_{new_base62()}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    try:
        yield schema, settings, deps, backend
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_singleton_collision_suppresses_without_a_strike(pg_dsn: str) -> None:
    """A tick whose fire collides with a live singleton blocker: no new job,
    no strike, no auto-disable, next_fire_at advanced to the next slot."""
    async with _cron_world(pg_dsn) as (schema, settings, deps, backend):
        conn = await deps.dispatcher_pool.acquire()
        try:
            worker_id = new_uuid()
            # The actor's stored policy declares singleton (what the tick's
            # preflight and the jobs_singleton_uniq index key on).
            await conn.execute(
                f"""INSERT INTO "{schema}".actor_config
                      (actor, queue, max_attempts, retry_kind, metadata)
                    VALUES ('sing_actor', 'default', 3, 'transient',
                            '{{"singleton": true}}'::jsonb)"""
            )
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, 'docs-contract', 1, $2)",
                worker_id,
                ["default"],
            )
            # The blocker: the actor's singleton job, mid-flight.
            await conn.execute(
                f"""INSERT INTO "{schema}".jobs
                      (id, actor, queue, payload, max_attempts, status, retry_kind,
                       metadata, locked_by_worker, lock_expires_at, started_at,
                       last_heartbeat_at)
                    VALUES ($1, 'sing_actor', 'default', '{{}}'::jsonb, 3,
                            'running', 'transient', '{{"singleton": true}}'::jsonb,
                            $2, clock_timestamp() + interval '60 seconds',
                            clock_timestamp(), clock_timestamp())""",
                new_uuid(),
                worker_id,
            )
            # A schedule for the singleton actor, due two hours ago.
            await conn.execute(
                f"""INSERT INTO "{schema}".cron_schedules
                      (id, actor, cron_expr, next_fire_at)
                    VALUES ($1, 'sing_actor', '* * * * *',
                            now() - interval '2 hours')""",
                new_uuid(),
            )

            # The tick's fire policy comes from the worker's actor registry
            # (the singleton flag is declared in code, bootstrap passes it
            # per actor) — the same shape a real deployment declares.
            async with conn.transaction():
                await tick_cron(
                    conn,
                    settings,
                    backend,
                    schema,
                    worker_id,
                    actor_policies={
                        "sing_actor": ActorFirePolicy(singleton=True, max_pending=None)
                    },
                )

            job_count = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', "sing_actor"
            )
            sched = await conn.fetchrow(
                f"SELECT enabled, consecutive_failures, last_fire_error, next_fire_at "
                f'FROM "{schema}".cron_schedules WHERE actor = $1',
                "sing_actor",
            )
            assert job_count == 1, (
                f"{job_count} jobs exist for the singleton actor: the colliding "
                "fire must be suppressed, not enqueued behind the blocker"
            )
            assert sched is not None and sched["enabled"] is True, (
                "a suppressed fire must not disable the schedule"
            )
            assert sched is not None and sched["consecutive_failures"] == 0, (
                f"a suppressed fire struck the schedule "
                f"(consecutive_failures={sched['consecutive_failures']}): a "
                "singleton collision is suppression, not a failure, and must "
                "leave the failure accounting untouched"
            )
            assert sched is not None and sched["last_fire_error"] is None, (
                "a suppressed fire must not stamp a fire error on the schedule"
            )
            assert (
                sched is not None
                and sched["next_fire_at"] is not None
                and sched["next_fire_at"] > datetime.now(UTC) - timedelta(minutes=1)
            ), (
                "a suppressed fire must advance next_fire_at to the next cron "
                "slot so the schedule re-evaluates there, not hot-loop on the "
                "blocked slot"
            )
        finally:
            await deps.dispatcher_pool.release(conn)

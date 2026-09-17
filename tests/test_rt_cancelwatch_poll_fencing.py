# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team pins: readers must fence on a cancel request that lost the race.

Three GREEN pins over the real PG protocol, covering the
heartbeat/cancel/watchdog interplay's "lost race" corners:

1. **A cancel request whose owner task completed is inert.**
   ``POLL_CANCEL_FLAGS_SQL`` fences on ``status = 'running'``; a terminal
   row keeps ``cancel_requested_at`` as its audit trail
   (``_sql_templates``: "the TERMINAL arms deliberately keep the cancel
   columns") but no reader may act on it: the poll stays silent, the
   escalation never re-fires, ``mark_abandoned``'s ``status = 'running'``
   guard makes every stale abandon a no-op, and the terminal outcome is
   never overwritten.  The stale flag also blocks nothing at the constraint
   level — the singleton partial unique index only covers active statuses.

2. **Phase-2's ``task.cancel()`` landing on an already-completed task is a
   no-op and starts no re-issue storm.**  The escalation applies (row still
   running), the done task is unaffected, and once the owner's terminal
   write lands, later ticks write nothing further.

3. **Heartbeat renewal is ownership-fenced.**
   ``UPDATE_JOBS_LOCK_SQL`` carries ``WHERE locked_by_worker = $1 AND
   status = 'running'`` — after a reclaim re-dispatches the job to another
   worker, the old holder's renewals are 0-row no-ops that leave the new
   holder's lease untouched.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
import structlog
from pydantic import BaseModel

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase, JobId
from taskq.backend._sql import POLL_CANCEL_FLAGS_SQL, build_heartbeat_sql
from taskq.backend._sql_templates import render
from taskq.backend._sweeps import sweep_expired_locks
from taskq.backend._terminal import _mark_succeeded_on_conn
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker, seed_actors
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=0)
_CLEANUP_GRACE = timedelta(seconds=10)


class _BackendDepsShim:
    """The three pools PostgresBackend accesses, on the module's own DSN."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool


class _StubPayload(BaseModel):
    """Minimal payload for a cancel-path JobContext."""


def _make_ctx(job_id: JobId, worker_id: UUID) -> JobContext[BaseModel]:
    return JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=worker_id,
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


def _sleeper() -> asyncio.Task[object]:
    return asyncio.get_running_loop().create_task(asyncio.sleep(3600))


@pytest_asyncio.fixture(scope="module")
async def rt_schema(pg_dsn: str) -> AsyncIterator[tuple[str, str]]:
    """A random migrated schema on this module's database: ``(schema, dsn)``."""
    schema = f"tcw_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    yield schema, pg_dsn
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _open(schema: str, dsn: str) -> tuple[asyncpg.Pool, PostgresBackend, WorkerDeps]:
    """One small pool + real PostgresBackend + controller deps for *schema*."""
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_LOCK_LEASE": "360",
            "TASKQ_TERMINATION_GRACE_PERIOD": "360",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "10",
        }
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    backend = PostgresBackend(
        _BackendDepsShim(settings, pool),  # type: ignore[arg-type]
        SystemClock(),
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
    )
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    return pool, backend, deps


async def _tick(deps: WorkerDeps, backend: PostgresBackend, dsn: str, worker_id: UUID) -> None:
    """One heartbeat-tick shape against real PG: in-tx hook, then the drain."""
    controller = make_cancel_controller(deps, worker_id, backend)
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            await controller.run_in_tx(conn)  # type: ignore[arg-type]
        await controller.run_post_tx()
    finally:
        await conn.close()


async def _escalation_events(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '
            "WHERE job_id = $1 AND kind = 'state_change' "
            "AND (detail->>'cancel_phase_to') = '2'",
            job_id,
        )
        or 0
    )


async def test_stale_cancel_request_on_terminal_row_is_inert(
    rt_schema: tuple[str, str],
) -> None:
    """A cancel request that lost the race to completion blocks nothing.

    Contract: the terminal row keeps ``cancel_requested_at`` as audit
    trail, but every cancel-protocol reader fences on ``status =
    'running'`` — the poll returns nothing, no escalation is ever issued,
    no abandon ever applies — and the stale flag does not block a fresh
    singleton enqueue (the partial unique index covers active statuses
    only).
    """
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool, backend, deps = await _open(schema, dsn)
    try:
        async with pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                job_id,
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
            )
            # The owner task completed between polls: the real terminal
            # write lands while the cancel request sits unobserved.
            assert await _mark_succeeded_on_conn(
                conn, render(schema), job_id, worker_id, attempt=1
            ), "fixture broken: the owner's terminal write must apply"
            stale_flag = await conn.fetchval(
                f'SELECT cancel_requested_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert stale_flag is not None, (
                "fixture broken: the terminal arm must KEEP cancel_requested_at as audit"
            )
            poll_rows = await conn.fetch(POLL_CANCEL_FLAGS_SQL.format(schema=schema), worker_id)
            assert len(poll_rows) == 0, "fixture broken: the poll must fence on status='running'"
            # The stale flag must not block a fresh singleton enqueue for the
            # same actor: jobs_singleton_uniq only covers active statuses.
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, max_attempts, retry_kind, "
                "status, priority, scheduled_at, schedule_to_close, metadata) "
                "VALUES ($1, 'test_actor', 'default', '{}'::jsonb, 3, 'transient', "
                "'pending', 0, clock_timestamp(), NULL, '{\"singleton\": true}'::jsonb)",
                new_job_id(),
            )

        # Registry entry maximally triggerable: already COOPERATIVE, both
        # deadlines long past. Three full ticks against the terminal row.
        await deps.active_jobs.register(job_id, _sleeper(), _make_ctx(job_id, worker_id))
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 100.0
        for _ in range(3):
            await _tick(deps, backend, dsn, worker_id)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, cancel_requested_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] == "succeeded", (
                "Contract: a cancel request that lost the race to completion must "
                "never change the job's terminal outcome. Current behavior changed "
                f"the row to {row['status']!r}."
            )
            assert row["cancel_requested_at"] is not None, "the audit trail must stand"
            assert await _escalation_events(conn, schema, job_id) == 0, (
                "Contract: no escalation may be issued for a row the poll does not "
                "return (status='running' fence) — an escalation event here means the "
                "re-issue arm acted on stale local state against a terminal row."
            )
    finally:
        await pool.close()
        entry_task = None
        for active in deps.active_jobs.all():
            if active.job_id == job_id:
                active.task.cancel()
                entry_task = active.task
        if entry_task is not None:
            await asyncio.gather(entry_task, return_exceptions=True)


async def test_escalation_then_terminal_write_no_reissue_storm(
    rt_schema: tuple[str, str],
) -> None:
    """Phase-2's ``task.cancel()`` on a completed task is a no-op, no storm.

    Interleaving: the cancel is observed, the actor task finishes on its
    own, and the phase-2 tick's ``task.cancel()`` therefore lands on a DONE
    task (asyncio no-op — the task's result stands).  The escalation itself
    applies (the row was still running), the owner's terminal write lands
    after it, and every later tick is silent: the poll fence keeps the
    re-issue arm from firing and ``mark_abandoned``'s ``status='running'``
    guard keeps the (per-tick, not-applied) abandons from touching the
    terminal row.
    """
    schema, dsn = rt_schema
    worker_id = new_uuid()
    job_id = new_job_id()
    pool, backend, deps = await _open(schema, dsn)
    try:
        async with pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await create_running_job(
                conn,
                schema,
                worker_id,
                job_id,
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
            )

        # The actor task already completed on its own.
        done_task = asyncio.get_running_loop().create_task(asyncio.sleep(0))
        await done_task
        await deps.active_jobs.register(job_id, done_task, _make_ctx(job_id, worker_id))
        entry = deps.active_jobs.get(job_id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        # Past the cancel grace (0), well inside the cleanup grace (10), so
        # this tick escalates but does not yet queue an abandon.
        entry.cancel_observed_at = asyncio.get_running_loop().time() - 1.0

        await _tick(deps, backend, dsn, worker_id)

        assert done_task.done() and not done_task.cancelled(), (
            "Contract: task.cancel() on an already-completed task must be a no-op — "
            "the completed attempt's outcome stands and no exception may escape the "
            "escalation arm."
        )
        assert entry.cancel_phase == CancelPhase.FORCED

        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1', job_id
                )
                == 2
            ), "fixture broken: the escalation must have applied"
            assert await _escalation_events(conn, schema, job_id) == 1
            # The owner's terminal write lands after the escalation.
            assert await _mark_succeeded_on_conn(
                conn, render(schema), job_id, worker_id, attempt=1
            ), "fixture broken: the owner's terminal write must still apply"

        # Age past the cleanup grace so every later tick's phase-3 arm fires
        # against the now-terminal row.
        entry.cancel_observed_at -= 100.0
        for _ in range(3):
            await _tick(deps, backend, dsn, worker_id)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
            assert row is not None
            assert row["status"] == "succeeded", (
                "Contract: after the owner's terminal write, the cancel machinery "
                "must never overwrite the outcome — mark_abandoned's status='running' "
                "guard must make every stale abandon a no-op. Current behavior: the "
                f"row landed on {row['status']!r}."
            )
            assert await _escalation_events(conn, schema, job_id) == 1, (
                "Contract: exactly one escalation event per applied escalation — a "
                "second event here means the re-issue arm fired on a terminal row "
                "(the poll's status='running' fence was bypassed)."
            )
            attempts = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', job_id
            )
            assert int(attempts or 0) == 1, (
                "Contract: only the owner's mark_succeeded attempt row may exist — a "
                "second row means a stale abandon applied against the terminal row."
            )
    finally:
        await pool.close()


async def test_renewal_noops_after_reclaim_and_spares_new_holder(
    rt_schema: tuple[str, str],
) -> None:
    """A reclaimed job's rows no longer renew for the old holder.

    Contract: ``UPDATE_JOBS_LOCK_SQL`` is fenced on ``locked_by_worker =
    $1 AND status = 'running'`` — once Sweep 1 re-pends the job and another
    worker re-dispatches it, the old holder's heartbeat renewal is a 0-row
    no-op and the new holder's ``lock_expires_at`` is untouched (no
    interference between the stale holder's ticks and the live one).
    """
    schema, dsn = rt_schema
    worker_a = new_uuid()
    worker_b = new_uuid()
    pool, backend, _deps = await _open(schema, dsn)
    try:
        async with pool.acquire() as conn:
            await seed_actors(conn, schema)
            await create_worker(conn, schema, worker_a)
            await create_worker(conn, schema, worker_b)
            job_id = await create_running_job(
                conn,
                schema,
                worker_a,
                lock_expires_at=datetime.now(UTC) - timedelta(seconds=120),
            )
            reclaimed = await sweep_expired_locks(
                conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
            )
            assert reclaimed == 1, "fixture broken: the expired lock was not reclaimed"
            # Sweep 1's retry branch backdates the retry by 5s; make it due.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() '
                f"- interval '1 second' WHERE id = $1",
                job_id,
            )

        dispatched = await backend.dispatch_batch(
            worker_b, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert len(dispatched) == 1 and dispatched[0].id == job_id, (
            "fixture broken: the re-pended job was not re-dispatched to worker B"
        )

        async with pool.acquire() as conn:
            before = await conn.fetchval(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert before is not None
            # Worker A's next heartbeat renewal, verbatim production SQL.
            tag = await conn.execute(
                build_heartbeat_sql(schema)[1], worker_a, timedelta(seconds=60), []
            )
            after = await conn.fetchval(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert tag.rsplit(" ", 1)[-1] == "0", (
                "Contract: the old holder's renewal must be a 0-row no-op once the "
                "job was reclaimed and re-locked by another worker — a non-zero "
                "rowcount means the stale holder extended a lease it no longer owns."
            )
            assert after == before, (
                "Contract: the old holder's renewal must not move the new holder's "
                "lock_expires_at — the stale holder's ticks must not interfere with "
                "the live attempt's lease."
            )
    finally:
        await pool.close()

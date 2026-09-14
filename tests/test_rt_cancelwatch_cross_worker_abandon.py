# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team: a stale registry entry's abandon must not kill a new attempt.

The interleaving this file attacks, step by step:

1. worker A owns the job, observes a cancel, escalates to phase 2 (its
   registry entry sits at local ``FORCED``);
2. A's event loop stalls past ``lock_lease`` (with the watchdog disabled,
   nothing kills it — the lag-budget/lease invariant is only validated
   when ``watchdog_enabled``), Sweep 1's cancel carve-out expires, the
   leader re-pends the job (cancel columns RESET) and it is re-dispatched
   to worker B as attempt 2;
3. the caller cancels again — entirely legitimate, the job is live — and
   B's own controller escalates ITS attempt to ``cancel_phase = 2``;
4. A's loop recovers and ticks.  A's poll stays silent (the row is locked
   by B), but the phase-3 arm queues on A's stale local ``FORCED`` alone,
   and ``run_post_tx`` calls ``mark_abandoned`` — whose guard is
   ``status = 'running' AND cancel_phase = 2`` with **no worker fence**
   (``_sql_templates.mark_abandoned``: "the abandoned job's worker id is
   the row's own").  A's abandon therefore terminates B's attempt.

Contract under test: an abandon issued from worker A's cancel-poll state
must not apply to a job row that A's own poll does not return — the poll's
predicate (``locked_by_worker = $1 AND cancel_requested_at IS NOT NULL AND
status = 'running'``) is exactly the set of rows A's abandon may touch.
Red today: the row lands on ``abandoned`` (with an attempt row for B's
attempt) while B's cooperative cancel was still inside its cleanup grace.
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
from taskq.backend._sweeps import sweep_expired_locks
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
    """The pools PostgresBackend accesses, on the module's own DSN."""

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
        attempt=2,
        worker_id=worker_id,
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=2,
            identity_key=None,
            trace_id="",
        ),
    )


def _sleeper() -> asyncio.Task[object]:
    return asyncio.get_running_loop().create_task(asyncio.sleep(3600))


def _deps_for(dsn: str, schema: str) -> WorkerDeps:
    return WorkerDeps(  # type: ignore[call-arg]
        settings=WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_LOCK_LEASE": "360",
                "TASKQ_TERMINATION_GRACE_PERIOD": "360",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
                "TASKQ_CLEANUP_GRACE_PERIOD": str(_CLEANUP_GRACE.total_seconds()),
            }
        ),
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _tick(deps: WorkerDeps, backend: PostgresBackend, dsn: str, worker_id: UUID) -> None:
    controller = make_cancel_controller(deps, worker_id, backend)
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            await controller.run_in_tx(conn)  # type: ignore[arg-type]
        await controller.run_post_tx()
    finally:
        await conn.close()


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


async def test_stale_entry_abandon_does_not_terminate_new_holders_escalated_attempt(
    rt_schema: tuple[str, str],
) -> None:
    schema, dsn = rt_schema
    worker_a = new_uuid()
    worker_b = new_uuid()
    job_id = new_job_id()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    backend = PostgresBackend(
        _BackendDepsShim(_deps_for(dsn, schema).settings, pool),  # type: ignore[arg-type]
        SystemClock(),
        _CANCEL_GRACE,
        _CLEANUP_GRACE,
    )
    deps_a = _deps_for(dsn, schema)
    # Worker B's controller: a huge cleanup grace so B's OWN abandon stays
    # out of this scenario — only A's stale abandon is under test.
    deps_b = _deps_for(dsn, schema)
    deps_b.settings.cleanup_grace_period = 3600.0
    task_a: asyncio.Task[object] | None = None
    task_b: asyncio.Task[object] | None = None
    try:
        async with pool.acquire() as conn:
            await seed_actors(conn, schema)
            await create_worker(conn, schema, worker_a)
            await create_worker(conn, schema, worker_b)
            await create_running_job(
                conn,
                schema,
                worker_a,
                job_id,
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
            )

        # ── Stage 1: A observes the cancel and escalates its own attempt. ──
        task_a = _sleeper()
        await deps_a.active_jobs.register(job_id, task_a, _make_ctx(job_id, worker_a))
        entry_a = deps_a.active_jobs.get(job_id)
        assert entry_a is not None
        entry_a.cancel_phase = CancelPhase.COOPERATIVE
        entry_a.cancel_observed_at = asyncio.get_running_loop().time() - 1.0
        await _tick(deps_a, backend, dsn, worker_a)
        assert entry_a.cancel_phase == CancelPhase.FORCED

        # ── Stage 2: A's loop stalls; the lease lapses past the sweep's
        # cancel carve-out (cg + cl + 60s); Sweep 1 re-pends the job with a
        # clean cancel slate and worker B re-dispatches it as attempt 2. ──
        async with pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET lock_expires_at = $2 WHERE id = $1',
                job_id,
                datetime.now(UTC) - timedelta(seconds=600),
            )
            reclaimed = await sweep_expired_locks(
                conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
            )
            assert reclaimed == 1, "fixture broken: the stalled lease was not reclaimed"
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

        # ── Stage 3: the caller cancels the live attempt again, and B's own
        # controller escalates it to phase 2. ──
        assert await backend.write_cancel_request(job_id, None), (
            "fixture broken: the second cancel request must land on B's running row"
        )
        task_b = _sleeper()
        await deps_b.active_jobs.register(job_id, task_b, _make_ctx(job_id, worker_b))
        entry_b = deps_b.active_jobs.get(job_id)
        assert entry_b is not None
        entry_b.cancel_phase = CancelPhase.COOPERATIVE
        entry_b.cancel_observed_at = asyncio.get_running_loop().time() - 1.0
        await _tick(deps_b, backend, dsn, worker_b)
        assert entry_b.cancel_phase == CancelPhase.FORCED
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1', job_id
                )
                == 2
            ), "fixture broken: B's escalation must have applied"

        # ── Stage 4: A's loop recovers and ticks with its stale entry. ──
        entry_a.cancel_observed_at -= 100.0
        await _tick(deps_a, backend, dsn, worker_a)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, locked_by_worker, attempt FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] == "running", (
                "Contract: an abandon issued from worker A's cancel-poll state must "
                "not apply to a job A's own poll does not return — B's re-dispatched "
                "attempt at cancel_phase=2 is inside B's protocol (cleanup grace not "
                "yet elapsed) and only B's own phase-3 or terminal write may end it. "
                "Current behavior: A's stale phase-3 abandon matched mark_abandoned's "
                "worker-unfenced guard (status='running' AND cancel_phase=2) and "
                "terminated B's attempt as 'abandoned'."
            )
            assert row["locked_by_worker"] == worker_b
            attempt2_rows = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1 AND attempt = 2',
                job_id,
            )
            assert int(attempt2_rows or 0) == 0, (
                "Contract: no attempt row may exist for B's live attempt — one here "
                "was written by A's stale abandon terminating B's attempt."
            )
    finally:
        for task in (task_a, task_b):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await pool.close()

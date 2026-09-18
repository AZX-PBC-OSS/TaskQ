# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""A stale registry entry's abandon must find nothing to kill.

The interleaving this file exercised, step by step:

1. worker A owns the job, observes a cancel, escalates to phase 2 (its
   registry entry sits at local ``FORCED``);
2. A's event loop stalls past ``lock_lease`` (with the watchdog disabled,
   nothing kills it); Sweep 1's cancel carve-out (cancel_grace +
   cleanup_grace + 60s) expires;
3. the OLD reclaim re-pended the job with its cancel columns RESET and
   re-dispatched it to worker B as attempt 2, where a second cancel
   escalated B's own attempt to ``cancel_phase = 2``;
4. A's loop recovered and ticked.  A's poll stayed silent (the row was
   locked by B), but the phase-3 arm queued on A's stale local
   ``FORCED`` alone, and ``run_post_tx`` called ``mark_abandoned`` --
   whose guard is ``status = 'running' AND cancel_phase = 2`` with no
   worker fence -- so A's abandon terminated B's live attempt.

PR #272 (issues #237/#238) closes the interleaving at its source.  The
reclaim's CASE now orders operator intent first: a row carrying
``cancel_phase != 0`` past the carve-out is terminalised 'cancelled',
NEVER re-pended, and it keeps cancel_phase/cancel_requested_at as the
audit trail of the honoured request.  A row whose holder observed a
cancel can therefore never re-enter the claim/reclaim cycle, so the
"new holder" of step 3 is unreachable: there is no attempt 2 for a
stale abandon to terminate, and ``mark_abandoned``'s running+phase-2
guard has no living match.  The cross-worker abandon-fence contract the
old test exercised (A's abandon killing B's attempt) has no reachable
interleaving under these semantics: every path out of a cancel-phase
row (cooperative completion, phase-3 abandon, the reclaim, isolate)
terminalises it.

What this file pins now: the exact interleaving point (A stalled, its
FORCED entry stale, the carve-out lapsed) ends in a terminalised row
that survives A's stale tick untouched.  Vendor shape: river's rescuer
routes a stuck row stamped ``metadata.cancel_attempted_at`` straight to
'cancelled', never into its retry decision
(vendor/river/internal/maintenance/job_rescuer.go), and pg-boss's
cancelJobs terminalises the row with only an explicit operator
resumeJobs to revive it (vendor/pg-boss/src/plans.ts).
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


async def test_stale_entry_abandon_after_carveout_meets_a_terminalised_row(
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
    task_a: asyncio.Task[object] | None = None
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
        # cancel carve-out (cg + cl + 60s). PR #272: the reclaim's cancel arm
        # outranks the retry budget, so the row terminalises 'cancelled' with
        # its cancel columns preserved. It never re-pends, so worker B's
        # dispatch finds nothing and no successor attempt can exist behind
        # A's stale entry. ──
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
            row = await conn.fetchrow(
                f"SELECT status::text AS status, locked_by_worker, cancel_phase, "
                f'cancel_requested_at, finished_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] == "cancelled", (
                "PR #272: a cancel-phase row past the carve-out terminalises "
                "'cancelled'. A re-pend here would hand a stale holder's "
                "abandon a live successor to kill; the old reset-on-re-pend "
                "shape is exactly what the CASE reorder removed."
            )
            assert row["locked_by_worker"] is None
            assert row["cancel_phase"] == 2, (
                "The terminalised row keeps the phase A's escalation wrote, as "
                "the audit trail of the honoured request (the mark_cancelled "
                "doctrine)."
            )
            assert row["cancel_requested_at"] is not None
            assert row["finished_at"] is not None
        dispatched = await backend.dispatch_batch(
            worker_b, ["default"], limit=1, lock_lease=timedelta(seconds=60)
        )
        assert dispatched == [], (
            "PR #272: the terminalised row cannot re-enter the claim cycle, so "
            "worker B never receives an attempt 2 behind A's stale entry"
        )

        # ── Stage 3: A's loop recovers and ticks with its stale entry. The
        # phase-3 arm queues on the stale local FORCED alone and run_post_tx
        # calls mark_abandoned, whose guard is status='running' AND
        # cancel_phase=2 with no worker fence. The guard matches nothing: the
        # row is terminal. ──
        entry_a.cancel_observed_at -= 100.0
        await _tick(deps_a, backend, dsn, worker_a)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status::text AS status, locked_by_worker "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] == "cancelled", (
                "Contract: A's stale abandon must leave the terminalised row "
                "untouched; mark_abandoned's running+phase-2 guard has no "
                "living match"
            )
            assert row["locked_by_worker"] is None
            successor_attempts = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1 AND attempt >= 2',
                job_id,
            )
            assert int(successor_attempts or 0) == 0, (
                "Contract: no attempt row may exist at attempt 2 or beyond. One "
                "here would mean a successor attempt lived for A's stale "
                "abandon to terminate."
            )
    finally:
        if task_a is not None:
            task_a.cancel()
            await asyncio.gather(task_a, return_exceptions=True)
        await pool.close()

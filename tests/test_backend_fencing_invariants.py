"""Fencing-predicate invariants for the PG terminal writes and sweeps.

Why: these contracts are carried entirely by predicates inside SQL string
literals (``AND status = 'running'``, ``AND cancel_phase = 2``,
``SKIP LOCKED``).  Nothing in the Python source changes when one is
dropped, so they execute — and count as covered — with nothing asserting
them.  Each test here pins an outcome a queue user can observe: a
terminal job that stays terminal, a job that is not abandoned before its
cooperative-cancel grace has escalated, and a reclaim sweep that is not
stalled by an unrelated row lock.

``abandoned`` is the state that makes the terminal-write fences load
bearing: unlike every other terminal transition, ``mark_abandoned``
deliberately leaves ``locked_by_worker`` and ``lock_expires_at`` in
place (they are the audit trail of which worker was abandoned).  The
zombie actor task that was abandoned therefore still holds a *matching*
worker id, so the ownership fence alone does not stop it from writing a
second terminal state — only ``AND status = 'running'`` does.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest

from taskq.backend._protocol import ErrorInfo
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_pending_job, setup_running_job

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)
_ERROR = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)

# A sweep that is blocked on a row lock never returns; bound the wait so
# the failure is a clear timeout rather than a hung suite.
_SWEEP_TIMEOUT = 10.0


async def _abandon(app: JobsApp, *, lock_expires_at: datetime | None = None) -> tuple[UUID, UUID]:
    """Drive a job to ``abandoned`` and return ``(worker_id, job_id)``."""
    deps = app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(
            conn, schema, cancel_phase=2, lock_expires_at=lock_expires_at
        )
    assert await app.backend.mark_abandoned(job_id) is True
    return worker_id, job_id


async def _status(app: JobsApp, job_id: UUID) -> str:
    schema = app.deps.settings.schema_name
    async with app.deps.worker_pool.acquire() as conn:
        status: str = await conn.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
    return status


class TestAbandonedJobStaysTerminal:
    """A zombie actor that finishes after abandonment cannot resurrect the job."""

    async def test_mark_succeeded_does_not_apply(self, clean_jobs_app: JobsApp) -> None:
        worker_id, job_id = await _abandon(clean_jobs_app)

        assert await clean_jobs_app.backend.mark_succeeded(job_id, worker_id, {"ok": True}) is False
        assert await _status(clean_jobs_app, job_id) == "abandoned"

    async def test_mark_cancelled_does_not_apply(self, clean_jobs_app: JobsApp) -> None:
        worker_id, job_id = await _abandon(clean_jobs_app)

        assert await clean_jobs_app.backend.mark_cancelled(job_id, worker_id) is False
        assert await _status(clean_jobs_app, job_id) == "abandoned"

    async def test_mark_failed_does_not_apply(self, clean_jobs_app: JobsApp) -> None:
        worker_id, job_id = await _abandon(clean_jobs_app)

        # mark_failed reports a non-applying write by raising; the job row
        # is what matters — it must still be the abandoned terminal state.
        with pytest.raises(WorkerOwnershipMismatch):
            await clean_jobs_app.backend.mark_failed_or_retry(job_id, worker_id, _ERROR, None)
        assert await _status(clean_jobs_app, job_id) == "abandoned"

    async def test_reclaim_sweep_does_not_requeue(self, clean_jobs_app: JobsApp) -> None:
        """The abandoned row keeps its expired lock, so only the sweep's
        ``status = 'running'`` fence stops crash-recovery re-running it."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        _, job_id = await _abandon(
            clean_jobs_app, lock_expires_at=datetime.now(UTC) - timedelta(seconds=120)
        )

        async with deps.worker_pool.acquire() as conn:
            expired: datetime | None = await conn.fetchval(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
                job_id,
            )
            count = await PostgresBackend.sweep_expired_locks(
                conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
            )

        assert expired is not None, "abandoned rows keep their lock — the fence is the only guard"
        assert count == 0
        assert await _status(clean_jobs_app, job_id) == "abandoned"


class TestAbandonRequiresEscalation:
    """``mark_abandoned`` applies only at cancel phase 2 (escalated)."""

    @pytest.mark.parametrize("cancel_phase", [0, 1])
    async def test_below_phase_two_does_not_abandon(
        self, clean_jobs_app: JobsApp, cancel_phase: int
    ) -> None:
        """Phase 1 is a cooperative cancel still inside its grace window;
        abandoning there would kill a job that is shutting down cleanly."""
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        async with deps.worker_pool.acquire() as conn:
            _, job_id = await setup_running_job(conn, schema, cancel_phase=cancel_phase)

        assert await clean_jobs_app.backend.mark_abandoned(job_id) is False
        assert await _status(clean_jobs_app, job_id) == "running"


class TestSweepsSkipLockedRows:
    """A row locked by another transaction must not stall a sweep.

    The sweeps run on the leader's maintenance loop; blocking on one
    contended row would stop every other job in the queue from being
    recovered, promoted or deadline-failed.  ``SKIP LOCKED`` is what
    makes the sweep step over the contended row and finish.
    """

    async def _lock_row(self, conn: _Conn, schema: str, job_id: UUID) -> None:
        await conn.execute(
            f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE',  # noqa: S608
            job_id,
        )

    async def test_expired_lock_sweep_skips_contended_row(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        past = datetime.now(UTC) - timedelta(seconds=120)

        async with deps.worker_pool.acquire() as conn:
            _, locked_job = await setup_running_job(conn, schema, lock_expires_at=past)
            _, free_job = await setup_running_job(conn, schema, lock_expires_at=past)

        async with deps.worker_pool.acquire() as holder, holder.transaction():
            await self._lock_row(holder, schema, locked_job)

            async with deps.worker_pool.acquire() as sweeper:
                count = await asyncio.wait_for(
                    PostgresBackend.sweep_expired_locks(
                        sweeper, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
                    ),
                    timeout=_SWEEP_TIMEOUT,
                )

        assert count == 1
        assert await _status(clean_jobs_app, free_job) == "pending"
        assert await _status(clean_jobs_app, locked_job) == "running"

    async def test_deadline_sweep_skips_contended_row(self, clean_jobs_app: JobsApp) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        past = datetime.now(UTC) - timedelta(seconds=120)

        async with deps.worker_pool.acquire() as conn:
            locked_job = await create_pending_job(conn, schema, schedule_to_close=past)
            free_job = await create_pending_job(conn, schema, schedule_to_close=past)

        async with deps.worker_pool.acquire() as holder, holder.transaction():
            await self._lock_row(holder, schema, locked_job)

            async with deps.worker_pool.acquire() as sweeper:
                count = await asyncio.wait_for(
                    PostgresBackend.sweep_deadline_exceeded(sweeper, schema=schema),
                    timeout=_SWEEP_TIMEOUT,
                )

        assert count == 1
        assert await _status(clean_jobs_app, free_job) == "failed"
        assert await _status(clean_jobs_app, locked_job) == "pending"

    async def test_scheduled_promotion_sweep_skips_contended_row(
        self, clean_jobs_app: JobsApp
    ) -> None:
        deps = clean_jobs_app.deps
        schema = deps.settings.schema_name
        past = datetime.now(UTC) - timedelta(seconds=120)

        async with deps.worker_pool.acquire() as conn:
            locked_job = await create_pending_job(
                conn, schema, status="scheduled", scheduled_at=past
            )
            free_job = await create_pending_job(conn, schema, status="scheduled", scheduled_at=past)

        async with deps.worker_pool.acquire() as holder, holder.transaction():
            await self._lock_row(holder, schema, locked_job)

            async with deps.worker_pool.acquire() as sweeper:
                count = await asyncio.wait_for(
                    PostgresBackend.sweep_scheduled_to_pending(sweeper, schema=schema),
                    timeout=_SWEEP_TIMEOUT,
                )

        assert count == 1
        assert await _status(clean_jobs_app, free_job) == "pending"
        assert await _status(clean_jobs_app, locked_job) == "scheduled"

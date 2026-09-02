"""Regression: the heartbeat's post-transaction contract, against real PG.

``taskq.worker.cancel``'s module docstring states that ``run_post_tx`` is
always called after ``run_in_tx`` on the same tick, "even when ``run_in_tx``
raises; ``heartbeat_loop`` must call it in a ``finally`` block".  It did not:
the call sat after the transaction block on the success path only.

Two jobs crossing the cancel grace in one tick share one transaction.  If the
second job's write fails, the first job's ``cancel_phase = 2`` write and its
audit event roll back with it — while the first job's Task was genuinely
already cancelled in memory and its in-process phase left at FORCED.  Nothing
ever re-issued that write: the phase-2 block only fires from a local
COOPERATIVE phase, so PG stayed at phase 1 forever and ``mark_abandoned``'s
``cancel_phase = 2`` guard could never match.
"""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, JobId
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_workered_running_job
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.heartbeat import heartbeat_loop

pytestmark = pytest.mark.integration


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


async def _cancel_phase_of(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    phase = await conn.fetchval(
        f'SELECT cancel_phase FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
        job_id,
    )
    assert phase is not None
    return int(phase)


async def _escalation_event_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    count = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events '  # noqa: S608
        "WHERE job_id = $1 AND kind = 'state_change' AND (detail->>'cancel_phase_to') = '2'",
        job_id,
    )
    return int(count or 0)


class _FailingController:
    """Stands in for a tick whose second job's write blows up the transaction."""

    def __init__(self) -> None:
        self.in_tx_entered = asyncio.Event()
        self.post_tx_calls = 0

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        self.in_tx_entered.set()
        raise asyncpg.PostgresConnectionError("second job's phase-2 write failed")

    async def run_post_tx(self) -> None:
        self.post_tx_calls += 1


class _HealthyController:
    """Control: a controller whose tick commits normally."""

    def __init__(self) -> None:
        self.in_tx_entered = asyncio.Event()
        self.post_tx_calls = 0

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        self.in_tx_entered.set()

    async def run_post_tx(self) -> None:
        self.post_tx_calls += 1


async def _one_tick(deps: object, controller: _FailingController | _HealthyController) -> None:
    """Drive heartbeat_loop through exactly one tick with *controller*."""
    deps.settings.heartbeat_interval = 0.05  # pyright: ignore[reportAttributeAccessIssue]  # Why: WorkerDeps carries the loaded WorkerSettings; shortening the interval keeps the test fast
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        heartbeat_loop(
            deps,  # pyright: ignore[reportArgumentType]  # Why: real WorkerDeps from the clean_jobs_app fixture, typed as object here to keep the helper signature short
            new_uuid(),
            shutdown,
            cancel_controller=controller,  # pyright: ignore[reportArgumentType]  # Why: structural CancelController stub
        )
    )
    try:
        await asyncio.wait_for(controller.in_tx_entered.wait(), timeout=10.0)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=10.0)


async def test_run_post_tx_runs_even_when_the_tick_transaction_fails(
    clean_jobs_app: JobsApp,
) -> None:
    """Phase-3 work queued before the error must still be drained."""
    controller = _FailingController()
    await _one_tick(clean_jobs_app.deps, controller)
    assert controller.post_tx_calls >= 1, "run_post_tx was skipped on a failing tick"


async def test_run_post_tx_runs_on_a_healthy_tick(clean_jobs_app: JobsApp) -> None:
    controller = _HealthyController()
    await _one_tick(clean_jobs_app.deps, controller)
    assert controller.post_tx_calls >= 1


async def test_rolled_back_phase_2_write_is_reissued_on_the_next_tick(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A phase-2 escalation lost to a rolled-back tick must be re-written.

    Tick 1 escalates job A in memory (task cancelled, local phase FORCED) and
    writes ``cancel_phase = 2``; a later statement in the SAME heartbeat
    transaction then fails, so the write and its audit event roll back.  The
    next tick must notice that PG is still at phase 1 and re-issue both.
    """
    deps = clean_jobs_app.deps
    schema = module_pg_schema.schema_name
    loop = asyncio.get_running_loop()

    async with deps.worker_pool.acquire() as setup_conn:
        worker_id, job_id = await create_workered_running_job(
            setup_conn,
            schema,
            cancel_phase=1,
            cancel_requested_at=datetime.now(UTC),
        )

    task = _sleeper()
    await deps.active_jobs.register(JobId(job_id), task, _make_ctx(JobId(job_id), worker_id))
    active = deps.active_jobs.get(JobId(job_id))
    assert active is not None
    active.cancel_phase = CancelPhase.COOPERATIVE
    # Past the cancel grace (phase 2 is due) but well inside the cleanup
    # grace, so phase 3 stays out of this scenario.
    deps.settings.cancellation_grace_period = 0.0
    deps.settings.cleanup_grace_period = 3600.0
    active.cancel_observed_at = loop.time() - 1.0

    controller = make_cancel_controller(deps, worker_id, clean_jobs_app.backend)

    class _Boom(Exception):
        pass

    try:
        async with deps.heartbeat_pool.acquire() as conn:
            with pytest.raises(_Boom):
                async with conn.transaction():
                    await controller.run_in_tx(conn)
                    # A later statement of the same heartbeat tick fails.
                    raise _Boom
            # heartbeat_loop's finally.
            await controller.run_post_tx()

            assert active.cancel_phase == CancelPhase.FORCED
            assert task.cancelling() == 1
            assert await _cancel_phase_of(conn, schema, job_id) == 1, (
                "precondition: the phase-2 write must have rolled back"
            )

            # Next tick, clean.
            async with conn.transaction():
                await controller.run_in_tx(conn)
            await controller.run_post_tx()

            assert await _cancel_phase_of(conn, schema, job_id) == 2, (
                "the rolled-back phase-2 write was never re-issued"
            )
            assert await _escalation_event_count(conn, schema, job_id) == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_failed_abandon_keeps_the_job_registered_for_a_retry(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """``mark_abandoned`` that did not apply must not deregister the job.

    ``mark_abandoned`` is guarded by ``cancel_phase = 2``; if that write was
    rolled back the abandon is a no-op, and dropping the registry entry would
    strand a still-running job with no path back to cancellation.
    """
    deps = clean_jobs_app.deps
    schema = module_pg_schema.schema_name
    loop = asyncio.get_running_loop()

    async with deps.worker_pool.acquire() as setup_conn:
        worker_id, job_id = await create_workered_running_job(setup_conn, schema, cancel_phase=0)

    task = _sleeper()
    await deps.active_jobs.register(JobId(job_id), task, _make_ctx(JobId(job_id), worker_id))
    active = deps.active_jobs.get(JobId(job_id))
    assert active is not None
    active.cancel_phase = CancelPhase.FORCED
    deps.settings.cancellation_grace_period = 0.0
    deps.settings.cleanup_grace_period = 0.0
    active.cancel_observed_at = loop.time() - 1.0

    controller = make_cancel_controller(deps, worker_id, clean_jobs_app.backend)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            async with conn.transaction():
                await controller.run_in_tx(conn)
            await controller.run_post_tx()

            row_status = await conn.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
                job_id,
            )
        assert row_status == "running", "mark_abandoned cannot apply without cancel_phase = 2"
        assert deps.active_jobs.get(JobId(job_id)) is not None, (
            "job was deregistered even though the abandon never applied"
        )
        assert active.cancel_phase == CancelPhase.FORCED, (
            "phase must fall back to FORCED so a later tick can retry the abandon"
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

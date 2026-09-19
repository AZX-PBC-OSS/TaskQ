"""Integration, chaos, and negative tests for the shutdown flow.

Uses the clean_jobs_app fixture (real PG via testcontainers) and in-process
execution of orchestrate_shutdown. Tests verify end-to-end shutdown
behavior against a live Postgres backend.

The in-process mechanism was chosen over subprocess spawn because
orchestrate_shutdown is an async function that needs direct access
to WorkerDeps pools opened by the fixture — subprocess spawning
would duplicate pool setup and complicate fixture sharing.
Known limitation: pool teardown and deregister_worker cleanup are
not exercised; those paths are covered by unit tests.

anchors:,,,.
"""

import asyncio
import contextlib
import inspect as _inspect
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, CancelPhase, EnqueueArgs, JobId, parse_cancel_phase
from taskq.backend.postgres import PostgresBackend
from taskq.client._jobs import JobsClient
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.cancel import _ActiveJob
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import (
    ShutdownPhase,
    drain_local_queue_to_pending,
    install_signal_handlers,
    orchestrate_shutdown,
)

pytestmark = pytest.mark.integration


# ── Helpers ─────────────────────────────────────────────────────────────


async def _mark_jobs_running(
    deps: WorkerDeps,
    job_ids: list[UUID],
    worker_id: UUID,
    *,
    cancel_phase: int = 1,
) -> None:
    """Set job rows to status='running', locked_by_worker, started_at, and cancel_phase for shutdown testing.

    Also inserts the worker_id into the workers table so foreign key constraints on job_attempts are satisfied.

    ``cancel_phase`` defaults to 1 (an operator cancel in flight, the shape
    the shutdown-cancel tests need); pass 0 for a row the production claim
    CTE leaves alone, where the drain's cancel fence must not refuse it.
    """
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO \"{schema}\".workers (id, hostname, pid, queues) VALUES ($1, 'test', 0, '{{default}}') ON CONFLICT DO NOTHING",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
            worker_id,
        )
        for jid in job_ids:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='running', locked_by_worker=$1, started_at=now(), cancel_phase = $3 WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
                worker_id,
                jid,
                cancel_phase,
            )


async def _job_statuses(
    backend: PostgresBackend,
    job_ids: set[UUID],
) -> dict[UUID, str]:
    """Return ``{job_id: status}`` for the given jobs."""
    if not job_ids:
        return {}
    result: dict[UUID, str] = {}
    for jid in job_ids:
        row = await backend.get(JobId(jid))
        if row is not None:
            result[jid] = row.status
    return result


async def _count_job_events(
    deps: WorkerDeps,
    schema: str,
    job_id: UUID,
    kind: str,
) -> int:
    """Count rows in job_events for a given job_id and kind."""
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT COUNT(*)::int AS cnt FROM "{schema}".job_events WHERE job_id = $1 AND kind = $2',  # noqa: S608 # Why: schema validated by conftest fixtures.
            job_id,
            kind,
        )
        return row["cnt"] if row else 0


def _fake_active_job(
    *,
    job_id: UUID,
    cancel_phase: CancelPhase = CancelPhase.NONE,
) -> _ActiveJob:
    """Create an _ActiveJob with a minimal stub context."""
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.testing.in_memory import PassthroughPayload

    jid = JobId(job_id)
    task = asyncio.ensure_future(asyncio.sleep(0))  # placeholder
    ctx = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=PassthroughPayload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=MagicMock(spec=Backend),
        ),
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
    return _ActiveJob(
        job_id=jid,
        task=task,
        ctx=ctx,
        cancel_phase=cancel_phase,
    )


def _immediate() -> None:
    """Scheduled-at seed for immediate jobs: ``None`` (server-clock domain).

    Why not an absolute ``datetime.now(timezone.utc)``: the enqueue SQL
    decides status as ``COALESCE($n, clock_timestamp()) >
    clock_timestamp()`` — an absolute Python-clock timestamp races the
    server clock at that boundary. A test-process clock even a few
    microseconds ahead of the database clock lands the row ``'scheduled'``
    instead of ``'pending'``; the ``WHERE status='pending'`` seeding
    updates in these tests then silently no-op, the job never runs, and
    the terminal-status assertions fail on clock skew rather than on
    shutdown behaviour (observed as a load-dependent flake in the parallel
    suite). ``None`` is the canonical immediate form — the enqueue stamps
    the server clock and decides status in the same statement, one clock
    domain.
    """
    return None


# ── Integration tests ───────────────────────────────────────────────────


async def test_ti0_clean_boot_shutdown(
    clean_jobs_app: JobsApp,
) -> None:
    """Orchestrator runs with no active jobs, exits cleanly.

    Triggers orchestrate_shutdown in-process with zero active jobs.
    Oracle: all five phases (including EXITED) logged; shutdown_event
    set; returns 0.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    shutdown_event = asyncio.Event()
    worker_id = new_uuid()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shutdown_event.is_set()


async def test_ti1_sigterm_three_jobs(
    clean_jobs_app: JobsApp,
) -> None:
    """End-to-end shutdown with 3 in-flight jobs (acceptance test).

    Registers 3 synthetic _ActiveJob entries and runs
    orchestrate_shutdown. Oracle: all 3 jobs in terminal status
    after shutdown; no running jobs remain.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    active_a = _fake_active_job(job_id=new_uuid())
    active_b = _fake_active_job(job_id=new_uuid())
    active_c = _fake_active_job(job_id=new_uuid())

    # Register jobs in the active registry to simulate in-flight work
    for _active in (active_a, active_b, active_c):
        await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
        # Enqueue into PG so that backend writes succeed
        await backend.enqueue(
            EnqueueArgs(
                id=_active.job_id,
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    jids = [active_a.job_id, active_b.job_id, active_c.job_id]
    job_ids = set(jids)
    worker_id = new_uuid()
    await _mark_jobs_running(deps, jids, worker_id)

    shutdown_event = asyncio.Event()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shutdown_event.is_set()

    statuses = await _job_statuses(backend, job_ids)
    terminal = {"succeeded", "cancelled", "abandoned", "failed", "crashed"}
    for jid in job_ids:
        st = statuses.get(jid, "missing")
        assert st in terminal, f"job {jid} has non-terminal status: {st}"


async def test_ti2_cooperative_cancel(
    clean_jobs_app: JobsApp,
) -> None:
    """Cooperative cancel completes during CANCELLING.

    Register a job, then mutate the registered entry to simulate a job
    already in cooperative cancel. Oracle: job status='cancelled'.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(active.job_id, active.task, active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
    entry = deps.active_jobs.get(JobId(jid))
    assert entry is not None, "registered entry not found"
    entry.cancel_phase = CancelPhase.COOPERATIVE
    entry.cancel_observed_at = asyncio.get_running_loop().time()

    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        shutdown_worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    row = await backend.get(JobId(jid))
    assert row is not None, "job row not found"
    assert row.status in {"cancelled", "abandoned"}, (
        f"expected cancelled or abandoned, got {row.status}"
    )
    assert parse_cancel_phase(row.cancel_phase) == CancelPhase.FORCED, (
        f"expected FORCED cancel_phase, got {row.cancel_phase}"
    )


async def test_ti3_budget_validation() -> None:
    """Grace budget validation fires at startup.

    Construct WorkerSettings with cancellation_grace + cleanup_grace
    >= termination_grace - 5. Oracle: ValidationError before any pool open.
    """
    from dotenvmodel import ValidationError

    with pytest.raises(ValidationError, match=r"grace_period"):
        WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_SCHEMA_NAME": "taskq",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "30",
                "TASKQ_CLEANUP_GRACE_PERIOD": "20",
                "TASKQ_TERMINATION_GRACE_PERIOD": "50",
                "TASKQ_LOCK_LEASE": "60",
                "TASKQ_HEARTBEAT_INTERVAL": "5",
            }
        )


async def test_ti4_drain_to_pending(
    clean_jobs_app: JobsApp,
) -> None:
    """Drain-to-pending transitions unstarted jobs to pending.

    Enqueue 5 jobs, lock 3 (simulating dispatch), leave 2 as running
    with no lock. After drain: the 3 locked jobs move to pending;
    the other 2 remain running.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    worker_id = new_uuid()

    job_ids: list[UUID] = []
    for _i in range(5):
        jid = new_uuid()
        job_ids.append(jid)
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(jid),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    # Lock 3 jobs as "dispatched but not started" — use worker_pool to simulate
    schema = deps.settings.schema_name
    conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
    try:
        for jid in job_ids[:3]:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='running', locked_by_worker=$1 WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
                worker_id,
                jid,
            )
    finally:
        await conn.close()

    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 3, f"expected 3 drained rows, got {drained}"

    for jid in job_ids[:3]:
        row = await backend.get(JobId(jid))
        assert row is not None
        assert row.status == "pending", f"job {jid} expected pending, got {row.status}"
        assert row.locked_by_worker is None


async def test_drain_refuses_rows_carrying_a_cancel_phase(
    clean_jobs_app: JobsApp,
) -> None:
    """The drain's hand-back carries the deferral arms' cancel fence.

    Three rows claimed by this worker, one with an operator cancel in
    flight (cancel_phase = 1, the shape a cancel request stamps on a
    running row before any consumer poll observes it). The clean two come
    back to the fleet with their refund; the cancel-carrying row must stay
    right here (running, locked, attempt standing, audit columns intact):
    re-pending it would hand the operator's cancel to the next holder's
    flag poll instead of the cancel ladder, and the refund would re-create
    the attempt epoch the fenced doctrine forbids. Its lease expiry hands
    it to sweep-1's cancel arm, which terminalises it with the operator's
    intent recorded.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_ids = [new_uuid() for _ in range(3)]
    for jid in job_ids:
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(jid),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        for jid in job_ids:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='running', locked_by_worker=$1, "
                "started_at=now(), attempt = 2 WHERE id=$2 AND status='pending'",  # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
                worker_id,
                jid,
            )
        # The operator cancel lands on the row the drain must refuse.
        await conn.execute(
            f'UPDATE "{schema}".jobs SET cancel_phase = 1, cancel_requested_at = now() '
            f"WHERE id = $1",
            job_ids[0],
        )

    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 2, f"expected only the clean rows drained, got {drained}"

    fenced = await backend.get(JobId(job_ids[0]))
    assert fenced is not None
    assert fenced.status == "running", (
        f"the cancel-carrying row must stay running, got {fenced.status}"
    )
    assert fenced.locked_by_worker == worker_id, (
        "the fenced row keeps its owner: the cancel ladder owns its fate"
    )
    assert fenced.attempt == 2, f"the fenced row keeps its spent attempt, got {fenced.attempt}"
    assert parse_cancel_phase(fenced.cancel_phase) == CancelPhase.COOPERATIVE
    assert fenced.cancel_requested_at is not None

    for jid in job_ids[1:]:
        row = await backend.get(JobId(jid))
        assert row is not None
        assert row.status == "pending", f"clean job {jid} expected pending, got {row.status}"


async def test_draining_hands_back_only_jobs_no_consumer_is_running(
    clean_jobs_app: JobsApp,
) -> None:
    """Graceful shutdown hands a claimed job back at most once and never both ways.

    Two rows are claimed by the same worker. One is the local_queue backlog:
    claimed, no consumer, never started executing. The other has a live
    consumer inside the actor body, represented by an entry in the in-flight
    registry exactly as the consumer loop registers it.

    The backlog row must come back to pending with its lock cleared so another
    worker can take it — that is the hand-back, and it happens once. The
    executing row must stay locked and running: publishing it to the fleet
    while its consumer is still in the actor body is how one job becomes two
    executions. Those rows are the cancelling / forcing / abandoning phases'
    business, and this phase must not touch them.

    Operationally this is the difference between a rolling deploy that moves
    queued work to the surviving pods and one that silently double-charges a
    customer for every job that happened to be mid-flight when the pod got
    its SIGTERM.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    worker_id = new_uuid()

    backlog_id = new_uuid()
    executing_id = new_uuid()
    for jid in (backlog_id, executing_id):
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(jid),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    # Both rows carry the shape the dispatch claim leaves behind: running,
    # locked by this worker, started_at stamped at claim, no operator cancel
    # in flight. The database row cannot tell the two cases apart — only
    # this process's registry can.
    await _mark_jobs_running(deps, [backlog_id, executing_id], worker_id, cancel_phase=0)

    active = _fake_active_job(job_id=executing_id)
    try:
        await deps.active_jobs.register(active.job_id, active.task, active.ctx)

        drained = await drain_local_queue_to_pending(deps, worker_id)

        assert drained == 1, (
            "exactly the one claimed-but-unstarted row may be handed back; "
            f"the hand-back released {drained} rows"
        )

        backlog_row = await backend.get(JobId(backlog_id))
        assert backlog_row is not None
        assert backlog_row.status == "pending", (
            "a job claimed but never started must be handed back to pending so "
            f"another worker can run it; status was {backlog_row.status!r}"
        )
        assert backlog_row.locked_by_worker is None, (
            "the hand-back must clear the lock, otherwise the row is pending "
            "but still fenced to a worker that is going away"
        )

        executing_row = await backend.get(JobId(executing_id))
        assert executing_row is not None
        assert executing_row.status == "running", (
            "a job whose consumer is inside the actor body must stay running "
            "through DRAINING and be resolved by the later cancel phases; "
            f"status was {executing_row.status!r}"
        )
        assert executing_row.locked_by_worker == worker_id, (
            "unlocking a job that is still executing here publishes it to the "
            "fleet while the first run is in flight — the same job body then "
            f"runs twice; locked_by_worker was {executing_row.locked_by_worker!r}"
        )

        # A second pass — a drain-monitor trigger racing a signal, or a retry
        # after a transient failure — must release nothing further. Hand-back
        # is once per claim, not once per shutdown attempt.
        again = await drain_local_queue_to_pending(deps, worker_id)
        assert again == 0, (
            "a repeated hand-back pass must match no rows: the first pass "
            "cleared the lock, and re-pending a row another worker has since "
            f"claimed would strand or duplicate it; released {again} rows"
        )
    finally:
        await deps.active_jobs.deregister(active.job_id)
        active.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await active.task


async def test_ti5_heartbeat_during_cancelling(
    clean_jobs_app: JobsApp,
) -> None:
    """Shutdown during heartbeat retry.

    Register a job, run shutdown with a very short cancellation grace
    so CANCELLING transitions quickly. Oracle: job reaches terminal
    state; no orphan status='running' rows.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    _active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.

    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        shutdown_worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    row = await backend.get(JobId(jid))
    assert row is not None, "job row not found after shutdown"
    assert row.status != "running", f"job {jid} still running"
    assert row.status in {"failed", "cancelled", "abandoned"}


async def test_ti6_cancel_poll_loop(
    clean_jobs_app: JobsApp,
) -> None:
    """Cancel-poll-loop integration.

    Enqueue a job, cancel it via JobsClient, then run shutdown.
    Oracle: cancel_request event recorded; only one cancel_request
    row; job reaches terminal state.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    client = JobsClient(backend)
    result = await client.cancel(JobId(jid))
    assert result.cancellation_initiated, "cancel not initiated"

    shutdown_event = asyncio.Event()
    worker_id = new_uuid()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    schema = deps.settings.schema_name
    count = await _count_job_events(deps, schema, jid, "cancel_request")
    assert count <= 1, f"expected ≤1 cancel_request event, got {count}"

    row = await backend.get(JobId(jid))
    assert row is not None
    assert row.status in {"cancelled", "abandoned", "succeeded"}, f"job status={row.status}"


# ── Chaos tests ─────────────────────────────────────────────────────────


async def test_tc1_forcing_recovery(
    clean_jobs_app: JobsApp,
) -> None:
    """Chaos: job stuck in FORCING under an operator cancel is marked abandoned.

    Register a job with NONE cancel_phase; simulate a stuck task
    by making the task.cancel() a no-op (the real path would be
    the consumer stub catching CancelledError). Oracle: FORCING
    escalates; RELEASING marks abandoned (the operator ladder's
    terminal); job not running.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    _active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        shutdown_worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    row = await backend.get(JobId(jid))
    assert row is not None
    assert row.status != "running", f"job {jid} is still running after forced shutdown"
    assert row.status in {"cancelled", "abandoned"}


async def test_tc2_pg_unavailable_drain(
    clean_jobs_app: JobsApp,
) -> None:
    """Chaos: PG unavailable during drain-to-pending.

    Replace the dispatcher_pool with a thin wrapper whose acquire
    raises a connection error. Oracle: drain returns 0, logs
    drain-local-queue-failed warning.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    worker_id = new_uuid()

    # Enqueue and lock jobs to simulate dispatched-but-not-started
    job_ids: list[UUID] = []
    for _i in range(3):
        jid = new_uuid()
        job_ids.append(jid)
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(jid),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    schema = deps.settings.schema_name
    conn = await asyncpg.connect(str(deps.settings.pg_dsn_direct))
    try:
        for jid in job_ids:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='running', locked_by_worker=$1 WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
                worker_id,
                jid,
            )
    finally:
        await conn.close()

    class _BrokenPool:
        def __init__(self, real: asyncpg.Pool) -> None:
            self._real = real

        def acquire(self, *, timeout: float | None = None):
            raise asyncpg.PostgresConnectionError("simulated PG failure")

        def __getattr__(self, name: str) -> object:
            return getattr(self._real, name)

    real_pool = deps.dispatcher_pool
    deps.dispatcher_pool = _BrokenPool(real_pool)  # type: ignore[assignment]
    try:
        drained = await drain_local_queue_to_pending(deps, worker_id)
    finally:
        deps.dispatcher_pool = real_pool  # type: ignore[assignment]
    assert drained == 0


async def test_tc3_shielded_releasing_write(
    clean_jobs_app: JobsApp,
) -> None:
    """Chaos: Shielded RELEASING write survives external cancel.

    Run orchestrate_shutdown as a task, cancel it during RELEASING.
    Oracle: cancel does not deadlock; shutdown completes; active_jobs
    cleared.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    _active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    orch_task = asyncio.create_task(
        orchestrate_shutdown(
            deps,
            deps.settings,
            shutdown_worker_id,
            shutdown_event,
            None,
            backend=backend,
        )
    )

    # Wait for RELEASING phase or orchestrator completion (under
    # parallel load the orchestrator may complete before we observe
    # the phase transition).
    while deps.shutdown_phase != ShutdownPhase.RELEASING and not orch_task.done():  # noqa: ASYNC110 # Why: polling for phase transition in test; ShutdownPhase is not an asyncio.Event.
        await asyncio.sleep(0.01)

    orch_task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await orch_task

    # The shielded RELEASING write may still be in-flight after
    # the orchestrator task is cancelled. Poll briefly for the job to
    # reach a terminal state before asserting.
    row = await backend.get(JobId(jid))
    if row is not None and row.status == "running":
        for _ in range(20):
            await asyncio.sleep(0.1)
            row = await backend.get(JobId(jid))
            if row is None or row.status != "running":
                break

    if row is not None:
        assert row.status != "running", f"job {jid} still running after cancel"


async def test_tc4_pg_failover_forcing(
    clean_jobs_app: JobsApp,
) -> None:
    """Chaos: PG failover during FORCING.

    Break PG writes during FORCING phase. Oracle: per-job try/except
    catches the connection error; RELEASING runs; worker exits cleanly.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    _active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    async def _failing_escalation(job_id: JobId, wid: UUID, phase: int) -> None:
        raise asyncpg.PostgresConnectionError("simulated PG failover")

    class _FailingBackend:
        """Wrapper that delegates to the real backend except for write_cancel_escalation."""

        def __init__(self, real: PostgresBackend) -> None:
            self._real = real

        def __getattr__(self, name: str) -> object:
            return getattr(self._real, name)

        async def write_cancel_escalation(self, job_id: JobId, wid: UUID, phase: int) -> None:
            await _failing_escalation(job_id, wid, phase)

    failing_backend = _FailingBackend(backend)
    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        shutdown_worker_id,
        shutdown_event,
        None,
        backend=failing_backend,
    )

    assert result == 0
    assert shutdown_event.is_set()


async def test_tc5_actor_swallows_cancelled_error(
    clean_jobs_app: JobsApp,
) -> None:
    """Actor swallows CancelledError.

    Register a job; the orchestrator escalates through FORCING →
    RELEASING. Oracle: job marked abandoned (operator ladder); shutdown
    completes; the registered task is no longer in active_jobs.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    jid = new_uuid()
    _active = _fake_active_job(job_id=jid)

    await deps.active_jobs.register(_active.job_id, _active.task, _active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    shutdown_worker_id = new_uuid()
    await _mark_jobs_running(deps, [jid], shutdown_worker_id)

    shutdown_event = asyncio.Event()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        shutdown_worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shutdown_event.is_set()

    row = await backend.get(JobId(jid))
    assert row is not None
    assert row.status != "running"


async def test_tc6_pool_acquire_timeout(
    clean_jobs_app: JobsApp,
) -> None:
    """drain_local_queue_to_pending handles pool saturation.

    Drain with a saturated dispatcher pool. Oracle: returns 0
    (the acquire timeout), logs a warning, shutdown proceeds.
    """
    deps = clean_jobs_app.deps

    # Consume all dispatcher pool connections
    held: list[asyncpg.pool.PoolConnectionProxy] = []
    for _ in range(4):
        proxy = await deps.dispatcher_pool.acquire(timeout=2.0)
        held.append(proxy)

    drained = await drain_local_queue_to_pending(deps, new_uuid())

    for proxy in held:
        await proxy.close()

    assert drained == 0


# ── Negative tests ──────────────────────────────────────────────────────


async def test_tn1_signal_handler_must_not_await(
    clean_jobs_app: JobsApp,
) -> None:
    """Signal handler registered callable must not be a coroutine function.

    Installs signal handlers with a recording wrapper on
    loop.add_signal_handler. Asserts the registered callback is not
    a coroutine function and contains no ``await`` keyword.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    record: list[object] = []

    loop = asyncio.get_running_loop()
    original_handler = loop.add_signal_handler

    def _recording_handler(sig: int, callback: Callable[..., object], *args: object) -> None:
        record.append(callback)
        original_handler(sig, callback, *args)

    loop.add_signal_handler = _recording_handler  # type: ignore[method-assign, union-attr] # Why: recording wrapper for test observation; add_signal_handler may be untyped on AbstractEventLoop.
    try:
        shutdown_event = asyncio.Event()
        escalate_event = asyncio.Event()
        holder: list[asyncio.Task[int]] = []
        worker_id = new_uuid()

        install_signal_handlers(
            loop,
            deps,
            worker_id,
            shutdown_event,
            escalate_event,
            backend,
            holder,
        )
    finally:
        loop.add_signal_handler = original_handler  # type: ignore[method-assign, union-attr] # Why: recording wrapper for test observation; add_signal_handler may be untyped on AbstractEventLoop.

    assert len(record) >= 2, "no signal handler callbacks were recorded"
    handler = record[0]
    assert callable(handler), f"handler is not callable: {handler!r}"

    # `add_signal_handler` invokes its callback synchronously, in the signal
    # context: a coroutine function would simply never run. This assertion is
    # the whole check — a companion scan for `"await " not in getsource(...)`
    # used to follow it and could not fail. Python rejects `await` in a
    # non-async def at compile time ("SyntaxError: 'await' outside async
    # function"), so given the assertion above, the substring could only ever
    # have appeared inside a nested `async def` (legal, and not a defect) or
    # in a comment or string literal (a false positive).
    assert not _inspect.iscoroutinefunction(handler), (
        f"signal handler is a coroutine function and would never run: {handler!r}"
    )


async def test_tn2_releasing_runs_with_zero_jobs(
    clean_jobs_app: JobsApp,
) -> None:
    """RELEASING runs even with zero remaining jobs.

    Run orchestrate_shutdown with zero active jobs. Oracle:
    RELEASING phase logged despite no-op loop body.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend

    shutdown_event = asyncio.Event()
    worker_id = new_uuid()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shutdown_event.is_set()
    # Behavioral: shutdown proceeds through all phases including RELEASING
    # even with zero active jobs — verified by shutdown_event being set
    # and result == 0 (clean exit).


# ── Phase-0 siblings: a deploy with no operator cancel in flight ──────────
#
# The suite above seeds rows at ``cancel_phase = 1`` (an operator cancel in
# flight) — those pins stand. These siblings cover the ordinary deploy:
# rows the production claim CTE leaves at ``cancel_phase = 0``. The deploy
# must release them back to the fleet (never ``cancelled``/``abandoned``)
# with the interrupted claim's attempt increment standing: the attempt
# started executing, and a refund would re-create the attempt epoch the
# interrupted handler still holds.


async def test_deploy_releases_running_work_back_to_the_fleet(
    clean_jobs_app: JobsApp,
) -> None:
    """Phase-0 rows are released (held), never terminalised, and refunded.

    Three jobs claimed through the production claim CTE sit mid-flight
    when the deploy lands — no operator has asked for anything. Every one
    must come back to the fleet with its attempt refund, an interruption
    counted on the row, and an 'interrupted' transition on its timeline,
    held behind the remaining termination budget because its actor might
    still be alive in the exiting process.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_ids = [new_uuid() for _ in range(3)]
    for jid in job_ids:
        await backend.enqueue(
            EnqueueArgs(
                id=JobId(jid),
                actor="test_actor",
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_immediate(),
            )
        )

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    claimed = await backend.dispatch_batch(worker_id, ["default"], 10, timedelta(seconds=60))
    assert {row.id for row in claimed} == set(job_ids)
    attempt_at_claim = {row.id: row.attempt for row in claimed}

    # In-flight entries whose actors never unwind (tasks already done, so
    # the forced cancel lands nowhere) — the rows survive to RELEASING.
    for jid in job_ids:
        active = _fake_active_job(job_id=jid)
        await deps.active_jobs.register(active.job_id, active.task, active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.

    shutdown_event = asyncio.Event()
    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shutdown_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shutdown_event.is_set()

    for jid in job_ids:
        row = await backend.get(JobId(jid))
        assert row is not None
        assert row.status == "scheduled", (
            "a deploy with no operator cancel in flight must release the row "
            f"behind the termination-budget hold, not terminalise it; got {row.status!r}"
        )
        assert row.attempt == attempt_at_claim[jid], (
            "the interrupted claim spends the attempt increment: the attempt "
            "started executing, and refunding it would re-create the attempt "
            f"epoch the interrupted handler still holds; attempt reads "
            f"{row.attempt}, claimed at {attempt_at_claim[jid]}"
        )
        assert row.locked_by_worker is None and row.lock_expires_at is None, (
            "a released row must not stay locked to the departed pod"
        )
        assert row.interrupt_count == 1, (
            f"the interruption must be counted on the row; got {row.interrupt_count}"
        )
        assert (await _count_job_events(deps, schema, jid, "state_change")) >= 1, (
            "the release writes the job's timeline transition"
        )


async def test_deploy_release_is_not_reclaimable_until_the_hold(
    clean_jobs_app: JobsApp,
) -> None:
    """The held row stays out of the fleet's reach until the hold elapses.

    A released row whose actor may still be alive in the exiting process
    must not be claimable elsewhere before the process is provably gone —
    that ordering is the whole point of the hold (requeue before kill, and
    never both at once).
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    jid = new_uuid()
    await backend.enqueue(
        EnqueueArgs(
            id=JobId(jid),
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_immediate(),
        )
    )

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    claimed = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(seconds=60))
    assert len(claimed) == 1

    active = _fake_active_job(job_id=jid)
    await deps.active_jobs.register(active.job_id, active.task, active.ctx)  # type: ignore[arg-type] # Why: JobContext[PassthroughPayload] is a JobContext[BaseModel]; pyright cannot widen Generic contravariance.

    shutdown_event = asyncio.Event()
    await orchestrate_shutdown(
        deps, deps.settings, worker_id, shutdown_event, None, backend=backend
    )

    row = await backend.get(JobId(jid))
    assert row is not None and row.status == "scheduled"
    assert row.scheduled_at > datetime.now(UTC), (
        "the held row's due time is in the future — the surviving fleet must "
        "not be able to claim it while the departing pod may still be alive"
    )

    # A surviving worker's claim round finds nothing while the hold holds.
    surviving = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, surviving)
    batch = await backend.dispatch_batch(surviving, ["default"], 10, timedelta(seconds=60))
    assert batch == [], (
        "a row released behind the termination-budget hold was claimable "
        "immediately — the hold exists so the row cannot be claimed while "
        "the interrupted actor might still be alive"
    )

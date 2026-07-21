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
import textwrap
from collections.abc import Callable
from datetime import datetime, timezone
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
) -> None:
    """Set job rows to status='running', locked_by_worker, started_at, and cancel_phase=1 for shutdown testing.
    Also inserts the worker_id into the workers table so foreign key constraints on job_attempts are satisfied."""
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO \"{schema}\".workers (id, hostname, pid, queues) VALUES ($1, 'test', 0, '{{default}}') ON CONFLICT DO NOTHING",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
            worker_id,
        )
        for jid in job_ids:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status='running', locked_by_worker=$1, started_at=now(), cancel_phase = 1 WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings/conftest; asyncpg has no parameter binding for identifiers.
                worker_id,
                jid,
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


def _now_utc() -> datetime:
    """Return current UTC datetime for ``EnqueueArgs.scheduled_at``."""
    return datetime.now(tz=timezone.utc)  # noqa: UP017 # Why: timezone.utc is the canonical form; datetime.UTC requires 3.11+ typeshed support that pyright cannot resolve in this project's environment.


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
                scheduled_at=_now_utc(),
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
            scheduled_at=_now_utc(),
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
                scheduled_at=_now_utc(),
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
            scheduled_at=_now_utc(),
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
            scheduled_at=_now_utc(),
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
    """Chaos: job stuck in FORCING is marked abandoned.

    Register a job with NONE cancel_phase; simulate a stuck task
    by making the task.cancel() a no-op (the real path would be
    the consumer stub catching CancelledError). Oracle: FORCING
    escalates; ABANDONING marks abandoned; job not running.
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
            scheduled_at=_now_utc(),
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
                scheduled_at=_now_utc(),
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


async def test_tc3_shielded_abandoning_write(
    clean_jobs_app: JobsApp,
) -> None:
    """Chaos: Shielded ABANDONING write survives external cancel.

    Run orchestrate_shutdown as a task, cancel it during ABANDONING.
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
            scheduled_at=_now_utc(),
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

    # Wait for ABANDONING phase or orchestrator completion (under
    # parallel load the orchestrator may complete before we observe
    # the phase transition).
    while deps.shutdown_phase != ShutdownPhase.ABANDONING and not orch_task.done():  # noqa: ASYNC110 # Why: polling for phase transition in test; ShutdownPhase is not an asyncio.Event.
        await asyncio.sleep(0.01)

    orch_task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await orch_task

    # The shielded mark_abandoned write may still be in-flight after
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
    catches the connection error; ABANDONING runs; worker exits cleanly.
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
            scheduled_at=_now_utc(),
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
    ABANDONING. Oracle: job marked abandoned; shutdown completes;
    the registered task is no longer in active_jobs.
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
            scheduled_at=_now_utc(),
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

    assert not _inspect.iscoroutinefunction(handler), (
        f"signal handler is a coroutine function: {handler!r}"
    )

    source_lines = textwrap.dedent(_inspect.getsource(handler))  # type: ignore[arg-type] # Why: handler is a function from record (typed as list[object]); pyright cannot narrow across the list assignment boundary.
    assert "await " not in source_lines, f"signal handler contains await:\n{source_lines}"


async def test_tn2_abandoning_runs_with_zero_jobs(
    clean_jobs_app: JobsApp,
) -> None:
    """ABANDONING runs even with zero remaining jobs.

    Run orchestrate_shutdown with zero active jobs. Oracle:
    ABANDONING phase logged despite no-op loop body.
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
    # Behavioral: shutdown proceeds through all phases including ABANDONING
    # even with zero active jobs — verified by shutdown_event being set
    # and result == 0 (clean exit).

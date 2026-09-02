"""Unit tests for orchestrate_shutdown four-phase orchestrator."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import UUID

import asyncpg
import pytest
import structlog
from hypothesis import HealthCheck, given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, CancelPhase, JobId
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import ConnFactory, PoolFactory, WorkerConnections
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.in_memory import PassthroughPayload
from taskq.worker.cancel import _ActiveJob
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.shutdown import orchestrate_shutdown

# ── Helpers ─────────────────────────────────────────────────────────────


class FakeClock:
    """Monotonically advancing clock for time-sensitive shutdown tests."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    @property
    def time_val(self) -> float:
        return self._t

    async def sleep(self, delta: float) -> None:
        self._t += delta


def _worker_settings(
    *,
    cancellation_grace: float = 30.0,
    cleanup_grace: float = 10.0,
    termination_grace: float = 60.0,
    lock_lease: float = 45.0,
    heartbeat_interval: float = 5.0,
    schema_name: str = "taskq",
) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema_name,
            "TASKQ_CANCELLATION_GRACE_PERIOD": str(cancellation_grace),
            "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_grace),
            "TASKQ_TERMINATION_GRACE_PERIOD": str(termination_grace),
            "TASKQ_LOCK_LEASE": str(lock_lease),
            "TASKQ_HEARTBEAT_INTERVAL": str(heartbeat_interval),
        }
    )


def _make_fake_active_job(
    *,
    job_id: UUID | None = None,
    cancel_phase: CancelPhase = CancelPhase.NONE,
    cancel_observed_at: float | None = None,
) -> _ActiveJob:
    if job_id is None:
        job_id = new_uuid()
    jid = JobId(job_id)
    return _ActiveJob(
        job_id=jid,
        task=MagicMock(),
        ctx=JobContext(
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
        ),
        cancel_phase=cancel_phase,
        cancel_observed_at=cancel_observed_at,
    )


class FakeActiveJobRegistry:
    """Test double for ActiveJobRegistry — mutable list of fake jobs."""

    def __init__(self, jobs: list[_ActiveJob] | None = None) -> None:
        self._jobs: list[_ActiveJob] = list(jobs or [])

    def all(self) -> list[_ActiveJob]:
        return list(self._jobs)

    def count(self) -> int:
        return len(self._jobs)

    def deregister(self, job_id: JobId) -> None:
        self._jobs = [j for j in self._jobs if j.job_id != job_id]

    def set_jobs(self, jobs: list[_ActiveJob]) -> None:
        self._jobs = list(jobs)


def _patch_clock(
    monkeypatch: pytest.MonkeyPatch,
    clock: FakeClock,
    fake_loop: Mock,
) -> None:
    fake_loop.time = lambda: clock.time_val  # type: ignore[method-assign] # Why: Mock attribute shadowing for loop.time() callable; the test clock replaces the event loop's monotonic clock.
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: fake_loop)
    monkeypatch.setattr(asyncio, "sleep", clock.sleep)


def _make_deps(
    *,
    registry: FakeActiveJobRegistry | None = None,
    settings: WorkerSettings | None = None,
    leader_conn: asyncpg.Connection | None = None,
    owns_leader_conn: bool = True,
) -> WorkerDeps:
    pool = MagicMock()
    deps = WorkerDeps(
        settings=settings or _worker_settings(),
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: MagicMock drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        # Default True: these tests exercise the TaskQ-owned close path.
        # Caller-owned (False) is covered in test_shutdown_signals.py.
        owns_leader_conn=owns_leader_conn,
    )
    if registry is not None:
        deps.active_jobs = registry  # type: ignore[assignment]
    return deps


# ── Phase ordering with mock clock ───────────────────────────────


async def test_phase_ordering_and_backend_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Phases transition in order; backend called at correct phases."""
    import taskq.worker.shutdown as shutdown_mod

    job_a = _make_fake_active_job(job_id=UUID("11111111-1111-1111-1111-111111111111"))
    job_b = _make_fake_active_job(job_id=UUID("22222222-2222-2222-2222-222222222222"))
    registry = FakeActiveJobRegistry([job_a, job_b])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()
    worker_id = new_uuid()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shut_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shut_event.is_set()

    mock_drain.assert_called_once_with(deps, worker_id)

    for job in (job_a, job_b):
        assert job.ctx.cancel_event.is_set()
        assert job.cancel_phase == CancelPhase.FORCED

    assert backend.write_cancel_escalation.call_count == 2
    assert backend.mark_abandoned.call_count == 2

    assert 0.7 < clock.time_val < 1.0


# ── Shielded cleanup ─────────────────────────────────────────────


async def test_shielded_write_completes_despite_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """asyncio.shield allows PG write to complete under CancelledError."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job()
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(
        cancellation_grace=0.01, cleanup_grace=60.0, termination_grace=200.0, lock_lease=70.0
    )
    deps = _make_deps(registry=registry, settings=settings)

    write_entered = asyncio.Event()
    write_may_return = asyncio.Event()
    write_done = False

    async def _shielded_write(job_id: JobId, worker_id: UUID, phase: int) -> bool:
        nonlocal write_done
        write_entered.set()
        await write_may_return.wait()
        write_done = True
        return True

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = _shielded_write
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    orch_task = asyncio.ensure_future(
        orchestrate_shutdown(
            deps,
            deps.settings,
            new_uuid(),
            shut_event,
            None,
            backend=backend,
        )
    )

    await write_entered.wait()
    orch_task.cancel()
    write_may_return.set()

    with pytest.raises(asyncio.CancelledError):
        await orch_task

    assert write_done


# ── FORCING PG-write-before-cancel ordering ─────────────────────


async def test_forcing_pg_write_before_cancel_phase_advances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FORCING: PG write succeeds → cancel_phase advances to FORCED."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job()
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()
    worker_id = new_uuid()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shut_event,
        None,
        backend=backend,
    )

    assert job.cancel_phase == CancelPhase.FORCED
    backend.write_cancel_escalation.assert_called_with(job.job_id, worker_id, phase=2)


# ── FORCING per-job PG write failure isolation ──────────────────


async def test_forcing_failure_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Single PG write failure isolates that job; others proceed."""
    import taskq.worker.shutdown as shutdown_mod

    job1 = _make_fake_active_job(job_id=UUID("11111111-1111-1111-1111-111111111111"))
    job2 = _make_fake_active_job(job_id=UUID("22222222-2222-2222-2222-222222222222"))
    job3 = _make_fake_active_job(job_id=UUID("33333333-3333-3333-3333-333333333333"))
    registry = FakeActiveJobRegistry([job1, job2, job3])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    wce_side_effects: list[object] = [
        asyncpg.PostgresConnectionError("job1 gone"),
        True,
        True,
        True,
        True,
        True,
    ]
    backend.write_cancel_escalation = AsyncMock(side_effect=wce_side_effects)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()
    worker_id = new_uuid()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shut_event,
        None,
        backend=backend,
    )

    assert job1.cancel_phase == CancelPhase.COOPERATIVE
    assert job2.cancel_phase == CancelPhase.FORCED
    assert job3.cancel_phase == CancelPhase.FORCED


async def test_abandoning_failure_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """ABANDONING variant. Single PG write failure isolates that job."""
    import taskq.worker.shutdown as shutdown_mod

    job1 = _make_fake_active_job(job_id=UUID("11111111-1111-1111-1111-111111111111"))
    job2 = _make_fake_active_job(job_id=UUID("22222222-2222-2222-2222-222222222222"))
    job3 = _make_fake_active_job(job_id=UUID("33333333-3333-3333-3333-333333333333"))
    registry = FakeActiveJobRegistry([job1, job2, job3])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    wce_succeeded = [True, True, True, True, True, True]
    abandon_call_count = 0

    async def _failing_abandon(job_id: JobId) -> bool:
        nonlocal abandon_call_count
        abandon_call_count += 1
        if abandon_call_count == 1:
            raise asyncpg.PostgresConnectionError("abandon failed for job1")
        return True

    backend.write_cancel_escalation = AsyncMock(side_effect=wce_succeeded)
    backend.mark_abandoned = AsyncMock(side_effect=_failing_abandon)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    assert backend.mark_abandoned.call_count == 3


# ── Empty active_jobs ────────────────────────────────────────────


async def test_empty_active_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero in-flight: all phases logged, shutdown_event set, returns 0."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    deps = _make_deps(registry=registry)
    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    result = await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    assert result == 0
    assert shut_event.is_set()

    backend.write_cancel_escalation.assert_not_called()
    backend.mark_abandoned.assert_not_called()


# ── Race winner ──────────────────────────────────────────────────


async def test_race_winner_not_abandoned(monkeypatch: pytest.MonkeyPatch) -> None:
    """Job that deregisters mid-CANCELLING is not marked abandoned."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job()
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()
    worker_id = new_uuid()

    call_count = 0

    async def _sleep_with_deregister(delta: float) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            registry.set_jobs([])
        await clock.sleep(delta)

    monkeypatch.setattr(shutdown_mod.asyncio, "sleep", _sleep_with_deregister)

    await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shut_event,
        None,
        backend=backend,
    )

    backend.mark_abandoned.assert_not_called()


# ── MAJOR-3: CANCELLING guard (don't restart grace clock) ───────────────


async def test_cancelling_guard_preserves_observed_at(monkeypatch: pytest.MonkeyPatch) -> None:
    """MAJOR-3. cancel_observed_at unchanged for already-cooperative jobs."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job(
        cancel_phase=CancelPhase.COOPERATIVE,
        cancel_observed_at=100.0,
    )
    registry = FakeActiveJobRegistry([job])
    deps = _make_deps(registry=registry)
    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock(start=125.0)
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    assert job.ctx.cancel_event.is_set()
    assert job.cancel_phase == CancelPhase.FORCED
    assert job.cancel_observed_at == 100.0


# ── W-2: Defensive cancel_observed_at is None guard ─────────────────────


async def test_defensive_none_observed_at_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """W-2. COOPERATIVE with cancel_observed_at=None sets it to loop.time()."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job(
        cancel_phase=CancelPhase.COOPERATIVE,
        cancel_observed_at=None,
    )
    registry = FakeActiveJobRegistry([job])
    deps = _make_deps(registry=registry)
    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    clock = FakeClock(start=50.0)
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    assert job.ctx.cancel_event.is_set()
    assert job.cancel_phase == CancelPhase.FORCED
    assert job.cancel_observed_at == 50.0


# ── Second-signal fast-advance ───────────────────────────────────


async def test_second_signal_fast_advance(monkeypatch: pytest.MonkeyPatch) -> None:
    """escalate_event triggers fast transition from CANCELLING to FORCING."""
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job()
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(cancellation_grace=60.0, termination_grace=200.0, lock_lease=80.0)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))

    shut_event = asyncio.Event()
    escalate_event = asyncio.Event()

    async def _delayed_escalate() -> None:
        await asyncio.sleep(0.005)
        escalate_event.set()

    asyncio.ensure_future(_delayed_escalate())  # noqa: RUF006 # Why: fire-and-forget escalation task; its side-effect (setting the event) is the test oracle.

    t0 = asyncio.get_running_loop().time()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        escalate_event,
        backend=backend,
    )

    elapsed = asyncio.get_running_loop().time() - t0
    assert elapsed < 11.0  # CANCELLING breaks early (~0.1s) + cleanup_grace (10s) + ABANDONING


# ── leader_conn close ───────────────────────────────────────────────────


async def test_leader_conn_closed_and_nulled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leader conn is closed and set to None during shutdown."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()
    leader_conn = MagicMock(spec=asyncpg.Connection)
    leader_conn.close = AsyncMock()

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        owns_leader_conn=True,  # Why: these tests exercise the TaskQ-owned close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    leader_conn.close.assert_called_once()
    assert deps.leader_conn is None


async def test_leader_conn_close_error_suppressed_and_nulled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leader conn close error is suppressed; conn is still set to None."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()
    leader_conn = MagicMock(spec=asyncpg.Connection)
    leader_conn.close = AsyncMock(side_effect=asyncpg.PostgresConnectionError("gone"))

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        owns_leader_conn=True,  # Why: these tests exercise the TaskQ-owned close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    leader_conn.close.assert_called_once()
    assert deps.leader_conn is None


async def test_leader_conn_os_error_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    """OSError on leader conn close is suppressed; conn nulled."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()
    leader_conn = MagicMock(spec=asyncpg.Connection)
    leader_conn.close = AsyncMock(side_effect=OSError("closed"))

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        owns_leader_conn=True,  # Why: these tests exercise the TaskQ-owned close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )

    leader_conn.close.assert_called_once()
    assert deps.leader_conn is None


async def test_leader_conn_none_skips_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """No leader_conn — close is skipped without error."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    deps = _make_deps(registry=registry, leader_conn=None)
    backend = AsyncMock(spec=Backend)
    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)

    shut_event = asyncio.Event()

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        None,
        backend=backend,
    )


async def test_orchestrate_shutdown_terminates_hung_leader_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung leader_conn.close() (dead PG) cannot wedge shutdown: after the
    bounded teardown timeout the conn is terminated and still nulled."""
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()
    leader_conn = MagicMock(spec=asyncpg.Connection)

    hang_forever = asyncio.Event()  # never set — close() blocks indefinitely

    async def _hung_close() -> None:
        await hang_forever.wait()

    leader_conn.close = AsyncMock(side_effect=_hung_close)

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        owns_leader_conn=True,  # Why: exercises the TaskQ-owned bounded-close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    mock_drain = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", mock_drain)
    # Shrink the teardown bound so the test doesn't wait the full 5s default.
    # The seam lives on shutdown_mod: orchestrate_shutdown reads the
    # CLOSE_TIMEOUT_SECS module global imported into taskq.worker.shutdown.
    monkeypatch.setattr(shutdown_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    shut_event = asyncio.Event()

    # Why the outer timeout: pre-fix orchestrate_shutdown awaited
    # leader_conn.close() unbounded, so the RED state would hang forever
    # instead of failing fast.
    async with asyncio.timeout(5):
        result = await orchestrate_shutdown(
            deps,
            deps.settings,
            new_uuid(),
            shut_event,
            None,
            backend=backend,
        )

    assert result == 0
    leader_conn.terminate.assert_called_once()
    assert deps.leader_conn is None


async def test_orchestrate_shutdown_does_not_null_swapped_leader_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Race regression: a leader conn swapped in mid-close must not be nulled.

    While the bounded close is parked, a still-live election loop can drop
    the closing conn and reopen a fresh one. Nulling unconditionally would
    orphan that fresh (possibly lock-holding) conn; the orchestrator nulls
    only before the park, so the swapped-in conn keeps its reference and
    the deps exit-stack guard closes it later.
    """
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()

    conn_a = MagicMock(spec=asyncpg.Connection)
    close_began = asyncio.Event()
    close_gate = asyncio.Event()

    async def _gated_close() -> None:
        close_began.set()
        await close_gate.wait()

    conn_a.close = AsyncMock(side_effect=_gated_close)
    conn_b = MagicMock(spec=asyncpg.Connection)
    conn_b.close = AsyncMock()

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=conn_a,
        owns_leader_conn=True,  # Why: exercises the TaskQ-owned bounded-close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))

    shut_event = asyncio.Event()

    orch_task = asyncio.ensure_future(
        orchestrate_shutdown(
            deps,
            deps.settings,
            new_uuid(),
            shut_event,
            None,
            backend=backend,
        )
    )

    # Why the outer timeout: the close gate parks the orchestrator, so a
    # broken RED state must fail fast rather than hang the suite.
    async with asyncio.timeout(5):
        await close_began.wait()
        # Simulate the election loop's health probe dropping the closing
        # conn and reopening a fresh one mid-park.
        deps.leader_conn = conn_b
        close_gate.set()
        result = await orch_task

    assert result == 0
    conn_a.close.assert_called_once()
    conn_b.close.assert_not_called()
    assert deps.leader_conn is conn_b


async def test_orchestrate_shutdown_sets_shutdown_event_before_leader_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The election loop must be stopped BEFORE the bounded close parks.

    The conn records shutdown_event.is_set() at the moment its close() is
    entered; on unfixed code the event fires only in the finally, leaving
    the election loop live for the whole close park — the swap window.
    """
    import taskq.worker.shutdown as shutdown_mod

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings()
    pool = MagicMock()
    leader_conn = MagicMock(spec=asyncpg.Connection)

    shut_event = asyncio.Event()
    event_set_at_close: list[bool] = []

    async def _recording_close() -> None:
        event_set_at_close.append(shut_event.is_set())

    leader_conn.close = AsyncMock(side_effect=_recording_close)

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,
        owns_leader_conn=True,  # Why: exercises the TaskQ-owned bounded-close path.
    )
    deps.active_jobs = registry  # type: ignore[assignment]

    backend = AsyncMock(spec=Backend)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))

    # Why the outer timeout: matches the hung-close test convention so a
    # RED state fails fast instead of hanging.
    async with asyncio.timeout(5):
        await orchestrate_shutdown(
            deps,
            deps.settings,
            new_uuid(),
            shut_event,
            None,
            backend=backend,
        )

    leader_conn.close.assert_called_once()
    assert event_set_at_close == [True]


# ── Round-3 race: orchestrator close vs deps exit-stack guard ─────────────


class _InstantPool:
    """Fake asyncpg.Pool whose close() completes immediately."""

    def __init__(self) -> None:
        self.close_calls = 0
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1

    def terminate(self) -> None:
        self.terminated = True


class _ListenConn:
    """Fake notify-role conn: accepts LISTEN, close() completes immediately."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.close_calls = 0

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def close(self) -> None:
        self.close_calls += 1

    def terminate(self) -> None:
        pass


class _GatedLeaderConn:
    """Fake leader conn whose close() blocks on a gate.

    Counts close() entries and the maximum number of concurrently in-flight
    closes — the double-close oracle. terminate() opens the gate, aborting
    any in-flight close the way a real conn abort unblocks _protocol.close().
    """

    def __init__(self) -> None:
        self.close_entries = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.close_entered = asyncio.Event()
        self.close_gate = asyncio.Event()  # never set by the test: close() hangs
        self.terminate_calls = 0

    async def close(self) -> None:
        self.close_entries += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            self.close_entered.set()
            await self.close_gate.wait()
        finally:
            self.in_flight -= 1

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.close_gate.set()


def _pool_factory(pool: _InstantPool) -> PoolFactory:
    async def factory() -> asyncpg.Pool:
        return pool  # type: ignore[return-value]  # Why: hand-rolled fake; asyncpg.Pool is a C-extension type tests cannot subclass.

    return factory


def _conn_factory(conn: _ListenConn | _GatedLeaderConn) -> ConnFactory:
    async def factory() -> asyncpg.Connection:
        return conn  # type: ignore[return-value]  # Why: hand-rolled fake; asyncpg.Connection is a C-extension type tests cannot subclass.

    return factory


async def test_orchestrate_shutdown_does_not_double_close_leader_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Race regression: the deps exit-stack guard must not double-close the
    leader conn while the orchestrator's bounded close is parked.

    _bootstrap wiring: ``await shutdown_event.wait()`` runs INSIDE
    ``async with open_worker_deps(...)``; the orchestrator task is awaited
    only AFTER the context exits. The early ``shutdown_event.set()`` (round-2
    fix — stops the election loop before the close park) therefore releases
    the exit-stack unwind CONCURRENTLY with the parked close, and the
    guard's ``_close_leader_conn`` enters a second ``close_conn_bounded`` on
    the same conn unless ``deps.leader_conn`` is nulled BEFORE the park. On
    a real asyncpg conn one closer's terminate aborts the other's in-flight
    close, logging a spurious conn-teardown-close-error.
    """
    import taskq.worker.shutdown as shutdown_mod
    from taskq.worker import deps as deps_mod

    # Shrink both bounded-close seams so the hung close resolves fast: the
    # orchestrator reads shutdown_mod.CLOSE_TIMEOUT_SECS; the exit-stack
    # guard reads deps_mod.CLOSE_TIMEOUT_SECS.
    monkeypatch.setattr(shutdown_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr(deps_mod, "CLOSE_TIMEOUT_SECS", 0.05)

    settings = _worker_settings()
    dispatcher, heartbeat, worker = _InstantPool(), _InstantPool(), _InstantPool()
    notify = _ListenConn()
    leader = _GatedLeaderConn()

    conns = WorkerConnections(
        dispatcher_pool_factory=_pool_factory(dispatcher),
        heartbeat_pool_factory=_pool_factory(heartbeat),
        worker_pool_factory=_pool_factory(worker),
        notify_conn_factory=_conn_factory(notify),
        leader_conn_factory=_conn_factory(leader),
    )

    backend = AsyncMock(spec=Backend)
    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", AsyncMock(return_value=0))

    shut_event = asyncio.Event()

    # Why the outer timeout: on the race the second close parks on the same
    # gate — the fail-fast bound keeps a broken state from hanging the suite;
    # the shrunk CLOSE_TIMEOUT_SECS resolves the parked close(s) in ~0.05s.
    async with asyncio.timeout(5):
        async with open_worker_deps(settings, connections=conns) as deps:
            # Mirror _bootstrap: the orchestrator is a bare create_task that
            # is NOT joined before the open_worker_deps context exits.
            orch_task = asyncio.ensure_future(
                orchestrate_shutdown(
                    deps,
                    deps.settings,
                    new_uuid(),
                    shut_event,
                    None,
                    backend=backend,
                )
            )
            # Wait until the orchestrator is parked in its bounded close —
            # shutdown_event is already set by then, and leaving the
            # with-body below releases the exit-stack unwind.
            await leader.close_entered.wait()
        result = await orch_task  # joined after the context exits, like _main

    assert result == 0
    assert leader.close_entries == 1, (
        "deps exit-stack guard entered a second close on the same conn while "
        f"the orchestrator's close was parked: entries={leader.close_entries}, "
        f"max_in_flight={leader.max_in_flight}"
    )
    assert leader.max_in_flight == 1
    assert deps.leader_conn is None


# ── Hypothesis grace-budget invariant ────────────────────────────


_valid_grace_settings = st.tuples(
    st.floats(min_value=0.1, max_value=29.9),
    st.floats(min_value=0.1, max_value=29.9),
    st.floats(min_value=10.0, max_value=120.0),
    st.floats(min_value=10.0, max_value=60.0),
    st.floats(min_value=0.5, max_value=14.9),
).filter(lambda t: t[0] + t[1] < t[2] - 5.0 and t[0] + t[1] < t[3] and t[3] >= 4 * t[4])


@given(_valid_grace_settings)
@hyp_settings(max_examples=50)
def test_grace_budget_accepted(
    args: tuple[float, float, float, float, float],
) -> None:
    """Valid grace budget tuples are accepted by load_from_dict."""
    cancel_g, cleanup_g, term_g, lock_l, hb_int = args
    # Lag budget derived as lease/2 keeps the lag-lease invariant satisfied
    # whenever the 4x lease invariant holds (hb <= lease/4 < lease/2), so
    # the grace boundaries stay the only ones under test.
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": "taskq",
            "TASKQ_CANCELLATION_GRACE_PERIOD": str(cancel_g),
            "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_g),
            "TASKQ_TERMINATION_GRACE_PERIOD": str(term_g),
            "TASKQ_LOCK_LEASE": str(lock_l),
            "TASKQ_HEARTBEAT_INTERVAL": str(hb_int),
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": str(lock_l / 2),
        }
    )
    assert settings.cancellation_grace_period == cancel_g
    assert settings.cleanup_grace_period == cleanup_g


_invalid_grace_settings = st.tuples(
    st.floats(min_value=0.1, max_value=30.0),
    st.floats(min_value=0.1, max_value=30.0),
    st.floats(min_value=5.0, max_value=120.0),
    st.floats(min_value=1.0, max_value=60.0),
    st.floats(min_value=0.5, max_value=15.0),
).filter(lambda t: t[0] + t[1] >= t[2] - 5.0 or t[0] + t[1] >= t[3] or t[3] < 4 * t[4])


@given(_invalid_grace_settings)
@hyp_settings(max_examples=50)
def test_grace_budget_rejected(
    args: tuple[float, float, float, float, float],
) -> None:
    """Invalid grace budget tuples are rejected by load_from_dict."""
    from dotenvmodel import DotEnvModelError

    cancel_g, cleanup_g, term_g, lock_l, hb_int = args
    with pytest.raises(DotEnvModelError):
        WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_SCHEMA_NAME": "taskq",
                "TASKQ_CANCELLATION_GRACE_PERIOD": str(cancel_g),
                "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_g),
                "TASKQ_TERMINATION_GRACE_PERIOD": str(term_g),
                "TASKQ_LOCK_LEASE": str(lock_l),
                "TASKQ_HEARTBEAT_INTERVAL": str(hb_int),
            }
        )


# ── Hypothesis adversarial-actor invariant ───────────────────────


_job_behaviour = st.sampled_from(["cooperative", "ignorer", "catcher"])


@st.composite
def _adversarial_job_setups(draw: st.DrawFn) -> list[tuple[UUID, str]]:
    n = draw(st.integers(min_value=1, max_value=10))
    return [(new_uuid(), draw(_job_behaviour)) for _ in range(n)]


@given(_adversarial_job_setups())
@hyp_settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_adversarial_actor_invariant(
    setups: list[tuple[UUID, str]],
) -> None:
    """Every job either deregistered cooperatively or marked abandoned."""
    import taskq.worker.shutdown as shutdown_mod

    active_jobs: list[_ActiveJob] = []
    for job_id, _behaviour in setups:
        active_jobs.append(_make_fake_active_job(job_id=job_id))

    registry = FakeActiveJobRegistry(active_jobs)
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    deregs: set[UUID] = set()

    call_count = 0

    async def _tracking_sleep(*args: object) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            for sid, sbehaviour in setups:
                if sbehaviour == "cooperative":
                    deregs.add(sid)
                    registry.deregister(JobId(sid))

    with (
        patch("taskq.worker.shutdown.drain_local_queue_to_pending", AsyncMock(return_value=0)),
        patch.object(  # type: ignore[unused-ignore] # Why: suppress F841 for unused-variable from with-statement; variable is referenced by the context manager.
            shutdown_mod.asyncio,
            "sleep",
            _tracking_sleep,
        ),
    ):
        shut_event = asyncio.Event()
        worker_id = new_uuid()

        result = await orchestrate_shutdown(
            deps,
            deps.settings,
            worker_id,
            shut_event,
            None,
            backend=backend,
        )

    assert result == 0

    abandoned_ids: set[UUID] = set()
    for call_args in backend.mark_abandoned.mock_calls:  # type: ignore[union-attr] # Why: AsyncMock(spec=Backend) mock_calls iterates call objects whose args attribute is not visible to the protocol type checker.
        abandoned_ids.add(call_args.args[0])

    total_ids = {job_id for job_id, _ in setups}
    covered = deregs | abandoned_ids
    assert covered == total_ids

    for job_id, behaviour in setups:
        if behaviour == "cooperative":
            assert job_id in deregs

"""Unit tests for orchestrate_shutdown four-phase orchestrator."""

import asyncio
import contextlib
from datetime import timedelta
from typing import cast
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
from taskq.context import CancelOrigin, JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.in_memory import PassthroughPayload
from taskq.worker._watchdog import (  # pyright: ignore[reportPrivateUsage]  # Why: the flush bound is half of the exit tail pinned above.
    _METRICS_FLUSH_TIMEOUT_SECS,
)
from taskq.worker.cancel import _ActiveJob
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.shutdown import (  # pyright: ignore[reportPrivateUsage]  # Why: the hold math under test is the module's own; the pinned constants are its deadline model.
    _release_hold,
    _watchdog_exit_tail,
    orchestrate_shutdown,
)

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
    """Phases transition in order; the release — never abandonment — ends each job.

    Two jobs still registered past both graces belong to actors that never
    unwound. The orchestration must: stamp the shutdown origin at
    CANCELLING, force-cancel at FORCING (the escalation probe declines —
    these rows carry no operator cancel), and RELEASE each at RELEASING via
    ``mark_interrupted`` with the remaining termination budget as the hold.
    ``mark_abandoned`` is the operator ladder's terminal and has no work
    here.
    """
    import taskq.worker.shutdown as shutdown_mod

    job_a = _make_fake_active_job(job_id=UUID("11111111-1111-1111-1111-111111111111"))
    job_b = _make_fake_active_job(job_id=UUID("22222222-2222-2222-2222-222222222222"))
    registry = FakeActiveJobRegistry([job_a, job_b])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    # The probe shape: these rows carry no operator cancel (cancel_phase
    # = 0), so the escalation write declines them.
    backend.write_cancel_escalation = AsyncMock(return_value=False)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")
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
        assert job.cancel_origin is CancelOrigin.SHUTDOWN

    # The probe reads each row once at FORCING; neither matched (no
    # operator cancel in flight), so nothing is escalated to phase 2.
    assert backend.write_cancel_escalation.call_count == 2
    # Both jobs are released back to the fleet at RELEASING — the shutdown
    # never terminalises them.
    assert backend.mark_interrupted.call_count == 2
    # The exit tail the hold must cover past the deadline itself: the
    # watchdog checks the deadline once per dump interval and then dumps
    # stacks + flushes metrics (bounded) before os._exit, so a hold ending
    # at the bare deadline leaves the row claimable while the dying process
    # can still touch it (#232).
    expected_tail = _watchdog_exit_tail(settings)
    assert expected_tail == pytest.approx(
        settings.watchdog_dump_interval + _METRICS_FLUSH_TIMEOUT_SECS + 1.0
    )
    for call in backend.mark_interrupted.call_args_list:
        assert call.kwargs["attempt"] == 1
        # The hold is the remaining termination budget PLUS that exit
        # tail: 60s grace counted from DRAINING, minus the ~0.8-0.9s the
        # two grace windows consume on the fake clock (the 0.1s sleep
        # quantum plus float drift can overshoot a grace boundary by one
        # step).
        hold = call.kwargs["hold"]
        assert hold.total_seconds() == pytest.approx(60.0 - 0.8 + expected_tail, abs=0.15)
    assert backend.mark_abandoned.call_count == 0

    assert 0.7 < clock.time_val < 1.0


async def test_consumers_are_stopped_before_claimed_work_is_handed_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dispatch stops before the hand-back sweeps, never after.

    The hand-back is a single statement over the rows this worker currently
    holds. If the producer were still claiming while it ran, a row claimed a
    moment later would be missed by the sweep and left running and locked to a
    worker that is exiting — stranded until its lock lease expires, minutes of
    dead latency on a job nobody is running.

    Stopping dispatch first closes that window: after the stop event, no new
    row can join the set the sweep is about to release, so every claimed row
    is handed back, and handed back once.

    The phase marker must also be visible before either step, since health
    endpoints and the consumer loops read it to decide they are draining.
    """
    import taskq.worker.shutdown as shutdown_mod
    from taskq.worker.shutdown import ShutdownPhase

    registry = FakeActiveJobRegistry([])
    settings = _worker_settings(cancellation_grace=0.0, cleanup_grace=0.0)
    deps = _make_deps(registry=registry, settings=settings)

    order: list[str] = []
    observed_phase: list[ShutdownPhase] = []
    producer_stopped_at_drain: list[bool] = []

    async def _recording_drain(d: WorkerDeps, w: UUID) -> int:
        order.append("hand_back")
        observed_phase.append(d.shutdown_phase)
        producer_stopped_at_drain.append(d.producer_stop_event.is_set())
        return 0

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", _recording_drain)

    original_set = deps.producer_stop_event.set

    def _recording_set() -> None:
        order.append("stop_dispatch")
        original_set()

    deps.producer_stop_event.set = _recording_set  # type: ignore[method-assign] # Why: recording wrapper to observe the ordering of the two DRAINING steps.

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=True)
    backend.mark_abandoned = AsyncMock(return_value=True)

    clock = FakeClock()
    fake_loop = Mock()
    _patch_clock(monkeypatch, clock, fake_loop)

    await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        asyncio.Event(),
        None,
        backend=backend,
    )

    assert order == ["stop_dispatch", "hand_back"], (
        "consumers must stop taking new work before claimed work is handed "
        "back; a claim landing between the two steps is missed by the sweep "
        f"and stranded locked until lease expiry. Observed order: {order!r}"
    )
    assert producer_stopped_at_drain == [True]
    assert observed_phase == [ShutdownPhase.DRAINING], (
        "the draining phase marker must already be readable when the "
        "hand-back runs — health endpoints and consumer loops read it to "
        f"decide they are draining. Observed: {observed_phase!r}"
    )


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
    """Single PG write failure isolates that job; others proceed.

    The job whose escalation write failed is still force-cancelled locally
    (#233): the row-side probe failing must not keep the process-side
    ``task.cancel()`` from being delivered: a cancellable actor that never
    gets the cancel runs untouched into RELEASING and is released-with-hold
    while still alive. Its registry phase advances to FORCED like every
    other job's; only the row-side escalation is missing, and the ladder's
    row-side arms recover it (or the lease sweep does).
    """
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

    assert job1.cancel_phase == CancelPhase.FORCED, (
        "a failed escalation write must not keep the local force-cancel "
        "from advancing the entry — the process-side half of FORCING is "
        "delivered regardless of the row-side probe's outcome (#233)"
    )
    job1_task = cast(
        MagicMock, job1.task
    )  # Why: _make_fake_active_job registered a MagicMock task; the dataclass field is typed asyncio.Task, so the mock's call record needs the cast.
    assert job1_task.cancel.called, (
        "the entry whose escalation write failed must still have its task "
        "cancelled — skipping it let a cancellable actor run into RELEASING "
        "untouched (#233)"
    )
    assert job2.cancel_phase == CancelPhase.FORCED
    assert job3.cancel_phase == CancelPhase.FORCED
    cast(MagicMock, job2.task).cancel.assert_called()
    cast(MagicMock, job3.task).cancel.assert_called()


async def test_forcing_write_failure_still_cancels_a_real_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A REAL consumer task is cancelled when its escalation write fails.

    The isolation test above pins the registry fields with a MagicMock
    task; this one pins the actual delivery: a cancellable asyncio task
    registered in-flight must END cancelled when FORCING's row-side write
    raises, not keep running into RELEASING. That is the overlap #233
    names: an actor alive past both graces because the one cancel that
    could reach it was skipped after a PG blip.
    """
    import taskq.worker.shutdown as shutdown_mod

    actor_running = asyncio.Event()
    actor_cancelled = asyncio.Event()

    async def _cancellable_actor() -> None:
        actor_running.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            actor_cancelled.set()
            raise

    actor_task = asyncio.ensure_future(_cancellable_actor())
    await actor_running.wait()

    job = _make_fake_active_job()
    # The real task replaces the MagicMock so task.cancel() lands on
    # something that can actually be cancelled.
    job.task = actor_task  # type: ignore[assignment]  # Why: _ActiveJob is a plain (non-frozen) dataclass; the test swaps the mock for a real cancellable task.
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(cancellation_grace=0.1, cleanup_grace=0.1)
    deps = _make_deps(registry=registry, settings=settings)

    async def _failing_escalation(*args: object, **kwargs: object) -> bool:
        raise asyncpg.PostgresConnectionError("escalation write failed")

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(side_effect=_failing_escalation)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")
    backend.mark_abandoned = AsyncMock(return_value=True)

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

    assert actor_cancelled.is_set(), (
        "the forced cancel must be delivered to a cancellable actor even "
        "when the escalation PG write fails — the actor running into "
        "RELEASING untouched is the double-execution overlap the shutdown "
        "contract forbids (#233)"
    )
    with contextlib.suppress(asyncio.CancelledError):
        await actor_task


# ── The releasing hold covers the watchdog's exit tail ────────────


async def test_release_hold_pads_the_remaining_budget_by_the_exit_tail() -> None:
    """The RELEASING hold ends past the deadline, not at it (#232).

    The watchdog checks the deadline once per ``watchdog_dump_interval``
    sleep and then dumps stacks and joins the bounded metrics flush before
    ``os._exit``: the process can be alive up to that tail past the
    deadline. A hold ending at the bare deadline leaves exactly that window
    in which the released row is claimable while the dying process could
    still touch it.
    """
    settings = _worker_settings(termination_grace=60.0)
    deps = _make_deps(settings=settings)
    loop = asyncio.get_running_loop()
    anchored = loop.time()
    deps.shutdown_started_at = anchored - 10.0

    hold = _release_hold(deps, settings, loop)

    tail = _watchdog_exit_tail(settings)
    assert tail > 0
    # 60s budget, 10s spent: 50s remaining, plus the tail: never the bare
    # remaining share. (abs tolerance: the remaining share decays with the
    # real loop clock between the anchoring and the computation.)
    expected_remaining = 60.0 - 10.0 - (loop.time() - anchored)
    assert hold.total_seconds() == pytest.approx(expected_remaining + tail, abs=0.1)


async def test_release_hold_unanchored_covers_the_full_budget_plus_tail() -> None:
    """No shutdown start stamped (defensive shape): the full budget + tail.

    The consumer's release arm can reach the hold computation on a bare
    call with no deps; the hold must stay the full watchdog budget plus the
    exit tail, never an assumption that the actor is gone.
    """
    settings = _worker_settings(termination_grace=60.0)
    loop = asyncio.get_running_loop()

    anchored_nowhere = _release_hold(None, settings, loop)
    assert anchored_nowhere.total_seconds() == pytest.approx(
        60.0 + _watchdog_exit_tail(settings), abs=1e-6
    )

    deps = _make_deps(settings=settings)
    assert deps.shutdown_started_at is None
    assert _release_hold(deps, settings, loop) == anchored_nowhere


async def test_release_hold_without_the_watchdog_is_the_lock_lease_unchanged() -> None:
    """Watchdog disabled: no guaranteed exit exists to pad towards, so the
    hold stays the lock lease: the bound the lease-expiry path already
    imposes (the pre-existing fallback, unpadded)."""
    settings = _worker_settings(termination_grace=60.0)
    settings.watchdog_enabled = False
    deps = _make_deps(settings=settings)
    loop = asyncio.get_running_loop()
    deps.shutdown_started_at = loop.time() - 10.0

    assert _release_hold(deps, settings, loop) == timedelta(seconds=settings.lock_lease)


async def test_releasing_failure_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """RELEASING variant. One failed release write isolates that job.

    A PG error releasing job1 must not stop job2 and job3 from being
    handed back to the fleet — the phase releases what it can and lets the
    lease-expiry sweep backstop the one whose write failed.
    """
    import taskq.worker.shutdown as shutdown_mod

    job1 = _make_fake_active_job(job_id=UUID("11111111-1111-1111-1111-111111111111"))
    job2 = _make_fake_active_job(job_id=UUID("22222222-2222-2222-2222-222222222222"))
    job3 = _make_fake_active_job(job_id=UUID("33333333-3333-3333-3333-333333333333"))
    registry = FakeActiveJobRegistry([job1, job2, job3])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=False)
    release_call_count = 0

    async def _failing_release(*args: object, **kwargs: object) -> str:
        nonlocal release_call_count
        release_call_count += 1
        if release_call_count == 1:
            raise asyncpg.PostgresConnectionError("release failed for job1")
        return "scheduled"

    backend.mark_interrupted = AsyncMock(side_effect=_failing_release)
    backend.mark_abandoned = AsyncMock(return_value=True)

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

    assert backend.mark_interrupted.call_count == 3
    # No operator cancel is in flight anywhere, so the operator ladder's
    # terminal write never runs.
    assert backend.mark_abandoned.call_count == 0


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


async def test_race_winner_not_released(monkeypatch: pytest.MonkeyPatch) -> None:
    """A job that deregisters mid-CANCELLING is not released either.

    The job's own consumer terminalised it (that is what deregistering
    means), so by RELEASING there is no entry — the phase must not write
    against a row it can no longer see. Neither the release nor the
    operator ladder's terminal write may fire.
    """
    import taskq.worker.shutdown as shutdown_mod

    job = _make_fake_active_job()
    registry = FakeActiveJobRegistry([job])
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    backend.write_cancel_escalation = AsyncMock(return_value=False)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")
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

    # asyncio.sleep is resolved on the asyncio module at call time; patching
    # it here is exactly what the orchestrator sees (and what the pre-rename
    # suite did through the shutdown module's re-import).
    monkeypatch.setattr(asyncio, "sleep", _sleep_with_deregister)

    await orchestrate_shutdown(
        deps,
        deps.settings,
        worker_id,
        shut_event,
        None,
        backend=backend,
    )

    backend.mark_interrupted.assert_not_called()
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
    assert elapsed < 11.0  # CANCELLING breaks early (~0.1s) + cleanup_grace (10s) + RELEASING


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
    st.floats(min_value=10.0, max_value=200.0),
    st.floats(min_value=0.5, max_value=14.9),
).filter(
    lambda t: (
        t[0] + t[1] < t[2] - 5.0
        and t[0] + t[1] < t[3]
        # The #284 cascade floor: (max_heartbeat_failures + 1) *
        # (heartbeat_interval + 2 * heartbeat_command_timeout) at the defaults
        # (F=3, c=2) is 4 * hb + 16.
        and t[3] >= 4 * t[4] + 16
    )
)


@given(_valid_grace_settings)
@hyp_settings(max_examples=50)
def test_grace_budget_accepted(
    args: tuple[float, float, float, float, float],
) -> None:
    """Valid grace budget tuples are accepted by load_from_dict."""
    cancel_g, cleanup_g, term_g, lock_l, hb_int = args
    # Lag budget derived as lease/2 keeps the lag-lease invariant satisfied
    # whenever the cascade lease invariant holds (hb <= (lease - 16)/4 <
    # lease/2), so the grace boundaries stay the only ones under test.
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
            # Half the derived budget so the warn-vs-budget invariant
            # stays quiet wherever the draw lands.
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": str(lock_l / 4),
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
).filter(
    lambda t: (
        t[0] + t[1] >= t[2] - 5.0
        or t[0] + t[1] >= t[3]
        # The #284 cascade floor at the defaults (F=3, c=2): 4 * hb + 16 —
        # its violation alone is enough for the rejection under test.
        or t[3] < 4 * t[4] + 16
    )
)


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
    """Every job is either deregistered by its own consumer or released.

    The invariant a deploy owes the fleet: whatever the actor did —
    unwound cooperatively, ignored the cancel, or swallowed it — the
    shutdown accounts for the row exactly once. Jobs still registered at
    RELEASING are interrupted (released back to the fleet with the attempt
    refunded); jobs that deregistered mid-flight are their consumers' own
    outcomes. Nothing is abandoned: abandonment belongs to the operator
    ladder, and no operator cancel is in flight anywhere here.
    """
    active_jobs: list[_ActiveJob] = []
    for job_id, _behaviour in setups:
        active_jobs.append(_make_fake_active_job(job_id=job_id))

    registry = FakeActiveJobRegistry(active_jobs)
    settings = _worker_settings(cancellation_grace=0.5, cleanup_grace=0.3)
    deps = _make_deps(registry=registry, settings=settings)

    backend = AsyncMock(spec=Backend)
    # No row carries an operator cancel, so the escalation probe declines
    # every entry.
    backend.write_cancel_escalation = AsyncMock(return_value=False)
    backend.mark_interrupted = AsyncMock(return_value="scheduled")
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
            asyncio,  # the orchestrator resolves asyncio.sleep on the asyncio module at call time
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

    released_ids: set[UUID] = set()
    for call_args in backend.mark_interrupted.mock_calls:  # type: ignore[union-attr] # Why: AsyncMock(spec=Backend) mock_calls iterates call objects whose args attribute is not visible to the protocol type checker.
        released_ids.add(call_args.args[0])

    total_ids = {job_id for job_id, _ in setups}
    covered = deregs | released_ids
    assert covered == total_ids, (
        "every in-flight job must be accounted for exactly once at shutdown: "
        f"deregistered by its consumer {sorted(deregs)} or released "
        f"{sorted(released_ids)} — uncovered: {sorted(total_ids - covered)}"
    )

    for job_id, behaviour in setups:
        if behaviour == "cooperative":
            assert job_id in deregs

    backend.mark_abandoned.assert_not_called()

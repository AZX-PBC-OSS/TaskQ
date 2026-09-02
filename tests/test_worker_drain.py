"""Unit tests for drain monitor and until-idle mode."""

import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend._protocol import Backend, CancelPhase, JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._consumer import AttemptOutcome
from taskq.worker.deps import WorkerDeps
from taskq.worker.drain import drain_monitor_loop
from taskq.worker.shutdown import ShutdownPhase
from tests.conftest import _FakePool, unique_health_sock_path

# ── Helpers ────────────────────────────────────────────────────────


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_drain"),
        }
    )


def _make_scopes(
    registry: ProviderRegistry,
) -> tuple[ProcessScope, ThreadScope, LoopScope]:
    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    return process_scope, thread_scope, loop_scope


async def _bootstrap_scopes(
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
) -> None:
    settings = _settings()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


def _backend_stub() -> Backend:
    class _Methods:
        async def mark_succeeded(
            self,
            job_id: object,
            worker_id: object,
            result: object,
            fallback_result_ttl: object = None,
        ) -> bool:
            return True

        async def mark_succeeded_with_conn(
            self,
            conn: object,
            job_id: object,
            worker_id: object,
            result: object,
            fallback_result_ttl: object = None,
        ) -> bool:
            return True

        async def mark_cancelled(self, job_id: object, worker_id: object) -> bool:
            return True

        async def write_cancel_escalation(
            self, job_id: object, worker_id: object, phase: object
        ) -> bool:
            return True

        async def mark_abandoned(
            self, job_id: object, progress_seq: object = 0, progress_state: object = None
        ) -> bool:
            return True

    raw = create_autospec(_Methods, instance=True)
    return raw  # type: ignore[return-value]


def _stub_deps(settings: WorkerSettings) -> WorkerDeps:
    pool = _FakePool()
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _make_job_row(actor_name: str) -> JobRow:
    return JobRow(
        id=new_job_id(),
        actor=actor_name,
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )


async def _run_one_job_with_fake_dispatch(
    fake_dispatch: Any,
    actor_name: str,
) -> WorkerDeps:
    """Run di_consumer_loop for one job with a patched dispatch_one_job.

    Returns the WorkerDeps so the caller can inspect drain_failures.
    """
    from taskq.worker.run import di_consumer_loop

    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    @actor(name=actor_name)
    async def _test_actor(payload: BaseModel, ctx: JobContext[BaseModel]) -> None: ...

    job = _make_job_row(actor_name)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)

    deps = _stub_deps(settings)
    backend = _backend_stub()
    shutdown_event = asyncio.Event()
    dispatch_event = asyncio.Event()

    original_fake = fake_dispatch

    async def _wrapping_dispatch(*args: object, **kwargs: object) -> object:
        try:
            result = await original_fake(*args, **kwargs)
        finally:
            dispatch_event.set()
            shutdown_event.set()
        return result

    with patch("taskq.worker.run.dispatch_one_job", side_effect=_wrapping_dispatch):
        loop_task = asyncio.create_task(
            di_consumer_loop(
                deps,
                local_queue,
                shutdown_event,
                backend=backend,
                worker_id=new_uuid(),
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_registry={actor_name: _test_actor},
                enqueuer=SubJobEnqueuer(
                    loop_scope_resolved=None,
                    worker_pool=None,
                    backend=backend,
                ),
            )
        )
        await asyncio.wait_for(dispatch_event.wait(), timeout=2.0)
        await asyncio.wait_for(loop_task, timeout=2.0)

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()

    return deps


# ── WorkerDeps.drain_failures default ──────────────────────────────


def test_worker_deps_drain_failures_default_zero() -> None:
    """WorkerDeps initializes drain_failures to 0."""
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": "/tmp/test_drain_deps.sock",  # noqa: S108
        }
    )
    pool = _FakePool()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    assert deps.drain_failures == 0


# ── dispatch_one_job return type ───────────────────────────────────


async def test_dispatch_one_job_returns_attempt_outcome() -> None:
    """dispatch_one_job returns AttemptOutcome, not None."""
    from taskq.worker.dispatch import dispatch_one_job

    sig = inspect.signature(dispatch_one_job)
    ret = sig.return_annotation
    # -> None stores None (the singleton) as the annotation; a real type
    # annotation stores the type object.  We want the latter.
    assert ret is not None, "return annotation is None (-> None), expected AttemptOutcome"
    if isinstance(ret, str):
        assert "AttemptOutcome" in ret
    else:
        # Should be the AttemptOutcome type alias, not None
        assert ret is not type(None)  # pyright: ignore[reportUnnecessaryComparison]  # Why: belt-and-suspenders — ret is not None already guarantees this, but the explicit check documents intent.


# ── di_consumer_loop increments drain_failures ─────────────────────


async def test_di_consumer_loop_increments_drain_failures_on_failure() -> None:
    """di_consumer_loop captures dispatch_one_job outcome and increments
    deps.drain_failures when a job returns 'failed'."""

    async def _fake_dispatch(*args: object, **kwargs: object) -> AttemptOutcome:
        return "failed"

    deps = await _run_one_job_with_fake_dispatch(
        _fake_dispatch, actor_name="test_drain_fail_failed"
    )
    assert deps.drain_failures == 1


async def test_di_consumer_loop_no_increment_on_success() -> None:
    """di_consumer_loop does NOT increment drain_failures on 'succeeded'."""

    async def _fake_dispatch(*args: object, **kwargs: object) -> AttemptOutcome:
        return "succeeded"

    deps = await _run_one_job_with_fake_dispatch(
        _fake_dispatch, actor_name="test_drain_fail_success"
    )
    assert deps.drain_failures == 0


async def test_di_consumer_loop_no_increment_on_scheduled() -> None:
    """di_consumer_loop does NOT increment drain_failures on 'scheduled'
    (snooze/retry — a retried job is not a drain failure)."""

    async def _fake_dispatch(*args: object, **kwargs: object) -> AttemptOutcome:
        return "scheduled"

    deps = await _run_one_job_with_fake_dispatch(
        _fake_dispatch, actor_name="test_drain_fail_scheduled"
    )
    assert deps.drain_failures == 0


async def test_di_consumer_loop_increments_on_exception() -> None:
    """di_consumer_loop increments drain_failures when dispatch_one_job raises."""

    async def _fake_dispatch(*args: object, **kwargs: object) -> AttemptOutcome:
        raise RuntimeError("boom")

    deps = await _run_one_job_with_fake_dispatch(_fake_dispatch, actor_name="test_drain_fail_exc")
    assert deps.drain_failures == 1


async def test_di_consumer_loop_no_increment_on_cancelled_as_value() -> None:
    """If dispatch_one_job somehow returns 'cancelled' as a value (not
    raising CancelledError), drain_failures is NOT incremented.

    In the real code path, CancelledError propagates as BaseException
    and never reaches the outcome check. But this test documents the
    contract: 'cancelled' is NOT in the failure set.
    """

    async def _fake_dispatch(*args: object, **kwargs: object) -> AttemptOutcome:
        return "cancelled"

    deps = await _run_one_job_with_fake_dispatch(
        _fake_dispatch, actor_name="test_drain_fail_cancelled"
    )
    assert deps.drain_failures == 0


# ── Drain monitor loop ─────────────────────────────────────────────


def _make_mock_deps(*, active_jobs_count: int = 0, drain_failures: int = 0) -> MagicMock:
    """Build a minimal mock WorkerDeps for drain monitor tests.

    shutdown_phase MUST be the real ShutdownPhase.NONE enum member —
    _trigger_drain_shutdown's double-orchestration guard is
    ``deps.shutdown_phase is not ShutdownPhase.NONE``, so a stand-in
    (string, MagicMock attribute) would trip the guard in EVERY test.
    """
    deps = MagicMock()
    deps.active_jobs.count.return_value = active_jobs_count
    deps.drain_failures = drain_failures
    deps.shutdown_phase = ShutdownPhase.NONE
    deps.settings = MagicMock()
    deps.settings.queues = ["default"]
    return deps


@contextlib.asynccontextmanager
async def _mock_orchestrate(
    exit_code: int = 0,  # Why: exit_code is unused — _drain_orchestrate supplies the exit code; the mock only simulates orchestrate_shutdown's finally-block shutdown_event.set().
) -> AsyncGenerator[None, None]:
    """Patch orchestrate_shutdown to set shutdown_event and return 0.

    The drain monitor's wrapper task calls orchestrate_shutdown and then
    returns the drain exit code. The mock simulates the finally-block
    shutdown_event.set() so the monitor's loop exits.
    """

    async def _mock(
        deps: object,
        settings: object,
        worker_id: object,
        shutdown_event: asyncio.Event,
        escalate_event: object,
        *,
        backend: object,
    ) -> int:
        try:
            await asyncio.sleep(0)  # yield once
        finally:
            shutdown_event.set()
        return 0

    with patch("taskq.worker.drain.orchestrate_shutdown", side_effect=_mock):
        yield


async def test_drain_monitor_triggers_shutdown_when_idle() -> None:
    """Drain monitor creates orchestrator task when queues are empty
    and no active jobs after settle window."""
    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )
        assert len(orchestrator_holder) == 1
        exit_code = await orchestrator_holder[0]

    assert exit_code == 0
    assert shutdown_event.is_set()


async def test_drain_monitor_exit_code_3_on_failures() -> None:
    """Exit code 3 when drain_failures > 0."""
    deps = _make_mock_deps(active_jobs_count=0, drain_failures=3)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )
        assert len(orchestrator_holder) == 1
        exit_code = await orchestrator_holder[0]

    assert exit_code == 3


async def test_drain_monitor_exit_code_4_on_timeout() -> None:
    """Exit code 4 when max_runtime is exceeded."""
    deps = _make_mock_deps(active_jobs_count=1)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=5)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=10.0,
            idle_poll_interval=0.05,
            max_runtime=0.2,
        )
        assert len(orchestrator_holder) == 1
        exit_code = await orchestrator_holder[0]

    assert exit_code == 4
    assert shutdown_event.is_set()


async def test_drain_monitor_resets_settle_on_new_jobs() -> None:
    """If a new job appears after idle was detected, the settle timer resets."""
    call_count = 0

    async def mock_count(queues: list[str]) -> int:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return 0  # idle
        return 1  # job appeared

    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = mock_count

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        task = asyncio.create_task(
            drain_monitor_loop(
                deps,
                deps.settings,
                new_uuid(),
                shutdown_event,
                escalate_event,
                orchestrator_holder,
                backend,
                idle_settle_window=0.5,
                idle_poll_interval=0.05,
                max_runtime=None,
            )
        )
        await asyncio.sleep(0.3)
        assert len(orchestrator_holder) == 0
        assert not shutdown_event.is_set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_drain_monitor_does_not_trigger_when_active_jobs() -> None:
    """Active jobs on this worker prevent drain even if queue is empty."""
    deps = _make_mock_deps(active_jobs_count=2)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        task = asyncio.create_task(
            drain_monitor_loop(
                deps,
                deps.settings,
                new_uuid(),
                shutdown_event,
                escalate_event,
                orchestrator_holder,
                backend,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=None,
            )
        )
        await asyncio.sleep(0.3)
        assert len(orchestrator_holder) == 0
        assert not shutdown_event.is_set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_drain_monitor_skips_when_orchestration_already_active() -> None:
    """Drain monitor does NOT trigger a second orchestrate_shutdown when
    one is already in progress (H2: double-orchestration guard)."""
    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(return_value=0)

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    fake_task = asyncio.create_task(asyncio.sleep(100))
    orchestrator_holder: list[asyncio.Task[int]] = [
        fake_task  # type: ignore[list-item]  # Why: fake_task is Task[None] (asyncio.sleep returns None), but orchestrator_holder is typed Task[int] to match the real API; the test only checks holder length, never the task result.
    ]
    deps.shutdown_phase = ShutdownPhase.CANCELLING

    async with _mock_orchestrate():
        task = asyncio.create_task(
            drain_monitor_loop(
                deps,
                deps.settings,
                new_uuid(),
                shutdown_event,
                escalate_event,
                orchestrator_holder,
                backend,
                idle_settle_window=0.1,
                idle_poll_interval=0.05,
                max_runtime=None,
            )
        )
        await asyncio.sleep(0.3)
        assert len(orchestrator_holder) == 1
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    fake_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await fake_task


async def test_drain_monitor_continues_after_count_error() -> None:
    """F4: Drain monitor continues polling after a transient count_active_jobs error."""
    call_count = 0

    async def mock_count(queues: list[str]) -> int:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise TimeoutError("PG timeout")
        return 0  # recovers

    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = mock_count

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )
        # After recovering from the error, the monitor should have triggered
        assert len(orchestrator_holder) == 1
        exit_code = await orchestrator_holder[0]

    assert exit_code == 0


async def test_drain_monitor_propagates_non_recoverable_error() -> None:
    """Non-recoverable exceptions from count_active_jobs propagate
    (tear down the TaskGroup), not silently swallowed."""
    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = AsyncMock(side_effect=TypeError("bad backend"))

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    with pytest.raises(TypeError):
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=0.1,
            idle_poll_interval=0.05,
            max_runtime=None,
        )


async def test_drain_monitor_settle_window_zero() -> None:
    """idle_settle_window=0.0 triggers after one poll interval (not instant).

    With settle=0, the first idle detection sets idle_since, then on the
    NEXT poll (after idle_poll_interval), idle_elapsed >= 0.0 is True.
    So two polls are needed, not one.
    """
    call_count = 0

    async def mock_count(queues: list[str]) -> int:
        nonlocal call_count
        call_count += 1
        return 0

    deps = _make_mock_deps(active_jobs_count=0)
    backend = MagicMock()
    backend.count_active_jobs = mock_count

    shutdown_event = asyncio.Event()
    escalate_event = asyncio.Event()
    orchestrator_holder: list[asyncio.Task[int]] = []

    async with _mock_orchestrate():
        await drain_monitor_loop(
            deps,
            deps.settings,
            new_uuid(),
            shutdown_event,
            escalate_event,
            orchestrator_holder,
            backend,
            idle_settle_window=0.0,
            idle_poll_interval=0.05,
            max_runtime=None,
        )
    assert len(orchestrator_holder) == 1
    assert call_count >= 2  # at least two polls needed


# ── Handler return-value contract tests ────────────────────────────
#
# These tests verify that handlers return the actual AttemptOutcome based
# on what happened in the DB, not a hardcoded value based on exception type.
# This is the core fix for the exit-code contract bug: a retryable failure
# must return "scheduled" (not "failed"), and a terminal snooze/retry-after
# failure must return "failed" (not "scheduled").


async def test_handle_generic_exception_returns_scheduled_on_retry() -> None:
    """_handle_generic_exception returns "scheduled" when the DB transitions
    to retry (status="scheduled"), not "failed"."""
    from dataclasses import replace
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_generic_exception

    class _RetryBackend:
        """Backend whose mark_failed_or_retry returns status='scheduled'."""

        async def mark_failed_or_retry(self, **kwargs: object) -> JobRow:
            return replace(
                make_job_row(),
                status="scheduled" if kwargs.get("retry_delay") is not None else "failed",
            )

    job = make_job_row(attempt=1, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_generic_exception(
        _RetryBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        RuntimeError("boom"),
        cfg,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )
    assert result == "scheduled"


async def test_handle_generic_exception_returns_failed_on_terminal() -> None:
    """_handle_generic_exception returns "failed" when the DB transitions
    to terminal failure (status="failed"), not "scheduled"."""
    from dataclasses import replace
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_generic_exception

    class _FailBackend:
        """Backend whose mark_failed_or_retry returns status='failed'."""

        async def mark_failed_or_retry(self, **kwargs: object) -> JobRow:
            return replace(make_job_row(), status="failed")

    job = make_job_row(attempt=3, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_generic_exception(
        _FailBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        RuntimeError("boom"),
        cfg,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )
    assert result == "failed"


async def test_handle_snooze_returns_failed_on_deadline_exceeded() -> None:
    """_handle_snooze returns "failed" when mark_snoozed returns "failed"
    (DeadlineExceeded), not "scheduled"."""
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.exceptions import Snooze
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_snooze

    class _SnoozeFailBackend:
        """Backend whose mark_snoozed returns 'failed'."""

        async def mark_snoozed(self, *args: object, **kwargs: object) -> str:
            return "failed"

    job = make_job_row(attempt=3, max_attempts=3, retry_kind="indefinite")
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_snooze(
        _SnoozeFailBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        Snooze(timedelta(seconds=1)),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
        cfg,
    )
    assert result == "failed"


async def test_handle_snooze_returns_scheduled_on_normal_snooze() -> None:
    """_handle_snooze returns "scheduled" when mark_snoozed returns "scheduled"."""
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.exceptions import Snooze
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_snooze

    class _SnoozeOkBackend:
        """Backend whose mark_snoozed returns 'scheduled'."""

        async def mark_snoozed(self, *args: object, **kwargs: object) -> str:
            return "scheduled"

    job = make_job_row(attempt=1, max_attempts=3, retry_kind="indefinite")
    cfg = StubActorConfig(retry=RetryPolicy(kind="indefinite", jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_snooze(
        _SnoozeOkBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        Snooze(timedelta(seconds=1)),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
        cfg,
    )
    assert result == "scheduled"


async def test_handle_retry_after_returns_failed_on_deadline_exceeded() -> None:
    """_handle_retry_after returns "failed" when mark_retry_after returns
    "failed:DeadlineExceeded", not "scheduled"."""
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.exceptions import RetryAfter
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_retry_after

    class _RetryAfterFailBackend:
        """Backend whose mark_retry_after returns 'failed:DeadlineExceeded'."""

        async def mark_retry_after(self, *args: object, **kwargs: object) -> str:
            return "failed:DeadlineExceeded"

    job = make_job_row(attempt=3, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_retry_after(
        _RetryAfterFailBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        RetryAfter(timedelta(seconds=1)),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
        cfg,
    )
    assert result == "failed"


async def test_handle_retry_after_returns_scheduled_on_normal_retry() -> None:
    """_handle_retry_after returns "scheduled" when mark_retry_after returns "scheduled"."""
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.exceptions import RetryAfter
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_retry_after

    class _RetryAfterOkBackend:
        """Backend whose mark_retry_after returns 'scheduled'."""

        async def mark_retry_after(self, *args: object, **kwargs: object) -> str:
            return "scheduled"

    job = make_job_row(attempt=1, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_retry_after(
        _RetryAfterOkBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        RetryAfter(timedelta(seconds=1)),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
        cfg,
    )
    assert result == "scheduled"


async def test_handle_timeout_returns_scheduled_on_retry() -> None:
    """_handle_timeout returns "scheduled" when the DB transitions to retry,
    not "failed" (the old hardcoded outcome)."""
    from dataclasses import replace
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_timeout

    class _RetryBackend:
        """Backend whose mark_failed_or_retry returns status='scheduled' for retry."""

        async def mark_failed_or_retry(self, **kwargs: object) -> JobRow:
            status = "scheduled" if kwargs.get("retry_delay") is not None else "failed"
            return replace(make_job_row(), status=status)

    job = make_job_row(attempt=1, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_timeout(
        _RetryBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        TimeoutError("slow"),
        cfg,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )
    assert result == "scheduled"


async def test_handle_timeout_returns_failed_on_terminal() -> None:
    """_handle_timeout returns "failed" when the DB transitions to terminal
    failure (retries exhausted), not "scheduled"."""
    from dataclasses import replace
    from datetime import timedelta
    from uuid import UUID

    import structlog

    from taskq.retry import RetryPolicy
    from taskq.testing.actor import StubActorConfig
    from taskq.testing.jobs import make_job_row
    from taskq.worker._handlers import _handle_timeout

    class _FailBackend:
        """Backend whose mark_failed_or_retry returns status='failed'."""

        async def mark_failed_or_retry(self, **kwargs: object) -> JobRow:
            return replace(make_job_row(), status="failed")

    job = make_job_row(attempt=3, max_attempts=3)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    log = structlog.get_logger("test")

    class _FakeSpan:
        def add_event(self, *_args: object, **_kwargs: object) -> None: ...

    result = await _handle_timeout(
        _FailBackend(),  # type: ignore[arg-type]
        job,
        UUID("00000000-0000-0000-0000-000000000001"),
        TimeoutError("slow"),
        cfg,
        timedelta(hours=24),
        _FakeSpan(),  # type: ignore[arg-type]
        log,
    )
    assert result == "failed"

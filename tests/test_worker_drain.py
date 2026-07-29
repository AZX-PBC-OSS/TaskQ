"""Unit tests for drain monitor and until-idle mode."""

import asyncio
import inspect
from datetime import UTC, datetime
from typing import Any
from unittest.mock import create_autospec, patch

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
from tests.conftest import _FakePool, unique_health_sock_path

# ── Helpers ────────────────────────────────────────────────────────


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({
        "PG_DSN": "postgres://u:p@localhost:5432/db",
        "LOCK_LEASE": 60,
        "HEARTBEAT_INTERVAL": 10,
        "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_drain"),
    })


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
    settings = WorkerSettings.load_from_dict({
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEALTH_SOCKET_PATH": "/tmp/test_drain_deps.sock",  # noqa: S108
    })
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

    deps = await _run_one_job_with_fake_dispatch(
        _fake_dispatch, actor_name="test_drain_fail_exc"
    )
    assert deps.drain_failures == 1

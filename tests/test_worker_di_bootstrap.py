"""Unit and integration tests for worker DI bootstrap wiring
(ProviderRegistry, ProcessScope, ThreadScope, LoopScope,
validate-before-bootstrap, scope teardown LIFO, Clock auto-registration).

Covers:
  - Bootstrap sequence happy path
  - WorkerSettings registered at PROCESS scope
  - validate() is called after all registrations, before scope.bootstrap()
  - Validate-time MissingProvider raises before TaskGroup starts
  - Validate-time DependencyCycle raises before TaskGroup starts
  - Validate-time ScopeViolation raises before TaskGroup starts
  - Scope teardown LIFO on shutdown
  - pre-registered Clock survives bootstrap
  - fresh registry auto-registers SystemClock
  - integration — worker bootstrap auto-registers SystemClock
  - integration — pre-registered FakeClock survives bootstrap
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor import ActorRef, actor
from taskq.backend._protocol import Backend, CancelPhase, JobRow
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import DependencyCycle, MissingProvider, ScopeViolation
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _main
from tests.conftest import unique_health_sock_path

# ── Helpers ────────────────────────────────────────────────────────


class _LoopDep:
    pass


class _ProcessDep:
    pass


class _Unregistered:
    pass


class _CycleA:
    pass


class _CycleB:
    pass


class _LoopToTransient:
    pass


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            # _main starts a real HealthServer — never the shared default path.
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_di_bootstrap"),
        },
    )


def _make_scopes_and_bootstrap(
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


def _backend_methods_stub() -> Backend:
    class _Methods:
        async def mark_succeeded(self, job_id: object, worker_id: object, result: object) -> bool:
            return True

        async def mark_succeeded_with_conn(
            self, conn: object, job_id: object, worker_id: object, result: object
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
    pool: object = object()
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _fake_install_with_holder(
    loop: asyncio.AbstractEventLoop,
    shutdown_event: asyncio.Event,
    holder: list[asyncio.Task[int]],
) -> None:
    shutdown_event.set()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_result(0)
    holder.append(fut)  # type: ignore[arg-type]


def _integration_settings(pg_dsn: str, *, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            # _main starts a real HealthServer — never the shared default path.
            "health_socket_path": unique_health_sock_path("worker_di_bootstrap"),
        },
    )


async def _run_main_with_mocked_deps(
    settings: WorkerSettings, *, _registry: ProviderRegistry | None = None
) -> int:
    fake_backend = _backend_methods_stub()
    worker_id_val = new_uuid()

    async def _fake_register(pool: object, s: WorkerSettings) -> object:
        return worker_id_val

    def _fake_install(
        loop: asyncio.AbstractEventLoop,
        deps: WorkerDeps,
        wid: object,
        sh_ev: asyncio.Event,
        esc_ev: asyncio.Event,
        backend: Backend,
        holder: list[asyncio.Task[int]],
    ) -> None:
        _fake_install_with_holder(loop, sh_ev, holder)

    async def _fake_all(*args: object, **kwargs: object) -> None:
        pass

    with (
        patch("taskq.worker._bootstrap.PostgresBackend", return_value=fake_backend),
        patch("taskq.worker._bootstrap.open_worker_deps") as mock_open,
        patch("taskq.worker.run.register_worker", side_effect=_fake_register),
        patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_fake_install),
        patch("taskq.worker._bootstrap.heartbeat_loop", side_effect=_fake_all),
        patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_fake_all),
        patch("taskq.worker._bootstrap.MaintenanceLeader") as mock_leader_cls,
        patch("taskq.worker.run.producer_loop", side_effect=_fake_all),
        patch("taskq.worker.run.consumer_loop_stub", side_effect=_fake_all),
        patch("taskq.worker.run.deregister_worker", new_callable=AsyncMock),
    ):
        mock_leader_instance = MagicMock()
        mock_leader_instance.run.side_effect = _fake_all
        mock_leader_cls.return_value = mock_leader_instance

        deps = _stub_deps(settings)
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)

        return await _main(settings, _registry=_registry)


# ── Bootstrap sequence happy path ───────────────────────────────


async def test_bootstrap_happy_path() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_ProcessDep, Scope.PROCESS, lambda: _ProcessDep())
    registry.register_factory(_LoopDep, Scope.LOOP, lambda: _LoopDep())
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    assert process_scope.get(WorkerSettings) is settings
    assert isinstance(process_scope.get(_ProcessDep), _ProcessDep)
    assert isinstance(loop_scope.get(_LoopDep), _LoopDep)
    assert thread_scope.get(_ProcessDep) is None

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── WorkerSettings registered at PROCESS scope ────────────────────


async def test_worker_settings_registered_at_process() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    entry = registry.get(WorkerSettings)
    assert entry.scope == Scope.PROCESS
    assert entry.kind == "value"
    assert entry.impl is settings


# ── validate() is called after all registrations, before bootstrap ─


async def test_validate_called_once_after_registrations() -> None:
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    with pytest.raises(RuntimeError, match="sealed"):
        registry.register_value(_ProcessDep, Scope.PROCESS, _ProcessDep())


# ── Validate-time MissingProvider raises before TaskGroup starts ──


async def test_missing_provider_raises_before_taskgroup() -> None:
    @actor(name="needs_unregistered")
    async def needs_unregistered(
        payload: BaseModel,
        ctx: JobContext[BaseModel],
        dep: _Unregistered,
    ) -> None: ...

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    actors_list: list[ActorRef[Any, Any]] = [needs_unregistered]

    with pytest.raises(MissingProvider):
        registry.validate(actors=actors_list)


# ── Validate-time DependencyCycle raises before TaskGroup starts ──


async def test_dependency_cycle_raises_before_taskgroup() -> None:
    registry = ProviderRegistry()

    async def make_a(dep_b: _CycleB) -> _CycleA:
        return _CycleA()

    async def make_b(dep_a: _CycleA) -> _CycleB:
        return _CycleB()

    registry.register_factory(_CycleA, Scope.LOOP, make_a)
    registry.register_factory(_CycleB, Scope.LOOP, make_b)

    with pytest.raises(DependencyCycle):
        registry.validate()


# ── Validate-time ScopeViolation raises before TaskGroup starts ──


async def test_scope_violation_raises_before_taskgroup() -> None:
    registry = ProviderRegistry()

    class _TransientDep:
        pass

    async def make_loop_dep(trans: _TransientDep) -> _LoopToTransient:
        return _LoopToTransient()

    registry.register_factory(_TransientDep, Scope.TRANSIENT, lambda: _TransientDep())
    registry.register_factory(_LoopToTransient, Scope.LOOP, make_loop_dep)

    with pytest.raises(ScopeViolation):
        registry.validate()


# ── Scope teardown LIFO on shutdown ──────────────────────────────


async def test_scope_teardown_lifo() -> None:
    teardown_order: list[str] = []

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    async def make_process() -> AsyncIterator[_ProcessDep]:
        yield _ProcessDep()
        teardown_order.append("process")

    async def make_loop() -> AsyncIterator[_LoopDep]:
        yield _LoopDep()
        teardown_order.append("loop")

    registry.register_factory(_ProcessDep, Scope.PROCESS, make_process)
    registry.register_factory(_LoopDep, Scope.LOOP, make_loop)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    async with contextlib.AsyncExitStack() as stack:
        stack.push_async_callback(process_scope.shutdown)
        stack.push_async_callback(thread_scope.shutdown)
        stack.push_async_callback(loop_scope.shutdown)

    assert teardown_order == ["loop", "process"]


# ── Scope teardown runs via AsyncExitStack even on exception ──────


async def test_scope_teardown_on_exception() -> None:
    teardown_order: list[str] = []

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    async def make_loop() -> AsyncIterator[_LoopDep]:
        yield _LoopDep()
        teardown_order.append("loop")

    registry.register_factory(_LoopDep, Scope.LOOP, make_loop)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    with pytest.raises(RuntimeError, match="boom"):
        async with contextlib.AsyncExitStack() as stack:
            stack.push_async_callback(process_scope.shutdown)
            stack.push_async_callback(thread_scope.shutdown)
            stack.push_async_callback(loop_scope.shutdown)
            raise RuntimeError("boom")

    assert teardown_order == ["loop"]


# ── pre-registered Clock survives bootstrap ─────────────


async def test_pre_registered_clock_survives_bootstrap() -> None:
    """pre-registered Clock survives bootstrap."""
    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)

    assert registry.has_provider(Clock) is True

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    clock_entry = registry.get(Clock)
    assert clock_entry.impl is fake_clock
    assert isinstance(clock_entry.impl, FakeClock)
    assert not isinstance(clock_entry.impl, SystemClock)


# ── fresh registry auto-registers SystemClock ────────


async def test_fresh_registry_auto_registers_system_clock() -> None:
    """has_provider(Clock) False on fresh registry; guard registers SystemClock."""
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    assert registry.has_provider(Clock) is False

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(Clock) is True
    clock_entry = registry.get(Clock)
    assert isinstance(clock_entry.impl, SystemClock)
    assert clock_entry.scope == Scope.PROCESS


# ── Integration tests ─────────────────────────────────────────────────


# ── integration — worker bootstrap auto-registers SystemClock ─


@pytest.mark.integration
async def test_integration_worker_bootstrap_auto_registers_system_clock(
    pg_dsn: str,
) -> None:
    """after bootstrap, resolved Clock is SystemClock with UTC now()."""

    from taskq.migrate import apply_pending

    schema = f"twdb_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    settings = _integration_settings(pg_dsn, schema=schema)

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    assert registry.has_provider(Clock) is True
    clock_entry = registry.get(Clock)
    assert isinstance(clock_entry.impl, SystemClock)
    assert clock_entry.scope == Scope.PROCESS

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    registry.validate()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    resolved = process_scope.get(Clock)
    assert resolved is not None
    assert isinstance(resolved, SystemClock)
    now_val = resolved.now()
    assert isinstance(now_val, datetime)
    assert now_val.tzinfo is not None
    delta = abs((now_val - datetime.now(UTC)).total_seconds())
    assert delta < 2.0

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── integration — pre-registered FakeClock survives bootstrap ─


@pytest.mark.integration
async def test_integration_pre_registered_fake_clock_survives_bootstrap(
    pg_dsn: str,
) -> None:
    """pre-registered FakeClock at PROCESS scope survives _main bootstrap."""

    from taskq.migrate import apply_pending

    schema = f"twdb_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    settings = _integration_settings(pg_dsn, schema=schema)
    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)

    result = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert result == 0

    clock_entry = registry.get(Clock)
    assert clock_entry.impl is fake_clock
    assert not isinstance(clock_entry.impl, SystemClock)

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    registry.validate()
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    resolved = process_scope.get(Clock)
    assert resolved is fake_clock
    assert not isinstance(resolved, SystemClock)

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── di_consumer_loop uses ProcessScope-cached Clock ────


async def test_di_consumer_loop_uses_process_scope_clock() -> None:
    """di_consumer_loop resolves Clock from ProcessScope, not SystemClock()."""
    from taskq.worker.run import di_consumer_loop

    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    captured_clock: Clock | None = None
    dispatch_event = asyncio.Event()

    async def _fake_dispatch(*args: object, **kwargs: object) -> None:
        nonlocal captured_clock
        captured_clock = kwargs.get("clock")  # type: ignore[assignment] # Why: kwargs.get() returns object | None; captured_clock is Clock | None — the assertion below verifies the runtime type.
        dispatch_event.set()

    shutdown_event = asyncio.Event()

    @actor(name="test_actor_scope_override_11")
    async def _test_actor(payload: BaseModel, ctx: JobContext[BaseModel]) -> None: ...

    job = JobRow(
        id=new_job_id(),
        actor=_test_actor.name,
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
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)

    deps = _stub_deps(settings)
    backend = _backend_methods_stub()

    with patch("taskq.worker.run.dispatch_one_job", side_effect=_fake_dispatch):
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
                actor_registry={_test_actor.name: _test_actor},
                enqueuer=SubJobEnqueuer(
                    loop_scope_resolved=None,
                    worker_pool=None,
                    backend=backend,
                ),
            )
        )
        await asyncio.wait_for(dispatch_event.wait(), timeout=2.0)
        shutdown_event.set()
        await asyncio.wait_for(loop_task, timeout=2.0)

    assert captured_clock is fake_clock

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── MissingProvider when ProcessScope has no Clock ──────


async def test_di_consumer_loop_raises_missing_provider_no_clock() -> None:
    """di_consumer_loop raises MissingProvider when ProcessScope has no cached Clock."""
    from taskq.worker.run import di_consumer_loop

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    deps = _stub_deps(settings)
    backend = _backend_methods_stub()

    with pytest.raises(MissingProvider, match="Clock"):
        await di_consumer_loop(
            deps,
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_registry={},
            enqueuer=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
        )

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Unknown actor releases the claimed job instead of stranding it ──


async def test_di_consumer_loop_releases_job_for_unknown_actor() -> None:
    """A dispatched job whose actor is absent from actor_registry is
    released via mark_snoozed (short delay) rather than left 'running'
    until lock-lease expiry."""
    from taskq.worker.run import di_consumer_loop

    fake_clock = FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, fake_clock)
    registry.validate()

    process_scope, thread_scope, loop_scope = _make_scopes_and_bootstrap(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    backend = _backend_methods_stub()
    released = asyncio.Event()
    snooze_calls: list[tuple[object, object, object, dict[str, object] | None]] = []

    async def _spy_mark_snoozed(
        job_id: object,
        worker_id: object,
        delay: object,
        *,
        metadata_update: dict[str, object] | None = None,
        **_kwargs: object,
    ) -> str:
        snooze_calls.append((job_id, worker_id, delay, metadata_update))
        released.set()
        return "scheduled"

    backend.mark_snoozed = _spy_mark_snoozed  # type: ignore[attr-defined] # Why: stub backend is a plain object; spy attribute injection is the established pattern in this file.

    job = JobRow(
        id=new_job_id(),
        actor="actor-not-in-registry",
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
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    await local_queue.put(job)

    deps = _stub_deps(settings)
    shutdown_event = asyncio.Event()

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
            actor_registry={},
            enqueuer=SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            ),
        )
    )
    await asyncio.wait_for(released.wait(), timeout=2.0)
    shutdown_event.set()
    await asyncio.wait_for(loop_task, timeout=2.0)

    assert len(snooze_calls) == 1
    released_job_id, _wid, delay, metadata_update = snooze_calls[0]
    assert released_job_id == job.id
    assert delay == timedelta(seconds=10)
    assert metadata_update == {"released_reason": "actor-not-found"}

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()

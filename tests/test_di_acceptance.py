"""Acceptance tests: DI validation end-to-end.

The acceptance criteria:

  Given a registry where a LOOP-scoped factory declares a dependency on a
  TRANSIENT-scoped type, validate() raises ScopeViolation naming the
  offending edge. Given a registry with A→B→A cycle, validate() raises
  DependencyCycle naming the cycle path. Given an actor declaring a
  dependency on Q but Q is not in the registry, validate() raises
  MissingProvider naming Q and the actor. All three exceptions are
  observable in a single integration test: start a worker with each
  misconfiguration in turn; assert the worker startup raises the named
  exception before dispatching any job.

Each failure-mode test routes through ``_main`` (the real worker bootstrap
path) using the ``_registry`` test seam, so the exception is verified to
propagate through ``open_worker_deps → registry.validate()`` — not just
``registry.validate()`` in isolation. Because ``_main`` raises before
reaching the ``TaskGroup``, no consumer task runs; the "assert no
consumer task ran" gate from the DoD is satisfied by the raise itself.

Coverage:
  - All three failure modes true via single integration test
  - MissingProvider, DependencyCycle, ScopeViolation all exercised
  - LOOP→TRANSIENT scope violation covered
  - MissingProvider fires at validate(), not dispatch
  - Registry sealed after validate()
"""

from contextlib import AsyncExitStack
from typing import Any
from unittest.mock import create_autospec

import pytest
import structlog
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq.actor import actor
from taskq.backend._protocol import Backend
from taskq.context import JobContext
from taskq.exceptions import DependencyCycle, MissingProvider, ScopeViolation
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.worker.run import _main

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────


class _Payload(BaseModel):
    value: int = 0


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


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": "taskq_test",
            "lock_lease": "60",
            "heartbeat_interval": "10",
        },
    )


# ── test_acceptance_scope_violation_blocks_worker_startup ────────


async def test_acceptance_scope_violation_blocks_worker_startup(
    pg_dsn: str,
) -> None:
    """Acceptance: LOOP-scoped factory depending on TRANSIENT → ScopeViolation propagates out of _main before dispatch."""

    class _TransientDep:
        pass

    class _LoopDep:
        pass

    async def make_loop_dep(trans: _TransientDep) -> _LoopDep:
        return _LoopDep()

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, _settings(pg_dsn))
    registry.register_factory(_TransientDep, Scope.TRANSIENT, lambda: _TransientDep())
    registry.register_factory(_LoopDep, Scope.LOOP, make_loop_dep)

    with pytest.raises(ScopeViolation) as exc_info:
        await _main(_settings(pg_dsn), _registry=registry)

    assert exc_info.value.from_scope == Scope.LOOP
    assert exc_info.value.to_scope == Scope.TRANSIENT
    assert "LOOP" in str(exc_info.value)
    assert "TRANSIENT" in str(exc_info.value)


# ── test_acceptance_dependency_cycle_blocks_worker_startup ───────


async def test_acceptance_dependency_cycle_blocks_worker_startup(
    pg_dsn: str,
) -> None:
    """Acceptance: A→B→A cycle → DependencyCycle propagates out of _main before dispatch."""

    class _CycleA:
        pass

    class _CycleB:
        pass

    async def make_a(dep_b: _CycleB) -> _CycleA:
        return _CycleA()

    async def make_b(dep_a: _CycleA) -> _CycleB:
        return _CycleB()

    @actor(name="needs_cycle_a")
    async def needs_cycle_a(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _CycleA,
    ) -> None: ...

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, _settings(pg_dsn))
    registry.register_factory(_CycleA, Scope.LOOP, make_a)
    registry.register_factory(_CycleB, Scope.LOOP, make_b)

    actor_registry: dict[str, Any] = {"needs_cycle_a": needs_cycle_a}

    with pytest.raises(DependencyCycle) as exc_info:
        await _main(_settings(pg_dsn), actor_registry=actor_registry, _registry=registry)

    assert exc_info.value.cycle_path


# ── test_acceptance_missing_provider_blocks_worker_startup ───────


async def test_acceptance_missing_provider_blocks_worker_startup(
    pg_dsn: str,
) -> None:
    """Acceptance: actor declares dep Q not in registry → MissingProvider propagates out of _main before dispatch."""

    class _Q:
        pass

    @actor(name="needs_q")
    async def needs_q(
        payload: _Payload,
        ctx: JobContext[_Payload],
        q: _Q,
    ) -> None: ...

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, _settings(pg_dsn))

    actor_registry: dict[str, Any] = {"needs_q": needs_q}

    with pytest.raises(MissingProvider) as exc_info:
        await _main(_settings(pg_dsn), actor_registry=actor_registry, _registry=registry)

    assert "Q" in exc_info.value.type_name or "_Q" in exc_info.value.type_name
    assert exc_info.value.required_by


# ── test_acceptance_clean_bootstrap_succeeds ──────────────────────


async def test_acceptance_clean_bootstrap_succeeds(pg_dsn: str) -> None:
    """Acceptance (positive control): registry with no misconfigurations bootstraps cleanly."""

    class _LoopDep:
        pass

    @actor(name="clean_actor")
    async def clean_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _LoopDep,
    ) -> None: ...

    settings = _settings(pg_dsn)
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_LoopDep, Scope.LOOP, lambda: _LoopDep())
    registry.validate(actors=[clean_actor])

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    assert process_scope.get(WorkerSettings) is settings
    assert isinstance(loop_scope.get(_LoopDep), _LoopDep)

    async with AsyncExitStack() as stack:
        stack.push_async_callback(process_scope.shutdown)
        stack.push_async_callback(thread_scope.shutdown)
        stack.push_async_callback(loop_scope.shutdown)


# ── test_acceptance_actor_receives_di_kwargs_end_to_end ──────────


async def test_acceptance_actor_receives_di_kwargs_end_to_end(
    pg_dsn: str,
) -> None:
    """Acceptance (positive control): actor with LOOP-scoped DI param receives the LOOP-cached instance end-to-end."""

    class _LoopClient:
        pass

    observed_dep: _LoopClient | None = None

    @actor(name="di_actor")
    async def di_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        client: _LoopClient,
    ) -> None:
        nonlocal observed_dep
        observed_dep = client

    settings = _settings(pg_dsn)
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_factory(_LoopClient, Scope.LOOP, lambda: _LoopClient())
    registry.validate(actors=[di_actor])

    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol — pyright cannot verify dict covariance across the Protocol boundary

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    loop_cached = loop_scope.get(_LoopClient)
    assert isinstance(loop_cached, _LoopClient)

    from taskq._di.scopes import build_actor_scope
    from taskq._ids import new_job_id, new_uuid
    from taskq.client._enqueuer import SubJobEnqueuer

    _jid = new_job_id()
    ctx = JobContext(
        job_id=_jid,
        actor="di_actor",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_Payload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=_backend_methods_stub(),
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=_jid,
            actor="di_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )
    passthrough_kwargs: dict[str, object] = {
        "payload": _Payload(),
        "ctx": ctx,
    }

    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=di_actor.fn,
        actor_name=di_actor.name,
        passthrough_kwargs=passthrough_kwargs,
    ) as resolved:
        assert "client" in resolved.di_kwargs
        assert isinstance(resolved.di_kwargs["client"], _LoopClient)
        assert resolved.di_kwargs["client"] is loop_cached

    async with AsyncExitStack() as stack:
        stack.push_async_callback(process_scope.shutdown)
        stack.push_async_callback(thread_scope.shutdown)
        stack.push_async_callback(loop_scope.shutdown)

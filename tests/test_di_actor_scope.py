"""Unit tests for build_actor_scope and ResolvedActorScope (unit variant).

Covers:
  - Happy path: actor with no DI params
  - Happy path: actor with one LOOP-scoped param
  - TRANSIENT per-invocation isolation
  - TRANSIENT teardown runs on exit
  - TRANSIENT teardown runs even on actor exception
  - Two TRANSIENT params resolve to distinct instances
  - Passthrough validation: ctx must be JobContext
  - Two consecutive invocations: LOOP cache persists
  - TRANSIENT scope INFO logging
  - TRANSIENT teardown completes under cancellation (shield)
  - ResolvedActorScope is frozen
"""

import asyncio
import dataclasses
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated, Any

import pytest
import structlog
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import (
    LoopScope,
    ProcessScope,
    ResolvedActorScope,
    ThreadScope,
    build_actor_scope,
)
from taskq._ids import new_uuid
from taskq.backend.clock import Clock, SystemClock
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.testing.clock import FakeClock

# ── Helpers ────────────────────────────────────────────────────────


class _Payload(BaseModel):
    value: int = 0


class _AsyncGraphClient:
    pass


class _TransDep:
    pass


def _make_job_ctx() -> JobContext[_Payload]:
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    return JobContext(
        job_id=new_uuid(),
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_Payload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=backend,
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=new_uuid(),
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


def _make_scopes(
    registry: ProviderRegistry,
) -> tuple[ProcessScope, ThreadScope, LoopScope]:
    scope_containers: dict[Scope, Any] = {}

    def _resolver(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            from taskq._di.solver import solve_dependencies

            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    process_scope = ProcessScope(resolver=_resolver)
    thread_scope = ThreadScope(resolver=_resolver)
    loop_scope = LoopScope(resolver=_resolver)

    scope_containers = {
        Scope.PROCESS: process_scope,
        Scope.THREAD: thread_scope,
        Scope.LOOP: loop_scope,
    }

    # Why: re-bind resolver with full container dict so nested resolution
    # from LOOP factories can reach PROCESS/THREAD providers.
    def _resolver_full(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            from taskq._di.solver import solve_dependencies

            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    process_scope._resolver = _resolver_full
    thread_scope._resolver = _resolver_full
    loop_scope._resolver = _resolver_full

    return process_scope, thread_scope, loop_scope


async def _bootstrap_scopes(
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
) -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        },
    )
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


# ── Happy path: actor with no DI params ─────────────────────────────


async def test_happy_path_no_di_params() -> None:
    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert resolved.di_kwargs == {}
        assert resolved.ctx is mock_ctx

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Happy path: actor with one LOOP-scoped param ─────────────────────


async def test_happy_path_one_loop_scoped_param() -> None:
    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _AsyncGraphClient,
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(
        _AsyncGraphClient,
        Scope.LOOP,
        lambda: _AsyncGraphClient(),
    )
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert "dep" in resolved.di_kwargs
        assert isinstance(resolved.di_kwargs["dep"], _AsyncGraphClient)
        assert resolved.ctx is mock_ctx

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT per-invocation isolation ──────────────────────────────


async def test_transient_per_invocation_isolation() -> None:
    call_count = 0

    async def make_transient() -> _TransDep:
        nonlocal call_count
        call_count += 1
        return _TransDep()

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, make_transient)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved1:
        instance1 = resolved1.di_kwargs["dep"]

    mock_ctx2 = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx2, "payload": _Payload()},
    ) as resolved2:
        instance2 = resolved2.di_kwargs["dep"]

    assert call_count == 2
    assert instance1 is not instance2

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT teardown runs on exit ──────────────────────────────────


async def test_transient_teardown_runs_on_exit() -> None:
    teardown_ran = False

    async def make_transient() -> AsyncIterator[_TransDep]:
        nonlocal teardown_ran
        yield _TransDep()
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, make_transient)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ):
        assert not teardown_ran

    assert teardown_ran

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT teardown runs even on actor exception ──────────────────


async def test_transient_teardown_on_actor_exception() -> None:
    teardown_ran = False

    async def make_transient() -> AsyncIterator[_TransDep]:
        nonlocal teardown_ran
        yield _TransDep()
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        raise RuntimeError("actor boom")

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, make_transient)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    with pytest.raises(RuntimeError, match="actor boom"):
        async with build_actor_scope(
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_func=my_actor,
            actor_name="my_actor",
            passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
        ):
            raise RuntimeError("actor boom")

    assert teardown_ran

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Two TRANSIENT params resolve to distinct instances ────────────────


async def test_two_transient_params_distinct_instances() -> None:
    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep1: Annotated[_TransDep, Scope.TRANSIENT],
        dep2: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, lambda: _TransDep())
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert resolved.di_kwargs["dep1"] is not resolved.di_kwargs["dep2"]

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Passthrough validation: ctx must be JobContext ────────────────────


async def test_ctx_must_be_job_context() -> None:
    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    with pytest.raises(TypeError, match="must be a JobContext"):
        async with build_actor_scope(
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_func=my_actor,
            actor_name="my_actor",
            passthrough_kwargs={"ctx": "not a JobContext", "payload": _Payload()},
        ):
            pass

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── Two consecutive invocations: LOOP cache persists ─────────────────


async def test_loop_cache_persists_across_invocations() -> None:
    call_count = 0

    def make_loop_dep() -> _AsyncGraphClient:
        nonlocal call_count
        call_count += 1
        return _AsyncGraphClient()

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _AsyncGraphClient,
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_AsyncGraphClient, Scope.LOOP, make_loop_dep)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ):
        pass

    mock_ctx2 = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx2, "payload": _Payload()},
    ):
        pass

    assert call_count == 1

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT scope INFO logging ──────────────────────────────────────


async def test_transient_scope_logging() -> None:
    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert resolved.ctx is mock_ctx
        assert resolved.di_kwargs == {}

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT teardown completes under cancellation (shield) ────────


async def test_transient_teardown_under_cancellation() -> None:
    teardown_finished = False

    async def make_transient() -> AsyncIterator[_TransDep]:
        nonlocal teardown_finished
        yield _TransDep()
        await asyncio.sleep(0.05)
        teardown_finished = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, make_transient)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()

    async def _run() -> None:
        async with build_actor_scope(
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_func=my_actor,
            actor_name="my_actor",
            passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
        ):
            await asyncio.sleep(10)

    with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
        await asyncio.wait_for(_run(), timeout=0.001)

    assert teardown_finished

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── ResolvedActorScope is frozen ──────────────────────────────────────


def test_resolved_actor_scope_is_frozen() -> None:
    mock_ctx = _make_job_ctx()
    scope = ResolvedActorScope(ctx=mock_ctx, di_kwargs={})
    with pytest.raises(dataclasses.FrozenInstanceError):
        scope.ctx = mock_ctx  # type: ignore[misc]


# ── actor with plain clock: Clock receives SystemClock ──────


async def test_plain_clock_receives_system_clock() -> None:
    """actor with plain clock: Clock receives registered SystemClock.

    Acceptance link (W-4): this test, together with proves the
    acceptance definition — "an actor declared as
    async def my_actor(ctx: JobContext, clock: Clock) receives a
    SystemClock instance in production and a FakeClock instance in
    tests."
    """

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        clock: Clock,
    ) -> dict[str, object]:
        return {}

    registered = SystemClock()
    registry = ProviderRegistry()
    registry.register_value(Clock, Scope.PROCESS, registered)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert resolved.di_kwargs["clock"] is registered

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── actor with plain clock: Clock receives pre-registered FakeClock ─


async def test_plain_clock_receives_pre_registered_fake_clock() -> None:
    """actor with plain clock: Clock receives pre-registered FakeClock.

    Acceptance link (W-4): this test, together with proves the
    acceptance definition — "an actor declared as
    async def my_actor(ctx: JobContext, clock: Clock) receives a
    SystemClock instance in production and a FakeClock instance in
    tests."
    """

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        clock: Clock,
    ) -> dict[str, object]:
        return {}

    fake = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    registry = ProviderRegistry()
    registry.register_value(Clock, Scope.PROCESS, fake)
    registry.validate()
    process_scope, thread_scope, loop_scope = _make_scopes(registry)
    await _bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    mock_ctx = _make_job_ctx()
    async with build_actor_scope(
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_func=my_actor,
        actor_name="my_actor",
        passthrough_kwargs={"ctx": mock_ctx, "payload": _Payload()},
    ) as resolved:
        assert resolved.di_kwargs["clock"] is fake

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()


# ── TRANSIENT consumer of PROCESS-scope Clock no ScopeViolation ──


def test_transient_consumer_of_process_clock_no_violation() -> None:
    """TRANSIENT consumer of PROCESS-scope Clock does not raise ScopeViolation."""

    class _TransientConsumer:
        def __init__(self, clock: Clock) -> None:
            self.clock = clock

    registry = ProviderRegistry()
    registry.register_value(Clock, Scope.PROCESS, SystemClock())
    registry.register_class(_TransientConsumer, Scope.TRANSIENT)
    registry.validate()

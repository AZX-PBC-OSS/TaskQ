"""Unit tests for dispatch_one_job (dispatch variant).

Covers:
  - Happy path: actor with no DI params
  - Happy path: actor with one LOOP-scoped DI param
  - TRANSIENT teardown runs on success
  - TRANSIENT teardown runs on actor exception
  - TRANSIENT teardown runs on timeout
  - Two consecutive dispatches: LOOP cache reused, TRANSIENT refreshed
  - Payload validation failure raises before TRANSIENT scope opens
  - No payload/ctx double-pass
  - Actor sees live ctx with working cancel_event
  - Interim ctx is not the actor's ctx (regression guard)
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import asyncpg
import pytest
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict

import taskq.obs as obs_mod
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import (
    LoopScope,
    ProcessScope,
    ThreadScope,
)
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import build_actor_scope, dispatch_one_job

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


# ── Shared helpers ─────────────────────────────────────────────────────


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _LoopDep:
    pass


class _TransDep:
    pass


class _FakeWorkerDeps:
    """Minimal WorkerDeps stub with just active_jobs."""

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.worker_pool: asyncpg.Pool | None = None
        self.settings = WorkerSettings()
        self.settings.worker_group = "default"
        self.redis_client: Any | None = None
        self.progress_buffers: dict[Any, Any] = {}


def _as_deps(fd: _FakeWorkerDeps) -> Any:
    return fd


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

    def _resolver_full(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            from taskq._di.solver import solve_dependencies

            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    process_scope._resolver = _resolver_full  # pyright: ignore[reportPrivateUsage]  # Why: test helper mirrors production make_resolver pattern
    thread_scope._resolver = _resolver_full  # pyright: ignore[reportPrivateUsage]  # Why: same pattern — updates resolver closure to see full scope_containers dict
    loop_scope._resolver = _resolver_full  # pyright: ignore[reportPrivateUsage]  # Why: same pattern — updates resolver closure to see full scope_containers dict

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


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry | None = None) -> None:
        self.registry = registry or ProviderRegistry()

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        self.process_scope, self.thread_scope, self.loop_scope = _make_scopes(self.registry)
        await _bootstrap_scopes(
            self.registry, self.process_scope, self.thread_scope, self.loop_scope
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


def _make_actor_ref(
    fn: Any,
    *,
    name: str = "test_actor",
) -> ActorRef[_Payload, None]:
    return ActorRef(
        name=name,
        queue="default",
        fn=fn,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not used in dispatch_one_job
        retry=RetryPolicy(),
        result_ttl=None,
    )


# ── Happy path: actor with no DI params ─────────────────────────────


async def test_happy_path_no_di_params() -> None:
    actor_called = False
    observed_payload: _Payload | None = None
    observed_ctx: JobContext[_Payload] | None = None

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        nonlocal actor_called, observed_payload, observed_ctx
        actor_called = True
        observed_payload = payload
        observed_ctx = ctx
        return {"value": payload.value}

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})
        clock = FakeClock(_NOW)

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=clock,
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert actor_called
        assert observed_payload is not None
        assert observed_payload.value == 42
        assert observed_ctx is not None
        assert observed_ctx.job_id == job.id
        assert len(fake_backend.mark_succeeded_calls) == 1


# ── Happy path: actor with one LOOP-scoped DI param ───────────────────


async def test_happy_path_one_loop_scoped_param() -> None:
    observed_dep: _LoopDep | None = None

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: _LoopDep,
    ) -> dict[str, object]:
        nonlocal observed_dep
        observed_dep = dep
        return {}

    registry = ProviderRegistry()
    registry.register_factory(
        _LoopDep,
        Scope.LOOP,
        lambda: _LoopDep(),
    )

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert observed_dep is not None
        assert isinstance(observed_dep, _LoopDep)


# ── TRANSIENT teardown runs on success ────────────────────────────────


async def test_transient_teardown_on_success() -> None:
    teardown_ran = False

    async def transient_factory() -> AsyncIterator[_TransDep]:
        yield _TransDep()
        nonlocal teardown_ran
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(
        _TransDep,
        Scope.TRANSIENT,
        transient_factory,
    )

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert teardown_ran


# ── TRANSIENT teardown runs on actor exception ────────────────────────


async def test_transient_teardown_on_actor_exception() -> None:
    teardown_ran = False

    async def transient_factory() -> AsyncIterator[_TransDep]:
        yield _TransDep()
        nonlocal teardown_ran
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        raise RuntimeError("actor boom")

    registry = ProviderRegistry()
    registry.register_factory(
        _TransDep,
        Scope.TRANSIENT,
        transient_factory,
    )

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert teardown_ran
        assert len(fake_backend.mark_failed_or_retry_calls) == 1


# ── TRANSIENT teardown runs on timeout ────────────────────────────────


async def test_transient_teardown_on_timeout() -> None:
    teardown_ran = False

    async def transient_factory() -> AsyncIterator[_TransDep]:
        yield _TransDep()
        nonlocal teardown_ran
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        await asyncio.sleep(1.0)
        return {}

    registry = ProviderRegistry()
    registry.register_factory(
        _TransDep,
        Scope.TRANSIENT,
        transient_factory,
    )

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        # 50ms deadline vs. the actor's 1.0s sleep: a wide margin so the
        # timeout fires deterministically under scheduler jitter/parallel
        # test load, while still triggering well before the actor returns.
        job = make_job_row(payload={"value": 42}, start_to_close=timedelta(milliseconds=50))

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert teardown_ran


# ── Two consecutive dispatches: LOOP cache reused, TRANSIENT refreshed ─


async def test_loop_cache_reused_transient_refreshed() -> None:
    loop_call_count = 0
    transient_call_count = 0

    class _LoopCounter:
        pass

    class _TransCounter:
        pass

    def loop_factory() -> _LoopCounter:
        nonlocal loop_call_count
        loop_call_count += 1
        return _LoopCounter()

    async def transient_counter_factory() -> AsyncIterator[_TransCounter]:
        nonlocal transient_call_count
        transient_call_count += 1
        yield _TransCounter()

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        loop_dep: Annotated[_LoopCounter, Scope.LOOP],
        trans_dep: Annotated[_TransCounter, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_LoopCounter, Scope.LOOP, loop_factory)
    registry.register_factory(_TransCounter, Scope.TRANSIENT, transient_counter_factory)

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert loop_call_count == 1
        assert transient_call_count == 2


# ── Payload validation failure raises before TRANSIENT scope opens ─────


async def test_payload_validation_failure_before_scope() -> None:
    teardown_ran = False

    async def transient_factory() -> AsyncIterator[_TransDep]:
        yield _TransDep()
        nonlocal teardown_ran
        teardown_ran = True

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_TransDep, Scope.TRANSIENT],
    ) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_TransDep, Scope.TRANSIENT, transient_factory)

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)

        job_with_bad_payload = make_job_row(
            payload={"not_a_valid_field": "oops"},
        )

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job_with_bad_payload,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert not teardown_ran
        assert len(fake_backend.mark_failed_or_retry_calls) == 1
        assert (
            fake_backend.mark_failed_or_retry_calls[0]["error_info"].error_class  # pyright: ignore[reportAttributeAccessIssue]  # Why: mark_failed_or_retry_calls stores untyped objects from mock; error_class exists at runtime.
            == "ValidationError"
        )


# ── No payload/ctx double-pass ────────────────────────────────────────


async def test_no_payload_ctx_double_pass() -> None:
    call_kwargs: dict[str, object] = {}

    class _LoopD:
        pass

    def loop_factory() -> _LoopD:
        return _LoopD()

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        dep: Annotated[_LoopD, Scope.LOOP],
    ) -> dict[str, object]:
        nonlocal call_kwargs
        call_kwargs = {
            "payload": payload,
            "ctx": ctx,
            "dep": dep,
        }
        return {}

    registry = ProviderRegistry()
    registry.register_factory(_LoopD, Scope.LOOP, loop_factory)

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert set(call_kwargs.keys()) == {"payload", "ctx", "dep"}
        assert isinstance(call_kwargs["payload"], _Payload)
        assert isinstance(call_kwargs["ctx"], JobContext)
        assert isinstance(call_kwargs["dep"], _LoopD)


# ── Actor sees live ctx with working cancel_event ─────────────────────


async def test_actor_sees_live_ctx_with_cancel_event() -> None:
    actor_ctx: JobContext[_Payload] | None = None
    registered_ctx: JobContext[BaseModel] | None = None

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        nonlocal actor_ctx, registered_ctx
        actor_ctx = ctx
        entry = active_jobs.get(job.id)
        if entry is not None:
            registered_ctx = entry.ctx
        return {}

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        active_jobs = fake_deps.active_jobs

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert actor_ctx is not None
        assert registered_ctx is not None
        assert actor_ctx is registered_ctx


# ── Interim ctx is not the actor's ctx ────────────────────────────────


async def test_interim_ctx_not_actor_ctx() -> None:
    actor_ctx: JobContext[_Payload] | None = None
    interim_ctx_ref: JobContext[BaseModel] | None = None

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        nonlocal actor_ctx
        actor_ctx = ctx
        return {}

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        active_jobs = fake_deps.active_jobs

        _real_build_actor_scope = build_actor_scope

        def _spy_build_actor_scope(**kwargs: Any) -> Any:
            passthrough: dict[str, object] | None = kwargs.get("passthrough_kwargs")
            if passthrough is not None and "ctx" in passthrough:
                nonlocal interim_ctx_ref
                interim_ctx_ref = passthrough["ctx"]  # type: ignore[assignment]  # Why: passthrough_kwargs is dict[str, object]; the value IS a JobContext at runtime but pyright cannot narrow from object
            return _real_build_actor_scope(**kwargs)

        with pytest.MonkeyPatch.context() as m:
            m.setattr("taskq.worker.dispatch.build_actor_scope", _spy_build_actor_scope)

            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        assert actor_ctx is not None
        assert interim_ctx_ref is not None
        assert actor_ctx is not interim_ctx_ref


# ── Actor sees live ctx whose cancel_event can be signalled ────────────


async def test_cancel_event_on_live_ctx_works() -> None:
    actor_saw_cancellation = False

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        nonlocal actor_saw_cancellation
        ctx.cancel_event.set()
        actor_saw_cancellation = ctx.cancellation_requested
        return {}

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})
        active_jobs = fake_deps.active_jobs

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )


# ── CONSUMER span and metrics ─────────────────


async def test_dispatch_one_job_creates_consumer_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dispatch_one_job creates a CONSUMER span named 'process {actor}'."""

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        pass

    from taskq.testing.otel import setup_tracer

    _, exporter = setup_tracer(monkeypatch)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        consumer = exporter.span_named("process test_actor")
        assert consumer is not None
        assert consumer.kind == trace.SpanKind.CONSUMER


async def test_dispatch_one_job_consumer_span_has_semconv_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CONSUMER span carries messaging semconv attributes."""

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        pass

    from taskq.testing.otel import setup_tracer

    _, exporter = setup_tracer(monkeypatch)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        consumer = exporter.span_named("process test_actor")
        assert consumer is not None
        assert consumer.attributes is not None
        assert consumer.attributes.get("messaging.system") == "taskq"
        assert consumer.attributes.get("messaging.destination.name") == "default"
        assert consumer.attributes.get("messaging.operation.type") == "process"
        assert consumer.attributes.get("taskq.actor") == "test_actor"


async def test_dispatch_one_job_records_consumed_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dispatch_one_job records messaging.client.consumed.messages on success."""

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {"ok": True}

    from taskq.testing.otel import (
        counter_data_points,
        setup_meter,
        setup_tracer,
    )

    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        dps = counter_data_points(reader, "messaging.client.consumed.messages")
        assert len(dps) >= 1
        dp = dps[0]
        assert dp.attributes is not None
        assert dp.attributes.get("actor") == "test_actor"
        assert dp.attributes.get("queue") == "default"
        assert dp.attributes.get("outcome") == "succeeded"


# ── Regression: snooze/retry maps "scheduled" outcome to "abandoned" metric ─


async def test_dispatch_one_job_records_abandoned_on_snooze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When actor raises Snooze, consumed metric records outcome="abandoned"."""

    from taskq.exceptions import Snooze

    async def snoozy_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise Snooze(delay=timedelta(seconds=30))

    from taskq.testing.otel import (
        counter_data_points,
        setup_meter,
        setup_tracer,
    )

    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(snoozy_actor)
        job = make_job_row(payload={"value": 42})

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        dps = counter_data_points(reader, "messaging.client.consumed.messages")
        assert len(dps) >= 1
        dp = dps[0]
        assert dp.attributes is not None
        assert dp.attributes.get("outcome") == "abandoned"


# ── CONSUMER span link integration ────────────────────────────────────


async def test_dispatch_one_job_consumer_span_links_to_producer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CONSUMER span links to the PRODUCER span when job has trace_id/span_id."""

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        pass

    from taskq.testing.otel import setup_tracer

    _, exporter = setup_tracer(monkeypatch)

    tracer = obs_mod.get_tracer()
    producer_span = tracer.start_span("enqueue test_actor")
    prod_ctx = producer_span.get_span_context()
    producer_span.end()

    trace_id_hex = format(prod_ctx.trace_id, "032x")
    span_id_hex = format(prod_ctx.span_id, "016x")

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42}, trace_id=trace_id_hex, span_id=span_id_hex)

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        consumer = exporter.span_named("process test_actor")
        assert consumer is not None
        assert consumer.links is not None
        assert len(consumer.links) == 1
        assert consumer.links[0].context.trace_id == prod_ctx.trace_id
        assert consumer.links[0].context.span_id == prod_ctx.span_id


async def test_dispatch_one_job_malformed_trace_id_no_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed trace_id produces no link; job still succeeds."""

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        pass

    from taskq.testing.otel import setup_tracer

    _, exporter = setup_tracer(monkeypatch)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(
            payload={"value": 42},
            trace_id="not-valid-hex",
            span_id="0123456789abcdef",
        )

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=job,
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        consumer = exporter.span_named("process test_actor")
        assert consumer is not None
        assert consumer.links is not None
        assert len(consumer.links) == 0
        assert len(fake_backend.mark_succeeded_calls) == 1

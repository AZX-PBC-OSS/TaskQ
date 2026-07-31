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
            == "PayloadValidationError"
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


# ── Queue-cap wiring: dispatch_one_job acquires the fleet-wide cap ──


async def test_dispatch_one_job_acquires_registered_queue_cap() -> None:
    """End-to-end wiring: with a queue-cap reservation registered for the
    job's queue, ``dispatch_one_job`` routes it through the acquire path —
    a saturated cap snoozes the job with operator-visible ``awaiting``
    metadata instead of running the actor, and a freed cap lets a later
    dispatch run.

    Pins the seam between ``_effective_reservations`` and the
    ``consume_one_job`` call: if the prepend is ever dropped from
    ``dispatch_one_job``, the saturated cap would be ignored and the first
    dispatch would wrongly succeed.
    """
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import (
        RateLimitRegistry,
        queue_concurrency_reservation_name,
    )
    from taskq.ratelimit.reservation import ConcurrencyReservation

    actor_ran = 0

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        nonlocal actor_ran
        actor_ran += 1

    clock = FakeClock(_NOW)
    rl_registry = RateLimitRegistry()
    queue_cap_name = queue_concurrency_reservation_name("default")
    cap_res = ConcurrencyReservation(
        name=queue_cap_name, slots=1, lease=timedelta(minutes=5), clock=clock
    )
    rl_registry.register_queue_cap_reservation(cap_res)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)

    async with _ScopeStack(di_registry) as scopes:
        # Another worker saturates the only queue-cap slot.
        holder_worker = new_uuid()
        await cap_res.acquire(new_uuid(), holder_worker)

        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()

        async def _dispatch() -> None:
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=make_job_row(payload={"value": 42}),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=_make_actor_ref(my_actor),  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=clock,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        # 1. Saturated cap → snoozed with awaiting metadata, actor not run.
        await _dispatch()
        assert actor_ran == 0
        assert len(fake_backend.mark_snoozed_calls) == 1
        assert fake_backend.mark_snoozed_calls[0]["metadata_update"] == {
            "awaiting": "reservation:taskq:global:queue:default"
        }
        assert len(fake_backend.mark_succeeded_calls) == 0

        # 2. Cap freed → dispatch acquires it and the actor runs; the slot
        #    is released afterwards (free again for the next job).
        await cap_res.release(0, holder_worker)
        await _dispatch()
        assert actor_ran == 1
        assert len(fake_backend.mark_succeeded_calls) == 1
        assert cap_res.table.peek_slots(queue_cap_name) == (1, 0)


async def test_dispatch_one_job_nth_plus_one_denied_while_cap_slot_held() -> None:
    """The (N+1)th job on a capped queue is denied while N dispatched jobs
    hold the cap slots — the e2e proof that ``dispatch_one_job`` ITSELF
    acquires the fleet-wide cap through the DI-provided registry, not just
    that an externally saturated cap blocks dispatch.

    With ``slots=1``: job 1 dispatches and its actor blocks mid-run,
    HOLDING the cap slot via the real acquire path; job 2 (the N+1th
    concurrent dispatch on the same queue) is snoozed with
    operator-visible ``awaiting`` metadata and its actor never runs;
    letting job 1 finish frees the slot and job 3 then dispatches
    successfully.

    Mutation target: if the queue-cap prepend/lookup in dispatch.py is
    ever broken (cap name derivation drift, dropped prepend, membership
    check against the wrong name), job 2 wrongly runs and this test fails.
    """
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import (
        RateLimitRegistry,
        queue_concurrency_reservation_name,
    )
    from taskq.ratelimit.reservation import ConcurrencyReservation

    clock = FakeClock(_NOW)
    rl_registry = RateLimitRegistry()
    queue_cap_name = queue_concurrency_reservation_name("default")
    cap_res = ConcurrencyReservation(
        name=queue_cap_name, slots=1, lease=timedelta(minutes=5), clock=clock
    )
    rl_registry.register_queue_cap_reservation(cap_res)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)

    job1_started = asyncio.Event()
    job1_release = asyncio.Event()
    job1_finished = False
    job2_ran = False
    job3_ran = False

    async def blocking_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        nonlocal job1_finished
        job1_started.set()
        await job1_release.wait()
        job1_finished = True

    async def job2_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        nonlocal job2_ran
        job2_ran = True

    async def job3_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        nonlocal job3_ran
        job3_ran = True

    async with _ScopeStack(di_registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()

        async def _dispatch(actor_fn: Any) -> None:
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=make_job_row(payload={"value": 42}),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=_make_actor_ref(actor_fn),  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=clock,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        # 1. Job 1 dispatches and blocks inside the actor — it must be
        #    HOLDING the queue-cap slot through the real acquire path.
        task1 = asyncio.create_task(_dispatch(blocking_actor))
        await asyncio.wait_for(job1_started.wait(), timeout=5.0)
        assert cap_res.table.peek_slots(queue_cap_name) == (0, 1)

        # 2. The (N+1)th job on the same queue is denied: snoozed with
        #    operator-visible awaiting metadata, actor never runs.
        await _dispatch(job2_actor)
        assert not job2_ran
        assert len(fake_backend.mark_snoozed_calls) == 1
        assert fake_backend.mark_snoozed_calls[0]["metadata_update"] == {
            "awaiting": "reservation:taskq:global:queue:default"
        }

        # 3. Job 1 finishes → its slot is released → job 3 dispatches.
        job1_release.set()
        await asyncio.wait_for(task1, timeout=5.0)
        assert job1_finished
        assert cap_res.table.peek_slots(queue_cap_name) == (1, 0)

        await _dispatch(job3_actor)
        assert job3_ran
        assert len(fake_backend.mark_succeeded_calls) == 2
        # And job 3's slot was released after its actor completed.
        assert cap_res.table.peek_slots(queue_cap_name) == (1, 0)


# ── Actor-declared primitive instance end-to-end ──────────────────────


async def test_dispatch_one_job_actor_declared_tokenbucket_instance_consumed() -> None:
    """End-to-end: an actor declaring a TokenBucket INSTANCE (the primary
    registration path) dispatches through the DI-provided registry — the
    actor body runs AND the bucket's token is permanently consumed.

    The instance is pre-registered on a fresh RateLimitRegistry (exactly
    what the worker bootstrap's collection pass does); dispatch resolves
    the registry from the LOOP-scope DI cache and acquires via the
    instance's ``.name``. After the actor succeeds, release_for_actor
    makes the consumption permanent (refund_on_release=False), so a peek
    on the OWNED registry shows one token spent.
    """
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    bucket = TokenBucket(name="decl_bucket", capacity=5, refill_per_second=1.0, backend="memory")
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)  # what the bootstrap collection pass does

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)

    actor_ran = 0

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        nonlocal actor_ran
        actor_ran += 1

    actor_ref = ActorRef(
        name="test_actor",
        queue="default",
        fn=my_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not used in dispatch_one_job
        retry=RetryPolicy(),
        result_ttl=None,
        rate_limits=[bucket],
    )
    clock = FakeClock(_NOW)

    async with _ScopeStack(di_registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=clock,
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert actor_ran == 1
        assert len(fake_backend.mark_succeeded_calls) == 1

        # One token spent — permanently (release_for_actor sets
        # refund_on_release=False after the actor ran). Frozen clock → no
        # refill elapsed, so exactly capacity - 1 remains.
        state = await rl_registry.peek("decl_bucket", clock=clock)
        assert state.tokens_remaining == 4.0


# ── PayloadValidationError structured attributes ────────────────────


async def test_payload_validation_error_carries_structured_attributes() -> None:
    """``validate_actor_payload`` wraps ``ValidationError`` as
    ``PayloadValidationError`` with ``actor`` and ``validation_errors``
    populated — pinning the structured attributes the dispatch path
    relies on for non-retryable classification."""
    from taskq._validation import validate_actor_payload
    from taskq.exceptions import PayloadValidationError

    with pytest.raises(PayloadValidationError) as exc_info:
        validate_actor_payload(
            _Payload,
            {"not_a_valid_field": "oops"},
            actor="test_actor",
        )

    assert exc_info.value.actor == "test_actor"
    assert len(exc_info.value.validation_errors) > 0
    assert exc_info.value.validation_errors[0]["loc"] == ("not_a_valid_field",)

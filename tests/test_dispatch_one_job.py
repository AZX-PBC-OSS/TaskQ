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
  - Slot-pool acquire failure raises outside the job-outcome accounting
  - Escape paths apply the batch policy hook (cooperative cancel,
    pre-actor payload validation)
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Generator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from unittest.mock import MagicMock
from uuid import UUID

import asyncpg
import pytest
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict

import taskq.obs as obs_mod
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import (
    build_actor_scope,
)
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import with_connection_init
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import (
    SlotPoolAcquireError,
    dispatch_one_job,
)
from tests._di_scopes import bootstrap_scopes, make_scopes

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
        self.slot_pool: asyncpg.Pool | None = None
        # Mirrors WorkerDeps.slot_pool_connection_init: None = this pool's
        # connections are not known to carry the registration's hook (the
        # injected-pool shape every slot-pool test in this file drives).
        self.slot_pool_connection_init: Callable[[asyncpg.Connection], Awaitable[None]] | None = (
            None
        )
        # Why load_from_dict, not WorkerSettings(): the bare constructor
        # skips post_load, leaving every field None - including the ones
        # the consumer reads on the success path (result_max_bytes).
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.settings.worker_group = "default"
        self.redis_client: Any | None = None
        self.progress_buffers: dict[Any, Any] = {}
        self.disowned_jobs: set[UUID] = set()


def _as_deps(fd: _FakeWorkerDeps) -> Any:
    return fd


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry | None = None) -> None:
        self.registry = registry or ProviderRegistry()

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        self.process_scope, self.thread_scope, self.loop_scope = make_scopes(self.registry)
        await bootstrap_scopes(
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


async def test_dispatch_threads_the_rows_stored_schema_ver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dispatch_one_job's pre-scope validation threads the ROW's stored
    ``payload_schema_ver`` into the ``PayloadValidationError`` - the
    worker path's half of the schema-drift diagnostic.

    The dispatch path routes the failure into the terminal write (the
    exception itself does not propagate), so the wiring is pinned with a
    delegate spy on ``taskq.worker.dispatch.validate_actor_payload``: the
    real helper still runs (the terminal write below proves the failure
    path is unchanged), and the spy records the version the call site
    passed. The row carries 0 - older than the current schema - so a
    threaded row version is distinguishable from the helper's
    current-version default.
    """
    import taskq.worker.dispatch as dispatch_mod

    real_validate = dispatch_mod.validate_actor_payload  # pyright: ignore[reportPrivateImportUsage]  # Why: the spy must read and replace the dispatch module's own binding, the seam the dispatch call site resolves; the helper's home module is private, so the re-export is not declared.
    seen_versions: list[str | None] = []

    def spy_validate(
        payload_type: type[BaseModel],
        raw_payload: dict[str, object] | BaseModel,
        actor: str | None = None,
        *,
        payload_schema_ver: str | None = None,
    ) -> BaseModel:
        seen_versions.append(payload_schema_ver)
        return real_validate(
            payload_type, raw_payload, actor, payload_schema_ver=payload_schema_ver
        )

    monkeypatch.setattr(dispatch_mod, "validate_actor_payload", spy_validate)

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {}

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        pre_migration_row = replace(
            make_job_row(payload={"not_a_valid_field": "oops"}),
            payload_schema_ver=0,
        )

        await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=pre_migration_row,
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

        assert seen_versions == ["0"], (
            "dispatch_one_job validated the payload without threading the "
            "row's stored payload_schema_ver - a pre-migration row is then "
            "indistinguishable from caller garbage on the worker path"
        )
        # The failure path itself is unchanged: the real helper raised
        # and the terminal write classified it as the documented
        # non-retryable validation failure.
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
        # The two contexts are one job: the DI factories' ctx and the
        # actor's ctx log through the same bound logger, so a job's lines
        # carry one set of fields from one logger, bound once.
        assert actor_ctx.log is interim_ctx_ref.log


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


# ── Escape paths apply the batch policy hook ──────────────────────────


async def test_cooperative_cancel_escape_applies_batch_hook() -> None:
    """A cooperative cancel escaping ``consume_one_job`` still reaches
    the batch policy hook: dispatch's CancelledError handler applies
    :func:`apply_batch_terminal_outcome` with ``cancelled`` best-effort
    before its re-raise - a batch completes on any terminal member,
    discarded included, so the finalizer runs immediately instead of
    waiting for a sweep interval. The recorder stands in for the hook
    (the FakeBackend carries no batch stores), pinning the call and its
    outcome; a dispatch that re-raises past the hook leaves the
    recorder empty and turns this pin red."""
    hook_calls: list[tuple[UUID, str]] = []

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        raise asyncio.CancelledError

    async def _recording_batch_hook(
        backend: object,
        job: JobRow,
        outcome: str,
        *,
        transaction_conn: object = None,
    ) -> None:
        hook_calls.append((job.id, outcome))

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)
        job = make_job_row(payload={"value": 42})
        active_jobs = fake_deps.active_jobs

        with pytest.MonkeyPatch.context() as m:
            m.setattr("taskq.worker.dispatch.apply_batch_terminal_outcome", _recording_batch_hook)

            with pytest.raises(asyncio.CancelledError):
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

        assert hook_calls == [(job.id, "cancelled")], (
            "the CancelledError escape must apply the batch hook with the "
            "cancelled outcome before its re-raise - a batch whose last "
            "member ends through the escape completes immediately, "
            f"not waiting for the sweep interval; got {hook_calls}"
        )


async def test_payload_validation_escape_applies_batch_hook() -> None:
    """A pre-actor payload-validation failure escaping dispatch's
    validate call still reaches the batch policy hook: the
    generic-exception escape routes through ``_handle_generic_exception``
    and applies :func:`apply_batch_terminal_outcome` with the handler's
    terminal outcome - ``failed`` for the non-retryable
    ``PayloadValidationError`` - before returning. The recorder stands
    in for the hook, pinning the call and its outcome; a dispatch that
    returns past the hook leaves the recorder empty and turns this pin
    red."""
    hook_calls: list[tuple[UUID, str]] = []

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {}

    async def _recording_batch_hook(
        backend: object,
        job: JobRow,
        outcome: str,
        *,
        transaction_conn: object = None,
    ) -> None:
        hook_calls.append((job.id, outcome))

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        actor_ref = _make_actor_ref(my_actor)

        job_with_bad_payload = make_job_row(
            payload={"not_a_valid_field": "oops"},
        )

        with pytest.MonkeyPatch.context() as m:
            m.setattr("taskq.worker.dispatch.apply_batch_terminal_outcome", _recording_batch_hook)

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

        assert hook_calls == [(job_with_bad_payload.id, "failed")], (
            "the payload-validation escape must apply the batch hook with "
            "the handler's terminal outcome - a batch whose last member "
            "ends through the escape completes immediately, "
            f"not waiting for the sweep interval; got {hook_calls}"
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


# ── A released row is outcome="scheduled", never "abandoned" ──────────────


async def test_dispatch_one_job_records_scheduled_on_snooze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the actor raises Snooze the row goes back to the queue: the
    consumed metric says outcome="scheduled". "abandoned" is the
    operator-cancel outcome and must never describe a snooze."""

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
        assert dp.attributes.get("outcome") == "scheduled"
        # A snooze is not a failure: the failure counter stays untouched.
        assert counter_data_points(reader, "taskq.jobs.attempt_failures") == []


async def _dispatch_with(
    monkeypatch: pytest.MonkeyPatch,
    actor_fn: Callable[..., Coroutine[Any, Any, object]],
    *,
    backend: FakeBackend | None = None,
    job: JobRow | None = None,
) -> tuple[Any, str]:
    """Dispatch one job through the real dispatch path against a FakeBackend
    with a per-test isolated meter; return (reader, outcome)."""
    from taskq.testing.otel import setup_meter, setup_tracer

    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)
    fake_backend = backend if backend is not None else FakeBackend()
    async with _ScopeStack() as scopes:
        outcome = await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(_FakeWorkerDeps()),
            job=job if job is not None else make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=_make_actor_ref(actor_fn),  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )
    return reader, outcome


def _timeouts(reader: Any) -> dict[tuple[str, str], int]:
    from taskq.testing.otel import counter_data_points

    return {
        (str(p.attributes["actor"]), str(p.attributes["kind"])): int(p.value)
        for p in counter_data_points(reader, "taskq.jobs.timeouts")
        if p.attributes
    }


async def test_start_to_close_timeout_is_counted_and_its_duration_labelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A per-attempt timeout records taskq.jobs.timeouts{kind="start_to_close"}
    once, and the process-duration sample carries the attempt's outcome so
    a budget-length timeout is not folded into the success distribution."""
    from taskq.testing.otel import histogram_points

    async def slow_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise TimeoutError("start_to_close")

    reader, outcome = await _dispatch_with(monkeypatch, slow_actor)

    assert outcome == "scheduled"
    assert _timeouts(reader) == {("test_actor", "start_to_close"): 1}
    durations = histogram_points(reader, "messaging.process.duration")
    assert [dict(p.attributes or {}) for p in durations] == [
        {"actor": "test_actor", "queue": "default", "outcome": "scheduled"}
    ]


async def test_queue_wait_is_the_rows_own_eligible_to_claimed_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """taskq.jobs.queue_wait_seconds is started_at - scheduled_at from the
    claimed row's server-clock stamps (eligibility to claim), per (actor,
    queue) - what every dispatched job actually waited, where the sampled
    oldest_pending_age gauge only shows the head of the line."""
    from taskq.testing.otel import histogram_points

    async def ok_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        return None

    job = replace(
        make_job_row(payload={"value": 42}),
        scheduled_at=_NOW,
        started_at=_NOW + timedelta(seconds=2.5),
    )
    reader, outcome = await _dispatch_with(monkeypatch, ok_actor, job=job)
    assert outcome == "succeeded"
    points = histogram_points(reader, "taskq.jobs.queue_wait_seconds")
    assert len(points) == 1
    assert dict(points[0].attributes or {}) == {"actor": "test_actor", "queue": "default"}
    assert points[0].count == 1
    assert points[0].sum == pytest.approx(2.5)


async def test_success_duration_is_labelled_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    from taskq.testing.otel import histogram_points

    async def ok_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {"ok": True}

    reader, outcome = await _dispatch_with(monkeypatch, ok_actor)
    assert outcome == "succeeded"
    assert [
        p.attributes.get("outcome")
        for p in histogram_points(reader, "messaging.process.duration")
        if p.attributes
    ] == ["succeeded"]
    assert _timeouts(reader) == {}


async def test_schedule_to_close_refusals_count_as_whole_job_timeouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the backend's deadline arbitration refuses a snooze or a
    RetryAfter with DeadlineExceeded, the job ended on its whole-job
    budget: taskq.jobs.timeouts{kind="schedule_to_close"} counts it. A
    MaxAttemptsExceeded refusal is not a timeout."""
    from taskq.exceptions import RetryAfter

    async def snoozy(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise Snooze(delay=timedelta(seconds=30))

    async def retry_later(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise RetryAfter(delay=timedelta(seconds=30))

    reader, outcome = await _dispatch_with(
        monkeypatch, snoozy, backend=FakeBackend(mark_snoozed_return="failed")
    )
    assert outcome == "failed"
    assert _timeouts(reader) == {("test_actor", "schedule_to_close"): 1}

    reader, outcome = await _dispatch_with(
        monkeypatch,
        retry_later,
        backend=FakeBackend(mark_retry_after_return="failed:DeadlineExceeded"),
    )
    assert outcome == "failed"
    assert _timeouts(reader) == {("test_actor", "schedule_to_close"): 1}

    reader, outcome = await _dispatch_with(
        monkeypatch,
        retry_later,
        backend=FakeBackend(mark_retry_after_return="failed:MaxAttemptsExceeded"),
    )
    assert outcome == "failed"
    assert _timeouts(reader) == {}


async def test_retry_refused_by_the_deadline_arm_counts_as_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The classifier decided a retry but the backend's deadline arbitration
    landed the row failed (schedule_to_close reached before the next
    dispatch): the attempt failure is counted as retryable - that was the
    decision - and the whole-job timeout is counted beside it."""
    from taskq.testing.otel import counter_data_points

    class _DeadlineRefusingBackend(FakeBackend):
        async def mark_failed_or_retry(self, *args: Any, **kwargs: Any) -> JobRow:
            row = await super().mark_failed_or_retry(*args, **kwargs)
            return replace(row, status="failed", error_class="DeadlineExceeded")

    async def flaky(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise RuntimeError("upstream 503")

    reader, outcome = await _dispatch_with(monkeypatch, flaky, backend=_DeadlineRefusingBackend())
    assert outcome == "failed"
    assert _timeouts(reader) == {("test_actor", "schedule_to_close"): 1}
    failures = counter_data_points(reader, "taskq.jobs.attempt_failures")
    assert [dict(p.attributes or {}) for p in failures] == [
        {"actor": "test_actor", "error_type": "RuntimeError", "retryable": "true"}
    ]


async def test_dispatch_one_job_counts_a_retried_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One retryable raise → one taskq.jobs.attempt_failures sample labelled
    by actor, exception class and retryable="true", and the consumed
    outcome is "scheduled" (the retry went back to the queue) - not
    "abandoned", which is what TaskQAbandonedJobs used to page on."""

    class _FlakyError(RuntimeError):
        pass

    async def flaky_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise _FlakyError("upstream 503")

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
        actor_ref = _make_actor_ref(flaky_actor)
        job = make_job_row(payload={"value": 42})

        outcome = await dispatch_one_job(
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
        assert outcome == "scheduled"

        failures = counter_data_points(reader, "taskq.jobs.attempt_failures")
        assert len(failures) == 1
        assert failures[0].value == 1
        assert failures[0].attributes is not None
        assert dict(failures[0].attributes) == {
            "actor": "test_actor",
            "error_type": "_FlakyError",
            "retryable": "true",
        }

        consumed = counter_data_points(reader, "messaging.client.consumed.messages")
        assert [dp.attributes.get("outcome") for dp in consumed if dp.attributes] == ["scheduled"]


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
    job's queue, ``dispatch_one_job`` routes it through the acquire path -
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
    hold the cap slots - the e2e proof that ``dispatch_one_job`` ITSELF
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

        # 1. Job 1 dispatches and blocks inside the actor - it must be
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
    registration path) dispatches through the DI-provided registry - the
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

        # One token spent - permanently (release_for_actor sets
        # refund_on_release=False after the actor ran). Frozen clock → no
        # refill elapsed, so exactly capacity - 1 remains.
        state = await rl_registry.peek("decl_bucket", clock=clock)
        assert state.tokens_remaining == 4.0


# ── PayloadValidationError structured attributes ────────────────────


async def test_payload_validation_error_carries_structured_attributes() -> None:
    """``validate_actor_payload`` wraps ``ValidationError`` as
    ``PayloadValidationError`` with ``actor`` and ``validation_errors``
    populated - pinning the structured attributes the dispatch path
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


# ── Slot-pool acquire failure: infrastructure, not a job outcome ──────


class _AcquireFailsPool:
    """Pool stand-in whose acquire fails like asyncpg's bounded acquire:
    the context manager's ``__aenter__`` raises from the bounded wait.

    ``error`` is injectable because asyncpg surfaces two distinct
    failure families there - the wait timing out (builtin TimeoutError)
    and a fresh connection being refused server-side (coded
    PostgresError subclasses like InvalidPasswordError that are NOT
    PostgresConnectionError children). Both are infrastructure, never a
    job outcome.
    """

    def __init__(self, error: BaseException) -> None:
        self.error = error

    def acquire(self, timeout: float | None = None) -> "_FailingAcquireCtx":
        return _FailingAcquireCtx(self.error)


class _FailingAcquireCtx:
    """Both asyncpg acquire shapes - awaitable and async context manager -
    failing at the same point the real bounded acquire does."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    def __await__(self) -> Generator[None, None, Any]:
        # Why raise-then-yield: __await__ must return an iterator; the
        # unreachable yield makes this a generator whose first next()
        # raises, surfacing the error exactly where the real bounded
        # acquire surfaces it.
        raise self._error
        yield  # pragma: no cover  # Why: generator marker - unreachable by construction.

    async def __aenter__(self) -> Any:
        raise self._error

    async def __aexit__(self, *_exc: object) -> None:
        return None


@pytest.mark.parametrize(
    "acquire_error",
    [
        TimeoutError(),
        asyncpg.InvalidPasswordError("password authentication failed"),
    ],
    ids=["acquire-wait-timeout", "connect-refused-postgres-error"],
)
async def test_slot_pool_acquire_failure_raises_outside_job_outcome_accounting(
    monkeypatch: pytest.MonkeyPatch,
    acquire_error: BaseException,
) -> None:
    """A bounded acquire that fails raises SlotPoolAcquireError before any
    span or metric exists - for both failure families asyncpg surfaces
    at acquire time.

    The acquire is infrastructure: the job is already claimed and
    recovers by lock-lease expiry, so counting it as a consumed message
    or a job failure would make a drain step report failures that never
    happened. The counter fires (from the exception branch) and the log
    event carries the per-occurrence cause; the consumed-message record
    must NOT fire.
    """
    import taskq.worker.dispatch as dispatch_mod

    record_acquire_failure = MagicMock()
    record_consumed = MagicMock()
    monkeypatch.setattr(dispatch_mod, "record_slot_pool_acquire_failure", record_acquire_failure)
    monkeypatch.setattr(dispatch_mod, "record_consumed_message", record_consumed)

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        fake_deps.slot_pool = _AcquireFailsPool(acquire_error)  # type: ignore[assignment]  # Why: duck-typed pool stand-in for the bounded-acquire failure path.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with pytest.raises(SlotPoolAcquireError):
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        # The counter fires from the acquire's exception branch...
        record_acquire_failure.assert_called_once_with()
        # ...and the job-outcome accounting never runs: no consumed
        # message, no backend terminal write (the job stays claimed and
        # lock-lease expiry reclaims it).
        record_consumed.assert_not_called()
        assert fake_backend.mark_succeeded_calls == []
        assert fake_backend.mark_failed_or_retry_calls == []
        assert fake_backend.mark_snoozed_calls == []


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
    return {}


# ── noop outcome: not a consumption, not a process duration ──────────────


async def test_noop_outcome_records_no_consumed_message_nor_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A noop terminal write (the row moved underneath this dispatch - a
    reclaim race) is not a consumption: the job will be re-consumed and
    re-recorded by its next dispatch, so the consumed-messages counter
    and the process-duration histogram must stay untouched.

    Mirrors the spy pattern of
    test_slot_pool_acquire_failure_raises_outside_job_outcome_accounting:
    infrastructure-shaped non-consumptions must not inflate the
    job-outcome metrics.
    """
    import taskq.worker.dispatch as dispatch_mod

    record_consumed = MagicMock()
    record_duration = MagicMock()
    monkeypatch.setattr(dispatch_mod, "record_consumed_message", record_consumed)
    monkeypatch.setattr(dispatch_mod, "record_process_duration", record_duration)

    async def snooze_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
        raise Snooze(timedelta(seconds=30))

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend(mark_snoozed_return="noop")
        fake_deps = _FakeWorkerDeps()

        outcome = await dispatch_one_job(
            backend=as_backend(fake_backend),
            deps=_as_deps(fake_deps),
            job=make_job_row(payload={"value": 42}),
            worker_id=_WORKER_ID,
            registry=scopes.registry,
            process_scope=scopes.process_scope,
            thread_scope=scopes.thread_scope,
            loop_scope=scopes.loop_scope,
            actor_ref=_make_actor_ref(snooze_actor),  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

        assert outcome == "noop"
        assert len(fake_backend.mark_snoozed_calls) == 1
        record_consumed.assert_not_called()
        record_duration.assert_not_called()


# ── Slot-pool release: a pool closed mid-dispatch must not replace the outcome ──


class _FakeSlotConn:
    """Connection stand-in for the per-slot path: the transaction context
    and savepoint statements succeed, and the connection is never inside
    a transaction at release time (the commit completed)."""

    def __init__(self) -> None:
        self.terminated = False
        self.statements: list[str] = []

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()

    async def execute(self, query: str, *args: object) -> str:
        self.statements.append(query)
        return "OK"

    def is_in_transaction(self) -> bool:
        return False

    def terminate(self) -> None:
        self.terminated = True


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _ReleaseFailsPool:
    """Pool stand-in whose acquire succeeds but whose release raises the
    way asyncpg's does when a credential-rotation drain closed the pool
    underneath an in-flight dispatch."""

    def __init__(self) -> None:
        self.conn = _FakeSlotConn()

    def acquire(self, timeout: float | None = None) -> Coroutine[Any, Any, _FakeSlotConn]:
        # asyncpg's acquire is a plain awaitable yielding the connection.
        return self._acquired()

    async def _acquired(self) -> _FakeSlotConn:
        return self.conn

    async def release(self, conn: object) -> None:
        raise asyncpg.InterfaceError("cannot call release(): the pool is closed")


async def test_slot_pool_release_against_closed_pool_preserves_job_outcome() -> None:
    """A release against a pool closed mid-dispatch (a credential-rotation
    drain terminated it underneath) is logged and swallowed - the unwind
    must never replace the job's real outcome with a release error. The
    job committed; it reports `succeeded`, and the skipped release is
    visible in the logs."""
    import structlog

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        pool = _ReleaseFailsPool()
        fake_deps.slot_pool = pool  # type: ignore[assignment]  # Why: duck-typed pool stand-in for the release-failure unwind path.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with structlog.testing.capture_logs() as logs:
            outcome = await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        assert outcome == "succeeded", (
            "a failed slot-pool release must not replace the job's committed "
            f"outcome (outcome is {outcome!r})"
        )
        assert len(fake_backend.mark_succeeded_calls) == 1
        events = [log.get("event") for log in logs]
        assert "slot-pool-release-skipped-pool-closed" in events


# ── Slot-pool release unwind: no failure may escape it ────────────────


class _ConnDeadAtReleasePool:
    """Pool stand-in whose checked-out connection is dead by release time.

    Models the credential-rotation drain / teardown the release path was
    written for: ``close_pool_bounded``'s close-timeout branch terminates
    checked-out connections underneath an in-flight dispatch. Per the
    installed asyncpg source, a connection killed that way has
    ``_protocol`` nulled by ``Connection._abort`` (connection.py), so
    ``is_in_transaction()`` raises AttributeError; a proxy detached by the
    holder's ``_release_on_close`` raises InterfaceError from the
    ``PoolConnectionProxy`` method wrapper instead. Both surface at the
    release unwind, from the same call.
    """

    def __init__(self, is_in_transaction_error: BaseException) -> None:
        self.conn = _FakeSlotConn()
        self.release_calls: list[object] = []
        self._is_in_transaction_error = is_in_transaction_error

    def acquire(self, timeout: float | None = None) -> Coroutine[Any, Any, _FakeSlotConn]:
        return self._acquired()

    async def _acquired(self) -> _FakeSlotConn:
        return self.conn

    async def release(self, conn: object) -> None:
        self.release_calls.append(conn)


@pytest.mark.parametrize(
    "is_in_transaction_error",
    [
        AttributeError("'NoneType' object has no attribute 'is_in_transaction'"),
        asyncpg.InterfaceError(
            "cannot call Connection.is_in_transaction(): "
            "connection has been released back to the pool"
        ),
    ],
    ids=["terminated-conn-protocol-gone", "released-proxy"],
)
async def test_slot_pool_release_unwind_failure_preserves_job_outcome(
    is_in_transaction_error: BaseException,
) -> None:
    """No failure from the release unwind may replace the job's outcome.

    The closed-pool test pins this for the ``pool.release`` call; the
    ``is_in_transaction()`` probe that selects the terminate branch is
    part of the same unwind and runs on a connection the pool may have
    terminated or released underneath the dispatch. A committed job whose
    release unwind raises is reported by the drain loop as a dispatch
    failure - a job failure that never happened.
    """
    import structlog

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        pool = _ConnDeadAtReleasePool(is_in_transaction_error)
        pool.conn.is_in_transaction = (  # type: ignore[method-assign]  # Why: test fake; simulates the connection dying between acquire and release.
            lambda: (_ for _ in ()).throw(is_in_transaction_error)
        )
        fake_deps.slot_pool = pool  # type: ignore[assignment]  # Why: duck-typed pool stand-in, same as the release-failure test above.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with structlog.testing.capture_logs():
            outcome = await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        assert outcome == "succeeded", (
            "a release-unwind failure must not replace the job's committed "
            f"outcome (outcome is {outcome!r})"
        )
        assert len(fake_backend.mark_succeeded_calls) == 1


@pytest.mark.parametrize(
    ("is_in_transaction_error", "expected_event"),
    [
        (
            AttributeError("'NoneType' object has no attribute 'is_in_transaction'"),
            "slot-pool-release-skipped-conn-dead",
        ),
        (
            asyncpg.InterfaceError(
                "cannot call Connection.is_in_transaction(): "
                "connection has been released back to the pool"
            ),
            "slot-pool-release-skipped-pool-closed",
        ),
    ],
    ids=["terminated-conn-protocol-gone", "released-proxy"],
)
async def test_slot_pool_release_unwind_skip_is_logged_with_its_cause(
    is_in_transaction_error: BaseException,
    expected_event: str,
) -> None:
    """A release skipped because the connection died underneath the
    dispatch is named in the logs, per shape, with its cause.

    The outcome-preservation tests pin that the unwind survives these
    states; this pins that the skip is visible - a degraded outcome the
    bounded close owns, hidden behind the job's success, would leave an
    operator nothing to correlate a lost connection with.
    """
    import structlog

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        pool = _ConnDeadAtReleasePool(is_in_transaction_error)
        pool.conn.is_in_transaction = (  # type: ignore[method-assign]  # Why: test fake; simulates the connection dying between acquire and release.
            lambda: (_ for _ in ()).throw(is_in_transaction_error)
        )
        fake_deps.slot_pool = pool  # type: ignore[assignment]  # Why: duck-typed pool stand-in, same as the release-failure test above.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with structlog.testing.capture_logs() as logs:
            outcome = await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        assert outcome == "succeeded"
        matching = [log for log in logs if log.get("event") == expected_event]
        assert matching, (
            f"a release skipped because the connection died must be logged "
            f"({expected_event!r} absent; events seen: "
            f"{[log.get('event') for log in logs]})"
        )
        assert matching[0].get("error_class") == type(is_in_transaction_error).__name__
        assert matching[0].get("job_id") == str(job.id)


async def test_slot_pool_release_unwind_unexpected_error_stays_loud() -> None:
    """The release guard swallows exactly two failure families (a dead
    connection's AttributeError, a closed pool's InterfaceError).
    Anything else is a programming error and must propagate - replacing
    the committed outcome loudly - rather than being swallowed behind
    the job's success. A well-meaning widening to ``except Exception``
    would hide it; this pins the deliberate narrowness.
    """
    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        pool = _ConnDeadAtReleasePool(ValueError("connection list is corrupted"))
        pool.conn.is_in_transaction = (  # type: ignore[method-assign]  # Why: test fake; simulates a broken connection object, not an asyncpg failure state.
            lambda: (_ for _ in ()).throw(ValueError("connection list is corrupted"))
        )
        fake_deps.slot_pool = pool  # type: ignore[assignment]  # Why: duck-typed pool stand-in, same as the release-failure tests above.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with pytest.raises(ValueError, match="connection list is corrupted"):
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        # The job committed before the unwind raised - the loud failure
        # replaces the reported outcome, never the durable state.
        assert len(fake_backend.mark_succeeded_calls) == 1


class _ReleaseRecordingPool:
    """Pool stand-in that records every release, for asserting that a
    connection still inside its transaction is never handed back."""

    def __init__(self) -> None:
        self.conn = _FakeSlotConn()
        self.release_calls: list[object] = []

    def acquire(self, timeout: float | None = None) -> Coroutine[Any, Any, _FakeSlotConn]:
        return self._acquired()

    async def _acquired(self) -> _FakeSlotConn:
        return self.conn

    async def release(self, conn: object) -> None:
        self.release_calls.append(conn)


async def test_slot_pool_release_terminates_in_flight_transaction_instead_of_releasing() -> None:
    """A connection still inside its transaction at release time is
    terminated, never released back to the pool.

    Releasing would hand a mid-transaction connection to a sibling slot -
    the shared-connection defect class this whole machinery exists to
    close. The terminated connection rolls back server-side, the job row
    stays `running`, and lease expiry reclaims it: loud and retryable,
    never a false success handed to another consumer.
    """
    import structlog

    async with _ScopeStack() as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        pool = _ReleaseRecordingPool()
        pool.conn.is_in_transaction = lambda: True  # type: ignore[method-assign]  # Why: test fake; the transaction is in flight at release time.
        fake_deps.slot_pool = pool  # type: ignore[assignment]  # Why: duck-typed pool stand-in, same as the release-failure test above.
        actor_ref = _make_actor_ref(_noop_actor)
        job = make_job_row(payload={"value": 42})

        with structlog.testing.capture_logs() as logs:
            outcome = await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_NOW),
                active_jobs=fake_deps.active_jobs,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

        assert outcome == "succeeded"
        assert pool.release_calls == [], (
            "a connection still inside its transaction must never be released "
            "back to the pool - a sibling slot would acquire it mid-transaction"
        )
        assert pool.conn.terminated is True
        events = [log.get("event") for log in logs]
        assert "slot-conn-terminated-transaction-in-flight" in events


# ── Slot connections must carry the registered connection's setup ─────


class _RegisteredConn(asyncpg.Connection):
    """Stand-in for a user's own fully-configured registered connection.

    Subclassing ``asyncpg.Connection`` mirrors a custom ``connection_class``;
    the ``has_user_setup`` sentinel stands in for whatever per-connection
    setup the registration declared -- a ``set_type_codec`` registration, an
    ``init``/``setup`` callback, session configuration. It is applied by the
    declared hook (``_registered_conn_setup`` below): ``with_connection_init``
    applies it to the registered connection itself, and a correct per-slot
    path replays it onto slot connections.
    """

    def __new__(cls) -> "_RegisteredConn":
        # asyncpg.Connection.__init__ requires live protocol machinery that
        # a unit test has no way to supply; __new__ alone yields an instance
        # of the right type without running it.
        return object.__new__(cls)

    def __init__(self) -> None:
        # Deliberately does not call super().__init__() -- see __new__.
        # asyncpg.Connection.__del__ reads _aborted; set it so a GC'd
        # instance built this way does not raise from the finalizer.
        self._aborted = True


async def _registered_conn_setup(conn: asyncpg.Connection) -> None:
    """The registration's declared per-connection setup hook.

    Setting the attribute is the unit-test-observable stand-in for a codec
    registration or a session GUC, neither of which a live-free unit test
    can observe.
    """
    setattr(conn, "has_user_setup", True)  # noqa: B010  # Why: pyright strict rejects the attribute assignment on asyncpg.Connection (unknown attribute); setattr is the typed-boundary-safe form for the sentinel.


async def _registered_conn_factory() -> asyncpg.Connection:
    return _RegisteredConn()


class _BareSetupPool:
    """Slot-pool stand-in that hands out a connection opened fresh off the
    direct DSN: no codecs, no init hook, no connection_class, no
    server_settings -- what a plain ``asyncpg.create_pool(dsn=direct, ...)``
    produces.
    """

    def __init__(self) -> None:
        self.conn = _FakeSlotConn()

    def acquire(self, timeout: float | None = None) -> Coroutine[Any, Any, _FakeSlotConn]:
        return self._acquired()

    async def _acquired(self) -> _FakeSlotConn:
        return self.conn

    async def release(self, conn: object) -> None:
        return None


async def test_slot_connection_carries_registered_connection_setup() -> None:
    """When a LOOP-scope ``asyncpg.Connection`` registration declares its
    per-connection setup and the per-slot transaction pool is active, the
    connection the actor's DI resolves must still carry that setup.

    The per-slot pool exists so concurrent slots never interleave operations
    on one connection. A pool the worker bootstrap opens inherits the
    registration's declared init hook at connect time (the pool factory's
    ``init=``), but the dispatch path also accepts pools bootstrap did not
    open - injected straight onto the deps, as below. Connections such a
    pool hands out are bare: any type codec, ``init``/``setup`` callback,
    custom ``connection_class``, or session configuration the registration
    declared is absent from them, so an actor silently reads and writes
    through a connection that decodes values differently than the one the
    application configured. That is data corruption with no error raised,
    and it appears at any ``max_concurrency`` above one.

    Per-slot isolation and registered-connection setup are not in tension:
    dispatch replays the registration's declared hook onto the slot
    connection - exactly once per physical connection - rather than
    discarding it. (A raw ``register_value`` connection cannot declare a
    replayable hook - the driver seals per-connection state - so the
    registration here declares it through ``with_connection_init``; the raw
    channel's loudly-warned non-inheritance boundary is pinned in
    tests/test_slot_pool.py.)
    """
    observed_conn: object | None = None

    async def my_actor(
        payload: _Payload,
        ctx: JobContext[_Payload],
        conn: asyncpg.Connection,
    ) -> dict[str, object]:
        nonlocal observed_conn
        observed_conn = conn
        return {}

    registry = ProviderRegistry()
    registry.register_factory(
        asyncpg.Connection,
        Scope.LOOP,
        with_connection_init(_registered_conn_factory, _registered_conn_setup),
    )

    async with _ScopeStack(registry) as scopes:
        fake_backend = FakeBackend()
        fake_deps = _FakeWorkerDeps()
        # Activate the per-slot path, the shape bootstrap opens whenever a
        # LOOP-scope Connection is registered and max_concurrency > 1.
        fake_deps.slot_pool = _BareSetupPool()  # type: ignore[assignment]  # Why: duck-typed pool stand-in, same as the release tests above.
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
            actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the tests above.
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=SubJobEnqueuer(
                backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
            ),
        )

    assert observed_conn is not None
    assert getattr(observed_conn, "has_user_setup", False) is True, (
        "the actor received a slot connection that dropped the registered "
        "connection's setup (codecs, init hook, connection_class, "
        "server_settings, role)"
    )

"""The dispatch path hands the DI-registered ErrorReporter to the consumer.

The observability guide promises that a reporter registered on the
``ProviderRegistry`` (PROCESS or LOOP scope) is resolved at dispatch time
and invoked when a job reaches a terminal ``failed`` state. These tests
drive the production dispatch seam (``dispatch_one_job`` over the
in-memory backend twin) and observe the reporter's ``report()`` calls —
the only thing an operator wiring Sentry or a DLQ can see.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_job_id
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, JobRow, RetryKind
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import PayloadValidationError
from taskq.obs import ErrorReporter
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import dispatch_one_job

_START = datetime(2026, 1, 1, tzinfo=UTC)
_ACTOR = "reporting_actor"


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _ActorBoomError(RuntimeError):
    pass


async def _raising_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    raise _ActorBoomError("actor failed")


class _SpyReporter:
    """Records every ``report()`` call, the way a Sentry adapter would forward it."""

    def __init__(self) -> None:
        self.calls: list[tuple[JobRow, BaseException]] = []

    async def report(self, job: JobRow, exception: BaseException) -> None:
        self.calls.append((job, exception))


class _FakeWorkerDeps:
    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.worker_pool: asyncpg.Pool | None = None
        self.slot_pool: asyncpg.Pool | None = None
        self.slot_pool_connection_init: Any | None = None
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.settings.worker_group = "default"
        self.redis_client: Any | None = None
        self.progress_buffers: dict[Any, Any] = {}
        self.disowned_jobs: set[UUID] = set()


class _Scopes:
    """A bootstrapped PROCESS/THREAD/LOOP scope chain over *registry*."""

    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> _Scopes:
        self.registry.validate()
        containers: dict[Scope, Any] = {}
        resolver = make_resolver(self.registry, containers)
        self.process_scope = ProcessScope(resolver=resolver)
        self.thread_scope = ThreadScope(resolver=resolver)
        self.loop_scope = LoopScope(resolver=resolver)
        containers[Scope.PROCESS] = self.process_scope
        containers[Scope.THREAD] = self.thread_scope
        containers[Scope.LOOP] = self.loop_scope
        settings = WorkerSettings.load_from_dict(
            {"PG_DSN": "postgres://u:p@localhost:5432/db"},
        )
        await self.process_scope.bootstrap(self.registry, settings)
        await self.thread_scope.bootstrap(self.registry, self.process_scope)
        await self.loop_scope.bootstrap(self.registry, self.process_scope, self.thread_scope)
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


async def _running_job(
    backend: InMemoryBackend,
    *,
    retry_kind: RetryKind,
    max_attempts: int,
    payload: dict[str, object] | None = None,
) -> tuple[JobRow, Any]:
    backend.register_actor_config(actor=_ACTOR)
    args = EnqueueArgs(
        id=new_job_id(),
        actor=_ACTOR,
        queue="default",
        payload=payload if payload is not None else {},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only; the runner's own dispatch uses the same worker id.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(dispatched) == 1
    return dispatched[0], worker_id


async def _dispatch(scopes: _Scopes, backend: InMemoryBackend, job: JobRow, worker_id: Any) -> str:
    actor_ref: ActorRef[_Payload, None] = ActorRef(
        name=_ACTOR,
        queue="default",
        fn=_raising_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; the adapter is never read on the failure path.
        retry=RetryPolicy(jitter=0.0),
        result_ttl=None,
    )
    deps = _FakeWorkerDeps()
    return await dispatch_one_job(
        backend=backend,
        deps=deps,  # type: ignore[arg-type]  # Why: the established dispatch_one_job unit pattern — a namespace with the fields dispatch reads.
        job=job,
        worker_id=worker_id,
        registry=scopes.registry,
        process_scope=scopes.process_scope,
        thread_scope=scopes.thread_scope,
        loop_scope=scopes.loop_scope,
        actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: ActorRef[_Payload, None] is the runtime shape; pyright cannot widen the generic parameters.
        actor_config=actor_ref.config,
        clock=FakeClock(start=_START),
        active_jobs=deps.active_jobs,
        enqueuer=SubJobEnqueuer(backend=backend, loop_scope_resolved=None, worker_pool=None),
    )


async def test_process_scoped_reporter_receives_the_terminal_failure() -> None:
    """A PROCESS-scope reporter is called exactly once with the final row
    and the actor's exception when the job fails non-retryably."""
    reporter = _SpyReporter()
    registry = ProviderRegistry()
    registry.register_value(ErrorReporter, Scope.PROCESS, reporter)
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend, retry_kind="non_retryable", max_attempts=1)

    async with _Scopes(registry) as scopes:
        outcome = await _dispatch(scopes, backend, job, worker_id)

    assert outcome == "failed"
    assert len(reporter.calls) == 1, (
        "the registered ErrorReporter was never invoked for a terminal failure "
        f"(calls={reporter.calls!r}) — the reporter is not wired at dispatch"
    )
    reported_row, reported_exc = reporter.calls[0]
    assert reported_row.id == job.id
    assert reported_row.status == "failed"
    assert isinstance(reported_exc, _ActorBoomError)


async def test_loop_scoped_reporter_receives_the_terminal_failure() -> None:
    """The LOOP scope is the documented alternative for a reporter holding
    a loop-lifetime connection; it resolves the same way."""
    reporter = _SpyReporter()
    registry = ProviderRegistry()
    registry.register_value(ErrorReporter, Scope.LOOP, reporter)
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend, retry_kind="non_retryable", max_attempts=1)

    async with _Scopes(registry) as scopes:
        outcome = await _dispatch(scopes, backend, job, worker_id)

    assert outcome == "failed"
    assert [(row.id, type(exc)) for row, exc in reporter.calls] == [(job.id, _ActorBoomError)]


async def test_reporter_is_silent_while_retries_remain() -> None:
    """A retryable failure is not a terminal state: the reporter's contract
    is retries exhausted or a non-retryable error, so a first attempt of
    three that reschedules must not report."""
    reporter = _SpyReporter()
    registry = ProviderRegistry()
    registry.register_value(ErrorReporter, Scope.PROCESS, reporter)
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend, retry_kind="transient", max_attempts=3)

    async with _Scopes(registry) as scopes:
        outcome = await _dispatch(scopes, backend, job, worker_id)

    assert outcome == "scheduled"
    assert reporter.calls == []


async def test_no_registered_reporter_dispatches_normally() -> None:
    """Without a registration the dispatch path runs the same terminal
    write and reports the same outcome — the reporter is optional."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(backend, retry_kind="non_retryable", max_attempts=1)

    async with _Scopes(ProviderRegistry()) as scopes:
        outcome = await _dispatch(scopes, backend, job, worker_id)

    assert outcome == "failed"
    row = await backend.get(job.id)
    assert row is not None and row.status == "failed"


async def test_pre_actor_failure_reports_through_the_same_hook() -> None:
    """A job that fails before its actor runs (here: a payload the actor's
    model rejects) is as terminal as one that fails inside it; the
    dispatch path's own failure handler reports it too."""
    reporter = _SpyReporter()
    registry = ProviderRegistry()
    registry.register_value(ErrorReporter, Scope.PROCESS, reporter)
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    job, worker_id = await _running_job(
        backend, retry_kind="transient", max_attempts=3, payload={"unexpected": 1}
    )

    async with _Scopes(registry) as scopes:
        outcome = await _dispatch(scopes, backend, job, worker_id)

    assert outcome == "failed"
    assert [(row.id, type(exc)) for row, exc in reporter.calls] == [
        (job.id, PayloadValidationError)
    ]

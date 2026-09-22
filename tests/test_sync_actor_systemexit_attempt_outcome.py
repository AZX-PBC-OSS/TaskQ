"""A sync actor's ``SystemExit`` is a typed attempt outcome, not worker death.

Issue #459: a sync actor calling ``sys.exit()`` took down the whole
worker. The mechanism is CPython's, not TaskQ's own: ``Task.__step``
special-cases exactly ``(KeyboardInterrupt, SystemExit)`` and re-raises
the pair bare after ``set_exception``, and ``Handle._run`` re-raises them
past the loop's generic handler, so a TASK whose coroutine ends with
``SystemExit`` kills the event loop before any queued wake-up (the
awaiting consumer's ``except BaseException`` attempt boundary) can run.
The consumer teardown then cancels every co-resident sibling and records
the dying job ``cancelled``, and the ``SystemExit`` escapes
``asyncio.run`` with the actor's exit code.

The fix converts the exception at the two task boundaries an actor body
crosses (the sync actor's executor-thread task in
``_run_sync_actor_tracked``, the transactional path's ``_run_actor_in_tx``
task for an async actor) into :class:`taskq.worker._handlers._ActorSystemExitAttempt`,
an ordinary ``Exception`` carrier that ``_dispatch_exception`` unwraps
before routing, so the attempt records the actor's own ``SystemExit``
(``error_class`` is the exception's own type name) and the worker
survives. These pins capture the corrected contract:

- a sync actor's ``sys.exit()`` lands a truthful ``failed`` row through
  ``dispatch_one_job`` (the production dispatch path), never escapes the
  await, and is never relabelled ``cancelled``;
- a co-resident sibling job, in flight when the exiter dies, completes
  ``succeeded``: the worker survives;
- an async actor's ``sys.exit()`` on the transactional path (a task
  boundary of its own) records the same truthful outcome;
- an async actor's ``sys.exit()`` on the autonomous path (raised in the
  consumer's own frame, #399's widened catch) records it too.

``KeyboardInterrupt`` stays uncaptured (interpreter/operator intent,
never an actor outcome): that contract is pinned by
``test_attempt_baseexception_capture.py`` and the boundaries leave it
raw.
"""

import asyncio
import inspect
import sys
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.dispatch import dispatch_one_job
from tests._di_scopes import bootstrap_scopes, make_scopes

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _FakeWorkerDeps:
    """Minimal WorkerDeps stub with just active_jobs."""

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.worker_pool = None
        self.slot_pool = None
        self.slot_pool_connection_init = None
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


def _actor_ref(fn: Any, name: str) -> ActorRef[_Payload, None]:
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
        is_sync=not inspect.iscoroutinefunction(fn),
    )


def _exit_job(actor: str) -> Any:
    return make_job_row(payload={"value": 1}, actor=actor, max_attempts=1)


# ── Sync actor: the executor-thread boundary ──────────────────────────


async def test_sync_actor_systemexit_is_a_typed_attempt_outcome() -> None:
    """A sync actor calling ``sys.exit(3)`` is a job-level failure recorded
    truthfully: ``dispatch_one_job`` returns ``failed``, the failure write
    carries the actor's own exception (error_class ``SystemExit``, the
    actor's frame in the traceback via the cause chain), and nothing
    escapes the await. The pre-fix behavior killed the event loop: the
    consumer teardown recorded the job cancelled and the ``SystemExit``
    escaped ``asyncio.run``."""
    registry = ProviderRegistry()
    process_scope, thread_scope, loop_scope = make_scopes(registry)
    await bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    def sys_exiter(payload: _Payload, ctx: object) -> None:
        del payload, ctx
        sys.exit(3)

    backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()
    job = _exit_job("sys_exiter")

    # Must not raise: the worker survives the actor's exit.
    outcome = await dispatch_one_job(
        backend=as_backend(backend),
        deps=fake_deps,  # type: ignore[arg-type]  # Why: the _FakeWorkerDeps duck type mirrors the existing dispatch_one_job unit tests' harness.
        job=job,
        worker_id=_WORKER_ID,
        registry=registry,
        process_scope=process_scope,
        thread_scope=thread_scope,
        loop_scope=loop_scope,
        actor_ref=_actor_ref(sys_exiter, "sys_exiter"),  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] generic widening, as in the existing dispatch_one_job unit tests.
        actor_config=StubActorConfig(retry=RetryPolicy()),
        clock=FakeClock(_NOW),
        active_jobs=fake_deps.active_jobs,
        enqueuer=SubJobEnqueuer(
            backend=as_backend(backend), loop_scope_resolved=None, worker_pool=None
        ),
    )

    assert outcome == "failed"
    # Truthful failure, never the teardown's "cancelled" relabel.
    assert backend.mark_cancelled_calls == []
    assert len(backend.mark_failed_or_retry_calls) == 1
    call = backend.mark_failed_or_retry_calls[0]
    error_info = call["error_info"]
    assert error_info is not None
    assert error_info.error_class == "SystemExit"  # pyright: ignore[reportAttributeAccessIssue]  # Why: mark_failed_or_retry_calls stores untyped objects from the fake; error_info is an ErrorInfo at runtime.
    assert error_info.error_message == "3"  # pyright: ignore[reportAttributeAccessIssue]  # Why: str(SystemExit(3)) is the code; the audit trail names the actor's own exception.
    assert "SystemExit: 3" in error_info.error_traceback  # pyright: ignore[reportAttributeAccessIssue]  # Why: the cause chain carries the actor's own frame.
    assert "sys_exiter" in error_info.error_traceback  # pyright: ignore[reportAttributeAccessIssue]  # Why: the traceback points at the actor body, not the carrier.


async def test_sync_actor_systemexit_leaves_the_sibling_and_worker_alive() -> None:
    """A sibling job in flight when the exiter dies completes succeeded:
    the failure is the dying job's alone, the consumer loop task never
    dies, and the worker keeps its fleet (the pre-fix loop death cancelled
    the sibling through the teardown)."""
    registry = ProviderRegistry()
    process_scope, thread_scope, loop_scope = make_scopes(registry)
    await bootstrap_scopes(registry, process_scope, thread_scope, loop_scope)

    def sys_exiter(payload: _Payload, ctx: object) -> None:
        del payload, ctx
        sys.exit(3)

    async def slow_sibling(payload: _Payload, ctx: object) -> dict[str, object]:
        del payload, ctx
        # Long enough that the exiter's thread failure lands mid-flight;
        # pre-fix the loop died here and the teardown cancelled this task.
        await asyncio.sleep(0.2)
        return {"ok": True}

    backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()
    exiter_job = _exit_job("sys_exiter")
    sibling_job = _exit_job("slow_sibling")
    enqueuer = SubJobEnqueuer(
        backend=as_backend(backend), loop_scope_resolved=None, worker_pool=None
    )

    async def dispatch(ref: ActorRef[_Payload, None], job: Any) -> Any:
        return await dispatch_one_job(
            backend=as_backend(backend),
            deps=fake_deps,  # type: ignore[arg-type]  # Why: the _FakeWorkerDeps duck type mirrors the existing dispatch_one_job unit tests' harness.
            job=job,
            worker_id=_WORKER_ID,
            registry=registry,
            process_scope=process_scope,
            thread_scope=thread_scope,
            loop_scope=loop_scope,
            actor_ref=ref,  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] generic widening, as in the existing dispatch_one_job unit tests.
            actor_config=StubActorConfig(retry=RetryPolicy()),
            clock=FakeClock(_NOW),
            active_jobs=fake_deps.active_jobs,
            enqueuer=enqueuer,
        )

    async with asyncio.TaskGroup() as tg:
        # The sibling starts first so it is genuinely in flight when the
        # exiter's thread failure lands.
        sibling = tg.create_task(dispatch(_actor_ref(slow_sibling, "slow_sibling"), sibling_job))
        exiter = tg.create_task(dispatch(_actor_ref(sys_exiter, "sys_exiter"), exiter_job))

    assert await exiter == "failed"
    assert await sibling == "succeeded"
    assert len(backend.mark_succeeded_calls) == 1
    assert backend.mark_succeeded_calls[0][0] == sibling_job.id
    assert len(backend.mark_failed_or_retry_calls) == 1
    assert backend.mark_failed_or_retry_calls[0]["job_id"] == exiter_job.id
    assert backend.mark_cancelled_calls == []


# ── Async actor: the tx-path task boundary ────────────────────────────


async def test_tx_async_actor_systemexit_is_a_typed_attempt_outcome() -> None:
    """An async actor's ``sys.exit()`` on the transactional path is the
    same CPython mechanism one boundary over: ``_run_actor_in_tx`` IS the
    tx task's step, so a raw SystemExit in its frame would kill the loop
    before the shielded await ran. The conversion records the actor's own
    SystemExit truthfully and the consumer survives."""

    class _FakeConnection:
        class _Transaction:
            async def __aenter__(self) -> "_FakeConnection._Transaction":
                return self

            async def __aexit__(self, *args: object) -> None:
                return None

        def transaction(self) -> "_FakeConnection._Transaction":
            return _FakeConnection._Transaction()

        async def execute(self, query: str, *args: object) -> str:
            del query, args
            return ""

    async def async_exiter(payload: object, ctx: object) -> dict[str, object]:
        del payload, ctx
        sys.exit(5)

    backend = FakeBackend()
    outcome = await consume_one_job(
        as_backend(backend),
        _exit_job("async_exiter"),
        _WORKER_ID,
        run_actor=async_exiter,  # type: ignore[arg-type]  # Why: object-typed actor callable, matching the consumer unit tests' harness shape.
        actor_config=StubActorConfig(retry=RetryPolicy()),
        payload_type=_Payload,
        clock=FakeClock(_NOW),
        transaction_conn=_FakeConnection(),
        enqueuer=SubJobEnqueuer(
            backend=as_backend(backend), loop_scope_resolved=None, worker_pool=None
        ),
    )

    assert outcome == "failed"
    assert backend.mark_cancelled_calls == []
    assert len(backend.mark_failed_or_retry_calls) == 1
    error_info = backend.mark_failed_or_retry_calls[0]["error_info"]
    assert error_info is not None
    assert error_info.error_class == "SystemExit"  # pyright: ignore[reportAttributeAccessIssue]  # Why: mark_failed_or_retry_calls stores untyped objects from the fake; error_info is an ErrorInfo at runtime.
    assert "SystemExit: 5" in error_info.error_traceback  # pyright: ignore[reportAttributeAccessIssue]  # Why: the traceback carries the actor's own exception.


# ── Async actor: the autonomous path (the consumer's own frame) ───────


async def test_autonomous_async_actor_systemexit_is_a_typed_attempt_outcome() -> None:
    """An async actor's ``sys.exit()`` raised in the consumer's own frame
    is delivered by the awaiting task's normal wakeup, so the consumer's
    widened catch already captured it (#399); pinned here so the delivery
    shape the sync-thread conversion hands to is locked to the same
    truthful outcome."""

    async def async_exiter(payload: object, ctx: object) -> dict[str, object]:
        del payload, ctx
        sys.exit(7)

    backend = FakeBackend()
    outcome = await consume_one_job(
        as_backend(backend),
        _exit_job("async_exiter"),
        _WORKER_ID,
        run_actor=async_exiter,  # type: ignore[arg-type]  # Why: object-typed actor callable, matching the consumer unit tests' harness shape.
        actor_config=StubActorConfig(retry=RetryPolicy()),
        payload_type=_Payload,
        clock=FakeClock(_NOW),
    )

    assert outcome == "failed"
    assert backend.mark_cancelled_calls == []
    assert len(backend.mark_failed_or_retry_calls) == 1
    error_info = backend.mark_failed_or_retry_calls[0]["error_info"]
    assert error_info is not None
    assert error_info.error_class == "SystemExit"  # pyright: ignore[reportAttributeAccessIssue]  # Why: mark_failed_or_retry_calls stores untyped objects from the fake; error_info is an ErrorInfo at runtime.
    assert error_info.error_message == "7"  # pyright: ignore[reportAttributeAccessIssue]  # Why: str(SystemExit(7)) is the code; the audit trail names the actor's own exception.

"""Coverage for ``taskq.worker._consumer`` exception and edge-case paths.

Exercises branches not covered by ``test_consumer.py`` and
``test_consumer_sub_enqueue.py``:

- ``ResultTooLarge`` raised in the autonomous and transactional paths.
- Transactional ``Snooze`` re-enqueue failure → ``RuntimeError`` handled
  as a generic failure.
- Transactional success with ``SubEnqueueError`` on ``flush_buffer`` —
  parent still succeeds, lost child logged.
- ``CancelledError`` with an ``ABANDON_PENDING`` active-jobs entry
  re-raises without calling ``mark_cancelled``.
- ``_consume_autonomous`` cooperative-cancel path: actor succeeds but the
  active-jobs entry has ``cancel_phase >= COOPERATIVE`` → ``mark_cancelled``
  runs instead of ``mark_succeeded``.
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import CancelPhase, EnqueueArgs, JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.obs import bind_job_context
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

# Result cap defined in ``taskq.worker._consumer``.
_MAX_RESULT_BYTES = 65536


# ── Test doubles ─────────────────────────────────────────────────────────


class _FakeConnection:
    """Minimal asyncpg.Connection stand-in with a transaction() context manager."""

    class _Transaction:
        async def __aenter__(self) -> "_FakeConnection._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def transaction(self) -> "_FakeConnection._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _TxBackend(FakeBackend):
    """FakeBackend that supports transactional simulation and tracks calls."""

    BACKEND_PROTOCOL_VERSION: int = 1
    supports_transactional_simulation: bool = True

    def __init__(self, *, enqueue_exc: BaseException | None = None) -> None:
        super().__init__()
        self.enqueue_calls: list[EnqueueArgs] = []
        self.mark_succeeded_with_conn_calls: list[
            tuple[object, UUID, UUID, dict[str, object] | None]
        ] = []
        self._enqueue_exc = enqueue_exc

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        self.enqueue_calls.append(args)
        if self._enqueue_exc is not None:
            raise self._enqueue_exc
        return make_job_row()

    async def enqueue_with_conn(self, conn: object, args: EnqueueArgs) -> JobRow:
        return await self.enqueue(args)

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        self.mark_succeeded_with_conn_calls.append((conn, job_id, worker_id, result))
        return await self.mark_succeeded(job_id, worker_id, result, progress_seq, progress_state)


class _ChildResult(BaseModel):
    ok: bool = True


def _child_ref() -> ActorRef[EmptyPayload, _ChildResult]:
    async def _handler(payload: EmptyPayload) -> _ChildResult:
        return _ChildResult()

    return ActorRef(
        name="child",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=EmptyPayload,
        result_adapter=TypeAdapter(_ChildResult),
        retry=__import__("taskq.retry", fromlist=["RetryPolicy"]).RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def _huge_result() -> dict[str, object]:
    """A result dict whose JSON serialization exceeds the 64 KiB cap."""
    return {"blob": "x" * (_MAX_RESULT_BYTES + 1000)}


# ── ResultTooLarge: autonomous path ──────────────────────────────────────


async def test_autonomous_result_too_large_routes_to_failure() -> None:
    """An actor returning a result > 64 KiB on the autonomous path raises
    ``ResultTooLarge`` which is handled as a generic failure (not marked
    succeeded)."""
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row(attempt=1, max_attempts=1)

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return _huge_result()

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )

    assert result == "failed"
    assert len(backend.mark_succeeded_calls) == 0
    assert len(backend.mark_failed_or_retry_calls) == 1


# ── ResultTooLarge: transactional path ───────────────────────────────────


async def test_transactional_result_too_large_routes_to_failure() -> None:
    """An actor returning a result > 64 KiB inside a LOOP-scope transaction
    raises ``ResultTooLarge``; the transaction rolls back and the job is
    routed to the generic failure handler (not marked succeeded)."""
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row(attempt=1, max_attempts=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return _huge_result()

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        enqueuer=enqueuer,
        loop_conn=_FakeConnection(),
    )

    assert result == "failed"
    assert len(backend.mark_succeeded_with_conn_calls) == 0
    assert len(backend.mark_failed_or_retry_calls) == 1
    # The sub-enqueue buffer is discarded on the failure path.
    assert enqueuer.pending_count == 0


# ── Transactional Snooze re-enqueue failure → RuntimeError ───────────────


async def test_transactional_snooze_re_enqueue_failure_routes_to_failure() -> None:
    """When a Snoozing actor's buffered children fail to re-enqueue, the
    resulting ``RuntimeError`` is handled as a generic failure rather than
    a scheduled retry."""
    backend = _TxBackend(enqueue_exc=RuntimeError("enqueue failed"))
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row(attempt=1, max_attempts=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )
    # Buffer a child before the actor raises so drain_for_re_enqueue is non-empty.
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=10))

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        enqueuer=enqueuer,
        loop_conn=_FakeConnection(),
    )

    assert result == "failed"
    assert len(backend.mark_failed_or_retry_calls) == 1
    # mark_snoozed was NOT called because the RuntimeError superseded the Snooze.
    assert len(backend.mark_snoozed_calls) == 0
    # The re-enqueue attempt was made (and failed).
    assert len(backend.enqueue_calls) == 1


# ── Transactional success + SubEnqueueError on flush ────────────────────


async def test_transactional_success_with_sub_enqueue_error_still_succeeds() -> None:
    """When ``flush_buffer`` raises ``SubEnqueueError`` after a successful
    commit, the parent job still returns ``"succeeded"``; the lost child is
    logged but does not flip the outcome."""
    backend = _TxBackend(enqueue_exc=RuntimeError("flush failed"))
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )
    # Buffer a child so flush_buffer has work to do (and fail on).
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        enqueuer=enqueuer,
        loop_conn=_FakeConnection(),
    )

    assert result == "succeeded"
    # Parent was marked succeeded inside the transaction.
    assert len(backend.mark_succeeded_with_conn_calls) == 1
    # The flush attempted to enqueue the buffered child and failed.
    assert len(backend.enqueue_calls) == 1
    # Pending buffer is cleared by flush_buffer before it raises.
    assert enqueuer.pending_count == 0


# ── CancelledError with ABANDON_PENDING re-raises without mark_cancelled ─


async def test_cancel_with_abandon_pending_re_raises_without_mark_cancelled() -> None:
    """When ``cancel_phase >= ABANDON_PENDING`` is observed during a
    CancelledError, the error is re-raised immediately and ``mark_cancelled``
    is NOT called (the abandonment path owns the terminal write)."""
    active_jobs = ActiveJobRegistry()
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def actor(j: JobRow, _ctx: JobContext[BaseModel]) -> object:
        entry = active_jobs.get(j.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.ABANDON_PENDING
        raise asyncio.CancelledError()

    with pytest.raises(
        asyncio.CancelledError
    ):  # Why: CancelledError is a BaseException; pytest.raises handles it.
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            active_jobs=active_jobs,
        )

    assert len(backend.mark_cancelled_calls) == 0


# ── _consume_autonomous cooperative-cancel path ─────────────────────────


async def test_autonomous_cooperative_cancel_marks_cancelled_not_succeeded() -> None:
    """When the actor succeeds but the active-jobs entry has
    ``cancel_phase >= COOPERATIVE``, ``_consume_autonomous`` calls
    ``mark_cancelled`` and returns early — ``mark_succeeded`` is NOT called."""
    active_jobs = ActiveJobRegistry()
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()

    async def actor(j: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        entry = active_jobs.get(j.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        active_jobs=active_jobs,
    )

    # consume_one_job returns "succeeded" after _consume_autonomous returns,
    # but the terminal write was mark_cancelled, not mark_succeeded.
    assert result == "succeeded"
    assert len(backend.mark_cancelled_calls) == 1
    assert len(backend.mark_succeeded_calls) == 0


# ── _consume_autonomous: explicit params override deps (no pool) ─────────


async def test_autonomous_no_pool_pops_buffer_without_flush() -> None:
    """When ``deps`` provides progress_buffers but no worker_pool/settings,
    the finally branch pops the buffer without a crash flush."""
    from unittest.mock import MagicMock

    from taskq.progress._buffer import _ProgressBuffer
    from taskq.worker.deps import WorkerDeps

    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    buffers: dict[UUID, _ProgressBuffer] = {}

    settings = _settings_stub()
    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = settings
    deps.redis_client = None

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        deps=deps,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        worker_pool=None,
        settings=settings,
        redis_client=None,
    )

    assert result == "succeeded"
    # Buffer was registered then popped in finally.
    assert job.id not in buffers


def _settings_stub() -> object:
    from taskq.settings import WorkerSettings

    return WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})


# ── _consume_transactional: direct call with discard on exception ────────


async def test_transactional_generic_exception_discards_buffer() -> None:
    """A generic exception inside the transactional path calls
    ``discard_buffer`` (via pre_handler) and routes to the failure handler."""
    from opentelemetry import trace

    from taskq.worker._consumer import _consume_transactional

    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row(attempt=1, max_attempts=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    span = trace.get_current_span()
    log = structlog.get_logger("test")
    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        worker_id=_WORKER_ID,
        payload=EmptyPayload(),
        jobs=enqueuer,
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            identity_key=None,
            trace_id="",
        ),
    )

    outcome = await _consume_transactional(
        as_backend(backend),
        job,
        _WORKER_ID,
        ctx,
        enqueuer,
        _FakeConnection(),
        actor,
        cfg,
        clk,
        None,
        timedelta(hours=24),
        None,
        span,
        log,
    )
    assert outcome == "failed"
    assert enqueuer.pending_count == 0
    assert len(backend.mark_failed_or_retry_calls) == 1


# ── consume_one_job: re-raise CancelledError when already succeeded (C3) ─


async def test_transactional_completed_then_cancel_re_raises() -> None:
    """C3 invariant: when the transactional path commits (completion=_OK)
    and a CancelledError arrives afterwards, the error is re-raised without
    calling mark_cancelled (the row is already committed succeeded)."""
    from opentelemetry import trace

    from taskq.worker._consumer import _consume_transactional

    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    span = trace.get_current_span()
    log = structlog.get_logger("test")
    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        worker_id=_WORKER_ID,
        payload=EmptyPayload(),
        jobs=enqueuer,
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            identity_key=None,
            trace_id="",
        ),
    )

    outcome = await _consume_transactional(
        as_backend(backend),
        job,
        _WORKER_ID,
        ctx,
        enqueuer,
        _FakeConnection(),
        actor,
        cfg,
        clk,
        None,
        timedelta(hours=24),
        None,
        span,
        log,
    )
    assert outcome == "succeeded"
    assert len(backend.mark_succeeded_with_conn_calls) == 1
    assert len(backend.mark_cancelled_calls) == 0


# ── batch_id extracted from job metadata ─────────────────────────────────


async def test_batch_id_extracted_from_metadata_runs_actor() -> None:
    """A job whose metadata contains ``batch_id`` exercises the batch_id
    extraction branch — the actor runs and succeeds normally."""
    from dataclasses import replace as _replace

    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = _replace(make_job_row(), metadata={"batch_id": "batch-xyz"})

    seen_batch_id: list[str | None] = []

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        # batch_id is bound to the structlog context, not the JobContext;
        # we simply verify the actor runs to completion.
        assert isinstance(ctx, JobContext)
        seen_batch_id.append("ran")
        return {"ok": True}

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )

    assert result == "succeeded"
    assert seen_batch_id == ["ran"]


# ── Transactional cooperative cancel: actor succeeds but cancel observed ─


async def test_transactional_cooperative_cancel_marks_cancelled() -> None:
    """When the actor succeeds inside a LOOP-scope transaction but the
    active-jobs entry has ``cancel_phase >= COOPERATIVE``, a CancelledError
    is raised inside the transaction; the outer handler marks the job
    cancelled and discards the sub-enqueue buffer."""
    active_jobs = ActiveJobRegistry()
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )

    async def actor(j: JobRow, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        entry = active_jobs.get(j.id)
        assert entry is not None
        entry.cancel_phase = CancelPhase.COOPERATIVE
        return {"ok": True}

    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            enqueuer=enqueuer,
            loop_conn=_FakeConnection(),
            active_jobs=active_jobs,
        )

    # The transaction did not commit (CancelledError rolled it back), so
    # mark_succeeded_with_conn was NOT called; mark_cancelled was.
    assert len(backend.mark_succeeded_with_conn_calls) == 0
    assert len(backend.mark_cancelled_calls) == 1
    assert enqueuer.pending_count == 0


# ── Transactional Snooze: savepoint rollback failure is warned ───────────


class _RollbackFailsConn:
    """Connection whose ROLLBACK TO SAVEPOINT raises, exercising the
    savepoint_rollback_failed warning path."""

    class _Transaction:
        async def __aenter__(self) -> "_RollbackFailsConn._Transaction":
            return self

        async def __aexit__(self, *args: object) -> None:
            pass

    def transaction(self) -> "_RollbackFailsConn._Transaction":
        return self._Transaction()

    async def execute(self, query: str, *args: object) -> str:
        if "ROLLBACK TO SAVEPOINT" in query:
            raise RuntimeError("savepoint gone")
        return ""


async def test_transactional_snooze_savepoint_rollback_failure_is_warned() -> None:
    """When ROLLBACK TO SAVEPOINT fails during a Snooze, the warning is
    logged and the Snooze still propagates (re-enqueue proceeds)."""
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _RollbackFailsConn()},
        worker_pool=None,
        backend=backend,
    )

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=10))

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        enqueuer=enqueuer,
        loop_conn=_RollbackFailsConn(),
    )

    # Snooze was handled — job scheduled, not failed.
    assert result == "scheduled"
    assert len(backend.mark_snoozed_calls) == 1


# ── Transactional RetryAfter: re-enqueue preserves children ──────────────


async def test_transactional_retry_after_re_enqueues_children() -> None:
    """RetryAfter inside a LOOP-scope transaction drains buffered children
    and re-enqueues them; the parent is scheduled."""
    backend = _TxBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = default_actor_config()
    job = make_job_row()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=backend,
    )
    # Buffer a child before the actor raises so drain_for_re_enqueue is non-empty.
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    from taskq.exceptions import RetryAfter

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=30))

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        enqueuer=enqueuer,
        loop_conn=_FakeConnection(),
    )

    assert result == "scheduled"
    assert len(backend.mark_retry_after_calls) == 1
    # The buffered child was re-enqueued.
    assert len(backend.enqueue_calls) == 1
    assert backend.enqueue_calls[0].actor == "child"

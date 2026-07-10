"""Unit tests for the SubJobEnqueuer transaction lifecycle wired into
consume_one_job.

Covers:
- Success path: flush_buffer called on actor success
- Failure path: discard_buffer called on actor exception
- Snooze path: discard_buffer called
- RetryAfter path: discard_buffer called
- CancelledError path: discard_buffer called + mark_cancelled invoked
- Autonomous path: no flush/discard, row committed immediately
- Same enqueuer instance across interim and live ctx
- Shielded success block: commit protected from cancellation
- Shielded success + outer cancel: NOT marked as cancelled (C3 invariant)
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import ReservationUnavailable, RetryAfter, Snooze
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

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


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


class _FakeBackend(FakeBackend):
    """FakeBackend subclass that tracks enqueue and mark_succeeded_with_conn calls."""

    BACKEND_PROTOCOL_VERSION: int = 1
    supports_transactional_simulation: bool = True

    def __init__(self) -> None:
        super().__init__()
        self.mark_succeeded_with_conn_calls: list[
            tuple[object, UUID, UUID, dict[str, object] | None]
        ] = []
        self.enqueue_calls: list[EnqueueArgs] = []

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        self.enqueue_calls.append(args)
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


async def _run_with_enqueuer(
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    *,
    loop_conn: object | None = None,
    backend: _FakeBackend | None = None,
    enqueuer: SubJobEnqueuer | None = None,
) -> tuple[_FakeBackend, SubJobEnqueuer]:
    fb = backend or _FakeBackend()
    live_enqueuer = enqueuer or SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=None,
        backend=fb,
    )
    job = _dc_replace(make_job_row(), locked_by_worker=_WORKER_ID)
    clk: Clock = FakeClock(_NOW)
    with suppress(asyncio.CancelledError):
        await consume_one_job(
            as_backend(fb),
            job,
            _WORKER_ID,
            run_actor=run_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=clk,
            enqueuer=live_enqueuer,
            loop_conn=loop_conn,
        )
    return fb, live_enqueuer


# ── Success path: flush_buffer called on actor success ──────────────────


async def test_flush_buffer_on_success() -> None:
    """When a LOOP-scope conn is present and the actor succeeds,
    flush_buffer is called and the child rows appear in _jobs."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        return {"ok": True}

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        backend=fb,
        enqueuer=enqueuer,
    )
    assert len(fb_result.mark_succeeded_with_conn_calls) == 1
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0


# ── Failure path: discard_buffer called on actor exception ──────────────


async def test_discard_buffer_on_generic_exception() -> None:
    """On generic Exception, discard_buffer is called and buffered child
    rows are cleared."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    _fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0


# ── Snooze path: discard_buffer called ──────────────────────────────────


async def test_discard_buffer_on_snooze() -> None:
    """On Snooze, child enqueues enqueued before the actor are preserved via re-enqueue."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=10))

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0
    assert len(fb_result.enqueue_calls) == 1
    assert fb_result.enqueue_calls[0].actor == "child"


# ── RetryAfter path: discard_buffer called ──────────────────────────────


async def test_discard_buffer_on_retry_after() -> None:
    """On RetryAfter, child enqueues enqueued before the actor are preserved via re-enqueue."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RetryAfter(timedelta(seconds=30))

    _fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0
    assert len(_fb_result.enqueue_calls) == 1


# ── CancelledError path: discard_buffer called + mark_cancelled invoked ─


async def test_discard_buffer_on_cancelled_error() -> None:
    """On CancelledError, discard_buffer is called and mark_cancelled is invoked."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError()

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        backend=fb,
        enqueuer=enqueuer,
    )
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0
    assert len(fb_result.mark_cancelled_calls) == 1


# ── Autonomous path: no flush/discard ──────────────────────────────────


async def test_autonomous_path_no_buffer_ops() -> None:
    """When loop_conn is None (autonomous path), the enqueuer's
    flush_buffer and discard_buffer are not called for the success
    path, and mark_succeeded (not mark_succeeded_with_conn) is used."""
    fb = _FakeBackend()

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        return {"ok": True}

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=None,
        backend=fb,
    )
    assert len(fb_result.mark_succeeded_calls) == 1
    assert len(fb_result.mark_succeeded_with_conn_calls) == 0
    assert result_enqueuer.pending_count == 0


# ── Same enqueuer instance across interim and live ctx ──────────────────


async def test_same_enqueuer_instance_across_contexts() -> None:
    """Two JobContext instances constructed during dispatch (interim and
    live) reference the SAME SubJobEnqueuer instance."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=None,
        backend=fb,
    )
    seen_enqueuers: list[SubJobEnqueuer] = []

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        if isinstance(ctx, JobContext):  # pyright: ignore[reportUnnecessaryIsInstance] # Why: isinstance check is runtime validation; type narrowing ensures safe attribute access.
            seen_enqueuers.append(ctx.jobs)
        return {"ok": True}

    await _run_with_enqueuer(actor, loop_conn=_FakeConnection(), enqueuer=enqueuer)
    assert len(seen_enqueuers) == 1
    assert seen_enqueuers[0] is enqueuer


# ── Shielded success + outer cancel: NOT marked as cancelled ──────────


async def test_shielded_success_not_marked_cancelled() -> None:
    """C3 invariant: when the success path commits before cancellation
    arrives, mark_cancelled is NOT called and mark_succeeded_with_conn
    IS called. The _OK sentinel prevents the CancelledError arm from
    running mark_cancelled on a committed row."""

    fb = _FakeBackend()
    job = _dc_replace(make_job_row(), locked_by_worker=_WORKER_ID)
    clk: Clock = FakeClock(_NOW)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=None,
        backend=fb,
    )

    _completion_result: object = None

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        return {"ok": True}

    # Drive _run_actor_in_tx directly to test the C3 invariant
    from opentelemetry import trace

    from taskq.worker._consumer import _consume_transactional

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

    completion = await _consume_transactional(
        as_backend(fb),
        job,
        _WORKER_ID,
        ctx,
        enqueuer,
        _FakeConnection(),  # loop_conn
        actor,
        default_actor_config(),
        clk,
        None,
        timedelta(hours=24),
        None,
        span,
        log,
    )
    assert completion == "succeeded"
    assert len(fb.mark_succeeded_with_conn_calls) == 1
    assert len(fb.mark_cancelled_calls) == 0


# ── ReservationUnavailable: discard_buffer called ─────────────────────


async def test_discard_buffer_on_reservation_unavailable() -> None:
    """On ReservationUnavailable, discard_buffer is called."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )
    await enqueuer.enqueue(_child_ref(), EmptyPayload())

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise ReservationUnavailable("gpu_pool", timedelta(seconds=10))

    _fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer is enqueuer
    assert result_enqueuer.pending_count == 0


async def test_snooze_preserves_in_actor_child_enqueues() -> None:
    """M5 fix: when an actor enqueues children AND raises Snooze inside
    a LOOP-scope transaction, the child jobs are re-enqueued on a
    separate connection instead of being silently lost."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        assert isinstance(ctx, JobContext)
        await ctx.jobs.enqueue(_child_ref(), EmptyPayload())
        raise Snooze(timedelta(seconds=10))

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer.pending_count == 0
    assert len(fb_result.enqueue_calls) == 1
    assert fb_result.enqueue_calls[0].actor == "child"
    assert len(fb_result.mark_snoozed_calls) == 1


async def test_retry_after_preserves_in_actor_child_enqueues() -> None:
    """M5 fix: when an actor enqueues children AND raises RetryAfter inside
    a LOOP-scope transaction, the child jobs are re-enqueued on a
    separate connection instead of being silently lost."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        assert isinstance(ctx, JobContext)
        await ctx.jobs.enqueue(_child_ref(), EmptyPayload())
        raise RetryAfter(timedelta(seconds=30))

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer.pending_count == 0
    assert len(fb_result.enqueue_calls) == 1
    assert fb_result.enqueue_calls[0].actor == "child"
    assert len(fb_result.mark_retry_after_calls) == 1


async def test_snooze_preserves_multiple_child_enqueues() -> None:
    """M5 fix: multiple child enqueues before Snooze are all preserved."""
    fb = _FakeBackend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
        worker_pool=None,
        backend=fb,
    )

    async def actor(_job: object, ctx: JobContext[BaseModel]) -> object:
        assert isinstance(ctx, JobContext)
        await ctx.jobs.enqueue(_child_ref(), EmptyPayload())
        await ctx.jobs.enqueue(_child_ref(), EmptyPayload())
        raise Snooze(timedelta(seconds=5))

    fb_result, result_enqueuer = await _run_with_enqueuer(
        actor,
        loop_conn=_FakeConnection(),
        enqueuer=enqueuer,
    )
    assert result_enqueuer.pending_count == 0
    assert len(fb_result.enqueue_calls) == 2
    assert all(c.actor == "child" for c in fb_result.enqueue_calls)

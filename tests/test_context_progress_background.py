"""Unit tests for JobContext.progress()'s fire-and-forget background publish.

``progress()`` schedules its Redis publish via ``asyncio.create_task(...)``
and returns without awaiting it, tracking the task in a shared,
worker-lifetime set (``_pending_publish_tasks``, sourced from
``WorkerDeps.pending_publish_tasks`` in production) so the task isn't
garbage-collected mid-flight — asyncio only holds a weak reference to
scheduled tasks, so something must hold a strong one until completion.

When no tracking set is available (``_pending_publish_tasks is None`` —
only possible when a caller constructs ``JobContext`` directly rather than
going through the worker consumer, which always wires
``deps.pending_publish_tasks``), ``progress()`` deliberately falls back to
awaiting the publish inline rather than risking that documented
garbage-collection pitfall. This is intentional, not a partial
implementation — see ``test_falls_back_to_blocking_without_a_tracking_set``.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
import structlog

from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload


def _make_ctx(
    *,
    redis_client: object,
    pending_publish_tasks: set["asyncio.Task[None]"] | None = None,
) -> tuple[JobContext[PassthroughPayload], _ProgressBuffer]:
    from taskq._ids import new_job_id, new_uuid

    job_id = new_job_id()
    worker_id = new_uuid()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers = {job_id: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=worker_id,
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=redis_client,  # type: ignore[arg-type]
        _worker_settings=settings,
        _pending_publish_tasks=pending_publish_tasks,
    )
    return ctx, buf


def _make_hanging_redis_client() -> AsyncMock:
    """A fake Redis client whose .publish() never resolves within a test's lifetime."""
    client = AsyncMock()

    async def _hang(*args: object, **kwargs: object) -> int:
        await asyncio.sleep(3600)
        return 1

    client.publish.side_effect = _hang
    return client


# ── Buffer mutation is synchronous regardless of publish latency ───────


async def test_buffer_mutated_before_progress_returns_even_with_slow_redis() -> None:
    """The in-memory buffer (pending_state, pending_seq_delta) is updated
    synchronously inside progress() before the Redis publish is scheduled.
    """
    redis_client = _make_hanging_redis_client()
    pending: set[asyncio.Task[None]] = set()
    ctx, buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    await ctx.progress(step=1)

    assert buf.pending_seq_delta == 1
    assert buf.pending_state["step"] == 1
    assert buf.dirty is True

    for task in pending:
        task.cancel()
    for task in list(pending):
        with pytest.raises(asyncio.CancelledError):
            await task


# ── Fire-and-forget scheduling ──────────────────────────────────────────


async def test_progress_returns_without_blocking_on_slow_redis() -> None:
    """With a tracking set available, progress() returns promptly even
    when the Redis publish hangs — it schedules the publish as a
    background task instead of awaiting it."""
    redis_client = _make_hanging_redis_client()
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    async with asyncio.timeout(0.2):
        await ctx.progress(step=1)

    for task in pending:
        task.cancel()
    for task in list(pending):
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_falls_back_to_blocking_without_a_tracking_set() -> None:
    """Without a tracking set, progress() awaits the publish inline rather
    than scheduling an untracked (garbage-collectable) background task —
    a deliberate safety trade-off, not a missing feature."""
    redis_client = _make_hanging_redis_client()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=None)

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await ctx.progress(step=1)


async def test_scheduled_task_is_tracked_in_pending_publish_tasks() -> None:
    """The task scheduled by progress() appears in the shared
    pending_publish_tasks set immediately (before it completes)."""
    redis_client = _make_hanging_redis_client()
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    async with asyncio.timeout(0.2):
        await ctx.progress(step=1)

    assert len(pending) == 1

    for task in pending:
        task.cancel()
    for task in list(pending):
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_task_removes_itself_from_pending_set_on_completion() -> None:
    """Once the background publish completes, its task is discarded from
    the pending set (task.add_done_callback(pending_publish_tasks.discard))."""
    redis_client = AsyncMock()
    redis_client.publish.return_value = 1
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    await ctx.progress(step=1)
    assert len(pending) == 1

    for _ in range(5):
        await asyncio.sleep(0)

    assert len(pending) == 0


async def test_redis_publish_failure_does_not_propagate_to_caller() -> None:
    """A Redis publish failure must not raise out of progress(), nor
    surface as an unhandled exception in the background task."""
    import warnings

    redis_client = AsyncMock()
    redis_client.publish.side_effect = ConnectionError("redis down")
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        await ctx.progress(step=1)  # must not raise

    for _ in range(5):
        await asyncio.sleep(0)

    assert len(pending) == 0


async def test_multiple_rapid_progress_calls_each_schedule_own_task() -> None:
    """N rapid progress() calls each schedule their own background task; all
    eventually complete and are removed from the pending set."""
    redis_client = AsyncMock()
    redis_client.publish.return_value = 1
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    for i in range(5):
        await ctx.progress(step=i)

    assert len(pending) == 5

    await asyncio.gather(*pending)
    for _ in range(5):
        await asyncio.sleep(0)

    assert len(pending) == 0

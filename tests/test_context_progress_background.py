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
from unittest.mock import AsyncMock

import pytest

from taskq.context import JobContext
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.in_memory import PassthroughPayload
from tests._progress_context import make_progress_context


def _make_ctx(
    *,
    redis_client: object,
    pending_publish_tasks: set["asyncio.Task[None]"] | None = None,
) -> tuple[JobContext[PassthroughPayload], _ProgressBuffer]:
    from taskq._ids import new_job_id, new_uuid

    job_id = new_job_id()
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers = {job_id: buf}

    ctx = make_progress_context(
        buffers,
        job_id,
        worker_id=new_uuid(),
        settings=settings,
        redis_client=redis_client,
        pending_publish_tasks=pending_publish_tasks,
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


async def test_multiple_rapid_progress_calls_coalesce_into_one_in_flight_task() -> None:
    """N rapid progress() calls share ONE in-flight publish per job; the
    later calls latch and the in-flight task drains them, and the set is
    empty again once everything lands."""
    redis_client = AsyncMock()
    redis_client.publish.return_value = 1
    pending: set[asyncio.Task[None]] = set()
    ctx, _buf = _make_ctx(redis_client=redis_client, pending_publish_tasks=pending)

    for i in range(5):
        await ctx.progress(step=i)

    # The coalescing contract: at most one publish round trip per job at
    # a time, whatever the call rate. The later calls latched onto the
    # in-flight task's buffer instead of scheduling their own tasks.
    assert len(pending) == 1

    await asyncio.gather(*pending)
    for _ in range(5):
        await asyncio.sleep(0)

    assert len(pending) == 0

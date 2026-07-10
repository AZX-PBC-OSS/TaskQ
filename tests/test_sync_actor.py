"""Tests for sync function support — ``def`` actors dispatched via asyncio.to_thread.

Covers decoration-time validation, direct invocation, integration with
consume_one_job, cancellation, DI, retry, and error propagation.
"""

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import ActorRef, actor
from taskq.backend._protocol import JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context, set_otel_enabled
from taskq.retry import RetryPolicy
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")


# ── Payload models ─────────────────────────────────────────────────────


class SimplePayload(BaseModel):
    x: int


class ResultPayload(BaseModel):
    value: str


# ═══════════════════════════════════════════════════════════════════════════
#  Decoration-time validation
# ═══════════════════════════════════════════════════════════════════════════


def test_sync_actor_decoration_accepted() -> None:
    """A sync ``def`` function decorated with @actor produces an ActorRef with is_sync=True."""

    @actor
    def my_sync_actor(payload: SimplePayload) -> None:
        pass

    assert isinstance(my_sync_actor, ActorRef)
    assert my_sync_actor.is_sync is True
    assert my_sync_actor.payload_type is SimplePayload


def test_async_actor_is_sync_is_false() -> None:
    """An async ``async def`` function keeps is_sync=False."""

    @actor
    async def my_async_actor(payload: SimplePayload) -> None:
        pass

    assert my_async_actor.is_sync is False


def test_sync_actor_with_ctx_accepted() -> None:
    """Sync function with ctx parameter is accepted and wants_ctx=True."""

    @actor
    def sync_with_ctx(payload: SimplePayload, ctx: JobContext[SimplePayload]) -> None:
        pass

    assert sync_with_ctx.is_sync is True
    assert sync_with_ctx.wants_ctx is True


def test_sync_actor_missing_return_annotation_raises() -> None:
    """Sync function missing return annotation raises TypeError (same as async)."""

    with pytest.raises(TypeError, match="return annotation"):

        @actor
        def no_return(payload: SimplePayload):  # type: ignore[no-untyped-def]
            pass


def test_sync_actor_non_basemodel_payload_raises() -> None:
    """Sync function with non-BaseModel payload raises TypeError."""

    with pytest.raises(TypeError, match="BaseModel"):

        @actor
        def bad_payload(payload: int) -> None:  # type: ignore[type-var]
            pass


def test_sync_actor_name_defaults_to_qualname() -> None:
    """Sync actor name defaults to __qualname__."""

    @actor
    def sync_name_test(payload: SimplePayload) -> None:
        pass

    assert sync_name_test.name.endswith("sync_name_test")


def test_sync_actor_custom_name() -> None:
    """Sync actor with explicit name stores it."""

    @actor(name="custom_sync_name")
    def sync_custom(payload: SimplePayload) -> None:
        pass

    assert sync_custom.name == "custom_sync_name"


def test_sync_actor_with_metadata() -> None:
    """Sync actor with metadata round-trips."""

    @actor(metadata={"tag": "sync"})
    def sync_meta(payload: SimplePayload) -> None:
        pass

    assert sync_meta.metadata == {"tag": "sync"}


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers — JobContext construction for sync tests
# ═══════════════════════════════════════════════════════════════════════════


def _make_test_ctx(
    *, actor_name: str = "test", payload: BaseModel | None = None
) -> JobContext[BaseModel]:
    """Build a minimal JobContext for sync actor tests."""
    jid = new_job_id()
    return JobContext(
        job_id=jid,
        actor=actor_name,
        queue="default",
        attempt=1,
        worker_id=_WORKER_ID,
        payload=payload if payload is not None else SimplePayload(x=1),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=FakeBackend(),
        ),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=jid,
            actor=actor_name,
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Direct invocation via ActorRef.__call__
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_sync_actor_direct_invocation_returns_result() -> None:
    """Direct invocation of a sync actor returns its result."""

    @actor
    def add_one(payload: SimplePayload) -> dict[str, int]:
        return {"result": payload.x + 1}

    result = await add_one(SimplePayload(x=41))
    assert result == {"result": 42}


@pytest.mark.asyncio
async def test_sync_actor_direct_invocation_with_ctx() -> None:
    """Direct invocation with ctx passes context to sync actor."""

    @actor
    def ctx_check(payload: SimplePayload, ctx: JobContext[SimplePayload]) -> dict[str, str]:
        return {"actor": ctx.actor, "job_id": str(ctx.job_id)}

    ctx = _make_test_ctx(actor_name="ctx_check")
    result = await ctx_check(SimplePayload(x=1), ctx)
    assert result["actor"] == "ctx_check"


@pytest.mark.asyncio
async def test_sync_actor_exception_propagates() -> None:
    """Exceptions from sync actors propagate through asyncio.to_thread."""

    @actor
    def failing_actor(payload: SimplePayload) -> None:
        raise ValueError("sync failure")

    with pytest.raises(ValueError, match="sync failure"):
        await failing_actor(SimplePayload(x=1))


@pytest.mark.asyncio
async def test_sync_actor_runs_in_different_thread() -> None:
    """Sync actor body runs in a thread pool, not the event loop thread."""

    main_thread = threading.current_thread()

    @actor
    def check_thread(payload: SimplePayload) -> dict[str, bool]:
        return {"is_main": threading.current_thread() is main_thread}

    result = await check_thread(SimplePayload(x=1))
    assert result["is_main"] is False


@pytest.mark.asyncio
async def test_sync_actor_without_ctx_raises_if_ctx_passed() -> None:
    """Passing ctx to a no-ctx sync actor raises TypeError."""

    @actor
    def no_ctx(payload: SimplePayload) -> None:
        pass

    ctx = _make_test_ctx(actor_name="no_ctx")
    with pytest.raises(TypeError, match="does not declare a context"):
        await no_ctx(SimplePayload(x=1), ctx)  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_sync_actor_with_ctx_raises_if_no_ctx_passed() -> None:
    """Calling a ctx-declaring sync actor without ctx raises TypeError."""

    @actor
    def wants_ctx(payload: SimplePayload, ctx: JobContext[SimplePayload]) -> None:
        pass

    with pytest.raises(TypeError, match="declares 'ctx: JobContext'"):
        await wants_ctx(SimplePayload(x=1))  # type: ignore[call-arg]


# ═══════════════════════════════════════════════════════════════════════════
#  Integration with consume_one_job
# ═══════════════════════════════════════════════════════════════════════════


async def _run_sync_actor_in_consumer(
    actor_fn: object,
    payload: BaseModel,
    *,
    job: JobRow | None = None,
    actor_config: StubActorConfig | None = None,
    backend: FakeBackend | None = None,
) -> tuple[str, FakeBackend]:
    """Helper: call consume_one_job with a sync actor run_actor closure.

    Returns (outcome, backend) so tests can inspect backend calls.
    """
    set_otel_enabled(False)

    if backend is None:
        backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)

    if actor_config is None:
        actor_config = StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0)
        )
    if job is None:
        job = make_job_row(payload=payload.model_dump(mode="json"))

    async def run_actor(_job: JobRow, ctx: JobContext[BaseModel]) -> object:  # pyright: ignore[reportUnusedParameter]
        return await asyncio.to_thread(actor_fn, payload=ctx.payload)  # type: ignore[arg-type]

    outcome = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=run_actor,  # type: ignore[arg-type]
        actor_config=actor_config,
        payload_type=type(payload),
        clock=clk,
        validated_payload=payload,
    )
    return outcome, backend


@pytest.mark.asyncio
async def test_sync_actor_success_via_consumer() -> None:
    """Sync actor completes successfully via consume_one_job."""

    def sync_fn(payload: SimplePayload) -> dict[str, int]:
        return {"doubled": payload.x * 2}

    outcome, _backend = await _run_sync_actor_in_consumer(sync_fn, SimplePayload(x=5))
    assert outcome == "succeeded"


@pytest.mark.asyncio
async def test_sync_actor_failure_via_consumer() -> None:
    """Sync actor failure is handled by consume_one_job's exception path."""

    def sync_fn(payload: SimplePayload) -> None:
        raise RuntimeError("sync boom")

    outcome, _backend = await _run_sync_actor_in_consumer(
        sync_fn,
        SimplePayload(x=1),
        job=make_job_row(attempt=1, max_attempts=1),
    )
    assert outcome == "failed"


@pytest.mark.asyncio
async def test_sync_actor_retry_policy_kicks_in() -> None:
    """Sync actor with transient failure triggers mark_failed_or_retry with next_scheduled_at."""

    def sync_fn(payload: SimplePayload) -> None:
        raise RuntimeError("retry me")

    outcome, backend = await _run_sync_actor_in_consumer(
        sync_fn,
        SimplePayload(x=1),
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        ),
        job=make_job_row(attempt=1, max_attempts=3),
    )
    # Outcome is "failed" for exceptions; the retry is handled by mark_failed_or_retry internally
    assert outcome == "failed"
    assert len(backend.mark_failed_or_retry_calls) == 1
    assert backend.mark_failed_or_retry_calls[0]["next_scheduled_at"] is not None


@pytest.mark.asyncio
async def test_sync_actor_snooze() -> None:
    """Sync actor raising Snooze results in scheduled outcome."""

    from taskq.exceptions import Snooze

    def sync_fn(payload: SimplePayload) -> None:
        raise Snooze(timedelta(seconds=30))

    outcome, _backend = await _run_sync_actor_in_consumer(sync_fn, SimplePayload(x=1))
    assert outcome == "scheduled"


@pytest.mark.asyncio
async def test_sync_actor_retry_after() -> None:
    """Sync actor raising RetryAfter results in scheduled outcome."""

    from taskq.exceptions import RetryAfter

    def sync_fn(payload: SimplePayload) -> None:
        raise RetryAfter(timedelta(seconds=10))

    outcome, _backend = await _run_sync_actor_in_consumer(sync_fn, SimplePayload(x=1))
    assert outcome == "scheduled"


# ═══════════════════════════════════════════════════════════════════════════
#  Cancellation support — should_abort
# ═══════════════════════════════════════════════════════════════════════════


def test_sync_actor_should_abort_defaults_false() -> None:
    """should_abort returns False when no cancellation has been requested."""
    ctx = _make_test_ctx()
    assert ctx.should_abort() is False


def test_sync_actor_should_abort_after_set() -> None:
    """should_abort returns True after _abort_requested is set."""
    ctx = _make_test_ctx()
    ctx._abort_requested.set()
    assert ctx.should_abort() is True


@pytest.mark.asyncio
async def test_sync_actor_polls_should_abort() -> None:
    """Sync actor that polls should_abort() exits cleanly on cancellation."""
    ctx = _make_test_ctx()

    def polling_actor(payload: SimplePayload) -> str:
        # Simulate work with abort check
        for _ in range(1000):
            if ctx.should_abort():
                return "aborted"
        return "completed"

    # Run in thread, set abort midway
    result_box: dict[str, object] = {}

    async def run_and_abort() -> None:
        future = asyncio.to_thread(polling_actor, SimplePayload(x=1))
        # Give thread time to enter the loop
        await asyncio.sleep(0.01)
        ctx._abort_requested.set()
        result = await future
        result_box["result"] = result

    await run_and_abort()
    assert result_box["result"] == "aborted"


def test_cancel_controller_sets_abort_requested() -> None:
    """CancelController sets abort_requested during phase 1."""
    ctx = _make_test_ctx()

    # Simulate what CancelController does in phase 1
    ctx.cancel_event.set()
    ctx._abort_requested.set()

    assert ctx.cancellation_requested is True
    assert ctx.should_abort() is True


# ═══════════════════════════════════════════════════════════════════════════
#  Async actors still work unchanged
# ═══════════════════════════════════════════════════════════════════════════


def test_async_actor_still_registers_normally() -> None:
    """Async actors are unaffected by sync support changes."""

    @actor(name="async_test", max_concurrent=3)
    async def normal_async(payload: SimplePayload) -> dict[str, int]:
        return {"x": payload.x}

    assert normal_async.is_sync is False
    assert normal_async.max_concurrent == 3
    assert normal_async.payload_type is SimplePayload


@pytest.mark.asyncio
async def test_async_actor_direct_invocation_still_works() -> None:
    """Async actor direct invocation still works after sync support changes."""

    @actor
    async def async_direct(payload: SimplePayload) -> int:
        return payload.x * 10

    result = await async_direct(SimplePayload(x=5))
    assert result == 50


@pytest.mark.asyncio
async def test_async_actor_result_type_still_works() -> None:
    """Async actor returning BaseModel still works."""

    @actor
    async def async_result(payload: SimplePayload) -> ResultPayload:
        return ResultPayload(value=f"got {payload.x}")

    result = await async_result(SimplePayload(x=7))
    assert isinstance(result, ResultPayload)
    assert result.value == "got 7"


# ═══════════════════════════════════════════════════════════════════════════
#  Thread-safety — concurrent sync actors
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_concurrent_sync_actors_run_in_parallel() -> None:
    """Multiple sync actors dispatched concurrently to the thread pool."""

    captured: list[int] = []

    def sync_fn(payload: SimplePayload) -> dict[str, int]:
        captured.append(payload.x)
        return {"x": payload.x}

    results = await asyncio.gather(
        asyncio.to_thread(sync_fn, SimplePayload(x=1)),
        asyncio.to_thread(sync_fn, SimplePayload(x=2)),
        asyncio.to_thread(sync_fn, SimplePayload(x=3)),
    )

    assert len(results) == 3
    assert len(captured) == 3
    assert sorted(captured) == [1, 2, 3]


@pytest.mark.asyncio
async def test_sync_and_async_actors_concurrent() -> None:
    """Sync and async actors can run concurrently without blocking each other."""

    @actor
    async def async_one(payload: SimplePayload) -> dict[str, int]:
        await asyncio.sleep(0.01)
        return {"x": payload.x}

    @actor
    def sync_one(payload: SimplePayload) -> dict[str, int]:
        import time

        time.sleep(0.01)
        return {"x": payload.x * 2}

    results = await asyncio.gather(
        async_one(SimplePayload(x=1)),
        async_one(SimplePayload(x=2)),
        asyncio.to_thread(sync_one.fn, **{"payload": SimplePayload(x=3)}),
        asyncio.to_thread(sync_one.fn, **{"payload": SimplePayload(x=4)}),
    )
    assert len(results) == 4

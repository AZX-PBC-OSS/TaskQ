"""Tests for the ``actor_runner`` fixture.

part 1: actor_runner calls a simple actor function returning a
value; assert the returned value is correct.

part 2: pass a pre-fired cancel_event kwarg; assert
ctx.cancellation_requested == True.

Sanity: passing additional kwargs (e.g. http_client=mock_http) reaches the
actor via ctx.deps["http_client"].
"""

import asyncio

from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.context import (
    JobContext as CtxJobContext,  # Why: the @actor decorator detects the ctx parameter by the production JobContext annotation.
)
from taskq.testing.fixtures import ActorRunnerCallable
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.job_context import JobContext

# ── part 1: simple actor returns value ─────────────────────────


async def test_actor_runner_simple_actor(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """part 1: actor_runner calls a simple actor returning a value."""

    def my_actor(payload: object, ctx: JobContext[BaseModel]) -> str:
        return "hello"

    result = await actor_runner(my_actor, {"key": "val"}, backend=memory_jobs)
    assert result == "hello"


async def test_actor_runner_async_actor(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """part 1: actor_runner calls an async actor returning a value."""

    async def my_async_actor(payload: object, ctx: JobContext[BaseModel]) -> int:
        return 42

    result = await actor_runner(my_async_actor, {"key": "val"}, backend=memory_jobs)
    assert result == 42


# ── part 2: pre-fired cancel_event ─────────────────────────────


async def test_actor_runner_prefired_cancel_event(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """part 2: passing a pre-fired cancel_event makes
    ctx.cancellation_requested == True inside the actor.
    """
    evt = asyncio.Event()
    evt.set()

    observed = False

    def my_actor(payload: object, ctx: JobContext[BaseModel]) -> None:
        nonlocal observed
        observed = ctx.cancellation_requested

    await actor_runner(my_actor, {}, backend=memory_jobs, cancel_event=evt)
    assert observed is True


# ── Sanity: **deps kwargs reach actor via ctx.deps ────────────────────


async def test_actor_runner_deps_forwarded(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """Sanity: passing additional kwargs reaches the actor via
    ctx.deps["http_client"].
    """

    class MockHttp:
        pass

    mock_http = MockHttp()
    observed_dep: object | None = None

    def my_actor(payload: object, ctx: JobContext[BaseModel]) -> None:
        nonlocal observed_dep
        assert ctx.deps is not None
        observed_dep = ctx.deps.get("http_client")

    await actor_runner(my_actor, {}, backend=memory_jobs, http_client=mock_http)
    assert observed_dep is mock_http


# ── Sanity: job_id and attempt are passed through ─────────────────────


async def test_actor_runner_custom_job_id(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """Sanity: custom job_id and attempt are passed through to JobContext."""
    custom_id = new_uuid()
    observed_id: object | None = None
    observed_attempt: int | None = None

    def my_actor(payload: object, ctx: JobContext[BaseModel]) -> None:
        nonlocal observed_id, observed_attempt
        observed_id = ctx.job_id
        observed_attempt = ctx.attempt

    await actor_runner(
        my_actor,
        {},
        backend=memory_jobs,
        job_id=custom_id,
        attempt=3,
    )
    assert observed_id == custom_id
    assert observed_attempt == 3


# ── part 4: deferral-cycle contract through the harness ────────


async def test_actor_runner_snooze_count_parameter_reaches_the_context(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """The ``snooze_count`` parameter exercises a deferral-cycled actor
    beyond first dispatch.

    Without the parameter the harness silently defaulting to 0 would let
    a snooze-N-then-succeed actor test pass while only ever exercising
    first-dispatch behaviour - false confidence, the exact silent-gap
    shape the issue tracks. The actor below is the documented contract
    (keyed off ``ctx.snooze_count``, as the PG e2e pins): it must observe
    the value the caller supplied.
    """
    observed: list[int] = []

    def cycler(payload: object, ctx: JobContext[BaseModel]) -> bool:
        observed.append(ctx.snooze_count)
        return ctx.snooze_count >= 2

    result = await actor_runner(
        cycler,
        {},
        backend=memory_jobs,
        snooze_count=2,
    )

    assert result is True
    assert observed == [2]


# ── ctx omission: handlers that declare no ctx parameter ───────────────


async def test_actor_runner_omits_ctx_for_payload_only_actor(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """A handler declaring no ``ctx`` parameter runs under actor_runner.

    The production call path omits the context for a no-ctx handler
    (calling one WITH a context raises ``TypeError``), so the fixture
    must mirror that decision instead of passing ctx unconditionally.
    """
    seen: dict[str, object] = {}

    async def payload_only(payload: object) -> str:
        seen["payload"] = payload
        return "no-ctx"

    result = await actor_runner(payload_only, {"key": "val"}, backend=memory_jobs)
    assert result == "no-ctx"
    assert seen["payload"] == {"key": "val"}


async def test_actor_runner_forwards_declared_deps_without_ctx(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """The common DI shape (deps, no ctx) works: only the dependency
    parameters the handler declared are injected as kwargs, the same
    selectivity the production DI pass applies."""
    mock_http = object()
    observed: object | None = None

    async def handler(payload: object, http_client: object) -> None:
        nonlocal observed
        observed = http_client

    await actor_runner(handler, {}, backend=memory_jobs, http_client=mock_http)
    assert observed is mock_http


async def test_actor_runner_accepts_actor_ref_without_ctx(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """An ActorRef is accepted directly and runs without a context
    (the ref's ``wants_ctx`` is False, the production dispatch decision)."""

    class RefPayload(BaseModel):
        value: int

    @actor
    async def ref_actor(payload: RefPayload) -> int:
        return payload.value + 1

    result = await actor_runner(ref_actor, RefPayload(value=1), backend=memory_jobs)
    assert result == 2


async def test_actor_runner_accepts_actor_ref_with_ctx(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """A ctx-declaring ActorRef still receives the JobContext."""

    class CtxPayload(BaseModel):
        value: int

    @actor
    async def ref_ctx_actor(payload: CtxPayload, ctx: CtxJobContext[CtxPayload]) -> str:
        return f"ctx-{ctx.job_id}"

    result = await actor_runner(ref_ctx_actor, CtxPayload(value=1), backend=memory_jobs)
    assert isinstance(result, str)
    assert result.startswith("ctx-")

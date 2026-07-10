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

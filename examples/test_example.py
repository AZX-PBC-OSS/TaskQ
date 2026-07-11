"""Testing example — unit-test actors without Postgres or Redis.

Demonstrates the ``taskq.testing`` package: ``InMemoryBackend``,
``FakeClock``, and ``run_until_drained`` for deterministic, fast
unit tests that exercise the full enqueue → dispatch → execute cycle.

Run with::

    uv run pytest examples/test_example.py -v

No Docker, no Postgres, no Redis required — everything runs in-process.
"""

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from taskq import JobContext, RetryPolicy, actor
from taskq.client import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Actors under test ──────────────────────────────────────────────────────


class DoublePayload(BaseModel):
    value: int


class DoubleResult(BaseModel):
    doubled: int


@actor
async def double_value(payload: DoublePayload) -> DoubleResult:
    return DoubleResult(doubled=payload.value * 2)


class RetryPayload(BaseModel):
    fail_until_attempt: int


@actor(retry=RetryPolicy(max_attempts=5, base=__import__("datetime").timedelta(seconds=0)))
async def flaky_actor(payload: RetryPayload, ctx: JobContext[RetryPayload]) -> None:
    if ctx.attempt <= payload.fail_until_attempt:
        raise ValueError(f"intentional failure on attempt {ctx.attempt}")


# ── Tests ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enqueue_and_wait() -> None:
    """Enqueue a job, drain the backend, and verify the result."""
    clock = FakeClock(start=datetime.now(UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    backend.register_stub(
        double_value.name,
        lambda payload, ctx: {"doubled": payload["value"] * 2},
    )

    handle = await client.enqueue(double_value, DoublePayload(value=21))
    assert not handle.was_existing

    await backend.run_until_drained()
    result = await handle.wait()
    assert result.doubled == 42


@pytest.mark.asyncio
async def test_direct_invocation() -> None:
    """Call an actor directly — bypasses the queue entirely."""
    result = await double_value(DoublePayload(value=21))
    assert result.doubled == 42


@pytest.mark.asyncio
async def test_retry_succeeds_after_failures() -> None:
    """Verify retry policy: actor fails N times then succeeds."""
    clock = FakeClock(start=datetime.now(UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    call_count = 0

    def stub(payload: dict[str, object], ctx: object) -> None:
        nonlocal call_count
        call_count += 1
        fail_until = int(payload["fail_until_attempt"])
        if call_count <= fail_until:
            raise ValueError("intentional failure")

    backend.register_stub(flaky_actor.name, stub)

    handle = await client.enqueue(flaky_actor, RetryPayload(fail_until_attempt=2))
    await backend.run_until_drained()

    status = await handle.status()
    assert status == "succeeded"
    assert call_count == 3


@pytest.mark.asyncio
async def test_dedup_via_idempotency_key() -> None:
    """Enqueuing with the same idempotency_key returns the existing job."""
    from taskq import IdempotencyKey

    clock = FakeClock(start=datetime.now(UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    backend.register_stub(
        double_value.name,
        lambda payload, ctx: {"doubled": payload["value"] * 2},
    )

    handle1 = await client.enqueue(
        double_value,
        DoublePayload(value=10),
        idempotency_key=IdempotencyKey("test-key-1"),
    )
    assert not handle1.was_existing

    handle2 = await client.enqueue(
        double_value,
        DoublePayload(value=10),
        idempotency_key=IdempotencyKey("test-key-1"),
    )
    assert handle2.was_existing
    assert handle2.job_id == handle1.job_id


@pytest.mark.asyncio
async def test_cancellation_sets_cancelled_status() -> None:
    """Cancelling a pending job transitions it to 'cancelled'."""
    clock = FakeClock(start=datetime.now(UTC))
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    backend.register_stub(double_value.name, lambda payload, ctx: {"doubled": payload["value"] * 2})

    handle = await client.enqueue(double_value, DoublePayload(value=1))
    await client.cancel(handle.job_id, reason="test_cancel")

    status = await handle.status()
    assert status == "cancelled"

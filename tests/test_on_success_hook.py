"""Unit tests for the on_success hook: invoke_on_success and consumer wiring."""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import cast

import structlog
from pydantic import BaseModel

import taskq.obs as obs_mod
from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.context import JobContext
from taskq.retry import RetryPolicy, invoke_on_success
from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


# ── invoke_on_success unit tests ────────────────────────────────────────


async def test_hook_called_with_job_row_and_result_on_success() -> None:
    """Sync hook receives (job_row, result) and is invoked exactly once."""
    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    job_row = make_job_row()
    result = {"value": 42}

    await invoke_on_success(hook, job_row, result, 3.0)

    assert len(calls) == 1
    assert calls[0][0] is job_row
    assert calls[0][1] is result


async def test_hook_none_is_noop() -> None:
    """hook=None returns immediately without raising."""
    job_row = make_job_row()
    await invoke_on_success(None, job_row, {"ok": True}, 3.0)


async def test_async_hook_awaited_correctly() -> None:
    """Async hook is awaited and its side effect is visible after invoke."""
    entries: list[str] = []

    async def async_hook(job_row: object, result: object) -> None:
        await asyncio.sleep(0.001)
        entries.append("called")

    job_row = make_job_row()
    await invoke_on_success(async_hook, job_row, {"ok": True}, 3.0)
    assert entries == ["called"]


async def test_sync_hook_returning_none_works() -> None:
    """A sync hook that returns None (not an awaitable) is handled — no
    attempt to await None."""
    called: list[bool] = []

    def sync_hook(job_row: object, result: object) -> None:
        called.append(True)

    job_row = make_job_row()
    await invoke_on_success(sync_hook, job_row, None, 3.0)
    assert called == [True]


async def test_hook_timeout_caught_and_logged() -> None:
    """A hook that hangs past the timeout is cancelled; a warning is logged
    and invoke returns normally."""

    async def hanging_hook(job_row: object, result: object) -> None:
        await asyncio.sleep(999)

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_success(hanging_hook, job_row, None, 0.5)
    timeouts = [e for e in captured if e.get("event") == "on-success-hook-timeout"]
    assert len(timeouts) == 1
    assert timeouts[0]["log_level"] == "warning"
    assert timeouts[0]["timeout_seconds"] == 0.5


async def test_hook_exception_caught_and_logged() -> None:
    """A hook that raises has its exception caught and logged at WARNING."""

    def bad_hook(job_row: object, result: object) -> None:
        raise RuntimeError("hook crashed")

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_success(bad_hook, job_row, None, 3.0)
    failures = [e for e in captured if e.get("event") == "on-success-hook-failed"]
    assert len(failures) == 1
    assert failures[0]["log_level"] == "warning"


async def test_async_hook_exception_caught_and_logged() -> None:
    """An async hook that raises after an await has its exception caught."""

    async def bad_async_hook(job_row: object, result: object) -> None:
        await asyncio.sleep(0.001)
        raise RuntimeError("hook crashed after await")

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_success(bad_async_hook, job_row, None, 3.0)
    failures = [e for e in captured if e.get("event") == "on-success-hook-failed"]
    assert len(failures) == 1


async def test_hook_timeout_returns_within_time_budget() -> None:
    """A hanging hook returns within ~1s of the timeout, not 999s."""

    async def hanging_hook(job_row: object, result: object) -> None:
        await asyncio.sleep(999)

    job_row = make_job_row()
    start = time.monotonic()
    await invoke_on_success(hanging_hook, job_row, None, 0.3)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0


# ── Consumer integration: on_success fires on success path ──────────────


async def test_on_success_fires_on_success_path() -> None:
    """consume_one_job invokes on_success with (job_row, result) when the
    actor succeeds."""
    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"value": 42}

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

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
    assert len(calls) == 1
    assert calls[0][0] is job
    assert calls[0][1] == {"value": 42}


async def test_on_success_not_called_on_failure() -> None:
    """on_success is NOT invoked when the actor raises and retries are
    exhausted."""
    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("boom")

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row(attempt=1, max_attempts=1)
    obs_mod.set_otel_enabled(False)

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
    assert len(calls) == 0


async def test_on_success_not_called_on_snooze() -> None:
    """on_success is NOT invoked when the actor raises Snooze."""
    from taskq.exceptions import Snooze

    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise Snooze(timedelta(seconds=30))

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    assert result == "scheduled"
    assert len(calls) == 0


async def test_on_success_async_hook_fires_on_success_path() -> None:
    """An async on_success hook is awaited correctly on the success path."""
    calls: list[tuple[object, object]] = []

    async def hook(job_row: object, result: object) -> None:
        await asyncio.sleep(0.001)
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

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
    assert len(calls) == 1
    assert calls[0][1] == {"ok": True}


async def test_on_success_hook_exception_does_not_crash_consumer() -> None:
    """A failing on_success hook is caught; the consumer still returns
    'succeeded'."""

    def bad_hook(job_row: object, result: object) -> None:
        raise RuntimeError("on_success crashed")

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=bad_hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

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


async def test_on_success_hook_timeout_does_not_crash_consumer() -> None:
    """A timing-out on_success hook is caught; the consumer still returns
    'succeeded'."""

    async def hanging_hook(job_row: object, result: object) -> None:
        await asyncio.sleep(999)

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"ok": True}

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hanging_hook,
        on_success_timeout=0.3,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    start = time.monotonic()
    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
    )
    elapsed = time.monotonic() - start
    assert result == "succeeded"
    assert elapsed < 2.0


async def test_on_success_fires_on_transactional_success_path() -> None:
    """on_success fires on the transactional success path (when loop_conn
    is provided)."""
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    import asyncpg

    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"tx": True}

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    fake_conn = AsyncMock(spec=asyncpg.Connection)

    @asynccontextmanager
    async def _fake_transaction() -> AsyncGenerator[None, None]:
        yield

    fake_conn.transaction = _fake_transaction  # type: ignore[method-assign]  # Why: test-only mock to simulate a transactional connection

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=clk,
        loop_conn=cast("asyncpg.Connection", fake_conn),
    )
    assert result == "succeeded"
    assert len(calls) == 1
    assert calls[0][1] == {"tx": True}


async def test_on_success_receives_none_result_for_none_returning_actor() -> None:
    """When the actor returns None, on_success receives None as the result."""
    calls: list[tuple[object, object]] = []

    def hook(job_row: object, result: object) -> None:
        calls.append((job_row, result))

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        return None

    backend = FakeBackend()
    clk: Clock = FakeClock(_NOW)
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_success=hook,
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

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
    assert len(calls) == 1
    assert calls[0][1] is None

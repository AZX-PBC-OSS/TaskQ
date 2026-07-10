"""Tests for the SlidingWindow log-style refund fix (memory + postgres).

Regression: ``refund()`` previously dispatched ``pass`` (no-op) for the
``("memory", "log")`` and ``("postgres", "log")`` cases, so a refunded
slot was never freed -- capacity stayed artificially constrained until
the timestamp slid out of the window.  These tests verify the slot is
actually released and that refunds are idempotent across both backends.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from taskq._ids import new_base62
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit.decision import RateLimitDecision
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _FakeSettings:
    schema_name: str = "taskq_test"


def _sw_log(limit: int = 2, window: timedelta = timedelta(seconds=60)) -> SlidingWindow:
    return SlidingWindow(
        name=f"refund-fix-{new_base62()}",
        limit=limit,
        window=window,
        backend="memory",
        style="log",
    )


# ── Memory log refund frees the slot ────────────────────────────────────


async def test_refund_log_memory_frees_slot() -> None:
    """Refunding an allowed memory-log decision releases the slot so a
    previously-denied acquire succeeds immediately."""
    sw = _sw_log(limit=2)
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    r2 = await sw.acquire(clock=clock)
    assert r1.allowed is True
    assert r2.allowed is True
    assert r1.request_id is not None

    r3 = await sw.acquire(clock=clock)
    assert r3.allowed is False

    await sw.refund(r1)

    r4 = await sw.acquire(clock=clock)
    assert r4.allowed is True
    assert r4.retry_after == timedelta(0)


async def test_refund_log_memory_restores_remaining() -> None:
    """Refunding a memory-log decision restores ``remaining`` by one."""
    sw = _sw_log(limit=3)
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    await sw.acquire(clock=clock)
    state_before = await sw.peek(clock=clock)
    assert state_before.remaining == 1.0

    await sw.refund(r1)

    state_after = await sw.peek(clock=clock)
    assert state_after.remaining == 2.0


async def test_refund_log_memory_idempotent_unknown_request_id() -> None:
    """Refunding a request_id that was never acquired is a silent no-op."""
    sw = _sw_log(limit=2)
    clock = FakeClock(_START)
    await sw.acquire(clock=clock)

    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="memory",
        request_id=str(uuid4()),
    )
    await sw.refund(decision)

    state = await sw.peek(clock=clock)
    assert state.remaining == 1.0


async def test_refund_log_memory_none_request_id_is_noop() -> None:
    """Refunding a memory-log decision with request_id=None is a no-op."""
    sw = _sw_log(limit=2)
    clock = FakeClock(_START)
    await sw.acquire(clock=clock)

    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="memory",
        request_id=None,
    )
    await sw.refund(decision)

    state = await sw.peek(clock=clock)
    assert state.remaining == 1.0


async def test_refund_log_memory_does_not_over_refund_same_timestamp() -> None:
    """Two acquires at the same now_ms refunded by request_id remove only
    the targeted entry, not both same-timestamp entries."""
    sw = _sw_log(limit=3)
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    r2 = await sw.acquire(clock=clock)
    assert r1.request_id != r2.request_id

    await sw.refund(r1)

    state = await sw.peek(clock=clock)
    assert state.remaining == 2.0


async def test_refund_memory_log_raises_when_not_initialised() -> None:
    """_refund_memory_log raises RuntimeError when built as GCRA."""
    sw = SlidingWindow(
        name="refund-fix-gcra",
        limit=2,
        window=timedelta(seconds=60),
        backend="memory",
        style="gcra",
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="memory",
        request_id=str(uuid4()),
    )
    with pytest.raises(RuntimeError, match="memory log not initialised"):
        await sw._refund_memory_log(decision)


# ── Postgres log refund ─────────────────────────────────────────────────


class _FakePgPool:
    """Minimal pool stub capturing direct ``pool.execute()`` calls."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> None:
        self.execute_calls.append((sql, args))


async def test_refund_pg_log_executes_delete_by_request_id() -> None:
    """Postgres log refund issues a parameterised DELETE keyed on request_id."""
    sw = SlidingWindow(
        name="refund-pg-log-fix",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    pool = _FakePgPool()
    rid = str(uuid4())
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-log-fix",
        backend="postgres",
        request_id=rid,
    )

    await sw.refund(decision, pg_pool=pool, settings=_FakeSettings())

    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert "DELETE FROM" in sql
    assert "rate_limit_window_entries" in sql
    assert "request_id = $2::uuid" in sql
    assert "bucket_name = $1" in sql
    assert args == ("refund-pg-log-fix", rid)


async def test_refund_pg_log_none_request_id_is_noop() -> None:
    """Postgres log refund with request_id=None is a no-op (no pool needed)."""
    sw = SlidingWindow(
        name="refund-pg-log-none",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    pool = _FakePgPool()
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-log-none",
        backend="postgres",
        request_id=None,
    )

    await sw.refund(decision, pg_pool=pool, settings=_FakeSettings())

    assert pool.execute_calls == []


async def test_refund_pg_log_raises_without_pool() -> None:
    """Postgres log refund raises RuntimeError when pg_pool is missing."""
    sw = SlidingWindow(
        name="refund-pg-log-nopool",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-log-nopool",
        backend="postgres",
        request_id=str(uuid4()),
    )

    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.refund(decision, settings=_FakeSettings())


async def test_refund_pg_log_raises_without_settings() -> None:
    """Postgres log refund raises RuntimeError when settings is missing."""
    sw = SlidingWindow(
        name="refund-pg-log-nosettings",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-log-nosettings",
        backend="postgres",
        request_id=str(uuid4()),
    )

    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.refund(decision, pg_pool=_FakePgPool())


# ── Postgres log acquire propagates request_id (enables refund) ─────────


class _NullAsyncCtx:
    async def __aenter__(self) -> "_NullAsyncCtx":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePgConn:
    def __init__(self, pool: "_FakeFullPgPool") -> None:
        self._pool = pool

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return self._pool.fetchrow_result

    async def execute(self, sql: str, *args: object) -> None:
        self._pool.execute_calls.append((sql, args))

    def transaction(self) -> _NullAsyncCtx:
        return _NullAsyncCtx()


class _FakeAcquireCtx:
    def __init__(self, pool: "_FakeFullPgPool") -> None:
        self._pool = pool

    async def __aenter__(self) -> _FakePgConn:
        return _FakePgConn(self._pool)

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeFullPgPool:
    """Fake asyncpg.Pool supporting ``acquire()`` + direct ``execute()``."""

    def __init__(self, fetchrow_result: object = None) -> None:
        self.fetchrow_result = fetchrow_result
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self)

    async def execute(self, sql: str, *args: object) -> None:
        self.execute_calls.append((sql, args))


async def test_acquire_pg_log_decision_carries_request_id() -> None:
    """PG log acquire stamps request_id onto the decision so refund can find it."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_log

    sw = SlidingWindow(
        name="acquire-pg-log-rid",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    rid = uuid4()
    pool = _FakeFullPgPool(fetchrow_result={"count": 1})

    decision = await _acquire_pg_log(
        sw,
        pg_pool=pool,
        clock=FakeClock(_START),
        settings=_FakeSettings(),
        request_id=rid,
    )

    assert decision.allowed is True
    assert decision.request_id == str(rid)

"""Tests for the SlidingWindow log-style refund fix (memory + postgres).

Regression: ``refund()`` dispatching ``pass`` (no-op) for the
``("memory", "log")`` and ``("postgres", "log")`` cases, so a refunded
slot was never freed -- capacity stayed artificially constrained until
the timestamp slid out of the window.  These tests verify the slot is
actually released and that refunds are idempotent across both backends.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_pg import DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS
from taskq.ratelimit.decision import RateLimitDecision
from taskq.ratelimit.token_bucket import DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _FakeSettings:
    """Settings stub for the PG dispatch arms: carries the fields the PG
    acquire/refund paths read - ``schema_name`` plus the lock budgets,
    mirrored at WorkerSettings' shipped defaults. The budget resolution
    seam reads them without a fallback, so a double lacking one fails
    loud instead of silently pinning the wait."""

    schema_name: str = "taskq_test"
    token_bucket_lock_timeout_ms: float = DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
    sliding_window_lock_timeout_ms: float = DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS


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
    denied acquire succeeds immediately after the refund."""
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
        request_id=str(new_uuid()),
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
        request_id=str(new_uuid()),
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
    rid = str(new_uuid())
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


# NOTE (the mock-subtraction cure): the DELETE's text pin above stays (the
# statement shape is the fault-injection-free contract this unit owns); its
# behavior — the admitted row actually deleted, the window refilled —
# converted to the real fixture in tests/test_ratelimit_sliding_window_pg.py
# (test_refund_log_removes_the_admitted_row), and the none-request-id no-op
# with it (test_refund_log_without_a_request_id_is_a_noop).


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
        request_id=str(new_uuid()),
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
        request_id=str(new_uuid()),
    )

    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.refund(decision, pg_pool=_FakePgPool())


# ── Postgres log acquire propagates request_id (enables refund) ─────────
#
# NOTE (the mock-subtraction cure): the request_id carry fake arm is deleted,
# with the fused-statement row double that existed only for it — the real
# fixture owns the round trip now: the log acquire's stamped request_id is
# what the real-PG refund deletes by
# (tests/test_ratelimit_sliding_window_pg.py::
# test_refund_log_removes_the_admitted_row asserts first.request_id is not
# None on the real decision, then deletes that exact row).

"""Unit tests for RateLimitRegistry — data structure, register, acquire context manager.

Tests All tests use in-memory backends
(``backend="memory"``) with ``FakeClock`` — no
Redis or PG instance required.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from taskq.ratelimit.registry import RateLimitRegistry, sync_rate_limit_buckets
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _token_bucket(
    name: str = "tb",
    capacity: float = 10.0,
    refill: float = 1.0,
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="memory")


def _sliding_window(
    name: str = "sw",
    limit: int = 5,
    window: timedelta = timedelta(minutes=1),
) -> SlidingWindow:
    return SlidingWindow(name=name, limit=limit, window=window, backend="memory")


def _reservation(
    name: str = "res",
    slots: int = 4,
    lease: timedelta = timedelta(seconds=10),
    clock: FakeClock | None = None,
) -> ConcurrencyReservation:
    if clock is None:
        clock = FakeClock(_START)
    return ConcurrencyReservation(name=name, slots=slots, lease=lease, clock=clock)


# ── Register and lookup ─────────────────────────────────────────


async def test_register_token_bucket() -> None:
    """Register a TokenBucket and look it up."""
    reg = RateLimitRegistry()
    tb = _token_bucket("openai")
    reg.register(tb)

    assert reg.rate_limits["openai"] is tb
    assert reg.get_rate_limit("openai") is tb


async def test_register_sliding_window() -> None:
    """Register a SlidingWindow and look it up."""
    reg = RateLimitRegistry()
    sw = _sliding_window("vendor_x")
    reg.register(sw)

    assert reg.rate_limits["vendor_x"] is sw
    assert reg.get_rate_limit("vendor_x") is sw


async def test_register_reservation() -> None:
    """Register a ConcurrencyReservation and look it up."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    res = _reservation("gpu", clock=clock)
    reg.register(res)

    assert reg.reservations["gpu"] is res
    assert reg.get_reservation("gpu") is res


async def test_properties_are_read_only() -> None:
    """rate_limits and reservations properties return copies."""
    reg = RateLimitRegistry()
    tb = _token_bucket("tb")
    reg.register(tb)

    rl = reg.rate_limits
    rl["injected"] = _token_bucket("injected")

    assert "injected" not in reg.rate_limits


# ── Duplicate name raises ValueError ────────────────────────────


async def test_duplicate_rate_limit_identical_is_noop() -> None:
    """Re-registering an identical rate-limit config is a no-op;
    a conflicting config for the same name raises ValueError."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("dup"))
    reg.register(_token_bucket("dup"))  # identical — no-op
    with pytest.raises(ValueError, match="rate-limit name already registered"):
        reg.register(_token_bucket("dup", capacity=99.0))


async def test_duplicate_reservation_identical_is_noop() -> None:
    """Re-registering an identical reservation config is a no-op;
    a conflicting config for the same name raises ValueError."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_reservation("dup", clock=clock))
    reg.register(_reservation("dup", clock=clock))  # identical — no-op
    with pytest.raises(ValueError, match="reservation name already registered"):
        reg.register(_reservation("dup", slots=99, clock=clock))


async def test_duplicate_sliding_window_identical_is_noop() -> None:
    """Re-registering an identical SlidingWindow config is a no-op;
    a conflicting config for the same name raises ValueError."""
    reg = RateLimitRegistry()
    reg.register(_sliding_window("dup"))
    reg.register(_sliding_window("dup"))  # identical — no-op
    with pytest.raises(ValueError, match="rate-limit name already registered"):
        reg.register(_sliding_window("dup", limit=99))


# ── Cross-dict name collision is allowed ────────────────────────


async def test_cross_dict_collision_allowed() -> None:
    """Same name in rate_limits and reservations is allowed."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_token_bucket("x"))
    reg.register(_reservation("x", clock=clock))

    assert "x" in reg.rate_limits
    assert "x" in reg.reservations
    assert isinstance(reg.get_rate_limit("x"), TokenBucket)
    assert isinstance(reg.get_reservation("x"), ConcurrencyReservation)


# ── registry.acquire() for non-job code ────────────────────────


async def test_acquire_returns_decision() -> None:
    """registry.acquire() returns RateLimitDecision, no release on exit."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("tb", capacity=10.0, refill=1.0))
    clock = FakeClock(_START)

    async with reg.acquire("tb", count=1.0, clock=clock) as decision:
        assert decision.allowed is True
        assert decision.remaining == 9.0
        assert decision.bucket_name == "tb"


async def test_acquire_no_release_on_exit() -> None:
    """Tokens consumed permanently — no release on context exit."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("tb", capacity=5.0, refill=0.0))
    clock = FakeClock(_START)

    async with reg.acquire("tb", count=1.0, clock=clock) as decision:
        assert decision.allowed is True
        assert decision.remaining == 4.0

    async with reg.acquire("tb", count=1.0, clock=clock) as decision:
        assert decision.remaining == 3.0


async def test_acquire_sliding_window() -> None:
    """registry.acquire() works for SlidingWindow."""
    reg = RateLimitRegistry()
    reg.register(_sliding_window("sw", limit=3))
    clock = FakeClock(_START)

    async with reg.acquire("sw", clock=clock) as decision:
        assert decision.allowed is True
        assert decision.remaining == 2.0


# ── registry.acquire() raises TypeError for reservation name ──


async def test_acquire_raises_typeerror_for_reservation() -> None:
    """registry.acquire() raises TypeError for reservation name."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu", clock=clock))

    with pytest.raises(TypeError, match="ConcurrencyReservation"):
        async with reg.acquire("gpu") as _:
            pass


# ── registry.acquire() yields allowed=False when exhausted ────


async def test_acquire_yields_denied_decision() -> None:
    """registry.acquire() yields decision with allowed=False when exhausted."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("tb", capacity=2.0, refill=0.0))
    clock = FakeClock(_START)

    async with reg.acquire("tb", count=1.0, clock=clock) as d1:
        assert d1.allowed is True

    async with reg.acquire("tb", count=1.0, clock=clock) as d2:
        assert d2.allowed is True

    async with reg.acquire("tb", count=1.0, clock=clock) as d3:
        assert d3.allowed is False
        assert d3.retry_after is None


# ── get_rate_limit("unknown") raises KeyError ───────────────────


async def test_get_rate_limit_unknown_raises() -> None:
    """get_rate_limit raises KeyError for unknown name."""
    reg = RateLimitRegistry()
    with pytest.raises(KeyError):
        reg.get_rate_limit("unknown")


# ── get_reservation("unknown") raises KeyError ──────────────────


async def test_get_reservation_unknown_raises() -> None:
    """get_reservation raises KeyError for unknown name."""
    reg = RateLimitRegistry()
    with pytest.raises(KeyError):
        reg.get_reservation("unknown")


# ── registry.acquire() raises KeyError for unregistered name ─


async def test_acquire_unregistered_name_raises_keyerror() -> None:
    """registry.acquire() raises KeyError for unregistered name."""
    reg = RateLimitRegistry()
    with pytest.raises(KeyError):
        async with reg.acquire("nonexistent") as _:
            pass


# ── _same_config falls through to False for mismatched types ────


async def test_duplicate_name_mismatched_kind_raises() -> None:
    """Registering a SlidingWindow under a name already holding a TokenBucket
    is a config mismatch (different types never compare equal) —
    _same_config falls through to False and register() raises ValueError."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("dup"))
    with pytest.raises(ValueError, match="rate-limit name already registered"):
        reg.register(_sliding_window("dup"))


# ── peek() ───────────────────────────────────────────────────────


async def test_peek_raises_typeerror_for_reservation() -> None:
    """peek() raises TypeError when name refers to a ConcurrencyReservation."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu", clock=clock))

    with pytest.raises(TypeError, match="ConcurrencyReservation"):
        await reg.peek("gpu")


async def test_peek_unregistered_name_raises_keyerror() -> None:
    """peek() raises KeyError for unregistered name."""
    reg = RateLimitRegistry()
    with pytest.raises(KeyError):
        await reg.peek("unknown")


async def test_peek_sliding_window() -> None:
    """peek() dispatches to SlidingWindow.peek() for non-TokenBucket primitives."""
    reg = RateLimitRegistry()
    reg.register(_sliding_window("sw", limit=3))
    clock = FakeClock(_START)

    state = await reg.peek("sw", clock=clock)

    assert state.remaining == 3.0


# ── peek_all() ───────────────────────────────────────────────────


async def test_peek_all_catches_and_logs_failures() -> None:
    """peek_all() catches per-bucket exceptions, logs a warning, and
    continues — a redis-backend bucket peeked without a redis_client raises
    RuntimeError internally, which peek_all() must swallow while still
    returning results for the other (healthy) buckets."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("healthy", capacity=5.0))
    broken = TokenBucket(name="broken", capacity=5.0, refill_per_second=1.0, backend="redis")
    reg.register(broken)
    clock = FakeClock(_START)

    results = await reg.peek_all(clock=clock)

    assert "healthy" in results
    assert "broken" not in results


# ── reset() ────────────────────────────────────────────────────


async def test_reset_raises_typeerror_for_reservation() -> None:
    """reset() raises TypeError when name refers to a ConcurrencyReservation."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu", clock=clock))

    with pytest.raises(TypeError, match="ConcurrencyReservation"):
        await reg.reset("gpu")


async def test_reset_unregistered_name_raises_keyerror() -> None:
    """reset() raises KeyError for unregistered name."""
    reg = RateLimitRegistry()
    with pytest.raises(KeyError):
        await reg.reset("unknown")


async def test_reset_sliding_window() -> None:
    """reset() dispatches to SlidingWindow.reset() for non-TokenBucket primitives."""
    reg = RateLimitRegistry()
    reg.register(_sliding_window("sw", limit=2))
    clock = FakeClock(_START)

    async with reg.acquire("sw", clock=clock) as d1:
        assert d1.remaining == 1.0

    await reg.reset("sw", clock=clock)

    async with reg.acquire("sw", clock=clock) as d2:
        assert d2.remaining == 1.0


# ── sync_rate_limit_buckets() ───────────────────────────────────


class _FakeConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def execute(self, sql: str, name: str, kind: str) -> None:
        self.calls.append((sql, name, kind))


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self.conn)


async def test_sync_rate_limit_buckets_invalid_schema_raises() -> None:
    """sync_rate_limit_buckets() raises ValueError for a schema identifier
    that fails the shared _IDENT_RE validation."""
    reg = RateLimitRegistry()
    pool: Any = _FakePool()

    with pytest.raises(ValueError, match="invalid schema identifier"):
        await sync_rate_limit_buckets(reg, pool, schema="not valid; drop table")


async def test_sync_rate_limit_buckets_writes_token_bucket_and_gcra() -> None:
    """sync_rate_limit_buckets() upserts TokenBucket and GCRA SlidingWindow
    entries, and skips log-style SlidingWindow (no PG backend to sync)."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("tb"))
    reg.register(SlidingWindow(name="gcra_sw", limit=5, window=timedelta(minutes=1), style="gcra"))
    reg.register(_sliding_window("log_sw"))  # style="log" — should be skipped

    pool = _FakePool()
    await sync_rate_limit_buckets(reg, pool, schema="taskq")  # type: ignore[arg-type]

    synced = {(name, kind) for _, name, kind in pool.conn.calls}
    assert synced == {("tb", "token_bucket"), ("gcra_sw", "gcra")}

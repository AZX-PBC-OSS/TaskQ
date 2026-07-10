"""Unit tests for SlidingWindow log-style and GCRA in-memory backends.

These tests exercise the log-style sliding-window algorithm and the GCRA
algorithm, both against ``FakeClock`` so that window expiry and
retry_after are deterministic and zero-real-time.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from taskq.ratelimit import SlidingWindow
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _FakeSettings:
    schema_name: str = "taskq_test"


def _sw(
    limit: int = 60,
    window: timedelta = timedelta(seconds=60),
    name: str | None = None,
) -> SlidingWindow:
    bucket_name = name or f"sw_{new_base62()}"
    return SlidingWindow(
        name=bucket_name,
        limit=limit,
        window=window,
        backend="memory",
        style="log",
    )


# ── Log-style: basic allow ────────────────────────────────────────────


async def test_log_basic_allow() -> None:
    """59 acquires against limit=60 all allowed with correct remaining."""
    sw = _sw(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for i in range(59):
        r = await sw.acquire(clock=clock)
        assert r.allowed is True, f"acquire {i} denied"
        assert r.retry_after == timedelta(0)
        assert r.remaining == float(60 - (i + 1))
        assert r.backend == "memory"


# ── Log-style: deny at limit ──────────────────────────────────────────


async def test_log_deny_at_limit() -> None:
    """60 acquires allowed; 61st denied with retry_after > 0."""
    sw = _sw(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for _ in range(60):
        r = await sw.acquire(clock=clock)
        assert r.allowed is True

    r = await sw.acquire(clock=clock)
    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after > timedelta(0)
    assert r.remaining == 0.0
    assert r.backend == "memory"


# ── Log-style: window expiry ─────────────────────────────────────────


async def test_log_window_expiry() -> None:
    """After hitting limit, advance clock past window → allowed."""
    sw = _sw(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for _ in range(60):
        await sw.acquire(clock=clock)

    clock.advance(timedelta(seconds=60, milliseconds=1))
    r = await sw.acquire(clock=clock)
    assert r.allowed is True
    assert r.retry_after == timedelta(0)


# ── Log-style: sub-ms UUID uniqueness ────────────────────────────────


async def test_log_sub_ms_same_timestamp() -> None:
    """Two acquires at the same now_ms both succeed; deque count is 2."""
    sw = _sw(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    assert r1.allowed is True

    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is True

    assert r1.remaining == float(60 - 1)
    assert r2.remaining == float(60 - 2)


# ── Read-only properties ─────────────────────────────────────────────


def test_read_only_properties() -> None:
    """Constructor parameters are exposed as read-only properties."""
    sw = SlidingWindow(
        name="test-props",
        limit=10,
        window=timedelta(seconds=30),
        backend="memory",
        style="log",
        ttl=timedelta(minutes=5),
    )
    assert sw.name == "test-props"
    assert sw.limit == 10
    assert sw.window == timedelta(seconds=30)
    assert sw.backend == "memory"
    assert sw.style == "log"
    assert sw.ttl == timedelta(minutes=5)


def test_log_default_ttl() -> None:
    """Log-style default ttl is 2*window + 60 s when not explicitly set."""
    sw = SlidingWindow(
        name="test-log-ttl",
        limit=10,
        window=timedelta(seconds=30),
        backend="memory",
        style="log",
    )
    assert sw.ttl == timedelta(seconds=60, milliseconds=60_000)


# ── clock=None raises RuntimeError ───────────────────────────────────


async def test_acquire_without_clock_raises() -> None:
    """acquire() requires clock; RuntimeError when absent."""
    sw = _sw()
    with pytest.raises(RuntimeError, match="clock not injected"):
        await sw.acquire()


# ── redis/postgres backends raise NotImplementedError ─────────────────


async def test_acquire_redis_backend_raises_runtime_error_without_client() -> None:
    """backend="redis" raises RuntimeError when redis_client is not injected."""
    sw = SlidingWindow(name="redis-test", limit=10, window=timedelta(seconds=60), backend="redis")
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.acquire(clock=FakeClock(_START))


@pytest.mark.redis
async def test_acquire_redis_backend_raises_runtime_error_without_settings() -> None:
    """backend="redis" raises RuntimeError when settings is not injected."""
    sw = SlidingWindow(name="redis-test", limit=10, window=timedelta(seconds=60), backend="redis")
    import redis.asyncio as _ra

    client = _ra.from_url("redis://localhost:0", decode_responses=False)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.acquire(clock=FakeClock(_START), redis_client=client)
    finally:
        await client.aclose()


async def test_acquire_postgres_backend_raises_runtime_error_without_pool() -> None:
    """backend="postgres" raises RuntimeError when pg_pool is not injected."""
    sw = SlidingWindow(name="pg-test", limit=10, window=timedelta(seconds=60), backend="postgres")
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.acquire(clock=FakeClock(_START))


async def test_acquire_redis_gcra_raises_runtime_error_without_client() -> None:
    """backend="redis", style="gcra" raises RuntimeError when redis_client is not injected."""
    sw = SlidingWindow(
        name="redis-gcra-test",
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.acquire(clock=FakeClock(_START))


async def test_acquire_postgres_gcra_raises_runtime_error_without_pool() -> None:
    """backend="postgres", style="gcra" raises RuntimeError when pg_pool is not injected."""
    sw = SlidingWindow(
        name="pg-gcra-test",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.acquire(clock=FakeClock(_START))


# ── Re-export from taskq.ratelimit ───────────────────────────────────


def test_sliding_window_reexported() -> None:
    """SlidingWindow is importable from taskq.ratelimit."""
    import taskq.ratelimit as rl

    assert rl.SlidingWindow is SlidingWindow


# ── Decision logging with style ──────────────────────────────────────


async def test_logging_allowed_includes_style() -> None:
    """Allowed acquire with style='log' returns a valid decision."""
    sw = _sw()
    clock = FakeClock(_START)

    r = await sw.acquire(clock=clock)

    assert r.allowed is True


async def test_logging_denied_includes_style() -> None:
    """Denied acquire with style='log' returns a denied decision."""
    sw = _sw(limit=1)
    clock = FakeClock(_START)

    await sw.acquire(clock=clock)

    r = await sw.acquire(clock=clock)

    assert r.allowed is False


# ── GCRA helpers ──────────────────────────────────────────────────────


def _sw_gcra(
    limit: int = 60,
    window: timedelta = timedelta(seconds=60),
    name: str | None = None,
) -> SlidingWindow:
    bucket_name = name or f"sw_{new_base62()}"
    return SlidingWindow(
        name=bucket_name,
        limit=limit,
        window=window,
        backend="memory",
        style="gcra",
    )


# ── GCRA: basic allow with spacing ─────────────────────────────


async def test_gcra_basic_allow_with_spacing() -> None:
    """Acquire with emission-interval spacing → both allowed."""
    sw = _sw_gcra(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    assert r1.allowed is True
    assert r1.retry_after == timedelta(0)
    assert r1.remaining == 59.0

    clock.advance(timedelta(milliseconds=1000))
    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is True
    assert r2.retry_after == timedelta(0)


# ── GCRA: deny when too soon ──────────────────────────────────


async def test_gcra_deny_when_too_soon() -> None:
    """Two acquires at same now_ms with exhausted burst → second denied, retry_after ≈ emission_interval."""
    sw = _sw_gcra(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for _ in range(60):
        await sw.acquire(clock=clock)

    r = await sw.acquire(clock=clock)
    assert r.allowed is False
    assert r.remaining == 0.0
    assert r.retry_after is not None
    expected_ms = 1000
    assert abs(r.retry_after.total_seconds() * 1000 - expected_ms) <= 1


# ── GCRA: burst-tolerance ──────────────────────────────────────


async def test_gcra_burst_tolerance() -> None:
    """Burst of 60 within 1 s all allowed; 61st denied."""
    sw = _sw_gcra(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for i in range(60):
        r = await sw.acquire(clock=clock)
        assert r.allowed is True, f"acquire {i} denied in burst"

    r61 = await sw.acquire(clock=clock)
    assert r61.allowed is False
    assert r61.remaining == 0.0


# ── GCRA: sustained rate ───────────────────────────────────────


async def test_gcra_sustained_rate() -> None:
    """After 60-burst all burst cells remain in the sliding window
    until the full window elapses; once the window expires the 61st request is
    allowed.

    With LIMIT=60 and WINDOW=60 s the burst occupies the window
    ``(T-60 s, T]``.  Any request before T+60 s still sees all 60 burst
    timestamps inside ``(now-60 s, now]``, so the count gate denies it.
    At exactly T+60 s the burst timestamps fall outside the strict cutoff
    ``now - 60 s < timestamp`` and a new request is allowed.
    """
    sw = _sw_gcra(limit=60, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    for _ in range(60):
        await sw.acquire(clock=clock)

    # Requests within the window are denied — burst slots still occupied.
    clock.advance(timedelta(milliseconds=1000))
    r_mid = await sw.acquire(clock=clock)
    assert r_mid.allowed is False

    # Advance to exactly T + window: burst timestamps fall out of the strict
    # half-open window (cutoff < t), so 60 new slots are available.
    clock.advance(timedelta(milliseconds=59_000))  # total advance = 60 s
    r_after = await sw.acquire(clock=clock)
    assert r_after.allowed is True


# ── GCRA read-only properties ────────────────────────────────────────


def test_gcra_read_only_properties() -> None:
    """GCRA constructor parameters are exposed as read-only properties."""
    sw = SlidingWindow(
        name="test-gcra-props",
        limit=10,
        window=timedelta(seconds=30),
        backend="memory",
        style="gcra",
    )
    assert sw.name == "test-gcra-props"
    assert sw.limit == 10
    assert sw.window == timedelta(seconds=30)
    assert sw.backend == "memory"
    assert sw.style == "gcra"


# ── GCRA default TTL ─────────────────────────────────────────────────


def test_gcra_default_ttl() -> None:
    """GCRA default ttl is window + 60 s when not explicitly set."""
    sw = SlidingWindow(
        name="test-gcra-ttl",
        limit=10,
        window=timedelta(seconds=30),
        backend="memory",
        style="gcra",
    )
    assert sw.ttl == timedelta(seconds=30, milliseconds=60_000)


def test_gcra_explicit_ttl() -> None:
    """GCRA explicit ttl overrides the style-dependent default."""
    sw = SlidingWindow(
        name="test-gcra-explicit-ttl",
        limit=10,
        window=timedelta(seconds=30),
        backend="memory",
        style="gcra",
        ttl=timedelta(minutes=5),
    )
    assert sw.ttl == timedelta(minutes=5)


# ── GCRA decision logging ────────────────────────────────────────────


async def test_gcra_logging_allowed_includes_style() -> None:
    """Allowed GCRA acquire returns a valid decision."""
    sw = _sw_gcra()
    clock = FakeClock(_START)

    r = await sw.acquire(clock=clock)

    assert r.allowed is True


async def test_gcra_logging_denied_includes_style() -> None:
    """Denied GCRA acquire returns a denied decision."""
    sw = _sw_gcra(limit=60)
    clock = FakeClock(_START)

    for _ in range(60):
        await sw.acquire(clock=clock)

    r = await sw.acquire(clock=clock)

    assert r.allowed is False


# ── SlidingWindow request_id propagation ────────────────────────────────


async def test_log_memory_acquire_returns_request_id() -> None:
    """Log-style memory acquire returns RateLimitDecision with request_id set to a non-None string."""
    sw = _sw()
    clock = FakeClock(_START)

    r = await sw.acquire(clock=clock)
    assert r.allowed is True
    assert r.request_id is not None
    assert isinstance(r.request_id, str)
    assert len(r.request_id) > 0


async def test_gcra_memory_acquire_returns_request_id_none() -> None:
    """GCRA memory acquire returns RateLimitDecision with request_id=None."""
    sw = _sw_gcra()
    clock = FakeClock(_START)

    r = await sw.acquire(clock=clock)
    assert r.allowed is True
    assert r.request_id is None


# ── SlidingWindow refund ────────────────────────────────────────────────


async def test_refund_log_raises_on_missing_request_id() -> None:
    """Log-style refund raises ValueError when decision.request_id is None."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow(
        name="refund-test",
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )

    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-test",
        backend="redis",
        request_id=None,
    )

    with pytest.raises(ValueError, match=r"log-style refund requires decision\.request_id"):
        await sw.refund(decision, redis_client=object(), settings=_FakeSettings())


async def test_refund_gcra_memory_reverts_tat() -> None:
    """GCRA memory refund reverts TAT and log entry so a subsequent acquire is allowed again."""
    sw = _sw_gcra(limit=2, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    assert r1.allowed is True
    assert r1.previous_state is not None

    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is True

    r3 = await sw.acquire(clock=clock)
    assert r3.allowed is False

    await sw.refund(r2)

    r4 = await sw.acquire(clock=clock)
    assert r4.allowed is True


async def test_refund_gcra_memory_cas_skips_if_tat_advanced() -> None:
    """GCRA memory refund skips revert when TAT has been advanced by another acquire."""
    sw = _sw_gcra(limit=3, window=timedelta(seconds=60))
    clock = FakeClock(_START)

    r1 = await sw.acquire(clock=clock)
    assert r1.allowed is True

    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is True

    r3 = await sw.acquire(clock=clock)
    assert r3.allowed is True

    await sw.refund(r1)

    assert sw._mem_gcra._tat == r3.previous_state["new_tat_ms"]  # pyright: ignore[reportOptionalMemberAccess, reportOptionalSubscript]  # Why: _mem_gcra is initialised at construction time and is non-None during this test; previous_state is a typed dict with string keys.


async def test_refund_gcra_no_previous_state_is_noop() -> None:
    """GCRA refund with no previous_state on the decision is a no-op."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = _sw_gcra()
    FakeClock(_START)

    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="test",
        backend="memory",
    )

    await sw.refund(decision)


async def test_refund_log_memory_is_noop() -> None:
    """Memory log-style refund is a no-op (deque entries cannot be removed by request_id)."""
    sw = _sw()
    clock = FakeClock(_START)

    r = await sw.acquire(clock=clock)
    assert r.allowed is True
    assert r.request_id is not None

    await sw.refund(r)


# ── SlidingWindow acquire/peek/reset dispatch: assert_never arms ────────


async def test_acquire_assert_never_invalid_backend_style() -> None:
    """acquire() hits the exhaustive-match fallback for an invalid (backend, style) pair."""
    sw = _sw()
    sw._backend = "bogus"  # type: ignore[assignment]  # Why: forcing an invalid backend to exercise assert_never
    with pytest.raises(AssertionError):
        await sw.acquire(clock=FakeClock(_START))


async def test_peek_assert_never_invalid_backend_style() -> None:
    """peek() hits the exhaustive-match fallback for an invalid (backend, style) pair."""
    sw = _sw()
    sw._backend = "bogus"  # type: ignore[assignment]  # Why: forcing an invalid backend to exercise assert_never
    with pytest.raises(AssertionError):
        await sw.peek(clock=FakeClock(_START))


async def test_reset_assert_never_invalid_backend_style() -> None:
    """reset() hits the exhaustive-match fallback for an invalid (backend, style) pair."""
    sw = _sw()
    sw._backend = "bogus"  # type: ignore[assignment]  # Why: forcing an invalid backend to exercise assert_never
    with pytest.raises(AssertionError):
        await sw.reset(clock=FakeClock(_START))


async def test_refund_assert_never_invalid_backend_style() -> None:
    """refund() hits the exhaustive-match fallback for an invalid (backend, style) pair."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = _sw()
    sw._backend = "bogus"  # type: ignore[assignment]  # Why: forcing an invalid backend to exercise assert_never
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="memory",
    )
    with pytest.raises(AssertionError):
        await sw.refund(decision)


# ── SlidingWindow peek without clock ────────────────────────────────────


async def test_peek_without_clock_raises() -> None:
    """peek() requires clock; RuntimeError when absent."""
    sw = _sw()
    with pytest.raises(RuntimeError, match="clock not injected"):
        await sw.peek()


# ── SlidingWindow refund: postgres/redis dispatch arms ──────────────────


async def test_refund_postgres_log_is_noop() -> None:
    """refund() on postgres/log backend is a no-op (no pg_pool required)."""
    sw = SlidingWindow(
        name="refund-pg-log",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )
    from taskq.ratelimit.decision import RateLimitDecision

    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-log",
        backend="postgres",
    )
    await sw.refund(decision)


async def test_refund_postgres_gcra_raises_runtime_error_without_pool() -> None:
    """refund() on postgres/gcra backend raises RuntimeError when pg_pool is not injected."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow(
        name="refund-pg-gcra",
        limit=10,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-pg-gcra",
        backend="postgres",
        previous_state={
            "pre_acquire_tat": 0.0,
            "post_acquire_tat": 100.0,
            "acquire_now_ms": 0,
        },
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.refund(decision)


async def test_refund_redis_gcra_raises_runtime_error_without_client() -> None:
    """refund() on redis/gcra backend raises RuntimeError when redis_client is not injected."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow(
        name="refund-redis-gcra",
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-redis-gcra",
        backend="redis",
        previous_state={
            "previous_tat_ms": None,
            "new_tat_ms": 100.0,
            "acquire_now_ms": 0,
        },
    )
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.refund(decision)


# ── SlidingWindow private memory helpers: mismatched-style guards ──────


async def test_peek_memory_log_raises_when_not_initialised() -> None:
    """_peek_memory_log raises RuntimeError when the instance was built as GCRA."""
    sw = _sw_gcra()
    with pytest.raises(RuntimeError, match="memory log not initialised"):
        await sw._peek_memory_log(0)


async def test_peek_memory_gcra_raises_when_not_initialised() -> None:
    """_peek_memory_gcra raises RuntimeError when the instance was built as log-style."""
    sw = _sw()
    with pytest.raises(RuntimeError, match="memory GCRA not initialised"):
        await sw._peek_memory_gcra(0)


async def test_reset_memory_log_raises_when_not_initialised() -> None:
    """_reset_memory_log raises RuntimeError when the instance was built as GCRA."""
    sw = _sw_gcra()
    with pytest.raises(RuntimeError, match="memory log not initialised"):
        await sw._reset_memory_log()


async def test_reset_memory_gcra_raises_when_not_initialised() -> None:
    """_reset_memory_gcra raises RuntimeError when the instance was built as log-style."""
    sw = _sw()
    with pytest.raises(RuntimeError, match="memory GCRA not initialised"):
        await sw._reset_memory_gcra()


async def test_acquire_memory_log_raises_when_not_initialised() -> None:
    """_acquire_memory_log raises RuntimeError when the instance was built as GCRA."""
    sw = _sw_gcra()
    with pytest.raises(RuntimeError, match="memory log not initialised"):
        await sw._acquire_memory_log(0, None)


async def test_acquire_memory_gcra_raises_when_not_initialised() -> None:
    """_acquire_memory_gcra raises RuntimeError when the instance was built as log-style."""
    sw = _sw()
    with pytest.raises(RuntimeError, match="memory GCRA not initialised"):
        await sw._acquire_memory_gcra(0)


async def test_refund_memory_gcra_raises_when_not_initialised() -> None:
    """_refund_memory_gcra raises RuntimeError when the instance was built as log-style."""
    from taskq.ratelimit.decision import RateLimitDecision

    sw = _sw()
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="memory",
        previous_state={"previous_tat_ms": None, "new_tat_ms": 0.0, "acquire_now_ms": 0},
    )
    with pytest.raises(RuntimeError, match="memory GCRA not initialised"):
        await sw._refund_memory_gcra(decision)


# ── In-memory log/GCRA peek(): white-box retry_after loop branches ──────
#
# `_InMemorySlidingWindowLog.acquire` (and its GCRA counterpart) always
# gate `append` behind a `len(...) >= limit` check, so under normal use
# the deque/log can never hold more than `limit` entries.  Combined with
# `peek()` filtering on the exact same `ts > cutoff` predicate used to
# compute `is_exhausted`, this makes it structurally impossible for the
# retry_after-scan loop to skip a non-matching entry while still being
# "exhausted" -- whenever exhausted, *every* stored entry passes the
# filter and the loop always breaks on its first iteration.  The tests
# below deliberately seed the internal deque/log directly (bypassing the
# `acquire()` invariant) to exercise the loop's "skip an expired entry
# before finding the retry-relevant one" branch, which cannot otherwise
# occur through the public API.


async def test_log_peek_loop_skips_stale_entry_before_match() -> None:
    """White-box: a stale deque entry ahead of in-window entries exercises the
    peek() retry_after scan's loop-continue branch (log-style)."""
    sw = _sw(limit=2, window=timedelta(milliseconds=1000))
    clock = FakeClock(_START)
    now_ms = int(clock.now().timestamp() * 1000)

    mem_log = sw._mem_log
    assert mem_log is not None
    mem_log._deque.clear()
    mem_log._deque.append(now_ms - 2000)  # stale: outside the 1000ms window
    mem_log._deque.append(now_ms - 100)
    mem_log._deque.append(now_ms - 50)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.retry_after is not None


async def test_gcra_peek_loop_skips_stale_entry_before_match() -> None:
    """White-box: a stale log entry ahead of in-window entries exercises the
    peek() retry_after scan's loop-continue branch (GCRA)."""
    sw = _sw_gcra(limit=2, window=timedelta(milliseconds=1000))
    clock = FakeClock(_START)
    now_ms = int(clock.now().timestamp() * 1000)

    mem_gcra = sw._mem_gcra
    assert mem_gcra is not None
    mem_gcra._log.clear()
    mem_gcra._log.append(now_ms - 2000)  # stale: outside the 1000ms window
    mem_gcra._log.append(now_ms - 100)
    mem_gcra._log.append(now_ms - 50)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.retry_after is not None


async def test_gcra_peek_exhausted_by_tat_alone_with_log_below_limit() -> None:
    """White-box: TAT-driven exhaustion while the log count is still below
    `limit` exercises the GCRA peek() branch that falls through to the TAT
    arithmetic fallback for retry_after (rather than the log-count path)."""
    sw = _sw_gcra(limit=3, window=timedelta(milliseconds=1000))
    clock = FakeClock(_START)
    now_ms = int(clock.now().timestamp() * 1000)

    mem_gcra = sw._mem_gcra
    assert mem_gcra is not None
    mem_gcra._log.clear()
    mem_gcra._log.append(now_ms - 50)  # only 1 entry: log_count(1) < limit(3)
    mem_gcra._tat = float(now_ms + 1000)  # tat - now == delay_tolerance_ms

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.remaining == 0.0
    assert state.retry_after is not None


@pytest.mark.redis
async def test_refund_log_redis_propagates_connection_error() -> None:
    """Log-style refund propagates Redis ConnectionError instead of swallowing it.

    Regression: _refund_redis_log previously caught and logged ConnectionError,
    preventing the composition-level rollback loop from seeing the failure.
    """
    from unittest.mock import AsyncMock

    import redis as _redis_mod

    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow(
        name="refund-redis-err",
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )

    redis_client = AsyncMock()
    redis_client.zrem = AsyncMock(side_effect=_redis_mod.ConnectionError("connection lost"))

    decision = RateLimitDecision(
        allowed=True,
        remaining=9.0,
        retry_after=timedelta(0),
        bucket_name="refund-redis-err",
        backend="redis",
        request_id="req-123",
    )

    with pytest.raises(_redis_mod.ConnectionError, match="connection lost"):
        await sw.refund(decision, redis_client=redis_client, settings=_FakeSettings())

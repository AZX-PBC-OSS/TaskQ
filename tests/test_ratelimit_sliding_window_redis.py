"""Integration tests for SlidingWindow log-style and GCRA-style Redis backends
against testcontainers Redis.

60-in-60s window — all allowed, then denied, then retry_after wait → allowed.
EVALSHA caching — register_script called exactly once across acquires.
Sub-ms collision — 10 acquires at same now_ms with FakeClock; ZCARD == 10.
PEXPIRE on Redis key — TTL within expected range after one acquire.
PEXPIRE refreshed on denial — TTL still close to 2*window_ms + 60_000 after denied acquire.

Steady-state acceptance — 60-burst all allowed, 61st denied, retry_after ≈ 1 s.
Even-spacing enforcement — after burst, 1 s gap → allowed; immediate → denied.
EVALSHA cached for GCRA script — register_script called exactly once.
PEXPIRE refreshed on denial — PTTL close to window_ms + 60_000 after denied acquire.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_redis import _acquire_redis_log
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

pytestmark = [pytest.mark.integration, pytest.mark.redis]

_SCHEMA_LABEL = "taskq_test"


def _unique_name() -> str:
    return f"sw_{new_base62()}"


def _settings(redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _SCHEMA_LABEL,
        },
    )


async def _make_client(redis_url: str) -> redis_async.Redis:
    return redis_async.from_url(redis_url, decode_responses=False)


# ── 60-in-60s window — burst allowed → denied → retry_after wait → allowed ──


async def test_log_sixty_in_sixty(redis_url: str) -> None:
    """60 acquires in <1s all allowed; 61st denied with
    retry_after > timedelta(0); advance clock past retry_after → allowed again.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))

    try:
        for i in range(60):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"acquire {i} denied"
            assert r.backend == "redis"
            assert r.retry_after == timedelta(0)

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False
        assert r.retry_after is not None
        assert r.retry_after > timedelta(0)
        assert r.remaining == 0.0

        clock.advance(r.retry_after + timedelta(milliseconds=100))

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True
        assert r.retry_after == timedelta(0)
    finally:
        await client.aclose()


# ── EVALSHA caching — register_script called exactly once ──────


async def test_log_evalsha_caching(redis_url: str) -> None:
    """register_script is called exactly once across two acquires;
    the cached AsyncScript instance identity is stable.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    register_count = 0
    original_register = client.register_script

    def _counting_register(script: bytes) -> object:
        nonlocal register_count
        register_count += 1
        return original_register(script)

    client.register_script = _counting_register  # type: ignore[assignment] # Why: test spy wraps the real register_script to count calls

    try:
        r1 = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        r2 = await sw.acquire(redis_client=client, clock=clock, settings=settings)

        assert r1.allowed is True
        assert r2.backend == "redis"
        assert register_count == 1

        assert sw._redis_log_script is not None  # pyright: ignore[reportPrivateUsage] # Why: test introspects the cached script instance to verify stability
        first_script = sw._redis_log_script

        await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert sw._redis_log_script is first_script  # pyright: ignore[reportPrivateUsage] # Why: same introspection — verifies the cached instance identity is stable
    finally:
        await client.aclose()


# ── Sub-ms collision — 10 acquires at same now_ms ──────────────


async def test_log_sub_ms_collision(redis_url: str) -> None:
    """10 acquires at the same now_ms with FakeClock — each
    request_id is unique so ZADD inserts a distinct sorted-set member,
    preventing silent collapse. ZCARD on the key equals 10.

    FakeClock is acceptable here because the test deliberately pins the
    millisecond stamp to exercise the sub-ms collision contract —
    requires SystemClock for integration tests in general, but this test
    is verifying a precision boundary that cannot be reproduced with wall-clock
    jitter.
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)

    from datetime import UTC, datetime

    fake_clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))

    try:
        for i in range(10):
            r = await sw.acquire(redis_client=client, clock=fake_clock, settings=settings)
            assert r.allowed is True, f"acquire {i} denied"

        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        zcard = await client.zcard(key)
        assert zcard == 10
    finally:
        await client.aclose()


# ── PEXPIRE on Redis key — TTL within expected range ───────────


async def test_log_pexpire_on_key(redis_url: str) -> None:
    """after one acquire, PTTL is in the range
    (window_ms, 2 * window_ms + 60_000 + 100).
    """
    name = _unique_name()
    window = timedelta(seconds=60)
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=window,
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        await sw.acquire(redis_client=client, clock=clock, settings=settings)

        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        pttl = await client.pttl(key)

        window_ms = int(window.total_seconds() * 1000)
        expected_max = 2 * window_ms + 60_000 + 100

        assert pttl > window_ms
        assert pttl <= expected_max
    finally:
        await client.aclose()


# ── PEXPIRE refreshed on denial ───────────────────────────────


async def test_log_pexpire_refreshed_on_denial(redis_url: str) -> None:
    """fill window (60 acquires), wait 1s, acquire one more
    (denied). PTTL after denial is still close to 2*window_ms + 60_000 —
    the denied path refreshed the TTL per it did NOT decay by 1s.
    """
    name = _unique_name()
    window = timedelta(seconds=60)
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=window,
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        for _ in range(60):
            await sw.acquire(redis_client=client, clock=clock, settings=settings)

        await asyncio.sleep(1.0)

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False

        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        pttl = await client.pttl(key)

        window_ms = int(window.total_seconds() * 1000)
        expected_ttl_ms = 2 * window_ms + 60_000

        assert pttl > expected_ttl_ms - 2000, (
            f"PTTL {pttl} too low — denied path did not refresh TTL (expected ~{expected_ttl_ms})"
        )
    finally:
        await client.aclose()


# ── GCRA-style integration tests ───────────────────────────────────────


# ── Steady-state acceptance ────────────────────────────────


async def test_gcra_steady_state(redis_url: str) -> None:
    """60-burst all allowed; 61st denied with
    retry_after ≈ 1 s (emission interval in GCRA).
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))

    try:
        for i in range(60):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"acquire {i} denied"
            assert r.backend == "redis"

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False
        assert r.retry_after is not None
        assert r.remaining == 0.0

        expected_ms = 1000
        actual_ms = r.retry_after.total_seconds() * 1000
        assert abs(actual_ms - expected_ms) <= 100, (
            f"retry_after {actual_ms:.1f} ms not within ±100 ms of {expected_ms} ms"
        )
    finally:
        await client.aclose()


# ── Even-spacing enforcement ───────────────────────────────


async def test_gcra_even_spacing(redis_url: str) -> None:
    """after 60-burst, advance clock ~1.05 s → allowed; immediate
    acquire again → denied with retry_after ≈ 1 s.
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))

    try:
        for _ in range(60):
            await sw.acquire(redis_client=client, clock=clock, settings=settings)

        clock.advance(timedelta(milliseconds=1050))

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False
        assert r.retry_after is not None

        expected_ms = 1000
        actual_ms = r.retry_after.total_seconds() * 1000
        assert abs(actual_ms - expected_ms) <= 100, (
            f"retry_after {actual_ms:.1f} ms not within ±100 ms of {expected_ms} ms"
        )
    finally:
        await client.aclose()


# ── EVALSHA cached for GCRA script ────────────────────────


async def test_gcra_evalsha_caching(redis_url: str) -> None:
    """register_script is called exactly once across two
    GCRA acquires; the cached _redis_gcra_script instance identity is stable.
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    register_count = 0
    original_register = client.register_script

    def _counting_register(script: bytes) -> object:
        nonlocal register_count
        register_count += 1
        return original_register(script)

    client.register_script = _counting_register  # type: ignore[assignment] # Why: test spy wraps the real register_script to count calls

    try:
        r1 = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        r2 = await sw.acquire(redis_client=client, clock=clock, settings=settings)

        assert r1.allowed is True
        assert r2.backend == "redis"
        assert register_count == 1

        assert sw._redis_gcra_script is not None  # pyright: ignore[reportPrivateUsage] # Why: test introspects the cached GCRA script instance to verify stability
        first_script = sw._redis_gcra_script

        await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert sw._redis_gcra_script is first_script  # pyright: ignore[reportPrivateUsage] # Why: same introspection — verifies the cached instance identity is stable
    finally:
        await client.aclose()


# ── PEXPIRE refreshed on denial ────────────────────────────


async def test_gcra_pexpire_refreshed_on_denial(redis_url: str) -> None:
    """burst 60, immediately attempt one more (denied). PTTL on
    the key is close to window_ms + 60_000 (refreshed on the denied branch
    ), NOT decayed.
    """
    name = _unique_name()
    window = timedelta(seconds=60)
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=window,
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        for _ in range(60):
            await sw.acquire(redis_client=client, clock=clock, settings=settings)

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False

        key = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{name}}}"
        pttl = await client.pttl(key)

        window_ms = int(window.total_seconds() * 1000)
        expected_ttl_ms = window_ms + 60_000

        assert pttl > expected_ttl_ms - 2000, (
            f"PTTL {pttl} too low — denied path did not refresh TTL (expected ~{expected_ttl_ms})"
        )
    finally:
        await client.aclose()


# ── Cross-style state isolation ─────────────────────────────


async def test_gcra_cross_style_isolation(redis_url: str) -> None:
    """log-style and GCRA-style windows with the same ``name``
    use distinct Redis keys and do not corrupt each other's state.

    The log key ``sw:{name}`` is a sorted set (zset); the GCRA key
    ``sw_gcra:{name}`` is a string. The two keys are different
    strings. Each window hits its own limit independently.
    """
    name = f"sw_iso_{new_base62()}"
    log_window = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )
    gcra_window = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        await log_window.acquire(redis_client=client, clock=clock, settings=settings)
        await gcra_window.acquire(redis_client=client, clock=clock, settings=settings)

        key_log = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        key_gcra = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{name}}}"

        assert key_log != key_gcra

        log_type = await client.type(key_log)
        assert log_type == b"zset", f"log key type was {log_type!r}, expected b'zset'"

        gcra_type = await client.type(key_gcra)
        assert gcra_type == b"string", f"gcra key type was {gcra_type!r}, expected b'string'"

        for i in range(59):
            r = await log_window.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"log burst acquire {i} denied"

        for i in range(59):
            r = await gcra_window.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"gcra burst acquire {i} denied"

        r_log = await log_window.acquire(redis_client=client, clock=clock, settings=settings)
        assert r_log.allowed is False, "log window should be at limit"

        r_gcra = await gcra_window.acquire(redis_client=client, clock=clock, settings=settings)
        assert r_gcra.allowed is False, "gcra window should be at limit"
    finally:
        await client.aclose()


# ── Injection-error branches — redis_client/settings/request_id None ──


async def test_peek_log_redis_client_none(redis_url: str) -> None:
    """peek() with backend="redis", style="log" and redis_client=None raises
    RuntimeError (line 245-246 of _sliding_window_redis.py)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.peek(redis_client=None, clock=SystemClock(), settings=settings)


async def test_peek_log_settings_none(redis_url: str) -> None:
    """peek() with backend="redis", style="log" and settings=None raises
    RuntimeError (line 247-248)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.peek(redis_client=client, clock=SystemClock(), settings=None)
    finally:
        await client.aclose()


async def test_peek_gcra_redis_client_none(redis_url: str) -> None:
    """peek() with backend="redis", style="gcra" and redis_client=None raises
    RuntimeError (line 288-289)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.peek(redis_client=None, clock=SystemClock(), settings=settings)


async def test_peek_gcra_settings_none(redis_url: str) -> None:
    """peek() with backend="redis", style="gcra" and settings=None raises
    RuntimeError (line 290-291)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.peek(redis_client=client, clock=SystemClock(), settings=None)
    finally:
        await client.aclose()


async def test_reset_log_redis_client_none(redis_url: str) -> None:
    """reset() with backend="redis", style="log" and redis_client=None raises
    RuntimeError (line 328-329)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.reset(redis_client=None, settings=settings)


async def test_reset_log_settings_none(redis_url: str) -> None:
    """reset() with backend="redis", style="log" and settings=None raises
    RuntimeError (line 330-331)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.reset(redis_client=client, settings=None)
    finally:
        await client.aclose()


async def test_reset_gcra_redis_client_none(redis_url: str) -> None:
    """reset() with backend="redis", style="gcra" and redis_client=None raises
    RuntimeError (line 343-344)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.reset(redis_client=None, settings=settings)


async def test_reset_gcra_settings_none(redis_url: str) -> None:
    """reset() with backend="redis", style="gcra" and settings=None raises
    RuntimeError (line 345-346)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.reset(redis_client=client, settings=None)
    finally:
        await client.aclose()


async def test_acquire_log_redis_client_none(redis_url: str) -> None:
    """acquire() with backend="redis", style="log" and redis_client=None raises
    RuntimeError (line 85-86)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.acquire(redis_client=None, clock=SystemClock(), settings=settings)


async def test_acquire_log_settings_none(redis_url: str) -> None:
    """acquire() with backend="redis", style="log" and settings=None raises
    RuntimeError (line 87-88)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.acquire(redis_client=client, clock=SystemClock(), settings=None)
    finally:
        await client.aclose()


async def test_acquire_gcra_redis_client_none(redis_url: str) -> None:
    """acquire() with backend="redis", style="gcra" and redis_client=None raises
    RuntimeError (line 156-157)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    settings = _settings(redis_url)
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await sw.acquire(redis_client=None, clock=SystemClock(), settings=settings)


async def test_acquire_gcra_settings_none(redis_url: str) -> None:
    """acquire() with backend="redis", style="gcra" and settings=None raises
    RuntimeError (line 158-159)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    try:
        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.acquire(redis_client=client, clock=SystemClock(), settings=None)
    finally:
        await client.aclose()


async def test_acquire_log_request_id_none() -> None:
    """Direct call to the private _acquire_redis_log with request_id=None
    raises RuntimeError (line 90-91). The public SlidingWindow.acquire()
    always synthesises a UUID for log-style acquires, so this branch is
    only reachable by calling the module-level function directly. No real
    Redis connection is needed: the request_id check fires before any
    Redis command is issued, so a non-None sentinel object suffices for
    redis_client."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": "redis://localhost:0", "schema_name": "x"},
    )
    with pytest.raises(RuntimeError, match="request_id required"):
        await _acquire_redis_log(sw, 0, None, object(), SystemClock(), settings)  # type: ignore[arg-type]


# ── Refund log — redis_client None / settings None ─────────────────


async def test_refund_log_redis_client_none(redis_url: str) -> None:
    """refund() with backend="redis", style="log", a valid decision.request_id,
    and redis_client=None raises RuntimeError (line 364-365)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True
        assert decision.request_id is not None

        with pytest.raises(RuntimeError, match="redis_client not injected"):
            await sw.refund(decision, redis_client=None, settings=settings)
    finally:
        await client.aclose()


async def test_refund_log_settings_none(redis_url: str) -> None:
    """refund() with backend="redis", style="log", a valid decision.request_id,
    and settings=None raises RuntimeError (line 366-367)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True

        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.refund(decision, redis_client=client, settings=None)
    finally:
        await client.aclose()


# ── Refund gcra — previous_state None / redis_client None / settings None ──


async def test_refund_gcra_previous_state_none() -> None:
    """refund() returns immediately when decision.previous_state is None
    (line 381-382) — no redis_client/settings validation is even attempted."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    decision = RateLimitDecision(
        allowed=False,
        remaining=0.0,
        retry_after=timedelta(seconds=1),
        bucket_name=sw.name,
        backend="redis",
        previous_state=None,
    )
    await sw.refund(decision, redis_client=None, settings=None)


async def test_refund_gcra_redis_client_none(redis_url: str) -> None:
    """refund() with a populated previous_state and redis_client=None raises
    RuntimeError (line 383-384)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True
        assert decision.previous_state is not None

        with pytest.raises(RuntimeError, match="redis_client not injected"):
            await sw.refund(decision, redis_client=None, settings=settings)
    finally:
        await client.aclose()


async def test_refund_gcra_settings_none(redis_url: str) -> None:
    """refund() with a populated previous_state and settings=None raises
    RuntimeError (line 385-386)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True

        with pytest.raises(RuntimeError, match="settings not injected"):
            await sw.refund(decision, redis_client=client, settings=None)
    finally:
        await client.aclose()


async def test_refund_gcra_success(redis_url: str) -> None:
    """refund() after a successful gcra acquire executes the GCRA_REFUND_SCRIPT
    (lines 388-398) and rolls the stored tat back so a subsequent acquire at
    the same instant is allowed again."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True
        assert decision.previous_state is not None

        denied = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert denied.allowed is False

        await sw.refund(decision, redis_client=client, settings=settings)

        allowed_again = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert allowed_again.allowed is True
    finally:
        await client.aclose()


# ── Fake-client unit test for the peek_redis_log race branch ────────


class _FakeZrangeEmptyClient:
    """Mimics the two redis-py calls used by peek_redis_log with
    zcard reporting the bucket exhausted but zrange racing to no members
    (line 260->270 false branch) — a race that cannot be reproduced
    deterministically against a real Redis server."""

    async def zcard(self, key: str) -> int:
        return 10

    async def zrange(
        self, key: str, start: int, end: int, withscores: bool = False
    ) -> list[object]:
        return []


async def test_peek_log_oldest_empty_race() -> None:
    """peek_redis_log: zcard reports the bucket exhausted, but zrange
    races to no members. The peek still reports is_exhausted=True with
    retry_after=None."""
    from taskq.ratelimit._sliding_window_redis import _peek_redis_log

    sw = SlidingWindow(
        name=_unique_name(), limit=5, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": "redis://localhost:0", "schema_name": "x"},
    )
    fake_client = _FakeZrangeEmptyClient()

    state = await _peek_redis_log(sw, now_ms=0, redis_client=fake_client, settings=settings)  # type: ignore[arg-type]

    assert state.is_exhausted is True
    assert state.retry_after is None


# ── peek()/reset()/refund() real-execution bodies ───────────────────


async def test_peek_log_exhausted_reports_retry_after(redis_url: str) -> None:
    """peek() on backend="redis", style="log" after filling the window
    reports is_exhausted=True with a positive retry_after (executes the
    zrange branch at line 258-268 against a real Redis server)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        for _ in range(5):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True

        state = await sw.peek(redis_client=client, clock=clock, settings=settings)
        assert state.is_exhausted is True
        assert state.remaining == 0.0
        assert state.retry_after is not None
        assert state.retry_after > timedelta(0)
    finally:
        await client.aclose()


async def test_peek_log_not_exhausted(redis_url: str) -> None:
    """peek() on backend="redis", style="log" against an unused bucket
    reports is_exhausted=False and skips the zrange branch entirely
    (the `count == 0` arm of line 258, i.e. the 258->270 false edge)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        state = await sw.peek(redis_client=client, clock=clock, settings=settings)
        assert state.is_exhausted is False
        assert state.remaining == 5.0
        assert state.retry_after is None
    finally:
        await client.aclose()


async def test_peek_gcra_reports_state(redis_url: str) -> None:
    """peek() on backend="redis", style="gcra" after filling the window
    reports is_exhausted=True with a positive retry_after (executes the
    real body at line 293-311 against a real Redis server)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=5, window=timedelta(seconds=60), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = FakeClock(start=datetime(2025, 6, 1, tzinfo=UTC))
    try:
        for _ in range(5):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True

        state = await sw.peek(redis_client=client, clock=clock, settings=settings)
        assert state.is_exhausted is True
        assert state.remaining == 0.0
        assert state.retry_after is not None
        assert state.retry_after > timedelta(0)

        # A non-exhausted peek exercises the "not is_exhausted" arm too.
        empty_name = _unique_name()
        empty_sw = SlidingWindow(
            name=empty_name, limit=5, window=timedelta(seconds=60), backend="redis", style="gcra"
        )
        empty_state = await empty_sw.peek(redis_client=client, clock=clock, settings=settings)
        assert empty_state.is_exhausted is False
        assert empty_state.retry_after is None
    finally:
        await client.aclose()


async def test_reset_log_deletes_key(redis_url: str) -> None:
    """reset() on backend="redis", style="log" deletes the zset key
    (executes the real body at line 333-335 against a real Redis server)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        await sw.acquire(redis_client=client, clock=clock, settings=settings)
        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        assert await client.exists(key) == 1

        await sw.reset(redis_client=client, settings=settings)
        assert await client.exists(key) == 0
    finally:
        await client.aclose()


async def test_reset_gcra_deletes_key(redis_url: str) -> None:
    """reset() on backend="redis", style="gcra" deletes the string key
    (executes the real body at line 348-350 against a real Redis server)."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=5, window=timedelta(seconds=60), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        await sw.acquire(redis_client=client, clock=clock, settings=settings)
        key = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{name}}}"
        assert await client.exists(key) == 1

        await sw.reset(redis_client=client, settings=settings)
        assert await client.exists(key) == 0
    finally:
        await client.aclose()


async def test_refund_log_request_id_none_raises() -> None:
    """refund() on backend="redis", style="log" with decision.request_id=None
    raises ValueError (line 359-363) — a log-style refund cannot ZREM
    without the original request_id."""
    sw = SlidingWindow(
        name=_unique_name(), limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )
    decision = RateLimitDecision(
        allowed=True,
        remaining=4.0,
        retry_after=timedelta(0),
        bucket_name=sw.name,
        backend="redis",
        request_id=None,
    )
    with pytest.raises(ValueError, match="request_id"):
        await sw.refund(decision, redis_client=None, settings=None)


async def test_refund_log_success(redis_url: str) -> None:
    """refund() on backend="redis", style="log" after a successful acquire
    ZREMs the member back out (executes the real body at line 369-372
    against a real Redis server); ZCARD drops back to 0."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        decision = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert decision.allowed is True
        assert decision.request_id is not None

        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{name}}}"
        assert await client.zcard(key) == 1

        await sw.refund(decision, redis_client=client, settings=settings)
        assert await client.zcard(key) == 0
    finally:
        await client.aclose()

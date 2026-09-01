"""Integration tests for SlidingWindow log-style and GCRA-style Redis backends
against testcontainers Redis.

The scripts derive now from redis TIME (the store's clock), so every
wait/elapsed scenario below uses REAL time — a FakeClock advance is
invisible to the store.

Burst fills window — all allowed, then denied, then retry_after wait → allowed.
EVALSHA caching — register_script called exactly once across acquires.
Sub-ms collision — 10 rapid acquires; unique request_id members; ZCARD == 10.
PEXPIRE on Redis key — TTL within expected range after one acquire.
PEXPIRE refreshed on denial — TTL still close to 2*window_ms + 60_000 after denied acquire.

Steady-state acceptance — 60-burst all allowed, 61st denied, retry_after ≈ 1 s.
Even-spacing enforcement — after burst, 1 s gap → allowed; immediate → denied.
EVALSHA cached for GCRA script — register_script called exactly once.
PEXPIRE refreshed on denial — PTTL close to window_ms + 60_000 after denied acquire.
"""

import asyncio
from datetime import timedelta

import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_redis import _acquire_redis_log
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings

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


async def test_log_burst_deny_wait_allows(redis_url: str) -> None:
    """60 acquires in <2s all allowed; 61st denied with retry_after >
    timedelta(0); sleep past retry_after of REAL time → allowed again.

    The script's window boundary and ZADD scores are TIME-domain (the
    store's clock), so the wait must be real time — a FakeClock advance
    would be invisible to the store. Window shrunk to 2 s so the
    retry_after wait stays test-fast.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=60,
        window=timedelta(seconds=2),
        backend="redis",
        style="log",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

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

        await asyncio.sleep(r.retry_after.total_seconds() + 0.15)

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
    """10 acquires in quick succession — each request_id is unique so ZADD
    inserts a distinct sorted-set member, preventing silent collapse even
    when two acquires land inside the same millisecond. ZCARD on the key
    equals 10.

    Scores are TIME-domain (the store's clock); same-millisecond collisions
    happen naturally under a fast burst, and the unique-member contract is
    what keeps them from collapsing.
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

    clock = SystemClock()

    try:
        for i in range(10):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
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
    """60-burst all allowed; 61st denied with retry_after <= one emission
    interval (10 s at window=600 s / limit=60).

    The TAT advances on the store's clock (TIME), so real elapsed time
    during the 60 roundtrips shaves the retry below the 10 s emission
    interval — the pinned invariant is 0 < retry_after <= 10 s. The wide
    window makes the denial deterministic: the 61st is denied for any
    inter-await gap under ~10 s, swallowing parallel-load scheduling
    stalls (measured up to ~4 s); a 60 s window left only ~1 s of margin
    and flaked under load.
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=600),
        backend="redis",
        style="gcra",
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        for i in range(60):
            r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"acquire {i} denied"
            assert r.backend == "redis"

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False
        assert r.retry_after is not None
        assert r.remaining == 0.0

        # GCRA retry_after = emission_interval (10 s) minus the real time
        # elapsed during the burst's roundtrips.
        assert 0 < r.retry_after.total_seconds() <= 10.0
    finally:
        await client.aclose()


# ── Even-spacing enforcement ───────────────────────────────


async def test_gcra_even_spacing(redis_url: str) -> None:
    """GCRA spacing is enforced in the store's clock domain.

    After a 60-burst and ~1.05 s of real time, the next acquire is allowed
    and advances the TAT by exactly one emission interval beyond the TAT it
    observed (the deterministic spacing demo). The follow-up acquire is
    denied only while less than ~one emission interval of store time has
    passed since that allowed one — under parallel load the gap between two
    awaits can exceed it, in which case admitting IS the contract, so the
    follow-up is asserted conditionally from the decision's own
    store-domain evidence: allowed ⇒ TAT advanced by ≥ one emission
    interval (spacing held); denied ⇒ bounded retry within one interval.
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

    try:
        for _ in range(60):
            await sw.acquire(redis_client=client, clock=clock, settings=settings)

        await asyncio.sleep(1.05)

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True
        # now < stored TAT here, so post = pre + exactly one emission
        # interval (1000 ms) — spacing, proven without any timing assumption.
        assert r.previous_state is not None
        pre_ms = float(str(r.previous_state["pre_acquire_tat_str"]))
        post_ms = float(str(r.previous_state["post_acquire_tat_str"]))
        assert post_ms - pre_ms >= 999.0

        r2 = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        if r2.allowed:
            assert r2.previous_state is not None
            r2_pre = float(str(r2.previous_state["pre_acquire_tat_str"]))
            r2_post = float(str(r2.previous_state["post_acquire_tat_str"]))
            assert r2_post - r2_pre >= 999.0
        else:
            assert r2.retry_after is not None
            assert 0 < r2.retry_after.total_seconds() <= 1.0
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

    window=600 s makes the denial deterministic: the immediate 61st is
    denied for any inter-await gap under ~10 s (one emission interval),
    swallowing parallel-load scheduling stalls.
    """
    name = _unique_name()
    window = timedelta(seconds=600)
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
        # 600 s window: the gcra arm's at-limit denial must survive any
        # parallel-load scheduling stall (denied for gaps < ~10 s, one
        # emission interval); the log arm's denial is count-based and
        # already stall-immune at 60 s.
        window=timedelta(seconds=600),
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
        await _acquire_redis_log(sw, None, object(), settings)  # type: ignore[arg-type]


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
    and rolls the stored TIME-domain tat back so an immediate subsequent
    acquire is allowed again."""
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=10), backend="redis", style="gcra"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
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
    """Mimics the redis-py calls used by peek_redis_log with zcount
    reporting the bucket exhausted but the oldest-in-window lookup racing
    to no members — a race that cannot be reproduced deterministically
    against a real Redis server."""

    async def time(self) -> list[int]:
        return [2000, 0]

    async def zcount(self, key: str, min: str, max: str) -> int:
        return 10

    async def zrangebyscore(
        self,
        key: str,
        min: str,
        max: str,
        start: int = 0,
        num: int = 1,
        withscores: bool = False,
    ) -> list[object]:
        return []


async def test_peek_log_oldest_empty_race() -> None:
    """peek_redis_log: the in-window count reports the bucket exhausted,
    but the oldest-in-window lookup races to no members. The peek still
    reports is_exhausted=True with retry_after=None."""
    from taskq.ratelimit._sliding_window_redis import _peek_redis_log

    sw = SlidingWindow(
        name=_unique_name(), limit=5, window=timedelta(seconds=10), backend="redis", style="log"
    )
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": "redis://localhost:0", "schema_name": "x"},
    )
    fake_client = _FakeZrangeEmptyClient()

    state = await _peek_redis_log(sw, redis_client=fake_client, settings=settings)  # type: ignore[arg-type]

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
    reports is_exhausted=False and skips the oldest-lookup branch entirely."""
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


async def test_peek_log_after_window_expiry_not_exhausted(redis_url: str) -> None:
    """Read-only peek must apply the window filter itself. Eviction only
    happens in acquire, so once the window empties with no intervening
    acquire the sorted set still holds the aged-out entries — the peek
    must count only in-window entries (ZCOUNT against the store's TIME)
    and report NOT exhausted, because the very next acquire IS allowed.
    Pre-fix the peek used ZCARD (the whole key) and overstated exhaustion.
    """
    name = _unique_name()
    sw = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=5), backend="redis", style="log"
    )
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()
    try:
        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True

        state = await sw.peek(redis_client=client, clock=clock, settings=settings)
        assert state.is_exhausted is True

        # Age the entry past the 5 s window with NO intervening acquire —
        # the key is not evicted until the next acquire runs. (5 s, not
        # 1 s: the exhausted-peek above needs margin against scheduling
        # stalls under parallel load.)
        await asyncio.sleep(5.2)

        state = await sw.peek(redis_client=client, clock=clock, settings=settings)
        assert state.is_exhausted is False, (
            "peek must count only in-window entries — the window has "
            "emptied even though the key still holds the aged-out entry"
        )
        assert state.remaining == 1.0
        assert state.retry_after is None

        r = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True, "the next acquire after window expiry must be allowed"
    finally:
        await client.aclose()


async def test_acquire_and_peek_without_clock_on_redis_backend(redis_url: str) -> None:
    """Unified clock contract (matches TokenBucket): the redis backend
    runs on the store's own clock (Redis TIME inside the scripts / peek
    reads), so an acquire/peek must not require a Python clock at all.
    Both styles."""
    settings = _settings(redis_url)
    client = await _make_client(redis_url)
    try:
        sw_log = SlidingWindow(
            name=_unique_name(),
            limit=10,
            window=timedelta(seconds=60),
            backend="redis",
            style="log",
        )
        r = await sw_log.acquire(redis_client=client, settings=settings)
        assert r.allowed is True
        assert r.backend == "redis"
        state = await sw_log.peek(redis_client=client, settings=settings)
        assert state.backend == "redis"

        sw_gcra = SlidingWindow(
            name=_unique_name(),
            limit=10,
            window=timedelta(seconds=60),
            backend="redis",
            style="gcra",
        )
        r = await sw_gcra.acquire(redis_client=client, settings=settings)
        assert r.allowed is True
        assert r.backend == "redis"
        state = await sw_gcra.peek(redis_client=client, settings=settings)
        assert state.backend == "redis"
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

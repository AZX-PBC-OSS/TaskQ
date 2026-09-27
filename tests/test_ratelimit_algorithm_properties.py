"""Algorithm-property attacks on the rate-limit primitives (red-team wave 2).

Each test hypothesizes a way the ratelimit ALGORITHMS' mathematical
properties could break, writes the experiment, and pins the survivor:

* **Concurrency conservation** — N concurrent acquires against one bucket
  must never grant more than ``capacity`` (plus honest refill): the
  ``_InMemoryBucket`` lock, the token-bucket Lua script, the log-window
  Lua script, and the GCRA Lua script are the four serialization points;
  a non-atomic read-modify-write in any of them leaks admissions.
* **GCRA burst boundary** — pure GCRA with ``delay_tolerance = window``
  admits up to ``limit + 1`` real timestamps in one window at boundary
  conditions (the documented divergence from the stricter in-memory
  twin); the Redis script's admission count is bounded by
  ``(window + arrival spread) / emission_interval + 1`` no matter how the
  racers interleave.
* **TAT chain monotonicity** — every allowed GCRA acquire advances the
  TAT by exactly one emission interval and persists it; denied acquires
  leave it untouched. The post-TATs of concurrent allowed acquires are
  therefore distinct and spaced by at least one emission interval.
* **Clock-step skew** — the memory backends' injected clock stepped
  BACKWARD mid-stream must never produce negative refill, a TAT
  regression, or a tokens/remaining value outside ``[0, capacity]``.
  (Redis and PG ignore the injected clock entirely; the store-domain
  pins live in ``test_ratelimit_clock_domain.py``.)
* **Refund conservation** — a refund is a return of spent quota, never a
  mint: refund without acquire, double refund, and refund after the
  bucket rolled over must all be no-ops capped at capacity (the Redis
  refund script's ``tokens == nil`` fence and the GCRA refund's
  compare-and-set are the two fences under attack).
* **PG-fallback ledger** — a Redis reply lost AFTER the script's effects
  landed (the classic partial-write shape) re-runs the script on each
  transient retry and then grants from Postgres: one caller-visible
  acquire can charge the REDIS ledger up to
  ``RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS`` tokens AND the PG ledger
  one admission. The caller still sees exactly ONE decision; this test
  pins the ledger-divergence BOUND (attempts, not unbounded) and proves
  the double-charge is real, so a future "fix" that makes the retry
  unbounded or lets the fallback re-enter the caller's admission loop
  trips here.
* **Keyed cap under concurrency** — the ``max_keyed_rate_limits``
  guardrail's check-then-stamp region is await-free, so even concurrent
  materialization of distinct keys can never push the tracked-entry dict
  past the cap; every loser gets ``ReservationUnavailable``.

Requires Docker (testcontainers Redis; the ledger probe also needs PG).
The memory-backend sections are deterministic (``FakeClock``) and
marker-free, so they run in every environment. Note honestly: the memory
acquire paths are currently await-free inside their locks, so the
``gather`` probes there serialize in practice — they pin the public-API
contract so that an ``await`` added later to those paths cannot silently
break conservation; the Redis sections below exercise TRUE concurrency
against the scripts.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import cast

import asyncpg
import pytest
import redis as _redis_mod
import redis.asyncio as redis_async
from pydantic import BaseModel

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.constants import RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit.decision import RateLimitDecision
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

_START = datetime(2025, 1, 1, tzinfo=UTC)
_SCHEMA_LABEL = "taskq_test"


class _DefaultPayload(BaseModel):
    tenant_id: str


def _unique_name() -> str:
    return f"algo_{new_base62()}"


def _redis_settings(redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _SCHEMA_LABEL,
        },
    )


# ── A. Memory-backend concurrency conservation (deterministic) ────────


async def test_memory_token_bucket_concurrent_burst_conservation() -> None:
    """Pinned invariant: N concurrent acquires against one in-memory bucket
    with a FROZEN clock (zero elapsed → zero refill) grant at most
    ``capacity`` admissions, and every decision's remaining stays in
    ``[0, capacity]``.

    Regression: if ``_InMemoryBucket.acquire``'s ``asyncio.Lock`` were
    dropped (or its read-modify-write escaped the lock), a racer would
    win the ``tokens >= count`` check against a stale balance and the
    gather would grant more than ``capacity`` admissions for a fixed
    budget.
    """
    tb = TokenBucket(
        name=_unique_name(),
        capacity=10,
        refill_per_second=0.5,
        backend="memory",
    )
    clock = FakeClock(_START)

    results = await asyncio.gather(*[tb.acquire(clock=clock) for _ in range(50)])

    allowed = [r for r in results if r.allowed]
    denied = [r for r in results if not r.allowed]

    # Frozen clock: elapsed == 0 exactly, so refill contributes nothing
    # and the bucket is a fixed quota of 10.
    assert len(allowed) == 10, f"conservation violated: {len(allowed)} granted"
    assert len(denied) == 40
    for r in results:
        assert 0.0 <= r.remaining <= 10.0
    for r in denied:
        assert r.retry_after is not None and r.retry_after > timedelta(0)


async def test_memory_log_concurrent_burst_conservation() -> None:
    """Pinned invariant: N concurrent log-style acquires at one timestamp
    grant at most ``limit`` admissions; every denial reports a positive
    retry hint (the oldest entry's window exit).

    Regression: a non-atomic eviction-then-count-then-append in
    ``_InMemorySlidingWindowLog.acquire`` could let two racers both observe
    ``len(deque) < limit`` and both append, exceeding the limit.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=8,
        window=timedelta(seconds=1),
        backend="memory",
        style="log",
    )
    clock = FakeClock(_START)

    results = await asyncio.gather(*[sw.acquire(clock=clock) for _ in range(30)])

    allowed = [r for r in results if r.allowed]
    assert len(allowed) == 8, f"conservation violated: {len(allowed)} granted"
    for r in results:
        if not r.allowed:
            assert r.retry_after is not None
            assert r.retry_after.total_seconds() > 0


async def test_memory_gcra_concurrent_burst_and_tat_chain() -> None:
    """Pinned invariant: under concurrent acquires at one timestamp the
    in-memory GCRA twin grants at most ``limit`` admissions AND the allowed
    acquires' post-TATs are distinct, strictly increasing, spaced by
    EXACTLY one emission interval (each allowance advances the TAT by
    ``quantity_ms``; denials leave it untouched).

    Regression: a lost TAT update (a read-modify-write race on
    ``self._tat``) would show up as duplicate post-TATs or a spacing other
    than the emission interval while still granting ``limit`` admissions.
    """
    limit = 10
    window_ms = 1000
    sw = SlidingWindow(
        name=_unique_name(),
        limit=limit,
        window=timedelta(seconds=1),
        backend="memory",
        style="gcra",
    )
    clock = FakeClock(_START)
    now_ms = _START.timestamp() * 1000
    emission_ms = window_ms / limit

    results = await asyncio.gather(*[sw.acquire(clock=clock) for _ in range(30)])

    allowed = [r for r in results if r.allowed]
    assert len(allowed) == limit, f"conservation violated: {len(allowed)} granted"

    posts = [float(r.previous_state["new_tat_ms"]) for r in allowed]  # type: ignore[index]  # Why: previous_state is dict[str, object] on the decision; the allowed GCRA arm always populates these keys
    assert len(set(posts)) == len(posts), "duplicate post-TATs: a TAT update was lost"
    assert posts == sorted(posts), "post-TATs not monotone in grant order"
    for a, b in pairwise(posts):
        assert b - a == pytest.approx(emission_ms, rel=1e-9)

    for r in results:
        if not r.allowed:
            assert r.previous_state is None

    # The chain is anchored at the acquire instant: post_TAT_k = now + k*e.
    for k, post in enumerate(posts, start=1):
        assert post == pytest.approx(now_ms + k * emission_ms, rel=1e-9)


# ── B. Clock-step skew on the memory backends ─────────────────────────


async def test_memory_token_bucket_backward_clock_step_bounded() -> None:
    """Pinned invariant: a clock stepped BACKWARD mid-stream never produces
    negative refill and never a token count outside ``[0, capacity]``; the
    damage of a backward step of skew S is bounded by the re-grant of at
    most ``S * refill`` tokens (capped at capacity) when the clock
    re-advances over the same interval.

    Regression: dropping the ``max(0.0, now_ts - self._ts)`` clamp in
    ``_InMemoryBucket`` would make the backward step itself a negative
    elapsed (deflating tokens into a permanent debt); dropping the
    ``min(capacity, ...)`` cap would let the re-advance explode the bucket
    past capacity.
    """
    tb = TokenBucket(
        name=_unique_name(),
        capacity=10,
        refill_per_second=1.0,
        backend="memory",
    )
    clock = FakeClock(_START)

    for _ in range(10):
        r = await tb.acquire(clock=clock)
        assert r.allowed
    drained = await tb.peek(clock=clock)
    assert drained.tokens_remaining == 0.0

    # Backward step of 2 hours: elapsed must clamp to 0, no negative
    # refill, no debt.
    clock.move_to(_START - timedelta(hours=2))
    r = await tb.acquire(clock=clock)
    assert r.allowed is False
    assert r.remaining == 0.0
    state = await tb.peek(clock=clock)
    assert 0.0 <= state.tokens_remaining <= 10.0

    # Re-advance through the original instant: the interval [T-2h, T] is
    # refilled a second time — the documented bounded divergence — but the
    # count is capped at capacity and the subsequent spend keeps remaining
    # inside [0, capacity].
    clock.move_to(_START)
    r2 = await tb.acquire(clock=clock)
    assert r2.allowed is True
    assert 0.0 <= r2.remaining <= 10.0


async def test_memory_gcra_backward_clock_step_tat_never_regresses() -> None:
    """Pinned invariant: after a backward clock step the GCRA twin's TAT
    does not regress (``tat = max(tat, now)``), the acquire stays denied,
    and once the clock returns and the window empties, admission resumes
    from a TAT that never went backward.

    Regression: removing the ``max(tat, float(now_ms))`` clamp would let
    the stepped-back ``now`` pull the TAT backward, re-opening the burst
    budget the pre-skew acquires already consumed (a capacity explosion
    against the shared window).
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=5,
        window=timedelta(seconds=1),
        backend="memory",
        style="gcra",
    )
    clock = FakeClock(_START)

    for _ in range(5):
        r = await sw.acquire(clock=clock)
        assert r.allowed

    clock.move_to(_START - timedelta(hours=2))
    r = await sw.acquire(clock=clock)
    assert r.allowed is False
    assert r.retry_after is not None and r.retry_after > timedelta(0)

    # TAT is internal; verify behaviorally: back at the original instant
    # the twin must still deny (the log is full AND the TAT is in the
    # future), proving neither the TAT nor the log regressed.
    clock.move_to(_START)
    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is False

    clock.advance(timedelta(milliseconds=1001))
    r3 = await sw.acquire(clock=clock)
    assert r3.allowed is True


async def test_memory_log_backward_clock_step_denies_finitely() -> None:
    """Pinned invariant: a backward clock step on the log-style twin denies
    with a POSITIVE, FINITE retry hint (the oldest entry's window exit is
    now in the future), never a negative ``timedelta`` and never a crash.

    Regression: a negative ``retry_ms`` (``oldest + window - now`` with a
    stepped-back ``now``) would construct a negative ``timedelta`` — the
    zero-wait sentinel shape of an ALLOWED decision — turning the denial
    into a spin; an unguarded huge one would overflow the constructor.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=3,
        window=timedelta(seconds=1),
        backend="memory",
        style="log",
    )
    clock = FakeClock(_START)

    for _ in range(3):
        r = await sw.acquire(clock=clock)
        assert r.allowed

    clock.move_to(_START - timedelta(hours=2))
    r = await sw.acquire(clock=clock)
    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after > timedelta(0)
    assert r.retry_after.total_seconds() == pytest.approx(7201.0, rel=1e-6)

    clock.move_to(_START + timedelta(milliseconds=1001))
    r2 = await sw.acquire(clock=clock)
    assert r2.allowed is True


# ── C. Redis script concurrency (real container) ──────────────────────


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_token_bucket_concurrent_conservation(redis_url: str) -> None:
    """Pinned invariant: 100 concurrent acquires against ONE Redis token
    bucket (capacity 25, refill 0) grant EXACTLY 25 admissions — the Lua
    script's atomicity holds; every reply's ``tokens_remaining`` stays in
    ``[0, capacity]``.

    Regression: any non-atomic read-modify-write in the token-bucket script
    (HMGET → compute → HMSET split across round trips) would let concurrent
    racers all read the same pre-spend balance and over-admit.
    """
    tb = TokenBucket(
        name=_unique_name(),
        capacity=25,
        refill_per_second=0,
        backend="redis",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        results = await asyncio.gather(
            *[tb.acquire(redis_client=client, clock=clock, settings=settings) for _ in range(100)]
        )

        allowed = [r for r in results if r.allowed]
        assert len(allowed) == 25, f"conservation violated: {len(allowed)} granted"
        for r in results:
            assert 0.0 <= r.remaining <= 25.0
        for r in results:
            if not r.allowed:
                assert r.retry_after is None  # refill == 0: no recovery hint
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_log_window_concurrent_conservation(redis_url: str) -> None:
    """Pinned invariant: 60 concurrent acquires against one Redis log-style
    window grant EXACTLY ``limit`` admissions, the sorted set ends with
    exactly ``limit`` members, and every denial's retry hint is positive.

    Regression: a ZCARD then check then ZADD split (the script's steps 2-4
    running as separate round trips) would let concurrent racers all count
    the same pre-ZADD cardinality and overshoot the limit.
    """
    limit = 20
    sw = SlidingWindow(
        name=_unique_name(),
        limit=limit,
        window=timedelta(seconds=60),
        backend="redis",
        style="log",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        results = await asyncio.gather(
            *[sw.acquire(redis_client=client, clock=clock, settings=settings) for _ in range(60)]
        )

        allowed = [r for r in results if r.allowed]
        assert len(allowed) == limit, f"conservation violated: {len(allowed)} granted"
        for r in results:
            if not r.allowed:
                assert r.retry_after is not None
                assert r.retry_after.total_seconds() > 0

        key = f"taskq:{_SCHEMA_LABEL}:sw:{{{sw.name}}}"
        zcard = await client.zcard(key)
        assert zcard == limit
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_gcra_concurrent_burst_boundary_and_tat_chain(
    redis_url: str,
) -> None:
    """Pinned invariant (the burst boundary): 120 concurrent GCRA acquires
    (limit 100 / 600 s) admit at most
    ``(window + arrival_spread) / emission_interval + 1`` — pure GCRA with
    ``delay_tolerance = window`` admits the +1th boundary cell, the
    documented correct-upstream divergence from the stricter in-memory
    twin, and arrivals spread over the gather can only widen the bound —
    and never more, no matter how the racers interleave.

    Pinned invariant (the TAT chain): every allowed acquire's post-TAT
    exceeds its pre-TAT by EXACTLY one emission interval, the post-TATs of
    distinct allowed acquires are all distinct (each allowance consumed one
    interval; denials left the TAT untouched), consecutive allowed post-TATs
    are spaced by at least one emission interval (``tat = max(stored, now)``
    can only widen a gap), and no denied decision carries a TAT echo.

    Regression: a GCRA script that advanced the TAT on the DENIAL branch
    (or returned a stale TAT echo) would break the chain spacing or hand
    the refund CAS a wrong compare target; a check-then-set split would
    blow past the burst boundary under this gather.
    """
    limit = 100
    window_ms = 600_000  # 600 s: emission 6 s, far above any gather spread
    emission_ms = window_ms / limit
    sw = SlidingWindow(
        name=_unique_name(),
        limit=limit,
        window=timedelta(seconds=600),
        backend="redis",
        style="gcra",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *[sw.acquire(redis_client=client, clock=clock, settings=settings) for _ in range(120)]
        )
        spread_ms = (time.perf_counter() - t0) * 1000

        allowed = [r for r in results if r.allowed]
        # Self-calibrated pure-GCRA bound: arrivals spread over the gather
        # widen the window the boundary cell lives in, never shrink it.
        calibrated_bound = int((window_ms + spread_ms) / emission_ms) + 1
        assert len(allowed) <= calibrated_bound, (
            f"burst boundary violated: {len(allowed)} granted "
            f"(bound={calibrated_bound}, spread={spread_ms:.1f}ms)"
        )

        posts: list[float] = []
        for r in allowed:
            ps = r.previous_state
            assert ps is not None, "allowed GCRA decision lost its TAT echo"
            pre = float(ps["pre_acquire_tat_str"])  # type: ignore[arg-type]  # Why: the allowed arm always populates the string echoes
            post = float(ps["post_acquire_tat_str"])  # type: ignore[arg-type]
            assert post - pre == pytest.approx(emission_ms, rel=1e-9)
            posts.append(post)

        assert len(set(posts)) == len(allowed), "duplicate post-TATs: a TAT write was lost"
        for a, b in zip(sorted(posts), sorted(posts)[1:], strict=False):
            # tat = max(stored, now) can only WIDEN a gap between
            # consecutive allowances, never shrink it below one interval.
            assert b - a >= emission_ms * (1 - 1e-9)

        for r in results:
            if not r.allowed:
                assert r.previous_state is None
    finally:
        await client.aclose()


# ── D. Refund conservation (real container) ───────────────────────────


def _redis_decision(tb: TokenBucket) -> RateLimitDecision:
    """A hand-built redis-backend decision for driving ``refund`` directly.

    The refund probes attack the refund script's fences in isolation —
    without a matching acquire — which is exactly the "refund without
    acquire" hypothesis; the decision carries nothing beyond the backend
    tag the refund dispatches on.
    """
    return RateLimitDecision(
        allowed=True,
        remaining=0.0,
        retry_after=timedelta(0),
        bucket_name=tb.name,
        backend="redis",
    )


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_refund_without_acquire_mints_no_quota(redis_url: str) -> None:
    """Pinned invariant: a refund landing on a bucket key that does not
    exist (no acquire ever ran, or the key expired) is a NO-OP — it must
    not create the key and must not mint quota; the next acquire starts
    from FULL capacity minus only its own spend.

    Regression: removing the REFUND script's ``tokens == nil → return
    {0, 0}`` fence would HMSET a hash out of a refund alone, and a fixed
    quota (refill == 0) could be inflated back to full capacity by refunds
    for tokens that were never spent.
    """
    tb = TokenBucket(
        name=_unique_name(),
        capacity=5,
        refill_per_second=0,
        backend="redis",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        key = f"taskq:{_SCHEMA_LABEL}:rl:tb:{{{tb.name}}}"
        await tb.refund(_redis_decision(tb), redis_client=client, settings=settings)
        assert not await client.exists(key), "a refund without acquire created the bucket key"

        r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True
        # Full capacity minus exactly this acquire's spend: the orphan
        # refund minted nothing.
        assert r.remaining == 4.0
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_refund_after_ttl_rollover_mints_no_quota(redis_url: str) -> None:
    """Pinned invariant: a refund arriving AFTER the bucket key's TTL
    expired (the bucket "rolled over") is a no-op; the re-materialized
    bucket starts at full capacity and the stale refund cannot stack on
    top of it.

    Regression: an unfenced refund (HMSET without the nil check) would
    resurrect the expired key with capacity + refund tokens — for a fixed
    quota, the refund path alone could hand back the entire spent budget
    after the rollover that was supposed to be the fresh start.
    """
    tb = TokenBucket(
        name=_unique_name(),
        capacity=5,
        refill_per_second=0,
        backend="redis",
        ttl=timedelta(seconds=1),
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        key = f"taskq:{_SCHEMA_LABEL}:rl:tb:{{{tb.name}}}"
        r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True
        assert await client.exists(key) == 1

        await asyncio.sleep(1.3)  # TTL (1 s) rolls the bucket over
        await tb.refund(r, redis_client=client, settings=settings)
        assert not await client.exists(key), "a stale refund resurrected the expired key"

        r2 = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        assert r2.allowed is True
        assert r2.remaining == 4.0  # fresh full bucket, no phantom refund
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_gcra_refund_double_refund_is_noop(redis_url: str) -> None:
    """Pinned invariant: the GCRA refund's compare-and-set makes a DOUBLE
    refund a no-op — the second refund's ``post_acquire_tat`` no longer
    matches the stored TAT after the first refund rewound it — so the same
    admission can never be refunded twice.

    Regression: an unconditional TAT rewind (no CAS compare) would let a
    double refund rewind the TAT twice, handing back an emission interval
    of budget that was never spent twice.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        key = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{sw.name}}}"
        d = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert d.allowed is True and d.previous_state is not None
        pre = str(d.previous_state["pre_acquire_tat_str"])

        await sw.refund(d, redis_client=client, settings=settings)
        stored = await client.get(key)
        assert stored is not None and float(stored) == float(pre)

        # Second refund of the SAME decision: CAS must reject it.
        await sw.refund(d, redis_client=client, settings=settings)
        stored = await client.get(key)
        assert stored is not None and float(stored) == float(pre)
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_gcra_refund_after_tat_advanced_is_noop(redis_url: str) -> None:
    """Pinned invariant: a refund arriving AFTER another acquire advanced
    the TAT (the bucket "rolled over" in GCRA terms) is rejected by the
    CAS fence, and only the LATEST allowance's refund rewinds — refunds
    unwind in reverse acquire order, never clobbering a newer allowance.

    Regression: an unconditional rewind would apply acquire A's refund
    over acquire B's advanced TAT, erasing B's spend and granting an
    extra emission interval of budget.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        key = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{sw.name}}}"
        d1 = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        d2 = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert d1.allowed and d2.allowed
        assert d1.previous_state is not None and d2.previous_state is not None

        # Refund the OLDER allowance: the stored TAT is d2's post — the CAS
        # compare against d1's post fails and nothing is written.
        await sw.refund(d1, redis_client=client, settings=settings)
        stored = await client.get(key)
        assert stored is not None
        assert float(stored) == float(str(d2.previous_state["post_acquire_tat_str"]))

        # Refund the LATEST allowance: CAS matches, the TAT rewinds to d2's
        # pre (one interval back, exactly d1's post).
        await sw.refund(d2, redis_client=client, settings=settings)
        stored = await client.get(key)
        assert stored is not None
        assert float(stored) == float(str(d2.previous_state["pre_acquire_tat_str"]))
    finally:
        await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_gcra_refund_on_absent_key_is_noop(redis_url: str) -> None:
    """Pinned invariant: the GCRA refund against a MISSING key (reset,
    expired, or never written) is a no-op that does NOT re-create the key.

    Regression: the CAS script's ``if not existing then return {0}`` fence
    removed, the refund's unconditional SET would resurrect a reset bucket
    at its pre-acquire TAT — stale state for a bucket the operator wiped.
    """
    sw = SlidingWindow(
        name=_unique_name(),
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style="gcra",
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    settings = _redis_settings(redis_url)
    clock = SystemClock()

    try:
        key = f"taskq:{_SCHEMA_LABEL}:sw_gcra:{{{sw.name}}}"
        d = await sw.acquire(redis_client=client, clock=clock, settings=settings)
        assert d.allowed and d.previous_state is not None

        await sw.reset(redis_client=client, settings=settings)
        assert not await client.exists(key)

        await sw.refund(d, redis_client=client, settings=settings)
        assert not await client.exists(key), "a refund resurrected a reset bucket"
    finally:
        await client.aclose()


# ── E. The PG-fallback ledger probe (real Redis + real PG) ────────────


class _LostReplyProbe:
    """A redis-client shim whose scripted calls let the REAL script's
    effects land, then lose the reply as a ``ConnectionError`` for the
    first *failures* calls — the partial-write shape: the store executed
    the admission, the caller cannot know it."""

    def __init__(self, failures: int) -> None:
        self.failures_left = failures
        self.executions = 0

    def client(self, real: redis_async.Redis) -> redis_async.Redis:
        """A client-shaped shim wrapping *real*'s script registration."""
        probe = self
        real_register = real.register_script

        def register(script: bytes) -> object:
            inner = real_register(script)

            class _Script:
                async def __call__(self, **kwargs: object) -> object:
                    # The REAL script runs to completion: its effects land
                    # in the store before the reply is lost.
                    raw = await inner(**kwargs)  # type: ignore[operator]  # Why: inner is the untyped AsyncScript; the call shape (keys=/args=) is the bucket's own
                    probe.executions += 1
                    if probe.failures_left > 0:
                        probe.failures_left -= 1
                        raise _redis_mod.ConnectionError("reply lost after effects applied")
                    return raw

            return _Script()

        shim = _Shim(register)
        return cast(redis_async.Redis, shim)


class _Shim:
    """Minimal client surface for the acquire path: register_script only."""

    def __init__(self, register: "object") -> None:
        self._register = register

    def register_script(self, script: bytes) -> object:
        return self._register(script)  # type: ignore[operator]  # Why: the probe hands the closure in untyped


@pytest.mark.integration
@pytest.mark.redis
async def test_lost_redis_reply_charges_both_ledgers_within_a_bound(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """THE double-admission hypothesis, answered: a Redis reply lost AFTER
    the token-bucket script's effects landed, persisted through the whole
    transient-retry budget, ends in the PG fallback — one caller-visible
    acquire whose effects landed in BOTH stores.

    Pinned here (the accepted divergence and its bound):

    * the caller sees EXACTLY ONE decision (the PG one, allowed);
    * the REDIS ledger was charged
      ``RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS`` tokens (every retry
      re-executed the script and its effects landed), proving the partial
      write is REAL — the "did the script run?" bit is lost with the
      reply;
    * the PG ledger was charged ONE admission;
    * the total ledger charge is BOUNDED by the retry budget — a future
      change that makes the retry unbounded or lets the fallback re-enter
      the caller's admission loop trips this assertion.

    Not pinned as a defect: the divergence is the at-least-once cost of
    weathering a connection blip against a store whose scripts are not
    idempotent (no request id to dedupe on); bounding it IS the design
    (cc611f09's transient-retry machinery).
    """
    capacity = 10.0
    tb = TokenBucket(
        name=_unique_name(),
        capacity=capacity,
        refill_per_second=0,  # fixed quota: the ledger arithmetic is exact
        backend="redis",
    )
    real_client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
    probe = _LostReplyProbe(failures=RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS)
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "schema_name": module_pg_schema.schema_name,
            "redis_url": redis_url,
        },
    )
    clock = SystemClock()

    try:
        decision = await tb.acquire(
            redis_client=probe.client(real_client),
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

        # Exactly one decision reached the caller, and it is the PG arm's.
        assert decision.backend == "postgres"
        assert decision.allowed is True

        # Every retry re-executed the script and its effects LANDED: the
        # redis ledger is charged once per attempt, bounded by the budget.
        assert probe.executions == RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS

        key = f"taskq:{module_pg_schema.schema_name}:rl:tb:{{{tb.name}}}"
        tokens_raw = await real_client.hget(key, "tokens")
        assert tokens_raw is not None
        assert float(tokens_raw) == pytest.approx(capacity - probe.executions)

        # ...and the PG fallback ALSO granted: both stores hold a charge
        # for this one logical acquire (the double-admission, bounded).
        row = await module_pg_pool.fetchrow(
            "SELECT (state->>'tokens')::float8 AS tokens, "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; the value is $1-bound
            "(state->>'granted')::bool AS granted "
            f'FROM "{module_pg_schema.schema_name}".rate_limit_buckets '
            "WHERE bucket_name=$1",
            tb.name,
        )
        assert row is not None
        assert row["granted"] is True
        assert row["tokens"] == pytest.approx(capacity - 1.0)
    finally:
        await real_client.aclose()


# ── F. The keyed cap under concurrent materialization ─────────────────


async def test_keyed_cap_never_exceeded_under_concurrent_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pinned invariant: 100 concurrent materializations of DISTINCT keys
    against a cap of 10 admit at most 10 (every loser raises
    ``ReservationUnavailable``) and the tracked-entry dict never exceeds
    the cap — the guardrail's check-then-stamp region is await-free, so no
    interleave can slip an extra entry past the cap.

    Regression: moving an ``await`` between the cap check and the
    tracking-dict stamp in ``_resolve_rate_limit_name`` would let every
    racer pass the check at ``len == cap - 1`` and stamp together,
    overshooting the per-process cap the guardrail exists to enforce.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    async def _stub_publish(*args: object, **kwargs: object) -> None:
        # Yield one loop turn so the concurrent resolves genuinely
        # interleave around the publish await (the cap's live boundary).
        await asyncio.sleep(0)

    monkeypatch.setattr(registry_mod, "_upsert_rate_limit_bucket_row", _stub_publish)

    cap = 10
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_MAX_KEYED_RATE_LIMITS": str(cap),
            "TASKQ_SCHEMA_NAME": _SCHEMA_LABEL,
        },
        validate=False,
    )
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name=_unique_name(),
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0,
    )

    outcomes = await asyncio.gather(
        *[
            reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: the cap guard under attack lives on the private resolver; the public acquire_for_actor would couple the probe to the reservation arm
                ref,
                payload=_DefaultPayload(tenant_id=f"t{i}"),
                settings=settings,
                pg_pool=cast(
                    asyncpg.Pool, object()
                ),  # Why: non-None routes into the (stubbed) publish await, the interleave point
            )
            for i in range(100)
        ],
        return_exceptions=True,
    )

    resolved = [o for o in outcomes if not isinstance(o, BaseException)]
    unavailable = [o for o in outcomes if isinstance(o, ReservationUnavailable)]
    crashed = [
        o
        for o in outcomes
        if isinstance(o, BaseException) and not isinstance(o, ReservationUnavailable)
    ]

    assert not crashed, f"unexpected crash shape: {crashed!r}"
    assert len(resolved) + len(unavailable) == 100
    assert len(resolved) <= cap, f"cap violated: {len(resolved)} materialized"
    assert len(unavailable) == 100 - len(resolved)
    assert len(reg._keyed_rate_limit_last_used) <= cap  # pyright: ignore[reportPrivateUsage]  # Why: the tracked dict IS the structure the cap bounds

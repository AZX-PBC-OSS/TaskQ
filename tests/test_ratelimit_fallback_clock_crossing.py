"""Clock-crossing pins for the Redis TIME <-> PG clock_timestamp() fallback.

The rate-limit state machine runs on two store clocks: the Redis paths
stamp and measure with ``redis.call('TIME')`` (inside the Lua scripts) and
``TIME`` (the peek paths' ``redis_time_seconds``); the PG paths - the
``backend="postgres"`` primitives AND the outage fallback every
``backend="redis"`` acquire can land on - stamp and measure with
``statement_timestamp()``/``clock_timestamp()``. Both are wall clocks.
The failure mode this file hunts is a STAMP surviving the crossing: a
bucket whose ts/TAT/score was written by the OTHER store's clock, so the
recovering store's window math folds the skew in - behind, a phantom
refill (the double-spend window); ahead, a wedge (the permanent denial).

Harness: :class:`tests._redis_time_skew_proxy.RedisTimeSkewProxy` shifts
the client-visible ``TIME`` reply by +/-2 min against the real PG; the
Lua-side ``TIME`` is server-internal and a proxy cannot reach it, so
cross-domain STAMPS are induced by direct state writes in the other
domain - exactly the hypothesized outage residue. The pins:

* the acquire scripts never read a client-supplied now (wire-level skew
  cannot flip an admission, both directions);
* the peek paths measure against the store clock they can see, and their
  elapsed math clamps at zero (no phantom refill from a behind stamp);
* the PG fallback stamps its recovery in ITS OWN domain: a poisoned Redis
  stamp (either direction, +/-2 min) cannot move the PG row's ts/TAT one
  microsecond;
* failover back is symmetric: the Redis paths never read or write the PG
  row the outage wrote, so a PG-domain residue (either direction) cannot
  wedge or refill the Redis bucket;
* every persisted stamp is epoch-scale: no ``monotonic()`` value (the one
  Python clock that leaks into nothing) ever reaches either store;
* the progress flush crosses NO clock: the seq is the only order, and the
  flush statement carries no timestamp input at all.

The admitted, bounded divergence a stateless failover cannot avoid (the
other store starts from its own last state, so admission capacity during
an outage is bounded by outage duration + window, not eliminated) is
documented on the acquire docstrings and deliberately NOT pinned here as
a bug: it is the fallback's contract, not a skew defect.
"""

from datetime import timedelta

import asyncpg
import pytest
import redis as _redis_mod
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.progress._flush import (  # pyright: ignore[reportPrivateUsage]  # Why: the pin asserts the flush statement's exact clock-free contract.
    _FLUSH_UNNEST_BINDING_ORDER,  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    _flush_update_sql,  # pyright: ignore[reportPrivateUsage]  # Why: see above.
)
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit.decision import RateLimitState
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from tests._redis_time_skew_proxy import RedisTimeSkewProxy

pytestmark = [pytest.mark.integration, pytest.mark.redis]

_SKEW_S = 120  # the campaign's +/-2 min
_EPOCH_SLACK_S = 5  # stamp-vs-clock equality tolerance
_EPOCH_RANGE_S = 3600  # epoch-scale check: any monotonic() value fails this


class _OutageScript:
    async def __call__(self, **kwargs: object) -> object:
        raise _redis_mod.ConnectionError("connection lost")


class _OutageRedis:
    """A Redis client whose every scripted call fails, as in a real outage."""

    def register_script(self, script: bytes) -> object:
        return _OutageScript()


def _pg_settings(schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": schema.pg_dsn, "schema_name": schema.schema_name},
    )


def _redis_settings(redis_url: str, schema_name: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": schema_name},
    )


async def _pg_epoch_s(pool: asyncpg.Pool) -> float:
    return float(await pool.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))


async def _redis_epoch_s(client: redis_async.Redis) -> float:
    t = await client.time()
    return float(t[0]) + float(t[1]) / 1_000_000


# ── Harness validity: the proxy skews TIME and nothing else ──────────


async def test_skew_proxy_shifts_time_and_forwards_everything_else(redis_url: str) -> None:
    """The harness pin: through the proxy, ``TIME`` is shifted by exactly
    the offset, while an HMGET reply that is structurally identical to a
    TIME reply (two numeric bulk strings) and an EVAL return pass through
    untouched. A proxy that rewrote either would fake the peek pins below."""
    direct = redis_async.from_url(redis_url, decode_responses=False)
    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)
    proxy = RedisTimeSkewProxy(
        upstream_host, upstream_port, skew_seconds=_SKEW_S, upstream_db=upstream_db
    )
    await proxy.start()
    try:
        skewed = redis_async.from_url(proxy.url(), decode_responses=False)
        real = await _redis_epoch_s(direct)
        shifted = await _redis_epoch_s(skewed)
        assert abs(shifted - (real + _SKEW_S)) < 5, "TIME must arrive skewed"

        await direct.hset("skewproxy:h", mapping={"tokens": "1.5", "ts": str(int(real))})
        got = await skewed.hmget("skewproxy:h", ["tokens", "ts"])
        assert got == [b"1.5", str(int(real)).encode()], (
            "an HMGET reply shaped like TIME must be forwarded untouched"
        )
        raw = await skewed.eval("return {1, tostring(2.5), 'x'}", 0)
        assert raw == [1, b"2.5", b"x"], "script returns must be forwarded untouched"
        await skewed.aclose()
    finally:
        await proxy.stop()
        await direct.aclose()


# ── Wire-level acquire pins: client-visible TIME skew cannot flip an admission ──


async def test_token_bucket_acquire_ignores_client_visible_time_skew(redis_url: str) -> None:
    """capacity=2, refill=0.01/s. Drain the bucket, then acquire 1.1 tokens
    through a client whose TIME is +2 min. The script measures elapsed with
    the SERVER's own TIME (a proxy cannot reach it): ~0 s have passed, the
    1.1-token acquire is DENIED. A client-supplied-now acquire (the shape
    the C8 doctrine removed, whose now reads TIME client-side and passes
    ARGV) would see 120 s of elapsed = 1.2 phantom tokens and ADMIT."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"tb_wire_{new_base62()}"
    bucket = TokenBucket(name=name, capacity=2.0, refill_per_second=0.01, backend="redis")

    direct = redis_async.from_url(redis_url, decode_responses=False)
    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)
    proxy = RedisTimeSkewProxy(
        upstream_host, upstream_port, skew_seconds=_SKEW_S, upstream_db=upstream_db
    )
    await proxy.start()
    try:
        drained = await bucket.acquire(
            count=2.0, redis_client=direct, clock=SystemClock(), settings=settings
        )
        assert drained.allowed, "setup: the first acquire drains the full bucket"

        skewed = redis_async.from_url(proxy.url(), decode_responses=False)
        after = await bucket.acquire(
            count=1.1, redis_client=skewed, clock=SystemClock(), settings=settings
        )
        assert after.allowed is False, (
            "a +2 min client-visible TIME skew must not mint the elapsed tokens"
        )
        await skewed.aclose()
    finally:
        await proxy.stop()
        await direct.aclose()


async def test_sliding_window_log_acquire_ignores_client_visible_time_skew(
    redis_url: str,
) -> None:
    """limit=1/60s. The first admission is logged, then a second acquire
    rides a +2 min TIME-skewed client: the script's ZREMRANGEBYSCORE runs
    on the server's clock, the logged entry survives, DENIED. A
    client-supplied now would put the cutoff 60 s past the entry and
    evict it outright (admitted)."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"swlog_wire_{new_base62()}"
    window = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
    )

    direct = redis_async.from_url(redis_url, decode_responses=False)
    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)
    proxy = RedisTimeSkewProxy(
        upstream_host, upstream_port, skew_seconds=_SKEW_S, upstream_db=upstream_db
    )
    await proxy.start()
    try:
        first = await window.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
        assert first.allowed, "setup: the first admission fills the window"

        skewed = redis_async.from_url(proxy.url(), decode_responses=False)
        second = await window.acquire(redis_client=skewed, clock=SystemClock(), settings=settings)
        assert second.allowed is False, (
            "a +2 min client-visible TIME skew must not evict the in-window entry"
        )
        assert second.retry_after is not None and second.retry_after > timedelta(seconds=25)
        await skewed.aclose()
    finally:
        await proxy.stop()
        await direct.aclose()


async def test_sliding_window_gcra_acquire_ignores_client_visible_time_skew(
    redis_url: str,
) -> None:
    """limit=2/60s (emission 30 s). Two admissions burn the burst; a third
    through a +2 min TIME-skewed client must be DENIED - the script clamps
    the TAT with the server's clock. A client-supplied now would shove the
    TAT (and allow_at with it) 120 s into the future of the skewed clock
    and admit the third cell immediately."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"swgcra_wire_{new_base62()}"
    window = SlidingWindow(
        name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="gcra"
    )

    direct = redis_async.from_url(redis_url, decode_responses=False)
    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)
    proxy = RedisTimeSkewProxy(
        upstream_host, upstream_port, skew_seconds=_SKEW_S, upstream_db=upstream_db
    )
    await proxy.start()
    try:
        for _ in range(2):
            d = await window.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
            assert d.allowed, "setup: the first two admissions burn the burst"

        skewed = redis_async.from_url(proxy.url(), decode_responses=False)
        third = await window.acquire(redis_client=skewed, clock=SystemClock(), settings=settings)
        assert third.allowed is False, (
            "a +2 min client-visible TIME skew must not advance the shared TAT"
        )
        await skewed.aclose()
    finally:
        await proxy.stop()
        await direct.aclose()


# ── Peek pins: store-clock now, elapsed clamped at zero ──────────────


async def test_sliding_window_log_peek_measures_the_client_visible_store_clock(
    redis_url: str,
) -> None:
    """The log peek's window filter runs on the store clock the CLIENT
    sees (the only clock it can read). One admission is logged, then the
    peek rides +/-2 min TIME proxies: the +2 min cutoff puts the real
    entry outside the window (1 token available, the peek cannot see what
    its clock says aged out), the -2 min cutoff stretches the window over
    the entry (exhausted, retry hint ~3 min). Both are distinct from the
    unskewed peek (0 available, ~60 s hint), so a peek that read a Python
    clock instead of the store's fails either direction."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"swlog_peek_{new_base62()}"
    window = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
    )

    direct = redis_async.from_url(redis_url, decode_responses=False)
    first = await window.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
    assert first.allowed
    await direct.aclose()

    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)

    async def peek_through(skew: int) -> RateLimitState:
        proxy = RedisTimeSkewProxy(
            upstream_host, upstream_port, skew_seconds=skew, upstream_db=upstream_db
        )
        await proxy.start()
        try:
            client = redis_async.from_url(proxy.url(), decode_responses=False)
            state = await window.peek(redis_client=client, settings=settings)
            await client.aclose()
            return state
        finally:
            await proxy.stop()

    ahead = await peek_through(_SKEW_S)
    assert ahead.remaining == 1.0 and not ahead.is_exhausted, (
        f"the +2 min peek's cutoff must age the entry out of its window, got {ahead.remaining}"
    )

    behind = await peek_through(-_SKEW_S)
    assert behind.is_exhausted and behind.remaining == 0.0
    assert behind.retry_after is not None, "the stretched window must still report a hint"
    assert 150 <= behind.retry_after.total_seconds() <= 210, (
        f"the -2 min hint must be the entry's expiry on the skewed clock (~180 s), "
        f"got {behind.retry_after}"
    )


async def test_sliding_window_gcra_peek_measures_the_client_visible_store_clock(
    redis_url: str,
) -> None:
    """The GCRA peek's TAT/now arithmetic runs on the store clock the
    CLIENT sees. One admission advances the TAT half a window, then the
    peek rides +/-2 min TIME proxies: +2 min shoves now past the tolerance
    (exhausted, the boundary hint clamps to ~1 ms), -2 min keeps the stored
    TAT ahead of now (exhausted, hint ~2 min). An unskewed peek is NOT
    exhausted (1 cell of headroom), so a peek reading a Python clock fails
    the +2 min direction."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"swgcra_peek_{new_base62()}"
    window = SlidingWindow(
        name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="gcra"
    )

    direct = redis_async.from_url(redis_url, decode_responses=False)
    first = await window.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
    assert first.allowed
    await direct.aclose()

    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)

    async def peek_through(skew: int) -> RateLimitState:
        proxy = RedisTimeSkewProxy(
            upstream_host, upstream_port, skew_seconds=skew, upstream_db=upstream_db
        )
        await proxy.start()
        try:
            client = redis_async.from_url(proxy.url(), decode_responses=False)
            state = await window.peek(redis_client=client, settings=settings)
            await client.aclose()
            return state
        finally:
            await proxy.stop()

    ahead = await peek_through(_SKEW_S)
    # The clamp (tat = max(tat, now)) puts the TAT on the skewed now, so
    # the ahead-direction peek sees a FULL window: remaining == limit,
    # while the unskewed peek sees one cell of headroom (remaining 1).
    # A peek reading a Python clock measures the unskewed spacing and
    # reports 1.0 here, failing this assert.
    assert not ahead.is_exhausted and ahead.remaining == 2.0, (
        f"the +2 min peek must measure against its own skewed clock, got remaining "
        f"{ahead.remaining}"
    )

    behind = await peek_through(-_SKEW_S)
    assert behind.is_exhausted
    assert behind.retry_after is not None and 100 <= behind.retry_after.total_seconds() <= 140, (
        f"the -2 min hint must be the stored TAT's boundary on the skewed clock (~120 s), "
        f"got {behind.retry_after}"
    )


async def test_token_bucket_peek_measures_the_client_visible_store_clock(
    redis_url: str,
) -> None:
    """The peek's elapsed runs on the store clock the CLIENT sees (the only
    clock it can read). Drain the bucket, then peek through +/-2 min TIME
    proxies: +2 min of store clock is 1.2 accrued tokens (refill 0.005/s,
    safely under capacity), -2 min clamps to zero elapsed (the max(0,
    elapsed) guard: a stamp in the peek's future must not produce negative
    tokens, the double-spend direction of a backward clock)."""
    settings = _redis_settings(redis_url, "taskq_fallback_clock")
    name = f"tb_peek_{new_base62()}"
    bucket = TokenBucket(name=name, capacity=2.0, refill_per_second=0.005, backend="redis")

    direct = redis_async.from_url(redis_url, decode_responses=False)
    drained = await bucket.acquire(
        count=2.0, redis_client=direct, clock=SystemClock(), settings=settings
    )
    assert drained.allowed
    await direct.aclose()

    upstream_host, upstream_port, upstream_db = _upstream_of(redis_url)

    async def peek_through(skew: int) -> float:
        proxy = RedisTimeSkewProxy(
            upstream_host, upstream_port, skew_seconds=skew, upstream_db=upstream_db
        )
        await proxy.start()
        try:
            client = redis_async.from_url(proxy.url(), decode_responses=False)
            state = await bucket.peek(redis_client=client, settings=settings)
            await client.aclose()
            return state.tokens_remaining
        finally:
            await proxy.stop()

    ahead = await peek_through(_SKEW_S)
    assert ahead == pytest.approx(0.005 * _SKEW_S, abs=0.15), (
        f"the +2 min peek must accrue exactly the skewed store clock's refill, got {ahead}"
    )
    behind = await peek_through(-_SKEW_S)
    assert behind == 0.0, (
        f"the -2 min peek must clamp elapsed at zero (no phantom deficit), got {behind}"
    )


def _upstream_of(redis_url: str) -> tuple[str, int, int]:
    from urllib.parse import urlparse

    parsed = urlparse(redis_url)
    db = int(parsed.path.strip("/") or 0)
    return parsed.hostname or "127.0.0.1", parsed.port or 6379, db


# ── The outage-fallback crossing: PG stamps its recovery in ITS OWN domain ──


@pytest.mark.parametrize("poison_s", [_SKEW_S, -_SKEW_S])
async def test_pg_fallback_token_bucket_stamps_fresh_in_pg_domain(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
    poison_s: int,
) -> None:
    """A fixed-quota bucket spends its token in Redis; the outage lands the
    next acquire on PG. The Redis hash is POISONED with a ts written by the
    other domain's clock (+/-2 min) before the fallback. The PG recovery
    must stamp fresh with the PG clock and grant from the row's own cold
    start: a poisoned Redis stamp can neither wedge the fallback nor move
    the PG row's state one microsecond."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"tb_fb_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        healthy = TokenBucket(name=name, capacity=1.0, refill_per_second=0.0, backend="redis")
        spent = await healthy.acquire(
            count=1.0, redis_client=direct, clock=SystemClock(), settings=settings
        )
        assert spent.allowed and spent.backend == "redis", "setup: Redis pays the first token"

        redis_epoch = await _redis_epoch_s(direct)
        key = f"taskq:{schema}:rl:tb:{{{name}}}"
        await direct.hset(
            key,
            mapping={"tokens": "1.0", "ts": str(int(redis_epoch + poison_s))},
        )

        outaged = TokenBucket(name=name, capacity=1.0, refill_per_second=0.0, backend="redis")
        during = await outaged.acquire(
            count=1.0,
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed, "the fallback's cold start grants from full"
        assert during.backend == "postgres"

        row = await module_pg_pool.fetchrow(
            f"SELECT state->>'tokens' AS tokens, (state->>'ts')::float8 AS ts, "  # noqa: S608  # Why: schema fixture-derived; name is $1-bound
            f'kind FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            name,
        )
        assert row is not None
        assert row["kind"] == "token_bucket"
        assert float(row["tokens"]) == 0.0, "the PG domain pays its own cold-start token"
        pg_epoch = await _pg_epoch_s(module_pg_pool)
        assert abs(float(row["ts"]) - pg_epoch) < _EPOCH_SLACK_S, (
            "the recovery ts must be the PG clock's, stamped fresh"
        )
        assert abs(float(row["ts"]) - (redis_epoch + poison_s)) > 60, (
            "a cross-domain stamp must not survive into the PG row"
        )
    finally:
        await direct.aclose()


@pytest.mark.parametrize("poison_s", [_SKEW_S, -_SKEW_S])
async def test_pg_fallback_gcra_stamps_fresh_in_pg_domain(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
    poison_s: int,
) -> None:
    """GCRA twin: the Redis TAT is poisoned +/-2 min before the fallback;
    the PG recovery computes its TAT from the PG clock in the same locked
    statement, so the poison cannot wedge the recovered bucket or move the
    row's TAT."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"gcra_fb_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        healthy = SlidingWindow(
            name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="gcra"
        )
        first = await healthy.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
        assert first.allowed, "setup: the Redis TAT advances"

        redis_epoch = await _redis_epoch_s(direct)
        key = f"taskq:{schema}:sw_gcra:{{{name}}}"
        await direct.set(key, str((redis_epoch + poison_s) * 1000))

        outaged = SlidingWindow(
            name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="gcra"
        )
        during = await outaged.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed, "the recovered GCRA cold start grants"
        assert during.backend == "postgres"

        row = await module_pg_pool.fetchrow(
            f"SELECT (state->>'tat')::float8 AS tat, kind FROM "  # noqa: S608
            f'"{schema}".rate_limit_buckets WHERE bucket_name = $1',
            name,
        )
        assert row is not None and row["kind"] == "gcra"
        # The cold-start INSERT arm stamps tat = now + emission_interval;
        # limit=1 over 60 s puts the fresh TAT one emission interval ahead
        # of the PG clock that stamped it. This equality IS the poison
        # discriminator: a PG path that read the Redis TAT would GREATEST
        # into it (either direction puts the candidate at poisoned+60 =
        # redis_epoch+180, 120 s off the fresh stamp) and fail here.
        pg_epoch = await _pg_epoch_s(module_pg_pool)
        assert abs(float(row["tat"]) - (pg_epoch + 60)) < _EPOCH_SLACK_S, (
            "the recovered TAT must be the PG clock's, stamped fresh"
        )
    finally:
        await direct.aclose()


async def test_pg_fallback_log_window_stamps_fresh_in_pg_domain(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Log-style twin: the Redis zset holds an entry stamped +2 min AHEAD
    (the would-be wedge direction) when the outage lands. The PG recovery
    admits from its own empty window and stamps its own entry with the PG
    clock; the ahead-domain score must not wedge the recovery, deny it, or
    import itself into the PG window."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"swlog_fb_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        healthy = SlidingWindow(
            name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
        )
        first = await healthy.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
        assert first.allowed, "setup: the Redis window fills"

        redis_epoch = await _redis_epoch_s(direct)
        key = f"taskq:{schema}:sw:{{{name}}}"
        await direct.zadd(key, {"cross-domain-residue": (redis_epoch + _SKEW_S) * 1000})

        outaged = SlidingWindow(
            name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
        )
        during = await outaged.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed, "an ahead-domain residue must not wedge the PG recovery"
        assert during.backend == "postgres"

        rows = await module_pg_pool.fetch(
            f"SELECT EXTRACT(EPOCH FROM ts) AS ts_epoch FROM "  # noqa: S608
            f'"{schema}".rate_limit_window_entries WHERE bucket_name = $1',
            name,
        )
        assert len(rows) == 1, "the PG window must hold exactly its own admission"
        pg_epoch = await _pg_epoch_s(module_pg_pool)
        assert abs(float(rows[0]["ts_epoch"]) - pg_epoch) < _EPOCH_SLACK_S, (
            "the recovery entry must be stamped with the PG clock"
        )
        assert abs(float(rows[0]["ts_epoch"]) - (redis_epoch + _SKEW_S)) > 60, (
            "the ahead-domain score must not survive into the PG window"
        )
    finally:
        await direct.aclose()


# ── Failover back: the Redis paths never read or write the PG row ────


async def test_failover_back_token_bucket_ignores_the_pg_row(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """The outage wrote PG-domain state; the row is then POISONED with a
    ts +2 min ahead of the PG clock (the residue a skewed failover leaves).
    With Redis healthy again, the acquire must run on the Redis domain
    alone: full Redis bucket grants, the poisoned row changes nothing, and
    the Redis path leaves the row byte-identical (it never touches PG)."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"tb_back_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        # 1. The outage fallback writes the PG row.
        outaged = TokenBucket(name=name, capacity=2.0, refill_per_second=0.0, backend="redis")
        during = await outaged.acquire(
            count=1.0,
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed and during.backend == "postgres", "setup: the outage writes PG"

        # 2. Poison the row: zero tokens, a +2 min ahead ts, both PG-domain
        #    values a reading Redis path would fold into its window math.
        pg_epoch = await _pg_epoch_s(module_pg_pool)
        poisoned_state = (
            f'{{"tokens": 0.0, "ts": {int(pg_epoch + _SKEW_S)}, "capacity": 2.0, "refill": 0.0}}'
        )
        await module_pg_pool.execute(
            f'UPDATE "{schema}".rate_limit_buckets SET state = $2::jsonb '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
            poisoned_state,
        )
        row_before = await module_pg_pool.fetchrow(
            f'SELECT state, updated_at FROM "{schema}".rate_limit_buckets '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
        assert row_before is not None, "setup: the poisoned row exists"

        # 3. Failover back: Redis healthy, the bucket's Redis hash is full.
        healed = TokenBucket(name=name, capacity=2.0, refill_per_second=0.0, backend="redis")
        after = await healed.acquire(
            count=1.0, redis_client=direct, clock=SystemClock(), settings=settings
        )
        assert after.allowed, "the poisoned PG row must not wedge the Redis domain"
        assert after.backend == "redis"
        assert after.remaining == pytest.approx(1.0), (
            "the grant must come from the Redis hash, not the zeroed PG row"
        )

        row_after = await module_pg_pool.fetchrow(
            f'SELECT state, updated_at FROM "{schema}".rate_limit_buckets '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
        assert row_after is not None, "the row must survive the Redis acquire"
        assert row_after["state"] == row_before["state"] and (
            row_after["updated_at"] == row_before["updated_at"]
        ), "the Redis path must never write the PG row"
    finally:
        await direct.aclose()


@pytest.mark.parametrize("poison_s", [_SKEW_S, -_SKEW_S])
async def test_failover_back_gcra_ignores_the_pg_row(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
    poison_s: int,
) -> None:
    """GCRA twin, both directions: a PG-row TAT +2 min ahead (the wedge
    shape) or behind (the phantom-shape) must not move the Redis domain's
    steady-state admission, and the Redis acquire must leave the row
    untouched."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"gcra_back_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        outaged = SlidingWindow(
            name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="gcra"
        )
        during = await outaged.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed and during.backend == "postgres", "setup: the outage writes PG"

        pg_epoch = await _pg_epoch_s(module_pg_pool)
        await module_pg_pool.execute(
            f'UPDATE "{schema}".rate_limit_buckets SET state = $2::jsonb '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
            f'{{"tat": {int(pg_epoch + poison_s)}}}',
        )
        row_before = await module_pg_pool.fetchrow(
            f'SELECT state, updated_at FROM "{schema}".rate_limit_buckets '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
        assert row_before is not None, "setup: the poisoned row exists"

        healed = SlidingWindow(
            name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="gcra"
        )
        after = await healed.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
        assert after.allowed, (
            f"a PG TAT {'+2 min ahead' if poison_s > 0 else '2 min behind'} "
            "must not wedge or refill the Redis GCRA"
        )
        assert after.backend == "redis"

        row_after = await module_pg_pool.fetchrow(
            f'SELECT state, updated_at FROM "{schema}".rate_limit_buckets '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
        assert row_after is not None, "the row must survive the Redis acquire"
        assert row_after["state"] == row_before["state"] and (
            row_after["updated_at"] == row_before["updated_at"]
        ), "the Redis path must never write the PG row"
    finally:
        await direct.aclose()


async def test_failover_back_log_window_ignores_the_pg_entries(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Log-style twin: the outage's PG window entry is stamped +2 min AHEAD
    (the wedge direction). The Redis recovery admits from its own zset and
    must leave the PG entries table untouched."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"swlog_back_{new_base62()}"

    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        outaged = SlidingWindow(
            name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="log"
        )
        during = await outaged.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=SystemClock(),
            settings=settings,
        )
        assert during.allowed and during.backend == "postgres", "setup: the outage writes PG"

        await module_pg_pool.execute(
            f'INSERT INTO "{schema}".rate_limit_window_entries '  # noqa: S608
            f"(bucket_name, ts, request_id) "
            f"VALUES ($1, clock_timestamp() + ($2::bigint * INTERVAL '1 millisecond'), gen_random_uuid())",
            name,
            _SKEW_S * 1000,
        )

        healed = SlidingWindow(
            name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="log"
        )
        after = await healed.acquire(redis_client=direct, clock=SystemClock(), settings=settings)
        assert after.allowed, "an ahead-domain PG entry must not wedge the Redis window"
        assert after.backend == "redis"
        # The outage never wrote Redis (the script failed before the
        # store), so the healed zset starts empty: the heal is the FIRST
        # Redis admission of the window (1 of 2), and the PG residue adds
        # nothing to that count.
        assert after.remaining == pytest.approx(1.0), (
            "the Redis domain must count only its own admission (1 of 2)"
        )

        pg_count = await module_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
        assert int(pg_count) == 2, "the Redis path must never write the PG window"
    finally:
        await direct.aclose()


# ── The reverse leak: no Python monotonic value reaches either store ──


async def test_persisted_stamps_are_epoch_scale_never_monotonic(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """``time.monotonic()``/``loop.time()`` values are meaningless to the
    other backend and to a restarted process (uptime-scale, not epoch).
    After exercising every persisted write (PG token bucket acquire +
    refund, PG GCRA acquire, Redis token bucket / GCRA / log acquires),
    every stored stamp must sit within an hour of its store's clock - any
    leaked monotonic value (hours since boot, or negative) fails the
    range."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    clock = SystemClock()

    tb_pg = TokenBucket(
        name=f"tb_epoch_{new_base62()}", capacity=2.0, refill_per_second=0.0, backend="postgres"
    )
    d = await tb_pg.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert d.allowed
    await tb_pg.refund(d, pg_pool=module_pg_pool, clock=clock, settings=settings)

    gcra_pg = SlidingWindow(
        name=f"gcra_epoch_{new_base62()}",
        limit=2,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )
    assert (await gcra_pg.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)).allowed

    tb_r = TokenBucket(
        name=f"tb_ep_r_{new_base62()}", capacity=2.0, refill_per_second=0.01, backend="redis"
    )
    direct = redis_async.from_url(redis_url, decode_responses=False)
    try:
        assert (
            await tb_r.acquire(count=1.0, redis_client=direct, clock=clock, settings=settings)
        ).allowed
        swlog = SlidingWindow(
            name=f"swlog_ep_{new_base62()}",
            limit=2,
            window=timedelta(seconds=60),
            backend="redis",
            style="log",
        )
        log_decision = await swlog.acquire(redis_client=direct, clock=clock, settings=settings)
        assert log_decision.allowed

        pg_epoch = await _pg_epoch_s(module_pg_pool)
        pg_stamps = await module_pg_pool.fetch(
            f"SELECT bucket_name, (state->>'ts')::float8 AS ts, "  # noqa: S608
            f"(state->>'tat')::float8 AS tat "
            f'FROM "{schema}".rate_limit_buckets WHERE bucket_name = ANY($1)',
            [tb_pg.name, gcra_pg.name],
        )
        assert len(pg_stamps) == 2, "setup: both PG buckets wrote rows"
        for row in pg_stamps:
            for col in ("ts", "tat"):
                if row[col] is None:
                    continue
                assert pg_epoch - _EPOCH_RANGE_S <= float(row[col]) <= pg_epoch + _EPOCH_RANGE_S, (
                    f"{row['bucket_name']}.{col} = {row[col]} is not epoch-scale: "
                    "a Python monotonic value leaked into PG state"
                )

        redis_epoch = await _redis_epoch_s(direct)
        # The Redis token bucket's hash ts, and the log window's newest
        # score: every stamp the Redis paths persisted.
        tb_key = f"taskq:{schema}:rl:tb:{{{tb_r.name}}}"
        tb_ts = await direct.hget(tb_key, "ts")
        assert tb_ts is not None, "setup: the Redis token bucket persisted its ts"
        assert redis_epoch - _EPOCH_RANGE_S <= float(tb_ts) <= redis_epoch + _EPOCH_RANGE_S, (
            f"{tb_key} ts {tb_ts!r} is not epoch-scale: a monotonic value leaked into Redis"
        )

        log_key = f"taskq:{schema}:sw:{{{swlog.name}}}"
        top = await direct.zrange(log_key, -1, -1, withscores=True)
        assert top, "setup: the log window persisted its entry"
        # The log script's ZADD scores are epoch MILLISECONDS.
        score = float(top[0][1])
        score_s = score / 1000.0
        assert redis_epoch - _EPOCH_RANGE_S <= score_s <= redis_epoch + _EPOCH_RANGE_S, (
            f"{log_key} score {score} is not epoch-scale: a monotonic value leaked into Redis"
        )
    finally:
        await direct.aclose()


# ── The progress flush crosses no clock ──────────────────────────────


def test_progress_flush_carries_no_timestamp_input() -> None:
    """The progress seq is the only order (monotone by construction); the
    flush must never acquire a clock: no store-clock function in the SQL
    (the fencing gate is running+worker+attempt, never a timestamp) and no
    time-ish parameter in the binding contract. A regression that filters
    or stamps by a caller-supplied boundary (an ``older_than`` on the
    progress stream) reintroduces the crossing this campaign hunts."""
    sql = _flush_update_sql("taskq")
    for forbidden in ("clock_timestamp", "statement_timestamp", "localtimestamp", "now()"):
        assert forbidden not in sql, (
            f"the progress flush must not consult any clock: found {forbidden}"
        )
    assert _FLUSH_UNNEST_BINDING_ORDER == (
        "job_ids: list[UUID]",
        "seq_deltas: list[int]",
        "state_docs: list[str]",
        "attempts: list[int]",
        "worker_id: UUID",
    ), "the flush binding contract grew a parameter: no new parameter may carry a timestamp"

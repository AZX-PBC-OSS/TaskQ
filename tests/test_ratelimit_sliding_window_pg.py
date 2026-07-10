"""Integration tests for SlidingWindow log-style and GCRA PG backends against testcontainers Postgres.

PG fallback — real PG. 60 acquires all allowed; 61st denied;
row count in rate_limit_window_entries is 60 after burst and still 60 after
denial (denial path issues no INSERT). Sleep retry_after → allowed.

PG fallback pruning. Acquire once, wait window + 100 ms,
acquire again. Row count == 1 — first row DELETE-pruned by Statement 1.

(style="log" slice): backend="postgres" never touches Redis.
Completes against a real PG container without resolving the Redis DI provider.

GCRA PG fallback — real PG. Burst 60 acquires; assert all allowed.
61st denied; retry_after ≈ 1 s. Sleep → allowed. Verify rate_limit_buckets
row has kind='gcra' and state contains tat float.

Bucket-name collision guard. Pre-seed token_bucket row, then
attempt GCRA upsert → RuntimeError, original row preserved.

(style="gcra" slice): backend="postgres", style="gcra" never touches Redis.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._records import jsonb_to_dict
from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._sliding_window_pg import (
    _acquire_pg_gcra,
    _acquire_pg_log,
    _peek_pg_gcra,
    _peek_pg_log,
)
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration


def _unique_name() -> str:
    return f"sw_log_{new_base62()}"


def _gcra_unique_name() -> str:
    return f"sw_gcra_{new_base62()}"


def _settings(module_pg_schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name},
    )


# ── PG fallback — real PG — burst → denial → row invariant → retry ──


async def test_log_pg_fallback_burst_and_deny(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """backend="postgres", style="log". Acquire 30 — all allowed.
    31st denied with retry_after > 0. Row count is 30 after burst, still 30
    after denial (no INSERT on denial). Sleep retry_after + 50 ms → allowed.
    """
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = SystemClock()
    name = _unique_name()

    sw = SlidingWindow(
        name=name,
        limit=30,
        window=timedelta(seconds=30),
        backend="postgres",
        style="log",
    )

    for i in range(30):
        r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is True, f"acquire {i} denied"
        assert r.backend == "postgres"
        assert r.retry_after == timedelta(0)

    async with module_pg_pool.acquire() as conn:
        count = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
    assert count == 30

    r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after > timedelta(0)
    assert r.remaining == 0.0

    async with module_pg_pool.acquire() as conn:
        count_after_deny = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
    assert count_after_deny == 30


# ── PG fallback pruning — DELETE removes expired rows ──────────────


async def test_log_pg_fallback_pruning(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """acquire once, wait window + 100 ms real time, acquire again.
    Row count == 1 — the first row was DELETE-pruned by Statement 1; the
    second row is the new INSERT.
    """
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = SystemClock()
    name = _unique_name()

    window = timedelta(seconds=0.5)
    sw = SlidingWindow(
        name=name,
        limit=60,
        window=window,
        backend="postgres",
        style="log",
    )

    await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)

    async with module_pg_pool.acquire() as conn:
        count = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
    assert count == 1

    await asyncio.sleep(window.total_seconds() + 0.1)

    await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)

    async with module_pg_pool.acquire() as conn:
        count_after = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
    assert count_after == 1


# ── (style="log" slice): backend="postgres" never touches Redis ────────


async def test_postgres_never_touches_redis(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """(log slice): backend="postgres" acquire succeeds against real PG
    without ever touching Redis — no redis_client= kwarg supplied.
    """
    settings = _settings(module_pg_schema)
    clock = SystemClock()
    name = _unique_name()

    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="postgres",
        style="log",
    )

    r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True
    assert r.backend == "postgres"


# ── GCRA PG fallback — burst → denial → retry → row invariant ──────


async def test_gcra_pg_fallback_burst_and_deny(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """backend="postgres", style="gcra". Burst 60 acquires — all
    allowed. 61st denied with retry_after ≈ 1 s. Advance FakeClock
    retry_after + 50 ms → allowed. Verify rate_limit_buckets row has
    kind='gcra' and state tat float.

    Uses FakeClock so wall-clock elapsed time during the 60 PG roundtrips
    does not slide the GCRA window (parallel-load robustness).
    """
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    name = _gcra_unique_name()

    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )

    for i in range(60):
        r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is True, f"acquire {i} denied"
        assert r.backend == "postgres"
        assert r.retry_after == timedelta(0)

    r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after > timedelta(0)
    assert r.remaining == 0.0

    # GCRA retry_after = emission_interval - elapsed_during_burst.
    # With 60 PG roundtrips, elapsed time reduces retry_below 1.0 s;
    # the invariant is 0 < retry_after <= emission_interval (1.0 s).
    assert r.retry_after.total_seconds() <= 1.0

    async with module_pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT kind, state FROM {schema}.rate_limit_buckets "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            name,
        )
    assert row is not None
    assert row["kind"] == "gcra"
    state = jsonb_to_dict(row["state"])
    assert state is not None
    assert "tat" in state
    assert isinstance(state["tat"], float | int)

    wait = r.retry_after.total_seconds() + 0.05
    clock.advance(timedelta(seconds=wait))

    r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True
    assert r.retry_after == timedelta(0)


# ── Bucket-name collision guard ──────────────────────────────────


async def test_gcra_bucket_name_collision_guard(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Pre-seed token_bucket row, then attempt GCRA upsert →
    RuntimeError naming the colliding bucket; original row preserved.
    """
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = SystemClock()

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {schema}.rate_limit_buckets (bucket_name, kind, state, updated_at) "  # noqa: S608 # Why: schema is fixture-derived; literal values in test
            f"VALUES ($1, 'token_bucket', $2::jsonb, now())",
            "collide",
            '{"tokens": 5, "ts": 0}',
        )

    sw = SlidingWindow(
        name="collide",
        limit=60,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )

    with pytest.raises(RuntimeError, match="collide"):
        await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)

    async with module_pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT kind, state FROM {schema}.rate_limit_buckets "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
            f"WHERE bucket_name = $1",
            "collide",
        )
    assert row is not None
    assert row["kind"] == "token_bucket"
    state = jsonb_to_dict(row["state"])
    assert state is not None
    assert state["tokens"] == 5
    assert state["ts"] == 0


# ── (style="gcra" slice): backend="postgres" never touches Redis ────────


async def test_gcra_postgres_never_touches_redis(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """(gcra slice): backend="postgres", style="gcra" acquire succeeds
    against real PG without ever touching Redis — no redis_client= kwarg supplied.
    """
    settings = _settings(module_pg_schema)
    clock = SystemClock()
    name = _gcra_unique_name()

    sw = SlidingWindow(
        name=name,
        limit=60,
        window=timedelta(seconds=60),
        backend="postgres",
        style="gcra",
    )

    r = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True
    assert r.backend == "postgres"


# ── Injection-error branches — pg_pool/settings/request_id None ────


def _fake_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_fake"},
    )


async def test_peek_log_pg_pool_none(module_pg_schema: ModulePgSchema) -> None:
    """peek() with backend="postgres", style="log" and pg_pool=None raises
    RuntimeError (line 40-41 of _sliding_window_pg.py)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.peek(pg_pool=None, clock=SystemClock(), settings=_settings(module_pg_schema))


async def test_peek_log_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """peek() with backend="postgres", style="log" and settings=None raises
    RuntimeError (line 42-43)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.peek(pg_pool=module_pg_pool, clock=SystemClock(), settings=None)


async def test_peek_gcra_pg_pool_none(module_pg_schema: ModulePgSchema) -> None:
    """peek() with backend="postgres", style="gcra" and pg_pool=None raises
    RuntimeError (line 95-96)."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.peek(pg_pool=None, clock=SystemClock(), settings=_settings(module_pg_schema))


async def test_peek_gcra_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """peek() with backend="postgres", style="gcra" and settings=None raises
    RuntimeError (line 97-98)."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.peek(pg_pool=module_pg_pool, clock=SystemClock(), settings=None)


async def test_reset_log_pg_pool_none(module_pg_schema: ModulePgSchema) -> None:
    """reset() with backend="postgres", style="log" and pg_pool=None raises
    RuntimeError (line 152-153)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.reset(pg_pool=None, settings=_settings(module_pg_schema))


async def test_reset_log_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """reset() with backend="postgres", style="log" and settings=None raises
    RuntimeError (line 154-155)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.reset(pg_pool=module_pg_pool, settings=None)


async def test_reset_gcra_pg_pool_none(module_pg_schema: ModulePgSchema) -> None:
    """reset() with backend="postgres", style="gcra" and pg_pool=None raises
    RuntimeError (line 170-171)."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.reset(pg_pool=None, settings=_settings(module_pg_schema))


async def test_reset_gcra_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """reset() with backend="postgres", style="gcra" and settings=None raises
    RuntimeError (line 172-173)."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.reset(pg_pool=module_pg_pool, settings=None)


async def test_acquire_log_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """acquire() with backend="postgres", style="log" and settings=None raises
    RuntimeError (line 220-221)."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.acquire(pg_pool=module_pg_pool, clock=SystemClock(), settings=None)


async def test_acquire_log_request_id_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """Direct call to the private _acquire_pg_log with request_id=None
    raises RuntimeError (line 222-223). The public SlidingWindow.acquire()
    always synthesises a UUID for log-style acquires, so this branch is
    only reachable by calling the module-level function directly."""
    sw = SlidingWindow(
        name=_unique_name(), limit=10, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    settings = _settings(module_pg_schema)
    with pytest.raises(RuntimeError, match="request_id required"):
        await _acquire_pg_log(sw, module_pg_pool, SystemClock(), settings, None)


async def test_acquire_gcra_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """acquire() with backend="postgres", style="gcra" and settings=None raises
    RuntimeError (line 308-309)."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.acquire(pg_pool=module_pg_pool, clock=SystemClock(), settings=None)


# ── Refund gcra — previous_state None / pg_pool None / settings None ────


async def test_refund_gcra_previous_state_none(module_pg_schema: ModulePgSchema) -> None:
    """refund() returns immediately when decision.previous_state is None
    (line 189-190) — no pg_pool/settings validation is even attempted."""
    sw = SlidingWindow(
        name=_gcra_unique_name(),
        limit=10,
        window=timedelta(seconds=10),
        backend="postgres",
        style="gcra",
    )
    decision = RateLimitDecision(
        allowed=False,
        remaining=0.0,
        retry_after=timedelta(seconds=1),
        bucket_name=sw.name,
        backend="postgres",
        previous_state=None,
    )
    await sw.refund(decision, pg_pool=None, settings=None)


async def test_refund_gcra_pg_pool_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """refund() with a populated previous_state and pg_pool=None raises
    RuntimeError (line 191-192)."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    name = _gcra_unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )

    decision = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert decision.allowed is True
    assert decision.previous_state is not None

    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await sw.refund(decision, pg_pool=None, settings=settings)


async def test_refund_gcra_settings_none(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """refund() with a populated previous_state and settings=None raises
    RuntimeError (line 193-194)."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    name = _gcra_unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )

    decision = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert decision.allowed is True

    with pytest.raises(RuntimeError, match="settings not injected"):
        await sw.refund(decision, pg_pool=module_pg_pool, settings=None)


async def test_refund_gcra_success(
    module_pg_schema: ModulePgSchema, module_pg_pool: asyncpg.Pool
) -> None:
    """refund() after a successful gcra acquire rolls the stored tat back
    to its pre-acquire value (lines 195-208 executed)."""
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    name = _gcra_unique_name()
    sw = SlidingWindow(
        name=name, limit=10, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )

    decision = await sw.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert decision.allowed is True
    assert decision.previous_state is not None
    pre_tat = float(decision.previous_state["pre_acquire_tat"])  # type: ignore[arg-type]

    await sw.refund(decision, pg_pool=module_pg_pool, settings=settings)

    async with module_pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT state FROM {schema}.rate_limit_buckets "  # noqa: S608
            f"WHERE bucket_name = $1",
            name,
        )
    assert row is not None
    state = jsonb_to_dict(row["state"])
    assert state is not None
    assert float(state["tat"]) == pytest.approx(pre_tat)  # type: ignore[arg-type]


# ── Fake-pool unit tests for defensive/race branches ────────────────
#
# The branches below require asyncpg row shapes that cannot be produced
# deterministically via real timing or real concurrent races (they guard
# against clock skew / stale reads between two queries in the same
# transaction, or against a competing writer changing `kind` between a
# SELECT and an upsert). A small hand-rolled fake pool gives full,
# deterministic control over the returned rows so each branch can be
# exercised directly.


class _FakeConn:
    def __init__(self, fetchrow_returns: list[object]) -> None:
        self._fetchrow_returns = list(fetchrow_returns)

    async def fetchrow(self, sql: str, *args: object) -> object:
        return self._fetchrow_returns.pop(0)

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    def transaction(self) -> "_NullTxn":
        return _NullTxn()


class _NullTxn:
    async def __aenter__(self) -> "_NullTxn":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePgPool:
    def __init__(self, fetchrow_returns: list[object]) -> None:
        self._conn = _FakeConn(fetchrow_returns)

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self._conn)


async def test_peek_log_oldest_row_none() -> None:
    """peek_pg_log: count query reports the bucket exhausted, but the
    oldest-row query races to no rows (line 70->76 false branch) — the
    peek still returns is_exhausted=True with retry_after=None."""
    sw = SlidingWindow(
        name="fake_log", limit=5, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    fake_pool = _FakePgPool(fetchrow_returns=[{"count": 5}, None])

    state = await _peek_pg_log(
        sw, now_ms=0, pg_pool=fake_pool, clock=SystemClock(), settings=_fake_settings()
    )  # type: ignore[arg-type]

    assert state.is_exhausted is True
    assert state.retry_after is None


async def test_peek_log_retry_after_clamped_to_1ms() -> None:
    """peek_pg_log: the oldest row's ts is exactly `now - window`, making
    the raw retry_after compute to timedelta(0); the clamp raises it to
    1 ms (line 73-74)."""
    now_dt = datetime(2025, 1, 1, tzinfo=UTC)
    window = timedelta(seconds=10)
    sw = SlidingWindow(name="fake_log2", limit=5, window=window, backend="postgres", style="log")
    fake_pool = _FakePgPool(fetchrow_returns=[{"count": 5}, {"ts": now_dt - window}])
    clock = FakeClock(now_dt)

    state = await _peek_pg_log(
        sw, now_ms=0, pg_pool=fake_pool, clock=clock, settings=_fake_settings()
    )  # type: ignore[arg-type]

    assert state.is_exhausted is True
    assert state.retry_after == timedelta(milliseconds=1)


async def test_peek_gcra_retry_after_clamped_to_1ms() -> None:
    """peek_pg_gcra: with limit=3 and window=1s, a stored tat offset of
    exactly `window - emission_interval` seconds ahead of `now` makes
    the raw retry_after_seconds compute to 0.0; the clamp raises it to
    1 ms (line 131-132). Derived analytically: emission = window/limit,
    boundary = window - emission is the exact float where int-truncated
    `remaining` first reports 0 (exhausted) while the continuous
    retry_after_seconds formula also lands on exactly 0.0."""
    sw = SlidingWindow(
        name="fake_gcra", limit=3, window=timedelta(seconds=1), backend="postgres", style="gcra"
    )
    now_seconds = 0.0
    boundary = 1.0 - (1.0 / 3)
    fake_pool = _FakePgPool(
        fetchrow_returns=[{"kind": "gcra", "state": {"tat": now_seconds + boundary}}]
    )

    state = await _peek_pg_gcra(
        sw,
        now_ms=int(now_seconds * 1000),
        pg_pool=fake_pool,
        clock=SystemClock(),
        settings=_fake_settings(),
    )  # type: ignore[arg-type]

    assert state.is_exhausted is True
    assert state.retry_after == timedelta(milliseconds=1)


async def test_acquire_log_oldest_row_none_fallback() -> None:
    """acquire_pg_log: the INSERT ... WHERE (subquery) < limit fails
    (denied), and the subsequent oldest-row lookup also races to no
    rows — retry_after falls back to 1 ms (line 285-286)."""
    sw = SlidingWindow(
        name="fake_log3", limit=1, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    fake_pool = _FakePgPool(fetchrow_returns=[None, None])

    decision = await _acquire_pg_log(
        sw, fake_pool, SystemClock(), _fake_settings(), "11111111-1111-1111-1111-111111111111"
    )  # type: ignore[arg-type]

    assert decision.allowed is False
    assert decision.retry_after == timedelta(milliseconds=1)


async def test_acquire_log_retry_after_clamped_to_1ms() -> None:
    """acquire_pg_log: denied, oldest row present with ts exactly
    `now - window` so the raw retry_after computes to timedelta(0); the
    clamp raises it to 1 ms (line 283-284)."""
    now_dt = datetime(2025, 1, 1, tzinfo=UTC)
    window = timedelta(seconds=10)
    sw = SlidingWindow(name="fake_log4", limit=1, window=window, backend="postgres", style="log")
    fake_pool = _FakePgPool(fetchrow_returns=[None, {"ts": now_dt - window}])
    clock = FakeClock(now_dt)

    decision = await _acquire_pg_log(
        sw, fake_pool, clock, _fake_settings(), "11111111-1111-1111-1111-111111111111"
    )  # type: ignore[arg-type]

    assert decision.allowed is False
    assert decision.retry_after == timedelta(milliseconds=1)


async def test_acquire_gcra_kind_collision_race_on_upsert() -> None:
    """acquire_pg_gcra: SELECT finds an existing gcra row (allowing the
    acquire to proceed), but the upsert's RETURNING clause comes back
    empty — simulating a competing writer that flipped `kind` away from
    'gcra' between the SELECT and the upsert. Raises RuntimeError
    (line 361-362)."""
    now_dt = datetime(1970, 1, 1, tzinfo=UTC)
    sw = SlidingWindow(
        name="collide_race", limit=3, window=timedelta(seconds=1), backend="postgres", style="gcra"
    )
    clock = FakeClock(now_dt)
    fake_pool = _FakePgPool(fetchrow_returns=[{"kind": "gcra", "state": {"tat": 0.0}}, None])

    with pytest.raises(RuntimeError, match="collide_race"):
        await _acquire_pg_gcra(sw, fake_pool, clock, _fake_settings())  # type: ignore[arg-type]

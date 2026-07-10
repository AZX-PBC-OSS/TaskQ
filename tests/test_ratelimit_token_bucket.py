"""Unit tests for TokenBucket in-memory, Redis, and postgres-backend dispatch.

In-memory tests use ``FakeClock`` injected via ``clock=`` so that refill is
deterministic and zero-real-time.

Redis unit tests use a lightweight fake that records calls to
``register_script`` and the script ``__call__`` — no network, no
testcontainers.

Postgres-backend dispatch tests verify routing only — the SQL behaviour
is exercised in the integration test file.
"""

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from taskq.ratelimit import TokenBucket
from taskq.ratelimit._scripts import TOKEN_BUCKET_SCRIPT
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _bucket(
    capacity: float = 100,
    refill: float = 10,
    name: str = "test",
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="memory")


def _redis_bucket(
    capacity: float = 100,
    refill: float = 10,
    name: str = "test",
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="redis")


# ── basic burst ────────────────────────────────────────────────────────


async def test_basic_burst() -> None:
    """100 acquires at capacity=100, refill=10 → all allowed, remaining=0."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)
    for i in range(100):
        r = await tb.acquire(clock=clock)
        assert r.allowed is True, f"acquire {i} denied"
        assert r.backend == "memory"
    r = await tb.acquire(clock=clock)
    assert r.remaining == 0.0


# ── throttle after burst ────────────────────────────────────────────────


async def test_throttle_after_burst() -> None:
    """101st acquire returns allowed=False, retry_after≈0.1s."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)
    for _ in range(100):
        await tb.acquire(clock=clock)

    r = await tb.acquire(clock=clock)
    assert r.allowed is False
    assert r.backend == "memory"
    assert r.retry_after is not None
    assert abs(r.retry_after.total_seconds() - 0.1) < 0.01


# ── refill via FakeClock ──────────────────────────────────────────────


async def test_refill_after_advance() -> None:
    """advance 1s → 10 more allowed, remaining=0."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)
    for _ in range(100):
        await tb.acquire(clock=clock)

    clock.advance(timedelta(seconds=1))
    for i in range(10):
        r = await tb.acquire(clock=clock)
        assert r.allowed is True, f"refill acquire {i} denied"
    r = await tb.acquire(clock=clock)
    assert r.remaining == 0.0
    assert r.backend == "memory"


# ── partial-fill acquire ─────────────────────────────────────────────


async def test_partial_fill_acquire() -> None:
    """acquire 50 then 60 → retry_after≈1.0s."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r1 = await tb.acquire(count=50, clock=clock)
    assert r1.allowed is True
    assert r1.remaining == 50.0

    r2 = await tb.acquire(count=60, clock=clock)
    assert r2.allowed is False
    assert r2.retry_after is not None
    assert abs(r2.retry_after.total_seconds() - 1.0) < 0.01


# ── count > capacity always denied ────────────────────────────────────


async def test_count_exceeds_capacity_denied() -> None:
    """count > capacity → always denied; retry_after finite."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r = await tb.acquire(count=200, clock=clock)
    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after.total_seconds() > 0


# ── fixed quota refill=0; retry_after=None after exhaustion ───────────


async def test_fixed_quota_retry_after_none() -> None:
    """refill_per_second=0; after exhaustion retry_after=None."""
    tb = _bucket(capacity=5, refill=0)
    clock = FakeClock(_START)

    for _ in range(5):
        r = await tb.acquire(clock=clock)
        assert r.allowed is True

    r = await tb.acquire(clock=clock)
    assert r.allowed is False
    assert r.retry_after is None
    assert r.backend == "memory"


# ── memory backend — no redis/pg; result.backend="memory" ─────────────


async def test_memory_backend_no_external_deps() -> None:
    """backend="memory" needs no redis_client/pg_pool; result.backend="memory"."""
    tb = _bucket()
    clock = FakeClock(_START)

    r = await tb.acquire(clock=clock)
    assert r.allowed is True
    assert r.backend == "memory"


# ── FakeClock backward step is safe ────────────────────────────────────


async def test_backward_clock_step_safe() -> None:
    """backward clock step clamps elapsed to 0; no negative refill."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r1 = await tb.acquire(count=50, clock=clock)
    assert r1.allowed is True
    assert r1.remaining == 50.0

    clock.advance(timedelta(seconds=-30))
    r2 = await tb.acquire(count=1, clock=clock)
    assert r2.allowed is True
    assert r2.remaining <= 50.0


# ── Logging: allowed acquire ──────────────────────────────────────────


async def test_logging_allowed_acquire() -> None:
    """Allowed acquire returns a valid decision with retry_after=0."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r = await tb.acquire(clock=clock)

    assert r.allowed is True
    assert r.retry_after == timedelta(0)
    assert r.remaining > 0


# ── Logging: denied acquire with positive refill ──────────────────────


async def test_logging_denied_positive_refill() -> None:
    """Denied acquire with positive refill returns retry_after > 0."""
    tb = _bucket(capacity=1, refill=10)
    clock = FakeClock(_START)

    await tb.acquire(clock=clock)

    r = await tb.acquire(clock=clock)

    assert r.allowed is False
    assert r.retry_after is not None
    assert r.retry_after > timedelta(0)
    assert abs(r.retry_after.total_seconds() - 0.1) < 0.01


# ── Logging: denied acquire with refill=0 (fixed quota exhausted) ─────


async def test_logging_denied_fixed_quota_retry_after_none() -> None:
    """Denied acquire with refill=0 returns retry_after=None."""
    tb = _bucket(capacity=1, refill=0)
    clock = FakeClock(_START)

    await tb.acquire(clock=clock)

    r = await tb.acquire(clock=clock)

    assert r.allowed is False
    assert r.retry_after is None


# ── RuntimeError when clock is None for memory backend ────────────────


async def test_acquire_memory_without_clock_raises() -> None:
    """backend="memory" requires clock; RuntimeError when absent."""
    tb = _bucket()

    with pytest.raises(RuntimeError, match="clock not injected for memory backend"):
        await tb.acquire()


# ── RuntimeError when dependencies missing for postgres backend ─────────


def _pg_bucket(
    capacity: float = 100,
    refill: float = 10,
    name: str = "pg-test",
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="postgres")


async def test_acquire_postgres_without_pg_pool_raises() -> None:
    """backend="postgres" requires pg_pool; RuntimeError when absent."""
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="pg_pool not injected for postgres backend"):
        await tb.acquire(clock=FakeClock(_START), settings=_FakeSettings())


async def test_acquire_postgres_without_settings_raises() -> None:
    """backend="postgres" requires settings; RuntimeError when absent."""
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for postgres backend"):
        await tb.acquire(clock=FakeClock(_START), pg_pool=_FakePgPool())


async def test_acquire_postgres_without_clock_raises() -> None:
    """backend="postgres" requires clock; RuntimeError when absent."""
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="clock not injected for postgres backend"):
        await tb.acquire(pg_pool=_FakePgPool(), settings=_FakeSettings())


# ── backend="postgres" never touches Redis ─────────────────────────────


async def test_postgres_backend_never_touches_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """backend="postgres" dispatches to PG without any Redis call."""
    tb = _pg_bucket()
    pg_calls: list[str] = []

    async def _fake_acquire_pg(
        self: TokenBucket,
        count: float,
        pg_pool: object,
        clock: object,
        settings: WorkerSettings,
    ) -> RateLimitDecision:
        pg_calls.append("called")
        return RateLimitDecision(
            allowed=True,
            remaining=99.0,
            retry_after=timedelta(0),
            bucket_name=self.name,
            backend="postgres",
        )

    monkeypatch.setattr(TokenBucket, "_acquire_pg", _fake_acquire_pg)
    r = await tb.acquire(pg_pool=_FakePgPool(), clock=FakeClock(_START), settings=_FakeSettings())

    assert r.backend == "postgres"
    assert r.allowed is True
    assert pg_calls == ["called"]


# ── rate_limit_pg_fallback_enabled=False re-raises ────────────────────


@pytest.mark.redis
async def test_pg_fallback_disabled_re_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """when rate_limit_pg_fallback_enabled=False, Redis errors
    propagate; PG path is NOT called."""
    import redis as _redis_mod

    tb = _redis_bucket(name="fallback-off")

    class _RaiseRedis:
        """redis_client stub that raises ConnectionError on any script call."""

        def register_script(self, script: bytes) -> object:
            return _RaiseScript()

    class _RaiseScript:
        async def __call__(self, **kwargs: object) -> object:
            raise _redis_mod.ConnectionError("connection lost")

    pg_calls: list[str] = []

    async def _fake_acquire_pg(
        self: TokenBucket,
        count: float,
        pg_pool: object,
        clock: object,
        settings: WorkerSettings,
    ) -> RateLimitDecision:
        pg_calls.append("called")
        return RateLimitDecision(
            allowed=True,
            remaining=99.0,
            retry_after=timedelta(0),
            bucket_name=self.name,
            backend="postgres",
        )

    class _NoFallbackSettings(_FakeSettings):
        rate_limit_pg_fallback_enabled: bool = False

    monkeypatch.setattr(TokenBucket, "_acquire_pg", _fake_acquire_pg)
    with pytest.raises(_redis_mod.ConnectionError, match="connection lost"):
        await tb.acquire(
            redis_client=_RaiseRedis(),
            pg_pool=_FakePgPool(),
            clock=FakeClock(_START),
            settings=_NoFallbackSettings(),
        )

    assert pg_calls == []


# ── RuntimeError when dependencies missing for redis backend ──────────


async def test_acquire_redis_without_client_raises() -> None:
    """backend="redis" requires redis_client; RuntimeError when absent."""
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="redis_client not injected for redis backend"):
        await tb.acquire(clock=FakeClock(_START), settings=_FakeSettings())


async def test_acquire_redis_without_settings_raises() -> None:
    """backend="redis" requires settings; RuntimeError when absent."""
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for redis backend"):
        await tb.acquire(redis_client=_FakeRedisClient(), clock=FakeClock(_START))


async def test_acquire_redis_without_clock_raises() -> None:
    """backend="redis" requires clock; RuntimeError when absent."""
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="clock not injected for redis backend"):
        await tb.acquire(redis_client=_FakeRedisClient(), settings=_FakeSettings())


# ── Redis fake: ARGV order, key format, register_script once ─────────


class _FakeAsyncScript:
    """Records calls made to the Lua script for unit-test assertions."""

    def __init__(self, script: bytes) -> None:
        self.script = script
        self.calls: list[dict[str, Any]] = []

    async def __call__(
        self,
        *,
        keys: list[str] | None = None,
        args: list[float | int] | None = None,
    ) -> list[object]:
        self.calls.append({"keys": keys, "args": args})
        return [1, "99.0", "0.0"]


class _FakeRedisClient:
    """Minimal fake that records ``register_script`` calls and returns a
    controllable ``_FakeAsyncScript``.
    """

    def __init__(self) -> None:
        self.register_script_calls: list[bytes] = []
        self._custom_script: _FakeAsyncScript | None = None
        self.hmget_return: list[object] | None = None
        self.hmget_calls: list[tuple[str, list[str]]] = []
        self.deleted_keys: list[str] = []

    def register_script(self, script: bytes) -> _FakeAsyncScript:
        self.register_script_calls.append(script)
        if self._custom_script is not None:
            return self._custom_script
        return _FakeAsyncScript(script)

    async def hmget(self, key: str, fields: list[str]) -> list[object]:
        self.hmget_calls.append((key, fields))
        if self.hmget_return is not None:
            return self.hmget_return
        return [None, None]

    async def delete(self, key: str) -> None:
        self.deleted_keys.append(key)


class _FakeSettings:
    """Minimal settings stub exposing ``schema_name`` and ``rate_limit_pg_fallback_enabled``."""

    schema_name: str = "taskq_test"
    rate_limit_pg_fallback_enabled: bool = True


class _FakePgPool:
    """Placeholder pool for dispatch-only unit tests; never used for SQL."""


class _NullAsyncCtx:
    """No-op async context manager standing in for ``conn.transaction()``."""

    async def __aenter__(self) -> "_NullAsyncCtx":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePgConn:
    """Hand-rolled connection stub mimicking the asyncpg.Connection surface
    used by TokenBucket's PG paths (fetchrow/execute/transaction)."""

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
    """Fake asyncpg.Pool supporting both ``pool.acquire()`` and
    direct ``pool.execute()`` (used by ``_reset_pg``). ``fetchrow_result``
    controls what ``conn.fetchrow`` returns regardless of what SQL is
    executed beforehand (e.g. simulating a row disappearing after preseed)."""

    def __init__(self, fetchrow_result: object = None) -> None:
        self.fetchrow_result = fetchrow_result
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self)

    async def execute(self, sql: str, *args: object) -> None:
        self.execute_calls.append((sql, args))


async def test_redis_argv_order_and_key_format() -> None:
    """Redis acquire passes ARGV in canonical order and constructs the key with hash tag."""
    client = _FakeRedisClient()
    tb = _redis_bucket(capacity=100, refill=10, name="my_bucket")
    clock = FakeClock(_START)

    await tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings())

    assert len(client.register_script_calls) == 1
    assert client.register_script_calls[0] == TOKEN_BUCKET_SCRIPT

    script_obj = tb._redis_script
    assert isinstance(script_obj, _FakeAsyncScript)
    assert len(script_obj.calls) == 1

    call = script_obj.calls[0]

    key = call["keys"][0]
    assert key == "taskq:taskq_test:rl:tb:{my_bucket}"

    argv = call["args"]
    assert len(argv) == 5
    assert argv[0] == _START.timestamp()
    assert argv[1] == 100.0
    assert argv[2] == 10.0
    assert argv[3] == 1.0
    ttl_val = argv[4]
    assert ttl_val == math.ceil(100 / 10 * 2) + 60


async def test_redis_register_script_called_once_across_multiple_acquires() -> None:
    """register_script is invoked exactly once per TokenBucket instance."""
    client = _FakeRedisClient()
    tb = _redis_bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    await tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings())
    await tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings())
    await tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings())

    assert len(client.register_script_calls) == 1

    script_obj = tb._redis_script
    assert isinstance(script_obj, _FakeAsyncScript)
    assert len(script_obj.calls) == 3


async def test_redis_register_script_once_under_concurrent_first_acquires() -> None:
    """Two concurrent first-acquires still register the script only once."""
    client = _FakeRedisClient()
    tb = _redis_bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    import asyncio

    await asyncio.gather(
        tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings()),
        tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings()),
    )

    assert len(client.register_script_calls) == 1


async def test_redis_fixed_quota_refill_zero_retry_after_none() -> None:
    """Redis path with refill=0: denied acquire returns retry_after=None."""
    client = _FakeRedisClient()

    class _DenyScript(_FakeAsyncScript):
        async def __call__(  # type: ignore[override]  # Why: test override with specific return shape
            self,
            *,
            keys: list[str] | None = None,
            args: list[float | int] | None = None,
        ) -> list[object]:
            return [0, "4.0", "nan"]

    client._custom_script = _DenyScript(TOKEN_BUCKET_SCRIPT)

    tb = _redis_bucket(capacity=5, refill=0, name="fixed")
    clock = FakeClock(_START)
    r = await tb.acquire(redis_client=client, clock=clock, settings=_FakeSettings())
    assert r.allowed is False
    assert r.retry_after is None
    assert r.remaining == 4.0
    assert r.backend == "redis"


# ── TTL default for positive refill ───────────────────────────────────


def test_ttl_default_positive_refill() -> None:
    """Default ttl for positive refill is ceil(capacity/refill*2)+60 seconds."""
    tb = _bucket(capacity=100, refill=10)
    expected = timedelta(seconds=math.ceil(100 / 10 * 2) + 60)
    assert tb.ttl == expected


# ── Re-export from taskq.ratelimit ────────────────────────────────────


def test_token_bucket_reexported() -> None:
    """TokenBucket is importable from taskq.ratelimit."""
    import taskq.ratelimit as rl

    assert rl.TokenBucket is TokenBucket


# ── TokenBucket refund (in-memory) ──────────────────────────────────────


async def test_refund_restores_tokens() -> None:
    """In-memory refund adds count tokens back (refund amount = count, NOT decision.remaining)."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r = await tb.acquire(count=5, clock=clock)
    assert r.allowed is True
    assert r.remaining == 95.0

    await tb.refund(r, count=5, clock=clock)

    r2 = await tb.acquire(count=5, clock=clock)
    assert r2.allowed is True
    assert r2.remaining == 95.0


async def test_refund_caps_at_capacity() -> None:
    """In-memory refund caps tokens at capacity; prevents over-refund."""
    tb = _bucket(capacity=10, refill=10)
    clock = FakeClock(_START)

    r = await tb.acquire(count=1, clock=clock)
    assert r.allowed is True
    assert r.remaining == 9.0

    await tb.refund(r, count=50, clock=clock)

    r2 = await tb.acquire(count=1, clock=clock)
    assert r2.remaining == 9.0


async def test_refund_count_two_adds_exactly_two() -> None:
    """In-memory refund with count=2 adds exactly 2 tokens back."""
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)

    r = await tb.acquire(count=10, clock=clock)
    assert r.allowed is True
    assert r.remaining == 90.0

    await tb.refund(r, count=2, clock=clock)

    r2 = await tb.acquire(count=1, clock=clock)
    assert r2.allowed is True
    assert r2.remaining == 91.0


# ── TokenBucket refund (postgres guard clauses) ────────────────────────


async def test_refund_postgres_without_pg_pool_raises() -> None:
    """backend="postgres" refund requires pg_pool; RuntimeError when absent."""
    tb = _pg_bucket()
    decision = RateLimitDecision(
        allowed=True,
        remaining=99.0,
        retry_after=timedelta(0),
        bucket_name="pg-test",
        backend="postgres",
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected for postgres backend refund"):
        await tb.refund(decision, clock=FakeClock(_START), settings=_FakeSettings())


async def test_refund_postgres_without_clock_raises() -> None:
    """backend="postgres" refund requires clock; RuntimeError when absent."""
    tb = _pg_bucket()
    decision = RateLimitDecision(
        allowed=True,
        remaining=99.0,
        retry_after=timedelta(0),
        bucket_name="pg-test",
        backend="postgres",
    )
    with pytest.raises(RuntimeError, match="clock not injected for postgres backend refund"):
        await tb.refund(decision, pg_pool=_FakePgPool(), settings=_FakeSettings())


async def test_refund_postgres_without_settings_raises() -> None:
    """backend="postgres" refund requires settings; RuntimeError when absent."""
    tb = _pg_bucket()
    decision = RateLimitDecision(
        allowed=True,
        remaining=99.0,
        retry_after=timedelta(0),
        bucket_name="pg-test",
        backend="postgres",
    )
    with pytest.raises(RuntimeError, match="settings not injected for postgres backend refund"):
        await tb.refund(decision, pg_pool=_FakePgPool(), clock=FakeClock(_START))


# ── constructor validation errors ──────────────────────────────────────


def test_constructor_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError, match="capacity must be > 0, got 0"):
        TokenBucket(name="bad-capacity", capacity=0, refill_per_second=1, backend="memory")


def test_constructor_rejects_negative_refill() -> None:
    with pytest.raises(ValueError, match="refill_per_second must be >= 0, got -1"):
        TokenBucket(name="bad-refill", capacity=10, refill_per_second=-1, backend="memory")


# ── properties ──────────────────────────────────────────────────────────


def test_capacity_and_refill_properties() -> None:
    tb = _bucket(capacity=42, refill=7)
    assert tb.capacity == 42
    assert tb.refill_per_second == 7


# ── memory peek/reset: success paths + clock-None guards ──────────────


async def test_peek_memory_before_any_acquire_reports_full_capacity() -> None:
    """peek() before any acquire (internal ``_ts`` still None) reports full capacity."""
    tb = _bucket(capacity=30, refill=5)
    clock = FakeClock(_START)

    state = await tb.peek(clock=clock)

    assert state.tokens_remaining == 30.0
    assert state.is_exhausted is False


async def test_peek_memory_returns_state_when_not_exhausted() -> None:
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)
    await tb.acquire(count=40, clock=clock)

    state = await tb.peek(clock=clock)

    assert state.backend == "memory"
    assert state.is_exhausted is False
    assert state.tokens_remaining == 60.0
    assert state.retry_after is None


async def test_peek_memory_exhausted_computes_retry_after() -> None:
    tb = _bucket(capacity=5, refill=2)
    clock = FakeClock(_START)
    await tb.acquire(count=5, clock=clock)

    state = await tb.peek(clock=clock)

    assert state.is_exhausted is True
    assert state.retry_after is not None
    assert abs(state.retry_after.total_seconds() - 0.5) < 1e-9


async def test_peek_memory_without_clock_raises() -> None:
    tb = _bucket()
    with pytest.raises(RuntimeError, match="clock not injected for memory backend"):
        await tb.peek()


async def test_reset_memory_restores_full_capacity() -> None:
    tb = _bucket(capacity=100, refill=10)
    clock = FakeClock(_START)
    await tb.acquire(count=100, clock=clock)

    await tb.reset(clock=clock)

    r = await tb.acquire(clock=clock)
    assert r.allowed is True
    assert r.remaining == 99.0


async def test_reset_memory_without_clock_raises() -> None:
    tb = _bucket()
    with pytest.raises(RuntimeError, match="clock not injected for memory backend"):
        await tb.reset()


# ── redis acquire: denied with positive refill (retry_after computed) ──


async def test_redis_denied_with_positive_refill_computes_retry_after() -> None:
    client = _FakeRedisClient()

    class _DenyScript(_FakeAsyncScript):
        async def __call__(  # type: ignore[override]  # Why: test override with specific return shape
            self,
            *,
            keys: list[str] | None = None,
            args: list[float | int] | None = None,
        ) -> list[object]:
            return [0, "0.0", "2.5"]

    client._custom_script = _DenyScript(TOKEN_BUCKET_SCRIPT)

    tb = _redis_bucket(capacity=5, refill=2, name="redis-denied-refill")
    r = await tb.acquire(redis_client=client, clock=FakeClock(_START), settings=_FakeSettings())

    assert r.allowed is False
    assert r.retry_after == timedelta(seconds=2.5)
    assert r.remaining == 0.0


# ── explicit ttl override (line 241) ──────────────────────────────────


def test_explicit_ttl_overrides_default() -> None:
    """Passing ttl= explicitly bypasses the default-ttl computation."""
    tb = TokenBucket(
        name="ttl-explicit",
        capacity=100,
        refill_per_second=10,
        backend="memory",
        ttl=timedelta(seconds=123),
    )
    assert tb.ttl == timedelta(seconds=123)


# ── unknown backend dispatch (defensive; requires mutated state) ──────


async def test_acquire_unknown_backend_raises() -> None:
    """acquire() raises RuntimeError for an unrecognised backend value."""
    tb = _bucket()
    tb._backend = "bogus"  # type: ignore[assignment]  # Why: force the defensive else-raise; unreachable via public API
    with pytest.raises(RuntimeError, match="unknown backend: 'bogus'"):
        await tb.acquire(clock=FakeClock(_START))


async def test_peek_unknown_backend_raises() -> None:
    """peek() raises RuntimeError for an unrecognised backend value."""
    tb = _bucket()
    tb._backend = "bogus"  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="unknown backend: 'bogus'"):
        await tb.peek(clock=FakeClock(_START))


async def test_reset_unknown_backend_raises() -> None:
    """reset() raises RuntimeError for an unrecognised backend value."""
    tb = _bucket()
    tb._backend = "bogus"  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="unknown backend: 'bogus'"):
        await tb.reset(clock=FakeClock(_START))


# ── refund() redis dispatch + unknown-backend silent no-op ────────────


async def test_refund_redis_dispatches_to_refund_script() -> None:
    """refund() with backend="redis" invokes the refund Lua script exactly once."""
    client = _FakeRedisClient()
    tb = _redis_bucket(name="refund-redis")
    clock = FakeClock(_START)
    decision = RateLimitDecision(
        allowed=True,
        remaining=95.0,
        retry_after=timedelta(0),
        bucket_name="refund-redis",
        backend="redis",
    )

    await tb.refund(decision, count=5, redis_client=client, clock=clock, settings=_FakeSettings())

    assert len(client.register_script_calls) == 1
    script_obj = tb._redis_refund_script
    assert isinstance(script_obj, _FakeAsyncScript)
    assert len(script_obj.calls) == 1
    call = script_obj.calls[0]
    assert call["keys"][0] == "taskq:taskq_test:rl:tb:{refund-redis}"
    assert call["args"] == [5, _START.timestamp(), 100.0, 10.0]


async def test_refund_unknown_backend_is_silent_noop() -> None:
    """refund() with an unrecognised backend takes no branch and returns None."""
    tb = _bucket()
    tb._backend = "bogus"  # type: ignore[assignment]
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name="test",
        backend="memory",
    )
    result = await tb.refund(decision, clock=FakeClock(_START))
    assert result is None


# ── refund_redis missing-dependency errors ─────────────────────────────


async def test_refund_redis_without_client_raises() -> None:
    tb = _redis_bucket()
    decision = RateLimitDecision(
        allowed=True, remaining=1.0, retry_after=timedelta(0), bucket_name="test", backend="redis"
    )
    with pytest.raises(RuntimeError, match="redis_client not injected for redis backend refund"):
        await tb.refund(decision, clock=FakeClock(_START), settings=_FakeSettings())


async def test_refund_redis_without_clock_raises() -> None:
    tb = _redis_bucket()
    decision = RateLimitDecision(
        allowed=True, remaining=1.0, retry_after=timedelta(0), bucket_name="test", backend="redis"
    )
    with pytest.raises(RuntimeError, match="clock not injected for redis backend refund"):
        await tb.refund(decision, redis_client=_FakeRedisClient(), settings=_FakeSettings())


async def test_refund_redis_without_settings_raises() -> None:
    tb = _redis_bucket()
    decision = RateLimitDecision(
        allowed=True, remaining=1.0, retry_after=timedelta(0), bucket_name="test", backend="redis"
    )
    with pytest.raises(RuntimeError, match="settings not injected for redis backend refund"):
        await tb.refund(decision, redis_client=_FakeRedisClient(), clock=FakeClock(_START))


# ── memory: defensive mem_bucket-not-initialised guards ────────────────


async def test_acquire_memory_bucket_not_initialised_raises() -> None:
    tb = _bucket()
    tb._mem_bucket = None
    with pytest.raises(RuntimeError, match="memory bucket not initialised"):
        await tb.acquire(clock=FakeClock(_START))


async def test_peek_memory_bucket_not_initialised_raises() -> None:
    tb = _bucket()
    tb._mem_bucket = None
    with pytest.raises(RuntimeError, match="memory bucket not initialised"):
        await tb.peek(clock=FakeClock(_START))


async def test_reset_memory_bucket_not_initialised_raises() -> None:
    tb = _bucket()
    tb._mem_bucket = None
    with pytest.raises(RuntimeError, match="memory bucket not initialised"):
        await tb.reset(clock=FakeClock(_START))


async def test_refund_memory_bucket_not_initialised_raises() -> None:
    tb = _bucket()
    tb._mem_bucket = None
    decision = RateLimitDecision(
        allowed=True, remaining=1.0, retry_after=timedelta(0), bucket_name="test", backend="memory"
    )
    with pytest.raises(RuntimeError, match="memory bucket not initialised"):
        await tb.refund(decision, clock=FakeClock(_START))


# ── peek() redis: missing-dependency errors + exhausted retry calc ────


async def test_peek_redis_without_client_raises() -> None:
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="redis_client not injected for redis backend"):
        await tb.peek(clock=FakeClock(_START), settings=_FakeSettings())


async def test_peek_redis_without_clock_raises() -> None:
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="clock not injected for redis backend"):
        await tb.peek(redis_client=_FakeRedisClient(), settings=_FakeSettings())


async def test_peek_redis_without_settings_raises() -> None:
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for redis backend"):
        await tb.peek(redis_client=_FakeRedisClient(), clock=FakeClock(_START))


async def test_peek_redis_exhausted_computes_retry_after() -> None:
    """peek() on redis with tokens<=0 and refill>0 computes a positive retry_after."""
    client = _FakeRedisClient()
    client.hmget_return = ["0.0", str(_START.timestamp())]
    tb = _redis_bucket(capacity=5, refill=2, name="peek-exhausted")
    clock = FakeClock(_START)

    state = await tb.peek(redis_client=client, clock=clock, settings=_FakeSettings())

    assert state.is_exhausted is True
    assert state.backend == "redis"
    assert state.retry_after is not None
    assert abs(state.retry_after.total_seconds() - 0.5) < 1e-9


async def test_peek_redis_missing_key_defaults_to_full_capacity() -> None:
    """peek() with no existing Redis key (hmget returns [None, None]) reports full capacity."""
    client = _FakeRedisClient()
    tb = _redis_bucket(capacity=42, refill=1, name="peek-fresh")

    state = await tb.peek(redis_client=client, clock=FakeClock(_START), settings=_FakeSettings())

    assert state.is_exhausted is False
    assert state.tokens_remaining == 42.0


# ── reset() redis: missing-dependency errors + success path ────────────


async def test_reset_redis_without_client_raises() -> None:
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="redis_client not injected for redis backend"):
        await tb.reset(clock=FakeClock(_START), settings=_FakeSettings())


async def test_reset_redis_without_settings_raises() -> None:
    tb = _redis_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for redis backend"):
        await tb.reset(redis_client=_FakeRedisClient(), clock=FakeClock(_START))


async def test_reset_redis_deletes_key() -> None:
    client = _FakeRedisClient()
    tb = _redis_bucket(name="reset-me")

    await tb.reset(redis_client=client, clock=FakeClock(_START), settings=_FakeSettings())

    assert client.deleted_keys == ["taskq:taskq_test:rl:tb:{reset-me}"]


# ── peek()/reset() postgres: missing-dependency errors ─────────────────


async def test_peek_pg_without_pool_raises() -> None:
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="pg_pool not injected for postgres backend"):
        await tb.peek(clock=FakeClock(_START), settings=_FakeSettings())


async def test_peek_pg_without_settings_raises() -> None:
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for postgres backend"):
        await tb.peek(pg_pool=_FakeFullPgPool(), clock=FakeClock(_START))


async def test_peek_pg_without_clock_raises() -> None:
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="clock not injected for postgres backend"):
        await tb.peek(pg_pool=_FakeFullPgPool(), settings=_FakeSettings())


async def test_peek_pg_exhausted_computes_retry_after() -> None:
    """peek() on postgres with tokens<=0 and refill>0 computes a positive retry_after."""
    now = _START.timestamp()
    pool = _FakeFullPgPool(fetchrow_result={"state": f'{{"tokens": 0.0, "ts": {now}}}'})
    tb = _pg_bucket(capacity=5, refill=2, name="pg-peek-exhausted")

    state = await tb.peek(pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert state.is_exhausted is True
    assert state.backend == "postgres"
    assert state.retry_after is not None
    assert abs(state.retry_after.total_seconds() - 0.5) < 1e-9


async def test_peek_pg_no_row_defaults_to_full_capacity() -> None:
    """peek() on postgres with no existing row reports full capacity."""
    pool = _FakeFullPgPool(fetchrow_result=None)
    tb = _pg_bucket(capacity=42, refill=1, name="pg-peek-fresh")

    state = await tb.peek(pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert state.is_exhausted is False
    assert state.tokens_remaining == 42.0


async def test_reset_pg_without_pool_raises() -> None:
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="pg_pool not injected for postgres backend"):
        await tb.reset(clock=FakeClock(_START), settings=_FakeSettings())


async def test_reset_pg_without_settings_raises() -> None:
    tb = _pg_bucket()
    with pytest.raises(RuntimeError, match="settings not injected for postgres backend"):
        await tb.reset(pg_pool=_FakeFullPgPool(), clock=FakeClock(_START))


async def test_reset_pg_executes_delete() -> None:
    pool = _FakeFullPgPool()
    tb = _pg_bucket(name="pg-reset-me")

    await tb.reset(pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert "DELETE FROM" in sql
    assert args == ("pg-reset-me",)


# ── acquire() postgres: defensive row-is-None fallback (unreachable in
#    normal operation — the preseed INSERT guarantees the row exists
#    before the locking SELECT; this fake pool ignores the preseed and
#    always returns None from fetchrow to force the defensive branch) ──


async def test_acquire_pg_defensive_row_none_uses_full_capacity() -> None:
    pool = _FakeFullPgPool(fetchrow_result=None)
    tb = _pg_bucket(capacity=10, refill=1, name="pg-row-none")

    r = await tb.acquire(count=3, pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert r.allowed is True
    assert r.remaining == 7.0
    assert r.backend == "postgres"


# ── acquire() postgres: existing-row decode (allowed + denied) ────────


async def test_acquire_pg_existing_row_allowed_decodes_state() -> None:
    """acquire() decodes an existing row's jsonb state and allows when tokens suffice."""
    now = _START.timestamp()
    pool = _FakeFullPgPool(fetchrow_result={"state": f'{{"tokens": 10.0, "ts": {now}}}'})
    tb = _pg_bucket(capacity=10, refill=1, name="pg-row-existing-allowed")

    r = await tb.acquire(count=4, pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert r.allowed is True
    assert r.remaining == 6.0
    assert r.retry_after == timedelta(0)


async def test_acquire_pg_existing_row_denied_with_refill_computes_retry_after() -> None:
    """acquire() on an existing exhausted row with refill>0 denies and computes retry_after."""
    now = _START.timestamp()
    pool = _FakeFullPgPool(fetchrow_result={"state": f'{{"tokens": 0.0, "ts": {now}}}'})
    tb = _pg_bucket(capacity=10, refill=2, name="pg-row-existing-denied")

    r = await tb.acquire(count=5, pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert r.allowed is False
    assert r.retry_after is not None
    assert abs(r.retry_after.total_seconds() - 2.5) < 1e-9


async def test_acquire_pg_existing_row_denied_fixed_quota_retry_after_none() -> None:
    """acquire() on an existing exhausted row with refill=0 denies with retry_after=None."""
    now = _START.timestamp()
    pool = _FakeFullPgPool(fetchrow_result={"state": f'{{"tokens": 0.0, "ts": {now}}}'})
    tb = _pg_bucket(capacity=10, refill=0, name="pg-row-existing-fixed")

    r = await tb.acquire(count=1, pg_pool=pool, clock=FakeClock(_START), settings=_FakeSettings())

    assert r.allowed is False
    assert r.retry_after is None

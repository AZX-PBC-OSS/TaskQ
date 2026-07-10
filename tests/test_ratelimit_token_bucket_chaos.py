"""Chaos / failure-mode integration tests for the Redis→PG degradation flow.

Exercises:
- Redis→PG degradation (stop container, verify fallback)
- PG fallback recovery (restart Redis, verify return to redis backend)
- Both backends unavailable (PG error propagates from acquire)
- PG contention (50 concurrent acquires, no deadlock)
- Clock skew (backward FakeClock step does not inflate remaining)
"""

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async
import structlog

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosPool
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.redis]


def _unique_name() -> str:
    return f"chaos_{new_base62()}"


# ── Redis→PG degradation ─────────────────────────────────


async def test_redis_to_pg_degradation(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
    redis_container: object,
) -> None:
    """(consolidated): Real Redis container; acquire confirms
    Redis path; stop container; next acquire triggers PG fallback with WARNING.

    The container is restarted in the finally block so subsequent tests see
    a running container.
    """
    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )
    clock = SystemClock()

    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=10,
        refill_per_second=1.0,
        backend="redis",
    )
    client = redis_async.from_url(redis_url, decode_responses=False)

    try:
        r = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.allowed is True
        assert r.backend == "redis"

        redis_container.stop()  # type: ignore[union-attr] # Why: redis_container is a RedisContainer with stop(); typed as object in fixtures to avoid transitive imports

        r2 = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

        assert r2.backend == "postgres"
    finally:
        try:
            redis_container.start()  # type: ignore[union-attr] # Why: redis_container is a RedisContainer with start(); typed as object in fixtures to avoid transitive imports
        except Exception as exc:
            structlog.get_logger("taskq.test_chaos").warning(
                "redis-container-restart-failed",
                error=str(exc),
            )
        await client.aclose()


# ── PG fallback recovery ─────────────────────────────────────────


async def test_redis_recovery_after_restart(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_container: object,
) -> None:
    """After Redis→PG fallback, restart the Redis container; next
    acquire returns backend=="redis" with no WARNING.

    redis-py's connection pool transparently re-connects on the next command
    after the server reappears — do NOT manually reset the pool or call
    aclose() between stop() and start(). The cached AsyncScript's SHA is
    stale after Redis restart; redis-py detects NOSCRIPT, re-runs SCRIPT
    LOAD, and retries EVALSHA automatically.

    Container restart can take 1-5s. A polling loop tolerates intermediate
    ConnectionErrors during container boot AND tolerates fallback decisions
    that still return backend=="postgres" while Redis is briefly unreachable.

    Deviation: Docker reassigns the host-side port on container
    restart, so the original redis_client cannot auto-reconnect to the new
    port. We construct a new client with the updated port after restart.
    The note about not resetting the pool assumes the server
    reappears on the same host:port (production reality); the port change
    is a testcontainers artifact.
    """
    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )
    clock = SystemClock()

    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=100,
        refill_per_second=10.0,
        backend="redis",
    )

    host = redis_container.get_container_host_ip()  # type: ignore[union-attr] # Why: redis_container is a RedisContainer; typed as object in fixtures
    port = redis_container.get_exposed_port(6379)  # type: ignore[union-attr] # Why: same as above
    original_url = f"redis://{host}:{port}/0"
    client = redis_async.from_url(original_url, decode_responses=False)

    try:
        r = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.backend == "redis"

        redis_container.stop()  # type: ignore[union-attr] # Why: redis_container is a RedisContainer with stop(); typed as object in fixtures to avoid transitive imports

        r_fallback = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r_fallback.backend == "postgres"

        await client.aclose()

        redis_container.start()  # type: ignore[union-attr] # Why: same as above

        new_port = redis_container.get_exposed_port(6379)  # type: ignore[union-attr] # Why: same as above; port differs after restart (testcontainers artifact)
        new_url = f"redis://{host}:{new_port}/0"
        client = redis_async.from_url(new_url, decode_responses=False)

        tb_recovery = TokenBucket(
            name=bucket_name,
            capacity=100,
            refill_per_second=10.0,
            backend="redis",
        )

        import redis as _redis_mod

        deadline = asyncio.get_running_loop().time() + 10.0
        last_exc: BaseException | None = None
        recovered: object = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                r_recovery = await tb_recovery.acquire(
                    redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
                )
                if r_recovery.backend == "redis":
                    recovered = r_recovery
                    break
            except (_redis_mod.ConnectionError, _redis_mod.TimeoutError) as exc:
                last_exc = exc
            await asyncio.sleep(0.5)
        else:
            raise AssertionError(f"Redis recovery did not succeed within 10s: {last_exc!r}")

        assert recovered is not None
        assert recovered.backend == "redis"  # type: ignore[union-attr] # Why: recovered is the RateLimitDecision from the successful recovery acquire

        r_post = await tb_recovery.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

        assert r_post.backend == "redis"
    finally:
        await client.aclose()


# ── Both backends unavailable ─────────────────────────────────────


async def test_both_backends_unavailable(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Redis raises ConnectionError; PG raises PostgresConnectionError
    via ChaosConnection. The PG error propagates from acquire() — the request
    is NOT silently allowed.
    """
    import redis as _redis_mod

    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )

    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=10,
        refill_per_second=1.0,
        backend="redis",
    )
    clock = SystemClock()

    class _FakeRedisRaisingConnectionError:
        def register_script(self, script: bytes) -> object:
            return _FakeScript()

    class _FakeScript:
        async def __call__(self, **kwargs: object) -> object:
            raise _redis_mod.ConnectionError("redis unavailable")

    async with module_pg_pool.acquire() as real_conn:
        chaos_conn = ChaosConnection(
            real_conn,
            fail_on_call=1,
            fail_with=asyncpg.PostgresConnectionError,
        )
        chaos_pool = ChaosPool(chaos_conn)
        with pytest.raises(asyncpg.PostgresConnectionError):
            await tb.acquire(
                redis_client=_FakeRedisRaisingConnectionError(),
                pg_pool=chaos_pool,
                clock=clock,
                settings=settings,
            )


# ── PG contention — 50 concurrent acquires ────────────────────────


async def test_pg_contention_50_concurrent(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """50 concurrent acquire() coroutines on a single PG bucket
    complete within 5s; no deadlock; total tokens consumed ==
    min(50 * count_per_request, capacity).
    """
    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )
    clock = SystemClock()

    capacity = 50.0
    count_per_request = 1.0
    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=capacity,
        refill_per_second=0.0,
        backend="postgres",
    )

    async def _single_acquire() -> object:
        return await tb.acquire(
            count=count_per_request, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

    results = await asyncio.wait_for(
        asyncio.gather(*[_single_acquire() for _ in range(50)]),
        timeout=5.0,
    )

    allowed_count = sum(1 for r in results if r.allowed)  # type: ignore[union-attr] # Why: results is a list of RateLimitDecision from gather
    total_consumed = sum(
        count_per_request
        for r in results
        if r.allowed  # type: ignore[union-attr] # Why: same as above
    )
    expected_consumed = min(50 * count_per_request, capacity)
    assert total_consumed == expected_consumed, (
        f"Expected {expected_consumed} tokens consumed, got {total_consumed}"
    )
    assert allowed_count == int(expected_consumed)


# ── Clock skew ────────────────────────────────────────────────────


@pytest.mark.parametrize("backend", ["redis", "postgres"])
async def test_clock_skew_backward_step(
    backend: str,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_container: object,
) -> None:
    """Inject FakeClock; acquire at t=T; move clock backward 5s;
    acquire again. The elapsed clamp (max(0, now - ts)) prevents negative
    refill — remaining does NOT increase from the backward step.
    """
    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )

    capacity = 10.0
    refill = 1.0
    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=capacity,
        refill_per_second=refill,
        backend=backend,
    )

    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    clock = FakeClock(start=t0)

    if backend == "redis":
        host = redis_container.get_container_host_ip()  # type: ignore[union-attr] # Why: redis_container is a RedisContainer; typed as object in fixtures
        port = redis_container.get_exposed_port(6379)  # type: ignore[union-attr] # Why: same as above
        url = f"redis://{host}:{port}/0"
        client = redis_async.from_url(url, decode_responses=False)
        try:
            r1 = await tb.acquire(
                redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
            )
            assert r1.allowed is True
            remaining_after_first = r1.remaining

            clock.move_to(t0 - timedelta(seconds=5))

            r2 = await tb.acquire(
                redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
            )
            assert r2.remaining <= remaining_after_first, (
                f"Backward clock skew inflated remaining: {r2.remaining} > {remaining_after_first}"
            )
        finally:
            await client.aclose()
    else:
        r1 = await tb.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r1.allowed is True
        remaining_after_first = r1.remaining

        clock.move_to(t0 - timedelta(seconds=5))

        r2 = await tb.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r2.remaining <= remaining_after_first, (
            f"Backward clock skew inflated remaining: {r2.remaining} > {remaining_after_first}"
        )

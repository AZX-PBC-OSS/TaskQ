"""Chaos / failure-mode integration tests for the Redis→PG degradation flow.

Exercises:
- Redis→PG degradation (stop container, verify fallback)
- PG fallback recovery (restart Redis, verify return to redis backend)
- Both backends unavailable (PG error propagates from acquire)
- PG contention (50 concurrent acquires, no deadlock)
- Clock skew (a backward step of the STORE clock neither refills nor indebts)
"""

import asyncio

import asyncpg
import pytest
import redis.asyncio as redis_async
import structlog

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.asyncpg_chaos import ChaosConnection, ChaosPool
from taskq.testing.fixtures import (  # pyright: ignore[reportPrivateUsage]  # Why: asserting the concrete shim type below is the pin; private prefix scopes it to the testing package (same pattern as _create_worker).
    ModulePgSchema,
    RedisContainerLike,
    _RedisContainerShim,
    redis_url_for,
)

pytestmark = [pytest.mark.integration, pytest.mark.redis]


def _unique_name() -> str:
    return f"chaos_{new_base62()}"


# ── Redis→PG degradation ─────────────────────────────────


async def test_redis_to_pg_degradation(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    killable_redis_container: object,
) -> None:
    """(consolidated): Real Redis container; acquire confirms
    Redis path; stop container; next acquire triggers PG fallback with WARNING.

    Uses a function-scoped killable container — the session container is
    shared by every module and must never be stopped (a restart can also
    remap the host port, invalidating the session URL for unrelated tests).
    The container is restarted in the finally block so the recovery path
    is exercised too.
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
    client = redis_async.from_url(redis_url_for(killable_redis_container), decode_responses=False)

    try:
        r = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.allowed is True
        assert r.backend == "redis"

        # Why: docker-py stop blocks the loop for the whole HTTP round-trip
        # (measured 2.4-3.8s continuous loop stalls) — off-loop.
        await asyncio.to_thread(killable_redis_container.stop)  # type: ignore[union-attr] # Why: fixture typed object to avoid transitive imports

        r2 = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

        assert r2.backend == "postgres"
    finally:
        try:
            # Why: docker-py start (+ readiness wait) blocks the loop — off-loop.
            await asyncio.to_thread(killable_redis_container.start)  # type: ignore[union-attr] # Why: fixture typed object to avoid transitive imports
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
    killable_redis_container: object,
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

    # Why: docker host/port resolution can hit the docker HTTP API (port
    # inspect + status poll) — off-loop.
    host = await asyncio.to_thread(killable_redis_container.get_container_host_ip)  # type: ignore[union-attr] # Why: fixture typed object to avoid transitive imports
    port = await asyncio.to_thread(killable_redis_container.get_exposed_port, 6379)  # type: ignore[union-attr] # Why: same as above
    original_url = f"redis://{host}:{port}/0"
    client = redis_async.from_url(original_url, decode_responses=False)

    try:
        r = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.backend == "redis"

        # Why: docker-py stop blocks the loop for the whole HTTP round-trip
        # (measured 2.4-3.8s continuous loop stalls) — off-loop.
        await asyncio.to_thread(killable_redis_container.stop)  # type: ignore[union-attr] # Why: fixture typed object to avoid transitive imports

        r_fallback = await tb.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r_fallback.backend == "postgres"

        await client.aclose()

        # Why: docker-py start (+ readiness wait) blocks the loop — off-loop.
        await asyncio.to_thread(killable_redis_container.start)  # type: ignore[union-attr] # Why: fixture typed object to avoid transitive imports

        # Why: get_exposed_port inspects the container via docker HTTP — off-loop.
        new_port = await asyncio.to_thread(
            killable_redis_container.get_exposed_port,  # type: ignore[union-attr] # Why: port differs after restart (testcontainers artifact)
            6379,
        )
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


# ── Clock skew: the STORE's clock, in the store's own domain ──────────
#
# The refill math for the redis and postgres backends runs entirely inside
# the store (``redis.call('TIME')`` / ``clock_timestamp()``); no Python clock
# participates.  Simulating a backward step of that clock therefore means
# moving the store's own persisted ``ts`` FORWARD relative to the store's
# now — which is exactly the state a backward step leaves behind: the last
# write is stamped in the future.  Injecting a FakeClock here would be inert.

_SKEW_SECS = 600.0


async def _skew_redis_ts_forward(
    client: "redis_async.Redis", schema: str, name: str, seconds: float
) -> None:
    """Move the bucket's stored ``ts`` *seconds* into the store's future."""
    key = f"taskq:{schema}:rl:tb:{{{name}}}"
    stored = await client.hget(key, "ts")  # pyright: ignore[reportGeneralTypeIssues, reportUnknownMemberType]  # Why: redis-py's async hget is typed as returning Awaitable[Any].
    assert stored is not None, "acquire must have persisted a ts to skew"
    await client.hset(key, "ts", str(float(stored) + seconds))  # pyright: ignore[reportGeneralTypeIssues, reportUnknownMemberType]  # Why: as above.


async def _skew_pg_ts_forward(pool: asyncpg.Pool, schema: str, name: str, seconds: float) -> None:
    """PG analog of :func:`_skew_redis_ts_forward`, in the server domain."""
    async with pool.acquire() as conn:
        updated = await conn.execute(
            f'UPDATE "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema is fixture-derived; values are $1/$2-bound
            f"SET state = jsonb_set(state, '{{ts}}', "
            f"to_jsonb((state->>'ts')::float8 + $2::float8)) "
            f"WHERE bucket_name = $1",
            name,
            seconds,
        )
    assert updated == "UPDATE 1", "acquire must have persisted a bucket row to skew"


@pytest.mark.parametrize("backend", ["redis", "postgres"])
async def test_a_backward_step_of_the_store_clock_neither_refills_nor_indebts(
    backend: str,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_container: RedisContainerLike,
) -> None:
    """A backward step of the store clock must cost the bucket nothing and
    credit it nothing.

    Acquire once from a capacity-10 bucket (remaining 9), then step the store
    clock back 600s by stamping the persisted ``ts`` 600s into the store's
    future, then acquire again.  The elapsed clamp turns those 600
    unaccountable seconds into zero refill, so the second acquire is admitted
    and leaves exactly 8.

    Without the clamp the refill term runs in reverse: 9 - 600*1.0 = -591, the
    acquire is DENIED, and the debt outlives the clock recovering because
    ``ts`` is restamped forward on every acquire.  That is the regression this
    pins — an assertion of the form ``remaining <= remaining_after_first``
    would accept the debt, since a token debt is also a decrease.
    """
    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )

    bucket_name = _unique_name()
    tb = TokenBucket(
        name=bucket_name,
        capacity=10.0,
        refill_per_second=1.0,
        backend=backend,
    )

    if backend == "redis":
        # Pin the no-docker-I/O invariant: the shared-pair shim's host/port
        # accessors are pure attributes, so these sync calls cannot block the
        # loop. If the fixture ever backs a real testcontainers object again
        # (blocking docker-py HTTP per call), this assert fires here instead
        # of the stall surfacing as a mystery loop-blocker.
        assert isinstance(redis_container, _RedisContainerShim)
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        client = redis_async.from_url(f"redis://{host}:{port}/0", decode_responses=False)
        try:
            r1 = await tb.acquire(redis_client=client, pg_pool=module_pg_pool, settings=settings)
            assert r1.allowed is True
            assert r1.remaining == pytest.approx(9.0)

            await _skew_redis_ts_forward(client, schema, bucket_name, _SKEW_SECS)

            r2 = await tb.acquire(redis_client=client, pg_pool=module_pg_pool, settings=settings)
        finally:
            await client.aclose()
    else:
        r1 = await tb.acquire(pg_pool=module_pg_pool, settings=settings)
        assert r1.allowed is True
        assert r1.remaining == pytest.approx(9.0)

        await _skew_pg_ts_forward(module_pg_pool, schema, bucket_name, _SKEW_SECS)

        r2 = await tb.acquire(pg_pool=module_pg_pool, settings=settings)

    assert r2.allowed is True, (
        f"a backward store-clock step must not deny: remaining={r2.remaining}"
    )
    assert r2.remaining == pytest.approx(8.0), (
        f"a backward store-clock step must neither refill nor indebt the "
        f"bucket: expected 8.0, got {r2.remaining}"
    )

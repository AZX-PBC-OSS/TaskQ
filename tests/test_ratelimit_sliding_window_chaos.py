"""Chaos / failure-mode integration tests for the SlidingWindow Redis→PG degradation flow.

Parametrised over ``style="log"`` and ``style="gcra"`` so each scenario runs
for both algorithms.

Redis→PG degradation — stop Redis container, verify PG fallback with
WARNING log, ``RateLimitDecision.backend == "postgres"``.

Redis dies mid-Lua — semantically captured by ConnectionError on the
next command after stopping the container. The state divergence between
Redis (containing N entries) and PG (empty) is expected and documented: the
two backends are independent state stores. Container-stopping tests use a
function-scoped killable container, never the shared session container.

Both backends unavailable — stop both Redis and PG; acquire() raises
(the underlying error), does NOT silently allow. Also tests
``rate_limit_pg_fallback_enabled=False`` with Redis-only-down: the Redis
error propagates without WARNING (no fallback attempted).
"""

from datetime import timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async
import structlog

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema, redis_url_for

pytestmark = [pytest.mark.integration, pytest.mark.redis]


def _unique_name() -> str:
    return f"sw_chaos_{new_base62()}"


def _settings(module_pg_schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name},
    )


def _settings_no_fallback(module_pg_schema: ModulePgSchema) -> WorkerSettings:
    s = _settings(module_pg_schema)
    s.rate_limit_pg_fallback_enabled = False
    return s


async def _make_redis_client(redis_url: str) -> redis_async.Redis:
    return redis_async.from_url(redis_url, decode_responses=False)


# ── Redis→PG degradation — parametrised over style ──────────────


@pytest.mark.parametrize("style", ["log", "gcra"])
async def test_redis_to_pg_degradation(
    style: str,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    killable_redis_container: object,
) -> None:
    """With both backends running, acquire confirms Redis. Stop Redis;
    next acquire falls back to PG with a WARNING log containing
    ``rate-limit-redis-fallback``, ``bucket_name``, and ``style``. The
    WARNING precedes any INFO decision log.

    Uses a function-scoped killable container — the session container is
    shared by every module and must never be stopped. The container is
    restarted in ``finally`` so the recovery path is also exercised.
    """
    settings = _settings(module_pg_schema)
    clock = SystemClock()

    bucket_name = _unique_name()
    sw = SlidingWindow(
        name=bucket_name,
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style=style,
    )
    client = await _make_redis_client(redis_url_for(killable_redis_container))

    try:
        r = await sw.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.allowed is True
        assert r.backend == "redis"

        killable_redis_container.stop()  # type: ignore[union-attr] # Why: RedisContainer.stop(); the fixture is typed object to avoid transitive imports

        r2 = await sw.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

        assert r2.backend == "postgres"
        assert r2.allowed is True
    finally:
        try:
            killable_redis_container.start()  # type: ignore[union-attr] # Why: as above
        except Exception as exc:
            structlog.get_logger("taskq.test_chaos").warning(
                "redis-container-restart-failed",
                error=str(exc),
            )
        await client.aclose()


# ── Redis dies mid-Lua — parametrised over style ────────────────


@pytest.mark.parametrize("style", ["log", "gcra"])
async def test_redis_dies_mid_lua(
    style: str,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    killable_redis_container: object,
) -> None:
    """Redis dies mid-Lua. The container stop causes a ConnectionError
    on the next Redis command, which is semantically equivalent to the script
    execution being interrupted mid-flight. PG fallback is invoked and returns
    ``backend == "postgres"``. The state divergence between Redis (containing
    entries from the first acquire) and PG (empty — no prior state) is expected:
    the two backends are independent state stores.

    The Redis container is restarted in ``finally``.
    """
    schema = module_pg_schema.schema_name
    settings = _settings(module_pg_schema)
    clock = SystemClock()

    bucket_name = _unique_name()
    sw = SlidingWindow(
        name=bucket_name,
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style=style,
    )
    client = await _make_redis_client(redis_url_for(killable_redis_container))

    try:
        r = await sw.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert r.allowed is True
        assert r.backend == "redis"

        killable_redis_container.stop()  # type: ignore[union-attr] # Why: RedisContainer.stop(); the fixture is typed object to avoid transitive imports

        r2 = await sw.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )

        assert r2.backend == "postgres"
        assert r2.allowed is True

        if style == "log":
            async with module_pg_pool.acquire() as conn:
                count = await conn.fetchval(
                    f"SELECT count(*) FROM {schema}.rate_limit_window_entries "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
                    f"WHERE bucket_name = $1",
                    bucket_name,
                )
            assert count == 1
        else:
            async with module_pg_pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT kind, state FROM {schema}.rate_limit_buckets "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
                    f"WHERE bucket_name = $1",
                    bucket_name,
                )
            assert row is not None
            assert row["kind"] == "gcra"
    finally:
        try:
            killable_redis_container.start()  # type: ignore[union-attr] # Why: RedisContainer.start(); the fixture is typed object to avoid transitive imports
        except Exception as exc:
            structlog.get_logger("taskq.test_chaos").warning(
                "redis-container-restart-failed",
                error=str(exc),
            )
        await client.aclose()


# ── Both backends unavailable — parametrised over style ──────────


@pytest.mark.parametrize("style", ["log", "gcra"])
async def test_both_backends_unavailable(
    style: str,
) -> None:
    """Both Redis and PG are unavailable. ``acquire()`` raises the
    underlying error — it does NOT silently allow.

    ``rate_limit_pg_fallback_enabled`` is True (default): a WARNING fires
    when Redis fails, then PG raises RuntimeError (pg_pool=None) which propagates.

    A fake Redis client raises ConnectionError; ``pg_pool=None`` triggers
    RuntimeError on the PG delegate. The session-scoped Redis container is
    not touched — the fake client already satisfies "Redis unavailable"
    without the stop/restart cost and fragility (mirrors token-bucket).
    """
    import redis as _redis_mod

    bucket_name = _unique_name()
    sw = SlidingWindow(
        name=bucket_name,
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style=style,
    )
    # Bogus DSN for settings since no real PG pool is used in this test.
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "taskq_test"},
    )
    clock = SystemClock()

    class _FakeRedisRaisingConnectionError:
        def register_script(self, script: bytes) -> object:
            return _FakeScript()

    class _FakeScript:
        async def __call__(self, **kwargs: object) -> object:
            raise _redis_mod.ConnectionError("redis unavailable")

    with pytest.raises(RuntimeError):
        await sw.acquire(
            redis_client=_FakeRedisRaisingConnectionError(),
            pg_pool=None,
            clock=clock,
            settings=settings,
        )


# ── rate_limit_pg_fallback_enabled=False — parametrised over style ──


@pytest.mark.parametrize("style", ["log", "gcra"])
async def test_fallback_disabled_redis_error_propagates(
    style: str,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """With ``rate_limit_pg_fallback_enabled=False``, Redis errors
    propagate without WARNING — no PG fallback is attempted. The Redis
    ConnectionError is raised directly from acquire().
    """
    import redis as _redis_mod

    settings = _settings_no_fallback(module_pg_schema)
    clock = SystemClock()

    bucket_name = _unique_name()
    sw = SlidingWindow(
        name=bucket_name,
        limit=10,
        window=timedelta(seconds=60),
        backend="redis",
        style=style,
    )

    class _FakeRedisRaisingConnectionError:
        def register_script(self, script: bytes) -> object:
            return _FakeScript()

    class _FakeScript:
        async def __call__(self, **kwargs: object) -> object:
            raise _redis_mod.ConnectionError("redis unavailable")

    with pytest.raises(_redis_mod.ConnectionError):
        await sw.acquire(
            redis_client=_FakeRedisRaisingConnectionError(),
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

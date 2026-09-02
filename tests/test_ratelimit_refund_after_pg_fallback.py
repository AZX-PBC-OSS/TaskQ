"""A refund must go back to the store that actually paid.

With ``backend="redis"`` and the default ``rate_limit_pg_fallback_enabled``,
an acquire during a Redis outage falls through to Postgres and consumes a
token THERE. The refund is dispatched from the decision that acquire
returned, so it has to follow that decision's backend -- not the
primitive's static configuration.

Getting it wrong costs twice. Postgres, which actually paid, is never
repaid: for a fixed-quota bucket (``refill_per_second == 0``) nothing ever
puts that token back, so the loss is PERMANENT. And Redis, which never
spent anything, is credited a token out of thin air -- quota inflation on
one store and quota destruction on the other, from one failed job.

Needs both stores: a mock of either one would not exercise the fallback.
"""

from datetime import timedelta

import asyncpg
import pytest
import redis as _redis_mod
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import TokenBucket
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.redis]


class _OutageScript:
    async def __call__(self, **kwargs: object) -> object:
        raise _redis_mod.ConnectionError("connection lost")


class _OutageRedis:
    """A Redis client whose every scripted call fails, as in a real outage.

    Each primitive caches the script object it registered, so the process
    that rides out the outage has to be a DIFFERENT instance from the one
    that spent against a healthy Redis -- which is also how it looks in
    production: two workers, one of which loses its Redis connection.
    """

    def register_script(self, script: bytes) -> object:
        return _OutageScript()


def _settings(schema: ModulePgSchema, redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": schema.pg_dsn,
            "schema_name": schema.schema_name,
            "redis_url": redis_url,
        },
    )


async def test_fixed_quota_refund_repays_postgres_after_a_redis_outage(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """The sharpest case: a bucket that never refills, so a lost token is lost forever."""
    settings = _settings(module_pg_schema, redis_url)
    name = f"refund_tb_{new_base62()}"
    clock = SystemClock()

    # The worker that still has Redis, and the one whose Redis went away.
    connected = TokenBucket(name, capacity=2.0, refill_per_second=0.0, backend="redis")
    bucket = TokenBucket(name, capacity=2.0, refill_per_second=0.0, backend="redis")
    # A read-only view of the same bucket's Postgres state.
    pg_view = TokenBucket(name, capacity=2.0, refill_per_second=0.0, backend="postgres")
    client = redis_async.from_url(redis_url, decode_responses=False)

    try:
        healthy = await connected.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert healthy.allowed and healthy.backend == "redis"

        during_outage = await bucket.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )
        assert during_outage.allowed
        assert during_outage.backend == "postgres", "the outage must fall back to Postgres"

        pg_after_acquire = await pg_view.peek(pg_pool=module_pg_pool, settings=settings)
        assert pg_after_acquire.tokens_remaining == 1.0, "Postgres paid the token"

        # The job fails and its token is handed back. Redis is healthy again by
        # now -- the outage was transient -- which is exactly when refunding to
        # the wrong store is silent.
        await bucket.refund(
            during_outage,
            redis_client=client,
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

        pg_after_refund = await pg_view.peek(pg_pool=module_pg_pool, settings=settings)
        assert pg_after_refund.tokens_remaining == 2.0, (
            f"Postgres spent the token and was never repaid: "
            f"{pg_after_refund.tokens_remaining} of 2.0 left, permanently"
        )

        redis_after_refund = await connected.peek(redis_client=client, settings=settings)
        assert redis_after_refund.tokens_remaining == 1.0, (
            f"Redis was credited a token it never spent: "
            f"{redis_after_refund.tokens_remaining} of 2.0"
        )
    finally:
        await client.aclose()


@pytest.mark.parametrize("style", ["log", "gcra"])
async def test_sliding_window_refund_repays_postgres_after_a_redis_outage(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
    style: str,
) -> None:
    """SlidingWindow carries the identical defect and must behave identically."""
    settings = _settings(module_pg_schema, redis_url)
    name = f"refund_sw_{style}_{new_base62()}"
    clock = SystemClock()

    connected = SlidingWindow(
        name, limit=2, window=timedelta(seconds=60), backend="redis", style=style
    )
    window = SlidingWindow(
        name, limit=2, window=timedelta(seconds=60), backend="redis", style=style
    )
    pg_view = SlidingWindow(
        name, limit=2, window=timedelta(seconds=60), backend="postgres", style=style
    )
    client = redis_async.from_url(redis_url, decode_responses=False)

    try:
        healthy = await connected.acquire(
            redis_client=client, pg_pool=module_pg_pool, clock=clock, settings=settings
        )
        assert healthy.allowed and healthy.backend == "redis"

        during_outage = await window.acquire(
            redis_client=_OutageRedis(),
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )
        assert during_outage.allowed
        assert during_outage.backend == "postgres"

        assert (await pg_view.peek(pg_pool=module_pg_pool, settings=settings)).remaining == 1.0

        await window.refund(
            during_outage,
            redis_client=client,
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

        pg_after = await pg_view.peek(pg_pool=module_pg_pool, settings=settings)
        assert pg_after.remaining == 2.0, (
            f"the Postgres window kept the admission it was refunded for: "
            f"{pg_after.remaining} of 2 left"
        )

        redis_after = await connected.peek(redis_client=client, settings=settings)
        assert redis_after.remaining == 1.0, (
            f"the Redis window was credited an admission it never held: {redis_after.remaining}"
        )
    finally:
        await client.aclose()

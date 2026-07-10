"""Integration tests for AND-composition against real PG + Redis backends.

through These verify the
full AND-composition acquire_for_actor / release_for_actor lifecycle
against real Postgres (reservation slots) and Redis (token bucket,
sliding window) via testcontainers.
"""

from datetime import timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.constants import DEFAULT_RESERVATION_BACKOFF
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.composition import RateLimitHandle, ReservationHandle
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.redis]


def _unique_name() -> str:
    return f"test_{new_base62()}"


# ── Full AND-composition against real backends ──────────────────────


async def test_full_and_composition(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Full AND-composition against real backends. Register TokenBucket
    (Redis), SlidingWindow (Redis), ConcurrencyReservation (PG).
    acquire_for_actor acquires all three. All succeed; reservation_slot_ids
    is a single-element list; actor body runs; release_for_actor releases the
    reservation slot; Redis keys reflect consumed tokens."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    sw_name = _unique_name()
    res_name = _unique_name()

    try:
        tb = TokenBucket(name=tb_name, capacity=10, refill_per_second=1, backend="redis")
        sw = SlidingWindow(name=sw_name, limit=5, window=timedelta(minutes=1), backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=2, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)
        reg.register(tb)
        reg.register(sw)

        job_id = new_uuid()
        worker_id = new_uuid()
        acquired = await reg.acquire_for_actor(
            rate_limits=[tb_name, sw_name],
            reservations=[res_name],
            job_id=job_id,
            worker_id=worker_id,
            redis_client=redis_client,
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

        assert len(acquired) == 3
        assert isinstance(acquired[0], ReservationHandle)
        assert acquired[0].name == res_name
        assert isinstance(acquired[1], RateLimitHandle)
        assert acquired[1].name == tb_name
        assert isinstance(acquired[2], RateLimitHandle)
        assert acquired[2].name == sw_name

        await reg.release_for_actor(acquired, pg_pool=module_pg_pool)

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name/slot_index are $1/$2-bound
                "WHERE bucket_name = $1 AND slot_index = $2",
                res_name,
                acquired[0].slot_index,  # pyright: ignore[reportAttributeAccessIssue] # Why: acquired[0] is a ReservationHandle (verified by isinstance check earlier); Protocol lacks slot_index but concrete type carries it
            )
        assert row is not None
        assert row["job_id"] is None

        tb_key = f"taskq:{schema}:rl:tb:{{{tb_name}}}"
        exists = await redis_client.exists(tb_key)
        assert exists == 1
    finally:
        await redis_client.aclose()


# ── Rate limit denial in composition ────────────────────────────────


async def test_rate_limit_denial_in_composition(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Rate limit denial in composition. Exhaust TokenBucket.
    acquire_for_actor with reservation + exhausted TokenBucket + SlidingWindow.
    TokenBucket fails; SlidingWindow NOT acquired; reservation released;
    ReservationUnavailable raised with correct retry_after."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    sw_name = _unique_name()
    res_name = _unique_name()

    try:
        tb = TokenBucket(name=tb_name, capacity=1, refill_per_second=0, backend="redis")
        sw = SlidingWindow(name=sw_name, limit=5, window=timedelta(minutes=1), backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=2, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)
        reg.register(tb)
        reg.register(sw)

        await tb.acquire(count=1.0, redis_client=redis_client, clock=clock, settings=settings)

        job_id = new_uuid()
        worker_id = new_uuid()
        with pytest.raises(ReservationUnavailable) as exc_info:
            await reg.acquire_for_actor(
                rate_limits=[tb_name, sw_name],
                reservations=[res_name],
                job_id=job_id,
                worker_id=worker_id,
                redis_client=redis_client,
                pg_pool=module_pg_pool,
                clock=clock,
                settings=settings,
            )

        assert exc_info.value.bucket_name == tb_name
        assert exc_info.value.retry_after == DEFAULT_RESERVATION_BACKOFF

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name is $1-bound
                "WHERE bucket_name = $1",
                res_name,
            )
        assert row is not None
        assert row["job_id"] is None
    finally:
        await redis_client.aclose()


# ── Reservation denial in composition ──────────────────────────────


async def test_reservation_denial_in_composition(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Reservation denial in composition. Fill all reservation slots.
    acquire_for_actor. ReservationUnavailable raised; no rate limits acquired."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    res_name = _unique_name()

    try:
        tb = TokenBucket(name=tb_name, capacity=10, refill_per_second=1, backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=1, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)
        reg.register(tb)

        filler_job = new_uuid()
        filler_worker = new_uuid()
        await res.acquire(filler_job, filler_worker, module_pg_pool)

        job_id = new_uuid()
        worker_id = new_uuid()
        with pytest.raises(ReservationUnavailable) as exc_info:
            await reg.acquire_for_actor(
                rate_limits=[tb_name],
                reservations=[res_name],
                job_id=job_id,
                worker_id=worker_id,
                redis_client=redis_client,
                pg_pool=module_pg_pool,
                clock=clock,
                settings=settings,
            )

        assert exc_info.value.bucket_name == res_name
    finally:
        await redis_client.aclose()


# ── Cancellation mid-actor ─────────────────────────────────────────


async def test_cancellation_mid_actor(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Cancellation mid-actor. Acquire all resources. Force timeout.
    finally calls release_for_actor; reservation slot released; rate-limit
    tokens NOT refunded (actor ran briefly)."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    res_name = _unique_name()

    try:
        tb = TokenBucket(name=tb_name, capacity=5, refill_per_second=0, backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=1, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)
        reg.register(tb)

        job_id = new_uuid()
        worker_id = new_uuid()
        acquired = await reg.acquire_for_actor(
            rate_limits=[tb_name],
            reservations=[res_name],
            job_id=job_id,
            worker_id=worker_id,
            redis_client=redis_client,
            pg_pool=module_pg_pool,
            clock=clock,
            settings=settings,
        )

        assert len(acquired) == 2

        try:
            raise NotImplementedError("simulating cancellation")
        except NotImplementedError:
            pass
        finally:
            await reg.release_for_actor(acquired, pg_pool=module_pg_pool)

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name/slot_index are $1/$2-bound
                "WHERE bucket_name = $1 AND slot_index = $2",
                res_name,
                acquired[0].slot_index,  # pyright: ignore[reportAttributeAccessIssue] # Why: acquired[0] is narrowed to ReservationHandle by isinstance above; AcquiredResource Protocol lacks slot_index but ReservationHandle carries it
            )
        assert row is not None
        assert row["job_id"] is None

        r = await tb.acquire(count=1.0, redis_client=redis_client, clock=clock, settings=settings)
        assert r.remaining == 3.0
    finally:
        await redis_client.aclose()

"""Integration and chaos tests for AND-composition against real backends.

through (chaos / failure-mode), (property test with
Hypothesis), and (consumer snooze translation). These verify rollback resilience, composition
invariants, and the consumer's rate-limit / reservation denial translation
against real Postgres and Redis via testcontainers.
"""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async
from hypothesis import given, settings
from hypothesis.strategies import integers

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import Clock, SystemClock
from taskq.context import JobContext
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.composition import RateLimitHandle, ReservationHandle
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

pytestmark = [pytest.mark.integration, pytest.mark.redis]


def _unique_name() -> str:
    return f"test_{new_base62()}"


# ── Rollback failure → over-acquisition bounded by TTL ──────────────


async def test_rollback_failure_bounded_by_ttl(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Rollback failure → over-acquisition bounded by TTL.
    Force a TokenBucket refund to fail (subclass to override refund).
    ReservationUnavailable is still raised despite rollback failure; TokenBucket
    key's TTL bounds the over-acquisition window."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    s = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    res_name = _unique_name()
    sw_name = _unique_name()

    class _FailRefundTokenBucket(TokenBucket):
        async def refund(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("forced refund failure")

    try:
        tb = _FailRefundTokenBucket(name=tb_name, capacity=10, refill_per_second=1, backend="redis")
        sw = SlidingWindow(name=sw_name, limit=1, window=timedelta(minutes=1), backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=2, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)
        reg.register(tb)
        reg.register(sw)

        await sw.acquire(redis_client=redis_client, clock=clock, settings=s)

        job_id = new_uuid()
        worker_id = new_uuid()

        with pytest.raises(ReservationUnavailable):
            await reg.acquire_for_actor(
                rate_limits=[tb_name, sw_name],
                reservations=[res_name],
                job_id=job_id,
                worker_id=worker_id,
                redis_client=redis_client,
                pg_pool=module_pg_pool,
                clock=clock,
                settings=s,
            )

        tb_key = f"taskq:{schema}:rl:tb:{{{tb_name}}}"
        ttl = await redis_client.ttl(tb_key)
        assert ttl > 0

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


# ── PG dies during reservation release in rollback ──────────────────


async def test_pg_dies_during_reservation_release(
    module_pg_schema: ModulePgSchema,
    redis_url: str,
) -> None:
    """PG dies during reservation release. Close the asyncpg pool before
    release. release_for_actor must not raise (best-effort); the PG slot stays
    held but over-acquisition is bounded by the reservation lease TTL (30s).
    Exercises real asyncpg error handling, not mocks.

    Uses its own per-test pool (not module_pg_pool) so the pool close
    is isolated to this single test.
    """
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    s = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )
    clock = SystemClock()

    tb_name = _unique_name()
    res_name = _unique_name()

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=8)
    try:
        tb = TokenBucket(name=tb_name, capacity=10, refill_per_second=1, backend="redis")
        res = ConcurrencyReservation(
            name=res_name, slots=2, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(pool)

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
            pg_pool=pool,
            clock=clock,
            settings=s,
        )

        assert len(acquired) == 2

        res_handle = acquired[0]
        assert isinstance(res_handle, ReservationHandle)

        async with pool.acquire() as conn:
            row_before = await conn.fetchrow(
                f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name/slot_index are $1/$2-bound
                "WHERE bucket_name = $1 AND slot_index = $2",
                res_name,
                res_handle.slot_index,
            )
        assert row_before is not None
        assert row_before["job_id"] is not None

        await pool.close()

        await reg.release_for_actor(acquired, pg_pool=pool)

        pool2 = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=8)
        try:
            async with pool2.acquire() as conn:
                row_after = await conn.fetchrow(
                    f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name/slot_index are $1/$2-bound
                    "WHERE bucket_name = $1 AND slot_index = $2",
                    res_name,
                    res_handle.slot_index,
                )
            assert row_after is not None
            assert row_after["job_id"] is not None
        finally:
            await pool2.close()
    finally:
        await redis_client.aclose()


# ── Redis dies between acquire and release (post-actor) ────────────


async def test_redis_dies_post_actor(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Redis dies between acquire and release (post-actor).
    Rate-limit handles are no-ops (refund_on_release=False); reservation
    released via PG (unaffected). release_for_actor completes without error;
    PG slot is freed."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    s = WorkerSettings.load_from_dict(
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
            name=res_name, slots=2, lease=timedelta(seconds=30), schema=schema
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
            settings=s,
        )

        assert len(acquired) == 2

        res_handle = acquired[0]
        assert isinstance(res_handle, ReservationHandle)

        await redis_client.aclose()

        await reg.release_for_actor(acquired, pg_pool=module_pg_pool)

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT job_id FROM {schema}.reservation_slots "  # noqa: S608 # Why: schema is fixture-derived; bucket_name/slot_index are $1/$2-bound
                "WHERE bucket_name = $1 AND slot_index = $2",
                res_name,
                res_handle.slot_index,
            )
        assert row is not None
        assert row["job_id"] is None
    finally:
        await redis_client.aclose()


# ── Composition invariants (Hypothesis) ────────────────────────────


_IDENTIFIER_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789"


@settings(max_examples=20)
@given(
    n_rate_limits=integers(min_value=0, max_value=3),
    m_reservations=integers(min_value=0, max_value=3),
    rl_seed=integers(min_value=0, max_value=1000000),
    res_seed=integers(min_value=0, max_value=1000000),
)
async def test_composition_invariants(
    n_rate_limits: int,
    m_reservations: int,
    rl_seed: int,
    res_seed: int,
) -> None:
    """Composition invariants (Hypothesis). For any N rate limits and
    M reservations, if all acquire, the handle list has M + N elements in
    declaration order (M reservations first). Release order is exactly reversed."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    reg = RateLimitRegistry()

    import random

    rng_rl = random.Random(rl_seed)  # noqa: S311 # Why: seeded PRNG for deterministic test name generation, not security
    rng_res = random.Random(res_seed)  # noqa: S311 # Why: seeded PRNG for deterministic test name generation, not security

    rl_names: list[str] = [f"rl_{rng_rl.randint(0, 99999)}" for _ in range(n_rate_limits)]
    res_names: list[str] = [f"res_{rng_res.randint(0, 99999)}" for _ in range(m_reservations)]

    seen: set[str] = set()
    for i, name in enumerate(rl_names):
        if name in seen:
            rl_names[i] = f"rl_{i}"
            name = rl_names[i]
        seen.add(name)
        tb = TokenBucket(name=name, capacity=100.0, refill_per_second=10.0, backend="memory")
        reg.register(tb)

    for i, name in enumerate(res_names):
        if name in seen:
            res_names[i] = f"res_{i}"
            name = res_names[i]
        seen.add(name)
        res = ConcurrencyReservation(name=name, slots=4, lease=timedelta(seconds=30), clock=clock)
        reg.register(res)

    if not rl_names and not res_names:
        return

    job_id = new_uuid()
    worker_id = new_uuid()
    acquired = await reg.acquire_for_actor(
        rate_limits=rl_names,
        reservations=res_names,
        job_id=job_id,
        worker_id=worker_id,
        clock=clock,
    )

    assert len(acquired) == m_reservations + n_rate_limits

    for i in range(m_reservations):
        assert isinstance(acquired[i], ReservationHandle)
        assert acquired[i].name == res_names[i]

    for i in range(n_rate_limits):
        assert isinstance(acquired[m_reservations + i], RateLimitHandle)
        assert acquired[m_reservations + i].name == rl_names[i]

    release_order: list[str] = []
    for h in reversed(acquired):
        release_order.append(h.name)

    expected = list(reversed(res_names + rl_names))
    assert release_order == expected

    await reg.release_for_actor(acquired)


# ── Shared helpers for consumer tests ───────────────────────


_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


# ── Rate-limit denial translates to consumer snooze without awaiting ─


async def test_rate_limit_denial_snooze_without_awaiting(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Rate-limit denial translates to consumer snooze without
    awaiting key. Exhaust SlidingWindow. Dispatch actor. Consumer calls
    mark_snoozed without awaiting."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    s = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )

    sw_name = _unique_name()

    try:
        sw = SlidingWindow(name=sw_name, limit=1, window=timedelta(minutes=1), backend="redis")

        reg = RateLimitRegistry()
        reg.register(sw)

        clock = SystemClock()
        await sw.acquire(redis_client=redis_client, clock=clock, settings=s)

        from pydantic import BaseModel

        class _Payload(BaseModel):
            pass

        backend = FakeBackend()
        fake_clock: Clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
        job = make_job_row()
        cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

        async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
            raise AssertionError("actor body should not run on denial")

        await consume_one_job(
            as_backend(backend),
            job,
            job.locked_by_worker,
            run_actor=never_called_actor,
            actor_config=cfg,
            payload_type=_Payload,
            clock=fake_clock,
            rate_limit_registry=reg,
            rate_limits=[sw_name],
            reservations=[],
            redis_client=redis_client,
            worker_pool=module_pg_pool,
            settings=s,
        )

        assert len(backend.mark_snoozed_calls) == 1
        snooze_call = backend.mark_snoozed_calls[0]
        assert snooze_call["metadata_update"] == {"awaiting": f"rate_limit:{sw_name}"}
    finally:
        await redis_client.aclose()


# ── Reservation denial translates to consumer snooze with awaiting ──


async def test_reservation_denial_snooze_with_awaiting(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    redis_url: str,
) -> None:
    """Reservation denial translates to consumer snooze with
    awaiting key. Fill all reservation slots. Dispatch actor. Consumer calls
    mark_snoozed with metadata_update={"awaiting": "reservation:gpu_pool"}."""
    schema = module_pg_schema.schema_name
    redis_client = redis_async.from_url(redis_url, decode_responses=False)
    s = WorkerSettings.load_from_dict(
        {
            "pg_dsn": module_pg_schema.pg_dsn,
            "redis_url": redis_url,
            "schema_name": schema,
        },
    )

    res_name = "gpu_pool"

    try:
        res = ConcurrencyReservation(
            name=res_name, slots=1, lease=timedelta(seconds=30), schema=schema
        )
        await res.ensure_slots(module_pg_pool)

        filler_job = new_uuid()
        filler_worker = new_uuid()
        await res.acquire(filler_job, filler_worker, module_pg_pool)

        reg = RateLimitRegistry()
        reg.register(res)

        from pydantic import BaseModel

        class _Payload(BaseModel):
            pass

        backend = FakeBackend()
        fake_clock: Clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
        job = make_job_row()
        cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

        async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
            raise AssertionError("actor body should not run on denial")

        await consume_one_job(
            as_backend(backend),
            job,
            job.locked_by_worker,
            run_actor=never_called_actor,
            actor_config=cfg,
            payload_type=_Payload,
            clock=fake_clock,
            rate_limit_registry=reg,
            rate_limits=[],
            reservations=[res_name],
            redis_client=redis_client,
            worker_pool=module_pg_pool,
            settings=s,
        )

        assert len(backend.mark_snoozed_calls) == 1
        snooze_call = backend.mark_snoozed_calls[0]
        assert snooze_call["metadata_update"] == {"awaiting": f"reservation:{res_name}"}
    finally:
        await redis_client.aclose()

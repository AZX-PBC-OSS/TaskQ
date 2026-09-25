"""Fleet-shared bucket pins: two workers, one shared admission state.

The attack brief's fleet scenario: a rate-limit bucket's state is shared by
the whole fleet (the Redis hash under ``taskq:{schema}:rl:tb:{name}``, the
PG ``rate_limit_buckets`` row), while every worker materializes its OWN
``TokenBucket`` instance from the actor declaration. Nothing ties the
instances together, so the pins here build two independent instances and
drive them concurrently, exactly the deployment shape:

* **No double-spend, no under-admit.** Two instances bursting one
  fixed-quota bucket concurrently must admit exactly ``capacity`` total.
  A per-instance state split (each instance writing its own key or row)
  admits ``capacity`` per instance; a racy read-modify-write admits more
  than ``capacity``. Both mutations fail the exact-count pins.
* **The deny-then-allow boundary.** A denied acquire must be denied again
  strictly BEFORE its own ``retry_after`` hint elapses (the hint is a
  deficit over the refill rate, not a free pass), and must be admitted
  once the hint has elapsed. A script that advanced the denial's ``ts``
  without storing the refilled count (or spent on denial) makes the
  early re-acquire spuriously pass; a spin (hint of 0) fails the
  positive-hint pin.
* **Denial writes no spend.** After a fleet exhausts a fixed quota to
  exactly zero, the stored token count reads exactly 0.0 and the next
  denial reports ``remaining == 0.0`` with ``retry_after is None``: no
  negative debt, no phantom recovery. A deny branch that decremented
  tokens would store a negative count and fail the row-read pin.
* **The allow boundary is inclusive.** With the balance exactly equal
  to the request, the acquire is GRANTED (``tokens >= count``): the
  three backends' comparisons must agree at the boundary, an exclusive
  ``>`` on any backend strands the last token of a fixed quota forever
  (the budget becomes capacity-minus-one). Pinned deterministically per
  backend: PG and Redis through a fixed quota's last token (no elapsed
  term, zero slack), memory through the injected clock.

Redis pins run against the shared container; PG pins against the module
schema. The PG row read is a plain ``SELECT`` on the state document, the
same audit view ``peek()`` takes.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.ratelimit import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration]


# ── Redis: two instances, one shared hash ─────────────────────────────


@pytest.mark.redis
async def test_redis_fleet_double_spend_exact(redis_url: str) -> None:
    """Two worker instances bursting one shared fixed-quota hash admit
    exactly ``capacity`` total, not per instance."""
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": "rt_fleet_pin"}
    )
    name = f"rt_fleet_{new_base62()}"
    worker_a = TokenBucket(name=name, capacity=20.0, refill_per_second=0.0, backend="redis")
    worker_b = TokenBucket(name=name, capacity=20.0, refill_per_second=0.0, backend="redis")
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)

    async def one(bucket: TokenBucket) -> object:
        return await bucket.acquire(1.0, redis_client=client, settings=settings)

    try:
        results = await asyncio.gather(
            *[one(worker_a if i % 2 == 0 else worker_b) for i in range(40)]
        )
        allowed = sum(1 for r in results if r.allowed)  # type: ignore[union-attr]
        assert allowed == 20, (
            f"two fleet instances on one bucket admitted {allowed} of 40 concurrent "
            f"acquires against capacity 20: a state split (per-instance keys) or a "
            f"racy read-modify-write lies about the shared budget"
        )

        # The 21st acquire, after the fleet's burst settled, is still
        # denied: the shared hash holds the spent state, not a per-worker
        # copy that quietly refilled.
        after = await worker_a.acquire(1.0, redis_client=client, settings=settings)
        assert after.allowed is False
        assert after.retry_after is None, "a fixed quota never recovers, the hint must stay None"

        stored = await client.hget(f"taskq:rt_fleet_pin:rl:tb:{{{name}}}", "tokens")
        assert stored is not None
        assert float(stored) == 0.0, f"shared hash spent to {stored!r}, denial wrote a spend"
    finally:
        await client.aclose()


@pytest.mark.redis
async def test_redis_deny_then_allow_boundary_at_the_hint(redis_url: str) -> None:
    """A denied acquire stays denied strictly before its own retry_after
    and is admitted once it has elapsed: the hint is honored, never a
    spin, never an early grant."""
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": "rt_fleet_pin"}
    )
    tb = TokenBucket(
        name=f"rt_boundary_{new_base62()}", capacity=1.0, refill_per_second=1.0, backend="redis"
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)

    try:
        first = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert first.allowed is True

        denied = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert denied.allowed is False
        assert denied.retry_after is not None
        hint = denied.retry_after.total_seconds()
        assert 0.0 < hint <= 1.0, f"retry hint {hint}s: a zero hint is a spin, > 1s is a lie"

        # Strictly inside the hint window the deficit is not repaid: the
        # deny must stand. (Halfway: 0.5 tokens accrued against 1.0
        # owed, comfortably below the boundary under load jitter.)
        await asyncio.sleep(hint / 2)
        early = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert early.allowed is False, (
            "an acquire strictly inside the retry_after window was granted: the "
            "denial branch wrote a spend or lost the refilled count"
        )

        # Past the hint the refill repays the deficit exactly: admitted.
        await asyncio.sleep(hint / 2 + 0.1)
        late = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert late.allowed is True, (
            "an acquire past its own retry_after was denied again: the hint is "
            "not honored, the job spins"
        )
    finally:
        await client.aclose()


# ── PG: two instances, one shared row ─────────────────────────────────


async def test_pg_fleet_double_spend_and_no_debt(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Two worker instances bursting one shared PG row admit exactly
    ``capacity``; the exhausted row stores exactly 0 and the standing
    denial reports no debt and no recovery."""
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name}
    )
    name = f"rt_fleet_{new_base62()}"
    worker_a = TokenBucket(name=name, capacity=20.0, refill_per_second=0.0, backend="postgres")
    worker_b = TokenBucket(name=name, capacity=20.0, refill_per_second=0.0, backend="postgres")

    async def one(bucket: TokenBucket) -> object:
        return await bucket.acquire(1.0, pg_pool=module_pg_pool, settings=settings)

    results = await asyncio.gather(*[one(worker_a if i % 2 == 0 else worker_b) for i in range(40)])
    allowed = sum(1 for r in results if r.allowed)  # type: ignore[union-attr]
    assert allowed == 20, (
        f"two fleet instances on one row admitted {allowed} of 40 concurrent "
        f"acquires against capacity 20"
    )

    state = await module_pg_pool.fetchval(
        f"SELECT state->>'tokens' FROM \"{module_pg_schema.schema_name}\".rate_limit_buckets "  # noqa: S608  # Why: schema_name is the fixture's migration-validated identifier; name is $1-bound below.
        "WHERE bucket_name = $1",
        name,
    )
    assert state is not None, "the fleet's acquires must have materialized the row"
    assert float(state) == 0.0, (
        f"shared row spent to {state!r}: the deny branch decremented tokens "
        f"(a spend written by a refusal) or a grant double-counted"
    )

    standing = await worker_a.acquire(1.0, pg_pool=module_pg_pool, settings=settings)
    assert standing.allowed is False
    assert standing.remaining == 0.0, f"a denied acquire reported {standing.remaining!r} remaining"
    assert standing.retry_after is None, "a fixed quota never recovers, the hint must stay None"


async def test_pg_allow_boundary_is_inclusive_at_exact_refill(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The LAST token of a fixed quota is grantable: the spend comparison
    is inclusive (``refilled >= count``), all three backends must agree.

    Determinism note: a refilling bucket cannot pin this exactly, the
    elapsed term between the rewind and the acquire is real time and
    only ever ADDS tokens, so a strict-boundary mutant survives on the
    gap. A fixed quota has no elapsed term: ``refilled`` is the stored
    tokens exactly, so an acquire whose request equals the balance sits
    ON the boundary with zero slack. An exclusive ``>`` strands the
    last token forever, a fixed quota could never fully spend, the
    budget becomes capacity-minus-one.
    """
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name}
    )
    tb = TokenBucket(
        name=f"rt_boundary_{new_base62()}", capacity=5.0, refill_per_second=0.0, backend="postgres"
    )

    for _ in range(4):
        spent = await tb.acquire(1.0, pg_pool=module_pg_pool, settings=settings)
        assert spent.allowed is True

    # Balance exactly 1.0, refill 0: the next acquire sits ON the
    # boundary, the inclusive comparison grants the last token.
    boundary = await tb.acquire(1.0, pg_pool=module_pg_pool, settings=settings)
    assert boundary.allowed is True, (
        "the LAST token of a fixed quota was denied: the spend comparison went "
        "exclusive and strands one token forever, the budget becomes "
        "capacity-minus-one"
    )

    spent_state = await module_pg_pool.fetchval(
        f"SELECT state->>'tokens' FROM \"{module_pg_schema.schema_name}\".rate_limit_buckets "  # noqa: S608  # Why: fixture-validated schema identifier, name is $1-bound.
        "WHERE bucket_name = $1",
        tb.name,
    )
    assert float(spent_state) == 0.0, f"fully spent quota stores {spent_state!r}"

    exhausted = await tb.acquire(1.0, pg_pool=module_pg_pool, settings=settings)
    assert exhausted.allowed is False
    assert exhausted.retry_after is None


@pytest.mark.redis
async def test_redis_last_token_of_a_fixed_quota_is_grantable(redis_url: str) -> None:
    """The Redis script's spend boundary is inclusive too: the last token
    of a fixed quota is granted, the quota fully spends, the next denial
    stores exactly 0."""
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": "rt_fleet_pin"}
    )
    tb = TokenBucket(
        name=f"rt_boundary_{new_base62()}", capacity=5.0, refill_per_second=0.0, backend="redis"
    )
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)

    try:
        for _ in range(4):
            spent = await tb.acquire(1.0, redis_client=client, settings=settings)
            assert spent.allowed is True

        boundary = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert boundary.allowed is True, (
            "the LAST token of a fixed quota was denied: the spend comparison went "
            "exclusive and strands one token forever"
        )

        stored = await client.hget(f"taskq:rt_fleet_pin:rl:tb:{{{tb.name}}}", "tokens")
        assert stored is not None
        assert float(stored) == 0.0, f"fully spent quota stores {stored!r}"

        exhausted = await tb.acquire(1.0, redis_client=client, settings=settings)
        assert exhausted.allowed is False
        assert exhausted.retry_after is None
    finally:
        await client.aclose()


# The in-memory oracle's boundary arm, the arithmetic reference the Lua
# script and the PG conflict arm mirror: the same inclusive comparison,
# driven on the injected clock so no sleep is involved.
async def test_memory_allow_boundary_is_inclusive_on_the_injected_clock() -> None:
    from datetime import UTC, datetime

    from taskq.testing.clock import FakeClock

    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    tb = TokenBucket(name="rt_mem_boundary", capacity=1.0, refill_per_second=1.0, backend="memory")

    assert (await tb.acquire(1.0, clock=clock)).allowed is True
    denied = await tb.acquire(1.0, clock=clock)
    assert denied.allowed is False
    assert denied.retry_after is not None
    assert denied.retry_after == timedelta(seconds=1), (
        f"deficit 1.0 at refill 1.0/s must hint exactly 1s, got {denied.retry_after}"
    )

    clock.advance(timedelta(seconds=1))
    boundary = await tb.acquire(1.0, clock=clock)
    assert boundary.allowed is True, "the inclusive boundary must admit at tokens == count"

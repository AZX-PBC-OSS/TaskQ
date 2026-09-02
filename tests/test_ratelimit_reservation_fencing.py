"""Regression: a zombie attempt must not release a live attempt's slot.

Failure mode.  Worker W runs a reservation-bound job.  Its heartbeats stall
(``heartbeat_pool`` is separate from ``worker_pool``, so the dispatch loop and
the actor keep running), the job's lock and the slot lease expire together,
sweep 1 flips the job row back to ``pending`` IN PLACE and sweep 4 frees the
slot.  The job is redispatched — plausibly to the same worker W — and acquires
a slot again.  The original coroutine, never cancelled, eventually finishes
and its ``finally`` calls ``release(slot_index, worker_id)``, which matched on
``(bucket_name, slot_index, held_by_worker_id)`` alone and freed a slot the
LIVE attempt legitimately holds.  A third job then acquires it and the bucket
runs over ``max_concurrent``.

Note what does NOT discriminate here: a retry reuses the SAME job row, so the
zombie and the live attempt share a ``job_id`` and a ``worker_id``.  Fencing
on ``job_id`` would let every assertion below pass while the bug survives —
the fence has to identify the LEASE, not the job.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_SHORT_LEASE = timedelta(milliseconds=300)
_NOW = datetime(2025, 1, 1, tzinfo=UTC)


def _unique_name() -> str:
    return f"fence_{new_base62()}"


async def test_zombie_release_cannot_free_a_live_slot_pg(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """PG: the stale holder's release is a no-op; the bucket stays at capacity."""
    bucket = _unique_name()
    res = ConcurrencyReservation(
        name=bucket,
        slots=1,
        lease=_SHORT_LEASE,
        schema=module_pg_schema.schema_name,
    )
    await res.ensure_slots(module_pg_pool)

    worker_id = new_uuid()
    job_id = new_uuid()

    # Attempt 1 acquires the only slot, then its heartbeats stall.
    zombie_lease = await res.acquire(job_id, worker_id, module_pg_pool)

    # Lock + slot lease expire; sweep 4 frees the slot and the SAME job row is
    # redispatched to the SAME worker, which acquires the slot again.
    await asyncio.sleep(_SHORT_LEASE.total_seconds() * 2)
    live_lease = await res.acquire(job_id, worker_id, module_pg_pool)
    assert int(live_lease) == int(zombie_lease)

    # The zombie coroutine finally unwinds and releases what it thinks it holds.
    await res.release(zombie_lease, worker_id, module_pg_pool)

    peek = await res.peek(module_pg_pool)
    assert peek["held_count"] == 1, "zombie release freed the live attempt's slot"
    with pytest.raises(ReservationUnavailable):
        await res.acquire(new_uuid(), new_uuid(), module_pg_pool)

    # The live attempt's own release still works.
    await res.release(live_lease, worker_id, module_pg_pool)
    await res.acquire(new_uuid(), new_uuid(), module_pg_pool)


async def test_zombie_release_cannot_free_a_live_slot_in_memory() -> None:
    """In-memory: identical behaviour, driven by a FakeClock."""
    clock = FakeClock(_NOW)
    res = ConcurrencyReservation(
        name="gpu",
        slots=1,
        lease=timedelta(seconds=10),
        clock=clock,
    )

    worker_id = new_uuid()
    job_id = new_uuid()

    zombie_lease = await res.acquire(job_id, worker_id, pool=None)

    clock.advance(timedelta(seconds=30))  # lock + slot lease expire
    live_lease = await res.acquire(job_id, worker_id, pool=None)
    assert int(live_lease) == int(zombie_lease)

    await res.release(zombie_lease, worker_id, pool=None)

    peek = await res.peek(pool=None)
    assert peek["held_count"] == 1, "zombie release freed the live attempt's slot"
    with pytest.raises(ReservationUnavailable):
        await res.acquire(new_uuid(), new_uuid(), pool=None)

    await res.release(live_lease, worker_id, pool=None)
    await res.acquire(new_uuid(), new_uuid(), pool=None)


async def test_double_release_of_one_lease_is_still_a_no_op_pg(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Releasing twice must not free the next holder's slot either."""
    bucket = _unique_name()
    res = ConcurrencyReservation(
        name=bucket,
        slots=1,
        lease=timedelta(seconds=30),
        schema=module_pg_schema.schema_name,
    )
    await res.ensure_slots(module_pg_pool)

    worker_id = new_uuid()
    lease = await res.acquire(new_uuid(), worker_id, module_pg_pool)
    await res.release(lease, worker_id, module_pg_pool)

    # Same worker, new job: the worker_id gate alone cannot tell the two
    # leases apart — only the lease fence can.
    await res.acquire(new_uuid(), worker_id, module_pg_pool)

    await res.release(lease, worker_id, module_pg_pool)  # late duplicate

    peek = await res.peek(module_pg_pool)
    assert peek["held_count"] == 1


async def test_fence_survives_the_registry_handle_round_trip(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The production path — acquire_for_actor → handle → release_for_actor.

    The fence is only worth anything if it survives the hop through
    ``ReservationHandle``, which is how every real release reaches the
    reservation.  Same zombie scenario, driven entirely through the registry.
    """
    from taskq.ratelimit.registry import RateLimitRegistry

    bucket = _unique_name()
    res = ConcurrencyReservation(
        name=bucket,
        slots=1,
        lease=_SHORT_LEASE,
        schema=module_pg_schema.schema_name,
    )
    await res.ensure_slots(module_pg_pool)

    registry = RateLimitRegistry()
    registry.register(res)

    worker_id = new_uuid()
    job_id = new_uuid()

    zombie = await registry.acquire_for_actor(
        rate_limits=[],
        reservations=[bucket],
        job_id=job_id,
        worker_id=worker_id,
        pg_pool=module_pg_pool,
    )

    await asyncio.sleep(_SHORT_LEASE.total_seconds() * 2)
    live = await registry.acquire_for_actor(
        rate_limits=[],
        reservations=[bucket],
        job_id=job_id,
        worker_id=worker_id,
        pg_pool=module_pg_pool,
    )

    await registry.release_for_actor(zombie, pg_pool=module_pg_pool)

    peek = await res.peek(module_pg_pool)
    assert peek["held_count"] == 1, "zombie handle released the live attempt's slot"

    await registry.release_for_actor(live, pg_pool=module_pg_pool)
    peek_after = await res.peek(module_pg_pool)
    assert peek_after["held_count"] == 0

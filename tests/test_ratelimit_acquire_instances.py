"""acquire_for_actor accepts primitive instances alongside names and keyed refs.

Instance entries are normalized to their registered ``.name`` up front; an
unregistered instance raises the same KeyError an unknown name raises.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _tb(name: str, capacity: float = 10.0) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=1.0, backend="memory")


def _res(name: str) -> ConcurrencyReservation:
    return ConcurrencyReservation(
        name=name, slots=2, lease=timedelta(seconds=30), clock=FakeClock(_START)
    )


async def test_unregistered_rate_limit_instance_raises_keyerror() -> None:
    reg = RateLimitRegistry()

    with pytest.raises(KeyError):
        await reg.acquire_for_actor(
            rate_limits=[_tb("ghost")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
        )


async def test_unregistered_reservation_instance_raises_keyerror() -> None:
    reg = RateLimitRegistry()

    with pytest.raises(KeyError):
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res("ghost")],
            job_id=new_uuid(),
            worker_id=new_uuid(),
        )


async def test_mixed_instance_and_name_declaration_both_resolve() -> None:
    """One actor mixing a TokenBucket instance and a name string: both resolve."""
    reg = RateLimitRegistry()
    inst = _tb("a")
    reg.register(inst)
    reg.register(_tb("b"))
    res = _res("r1")
    reg.register(res)

    acquired = await reg.acquire_for_actor(
        rate_limits=[inst, "b"],
        reservations=[res],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        clock=FakeClock(_START),
    )

    assert [h.name for h in acquired] == ["r1", "a", "b"]

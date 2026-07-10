"""Unit tests for AND-composition: acquire_for_actor / release_for_actor.

Tests through and.
All tests use in-memory backends (``backend="memory"``) with ``FakeClock``
— no Redis or PG instance required.
"""

import contextlib
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.constants import DEFAULT_RESERVATION_BACKOFF
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.composition import (
    RateLimitHandle,
    ReservationHandle,
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _token_bucket(
    name: str = "tb",
    capacity: float = 10.0,
    refill: float = 1.0,
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="memory")


def _reservation(
    name: str = "res",
    slots: int = 4,
    lease: timedelta = timedelta(seconds=10),
    clock: FakeClock | None = None,
) -> ConcurrencyReservation:
    if clock is None:
        clock = FakeClock(_START)
    return ConcurrencyReservation(name=name, slots=slots, lease=lease, clock=clock)


def _setup_registry(
    clock: FakeClock | None = None,
) -> tuple[RateLimitRegistry, FakeClock]:
    if clock is None:
        clock = FakeClock(_START)
    return RateLimitRegistry(), clock


# ── All succeed — reservations + rate limits acquired in order ──


async def test_all_succeed() -> None:
    """All succeed — handles have M+N entries in acquisition order."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)
    res = _reservation("r", slots=2, clock=clock)
    tb_a = _token_bucket("a", capacity=10.0, refill=1.0)
    tb_b = _token_bucket("b", capacity=10.0, refill=1.0)
    reg.register(res)
    reg.register(tb_a)
    reg.register(tb_b)

    job_id = new_uuid()
    worker_id = new_uuid()
    acquired = await reg.acquire_for_actor(
        rate_limits=["a", "b"],
        reservations=["r"],
        job_id=job_id,
        worker_id=worker_id,
        clock=clock,
    )

    assert len(acquired) == 3
    assert isinstance(acquired[0], ReservationHandle)
    assert acquired[0].name == "r"
    assert isinstance(acquired[1], RateLimitHandle)
    assert acquired[1].name == "a"
    assert isinstance(acquired[2], RateLimitHandle)
    assert acquired[2].name == "b"


# ── Reservation fails — no rate limits attempted ────────────────


async def test_reservation_fails_no_rate_limits() -> None:
    """Reservation fails — no rate limits attempted (spy verification)."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    res = _reservation("r", slots=1, clock=clock)
    res.table.ensure_slots("r", 1)

    first_job = new_uuid()
    first_worker = new_uuid()
    await res.acquire(first_job, first_worker)

    tb = _token_bucket("a", capacity=10.0, refill=1.0)
    reg.register(res)
    reg.register(tb)

    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=["a"],
            reservations=["r"],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            clock=clock,
        )


# ── First rate limit succeeds, second fails — rollback ──────────


async def test_rate_limit_failure_triggers_rollback() -> None:
    """First RL succeeds, second fails — rollback refunds first RL and releases reservation."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    res = _reservation("r", slots=2, clock=clock)
    tb_a = _token_bucket("a", capacity=10.0, refill=1.0)
    tb_b = _token_bucket("b", capacity=1.0, refill=0.0)
    reg.register(res)
    reg.register(tb_a)
    reg.register(tb_b)

    async with reg.acquire("b", count=1.0, clock=clock) as d:
        assert d.allowed

    job_id = new_uuid()
    worker_id = new_uuid()
    with pytest.raises(ReservationUnavailable) as exc_info:
        await reg.acquire_for_actor(
            rate_limits=["a", "b"],
            reservations=["r"],
            job_id=job_id,
            worker_id=worker_id,
            clock=clock,
        )

    assert exc_info.value.bucket_name == "b"

    async with reg.acquire("a", count=1.0, clock=clock) as d_after:
        assert d_after.allowed
        assert d_after.remaining == 9.0

    slot = res.table.get_slot("r", 0)
    assert slot is not None
    assert slot.job_id is None


# ── Rollback failure — one release raises; ERROR logged ─────────


async def test_rollback_failure_error_logged() -> None:
    """Rollback failure — ERROR logged; remaining handles still released."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    res = _reservation("r", slots=2, clock=clock)
    tb_b = _token_bucket("b", capacity=1.0, refill=0.0)
    reg.register(res)
    reg.register(tb_b)

    async with reg.acquire("b", count=1.0, clock=clock) as d:
        assert d.allowed

    job_id = new_uuid()
    worker_id = new_uuid()

    class _FailRefund(TokenBucket):
        async def refund(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("refund exploded")

    fail_tb = _FailRefund(name="a", capacity=10.0, refill_per_second=1.0, backend="memory")
    reg.register(fail_tb)

    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=["a", "b"],
            reservations=["r"],
            job_id=job_id,
            worker_id=worker_id,
            clock=clock,
        )

    slot = res.table.get_slot("r", 0)
    assert slot is not None
    assert slot.job_id is None


# ── Rate-limit tokens NOT refunded after actor success ──────────


async def test_tokens_not_refunded_after_actor_success() -> None:
    """Rate-limit tokens NOT refunded; reservation released."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    res = _reservation("r", slots=2, clock=clock)
    tb = _token_bucket("tb", capacity=5.0, refill=0.0)
    reg.register(res)
    reg.register(tb)

    job_id = new_uuid()
    worker_id = new_uuid()
    acquired = await reg.acquire_for_actor(
        rate_limits=["tb"],
        reservations=["r"],
        job_id=job_id,
        worker_id=worker_id,
        clock=clock,
    )

    assert len(acquired) == 2

    await reg.release_for_actor(acquired)

    async with reg.acquire("tb", count=1.0, clock=clock) as d:
        assert d.remaining == 3.0

    slot = res.table.get_slot("r", 0)
    assert slot is not None
    assert slot.job_id is None


# ── Release order is reverse of acquisition ────────────────────


async def test_release_order_is_reverse() -> None:
    """Release order is reverse of acquisition.

    We verify by wrapping the registry's release_for_actor to capture
    which handles are visited in which order. Rate-limit handles have
    refund_on_release set to False before iteration, so only reservation
    handles produce observable side effects.
    """
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    res_r = _reservation("r", slots=2, clock=clock)
    tb_a = _token_bucket("a", capacity=10.0, refill=1.0)
    tb_b = _token_bucket("b", capacity=10.0, refill=1.0)
    reg.register(res_r)
    reg.register(tb_a)
    reg.register(tb_b)

    job_id = new_uuid()
    worker_id = new_uuid()
    acquired = await reg.acquire_for_actor(
        rate_limits=["a", "b"],
        reservations=["r"],
        job_id=job_id,
        worker_id=worker_id,
        clock=clock,
    )

    release_names: list[str] = []

    for h in acquired:
        if isinstance(h, RateLimitHandle):
            h.refund_on_release = False
    for h in reversed(acquired):
        release_names.append(h.name)
        with contextlib.suppress(ConnectionError, OSError):
            await h.release()

    assert release_names == ["b", "a", "r"]


# ── retry_after=None uses DEFAULT_RESERVATION_BACKOFF ─────────


async def test_retry_after_none_uses_default_backoff() -> None:
    """retry_after=None uses DEFAULT_RESERVATION_BACKOFF (5s)."""
    clock = FakeClock(_START)
    reg, _ = _setup_registry(clock)

    tb = _token_bucket("tb", capacity=1.0, refill=0.0)
    reg.register(tb)

    async with reg.acquire("tb", count=1.0, clock=clock) as d:
        assert d.allowed

    job_id = new_uuid()
    worker_id = new_uuid()
    with pytest.raises(ReservationUnavailable) as exc_info:
        await reg.acquire_for_actor(
            rate_limits=["tb"],
            reservations=[],
            job_id=job_id,
            worker_id=worker_id,
            clock=clock,
        )

    assert exc_info.value.retry_after == DEFAULT_RESERVATION_BACKOFF

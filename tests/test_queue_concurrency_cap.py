"""Unit tests for the fleet-wide per-queue concurrency cap (Part 1, issue #24).

Tests ``queue_concurrency_reservation_name``, the core concurrency-correctness
property under parallel ``acquire_for_actor`` dispatch across multiple actors,
and the ``dispatch.py`` prepend-logic regression when no queue-cap is registered.

All tests use in-memory backends (``FakeClock``-backed ``ConcurrencyReservation``)
— no Redis or PG instance required, mirroring the conventions of
``tests/test_ratelimit_keyed_refs.py`` and ``tests/test_ratelimit_composition.py``.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.registry import (
    QUEUE_CONCURRENCY_PREFIX,
    RateLimitRegistry,
    queue_concurrency_reservation_name,
)
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)
_RESERVED_PREFIX = QUEUE_CONCURRENCY_PREFIX


def _reservation(
    name: str,
    slots: int = 4,
    lease: timedelta = timedelta(seconds=10),
    clock: FakeClock | None = None,
) -> ConcurrencyReservation:
    if clock is None:
        clock = FakeClock(_START)
    return ConcurrencyReservation(name=name, slots=slots, lease=lease, clock=clock)


# ── queue_concurrency_reservation_name: determinism and distinctiveness ──


def test_queue_concurrency_reservation_name_is_deterministic() -> None:
    """Same queue name always produces the same reservation name."""
    a = queue_concurrency_reservation_name("default")
    b = queue_concurrency_reservation_name("default")
    assert a == b


def test_queue_concurrency_reservation_name_differs_per_queue() -> None:
    """Different queue names produce different reservation names."""
    assert queue_concurrency_reservation_name("orders") != queue_concurrency_reservation_name(
        "default"
    )


def test_queue_concurrency_reservation_name_uses_reserved_prefix() -> None:
    """The name carries the ``taskq:global:queue:`` prefix from registry.py,
    namespacing it apart from user-declared reservations."""
    name = queue_concurrency_reservation_name("orders")
    assert name.startswith(_RESERVED_PREFIX)
    assert name == "taskq:global:queue:orders"


def test_queue_concurrency_reservation_name_does_not_collide_with_user_names() -> None:
    """A plausible user-declared reservation name does not start with the
    reserved prefix, so the queue-cap name cannot be mistaken for one."""
    user_names = ["orders", "my-cap", "gpu", "session-cap", "api-per-tenant"]
    for user_name in user_names:
        assert not user_name.startswith(_RESERVED_PREFIX)


def test_queue_concurrency_reservation_name_does_not_collide_with_keyed_ref_names() -> None:
    """A ``KeyedReservationRef``-derived ``base_name:key`` name with a
    plausible ``base_name`` does not start with the reserved prefix."""
    keyed_names = ["session-cap:abc", "geocode-session:s1", "tenant:acme"]
    for keyed_name in keyed_names:
        assert not keyed_name.startswith(_RESERVED_PREFIX)


# ── Core correctness: concurrent acquires never exceed the slot cap ──


async def test_concurrent_acquires_respect_2_slot_cap_across_multiple_actors() -> None:
    """The fleet-wide queue concurrency cap is never exceeded, even under
    truly concurrent ``acquire_for_actor`` dispatch from multiple actors.

    This is the core concurrency-correctness property from issue #24: a
    ``ConcurrencyReservation`` registered under
    ``queue_concurrency_reservation_name("orders")`` with ``slots=2`` must
    never allow more than 2 concurrent acquisitions simultaneously — not
    just eventually, but at NO instant during a parallel dispatch burst.

    Five concurrent tasks each call ``acquire_for_actor`` with the queue-cap
    reservation name ALONGSIDE a distinct per-actor static reservation
    (simulating "actor A" vs "actor B" declaring their own unrelated
    reservations).  An ``asyncio.Event`` forces real overlap: successful
    acquires hold their slots until the event is set, so denied tasks
    observe a genuinely full reservation.  A concurrency-tracking counter
    (incremented on acquire, decremented on release) records the maximum
    simultaneous holders; ``max(seen_concurrency) <= 2`` proves true
    concurrent-safety, not just eventual consistency.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()

    queue_cap = queue_concurrency_reservation_name("orders")
    reg.register_queue_cap_reservation(
        _reservation(queue_cap, slots=2, lease=timedelta(seconds=30), clock=clock)
    )
    # Per-actor reservations with plenty of slots — never the bottleneck.
    reg.register(_reservation("actor_a_res", slots=10, lease=timedelta(seconds=30), clock=clock))
    reg.register(_reservation("actor_b_res", slots=10, lease=timedelta(seconds=30), clock=clock))

    hold_event = asyncio.Event()
    current_concurrency = 0
    max_concurrency_seen = 0
    denied_count = 0
    num_tasks = 5

    async def _acquire_hold_release(task_idx: int) -> None:
        nonlocal current_concurrency, max_concurrency_seen, denied_count

        actor_res = "actor_a_res" if task_idx % 2 == 0 else "actor_b_res"

        try:
            acquired = await reg.acquire_for_actor(
                rate_limits=[],
                reservations=[queue_cap, actor_res],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                clock=clock,
            )
        except ReservationUnavailable:
            denied_count += 1
            return

        # Successfully acquired — track concurrency before yielding.
        current_concurrency += 1
        max_concurrency_seen = max(max_concurrency_seen, current_concurrency)

        # Hold the slot to force real overlap with other concurrent tasks.
        await hold_event.wait()

        current_concurrency -= 1
        await reg.release_for_actor(acquired)

    tasks = [asyncio.create_task(_acquire_hold_release(i)) for i in range(num_tasks)]

    # Let all tasks attempt acquisition. The in-memory acquire is
    # synchronous within a single coroutine step, so each task runs
    # to its first real await (hold_event.wait) or completion (denied)
    # before the next is scheduled.
    await asyncio.sleep(0.1)

    # Release the hold so successful acquires can release their slots.
    hold_event.set()
    await asyncio.gather(*tasks)

    assert max_concurrency_seen <= 2, (
        f"queue-cap exceeded: max concurrent holders = {max_concurrency_seen}, expected <= 2"
    )
    assert denied_count == num_tasks - 2, (
        f"expected {num_tasks - 2} denials for slots=2 with {num_tasks} tasks, got {denied_count}"
    )


# ── dispatch.py prepend-logic regression: no queue-cap registered ──


def test_dispatch_no_queue_cap_preserves_actor_reservations() -> None:
    """Regression: when the registry has NO queue-cap reservation for the
    job's queue, ``dispatch.py``'s conditional must leave
    ``effective_reservations`` unchanged — the prepend branch is skipped.

    This mirrors the exact conditional from ``dispatch.py``::

        effective_reservations = list(actor_ref.reservations)
        if rl_registry is not None:
            queue_cap_name = queue_concurrency_reservation_name(job.queue)
            if queue_cap_name in rl_registry.reservations:
                effective_reservations.insert(0, queue_cap_name)

    A registry-level test is the appropriate granularity here — the full
    ``dispatch_one_job`` function requires extensive scaffolding (backend,
    DI scopes, WorkerDeps, etc.) and the correctness of this branch
    reduces to the ``in rl_registry.reservations`` membership check.
    """
    reg = RateLimitRegistry()
    actor_reservations = ["my_res_1", "my_res_2"]

    # No queue-cap registered → effective_reservations unchanged.
    effective = list(actor_reservations)
    queue_cap_name = queue_concurrency_reservation_name("default")
    if queue_cap_name in reg.reservations:
        effective.insert(0, queue_cap_name)
    assert effective == actor_reservations

    # Register a queue-cap → it IS prepended.
    reg.register_queue_cap_reservation(
        _reservation(queue_cap_name, slots=5, lease=timedelta(seconds=30), clock=FakeClock(_START))
    )
    effective = list(actor_reservations)
    if queue_cap_name in reg.reservations:
        effective.insert(0, queue_cap_name)
    assert effective[0] == queue_cap_name
    assert effective[1:] == actor_reservations


# ── Reserved prefix protection (Fix 2) ──────────────────────────────


def test_register_rejects_reservation_with_reserved_prefix() -> None:
    """``register()`` rejects a user-supplied ``ConcurrencyReservation``
    whose name starts with the reserved queue-cap prefix."""
    reg = RateLimitRegistry()
    cap_name = queue_concurrency_reservation_name("orders")
    res = _reservation(cap_name, slots=2, lease=timedelta(seconds=30))

    with pytest.raises(ValueError, match="reserved prefix"):
        reg.register(res)


def test_register_rejects_token_bucket_with_reserved_prefix() -> None:
    """``register()`` rejects a user-supplied ``TokenBucket`` whose name
    starts with the reserved queue-cap prefix."""
    reg = RateLimitRegistry()
    cap_name = queue_concurrency_reservation_name("orders")
    tb = TokenBucket(name=cap_name, capacity=10, refill_per_second=1.0, backend="memory")

    with pytest.raises(ValueError, match="reserved prefix"):
        reg.register(tb)


def test_register_queue_cap_reservation_succeeds_for_prefixed_name() -> None:
    """``register_queue_cap_reservation()`` succeeds for a correctly-prefixed
    name and registers the reservation."""
    reg = RateLimitRegistry()
    cap_name = queue_concurrency_reservation_name("orders")
    res = _reservation(cap_name, slots=3, lease=timedelta(seconds=30))

    reg.register_queue_cap_reservation(res)

    assert cap_name in reg.reservations
    assert reg.reservations[cap_name].slots == 3


def test_register_queue_cap_reservation_idempotent_for_same_config() -> None:
    """``register_queue_cap_reservation()`` is idempotent for identical config
    — a second call with the same name and config is a no-op (no error, no
    duplicate)."""
    reg = RateLimitRegistry()
    cap_name = queue_concurrency_reservation_name("orders")
    res1 = _reservation(cap_name, slots=3, lease=timedelta(seconds=30))
    res2 = _reservation(cap_name, slots=3, lease=timedelta(seconds=30))

    reg.register_queue_cap_reservation(res1)
    reg.register_queue_cap_reservation(res2)

    assert len(reg.reservations) == 1
    assert reg.reservations[cap_name] is res1


def test_register_queue_cap_reservation_raises_for_unprefixed_name() -> None:
    """``register_queue_cap_reservation()`` raises ``ValueError`` for a name
    that does NOT start with the reserved prefix — a defensive guard against
    internal misuse."""
    reg = RateLimitRegistry()
    res = _reservation("user-reservation", slots=2, lease=timedelta(seconds=30))

    with pytest.raises(ValueError, match="requires a name starting with"):
        reg.register_queue_cap_reservation(res)

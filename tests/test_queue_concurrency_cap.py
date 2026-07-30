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
from pydantic import BaseModel

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


class _SessionPayload(BaseModel):
    session_id: str


class _TenantPayload(BaseModel):
    tenant_id: str


class _CompositionPayload(BaseModel):
    session_id: str
    tenant_id: str


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


# ── Cap interaction: queue cap AND keyed reservation AND keyed rate limit ──


async def test_queue_cap_composes_with_keyed_refs_and_rolls_back_across_kinds() -> None:
    """Per-queue cap + keyed reservation + keyed rate limit in one acquire:
    AND-composition (ALL must be acquired), acquired in declaration order
    (queue cap first, via the dispatch prepend), and a denial at any stage
    rolls back every earlier stage — across all three cap kinds.

    - (a) Full acquire of all three succeeds, in order.
    - (b) With the queue-cap slot held, a second acquire is denied AT THE
      QUEUE CAP (source="reservation", bucket is the queue-cap name) —
      the fleet-wide cap binds first.
    - (c) After actor completion (release_for_actor: reservation slots
      freed, token permanently consumed), a re-acquire is denied AT THE
      RATE LIMIT; the rollback releases the queue-cap slot and the keyed
      reservation slot it had just acquired — no cross-kind leak.
    """
    from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef

    clock = FakeClock(_START)
    reg = RateLimitRegistry()

    queue_cap = queue_concurrency_reservation_name("orders")
    reg.register_queue_cap_reservation(
        _reservation(queue_cap, slots=1, lease=timedelta(minutes=5), clock=clock)
    )
    # Pre-register the keyed concrete primitives with in-memory backends.
    reg.register(_reservation("session-cap:s1", slots=1, lease=timedelta(minutes=5), clock=clock))
    reg.register(
        TokenBucket(name="api-per-tenant:t1", capacity=1, refill_per_second=0, backend="memory")
    )

    res_ref = KeyedReservationRef.typed(
        _SessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=1,
        lease=timedelta(minutes=5),
    )
    rl_ref = KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=1,
        refill_per_second=0,
    )
    payload = _CompositionPayload(session_id="s1", tenant_id="t1")  # type: ignore[arg-type]  # Why: registry accepts dict[str, object] but passes payload through to key_fn; a BaseModel with both fields satisfies both key_fns at runtime.

    # ── (a) Full acquire: queue cap first (as dispatch prepends it). ──
    acquired = await reg.acquire_for_actor(
        rate_limits=[rl_ref],
        reservations=[queue_cap, res_ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=payload,
        clock=clock,
    )
    assert [h.name for h in acquired] == [queue_cap, "session-cap:s1", "api-per-tenant:t1"]

    # ── (b) Queue-cap binds first while its slot is held. ──
    with pytest.raises(ReservationUnavailable) as exc_info:
        await reg.acquire_for_actor(
            rate_limits=[rl_ref],
            reservations=[queue_cap, res_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=payload,
            clock=clock,
        )
    assert exc_info.value.source == "reservation"
    assert exc_info.value.bucket_name == queue_cap

    # ── (c) Actor completes: slots freed, token consumed permanently. ──
    await reg.release_for_actor(acquired)

    # Re-acquire: queue cap + keyed reservation acquire fine, rate limit
    # denies (its single token was consumed) → rollback frees both slots.
    with pytest.raises(ReservationUnavailable) as exc_info_2:
        await reg.acquire_for_actor(
            rate_limits=[rl_ref],
            reservations=[queue_cap, res_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=payload,
            clock=clock,
        )
    assert exc_info_2.value.source == "rate_limit"
    assert exc_info_2.value.bucket_name == "api-per-tenant:t1"

    # No cross-kind leak: both reservation slots are free again.
    assert reg.get_reservation(queue_cap).table.peek_slots(queue_cap) == (1, 0)
    assert reg.get_reservation("session-cap:s1").table.peek_slots("session-cap:s1") == (1, 0)


async def test_keyed_reservation_denial_rolls_back_queue_cap() -> None:
    """A denial at the keyed-reservation stage (after the queue-cap slot
    was acquired) releases the queue-cap slot — the fleet-wide cap is
    never leaked by a later stage's denial."""
    from taskq.ratelimit.refs import KeyedReservationRef

    clock = FakeClock(_START)
    reg = RateLimitRegistry()

    queue_cap = queue_concurrency_reservation_name("orders")
    queue_cap_res = _reservation(queue_cap, slots=1, lease=timedelta(minutes=5), clock=clock)
    reg.register_queue_cap_reservation(queue_cap_res)
    keyed_res = _reservation("session-cap:s1", slots=1, lease=timedelta(minutes=5), clock=clock)
    reg.register(keyed_res)

    # Another holder occupies the keyed reservation's only slot.
    await keyed_res.acquire(new_uuid(), new_uuid())

    res_ref = KeyedReservationRef.typed(
        _SessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=1,
        lease=timedelta(minutes=5),
    )

    with pytest.raises(ReservationUnavailable) as exc_info:
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[queue_cap, res_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
        )
    assert exc_info.value.bucket_name == "session-cap:s1"

    # The queue-cap slot acquired moments earlier was rolled back.
    assert queue_cap_res.table.peek_slots(queue_cap) == (1, 0)


# ── dispatch.py prepend logic (_effective_reservations) ──


def test_dispatch_no_queue_cap_preserves_actor_reservations() -> None:
    """When the registry has NO queue-cap reservation for the job's queue,
    ``_effective_reservations`` returns the actor's reservations unchanged
    — same object, no per-job copy on the hot path."""
    from taskq.worker.dispatch import _effective_reservations

    reg = RateLimitRegistry()
    actor_reservations = ["my_res_1", "my_res_2"]

    effective = _effective_reservations(actor_reservations, "default", reg)
    assert effective == actor_reservations
    assert effective is actor_reservations  # no copy when nothing to prepend


def test_dispatch_queue_cap_prepended_before_actor_reservations() -> None:
    """When a queue-cap IS registered, its name is prepended ahead of the
    actor-declared reservations (acquired first, released last)."""
    from taskq.worker.dispatch import _effective_reservations

    reg = RateLimitRegistry()
    actor_reservations = ["my_res_1", "my_res_2"]
    queue_cap_name = queue_concurrency_reservation_name("default")
    reg.register_queue_cap_reservation(
        _reservation(queue_cap_name, slots=5, lease=timedelta(seconds=30), clock=FakeClock(_START))
    )

    effective = _effective_reservations(actor_reservations, "default", reg)
    assert list(effective) == [queue_cap_name, "my_res_1", "my_res_2"]
    # The actor's own list is not mutated by the prepend.
    assert actor_reservations == ["my_res_1", "my_res_2"]


def test_dispatch_queue_cap_only_prepended_for_matching_queue() -> None:
    """A cap registered for queue A is not prepended for jobs on queue B."""
    from taskq.worker.dispatch import _effective_reservations

    reg = RateLimitRegistry()
    reg.register_queue_cap_reservation(
        _reservation(
            queue_concurrency_reservation_name("orders"),
            slots=5,
            lease=timedelta(seconds=30),
            clock=FakeClock(_START),
        )
    )
    actor_reservations = ["my_res_1"]

    effective = _effective_reservations(actor_reservations, "emails", reg)
    assert effective is actor_reservations


def test_dispatch_no_rl_registry_leaves_reservations_unchanged() -> None:
    """rl_registry=None (rate limiting not wired) → unchanged reservations."""
    from taskq.worker.dispatch import _effective_reservations

    actor_reservations = ["my_res_1"]
    effective = _effective_reservations(actor_reservations, "default", None)
    assert effective is actor_reservations


def test_effective_reservations_never_copies_registry_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: the dispatch hot path must NOT read the ``reservations``
    property, which defensively copies the whole dict — an O(n)-per-dispatch
    stall at high keyed-entry cardinality.

    Patches the property to raise on ANY access; if the queue-cap check is
    ever re-implemented in terms of ``rl_registry.reservations`` (or another
    copying accessor), this test fails.
    """
    from taskq.worker.dispatch import _effective_reservations

    def _boom(self: RateLimitRegistry) -> dict[str, ConcurrencyReservation]:
        raise AssertionError("dispatch hot path copied the reservations dict")

    monkeypatch.setattr(
        RateLimitRegistry,
        "reservations",
        property(_boom),  # type: ignore[arg-type]
    )

    reg = RateLimitRegistry()
    queue_cap_name = queue_concurrency_reservation_name("default")
    reg.register_queue_cap_reservation(
        _reservation(queue_cap_name, slots=5, lease=timedelta(seconds=30), clock=FakeClock(_START))
    )

    # Both branches (cap registered / not registered) must work without
    # touching the copying property.
    effective = _effective_reservations(["my_res"], "default", reg)
    assert list(effective) == [queue_cap_name, "my_res"]
    effective_no_cap = _effective_reservations(["my_res"], "other", reg)
    assert list(effective_no_cap) == ["my_res"]


def test_has_reservation_correct_at_large_registry_size() -> None:
    """``has_reservation`` stays correct for hits and misses at 10k entries
    (the cardinality where the copy-based check it replaces was a problem).
    The no-copy guarantee itself is pinned by
    ``test_effective_reservations_never_copies_registry_dict``."""
    reg = RateLimitRegistry()
    for i in range(10_000):
        reg.register(_reservation(f"res-{i}", slots=1, lease=timedelta(seconds=30)))
    assert reg.has_reservation("res-9999") is True
    assert reg.has_reservation("res-0") is True
    assert reg.has_reservation("nope") is False
    assert reg.has_rate_limit("res-0") is False  # separate namespace


def test_has_accessors_membership() -> None:
    """``has_reservation`` / ``has_rate_limit`` reflect their own dicts only."""
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu", slots=2, lease=timedelta(seconds=30)))
    reg.register(TokenBucket(name="api", capacity=10, refill_per_second=1.0, backend="memory"))

    assert reg.has_reservation("gpu") is True
    assert reg.has_reservation("api") is False
    assert reg.has_rate_limit("api") is True
    assert reg.has_rate_limit("gpu") is False


def test_has_rate_limit_never_copies_registry_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``has_rate_limit`` carries the same no-copy guarantee as
    ``has_reservation`` (pinned by
    ``test_effective_reservations_never_copies_registry_dict``): the
    ``rate_limits`` property defensively copies the whole dict — fine at
    startup/admin cadence, prohibitive per call at high keyed-entry
    cardinality. Patches the property to raise on ANY access, so a
    re-implementation of membership checks in terms of the copying
    property fails loudly."""

    def _boom(self: RateLimitRegistry) -> dict[str, object]:
        raise AssertionError("membership check copied the rate_limits dict")

    monkeypatch.setattr(
        RateLimitRegistry,
        "rate_limits",
        property(_boom),  # type: ignore[arg-type]
    )

    reg = RateLimitRegistry()
    reg.register(TokenBucket(name="api", capacity=10, refill_per_second=1.0, backend="memory"))

    assert reg.has_rate_limit("api") is True
    assert reg.has_rate_limit("nope") is False


# ── Queue-cap saturation: operator-visible denial ──


async def test_queue_cap_saturation_snoozes_with_operator_visible_awaiting() -> None:
    """When the fleet-wide queue cap is saturated, the job is snoozed (not
    failed, not silently dropped) with metadata telling an operator exactly
    WHY it isn't dispatching: ``awaiting: reservation:taskq:global:queue:<q>``.

    Uses the real registry + acquire path (not a stub): the queue-cap
    reservation is held by another worker, so ``acquire_for_actor`` raises
    ``ReservationUnavailable(source="reservation")`` and
    ``consume_one_job`` routes it to the reservation-denied snooze path.
    """
    from pydantic import BaseModel

    from taskq.backend.clock import Clock
    from taskq.context import JobContext
    from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job
    from taskq.worker.dispatch import _effective_reservations

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    queue_cap_name = queue_concurrency_reservation_name("orders")
    cap_res = _reservation(queue_cap_name, slots=1, lease=timedelta(minutes=5), clock=clock)
    reg.register_queue_cap_reservation(cap_res)

    # Another worker holds the only slot → the cap is saturated.
    await cap_res.acquire(new_uuid(), new_uuid())

    # The dispatch-time prepend produces the queue-cap-first acquire list.
    reservations = _effective_reservations([], "orders", reg)
    assert list(reservations) == [queue_cap_name]

    backend = FakeBackend()

    async def never_called_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise AssertionError("actor body must not run when the queue cap denies")

    clk: Clock = clock
    outcome = await consume_one_job(
        as_backend(backend),
        make_job_row(queue="orders"),
        new_uuid(),
        run_actor=never_called_actor,
        actor_config=default_actor_config(),
        payload_type=EmptyPayload,
        clock=clk,
        rate_limit_registry=reg,
        rate_limits=[],
        reservations=list(reservations),
        validated_payload=EmptyPayload(),
    )

    assert outcome == "scheduled"
    assert len(backend.mark_snoozed_calls) == 1
    snooze_call = backend.mark_snoozed_calls[0]
    assert snooze_call["metadata_update"] == {"awaiting": "reservation:taskq:global:queue:orders"}
    assert snooze_call["outcome"] == "reservation_denied"


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

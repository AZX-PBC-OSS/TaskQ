"""Claim-time reservation headroom: a reservation-saturated actor's rows
are not claimed into consumer slots that can only deny them.

The dispatch CTE (``src/taskq/backend/_dispatch_sql.py``,
``reservation_holdings`` / ``reservation_headroom``) folds live
reservation-slot occupancy into per-actor admission, derived from holder
state alone (``reservation_slots.job_id`` → ``jobs.actor`` — the database
carries no actor→bucket declaration mapping, so static, keyed, and
queue-cap buckets all ride the same derivation). The contract pinned
here, through the production claim path:

* an actor whose every held bucket is full is admitted nothing — its
  pending rows stay pending instead of churning a shared consumer
  coroutine per row into a denied ``acquire_for_actor`` (the measured
  25-45% neighbour-throughput regression of the un-gated shape);
* the gate is a damper, not an authority: the post-claim acquire remains
  the decision of record, the first claim of a never-running actor is
  never gated (NULL headroom leaves the residual untouched), and the
  moment capacity frees — release, or lease expiry, which the acquire
  path treats as acquirable — the actor's rows flow again (no starvation
  inversion);
* a co-located actor sharing the queue is untouched throughout;
* a partially free held bucket bounds the candidate window to
  headroom x oversample, never to zero.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from taskq._ids import new_base62
from taskq.ratelimit.reservation import ConcurrencyReservation
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "headroom_q"
_SATURATED = "headroom_saturated"
_HEALTHY = "headroom_healthy"


async def _held_reservation(fleet: Fleet, *, slots: int) -> ConcurrencyReservation:
    reservation = ConcurrencyReservation(
        name=f"headroom_slot_{new_base62()}".lower(),
        slots=slots,
        lease=timedelta(minutes=10),
        schema=fleet.schema,
    )
    await reservation.ensure_slots(fleet.any_pod().deps.worker_pool)
    return reservation


async def test_full_held_bucket_excludes_actor_until_capacity_frees(pg_dsn: str) -> None:
    """While the actor's only slot is live-held, its pending rows are not
    claimed; the moment the slot frees, they are."""
    schema = f"headroom_gate_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        pod = fleet.pod("pod-1")
        reservation = await _held_reservation(fleet, slots=1)

        # The first claim of a never-running actor is never gated: the
        # holder job itself must be claimable, or capacity could never be
        # taken in the first place.
        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 4)
        assert [job.id for job in first] == [holder_id]
        slot = await reservation.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        saturated_ids = set(await fleet.enqueue(5, actor=_SATURATED, queue=_QUEUE))
        healthy_ids = set(await fleet.enqueue(3, actor=_HEALTHY, queue=_QUEUE))

        claimed = await pod.claim([_QUEUE], 10)
        assert {job.id for job in claimed} == healthy_ids, (
            "the saturated actor's rows must not be claimed while its only "
            "reservation bucket is full — each would be claimed straight into "
            "a denied acquire, churning a shared consumer slot per row"
        )
        # The gated rows were never touched: still pending, never claimed.
        states = await fleet.job_states()
        assert all(states[job_id] == "pending" for job_id in saturated_ids)

        # Capacity frees: the gate opens on the same live state the
        # acquire path reads, so the actor drains immediately.
        await reservation.release(slot, pod.worker_id, pod.deps.worker_pool)
        claimed_after = await pod.claim([_QUEUE], 10)
        assert {job.id for job in claimed_after} == saturated_ids, (
            "a saturated actor whose capacity freed must be claimed again — "
            "the gate is a damper against un-runnable claims, never a "
            "starvation mechanism"
        )


async def test_expired_lease_does_not_gate(pg_dsn: str) -> None:
    """An expired-lease slot is acquirable (the acquire CTE hands it out
    on the spot), so it must not close the gate either — the claim's
    occupancy read is the acquire path's own free/held definition."""
    schema = f"headroom_expired_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE),),
    ) as fleet:
        pod = fleet.pod("pod-1")
        reservation = await _held_reservation(fleet, slots=1)

        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 1)
        assert len(first) == 1
        await reservation.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)
        # The holder's worker vanishes without a release: the lease runs
        # out, which is the design's abandonment signal.
        await fleet.fetch(
            'UPDATE "{schema}".reservation_slots '
            "SET lease_expires_at = clock_timestamp() - interval '1 second' "
            "WHERE bucket_name = $1",
            reservation.name,
        )

        saturated_ids = set(await fleet.enqueue(3, actor=_SATURATED, queue=_QUEUE))
        claimed = await pod.claim([_QUEUE], 10)
        assert saturated_ids <= {job.id for job in claimed}, (
            "an expired-lease slot is acquirable capacity; treating it as "
            "held would strand the actor behind a dead holder's lease row"
        )


async def test_gate_covers_both_routing_arms(pg_dsn: str) -> None:
    """A re-pended saturated-actor row (assignment_routed=true — the shape
    a denied job returns in) is gated exactly like a producer-placed one:
    the fold sits in both capacity CTEs, so neither routing arm leaks a
    claim into a consumer slot while the bucket is full."""
    schema = f"headroom_arms_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        pod = fleet.pod("pod-1")
        reservation = await _held_reservation(fleet, slots=1)

        # Holder takes the slot; one further saturated row stays
        # producer-placed, one is re-pended (assignment_routed — the
        # routing contract's marker for a row a re-pend path handed back).
        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 1)
        assert len(first) == 1
        slot = await reservation.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        (label_routed_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        (repended_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET assignment_routed = true, started_at = clock_timestamp() '
            "WHERE id = $1",
            repended_id,
        )
        (healthy_id,) = await fleet.enqueue(1, actor=_HEALTHY, queue=_QUEUE)

        claimed = await pod.claim([_QUEUE], 10)
        assert {job.id for job in claimed} == {healthy_id}, (
            "while the bucket is full, neither the producer-placed nor the "
            "re-pended saturated row may be claimed — the gate lives in "
            "per_actor_capacity AND repend_capacity"
        )
        states = await fleet.job_states()
        assert states[label_routed_id] == "pending" and states[repended_id] == "pending"

        await reservation.release(slot, pod.worker_id, pod.deps.worker_pool)
        claimed_after = await pod.claim([_QUEUE], 10)
        assert {label_routed_id, repended_id} <= {job.id for job in claimed_after}, (
            "both routing arms re-open the moment capacity frees"
        )


async def test_partial_headroom_bounds_the_candidate_window(pg_dsn: str) -> None:
    """A partially free held bucket admits at most headroom x oversample
    rows of that actor per round — enough to fill the real capacity, never
    the whole round's backlog of an actor that can mostly not run."""
    schema = f"headroom_partial_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        pod = fleet.pod("pod-1")
        reservation = await _held_reservation(fleet, slots=2)

        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 1)
        assert len(first) == 1
        await reservation.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        await fleet.enqueue(6, actor=_SATURATED, queue=_QUEUE)
        healthy_ids = set(await fleet.enqueue(2, actor=_HEALTHY, queue=_QUEUE))

        claimed = await pod.claim([_QUEUE], 8)
        claimed_saturated = [job for job in claimed if job.actor == _SATURATED]
        # headroom = 1 free slot; the candidate window is residual x
        # oversample (1 x 2), the same best-effort over-admission doctrine
        # max_concurrent keeps — the post-claim acquire stays the
        # authority for the final slot.
        assert 0 < len(claimed_saturated) <= 2, (
            f"with one of two slots free the claim admitted "
            f"{len(claimed_saturated)} saturated rows; the window bound is "
            "headroom x oversample (2), never the full backlog"
        )
        assert {job.id for job in claimed if job.actor == _HEALTHY} == healthy_ids

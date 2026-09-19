"""Claim-time reservation headroom: a reservation-saturated actor's rows
are not claimed into consumer slots that can only deny them.

The dispatch CTE (``src/taskq/backend/_dispatch_sql.py``,
``reservation_holdings`` / ``reservation_headroom``) folds live
reservation-slot occupancy into per-actor admission, derived from holder
state alone (``reservation_slots.job_id`` → ``jobs.actor`` - the database
carries no actor→bucket declaration mapping). The contract pinned
here, through the production claim path:

* an actor whose every held STATIC bucket is full is admitted nothing:
  its pending rows stay pending instead of churning a shared consumer
  coroutine per row into a denied ``acquire_for_actor`` (the measured
  25-45% neighbour-throughput regression of the un-gated shape);
* the gate is a damper, not an authority: the post-claim acquire remains
  the decision of record, the first claim of a never-running actor is
  never gated (NULL headroom leaves the residual untouched), and the
  moment capacity frees - release, or lease expiry, which the acquire
  path treats as acquirable - the actor's rows flow again (no starvation
  inversion);
* a co-located actor sharing the queue is untouched throughout;
* a partially free held bucket bounds the candidate window to
  headroom x oversample, never to zero.

Two scoping rules extend the contract:

* KEYED buckets are excluded from the claim-time fold: their concrete
  names are payload-derived per job (``f"{base_name}:{key}"``), so the
  claim cannot know which pending row needs which key; one saturated
  tenant must not block every other tenant of the same actor. Their caps
  stay enforced where the key IS known: the consumer's post-claim
  ``acquire_for_actor`` resolves the concrete name from the validated
  payload and denies (snooze) when that key's bucket is full, pinned
  here end-to-end (a saturated tenant's extra jobs deny while its
  sibling tenant's jobs complete).
* QUEUE-CAP buckets fold per (actor, queue), not per actor: the
  fleet-wide queue cap binds per queue, so queue X's saturation must not
  zero the same actor's claims on queue Y.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry, queue_concurrency_reservation_name
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.worker._consumer import consume_one_job
from tests._fleet import Fleet, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "headroom_q"
_QUEUE_ALT = "headroom_alt_q"
_SATURATED = "headroom_saturated"
_HEALTHY = "headroom_healthy"


async def _held_reservation(
    fleet: Fleet,
    *,
    slots: int,
    keyed: bool = False,
    name: str | None = None,
) -> ConcurrencyReservation:
    reservation = ConcurrencyReservation(
        name=name or f"headroom_slot_{new_base62()}".lower(),
        slots=slots,
        lease=timedelta(minutes=10),
        schema=fleet.schema,
        keyed=keyed,
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
            "reservation bucket is full - each would be claimed straight into "
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
            "a saturated actor whose capacity freed must be claimed again - "
            "the gate is a damper against un-runnable claims, never a "
            "starvation mechanism"
        )


async def test_expired_lease_does_not_gate(pg_dsn: str) -> None:
    """An expired-lease slot is acquirable (the acquire CTE hands it out
    on the spot), so it must not close the gate either - the claim's
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
    """A re-pended saturated-actor row (assignment_routed=true - the shape
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
        # producer-placed, one is re-pended (assignment_routed - the
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
            "re-pended saturated row may be claimed - the gate lives in "
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
    rows of that actor per round - enough to fill the real capacity, never
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
        # max_concurrent keeps - the post-claim acquire stays the
        # authority for the final slot.
        assert 0 < len(claimed_saturated) <= 2, (
            f"with one of two slots free the claim admitted "
            f"{len(claimed_saturated)} saturated rows; the window bound is "
            "headroom x oversample (2), never the full backlog"
        )
        assert {job.id for job in claimed if job.actor == _HEALTHY} == healthy_ids


async def test_keyed_bucket_never_gates_its_holder_actor(pg_dsn: str) -> None:
    """A KEYED bucket held full by one tenant must not gate the actor's
    other rows: the claim cannot know which payload-derived key a
    pending row needs, so the keyed occupancy leaves the headroom fold
    entirely: the per-key cap stays with the consumer's post-claim
    acquire. A STATIC full bucket gates the same actor (the first test);
    the keyed twin of the same shape must not."""
    schema = f"headroom_keyed_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        pod = fleet.pod("pod-1")
        reservation = await _held_reservation(fleet, slots=1, keyed=True)
        # Fixture validity: the keyed mark is what the claim's exclusion
        # keys off: an unmarked row would ride the static fold and the
        # test would pin the wrong thing.
        marked = await fleet.fetch(
            'SELECT count(*) AS n FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND keyed",
            reservation.name,
        )
        assert int(marked[0]["n"]) == 1

        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 4)
        assert [job.id for job in first] == [holder_id]
        slot = await reservation.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        # Sibling rows of the SAME actor: at claim time these are
        # indistinguishable from the holder tenant's own extra rows, and
        # that is exactly why the keyed bucket must not gate them: the
        # other tenants' rows would be stranded behind a tenant they do
        # not belong to.
        sibling_ids = set(await fleet.enqueue(5, actor=_SATURATED, queue=_QUEUE))
        healthy_ids = set(await fleet.enqueue(2, actor=_HEALTHY, queue=_QUEUE))

        claimed = await pod.claim([_QUEUE], 10)
        assert sibling_ids <= {job.id for job in claimed}, (
            "a keyed bucket held full by one tenant gated the actor's "
            "other rows, the claim cannot know which pending row needs "
            "which payload-derived key, so keyed occupancy must leave the "
            "headroom fold entirely"
        )
        assert healthy_ids <= {job.id for job in claimed}

        # The keyed cap itself still bites where the key is known: the
        # post-claim acquire (the consumer-level test below pins it
        # end-to-end; this claim-level pin keeps the fixture honest by
        # showing the slot is genuinely held full while the rows flow).
        states = await fleet.job_states()
        assert states[holder_id] == "running"
        await reservation.release(slot, pod.worker_id, pod.deps.worker_pool)


async def test_queue_cap_saturation_scopes_to_its_queue(pg_dsn: str) -> None:
    """A full QUEUE-CAP bucket gates its holder actors' claims on THAT
    queue only, the same actor's claims on every other queue flow
    untouched (pre-fix, the per-actor MIN fold zeroed the actor's
    admission everywhere once any one queue's cap saturated)."""
    schema = f"headroom_qcap_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE),),
    ) as fleet:
        pod = fleet.pod("pod-1")
        cap = await _held_reservation(
            fleet,
            slots=1,
            name=queue_concurrency_reservation_name(_QUEUE),
        )

        # The holder job runs on _QUEUE and takes the queue's only cap
        # slot; the same actor also holds pending rows on _QUEUE_ALT.
        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 4)
        assert [job.id for job in first] == [holder_id]
        slot = await cap.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        gated_ids = set(await fleet.enqueue(3, actor=_SATURATED, queue=_QUEUE))
        alt_ids = set(await fleet.enqueue(3, actor=_SATURATED, queue=_QUEUE_ALT))

        claimed = await pod.claim([_QUEUE, _QUEUE_ALT], 10)
        assert {job.id for job in claimed} == alt_ids, (
            "a full queue-cap bucket on one queue gated the SAME actor's "
            "claims on another queue, the queue cap binds per queue, so "
            "its headroom fold must be scoped to the queue being probed"
        )

        # Capacity frees on the saturated queue: its rows flow again,
        # exactly like the static gate (no starvation inversion).
        await cap.release(slot, pod.worker_id, pod.deps.worker_pool)
        claimed_after = await pod.claim([_QUEUE, _QUEUE_ALT], 10)
        assert gated_ids <= {job.id for job in claimed_after}


async def test_queue_cap_fold_covers_the_round_robin_arm(pg_dsn: str) -> None:
    """The round-robin variant's per-cohort admission LIMIT carries the
    same queue-scoped fold (its LEAST(residual, queue_cap_headroom)
    bound), so a full queue-cap bucket gates its holder actor's
    round-robin claims on that queue while the other queue flows."""
    schema = f"headroom_qcap_rr_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE),),
    ) as fleet:
        pod = fleet.pod("pod-1")
        for queue in (_QUEUE, _QUEUE_ALT):
            await fleet.fetch(
                'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
                "ON CONFLICT (name) DO UPDATE SET mode = $2",
                queue,
                "round_robin",
            )

        cap = await _held_reservation(
            fleet,
            slots=1,
            name=queue_concurrency_reservation_name(_QUEUE),
        )
        (holder_id,) = await fleet.enqueue(1, actor=_SATURATED, queue=_QUEUE)
        first = await pod.claim([_QUEUE], 4)
        assert [job.id for job in first] == [holder_id]
        slot = await cap.acquire(holder_id, pod.worker_id, pod.deps.worker_pool)

        gated_ids = set(await fleet.enqueue(3, actor=_SATURATED, queue=_QUEUE))
        alt_ids = set(await fleet.enqueue(3, actor=_SATURATED, queue=_QUEUE_ALT))

        claimed = await pod.claim([_QUEUE, _QUEUE_ALT], 10)
        assert {job.id for job in claimed} == alt_ids, (
            "the round-robin arm's admission window ignored the queue-cap "
            "headroom fold, a full cap bucket must gate its holder actor's "
            "claims on that queue in BOTH dispatch variants"
        )

        await cap.release(slot, pod.worker_id, pod.deps.worker_pool)
        claimed_after = await pod.claim([_QUEUE, _QUEUE_ALT], 10)
        assert gated_ids <= {job.id for job in claimed_after}


# ── Consumer-level: the keyed cap still enforces post-claim ─────────────
#
# The claim-level exclusion above is only safe because the keyed limit
# still bites somewhere the key IS known: the consumer's post-claim
# acquire_for_actor resolves f"{base_name}:{key}" from the validated
# payload and denies (snooze/reschedule) when that key's bucket is full.
# This is the adversarial-review pin for that: one tenant saturates its keyed
# bucket, and the OTHER tenant's jobs of the same actor complete anyway
# while the saturated tenant's extra jobs are denied by the acquire,
# never by the claim.


class _TenantPayload(BaseModel):
    tenant_id: str
    marker: str = ""


_KEYED_REF = KeyedReservationRef.typed(
    _TenantPayload,
    base_name="headroom_session",
    key_fn=lambda p: p.tenant_id,
    slots=1,
    lease=timedelta(minutes=10),
)
_MAX_CONCURRENCY = 4
_N_TENANT_A_FLOOD = 6
_N_TENANT_B = 6
_RUN_CEILING_SECONDS = 45.0


async def _enqueue_tenant(fleet: Fleet, *, actor: str, queue: str, tenant: str) -> JobId:
    """One due job carrying a tenant-scoped payload, as a producer would."""
    pod = fleet.any_pod()
    job_id = JobId(new_uuid())
    await pod.backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue=queue,
            payload={"tenant_id": tenant, "marker": f"{actor}-{tenant}"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    return job_id


async def _hog_forever(payload: _TenantPayload, ctx: object) -> None:
    await asyncio.sleep(3600)


async def _noop(payload: _TenantPayload, ctx: object) -> None:
    return None


async def test_keyed_limit_enforces_post_claim_per_tenant(pg_dsn: str) -> None:
    """end-to-end: tenant A saturates its keyed session bucket (one
    hog job holds the only slot); tenant B's jobs of the SAME actor
    complete (the claim never gated them on A's keyed occupancy), while
    A's further jobs are denied by the consumer's acquire, rescheduled,
    never run, so the per-key cap is enforced exactly where the
    exclusion moved it."""
    schema = f"headroom_keyed_e2e_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_SATURATED, _QUEUE),),
    ) as fleet:
        pod = fleet.pod("pod-1")
        registry = RateLimitRegistry()

        job_ids: dict[str, list[JobId]] = {"a": [], "b": []}
        for tenant, count in (("a", 1 + _N_TENANT_A_FLOOD), ("b", _N_TENANT_B)):
            for _ in range(count):
                job_ids[tenant].append(
                    await _enqueue_tenant(fleet, actor=_SATURATED, queue=_QUEUE, tenant=tenant)
                )

        local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=_MAX_CONCURRENCY)
        stop = asyncio.Event()
        b_done: list[JobId] = []

        async def producer() -> None:
            while not stop.is_set():
                available = local_queue.maxsize - local_queue.qsize()
                if available <= 0:
                    await asyncio.sleep(0.01)
                    continue
                claimed = await pod.claim([_QUEUE], available)
                for job in claimed:
                    await local_queue.put(job)
                if not claimed:
                    await asyncio.sleep(0.02)

        async def consumer() -> None:
            while not stop.is_set():
                try:
                    job = await asyncio.wait_for(local_queue.get(), timeout=0.05)
                except TimeoutError:
                    continue
                payload = _TenantPayload.model_validate(job.payload)
                handler: Callable[[_TenantPayload, object], Awaitable[None]] = (
                    _hog_forever if payload.tenant_id == "a" else _noop
                )

                async def _run_actor(
                    _row: JobRow,
                    ctx: object,
                    _h: Callable[[_TenantPayload, object], Awaitable[None]] = handler,
                    _p: _TenantPayload = payload,
                ) -> None:
                    return await _h(_p, ctx)

                await consume_one_job(
                    pod.backend,
                    job,
                    pod.worker_id,
                    deps=pod.deps,
                    run_actor=_run_actor,
                    actor_config=fleet_actor_config(),
                    payload_type=_TenantPayload,
                    clock=SystemClock(),
                    active_jobs=pod.deps.active_jobs,
                    rate_limit_registry=registry,
                    reservations=[_KEYED_REF],
                    worker_pool=pod.deps.worker_pool,
                    # The keyed ref's materialization needs the schema
                    # source for its reservation_slots rows (a static
                    # name does not: the isolation sibling omits it).
                    settings=pod.deps.settings,
                )

                if payload.tenant_id == "b":
                    b_done.append(job.id)
                    if len(b_done) >= _N_TENANT_B:
                        stop.set()

        prod_task = asyncio.create_task(producer())
        cons_tasks = [asyncio.create_task(consumer()) for _ in range(_MAX_CONCURRENCY)]
        try:
            deadline = time.monotonic() + _RUN_CEILING_SECONDS
            while not stop.is_set() and time.monotonic() < deadline:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=0.05)
        finally:
            stop.set()

        # Snapshot BEFORE cancelling the consumers, after a short drain
        # for in-flight denial writes to land: the hog job is parked
        # mid-actor-body and cancelling its consumer task terminalises
        # it (the production cancellation path), so the running-state
        # assertion must read it while it still holds the session slot.
        await asyncio.sleep(0.3)
        states = await fleet.job_states()
        denied_rows = await fleet.fetch(
            'SELECT count(*) AS n FROM "{schema}".jobs '
            "WHERE payload->>'tenant_id' = 'a' AND rate_limit_blocked_count > 0",
        )
        denied_states = await fleet.fetch(
            'SELECT status, count(*) AS n FROM "{schema}".jobs '
            "WHERE payload->>'tenant_id' = 'a' AND rate_limit_blocked_count > 0 "
            "GROUP BY status",
        )
        prod_task.cancel()
        for task in cons_tasks:
            task.cancel()
        await asyncio.gather(prod_task, *cons_tasks, return_exceptions=True)

        assert len(b_done) == _N_TENANT_B, (
            f"tenant B completed only {len(b_done)}/{_N_TENANT_B} jobs within the "
            f"{_RUN_CEILING_SECONDS}s ceiling while tenant A's keyed bucket was "
            "saturated, one tenant's keyed occupancy must not strand the "
            "actor's other tenants"
        )

        # The keyed cap still bites for the saturated tenant: of A's
        # jobs, exactly one holds the session slot (running the hog);
        # every other A job was denied by the acquire and rescheduled,
        # the claim-level exclusion above is safe BECAUSE this half holds.
        a_running = [jid for jid in job_ids["a"] if states[jid] == "running"]
        assert len(a_running) == 1, (
            f"{len(a_running)} tenant-A jobs are running against a "
            "single-slot keyed session bucket, the per-key cap is not "
            "enforcing"
        )
        denied_count = int(denied_rows[0]["n"])
        assert denied_count >= _N_TENANT_A_FLOOD, (
            f"only {denied_count} tenant-A jobs were ever denied, the keyed "
            "cap must keep denying the saturated tenant's extra jobs from "
            "the consumer's post-claim acquire (the authority the claim's "
            "exclusion relies on)"
        )
        # And the denial is admission control, not failure: the denied
        # rows are scheduled (snoozed), never terminalised by the denial.
        assert {row["status"] for row in denied_states} <= {"scheduled"}, denied_states

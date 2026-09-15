"""Dispatch starvation regression tests — spec

Validates pending_rank fairness, identity dedup, actor_config gate,
oversample absorption, per-actor priority resolution, and round-robin
cohort interleave. All tests use InMemoryBackend (unit tier).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, IdentityKey
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=30)


def _make_backend() -> InMemoryBackend:
    """Return a fresh InMemoryBackend with no pre-registered actors."""
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _enqueue_bulk(
    backend: InMemoryBackend,
    actor: str,
    queue: str,
    count: int,
    max_concurrent: int | None,
    *,
    priority: int = 0,
    identity_key: IdentityKey | None = None,
    fairness_key: str | None = None,
    scheduled_at: datetime | None = None,
) -> None:
    if max_concurrent is not None and actor not in backend._actor_configs_meta:  # type: ignore[reportPrivateUsage]
        backend.register_actor_config(actor=actor, max_concurrent=max_concurrent)
    now = scheduled_at if scheduled_at is not None else backend._clock.now()  # type: ignore[reportPrivateUsage]
    for _ in range(count):
        await backend.enqueue(
            EnqueueArgs(
                id=new_uuid(),
                actor=actor,
                queue=queue,
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=now,
                priority=priority,
                identity_key=identity_key,
                fairness_key=fairness_key,
            )
        )


async def _dispatch_cycles(
    backend: InMemoryBackend,
    worker_id: UUID,
    queues: list[str],
    limit: int,
    max_cycles: int = 5,
) -> list[set[str]]:
    """Dispatch max_cycles times, returning set of actor names per cycle."""
    results: list[set[str]] = []
    for _ in range(max_cycles):
        dispatched = await backend.dispatch_batch(worker_id, queues, limit, _LOCK_LEASE)
        actors = {j.actor for j in dispatched}
        results.append(actors)
        if not dispatched:
            break
        for j in dispatched:
            await backend.mark_succeeded(j.id, worker_id, result={})
    return results


# ── Starvation regression ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_starvation_regression_intra_queue() -> None:
    """Spec Test 1: 1700 copy jobs + 1 monitor job — monitor dispatched within 2 cycles."""
    backend = _make_backend()
    wid = new_uuid()
    await _enqueue_bulk(backend, "copy_file", "default", 1700, max_concurrent=5)
    await _enqueue_bulk(backend, "migration_monitor", "default", 1, max_concurrent=1)
    results = await _dispatch_cycles(backend, wid, ["default"], limit=30)
    assert any("migration_monitor" in cycle for cycle in results[:2]), (
        f"Monitor starved! Dispatch results: {results}"
    )


@pytest.mark.asyncio
async def test_starvation_regression_cross_queue() -> None:
    """Spec Test 2: 1700 copy jobs on 'copy' + 1 monitor on 'monitor' — monitor dispatched."""
    backend = _make_backend()
    wid = new_uuid()
    await _enqueue_bulk(backend, "copy_file", "copy", 1700, max_concurrent=5)
    await _enqueue_bulk(backend, "migration_monitor", "monitor", 1, max_concurrent=1)
    results = await _dispatch_cycles(backend, wid, ["copy", "monitor"], limit=30)
    assert any("migration_monitor" in cycle for cycle in results[:2])


# ── pending_rank ordering ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pending_rank_ordering() -> None:
    """Spec Test 3: rank-1 jobs from all actors dispatch before any rank-2."""
    backend = _make_backend()
    wid = new_uuid()
    await _enqueue_bulk(backend, "A", "default", 50, max_concurrent=10)
    await _enqueue_bulk(backend, "B", "default", 10, max_concurrent=1)
    await _enqueue_bulk(backend, "C", "default", 10, max_concurrent=1)
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=20, lock_lease=_LOCK_LEASE)
    actors_in_order = [j.actor for j in dispatched]
    first_a = actors_in_order.index("A")
    first_b = actors_in_order.index("B")
    first_c = actors_in_order.index("C")
    second_a_candidates = [i for i, a in enumerate(actors_in_order) if a == "A" and i != first_a]
    if second_a_candidates:
        second_a = second_a_candidates[0]
        assert first_b < second_a and first_c < second_a, (
            f"Rank-2 of A dispatched before rank-1 of B or C: {actors_in_order}"
        )


# ── Identity dedup ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_identity_dedup_ordering() -> None:
    """Spec Test 5: deterministic identity dedup selects highest-priority, earliest-scheduled."""
    backend = _make_backend()
    wid = new_uuid()
    earlier = _START - timedelta(hours=2)
    later = _START - timedelta(hours=1)
    ik = IdentityKey("dedup-test-1")

    await _enqueue_bulk(
        backend,
        "A",
        "default",
        1,
        max_concurrent=5,
        identity_key=ik,
        priority=10,
        scheduled_at=later,
    )
    await _enqueue_bulk(
        backend,
        "A",
        "default",
        1,
        max_concurrent=5,
        identity_key=ik,
        priority=10,
        scheduled_at=earlier,
    )
    await _enqueue_bulk(
        backend,
        "A",
        "default",
        1,
        max_concurrent=5,
        identity_key=ik,
        priority=5,
        scheduled_at=earlier,
    )
    await _enqueue_bulk(
        backend,
        "A",
        "default",
        1,
        max_concurrent=5,
        identity_key=ik,
        priority=10,
        scheduled_at=later,
    )

    dispatched = await backend.dispatch_batch(wid, ["default"], limit=20, lock_lease=_LOCK_LEASE)
    assert len(dispatched) == 1, f"Expected 1 deduplicated, got {len(dispatched)}"
    # Highest priority, earliest-scheduled: priority=10, earlier
    assert dispatched[0].priority == 10
    assert dispatched[0].scheduled_at == earlier


@pytest.mark.asyncio
async def test_identity_dedup_slot_preservation() -> None:
    """Spec Test 6: identity dedup doesn't waste max_concurrent slots."""
    backend = _make_backend()
    wid = new_uuid()
    ik = IdentityKey("dedup-test-2")
    for _ in range(5):
        await _enqueue_bulk(backend, "A", "default", 1, max_concurrent=5, identity_key=ik)
    await _enqueue_bulk(
        backend, "A", "default", 1, max_concurrent=5, identity_key=IdentityKey("other")
    )
    await _enqueue_bulk(backend, "B", "default", 1, max_concurrent=1)

    dispatched = await backend.dispatch_batch(wid, ["default"], limit=20, lock_lease=_LOCK_LEASE)
    actors = [j.actor for j in dispatched]
    assert len(dispatched) == 3, (
        f"Expected 3 (1 dedup group A + 1 other A + 1 B), got {len(dispatched)}"
    )
    assert actors.count("A") == 2
    assert actors.count("B") == 1


# ── Oversample absorption ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_oversample_absorption() -> None:
    """Spec Test 7: oversample=2 reaches 3 distinct identities per actor.

    InMemory doesn't have a configurable oversample — it iterates all
    candidates. This test confirms that all distinct identities dispatch
    (identity dedup doesn't collapse more than it should).
    """
    backend = _make_backend()
    wid = new_uuid()
    for ident in ["x", "y", "z"]:
        for _ in range(2):
            await _enqueue_bulk(
                backend,
                "A",
                "default",
                1,
                max_concurrent=5,
                identity_key=IdentityKey(ident),
            )
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=20, lock_lease=_LOCK_LEASE)
    idents = {j.identity_key for j in dispatched}
    assert len(idents) == 3, f"Expected 3 distinct identities, got {len(idents)}: {idents}"


@pytest.mark.asyncio
async def test_oversample_absorption_identity_dedupped() -> None:
    """Identity dedup: 2 per identity, 3 identities — dispatches 1 per identity."""
    backend = _make_backend()
    wid = new_uuid()
    for ident in ["x", "y", "z"]:
        for _ in range(2):
            await _enqueue_bulk(
                backend,
                "A",
                "default",
                1,
                max_concurrent=5,
                identity_key=IdentityKey(ident),
            )
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=20, lock_lease=_LOCK_LEASE)
    # 3 identities, each deduplicated to 1 -> 3 total dispatched
    assert len(dispatched) == 3, f"Expected 3 (1 per identity), got {len(dispatched)}"


# ── Per-actor priority ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_per_actor_priority() -> None:
    """Spec Test 8: actor default priority flows through via build_enqueue_args."""
    from pydantic import BaseModel

    from taskq.actor import actor as actor_decorator
    from taskq.client._args import build_enqueue_args

    class DummyPayload(BaseModel):
        x: int = 0

    @actor_decorator(name="test_prio", priority=10)
    async def test_prio_actor(payload: DummyPayload) -> None:
        pass

    args = build_enqueue_args(test_prio_actor, DummyPayload())
    assert args.priority == 10

    args_override = build_enqueue_args(test_prio_actor, DummyPayload(), priority=5)
    assert args_override.priority == 5

    args_explicit_zero = build_enqueue_args(test_prio_actor, DummyPayload(), priority=0)
    assert args_explicit_zero.priority == 0


def test_priority_smallint_validation() -> None:
    """Priority must fit smallint range."""
    from pydantic import BaseModel

    from taskq.actor import actor as actor_decorator

    class DummyPayload(BaseModel):
        x: int = 0

    with pytest.raises(ValueError, match="smallint"):

        @actor_decorator(name="bad_prio", priority=40000)
        async def bad_prio_actor(payload: DummyPayload) -> None:  # pyright: ignore[reportUnusedFunction] # Why: test case uses the function as a side effect; the decorator raises the expected ValueError.
            pass


# ── Actor_config gate ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_actor_config_gate_registered() -> None:
    """Spec Test 12: registered actors dispatch."""
    backend = _make_backend()
    wid = new_uuid()
    await _enqueue_bulk(backend, "registered_actor", "default", 1, max_concurrent=5)
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=30, lock_lease=_LOCK_LEASE)
    assert any(j.actor == "registered_actor" for j in dispatched)


@pytest.mark.asyncio
async def test_actor_config_gate_unregistered() -> None:
    """Spec Test 12: unregistered actors do NOT dispatch when actor_config is populated."""
    backend = _make_backend()
    wid = new_uuid()
    # Register one actor so _actor_configs_meta is non-empty → gate activates
    backend.register_actor_config(actor="some_actor")
    # Enqueue for a different unregistered actor
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="unregistered_z",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=backend._clock.now(),  # type: ignore[reportPrivateUsage]
        )
    )
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=30, lock_lease=_LOCK_LEASE)
    assert all(j.actor != "unregistered_z" for j in dispatched), (
        "Unregistered actor should not dispatch when gate is active"
    )


@pytest.mark.asyncio
async def test_actor_config_gate_empty_blocks_all() -> None:
    """When _actor_configs_meta is empty, NOTHING dispatches.

    PG's per_actor_capacity CTE builds candidates FROM the actor_config
    registry (backend/_dispatch_sql.py): zero registered actors means
    zero capacity rows means zero candidates — "no actors registered"
    must never read as "no filter". The old escape let the mirror
    dispatch work a real worker polling the same empty registry never
    would (the mirror was greener than production — pinned as a RED
    differential in tests/test_rt_diff_dispatch.py).
    """
    backend = _make_backend()
    wid = new_uuid()
    # No actor_config registered → the gate admits nothing.
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="any_actor_no_config",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=backend._clock.now(),  # type: ignore[reportPrivateUsage]
        )
    )
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=30, lock_lease=_LOCK_LEASE)
    assert dispatched == [], (
        "An empty actor registry must dispatch NOTHING on either backend "
        "(PG's per_actor_capacity has no rows to build candidates from) — "
        "never silently read 'no actors registered' as 'no filter'"
    )


# ── Round-robin fairness ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_round_robin_cohort_interleave() -> None:
    """Spec Test 11: cohorts interleave when both in window.

    InMemory does not have configurable oversample, but the fairness_rank
    sort ensures cohort interleave within each actor's pending window.
    """
    backend = _make_backend()
    wid = new_uuid()
    backend.set_queue_mode("rr_queue", "round_robin")  # type: ignore[reportPrivateUsage]
    backend.register_actor_config(actor="A", max_concurrent=4)

    now = backend._clock.now()  # type: ignore[reportPrivateUsage]
    for _ in range(4):
        await backend.enqueue(
            EnqueueArgs(
                id=new_uuid(),
                actor="A",
                queue="rr_queue",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=now,
                fairness_key="a",
                priority=0,
            )
        )
        await backend.enqueue(
            EnqueueArgs(
                id=new_uuid(),
                actor="A",
                queue="rr_queue",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=now,
                fairness_key="b",
                priority=0,
            )
        )

    dispatched = await backend.dispatch_batch(wid, ["rr_queue"], limit=4, lock_lease=_LOCK_LEASE)
    cohorts = {j.fairness_key for j in dispatched}
    assert len(cohorts) >= 2, f"Expected interleaved cohorts, got only: {cohorts}"


@pytest.mark.asyncio
async def test_round_robin_priority_tiebreak() -> None:
    """Fairness_rank interleave preserves priority ordering within each cohort."""
    backend = _make_backend()
    wid = new_uuid()
    backend.set_queue_mode("rr_queue", "round_robin")  # type: ignore[reportPrivateUsage]
    backend.register_actor_config(actor="A", max_concurrent=5)

    now = backend._clock.now()  # type: ignore[reportPrivateUsage]
    # Cohort a: 2 jobs (priority 10, then priority 0)
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="A",
            queue="rr_queue",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            fairness_key="a",
            priority=10,
        )
    )
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="A",
            queue="rr_queue",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            fairness_key="a",
            priority=0,
        )
    )
    # Cohort b: 2 jobs (priority 0 only)
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="A",
            queue="rr_queue",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            fairness_key="b",
            priority=0,
        )
    )
    await backend.enqueue(
        EnqueueArgs(
            id=new_uuid(),
            actor="A",
            queue="rr_queue",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=now,
            fairness_key="b",
            priority=0,
        )
    )

    dispatched = await backend.dispatch_batch(wid, ["rr_queue"], limit=4, lock_lease=_LOCK_LEASE)
    actors = [j.actor for j in dispatched]
    assert len(dispatched) == 4
    assert actors.count("A") == 4

    # First dispatched job should have priority=10 from cohort a
    assert dispatched[0].priority == 10


# ── Enqueue-time priority validation ──────────────────────────────────────


def test_enqueue_priority_validation() -> None:
    """Spec Test 9: enqueue(priority=40000) -> ValueError at enqueue time."""
    from pydantic import BaseModel

    from taskq.actor import actor as actor_decorator
    from taskq.client._args import build_enqueue_args

    class DummyPayload(BaseModel):
        x: int = 0

    @actor_decorator(name="test_enq_prio")
    async def test_enq_prio_actor(payload: DummyPayload) -> None:
        pass

    with pytest.raises(ValueError, match="smallint"):
        build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=40000)

    with pytest.raises(ValueError, match="smallint"):
        build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=-40000)

    # Valid priorities should succeed
    args = build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=32767)
    assert args.priority == 32767

    args = build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=-32768)
    assert args.priority == -32768


# ── Actor count above the dispatch limit ────────────────────────────────


async def _seed_uncapped_backlog(
    backend: InMemoryBackend, actors: list[str], queue: str, depth: int
) -> None:
    """Register each actor uncapped and give it *depth* due, equal-priority jobs.

    Uncapped is the point: nothing about any actor's own configuration limits
    how much of it may run, so whatever a dispatch round leaves behind was
    left behind by selection, not by a capacity rule.
    """
    for name in actors:
        backend.register_actor_config(actor=name, max_concurrent=None)
        await _enqueue_bulk(backend, name, queue, depth, max_concurrent=None)


async def _drain_tally(
    backend: InMemoryBackend,
    worker_id: UUID,
    queues: list[str],
    limit: int,
    rounds: int,
) -> dict[str, int]:
    """Run *rounds* dispatch rounds, completing every claim, and tally the
    claims each actor received. Completing each round's claims models a
    fleet whose workers keep up: no actor is held back by its own in-flight
    count, so any actor never claimed was excluded by dispatch selection.
    """
    tally: dict[str, int] = {}
    for _ in range(rounds):
        dispatched = await backend.dispatch_batch(worker_id, queues, limit, _LOCK_LEASE)
        for job in dispatched:
            tally[job.actor] = tally.get(job.actor, 0) + 1
        for job in dispatched:
            await backend.mark_succeeded(job.id, worker_id, result={})
    return tally


@pytest.mark.asyncio
async def test_every_actor_with_backlog_is_eventually_claimed() -> None:
    """More uncapped actors with pending work than the dispatch limit: every
    actor must eventually be claimed.

    A worker dispatches at most ``max_concurrency`` jobs per round, so a
    deployment registering more actors than that has more actors holding
    pending work than one round can carry. The surplus actors must be served
    on a later round. If selection is a stable total order over all actors'
    head jobs, the same prefix wins every round and the remaining actors
    never run at all -- an operator sees jobs for those actors sit pending
    forever with no error, no denial and no alert, while the queue drains
    briskly for everyone else.
    """
    backend = _make_backend()
    wid = new_uuid()
    actors = [f"actor_{i:02d}" for i in range(20)]
    limit = 5
    await _seed_uncapped_backlog(backend, actors, "default", 30)

    tally = await _drain_tally(backend, wid, ["default"], limit=limit, rounds=20)

    starved = sorted(name for name in actors if tally.get(name, 0) == 0)
    assert not starved, (
        f"{len(starved)} of {len(actors)} actors were never claimed across 20 dispatch "
        f"rounds at limit {limit}, while every one of them held 30 pending jobs the "
        f"whole time: {starved}. Claims went entirely to "
        f"{sorted(k for k, v in tally.items() if v)}. Work for the starved actors "
        f"never runs -- there is no cap, no denial and no failure to observe, so the "
        f"only symptom is jobs that stay pending forever."
    )


@pytest.mark.asyncio
async def test_surplus_actors_are_served_when_spread_across_queues() -> None:
    """Splitting the same surplus actors across several subscribed queues does
    not rescue the ones the dispatch limit leaves out.

    An operator whose actors are not all being served reaches first for queue
    separation -- give the quiet actors their own queue and subscribe the same
    worker to both. That is only a fix if the round's selection considers the
    queues independently. If the round ranks every subscribed queue's actors
    into one order and cuts at the limit, the new queue changes nothing and the
    same actors stay dark, which makes the natural remedy look like it did not
    work.
    """
    backend = _make_backend()
    wid = new_uuid()
    busy = [f"busy_{i:02d}" for i in range(8)]
    quiet = [f"quiet_{i:02d}" for i in range(4)]
    limit = 5
    await _seed_uncapped_backlog(backend, busy, "busy_queue", 40)
    await _seed_uncapped_backlog(backend, quiet, "quiet_queue", 40)

    tally = await _drain_tally(backend, wid, ["busy_queue", "quiet_queue"], limit=limit, rounds=20)

    starved_quiet = sorted(name for name in quiet if tally.get(name, 0) == 0)
    assert not starved_quiet, (
        f"moving actors to their own queue did not get them dispatched: "
        f"{starved_quiet} held 40 pending jobs each on a dedicated, subscribed queue "
        f"and were never claimed across 20 rounds at limit {limit}. Claims: "
        f"{ {k: v for k, v in sorted(tally.items()) if v} }. An operator who separates "
        f"queues to unblock quiet actors sees the quiet queue stay at full depth."
    )


@pytest.mark.asyncio
async def test_an_actor_added_to_a_busy_fleet_starts_dispatching() -> None:
    """A newly registered actor on a queue that already has more actors than
    the dispatch limit must start getting claims.

    This is the deploy-time shape: a fleet is running and draining fine, a
    release adds one more actor, and its first jobs arrive. The new actor's
    work has to run. If selection is a stable order over all actors' head jobs
    and the limit cuts before the newcomer, its jobs stay pending indefinitely
    with nothing in logs, metrics or job state to show why -- the release looks
    successful and the feature is simply dead.
    """
    backend = _make_backend()
    wid = new_uuid()
    incumbents = [f"incumbent_{i:02d}" for i in range(10)]
    limit = 4
    await _seed_uncapped_backlog(backend, incumbents, "default", 40)

    warmup = await _drain_tally(backend, wid, ["default"], limit=limit, rounds=5)
    assert sum(warmup.values()) > 0, "fixture broken: the fleet claimed nothing at all"

    newcomer = "newcomer"
    await _seed_uncapped_backlog(backend, [newcomer], "default", 20)
    after = await _drain_tally(backend, wid, ["default"], limit=limit, rounds=20)

    assert after.get(newcomer, 0) > 0, (
        f"a newly deployed actor never dispatched: {newcomer} held 20 pending jobs and "
        f"got zero claims across 20 rounds at limit {limit} on a queue already carrying "
        f"{len(incumbents)} actors. Claims went to "
        f"{ {k: v for k, v in sorted(after.items()) if v} }. The operator's release "
        f"reports success while the new actor's jobs never run."
    )

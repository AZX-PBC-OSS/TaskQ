"""Dispatch starvation regression tests — spec

Validates pending_rank fairness, identity dedup, actor_config gate,
oversample absorption, per-actor priority resolution, and round-robin
cohort interleave. All tests use InMemoryBackend (unit tier).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

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
    wid = uuid4()
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
    wid = uuid4()
    await _enqueue_bulk(backend, "copy_file", "copy", 1700, max_concurrent=5)
    await _enqueue_bulk(backend, "migration_monitor", "monitor", 1, max_concurrent=1)
    results = await _dispatch_cycles(backend, wid, ["copy", "monitor"], limit=30)
    assert any("migration_monitor" in cycle for cycle in results[:2])


# ── pending_rank ordering ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pending_rank_ordering() -> None:
    """Spec Test 3: rank-1 jobs from all actors dispatch before any rank-2."""
    backend = _make_backend()
    wid = uuid4()
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
    wid = uuid4()
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
    wid = uuid4()
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
    wid = uuid4()
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
    wid = uuid4()
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

    clock = FakeClock(start=_START)

    args = build_enqueue_args(test_prio_actor, DummyPayload(), clock=clock)
    assert args.priority == 10

    args_override = build_enqueue_args(test_prio_actor, DummyPayload(), priority=5, clock=clock)
    assert args_override.priority == 5

    args_explicit_zero = build_enqueue_args(
        test_prio_actor, DummyPayload(), priority=0, clock=clock
    )
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
    wid = uuid4()
    await _enqueue_bulk(backend, "registered_actor", "default", 1, max_concurrent=5)
    dispatched = await backend.dispatch_batch(wid, ["default"], limit=30, lock_lease=_LOCK_LEASE)
    assert any(j.actor == "registered_actor" for j in dispatched)


@pytest.mark.asyncio
async def test_actor_config_gate_unregistered() -> None:
    """Spec Test 12: unregistered actors do NOT dispatch when actor_config is populated."""
    backend = _make_backend()
    wid = uuid4()
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
async def test_actor_config_gate_empty_allows_all() -> None:
    """When _actor_configs_meta is empty, all actors pass the gate."""
    backend = _make_backend()
    wid = uuid4()
    # No actor_config registered → gate is open
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
    assert any(j.actor == "any_actor_no_config" for j in dispatched), (
        "Actor should dispatch when gate is open (empty _actor_configs_meta)"
    )


# ── Round-robin fairness ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_round_robin_cohort_interleave() -> None:
    """Spec Test 11: cohorts interleave when both in window.

    InMemory does not have configurable oversample, but the fairness_rank
    sort ensures cohort interleave within each actor's pending window.
    """
    backend = _make_backend()
    wid = uuid4()
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
    wid = uuid4()
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

    clock = FakeClock(start=_START)

    with pytest.raises(ValueError, match="smallint"):
        build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=40000, clock=clock)

    with pytest.raises(ValueError, match="smallint"):
        build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=-40000, clock=clock)

    # Valid priorities should succeed
    args = build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=32767, clock=clock)
    assert args.priority == 32767

    args = build_enqueue_args(test_enq_prio_actor, DummyPayload(), priority=-32768, clock=clock)
    assert args.priority == -32768

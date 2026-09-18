"""The in-memory dispatch twin mirrors the claim-time reservation
headroom gate (parity with
``src/taskq/backend/_dispatch_sql.py``'s ``reservation_holdings`` /
``reservation_headroom`` CTEs).

The twin reads the same holder-state signal PG reads — the backend's
slot table (the ``reservation_slots`` mirror) joined to the jobs store
for the holder's actor — because neither store carries an actor→bucket
declaration mapping. Pinned behavior, identical to the PG gate's
(tests/test_dispatch_reservation_headroom.py):

* a live-held, full STATIC bucket gates its holder's actor out of the
  round, across every queue the actor holds rows on, because the
  static fold is per actor;
* an actor holding nothing is never gated (the first claim gets
  through — capacity can always be taken);
* a freed or expired-lease slot re-opens admission (no starvation
  inversion);
* a co-located actor's rows are claimed throughout;
* a live-held, full KEYED bucket gates nothing (#242): the keyed mark
  on its slot rows (the twin of the PG ``reservation_slots.keyed``
  column) excludes the bucket from the fold, because claim time cannot
  know which payload-derived key a pending row needs;
* a live-held, full QUEUE-CAP bucket gates its holder actor's claims on
  THAT queue only: the same actor's claims on every other queue flow
  (#242), in strict-FIFO and round-robin alike.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from taskq.backend.clock import Clock
from taskq.constants import QUEUE_CONCURRENCY_PREFIX
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=30)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _enqueue(backend: InMemoryBackend, *, actor: str, queue: str = "q") -> None:
    await backend.enqueue(make_enqueue_args(actor=actor, queue=queue, scheduled_at=_START))


def _register(backend: InMemoryBackend, *actors: str) -> None:
    for actor in actors:
        backend.register_actor_config(actor=actor, queue="q")


async def test_full_held_bucket_gates_only_the_holder_actor() -> None:
    backend = _make_backend()
    _register(backend, "sat", "healthy")
    await _enqueue(backend, actor="sat")

    # The first claim is never gated: nothing is held yet, so the
    # saturated actor's head job is admitted (and becomes the holder).
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    assert [row.actor for row in first] == ["sat"]
    holder = first[0]

    # The running saturated job now holds the bucket's only slot.
    table = backend.slot_table
    table.ensure_slots("bucket", 1)
    acquired = table.acquire("bucket", holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert acquired >= 0

    # Two saturated rows wait behind the full bucket beside a healthy
    # sibling; only the healthy rows may be claimed while it is full.
    await _enqueue(backend, actor="sat")
    await _enqueue(backend, actor="sat")
    await _enqueue(backend, actor="healthy")
    await _enqueue(backend, actor="healthy")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert [row.actor for row in claimed] == ["healthy", "healthy"]

    # Release re-opens the gate immediately: the saturated rows are claimed.
    assert table.release("bucket", acquired, backend._worker_id)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    claimed_after = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert [row.actor for row in claimed_after] == ["sat", "sat"]


async def test_expired_lease_does_not_gate() -> None:
    """An expired lease is acquirable capacity, matching the acquire
    path's free/held definition; it must not close the gate."""
    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    table = backend.slot_table
    table.ensure_slots("bucket", 1)
    table.acquire("bucket", holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    # The holder's worker vanishes; the lease lapses (the abandonment
    # signal the acquire path also treats as acquirable).
    clock: Clock = backend._clock  # pyright: ignore[reportPrivateUsage]  # Why: driving the injected clock is the twin's documented time control.
    assert isinstance(clock, FakeClock)
    clock.advance(timedelta(minutes=11))

    await _enqueue(backend, actor="sat")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert [row.actor for row in claimed] == ["sat"]


async def test_repended_row_is_gated_like_a_producer_placed_one() -> None:
    """The fold sits in the residual both routing arms read: a re-pended
    (assignment_routed) saturated row waits while the bucket is full, and
    is claimed when it frees — PG's repend_capacity fold mirrored."""
    from dataclasses import replace as _replace

    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    table = backend.slot_table
    table.ensure_slots("bucket", 1)
    acquired = table.acquire("bucket", holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    # A pending saturated row carrying the re-pend marker (the shape a
    # denied job returns in after the snooze/promote round trip).
    await _enqueue(backend, actor="sat")
    repended = next(
        row
        for row in backend._jobs.values()
        if row.status == "pending"  # pyright: ignore[reportPrivateUsage]  # Why: the twin's own store, adjusted the way the backend's own tests adjust it.
    )
    backend._jobs[repended.id] = _replace(repended, assignment_routed=True)  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    claimed = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert claimed == []

    assert table.release("bucket", acquired, backend._worker_id)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    claimed_after = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert [row.id for row in claimed_after] == [repended.id]


async def test_partial_headroom_bounds_admission() -> None:
    """Two slots, one live-held: the free slot admits at most
    headroom x oversample (1 x 2) further rows of the holder's actor per
    round — never the whole pending set."""
    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    table = backend.slot_table
    table.ensure_slots("bucket", 2)
    table.acquire("bucket", holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    for _ in range(5):
        await _enqueue(backend, actor="sat")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert 0 < len(claimed) <= 2


async def test_keyed_full_bucket_does_not_gate() -> None:
    """A keyed bucket held full leaves the actor's admission untouched
    (#242): the keyed mark on the slot rows excludes the bucket from the
    headroom fold, the per-key cap is the consumer's post-claim
    acquire's to enforce, where the payload-derived key is known."""
    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    table = backend.slot_table
    table.ensure_slots("session:a", 1, keyed=True)
    acquired = table.acquire(
        "session:a", holder.id, backend._worker_id, timedelta(minutes=10), _START
    )  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert acquired >= 0

    await _enqueue(backend, actor="sat")
    await _enqueue(backend, actor="sat")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert len(claimed) == 2, (
        "a keyed bucket held full gated the actor's rows, the keyed mark "
        "must exclude the bucket from the headroom fold (claim time cannot "
        "know which pending row needs which payload-derived key)"
    )


async def test_static_full_bucket_gates_across_queues() -> None:
    """The static fold is per ACTOR: a full static bucket gates the
    actor's claims on every queue, the complement of the queue-cap
    scoping pin below, and the behavior a static (non-keyed,
    non-queue-cap) reservation has always owed."""
    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    table = backend.slot_table
    table.ensure_slots("static-bucket", 1)
    table.acquire("static-bucket", holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    await _enqueue(backend, actor="sat", queue="q")
    await _enqueue(backend, actor="sat", queue="q2")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q", "q2"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert claimed == []


async def test_queue_cap_full_bucket_gates_only_its_queue() -> None:
    """A full queue-cap bucket gates the holder actor's claims on ITS
    queue only; the same actor's claims on another queue flow (#242,
    the twin of tests/test_dispatch_reservation_headroom.py's PG pin)."""
    backend = _make_backend()
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    cap_bucket = f"{QUEUE_CONCURRENCY_PREFIX}q"
    table = backend.slot_table
    table.ensure_slots(cap_bucket, 1)
    acquired = table.acquire(
        cap_bucket, holder.id, backend._worker_id, timedelta(minutes=10), _START
    )  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    await _enqueue(backend, actor="sat", queue="q")
    await _enqueue(backend, actor="sat", queue="q")
    await _enqueue(backend, actor="sat", queue="q2")
    await _enqueue(backend, actor="sat", queue="q2")
    claimed = await backend.dispatch_batch(backend._worker_id, ["q", "q2"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert {row.queue for row in claimed} == {"q2"}, (
        "a full queue-cap bucket on one queue gated the actor's claims on "
        "another queue, the queue cap binds per queue, so its fold must be "
        "scoped to the queue being probed"
    )

    assert table.release(cap_bucket, acquired, backend._worker_id)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    claimed_after = await backend.dispatch_batch(backend._worker_id, ["q", "q2"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert {row.queue for row in claimed_after} == {"q"}


async def test_queue_cap_fold_applies_in_round_robin_mode() -> None:
    """The round-robin arm carries the same per-queue queue-cap fold
    (PG: the RR candidates lateral's LEAST(residual, queue_cap_headroom)
    cohort LIMIT), so a full cap bucket gates its queue's claims under
    round_robin dispatch too."""
    from taskq.testing._runner import set_queue_mode

    backend = _make_backend()
    set_queue_mode(backend, "q", "round_robin")
    set_queue_mode(backend, "q2", "round_robin")
    _register(backend, "sat")
    await _enqueue(backend, actor="sat")
    first = await backend.dispatch_batch(backend._worker_id, ["q"], 1, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: the fixture worker identity, same seam the backend's own tests use.
    holder = first[0]

    cap_bucket = f"{QUEUE_CONCURRENCY_PREFIX}q"
    table = backend.slot_table
    table.ensure_slots(cap_bucket, 1)
    table.acquire(cap_bucket, holder.id, backend._worker_id, timedelta(minutes=10), _START)  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    # Rows on both queues (unkeyed rows share the "__null__ cohort,
    # the per-cohort windows are where the queue-cap bound rides).
    for queue in ("q", "q2"):
        await _enqueue(backend, actor="sat", queue=queue)
        await _enqueue(backend, actor="sat", queue=queue)
    claimed = await backend.dispatch_batch(backend._worker_id, ["q", "q2"], 10, _LOCK_LEASE)  # pyright: ignore[reportPrivateUsage]  # Why: see above.
    assert {row.queue for row in claimed} == {"q2"}, (
        "the round-robin arm's cohort windows ignored the queue-cap "
        "headroom fold, both dispatch variants must scope the queue-cap "
        "gate to the queue being probed"
    )

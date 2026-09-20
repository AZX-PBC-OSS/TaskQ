"""ATTACK tests: the twin's batch rollback contract under adversarial
interleaving (branch fix/twin-batch-atomic).

The contract under attack (stated on ``_enqueue_batch`` /
``_enqueue_with_conn`` / ``_rollback_inserted_rows``):

* A whole-call refusal is ATOMIC: no admitted item's row survives.
* The compensating rollback withdraws exactly the rows THIS call
  inserted - never a dedup hit's holder row, never an intruder task's
  concurrently committed row.
* The idempotency index ends consistent with ``_jobs``: no dangling
  pair→id entry (which would discount the cap preflight and
  over-admit).

Interleaving is created by hooking the backend's ``enqueue_with_conn``
seam (the only await boundary the batch loop crosses) to yield to the
event loop and run scripted intruder actions at chosen item boundaries -
the deterministic form of the scheduler lottery a concurrent hammer
would otherwise leave to chance.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Generator
from datetime import UTC, datetime
from typing import Any

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.exceptions import (
    BatchIdExistsError,
    BatchMaxPendingExceededError,
    IdempotencyKeyActorMismatchError,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2026, 1, 1, tzinfo=UTC)


def _args(
    *,
    actor: str = "a",
    jid: JobId | None = None,
    key: str | None = None,
    scope: str = "",
    tag: str | None = None,
    max_pending: int | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=JobId(jid) if jid is not None else new_job_id(),
        actor=actor,
        queue="default",
        payload={"v": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
        idempotency_key=key,  # type: ignore[arg-type]  # Why: IdempotencyKey is a NewType over str; runtime-transparent.
        idempotency_scope=scope,
        metadata={"tag": tag} if tag is not None else {},
        max_pending=max_pending,
    )


def assert_index_consistent(backend: InMemoryBackend, context: str) -> None:
    """The checked invariant: the idempotency index and ``_jobs`` agree.

    * Every index entry points at a live row whose (scope, key) is the
      entry's pair (no dangling pair→id).
    * Every live keyed row's pair is indexed back at that row (no
      shadowed pair, no pair held by two rows).
    """
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]  # Why: the attack reads the twin's storage directly; that IS the observable.
    idx = backend._idempotency_index  # pyright: ignore[reportPrivateUsage]
    for pair, jid in idx.items():
        row = jobs.get(jid)
        assert row is not None, (
            f"{context}: index entry {pair} dangles at {jid} (row withdrawn, entry left behind)"
        )
        assert (row.idempotency_scope, row.idempotency_key) == pair, (
            f"{context}: index entry {pair} points at {jid} whose pair is "
            f"({row.idempotency_scope}, {row.idempotency_key})"
        )
    seen: dict[tuple[str, str], JobId] = {}
    for row in jobs.values():
        if row.idempotency_key is None:
            continue
        pair = (row.idempotency_scope, row.idempotency_key)
        assert pair not in seen, (
            f"{context}: two live rows hold pair {pair}: {seen[pair]} and {row.id}"
        )
        seen[pair] = row.id
        assert idx.get(pair) == row.id, (
            f"{context}: live row {row.id} holds pair {pair} but the index points at {idx.get(pair)}"
        )


@contextlib.contextmanager
def _install_hook(
    backend: InMemoryBackend,
    *,
    script: Callable[[int, EnqueueArgs], Awaitable[None] | None] | None = None,
    observations: list[set[JobId]] | None = None,
    tag_of: Callable[[EnqueueArgs], str | None] | None = None,
) -> Generator[dict[str, int], None, None]:
    """Yield to the event loop at every insert seam; run the scripted
    intruder action before chosen inserts; optionally snapshot the ids
    present between items.

    Hooks the MODULE-LEVEL ``_enqueue`` (both batch arms funnel through
    it on every commit shape), not the instance method: the pushed
    branch's ``_enqueue_batch`` loops over ``self.enqueue_with_conn``,
    the parent loops over the module function directly - the module seam
    intercepts both.
    """
    import taskq.testing._enqueue as enqueue_mod

    original = enqueue_mod._enqueue
    state = {"n": 0}

    async def hooked(self: InMemoryBackend, args: EnqueueArgs) -> Any:
        state["n"] += 1
        await asyncio.sleep(0)
        if observations is not None and tag_of is not None:
            tag = tag_of(args)
            if tag is not None:
                observations.append(
                    {
                        jid
                        for jid, row in backend._jobs.items()  # pyright: ignore[reportPrivateUsage]
                        if row.metadata.get("tag") == tag
                    }
                )
        if script is not None:
            action = script(state["n"], args)
            if action is not None:
                await action
        return await original(self, args)

    enqueue_mod._enqueue = hooked  # type: ignore[assignment]  # Why: the attack needs a deterministic interleaving point at the insert seam.
    try:
        yield state
    finally:
        enqueue_mod._enqueue = original  # type: ignore[assignment]


# ── Attack 1: the intruder's committed rows must survive the rollback ───


async def test_rollback_withdraws_exactly_own_rows_intruder_survives() -> None:
    """A same-actor intruder pre-stores the batch's pair mid-loop and a
    second intruder takes a later item's job id, poisoning it. The
    rollback must withdraw exactly the batch's own inserted rows, leave
    both intruder rows stored, and end index-consistent."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    k0, k1, k2, k3 = new_job_id(), new_job_id(), new_job_id(), new_job_id()
    intruder_pair_row = None
    intruder_id_row = None

    async def intruder_store_pair() -> None:
        nonlocal intruder_pair_row
        intruder_pair_row = await backend.enqueue(_args(key="P", tag="intruder-pair"))

    async def intruder_take_id() -> None:
        nonlocal intruder_id_row
        intruder_id_row = await backend.enqueue(_args(jid=k3, tag="intruder-id"))

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        if n == 2:  # before item k1 (pair P) inserts
            return intruder_store_pair()
        if n == 4:  # before item k3 inserts: the intruder takes its id
            return intruder_take_id()
        return None

    with _install_hook(backend, script=script):
        items = [
            _args(jid=k0, tag="batch"),
            _args(jid=k1, key="P", tag="batch"),
            _args(jid=k2, key="Q", tag="batch"),
            _args(jid=k3, tag="batch"),
        ]
        from asyncpg.exceptions import UniqueViolationError

        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(items)

    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    # Exactly the call's own rows are withdrawn (k1 was a dedup hit: it
    # never stored, so there is nothing of it to withdraw). The
    # intruder's row that took k3's id survives - it is a committed row
    # of another call.
    assert all(jid not in jobs for jid in (k0, k1, k2)), (
        "the rollback withdrew a row the call did not insert, or left one of its own behind"
    )
    assert k3 in jobs
    assert intruder_pair_row is not None and intruder_pair_row.id in jobs
    assert intruder_id_row is not None and jobs[k3].id == intruder_id_row.id
    # The index ends consistent with _jobs.
    idx = backend._idempotency_index  # pyright: ignore[reportPrivateUsage]
    assert idx.get(("", "P")) == intruder_pair_row.id
    assert ("", "Q") not in idx
    assert_index_consistent(backend, "after poisoned-batch rollback")


async def test_mid_batch_state_is_always_a_prefix_of_the_call() -> None:
    """Poll _jobs at every insert seam while the batch is in flight: the
    observable rows of the in-flight call are always a prefix of its
    items (never a gap or a suffix), and the post-refusal state is
    all-or-nothing."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    ids = [new_job_id() for _ in range(4)]
    observed: list[set[JobId]] = []

    def tag_of(args: EnqueueArgs) -> str | None:
        return "batch" if args.metadata.get("tag") == "batch" else None

    k3 = ids[3]

    async def intruder_take_id() -> None:
        await backend.enqueue(_args(jid=k3, tag="intruder"))

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        return intruder_take_id() if n == 4 else None

    with _install_hook(backend, script=script, observations=observed, tag_of=tag_of):
        items = [_args(jid=jid, tag="batch") for jid in ids]
        from asyncpg.exceptions import UniqueViolationError

        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(items)

    # Every observation is a prefix of the batch's own id list.
    expected: list[JobId] = []
    for snapshot in observed:
        assert snapshot == set(expected), (
            f"mid-batch observation was not a prefix: saw {snapshot}, expected {set(expected)}"
        )
        expected.append(ids[len(expected)])
    assert len(observed) == 4, f"expected one observation per item, got {len(observed)}"
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    # Post-refusal: none of the call's own rows remain; the intruder's
    # row (it took ids[3]) survives.
    assert all(jid not in jobs for jid in ids[:3])
    assert ids[3] in jobs


# ── Attack 2: a cross-actor intruder landing mid-loop ────────────────────


async def test_cross_actor_intruder_mid_loop_refusal_is_atomic() -> None:
    """The preflight cannot see a cross-actor holder that lands after it:
    the insert loop must raise the typed mismatch at the offending index
    and the rollback must withdraw the clean prefix - nothing from the
    call survives, the error names the intruder's row."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    k0, k1 = new_job_id(), new_job_id()
    intruder_row = None

    async def intruder_store_pair() -> None:
        nonlocal intruder_row
        intruder_row = await backend.enqueue(_args(actor="other", key="P", tag="intruder"))

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        return intruder_store_pair() if n == 2 else None

    with (
        _install_hook(backend, script=script),
        pytest.raises(IdempotencyKeyActorMismatchError) as exc_info,
    ):
        await backend.enqueue_batch(
            [
                _args(jid=k0, tag="batch"),
                _args(jid=k1, key="P", tag="batch"),
            ]
        )

    err = exc_info.value
    assert intruder_row is not None
    assert err.actor == "a"
    assert err.existing_actor == "other"
    assert err.existing_job_id == intruder_row.id
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    assert k0 not in jobs, "the clean prefix leaked: the refusal was not atomic"
    assert k1 not in jobs
    assert intruder_row.id in jobs
    assert_index_consistent(backend, "after cross-actor mid-loop refusal")


# ── Attack 3: the dangling-index scenario the fix claims to close ────────


async def test_keyed_rollback_does_not_over_admit_the_cap() -> None:
    """The dangling-index payoff: after a keyed batch rolls back, its
    pair must NOT discount the cap preflight. With the pair dangling, a
    capped batch mixing the rolled-back pair with a fresh pair is
    discounted by the phantom, refuses nobody, and stores one row per
    item anyway - silently admitting the actor past its cap."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    k0, poison = new_job_id(), new_job_id()

    async def intruder_take_id() -> None:
        # A different actor: the poison row must not count toward
        # actor a's cap.
        await backend.enqueue(_args(actor="poison-actor", jid=poison, tag="intruder"))

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        return intruder_take_id() if n == 2 else None

    with _install_hook(backend, script=script):
        from asyncpg.exceptions import UniqueViolationError

        with pytest.raises(UniqueViolationError):
            await backend.enqueue_batch(
                [_args(jid=k0, key="P", tag="b1"), _args(jid=poison, tag="b1")]
            )
        assert_index_consistent(backend, "after keyed rollback")

    # Cap 1 for actor a. The rolled-back pair P must not discount the
    # preflight: two fresh-admission items over the cap are refused as a
    # group and nothing is stored.
    with pytest.raises(BatchMaxPendingExceededError):
        await backend.enqueue_batch(
            [
                _args(key="P", tag="b2", max_pending=1),
                _args(key="Q", tag="b2", max_pending=1),
            ]
        )
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    assert not [jid for jid, row in jobs.items() if row.metadata.get("tag") == "b2"], (
        "the refused batch stored rows anyway: the dangling index "
        "discounted the cap preflight into over-admission"
    )
    assert_index_consistent(backend, "after capped retry")


# ── Attack 4: the atomic arm's rollback (batch row + finalizer) ─────────


async def test_atomic_arm_rollback_covers_finalizer_and_batch_row() -> None:
    """A pre-existing batch id fails ``_create_batch`` AFTER every item
    and the finalizer inserted: the rollback withdraws all of them (the
    finalizer's in-batch dedup alias included) and leaves the pre-existing
    batch row untouched."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    bid = new_uuid()
    # A pre-existing batch row the call collides with.
    await backend.create_batch(
        bid,
        queue="default",
        expected_size=1,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
    )
    # The batch_row the caller carries (its metadata feeds _create_batch).
    batch_row = await backend.get_batch(bid)

    item_ids = [new_job_id() for _ in range(3)]
    finalizer_id = new_job_id()
    items = [_args(jid=jid, key=f"k{i}", tag="atomic") for i, jid in enumerate(item_ids)]
    finalizer = _args(jid=finalizer_id, key="k2", tag="atomic-finalizer")

    with pytest.raises(BatchIdExistsError):
        await backend.enqueue_batch_atomic(
            iter(items),
            batch_id=bid,
            queue="default",
            batch_row=batch_row,
            finalizer_args=finalizer,
        )

    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    assert not [
        jid
        for jid, row in jobs.items()
        if row.metadata.get("tag") in ("atomic", "atomic-finalizer")
    ], "the atomic arm left rows behind after the batch-row refusal"
    assert finalizer_id not in jobs
    assert_index_consistent(backend, "after atomic-arm batch-row refusal")


# ── Attack 5: the concurrent hammer ─────────────────────────────────────


async def test_concurrent_hammer_overlapping_keyed_batches() -> None:
    """Many tasks enqueueing overlapping keyed batches and keyed singles
    while batches are in flight. Every batch is either fully stored or
    fully withdrawn, the index ends consistent with _jobs, and every
    cross-actor refusal withdraws the refusing call's own rows."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    n_tasks = 12
    keys = [f"key{i}" for i in range(4)]
    # Per-task outcome bookkeeping: (batch refused?, the ids the task's
    # calls returned). The hammer's contract: a REFUSED batch leaves zero
    # of its own inserted rows, the index ends consistent, and every id
    # any call RETURNED resolves to a live row - a completed call must
    # never hand back an id its own rollback later withdraws.
    outcomes: dict[int, tuple[bool, list[JobId]]] = {}

    async def one_task(t: int) -> None:
        actor = "even" if t % 2 == 0 else "odd"
        batch = [_args(actor=actor, key=keys[(t + i) % len(keys)], tag=f"t{t}") for i in range(5)]
        returned: list[JobId] = []
        single = await backend.enqueue(_args(actor=actor, key=keys[t % len(keys)], tag=f"s{t}"))
        returned.append(single.id)
        refused = False
        try:
            rows = await backend.enqueue_batch(batch)
            returned.extend(row.id for row in rows)
        except IdempotencyKeyActorMismatchError:
            refused = True  # whole-call refusal: its own rows must all be gone
        outcomes[t] = (refused, returned)

    with _install_hook(backend):  # yield at every insert seam: maximum interleaving
        await asyncio.gather(*(one_task(t) for t in range(n_tasks)))

    assert_index_consistent(backend, "after hammer")
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    for t, (refused, returned) in outcomes.items():
        batch_tag_rows = sum(1 for row in jobs.values() if row.metadata.get("tag") == f"t{t}")
        if refused:
            assert batch_tag_rows == 0, (
                f"task {t}'s batch was refused as a whole call yet {batch_tag_rows} "
                "of its own inserted rows survived: the refusal was not atomic"
            )
        else:
            assert batch_tag_rows <= 5
        # A COMPLETED call's returned ids must stay resolvable: a dedup
        # hit that aliases a row another task's rollback withdraws is a
        # dangling handle no PG caller can observe (PG's unique-index
        # wait means the aliasing call cannot complete before the holder
        # commits or aborts).
        dangling = [jid for jid in returned if jid not in jobs]
        assert not dangling, (
            f"task {t} completed with returned ids that no longer resolve: {dangling}"
        )


# ── Attack 6: cap admission through the fast tier ────────────────────────


async def test_batch_fast_cap_refusal_keeps_admitted_rows_and_index() -> None:
    """The fast tier's typed cap refusal raises AFTER the admitted rows
    are stored (PG parity: the COPY commits, then the refusal). The
    admitted rows survive, refused actors' items are absent, and the
    index stays consistent."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    with _install_hook(backend), pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await backend.enqueue_batch_fast(
            [
                _args(actor="a", key="p1", tag="refused", max_pending=1),
                _args(actor="a", key="p2", tag="refused", max_pending=1),
                _args(actor="b", key="p3", tag="admitted", max_pending=100),
            ]
        )
    err = exc_info.value
    assert err.refused_indices == {"a": [0, 1]}
    assert err.admitted_count == 1
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    refused = [jid for jid, row in jobs.items() if row.metadata.get("tag") == "refused"]
    assert refused == [], "the refused actor's items were stored"
    admitted = [jid for jid, row in jobs.items() if row.metadata.get("tag") == "admitted"]
    assert len(admitted) == 1
    assert_index_consistent(backend, "after fast-tier cap refusal")


# ── Attack 7: the dedup-alias-onto-a-doomed-row window ───────────────────


async def test_dedup_alias_onto_in_flight_batch_row_dangles_after_rollback() -> None:
    """DEMONSTRATION (model boundary, not a pin of the refusal contract).

    Task A's batch stores a keyed row, yields; task B's same-actor item
    dedups onto it and COMPLETES (returning A's row id); A's batch then
    hits a cross-actor-held key and rolls back - withdrawing the row B
    already holds a handle to. The index ends consistent and the refusal
    is atomic, but B's completed call returned an id that no longer
    resolves.

    On PG this interleaving cannot complete that way: B's INSERT blocks
    on the uncommitted unique-index entry A's transaction created, A's
    abort releases it, and B inserts its OWN row - B's returned id always
    resolves. The twin cannot block a cooperative task at the dedup
    read, so the divergence is inherent to the in-memory model: the
    single-threaded-by-contract backend has no isolation between tasks,
    and no small rollback change can retract a handle another task
    already returned. Recorded here so the boundary is executed and
    dated, not speculative.
    """
    backend = InMemoryBackend(clock=FakeClock(_START))
    alias_returned: list[JobId] = []

    async def task_a() -> None:
        # item0: fresh key P (first writer). item1: cross-actor-held
        # key Q -> the whole-call refusal, rolling back item0.
        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch(
                [
                    _args(actor="a", key="P", tag="a0"),
                    _args(actor="a", key="Q", tag="a1"),
                ]
            )

    async def task_b() -> None:
        # Runs its keyed single only after A's item0 is stored (the
        # yield hook inside task A's item1 insert wakes this task).
        await started.wait()
        row = await backend.enqueue(_args(actor="a", key="P", tag="b"))
        alias_returned.append(row.id)

    started = asyncio.Event()

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        async def poison_and_release() -> None:
            # Land the cross-actor holder for key Q HERE (after the
            # batch's preflight, before item1's insert) so the refusal
            # is the mid-loop insert-time one, then wake task B.
            await backend.enqueue(_args(actor="other", key="Q", tag="holder"))
            started.set()
            await asyncio.sleep(0)  # hand control to task B, parked on started

        # n==2: task A's item0 (pair P) is stored; before item1's insert.
        return poison_and_release() if n == 2 else None

    async def runner() -> None:
        with _install_hook(backend, script=script):
            await asyncio.gather(task_a(), task_b())

    await runner()

    # The refusal was atomic and the index consistent - the pinned
    # contract held.
    assert_index_consistent(backend, "after alias-then-rollback")
    jobs = backend._jobs  # pyright: ignore[reportPrivateUsage]
    assert not [
        jid for jid, row in jobs.items() if str(row.metadata.get("tag", "")).startswith("a")
    ], "the refusing batch's own rows survived"
    holder = next(row for row in jobs.values() if row.metadata.get("tag") == "holder")
    assert holder.id in jobs, "the mid-loop intruder's committed row did not survive"
    # The demonstration: task B completed holding A's doomed row id.
    assert alias_returned, "task B never ran: schedule broken"
    assert alias_returned[0] not in jobs, (
        "schedule did not produce the alias window; adjust the yield points"
    )

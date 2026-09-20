"""ATTACK tests: the twin's batch rollback contract under adversarial
interleaving (branch fix/twin-batch-atomic).

The contract under attack (stated on ``enqueue_batch`` /
``enqueue_with_conn`` / the rollback), asserted through PUBLIC surfaces
only - ``get``, ``list_jobs``, and what enqueue/batch calls return and
raise; never a peek at the twin's storage dicts:

* A whole-call refusal is ATOMIC: no admitted item's row survives; a
  mid-batch observation sees only a prefix of the call's items.
* The compensating rollback withdraws exactly the rows THIS call
  inserted - never a dedup hit's holder row, never an intruder task's
  concurrently committed row.
* The dedup state ends consistent with the live rows: a re-enqueue of
  any exercised pair resolves to the pair's one live holder (a
  dangling entry would resolve to a phantom or store a second row).

Interleaving is created by hooking the module enqueue seam (the only
await boundary the batch loop crosses) to yield to the event loop and
run scripted intruder actions at chosen item boundaries - the
deterministic form of the scheduler lottery a concurrent hammer would
otherwise leave to chance.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Generator
from datetime import UTC, datetime
from typing import Any

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobFilter, JobId
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


async def none_stored(backend: InMemoryBackend, ids: list[JobId]) -> bool:
    """True when NO id in ``ids`` resolves through the public read."""
    return all([await backend.get(jid) is None for jid in ids])


async def stored_ids(backend: InMemoryBackend) -> set[JobId]:
    """The ids every public read resolves, the observable stored set."""
    rows = await backend.list_jobs(JobFilter(limit=10_000))
    return {row.id for row in rows}


async def assert_dedup_contract(backend: InMemoryBackend, context: str) -> None:
    """The behavioral dedup invariant, checked through public calls.

    Every live keyed row is the UNIQUE dedup target of its pair: a
    same-actor re-enqueue of the pair returns THAT row's id, the
    returned handle still resolves, and the probe stores nothing new.
    A dangling index entry (the pre-fix defect) makes the probe resolve
    to a phantom or write a second row for the pair - both caught here.
    """
    before = await stored_ids(backend)
    seen: set[tuple[str, str]] = set()
    for row in await backend.list_jobs(JobFilter(limit=10_000)):
        if row.idempotency_key is None:
            continue
        pair = (row.idempotency_scope, row.idempotency_key)
        assert pair not in seen, (
            f"{context}: two live rows hold pair {pair} - dedup admitted a second write"
        )
        seen.add(pair)
        probe = await backend.enqueue(
            _args(actor=row.actor, key=row.idempotency_key, scope=row.idempotency_scope)
        )
        assert probe.id == row.id, (
            f"{context}: re-enqueue of pair {pair} returned {probe.id}, not the "
            f"live holder {row.id} - the dedup state diverged from the live rows"
        )
        assert await backend.get(probe.id) is not None, (
            f"{context}: the pair's dedup target no longer resolves"
        )
    after = await stored_ids(backend)
    assert after == before, f"{context}: the dedup probes stored rows ({after - before})"


@contextlib.contextmanager
def _install_hook(
    backend: InMemoryBackend,
    *,
    script: Callable[[int, EnqueueArgs], Awaitable[None] | None] | None = None,
    observations: list[set[JobId]] | None = None,
    watch_ids: list[JobId] | None = None,
) -> Generator[dict[str, int], None, None]:
    """Yield to the event loop at every insert seam; run the scripted
    intruder action before chosen inserts; optionally snapshot which of
    ``watch_ids`` are stored between items (via public ``get``).

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
        if observations is not None and watch_ids is not None:
            observations.append({jid for jid in watch_ids if await backend.get(jid) is not None})
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
    both intruder rows stored, and end dedup-consistent."""
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

    # Exactly the call's own rows are withdrawn (k1 was a dedup hit: it
    # never stored, so there is nothing of it to withdraw). The
    # intruder's row that took k3's id survives - it is a committed row
    # of another call.
    assert await none_stored(backend, [k0, k1, k2]), (
        "the rollback withdrew a row the call did not insert, or left one of its own behind"
    )
    assert await backend.get(k3) is not None
    assert intruder_pair_row is not None and await backend.get(intruder_pair_row.id) is not None
    assert intruder_id_row is not None
    # The pair P resolves to the intruder's committed row, not a phantom.
    probe = await backend.enqueue(_args(key="P"))
    assert probe.id == intruder_pair_row.id
    # The rolled-back pair Q is gone: a fresh enqueue of it writes a NEW
    # resolvable row (and dedup never aliases onto the withdrawn call).
    probe_q = await backend.enqueue(_args(key="Q"))
    assert await backend.get(probe_q.id) is not None
    await assert_dedup_contract(backend, "after poisoned-batch rollback")


async def test_mid_batch_state_is_always_a_prefix_of_the_call() -> None:
    """Poll the stored set at every insert seam while the batch is in
    flight: the observable rows of the in-flight call are always a
    prefix of its items (never a gap or a suffix), and the post-refusal
    state is all-or-nothing."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    ids = [new_job_id() for _ in range(4)]
    observed: list[set[JobId]] = []

    k3 = ids[3]

    async def intruder_take_id() -> None:
        await backend.enqueue(_args(jid=k3, tag="intruder"))

    def script(n: int, args: EnqueueArgs) -> Awaitable[None] | None:
        return intruder_take_id() if n == 4 else None

    with _install_hook(backend, script=script, observations=observed, watch_ids=ids):
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
    # Post-refusal: none of the call's own rows remain; the intruder's
    # row (it took ids[3]) survives.
    assert await none_stored(backend, ids[:3])
    assert await backend.get(ids[3]) is not None


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
    assert await backend.get(k0) is None, "the clean prefix leaked: the refusal was not atomic"
    assert await backend.get(k1) is None
    assert await backend.get(intruder_row.id) is not None
    await assert_dedup_contract(backend, "after cross-actor mid-loop refusal")


# ── Attack 3: the dangling-index scenario the fix claims to close ────────


async def test_keyed_rollback_does_not_over_admit_the_cap() -> None:
    """The cap preflight discounts an already-stored (or in-batch
    repeated) pair from the cap count, because the pair dedups instead
    of writing. That discount is only sound while the rollback pops the
    rolled-back pair's index entry: a discount that survives its row
    (a dangling entry) makes a later capped batch mixing the rolled-back
    pair with a fresh pair under-count its admission, refuse nobody, and
    store one row per item anyway - silently admitting the actor past
    its cap. The discount and the pop must stand or fall together; this
    pin fails if either is broken alone."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    k0, poison, b2a, b2b = new_job_id(), new_job_id(), new_job_id(), new_job_id()

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
        await assert_dedup_contract(backend, "after keyed rollback")

        # Cap 1 for actor a. The rolled-back pair P must not discount the
        # preflight: two fresh-admission items over the cap are refused
        # as a group and nothing is stored.
        with pytest.raises(BatchMaxPendingExceededError):
            await backend.enqueue_batch(
                [
                    _args(jid=b2a, key="P", tag="b2", max_pending=1),
                    _args(jid=b2b, key="Q", tag="b2", max_pending=1),
                ]
            )
    assert await backend.get(b2a) is None and await backend.get(b2b) is None, (
        "the refused batch stored rows anyway: the dangling index "
        "discounted the cap preflight into over-admission"
    )
    await assert_dedup_contract(backend, "after capped retry")


# ── Attack 4: the atomic arm's rollback (batch row + finalizer) ─────────


async def test_atomic_arm_rollback_covers_finalizer_and_batch_row() -> None:
    """A pre-existing batch id fails the batch-row write AFTER every item
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
    # The batch_row the caller carries (its metadata feeds the batch write).
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

    assert await none_stored(backend, [*item_ids, finalizer_id]), (
        "the atomic arm left rows behind after the batch-row refusal"
    )
    assert await backend.get_batch(bid) is not None
    await assert_dedup_contract(backend, "after atomic-arm batch-row refusal")


# ── Attack 5: the concurrent hammer ─────────────────────────────────────


async def test_concurrent_hammer_overlapping_keyed_batches() -> None:
    """Many tasks enqueueing overlapping keyed batches and keyed singles
    while batches are in flight. Every refused batch withdraws ALL of its
    own inserted rows, every completed call's returned ids stay
    resolvable, and the dedup state ends consistent with the live rows."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    n_tasks = 12
    keys = [f"key{i}" for i in range(4)]
    # Per-task outcome bookkeeping: (batch refused?, the batch's own item
    # ids, the ids the task's calls RETURNED).
    outcomes: dict[int, tuple[bool, list[JobId], list[JobId]]] = {}

    async def one_task(t: int) -> None:
        actor = "even" if t % 2 == 0 else "odd"
        item_ids = [new_job_id() for _ in range(5)]
        batch = [
            _args(jid=jid, actor=actor, key=keys[(t + i) % len(keys)], tag=f"t{t}")
            for i, jid in enumerate(item_ids)
        ]
        returned: list[JobId] = []
        single = await backend.enqueue(_args(actor=actor, key=keys[t % len(keys)], tag=f"s{t}"))
        returned.append(single.id)
        refused = False
        try:
            rows = await backend.enqueue_batch(batch)
            returned.extend(row.id for row in rows)
        except IdempotencyKeyActorMismatchError:
            refused = True  # whole-call refusal: its own rows must all be gone
        outcomes[t] = (refused, item_ids, returned)

    with _install_hook(backend):  # yield at every insert seam: maximum interleaving
        await asyncio.gather(*(one_task(t) for t in range(n_tasks)))

    await assert_dedup_contract(backend, "after hammer")
    for t, (refused, item_ids, returned) in outcomes.items():
        if refused:
            stored_own = [jid for jid in item_ids if await backend.get(jid) is not None]
            assert not stored_own, (
                f"task {t}'s batch was refused as a whole call yet its own rows "
                f"{stored_own} survived: the refusal was not atomic"
            )
        # A COMPLETED call's returned ids must stay resolvable: a dedup
        # hit that aliases a row another task's rollback withdraws is a
        # dangling handle no PG caller can observe (PG's unique-index
        # wait means the aliasing call cannot complete before the holder
        # commits or aborts).
        dangling = [jid for jid in returned if await backend.get(jid) is None]
        assert not dangling, (
            f"task {t} completed with returned ids that no longer resolve: {dangling}"
        )


# ── Attack 6: cap admission through the fast tier ────────────────────────


async def test_batch_fast_cap_refusal_keeps_admitted_rows_and_index() -> None:
    """The fast tier's typed cap refusal raises AFTER the admitted rows
    are stored (PG parity: the COPY commits, then the refusal). The
    admitted rows survive, refused actors' items are absent, and the
    dedup state stays consistent."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    r1, r2, adm = new_job_id(), new_job_id(), new_job_id()

    with _install_hook(backend), pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await backend.enqueue_batch_fast(
            [
                _args(jid=r1, actor="a", key="p1", tag="refused", max_pending=1),
                _args(jid=r2, actor="a", key="p2", tag="refused", max_pending=1),
                _args(jid=adm, actor="b", key="p3", tag="admitted", max_pending=100),
            ]
        )
    err = exc_info.value
    assert err.refused_indices == {"a": [0, 1]}
    assert err.admitted_count == 1
    assert await none_stored(backend, [r1, r2]), "the refused actor's items were stored"
    assert await backend.get(adm) is not None
    await assert_dedup_contract(backend, "after fast-tier cap refusal")


# ── Attack 7: the dedup-alias-onto-a-doomed-row window ───────────────────


async def test_dedup_alias_onto_in_flight_batch_row_dangles_after_rollback() -> None:
    """DEMONSTRATION (model boundary, not a pin of the refusal contract).

    Task A's batch stores a keyed row, yields; task B's same-actor item
    dedups onto it and COMPLETES (returning A's row id); A's batch then
    hits a cross-actor-held key and rolls back - withdrawing the row B
    already holds a handle to. The refusal stays atomic and the dedup
    state consistent, but B's completed call returned an id that no
    longer resolves.

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
    a0, a1 = new_job_id(), new_job_id()

    async def task_a() -> None:
        # item0: fresh key P (first writer). item1: cross-actor-held
        # key Q -> the whole-call refusal, rolling back item0.
        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch(
                [
                    _args(jid=a0, actor="a", key="P", tag="a0"),
                    _args(jid=a1, actor="a", key="Q", tag="a1"),
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

    # The refusal was atomic and the dedup state consistent - the pinned
    # contract held.
    await assert_dedup_contract(backend, "after alias-then-rollback")
    assert await none_stored(backend, [a0, a1]), "the refusing batch's own rows survived"
    holder_rows = await backend.list_jobs(JobFilter(actor="other", limit=10_000))
    assert len(holder_rows) == 1, "the mid-loop intruder's committed row did not survive"
    # The demonstration: task B completed holding A's doomed row id.
    assert alias_returned, "task B never ran: schedule broken"
    assert await backend.get(alias_returned[0]) is None, (
        "schedule did not produce the alias window; adjust the yield points"
    )

"""Tests for InMemoryBackend idempotency_scope handling.

Covers:
- Regression: unscoped idempotency_key (default "" or explicit "") still
  dedupes globally exactly as before.
- Same idempotency_key with DIFFERENT idempotency_scope values → both
  enqueue as distinct jobs.
- Same idempotency_key AND same idempotency_scope → dedupes (second
  returns first job).
"""

import asyncio
from datetime import UTC, datetime

import pytest

from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.exceptions import IdempotencyKeyActorMismatchError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


# ── Regression: unscoped key still dedupes globally ────────────


class TestUnscopedKeyDedupesGlobally:
    """idempotency_key with default scope ("" or explicit "") dedupes
    exactly as before the idempotency_scope feature was added."""

    async def test_default_scope_dedupes(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_explicit_empty_scope_dedupes(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", idempotency_scope="", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", idempotency_scope="", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_default_scope_equals_explicit_empty(self) -> None:
        """Default scope ("") and explicit "" are the same scope."""
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", idempotency_scope="", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_unscoped_dedup_preserves_original_payload(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", payload={"v": 1}, scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", payload={"v": 2}, scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row2.id == row1.id
        assert row2.payload == {"v": 1}


# ── Same key, different scope → distinct jobs ──────────────────


class TestSameKeyDifferentScope:
    """Same idempotency_key with different idempotency_scope values
    should both enqueue as distinct jobs."""

    async def test_different_scopes_create_distinct_jobs(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
        )
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="run-B", scheduled_at=_START
        )
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id

    async def test_scoped_key_vs_unscoped_key_distinct(self) -> None:
        """A scoped key and an unscoped (default "") key with the same
        idempotency_key value are distinct jobs."""
        backend = _make_backend()

        args1 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
        )
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id

    async def test_three_scopes_all_distinct(self) -> None:
        backend = _make_backend()

        ids: set[str] = set()
        for scope in ("run-A", "run-B", "run-C"):
            args = make_enqueue_args(
                idempotency_key="k1", idempotency_scope=scope, scheduled_at=_START
            )
            row = await backend.enqueue(args)
            ids.add(str(row.id))

        assert len(ids) == 3


# ── Batch parity: scope semantics hold through enqueue_batch ────


class TestEnqueueBatchScopeParity:
    """InMemoryBackend.enqueue_batch delegates to the single-enqueue path
    per item; scope semantics must match the single-enqueue behavior and
    the Postgres batch path (tests/test_postgres_batch_scope_collision.py)."""

    async def test_batch_cross_scope_same_key_all_distinct(self) -> None:
        backend = _make_backend()

        rows = await backend.enqueue_batch(
            [
                make_enqueue_args(
                    idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
                ),
                make_enqueue_args(
                    idempotency_key="k1", idempotency_scope="run-B", scheduled_at=_START
                ),
                make_enqueue_args(idempotency_key="k1", scheduled_at=_START),
            ]
        )

        assert len({str(r.id) for r in rows}) == 3

    async def test_batch_same_scope_same_key_dedupes(self) -> None:
        backend = _make_backend()

        rows = await backend.enqueue_batch(
            [
                make_enqueue_args(
                    idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
                ),
                make_enqueue_args(
                    idempotency_key="k1",
                    idempotency_scope="run-A",
                    payload={"second": True},
                    scheduled_at=_START,
                ),
            ]
        )

        assert len(rows) == 2
        assert rows[0].id == rows[1].id


class TestInBatchCrossActorDuplicateRefusedAtomically:
    """Two items of ONE enqueue_batch call sharing an
    (idempotency_scope, idempotency_key) pair across actors.

    Postgres' bulk tier is one INSERT in one transaction: the second item's
    pair conflicts inside the statement, the dedup resolution finds the
    first item's job, and the cross-actor refusal withdraws the whole
    INSERT. Nothing from the batch is stored. The mirror's preflight must
    refuse with the identical typed error BEFORE its first insert, never
    store the good prefix and raise at the offending item's index.
    """

    async def test_cross_actor_in_batch_duplicate_refuses_whole_batch(self) -> None:
        backend = _make_backend()

        item_a = make_enqueue_args(actor="actor-a", idempotency_key="k1", scheduled_at=_START)
        item_b = make_enqueue_args(actor="actor-b", idempotency_key="k1", scheduled_at=_START)

        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch([item_a, item_b])

        assert await backend.get(item_a.id) is None, "the refused batch must leave no rows behind"
        assert await backend.get(item_b.id) is None

    async def test_cross_actor_in_batch_duplicate_names_both_actors_in_call_order(self) -> None:
        backend = _make_backend()

        item_a = make_enqueue_args(actor="actor-a", idempotency_key="k1", scheduled_at=_START)
        item_b = make_enqueue_args(actor="actor-b", idempotency_key="k1", scheduled_at=_START)

        with pytest.raises(IdempotencyKeyActorMismatchError) as excinfo:
            await backend.enqueue_batch([item_a, item_b])

        err = excinfo.value
        assert err.actor == "actor-b"
        assert err.existing_actor == "actor-a"
        assert err.idempotency_key == "k1"

    async def test_cross_actor_in_batch_duplicate_after_distinct_items_refuses_whole_batch(
        self,
    ) -> None:
        """A clean item before the colliding pair is withdrawn with the
        refusal too: the refusal is whole-call, not a prefix trim."""
        backend = _make_backend()

        clean = make_enqueue_args(idempotency_key="clean", scheduled_at=_START)
        item_a = make_enqueue_args(actor="actor-a", idempotency_key="k1", scheduled_at=_START)
        item_b = make_enqueue_args(actor="actor-b", idempotency_key="k1", scheduled_at=_START)

        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch([clean, item_a, item_b])

        assert await backend.get(clean.id) is None
        assert await backend.get(item_a.id) is None
        assert await backend.get(item_b.id) is None


class TestConcurrentPairMidBatchRefusesAtomically:
    """A conflicting (idempotency_scope, idempotency_key) pair stored by a
    CONCURRENT task between the batch preflight and an item's insert is
    the same cross-actor defect the preflight refuses, so the refusal must
    be whole-call here too: the mismatch may surface at the offending
    item's index, but the good prefix is withdrawn with it and no item
    from the call survives.

    The concurrent writer is a plain ``asyncio.create_task``. Its
    interleaving is made deterministic by the backend double below: the
    twin's per-item enqueue is an ``await`` a real scheduler can
    interleave into, so the double takes that suspension once per item
    (one ``sleep(0)`` slice) and flips the event the task waits on the
    moment the batch's first item is stored - after the preflight, before
    item two's insert resolves its pair. Postgres needs no such seam -
    its batch is one statement inside one transaction, so a committed
    conflicting row conflicts inside that statement and aborts it - and
    the twin's observable contract must not depend on its loop never
    suspending.
    """

    async def test_concurrent_pair_mid_batch_refuses_whole_batch(self) -> None:
        clean = make_enqueue_args(actor="batch-actor", scheduled_at=_START)
        keyed = make_enqueue_args(actor="batch-actor", idempotency_key="k2", scheduled_at=_START)
        first_item_stored = asyncio.Event()
        stored_by_intruder: list[JobRow] = []

        class _InterleavingBackend(InMemoryBackend):
            async def enqueue_with_conn(self, conn: object, args: EnqueueArgs) -> JobRow:
                await asyncio.sleep(0)
                row = await super().enqueue_with_conn(conn, args)
                if args.id == clean.id:
                    first_item_stored.set()
                return row

        backend = _InterleavingBackend(clock=FakeClock(_START))

        async def intruder() -> None:
            await first_item_stored.wait()
            stored_by_intruder.append(
                await backend.enqueue(
                    make_enqueue_args(
                        actor="intruder-actor", idempotency_key="k2", scheduled_at=_START
                    )
                )
            )

        intruder_task = asyncio.create_task(intruder())
        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch([clean, keyed])
        await intruder_task

        assert await backend.get(clean.id) is None, (
            "the refused batch must leave no rows behind, even when the "
            "conflicting pair landed between the preflight and the insert"
        )
        assert await backend.get(keyed.id) is None
        # The concurrent call is its own committed enqueue: PG's rollback
        # withdraws only the failed statement's rows, and so must the
        # twin's.
        assert await backend.get(stored_by_intruder[0].id) is not None


# ── Same key AND same scope → dedupes ──────────────────────────


class TestSameKeySameScope:
    """Same idempotency_key AND same idempotency_scope → dedupes
    (second returns first job)."""

    async def test_same_scope_dedupes(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
        )
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="run-A", scheduled_at=_START
        )
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_same_scope_dedup_preserves_payload(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(
            idempotency_key="k1",
            idempotency_scope="run-A",
            payload={"v": 1},
            scheduled_at=_START,
        )
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(
            idempotency_key="k1",
            idempotency_scope="run-A",
            payload={"v": 2},
            scheduled_at=_START,
        )
        row2 = await backend.enqueue(args2)

        assert row2.id == row1.id
        assert row2.payload == {"v": 1}

    async def test_scope_isolation(self) -> None:
        """Within scope-A the key dedupes; within scope-B the same key
        dedupes independently; the two scopes are isolated from each other."""
        backend = _make_backend()

        args_a1 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="A", scheduled_at=_START
        )
        row_a1 = await backend.enqueue(args_a1)

        args_a2 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="A", scheduled_at=_START
        )
        row_a2 = await backend.enqueue(args_a2)

        args_b1 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="B", scheduled_at=_START
        )
        row_b1 = await backend.enqueue(args_b1)

        args_b2 = make_enqueue_args(
            idempotency_key="k1", idempotency_scope="B", scheduled_at=_START
        )
        row_b2 = await backend.enqueue(args_b2)

        assert row_a1.id == row_a2.id
        assert row_b1.id == row_b2.id
        assert row_a1.id != row_b1.id

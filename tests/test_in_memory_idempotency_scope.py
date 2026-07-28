"""Tests for InMemoryBackend idempotency_scope handling.

Covers:
- Regression: unscoped idempotency_key (default "" or explicit "") still
  dedupes globally exactly as before.
- Same idempotency_key with DIFFERENT idempotency_scope values → both
  enqueue as distinct jobs.
- Same idempotency_key AND same idempotency_scope → dedupes (second
  returns first job).
"""

from datetime import UTC, datetime

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

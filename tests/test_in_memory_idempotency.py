"""Tests for InMemoryBackend idempotency-key handling.

Covers enqueue with a duplicate key returning the same job_id, and a
second enqueue with the same key not overwriting the stored payload.
"""

from datetime import UTC, datetime

from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


# ── idempotency-key collision returns existing row ─────────────


class TestDuplicateKeyReturnsExistingRow:
    """enqueue with key "k1"; second enqueue with same key returns
    the same job_id and _jobs contains exactly one row with that key.
    """

    async def test_duplicate_key_returns_same_job_id(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        # Second enqueue with same key but different id
        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id
        assert row2.idempotency_key == "k1"

    async def test_only_one_row_stored(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        await backend.enqueue(args2)

        # Only the first job should exist in storage
        assert await backend.get(row1.id) is not None
        assert await backend.get(args2.id) is None

    async def test_different_keys_create_separate_rows(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k2", scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id

    async def test_none_key_allows_duplicates(self) -> None:
        """idempotency_key=None does not cause collision detection."""
        backend = _make_backend()

        args1 = make_enqueue_args(scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id
        assert await backend.get(row1.id) is not None
        assert await backend.get(row2.id) is not None


# ── second enqueue does not overwrite payload ──────────────────


class TestSecondEnqueueDoesNotOverwritePayload:
    """enqueue with key="k1", payload={"value": 1}. Second enqueue
    with same key, payload={"value": 2}. Assert returned job_id is the
    first one. Assert the stored payload is still {"value": 1}.
    """

    async def test_second_enqueue_does_not_overwrite_payload(self) -> None:
        backend = _make_backend()

        args1 = make_enqueue_args(idempotency_key="k1", payload={"value": 1}, scheduled_at=_START)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key="k1", payload={"value": 2}, scheduled_at=_START)
        row2 = await backend.enqueue(args2)

        assert row2.id == row1.id

        stored = await backend.get(row1.id)
        assert stored is not None
        assert stored.payload == {"value": 1}

"""Tests for dedup behavioural outcomes on InMemoryBackend.

Covers:
  - unique_for dedup returns existing row
  - idempotency_key dedup returns existing row
  - fresh insert creates a new row (no dedup)

PG path covered by integration tests.
"""

from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


async def test_unique_for_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    identity = IdentityKey("account:99")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    assert row1.id == row2.id


async def test_idempotency_key_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    key = IdempotencyKey("dedup-key-1")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    assert row1.id == row2.id


async def test_fresh_insert_creates_new_row() -> None:
    backend = _make_backend()

    row = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=IdentityKey("account:42"),
            unique_for=timedelta(minutes=15),
        )
    )

    assert row.id is not None

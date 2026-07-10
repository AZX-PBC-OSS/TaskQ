"""Unit, property, and negative tests for max_pending (in-memory backend).

Coverage: structured-log test, bulk-enqueue-at-limit shape test.
All tests run against InMemoryBackend with FakeClock — no PG required.
"""

from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey
from taskq.exceptions import BackpressureError, MaxPendingExceededError, SingletonCollisionError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=30)
_ACTOR = "max_pending_actor"


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _max_pending_args(
    actor: str = _ACTOR,
    queue: str = "default",
    *,
    max_pending: int | None = None,
    payload: dict[str, object] | None = None,
    identity_key: IdentityKey | None = None,
    unique_for: timedelta | None = None,
    scheduled_at: datetime | None = None,
    idempotency_key: IdempotencyKey | None = None,
    metadata: dict[str, object] | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload or {},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=scheduled_at or _START,
        max_pending=max_pending,
        identity_key=identity_key,
        unique_for=unique_for,
        idempotency_key=idempotency_key,
        metadata=metadata or {},
    )


# ── max_pending count enforcement ──────────────────────────────


async def test_max_pending_eleventh_enqueue_raises_with_correct_fields() -> None:
    """max_pending=10; enqueue 10 jobs; 11th raises
    MaxPendingExceededError with current_count=10, max_pending=10,
    isinstance BackpressureError."""
    backend = _make_backend()

    for _ in range(10):
        await backend.enqueue(_max_pending_args(max_pending=10))

    with pytest.raises(MaxPendingExceededError, match="actor=max_pending_actor") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=10))

    assert exc_info.value.actor == _ACTOR
    assert exc_info.value.current_count == 10
    assert exc_info.value.max_pending == 10
    assert isinstance(exc_info.value, BackpressureError)


# ── max_pending=None unbounded ─────────────────────────────────


async def test_max_pending_none_allows_large_volume() -> None:
    """max_pending=None; enqueue 100 jobs; no MaxPendingExceededError."""
    backend = _make_backend()

    for _ in range(100):
        row = await backend.enqueue(_max_pending_args(max_pending=None))

        assert row.status == "pending"


# ── scheduled jobs count ───────────────────────────────────────


async def test_scheduled_jobs_are_counted_in_max_pending() -> None:
    """count includes pending+scheduled. Enqueue 3 pending, snooze
    2 to scheduled; count is 3; further enqueue raises when count >= 3."""
    backend = _make_backend()
    worker_id = backend._worker_id

    for _ in range(3):
        row = await backend.enqueue(_max_pending_args(max_pending=3))

    # Dispatch the first 2 jobs to running.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=2, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 2

    for row in dispatched:
        result = await backend.mark_snoozed(row.id, worker_id, delay=timedelta(seconds=10))
        assert result == "scheduled"

    # 3 pending → 1 pending + 2 scheduled = count 3, which >= max_pending=3.
    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=3))

    assert exc_info.value.current_count == 3
    assert exc_info.value.max_pending == 3


# ── running excluded from count ────────────────────────────────


async def test_running_jobs_are_excluded_from_count() -> None:
    """running excluded. max_pending=10; enqueue 10; dispatch all to
    running; enqueue one more succeeds (pending+scheduled count == 0)."""
    backend = _make_backend()
    worker_id = backend._worker_id

    for _ in range(10):
        await backend.enqueue(_max_pending_args(max_pending=10))

    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 10

    row = await backend.enqueue(_max_pending_args(max_pending=10))

    assert row.status == "pending"


# ── unique_for runs before max_pending ─────────────────────────


async def test_unique_for_dedup_runs_before_max_pending() -> None:
    """unique_for runs before max_pending. max_pending=2,
    unique_for=15min; enqueue keys 'x:A', 'x:B', 'x:A' again.
    Dedup hit returns was_existing handle; 2 jobs total, no error."""
    backend = _make_backend()

    ik_a = IdentityKey("x:A")
    ik_b = IdentityKey("x:B")
    common = {"max_pending": 2, "unique_for": timedelta(minutes=15)}

    row_a1 = await backend.enqueue(_max_pending_args(identity_key=ik_a, **common))

    row_b = await backend.enqueue(_max_pending_args(identity_key=ik_b, **common))

    row_a2 = await backend.enqueue(_max_pending_args(identity_key=ik_a, **common))

    assert row_a1.id == row_a2.id  # dedup hit — unique_for returned existing row.
    assert row_a2.status in ("pending", "scheduled")
    assert row_b.status in ("pending", "scheduled")

    current = sum(
        1
        for row in backend._jobs.values()
        if row.actor == _ACTOR and row.status in ("pending", "scheduled")
    )
    assert current == 2
    assert row_a1.id != row_b.id


# ── fields and LSP ─────────────────────────────────────────────


async def test_exception_fields_and_lsp_compat() -> None:
    """MaxPendingExceededError exposes.actor,.current_count,
    max_pending, and.pending ==.current_count (LSP via BackpressureError)."""
    backend = _make_backend()
    await backend.enqueue(_max_pending_args(max_pending=1))

    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=1))

    exc = exc_info.value
    assert exc.actor == _ACTOR
    assert exc.current_count == 1
    assert exc.max_pending == 1
    assert exc.pending == exc.current_count
    assert isinstance(exc, BackpressureError)


# ── count after job completion ─────────────────────────────────


async def test_count_after_job_completion_frees_capacity() -> None:
    """count after job completion. max_pending=5; enqueue 5;
    complete 2; enqueue 2 more succeed (pending count drops to 3 < 5)."""
    backend = _make_backend()
    worker_id = backend._worker_id

    rows = []
    for _ in range(5):
        row = await backend.enqueue(_max_pending_args(max_pending=5))
        rows.append(row)

    # Dispatch 2 jobs so they can be completed.
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=2, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 2

    for row in dispatched:
        ok = await backend.mark_succeeded(row.id, worker_id, result=None)
        assert ok is True

    # 2 jobs succeeded (excluded from count); 3 remaining pending → count == 3.
    for _ in range(2):
        row = await backend.enqueue(_max_pending_args(max_pending=5))
        assert row.status == "pending"

    # 5th enqueue should raise (3 pending + 2 new = 5 >= max_pending=5).
    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}"):
        await backend.enqueue(_max_pending_args(max_pending=5))


# ── singleton fires before max_pending ─────────────────────────


async def test_singleton_fires_before_max_pending() -> None:
    """singleton fires before max_pending. Actor singleton=True,
    max_pending=10; first enqueue (singleton blocks); second raises
    SingletonCollisionError, NOT MaxPendingExceededError."""
    backend = _make_backend()

    await backend.enqueue(_max_pending_args(max_pending=10, metadata={"singleton": True}))

    with pytest.raises(SingletonCollisionError, match=f"actor={_ACTOR}"):
        await backend.enqueue(_max_pending_args(max_pending=10, metadata={"singleton": True}))


# ── idempotency_key does NOT bypass max_pending ────────────────


async def test_idempotency_key_does_not_bypass_max_pending() -> None:
    """idempotency_key does NOT bypass max_pending. Actor with
    max_pending=1, no unique_for. Enqueue with idempotency_key='k1'
    (count=1). Re-enqueue with same key raises MaxPendingExceededError —
    the count check (step 4) fires before the idempotency-key INSERT
    (step 5).

    This is a documented corner-case: only unique_for (step 2) bypasses
    the max_pending count check. Re-enqueuing with a duplicate
    idempotency_key when the queue is full receives
    MaxPendingExceededError, not the deduplicated handle.
    """
    backend = _make_backend()
    k1 = IdempotencyKey("k1")

    await backend.enqueue(_max_pending_args(max_pending=1, idempotency_key=k1))

    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=1, idempotency_key=k1))

    assert exc_info.value.current_count == 1
    assert exc_info.value.max_pending == 1


# ── max_pending=0 rejects immediately ──────────────────────────


async def test_max_pending_zero_rejects_immediately() -> None:
    """max_pending=0 is valid; enqueue immediately raises
    MaxPendingExceededError (count=0 >= 0 is True). Not a ValueError —
    zero is the documented 'never accept any jobs' configuration."""
    backend = _make_backend()

    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=0))

    assert exc_info.value.current_count == 0
    assert exc_info.value.max_pending == 0


# ── structured logging ──────────────────────────────────────────


async def test_max_pending_exceeded_error_fields() -> None:
    """on MaxPendingExceededError, the exception carries correct
    actor, current_count, max_pending fields and satisfies the
    BackpressureError LSP contract."""
    backend = _make_backend()
    await backend.enqueue(_max_pending_args(max_pending=1))

    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=1))

    exc = exc_info.value
    assert exc.actor == _ACTOR
    assert exc.current_count == 1
    assert exc.max_pending == 1
    assert exc.pending == exc.current_count
    assert isinstance(exc, BackpressureError)


# ── bulk enqueue at limit: 1000 jobs, 1001st raises ─────────────────────


async def test_bulk_enqueue_shape_max_pending_1000() -> None:
    """In-memory variant of a bulk-import shape: max_pending=1000; enqueue
    1000 succeed; 1001st raises with current_count=1000, max_pending=1000.
    Field semantics locked at unit tier without PG."""
    backend = _make_backend()

    for _ in range(1000):
        await backend.enqueue(_max_pending_args(max_pending=1000))

    with pytest.raises(MaxPendingExceededError, match=f"actor={_ACTOR}") as exc_info:
        await backend.enqueue(_max_pending_args(max_pending=1000))

    assert exc_info.value.actor == _ACTOR
    assert exc_info.value.current_count == 1000
    assert exc_info.value.max_pending == 1000
    assert isinstance(exc_info.value, BackpressureError)


# ── max_pending invariant (Hypothesis) ─────────────────────────


@st.composite
def _max_pending_operations(draw: st.DrawFn) -> tuple[int, list[str]]:
    """Generate a max_pending value and a sequence of operations for property test."""
    max_pending = draw(st.integers(min_value=0, max_value=10))
    length = draw(st.integers(min_value=10, max_value=80))
    ops = draw(
        st.lists(
            st.sampled_from(
                [
                    "enqueue",
                    "enqueue",
                    "enqueue",
                    "enqueue",
                    "enqueue",
                    "dispatch_one",
                    "dispatch_one",
                    "dispatch_one",
                    "complete_one",
                    "complete_one",
                ]
            ),
            min_size=length,
            max_size=length,
        )
    )
    return max_pending, ops


@given(scenario=_max_pending_operations())
@hyp_settings(max_examples=300)
async def test_max_pending_invariant(scenario: tuple[int, list[str]]) -> None:
    """max_pending invariant. For any max_pending=N, every enqueue
    succeeds when pending+scheduled < N and raises when >= N."""
    max_pending, ops = scenario
    backend = InMemoryBackend(clock=FakeClock(_START))
    worker_id = backend._worker_id
    actor = "prop_actor"

    for op in ops:
        current = sum(
            1
            for row in backend._jobs.values()
            if row.actor == actor and row.status in ("pending", "scheduled")
        )

        if op == "enqueue":
            if current < max_pending:
                row = await backend.enqueue(_max_pending_args(actor=actor, max_pending=max_pending))
                assert row.status in ("pending", "scheduled")
            else:
                with pytest.raises(MaxPendingExceededError) as exc_info:
                    await backend.enqueue(_max_pending_args(actor=actor, max_pending=max_pending))
                assert exc_info.value.current_count == current
                assert exc_info.value.max_pending == max_pending

        elif op == "dispatch_one":
            _dispatched = await backend.dispatch_batch(
                worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
            )

        elif op == "complete_one":
            for row in backend._jobs.values():
                if row.actor == actor and row.status == "running":
                    await backend.mark_succeeded(row.id, worker_id, result=None)
                    break


async def test_max_pending_invariant_exhaustive() -> None:
    """Exhaustive sanity check for the invariant across small fixed values."""
    for max_pending in range(0, 6):
        backend = InMemoryBackend(clock=FakeClock(_START))
        actor = "exhaust_actor"

        for _ in range(15):
            current = sum(
                1
                for row in backend._jobs.values()
                if row.actor == actor and row.status in ("pending", "scheduled")
            )

            if current < max_pending:
                row = await backend.enqueue(_max_pending_args(actor=actor, max_pending=max_pending))
                assert row.status in ("pending", "scheduled")
            else:
                with pytest.raises(MaxPendingExceededError) as exc_info:
                    await backend.enqueue(_max_pending_args(actor=actor, max_pending=max_pending))
                assert exc_info.value.current_count == current
                assert exc_info.value.max_pending == max_pending

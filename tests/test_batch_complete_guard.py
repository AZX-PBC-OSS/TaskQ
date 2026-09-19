"""Guard semantics of batch completion, pinned through the in-memory mirror.

The PG ``complete_batch`` UPDATE arbitrates completion inside its own
statement via a NOT EXISTS guard over non-terminal member jobs (the same
shape as the stale-batch sweep's ``complete_stale_batches``), so a
completion attempt can never land while a member is still in flight, and
never waits on a caller-held count that a concurrent terminal write may
have already made stale. The in-memory mirror models the same decision
(``taskq.testing._batch.py``), so the guard's decision table is pinned
here at unit tier.

The concurrent-terminal race itself - two members terminating on
different connections, each hook's count statement reading the other as
non-terminal from a stale READ COMMITTED snapshot - needs real PG
snapshots and lives in the integration lane.
"""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.batch import apply_batch_terminal_outcome
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _create_batch(
    backend: InMemoryBackend,
    bid: UUID,
    *,
    failure_threshold: int | None = None,
) -> None:
    await backend.create_batch(
        bid,
        queue="default",
        expected_size=2,
        failure_threshold=failure_threshold,
        finalizer_job_id=None,
        originating_actor=None,
    )


def _seed_member(backend: InMemoryBackend, bid: UUID, status: str) -> UUID:
    row = make_job_row(status=status)  # type: ignore[arg-type]  # Why: helper accepts str; every caller passes a valid JobStatus literal at runtime
    row = replace(row, metadata={**row.metadata, "batch_id": str(bid)})
    backend._jobs[row.id] = row  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access to seed member rows with chosen statuses
    return row.id


def _terminal_job(bid: UUID) -> JobRow:
    """A JobRow for a member whose terminal write already landed."""
    return replace(make_job_row(status="succeeded"), metadata={"batch_id": str(bid)})


# ── Mirror guard: completion is arbitrated at write time ───────────


class TestCompleteBatchGuard:
    """complete_batch decides against the live member set at the moment
    of the write, not against any count the caller computed earlier."""

    async def test_noop_while_any_member_non_terminal(self) -> None:
        backend = _make_backend()
        bid = new_uuid()
        await _create_batch(backend, bid)
        _seed_member(backend, bid, "succeeded")
        _seed_member(backend, bid, "running")

        await backend.complete_batch(bid)

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "active", "completed while a member was still in flight"
        assert row.completed_at is None

    async def test_completes_when_all_members_terminal(self) -> None:
        backend = _make_backend()
        bid = new_uuid()
        await _create_batch(backend, bid)
        _seed_member(backend, bid, "succeeded")
        _seed_member(backend, bid, "failed")

        await backend.complete_batch(bid)

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "complete"
        assert row.completed_at is not None

    async def test_decision_is_arbitrated_at_call_time(self) -> None:
        """The same complete call that was vetoed while a member was in
        flight lands once that member turns terminal: the decision reads
        the member set at write time, so no caller-side recount is
        needed - the optimistic-attempt contract the terminal-outcome
        hook relies on."""
        backend = _make_backend()
        bid = new_uuid()
        await _create_batch(backend, bid)
        _seed_member(backend, bid, "succeeded")
        member = _seed_member(backend, bid, "running")

        await backend.complete_batch(bid)
        after_veto = await backend.get_batch(bid)
        assert after_veto is not None
        assert after_veto.status == "active"

        stored = backend._jobs[member]  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access to settle the in-flight member
        backend._jobs[member] = replace(stored, status="succeeded")

        await backend.complete_batch(bid)

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "complete"


# ── Hook: the completion attempt is optimistic, the guard decides ───


class _StaleCountBackend(InMemoryBackend):
    """Mirror that reports the race's count input: the increment/reset
    statement's READ COMMITTED snapshot predates a concurrent member's
    terminal write, so it over-counts non-terminal members. The stored
    rows are the truth the complete guard re-checks."""

    async def reset_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: object = None,
    ) -> int:
        _ = await super().reset_batch_failures(batch_id, connection=connection)
        return 1  # stale over-count: the other member has since terminated


class TestHookDoesNotTrustStaleCount:
    """The hook must complete the batch from the guarded statement's own
    re-check, not from the increment/reset statement's count - two
    members terminating concurrently can each read the other as
    non-terminal, and a hook that gates on that count leaves the row for
    the leader sweep."""

    async def test_stale_overcount_still_completes(self) -> None:
        backend = _StaleCountBackend(clock=FakeClock(start=_START))
        bid = new_uuid()
        await _create_batch(backend, bid)
        _seed_member(backend, bid, "succeeded")
        _seed_member(backend, bid, "succeeded")

        await apply_batch_terminal_outcome(backend, _terminal_job(bid), "succeeded")

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "complete", (
            "the hook trusted the stale count and skipped the completion attempt - "
            "the concurrent-terminal race that leaves the row for the leader sweep"
        )
        assert row.completed_at is not None


class TestHookOptimisticAttemptSafety:
    """The optimistic attempt must change nothing while members are in
    flight, and must not manufacture state for batches that have no row."""

    async def test_attempt_with_member_in_flight_leaves_batch_active(self) -> None:
        backend = _make_backend()
        bid = new_uuid()
        await _create_batch(backend, bid)
        _seed_member(backend, bid, "succeeded")
        _seed_member(backend, bid, "pending")
        await backend.increment_batch_failures(bid)

        await apply_batch_terminal_outcome(backend, _terminal_job(bid), "succeeded")

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "active", "the optimistic attempt completed past an in-flight member"
        # The reset still landed - the completion attempt is the only
        # write the guard vetoes.
        assert row.consecutive_failures == 0

    async def test_attempt_without_batch_row_is_a_noop(self) -> None:
        """enqueue_batch_fast batches carry ``batch_id`` metadata with no
        batches row; the optimistic attempt must not create one."""
        backend = _make_backend()
        bid = new_uuid()
        _seed_member(backend, bid, "succeeded")

        await apply_batch_terminal_outcome(backend, _terminal_job(bid), "succeeded")

        assert bid not in backend._batches  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access

"""In-memory integration tests for batch abort/completion hook.

Tests the end-to-end flow: enqueue batch with failure policy →
run_until_drained → verify batch status and job terminal statuses.

The Hypothesis property test lives here (not in test_wait_for_batch.py)
because it exercises the abort/completion hook, not wait_for_batch.
"""

from datetime import UTC, datetime
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_uuid
from taskq.batch import EnqueueItem, apply_batch_terminal_outcome
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.testing._runner import register_stub
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


@actor(name="batch_abort_test_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


def _make_item(value: int = 0) -> EnqueueItem:
    return EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=value))


# ── test_abort_after_threshold ──────────────────────────────────


class TestAbortAfterThreshold:
    """5 jobs all fail, AbortBatchAfter(3) → 3 failed + 2 cancelled, batch status=aborted."""

    async def test_abort_after_threshold(self) -> None:
        backend = _make_backend()

        def _fail_stub(_payload: dict[str, object], _ctx: object) -> None:
            raise RuntimeError("boom")

        register_stub(
            backend,
            _test_actor.name,
            _fail_stub,
            non_retryable_exceptions=(RuntimeError,),
        )

        client = _make_client(backend)
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(5)]
        handle = await client.enqueue_batch(
            items, batch_id=batch_id, failure_policy=AbortBatchAfter(3)
        )

        await backend.run_until_drained()

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None
        assert batch_row.status == "aborted"

        from taskq.backend.statemachine import TERMINAL_STATUSES

        statuses: dict[str, int] = {}
        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.status in TERMINAL_STATUSES
            statuses[row.status] = statuses.get(row.status, 0) + 1

        assert statuses.get("failed", 0) == 3
        assert statuses.get("cancelled", 0) == 2


# ── test_success_resets_counter ─────────────────────────────────


class TestSuccessResetsCounter:
    """Alternating fail/succeed → counter resets, no abort, batch complete."""

    async def test_success_resets_counter(self) -> None:
        backend = _make_backend()

        call_count = 0

        def _alternating_stub(_payload: dict[str, object], _ctx: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 1:
                raise RuntimeError("boom")

        register_stub(
            backend,
            _test_actor.name,
            _alternating_stub,
            non_retryable_exceptions=(RuntimeError,),
        )

        client = _make_client(backend)
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(6)]
        handle = await client.enqueue_batch(
            items, batch_id=batch_id, failure_policy=AbortBatchAfter(3)
        )

        await backend.run_until_drained()

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None
        assert batch_row.status == "complete"

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.status == "succeeded" or row.status == "failed"


# ── test_no_abort_without_policy ────────────────────────────────


class TestNoAbortWithoutPolicy:
    """No failure_policy → all fail, none cancelled, no batch row."""

    async def test_no_abort_without_policy(self) -> None:
        backend = _make_backend()

        def _fail_stub(_payload: dict[str, object], _ctx: object) -> None:
            raise RuntimeError("boom")

        register_stub(
            backend,
            _test_actor.name,
            _fail_stub,
            non_retryable_exceptions=(RuntimeError,),
        )

        client = _make_client(backend)
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(5)]
        handle = await client.enqueue_batch(items, batch_id=batch_id)

        await backend.run_until_drained()

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is None

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.status == "failed"


# ── test_batch_marked_complete_on_all_terminal ──────────────────


class TestBatchMarkedCompleteOnAllTerminal:
    """All succeed → batch status=complete, completed_at set."""

    async def test_batch_marked_complete(self) -> None:
        backend = _make_backend()

        def _ok_stub(_payload: dict[str, object], _ctx: object) -> None:
            pass

        register_stub(backend, _test_actor.name, _ok_stub)

        client = _make_client(backend)
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(5)]
        handle = await client.enqueue_batch(
            items, batch_id=batch_id, failure_policy=AbortBatchAfter(3)
        )

        await backend.run_until_drained()

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None
        assert batch_row.status == "complete"
        assert batch_row.completed_at is not None

        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.status == "succeeded"


# ── Hypothesis property test ────────────────────────────────────


class TestBatchAbortProperty:
    """Property: after run_until_drained with a random batch of mixed
    succeed/fail outcomes and a random threshold:

    - Compute the expected batch status (aborted vs complete) from the
      inputs by simulating the consecutive-failure counter, then assert
      the observed batch row matches.
    - If aborted: failed + succeeded + cancelled == total.
    - If complete: cancelled == 0, failed + succeeded == total.
    """

    @given(
        n_jobs=st.integers(min_value=1, max_value=15),
        threshold=st.integers(min_value=1, max_value=10),
        outcomes=st.lists(
            st.booleans(),
            min_size=1,
            max_size=15,
        ),
    )
    @settings(max_examples=50, deadline=None)
    async def test_batch_abort_property(
        self,
        n_jobs: int,
        threshold: int,
        outcomes: list[bool],
    ) -> None:
        backend = _make_backend()

        call_idx = 0

        def _mixed_stub(_payload: dict[str, object], _ctx: object) -> None:
            nonlocal call_idx
            idx = call_idx
            call_idx += 1
            if idx < len(outcomes) and not outcomes[idx]:
                raise RuntimeError("boom")

        register_stub(
            backend,
            _test_actor.name,
            _mixed_stub,
            non_retryable_exceptions=(RuntimeError,),
        )

        client = _make_client(backend)
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(n_jobs)]
        handle = await client.enqueue_batch(
            items, batch_id=batch_id, failure_policy=AbortBatchAfter(threshold)
        )

        await backend.run_until_drained()

        # Collect terminal statuses
        status_counts: dict[str, int] = {}
        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            status_counts[row.status] = status_counts.get(row.status, 0) + 1

        total = n_jobs
        failed = status_counts.get("failed", 0)
        succeeded = status_counts.get("succeeded", 0)
        cancelled = status_counts.get("cancelled", 0)

        # Compute expected batch status from inputs by simulating the
        # consecutive-failure counter. The in-memory runner dispatches
        # one job at a time (limit=1) in insertion order (same priority,
        # same scheduled_at), so job processing order is deterministic.
        expected_aborted = False
        consecutive = 0
        for i in range(n_jobs):
            job_succeeds = i >= len(outcomes) or outcomes[i]
            if job_succeeds:
                consecutive = 0
            else:
                consecutive += 1
                if consecutive >= threshold:
                    expected_aborted = True
                    break

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None

        if expected_aborted:
            assert batch_row.status == "aborted", (
                f"expected aborted but got {batch_row.status} "
                f"(n_jobs={n_jobs}, threshold={threshold}, outcomes={outcomes}, "
                f"status_counts={status_counts})"
            )
            assert failed + succeeded + cancelled == total
        else:
            assert batch_row.status == "complete", (
                f"expected complete but got {batch_row.status} "
                f"(n_jobs={n_jobs}, threshold={threshold}, outcomes={outcomes}, "
                f"status_counts={status_counts})"
            )
            assert cancelled == 0
            assert failed + succeeded == total


# ── test_abort_wins_over_complete ───────────────────────────────


class TestAbortWinsOverComplete:
    """When the last failure hits the threshold AND remaining==0, the
    batch is aborted, not complete.

    This is a race-condition test: the last job's failure increments
    consecutive_failures to the threshold AND the non-terminal count
    drops to 0 simultaneously. The abort path must win over the
    complete path.
    """

    async def test_abort_wins_over_complete(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        from dataclasses import replace

        # Seed 2 already-failed jobs so consecutive_failures starts at 2
        # (we increment it manually below), and the 3rd failure will
        # hit the threshold. All 3 jobs are terminal, so remaining==0.
        for _ in range(3):
            row = make_job_row(status="failed")
            row = replace(row, metadata={"batch_id": str(bid)})
            backend._jobs[row.id] = row

        # Pre-increment to 2 so the next failure hits threshold=3.
        await backend.increment_batch_failures(bid)
        await backend.increment_batch_failures(bid)
        assert backend._batches[bid].consecutive_failures == 2

        # The 3rd failure: count=3 >= threshold=3 AND remaining==0.
        job = make_job_row(status="failed")
        job = replace(job, metadata={"batch_id": str(bid)})

        await apply_batch_terminal_outcome(backend, job, "failed")

        batch_row = await backend.get_batch(bid)
        assert batch_row is not None
        # Abort must win over complete even though remaining==0.
        assert batch_row.status == "aborted"
        assert batch_row.completed_at is not None


# ── Direct unit tests for apply_batch_terminal_outcome ───────────


class TestApplyBatchTerminalOutcome:
    """Direct unit tests for the batch terminal-outcome hook.

    These tests call ``apply_batch_terminal_outcome`` directly (not through
    ``run_until_drained``) to verify each outcome branch in isolation.
    """

    async def test_non_batched_job_returns_immediately(self) -> None:
        backend = _make_backend()
        # Job with no batch_id in metadata → hook must return immediately.
        job = make_job_row(status="succeeded")
        assert "batch_id" not in job.metadata

        await apply_batch_terminal_outcome(backend, job, "succeeded")

        # No batch row should have been created, no state changed.
        assert len(backend._batches) == 0

    async def test_succeeded_resets_failures_and_completes(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=2,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        # Pre-set some failures.
        await backend.increment_batch_failures(bid)
        await backend.increment_batch_failures(bid)
        assert backend._batches[bid].consecutive_failures == 2

        # Create a succeeded job with batch_id; no non-terminal jobs remain.
        job = make_job_row(status="succeeded")
        from dataclasses import replace

        job = replace(job, metadata={"batch_id": str(bid)})

        await apply_batch_terminal_outcome(backend, job, "succeeded")

        batch_row = await backend.get_batch(bid)
        assert batch_row is not None
        assert batch_row.consecutive_failures == 0
        assert batch_row.status == "complete"
        assert batch_row.completed_at is not None

    async def test_failed_increments_and_aborts_at_threshold(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        # Create some pending jobs so remaining > 0 after first failures.
        from dataclasses import replace

        for _ in range(4):
            row = make_job_row(status="pending")
            row = replace(row, metadata={"batch_id": str(bid)})
            backend._jobs[row.id] = row

        job = make_job_row(status="failed")
        job = replace(job, metadata={"batch_id": str(bid)})

        # First failure: count=1 < 3, no abort.
        await apply_batch_terminal_outcome(backend, job, "failed")
        assert backend._batches[bid].consecutive_failures == 1
        assert backend._batches[bid].status == "active"

        # Second failure: count=2 < 3, no abort.
        await apply_batch_terminal_outcome(backend, job, "failed")
        assert backend._batches[bid].consecutive_failures == 2
        assert backend._batches[bid].status == "active"

        # Third failure: count=3 >= 3 → abort.
        await apply_batch_terminal_outcome(backend, job, "failed")
        batch_row = await backend.get_batch(bid)
        assert batch_row is not None
        assert batch_row.status == "aborted"
        assert batch_row.completed_at is not None

    async def test_failed_below_threshold_completes_when_empty(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=1,
            failure_threshold=100,
            finalizer_job_id=None,
            originating_actor=None,
        )
        # No non-terminal jobs — the only job is the one that just failed.
        from dataclasses import replace

        job = make_job_row(status="failed")
        job = replace(job, metadata={"batch_id": str(bid)})
        backend._jobs[job.id] = job

        await apply_batch_terminal_outcome(backend, job, "failed")

        batch_row = await backend.get_batch(bid)
        assert batch_row is not None
        assert batch_row.consecutive_failures == 1
        # count=1 < threshold=100, but remaining=0 → complete.
        assert batch_row.status == "complete"
        assert batch_row.completed_at is not None

    async def test_cancelled_completes_when_empty(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=1,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        from dataclasses import replace

        # The only job in the batch is the cancelled one — no non-terminal remain.
        job = make_job_row(status="cancelled")
        job = replace(job, metadata={"batch_id": str(bid)})
        backend._jobs[job.id] = job

        await apply_batch_terminal_outcome(backend, job, "cancelled")

        batch_row = await backend.get_batch(bid)
        assert batch_row is not None
        assert batch_row.status == "complete"
        assert batch_row.completed_at is not None

    async def test_snoozed_returns_immediately(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        # Pre-set some failures to verify they are NOT reset.
        await backend.increment_batch_failures(bid)
        assert backend._batches[bid].consecutive_failures == 1

        from dataclasses import replace

        job = make_job_row(status="scheduled")
        job = replace(job, metadata={"batch_id": str(bid)})

        await apply_batch_terminal_outcome(backend, job, "snoozed")

        # Snoozed is non-terminal — no state should change.
        assert backend._batches[bid].consecutive_failures == 1
        assert backend._batches[bid].status == "active"

    async def test_scheduled_returns_immediately(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        from dataclasses import replace

        job = make_job_row(status="scheduled")
        job = replace(job, metadata={"batch_id": str(bid)})

        await apply_batch_terminal_outcome(backend, job, "scheduled")

        # Scheduled is non-terminal — no state should change.
        assert backend._batches[bid].status == "active"
        assert backend._batches[bid].consecutive_failures == 0


# ── Hook try/except guard test ──────────────────────────────────


class _IncrementRaisesBackend(InMemoryBackend):
    """InMemoryBackend subclass whose ``increment_batch_failures`` raises.

    Used to verify that the runner's try/except around
    ``apply_batch_terminal_outcome`` prevents backend errors from
    affecting the job's terminal state.
    """

    async def increment_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: object = None,
    ) -> tuple[int, int | None, int]:
        raise RuntimeError("simulated backend failure")


class TestHookFailureGuard:
    """Verify that a failure in the batch policy hook does not affect
    the job's terminal state — the hook is wrapped in try/except by
    both the in-memory runner and the production dispatch path.
    """

    async def test_hook_failure_does_not_affect_job_terminal_state(self) -> None:
        backend = _IncrementRaisesBackend(clock=FakeClock(start=_START))

        def _fail_stub(_payload: dict[str, object], _ctx: object) -> None:
            raise RuntimeError("boom")

        register_stub(
            backend,
            _test_actor.name,
            _fail_stub,
            non_retryable_exceptions=(RuntimeError,),
        )

        client = JobsClient(backend=backend, clock=FakeClock(start=_START))
        batch_id = new_uuid()
        items = [_make_item(i) for i in range(3)]
        handle = await client.enqueue_batch(
            items, batch_id=batch_id, failure_policy=AbortBatchAfter(2)
        )

        # run_until_drained should not propagate the RuntimeError from
        # increment_batch_failures — the runner catches it.
        await backend.run_until_drained()

        # The job must still reach its terminal state ("failed").
        for h in handle.job_handles:
            row = await backend.get(h.job_id)
            assert row is not None
            assert row.status == "failed"

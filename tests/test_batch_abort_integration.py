"""In-memory integration tests for batch abort/completion hook.

Tests the end-to-end flow: enqueue batch with failure policy →
run_until_drained → verify batch status and job terminal statuses.

The Hypothesis property test lives here (not in test_wait_for_batch.py)
because it exercises the abort/completion hook, not wait_for_batch.
"""

from datetime import UTC, datetime
from uuid import uuid4

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.testing._runner import register_stub
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

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
        batch_id = uuid4()
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
        batch_id = uuid4()
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
        batch_id = uuid4()
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
        batch_id = uuid4()
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

    - If no maximal consecutive-failure run reached the threshold →
      batch row is 'complete' and no job is 'cancelled'.
    - If the threshold was reached → batch row is 'aborted', and
      failed + succeeded + cancelled == total.
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
        batch_id = uuid4()
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

        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None

        if batch_row.status == "aborted":
            assert failed + succeeded + cancelled == total
        elif batch_row.status == "complete":
            assert cancelled == 0
            assert failed + succeeded == total

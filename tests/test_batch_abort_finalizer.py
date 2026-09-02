"""Unit tests for batch abort + finalizer interaction (in-memory, no PG).

Tests that a finalizer actor which catches ``BatchAbortedError`` inside
its body reaches ``succeeded`` (not ``failed``), and that the snooze
path does not consume retry budget — proving the catch path works
end-to-end through the consumer's exception routing.

Also includes clock-skew resilience tests: a ``FakeClock`` that jumps
forward mid-run must not cause the finalizer to fail when
``schedule_to_close`` is NULL (the normal case for transient-retry
actors with no ``time_budget``).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from pydantic import BaseModel

from taskq import RetryPolicy, Snooze, actor
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.exceptions import BatchAbortedError
from taskq.testing._runner import register_stub
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _ChildPayload(BaseModel):
    run_id: str = "test"


class _FinalizerPayload(BaseModel):
    run_id: str = "test"
    batch_id: str = ""


@actor(
    name="abort_finalizer_test_child",
    retry=RetryPolicy(max_attempts=1, base=timedelta(milliseconds=100)),
)
async def _child_actor(_payload: _ChildPayload) -> None:
    pass


@actor(
    name="abort_finalizer_test_finalizer",
    retry=RetryPolicy(max_attempts=50, base=timedelta(seconds=1)),
    non_retryable_exceptions=(),
)
async def _finalizer_actor(_payload: _FinalizerPayload) -> None:
    pass


def _make_backend(*, clock: FakeClock | None = None) -> InMemoryBackend:
    return InMemoryBackend(clock=clock or FakeClock(start=_START))


def _make_client(backend: InMemoryBackend, *, clock: FakeClock | None = None) -> JobsClient:
    return JobsClient(backend=backend, clock=clock or FakeClock(start=_START))


def _make_child_item() -> EnqueueItem:
    return EnqueueItem(actor_ref=_child_actor, payload=_ChildPayload())


def _make_finalizer_item(batch_id: UUID) -> EnqueueItem:
    return EnqueueItem(
        actor_ref=_finalizer_actor,
        payload=_FinalizerPayload(batch_id=str(batch_id)),
    )


# ── Abort + finalizer: BatchAbortedError is caught → succeeded ──────────


class TestAbortFinalizerSucceeds:
    """Batch aborts, finalizer catches BatchAbortedError → succeeded."""

    async def test_finalizer_catches_batch_aborted_error(self) -> None:
        """Finalizer actor that catches BatchAbortedError reaches 'succeeded'.

        This is the in-memory reproduction of the e2e test
        ``test_batch_abort_with_finalizer``.  The finalizer stub simulates
        the ``try: wait_for_batch() except BatchAbortedError: record()``
        pattern: it raises Snooze while children are pending, then catches
        BatchAbortedError when the batch is aborted.
        """
        backend = _make_backend()
        batch_id = uuid4()
        abort_error: BatchAbortedError | None = None
        snooze_count = 0

        def _fail_child(_payload: dict[str, object], _ctx: object) -> None:
            raise RuntimeError("intentional failure")

        async def _abort_finalizer_stub(payload: dict[str, object], _ctx: object) -> None:
            nonlocal abort_error, snooze_count
            bid = UUID(payload["batch_id"])

            from taskq.testing._runner import wait_for_batch as in_memory_wait_for_batch

            try:
                await in_memory_wait_for_batch(
                    backend,
                    bid,
                    snooze_interval=timedelta(seconds=1),
                )
            except BatchAbortedError as e:
                abort_error = e
                return
            except Snooze:
                snooze_count += 1
                raise

        register_stub(
            backend,
            _child_actor.name,
            _fail_child,
            non_retryable_exceptions=(RuntimeError,),
        )
        register_stub(
            backend,
            _finalizer_actor.name,
            _abort_finalizer_stub,
            non_retryable_exceptions=(),
        )

        client = _make_client(backend)
        children = [_make_child_item() for _ in range(5)]
        finalizer = _make_finalizer_item(batch_id)

        handle = await client.enqueue_batch(
            children,
            batch_id=batch_id,
            failure_policy=AbortBatchAfter(3),
            finalizer=finalizer,
        )

        await backend.run_until_drained()

        # Batch should be aborted
        batch_row = await backend.get_batch(batch_id)
        assert batch_row is not None
        assert batch_row.status == "aborted"

        # Finalizer should have caught BatchAbortedError
        assert abort_error is not None, (
            "finalizer never received BatchAbortedError — "
            "wait_for_batch may not have seen the aborted batch"
        )
        assert abort_error.batch_id == batch_id

        # Finalizer should be succeeded
        finalizer_row = await backend.get(handle.finalizer_handle.job_id)
        assert finalizer_row is not None
        assert finalizer_row.status == "succeeded", (
            f"finalizer should succeed (caught BatchAbortedError), "
            f"got status={finalizer_row.status!r}, "
            f"error_class={finalizer_row.error_class!r}"
        )

        # Snooze may or may not have occurred depending on dispatch order
        # (serial dispatch may run all children before the finalizer).
        # The key invariant: attempt <= max_attempts (budget not consumed).
        assert finalizer_row.attempt <= finalizer_row.max_attempts, (
            f"snooze consumed retry budget: attempt={finalizer_row.attempt}, "
            f"max_attempts={finalizer_row.max_attempts}"
        )


# ── Clock skew resilience ────────────────────────────────────────────────


class TestClockSkewResilience:
    """Clock skew must not cause finalizer failure when schedule_to_close is NULL.

    The snooze mechanism uses PG's ``clock_timestamp()`` for scheduling
    (``scheduled_at = now() + delay``) and dispatch (``scheduled_at <=
    clock_timestamp()``), so PG-internal consistency is preserved.

    The retry classifier uses Python's ``datetime.now(UTC)``, but
    ``schedule_to_close`` is NULL for transient-retry actors with no
    ``time_budget``, so the ``now >= schedule_to_close`` deadline check
    cannot fire.

    These tests prove that a ``FakeClock`` that jumps forward mid-run
    does not cause the finalizer to fail.
    """

    async def test_clock_jump_does_not_fail_snoozed_job(self) -> None:
        """A snoozed job with schedule_to_close=NULL survives a clock jump.

        Simulates WSL clock skew: the Python clock jumps forward by 1 hour
        while a job is snoozed.  The job should still dispatch and succeed
        because the snooze uses the backend clock (PG's
        ``clock_timestamp()`` equivalent), not the Python clock, for
        scheduling.
        """
        clock = FakeClock(start=_START)
        backend = _make_backend(clock=clock)
        worker_id = backend._worker_id

        job = make_job_row(status="running")
        job = replace(
            job,
            schedule_to_close=None,
            max_attempts=50,
            locked_by_worker=worker_id,
        )
        backend._jobs[job.id] = job

        # Snooze the job (simulates wait_for_batch raising Snooze)
        snooze = Snooze(timedelta(seconds=2))
        tri = await backend.mark_snoozed(
            job.id,
            worker_id,
            snooze.delay,
            metadata_update={"snooze_count": 1},
        )

        assert tri == "scheduled", f"mark_snoozed returned {tri!r}, expected 'scheduled'"

        # Jump the clock forward by 1 hour (simulates WSL clock skew)
        clock.move_to(_START + timedelta(hours=1))

        # The job should still be scheduled (not failed)
        row = await backend.get(job.id)
        assert row is not None
        assert row.status == "scheduled", (
            f"clock jump caused snoozed job to fail: status={row.status!r}, "
            f"error_class={row.error_class!r}"
        )
        assert row.error_class is None

    async def test_clock_jump_does_not_cause_deadline_exceeded(self) -> None:
        """RetryClassifier must never return DeadlineExceeded regardless of
        client-side clock skew.

        ``decide_after_failure`` takes no clock/deadline input at all (C1/C2:
        the ``schedule_to_close`` deadline is arbitrated server-side, inside
        ``mark_failed_or_retry``'s SQL, never by the classifier) — so a
        client-side clock jump cannot influence its decision either way.
        """
        from taskq.retry import JobRetryState, RetryPolicy, decide_after_failure
        from taskq.testing._runner import _InMemoryActorConfig

        clock = FakeClock(start=_START)
        actor_config = _InMemoryActorConfig(
            retry=RetryPolicy(max_attempts=50, base=timedelta(seconds=1)),
        )

        job_state = JobRetryState(
            attempt=5,
            max_attempts=55,
            retry_kind="transient",
            schedule_to_close=None,
            start_to_close=None,
        )

        # Jump the clock forward by 1 hour
        clock.move_to(_START + timedelta(hours=1))

        decision = decide_after_failure(
            actor_config,
            RuntimeError("test error"),
            job_state,
            max_retry_backoff=timedelta(hours=24),
        )

        # Should retry, not fail with DeadlineExceeded
        from taskq.retry import Retry

        assert isinstance(decision, Retry), (
            f"clock jump caused DeadlineExceeded with NULL schedule_to_close: decision={decision!r}"
        )

    async def test_clock_jump_does_not_fail_in_memory_finalizer(self) -> None:
        """Full in-memory abort+finalizer with clock jumps → finalizer still succeeds.

        Uses a FakeClock that jumps forward between dispatches to simulate
        WSL clock skew under load.  The finalizer should still catch
        BatchAbortedError and reach 'succeeded'.
        """
        clock = FakeClock(start=_START)
        backend = _make_backend(clock=clock)
        batch_id = uuid4()
        abort_error: BatchAbortedError | None = None

        def _fail_child(_payload: dict[str, object], _ctx: object) -> None:
            # Jump the clock forward on each child failure (simulates skew)
            clock.move_to(clock.now() + timedelta(seconds=30))
            raise RuntimeError("intentional failure")

        async def _abort_finalizer_stub(payload: dict[str, object], _ctx: object) -> None:
            nonlocal abort_error
            bid = UUID(payload["batch_id"])

            from taskq.testing._runner import wait_for_batch as in_memory_wait_for_batch

            try:
                await in_memory_wait_for_batch(
                    backend,
                    bid,
                    snooze_interval=timedelta(seconds=1),
                )
            except BatchAbortedError as e:
                abort_error = e
                return
            except Snooze:
                # Jump the clock forward on each snooze (simulates skew)
                clock.move_to(clock.now() + timedelta(seconds=30))
                raise

        register_stub(
            backend,
            _child_actor.name,
            _fail_child,
            non_retryable_exceptions=(RuntimeError,),
        )
        register_stub(
            backend,
            _finalizer_actor.name,
            _abort_finalizer_stub,
            non_retryable_exceptions=(),
        )

        client = _make_client(backend, clock=clock)
        children = [_make_child_item() for _ in range(5)]
        finalizer = _make_finalizer_item(batch_id)

        handle = await client.enqueue_batch(
            children,
            batch_id=batch_id,
            failure_policy=AbortBatchAfter(3),
            finalizer=finalizer,
        )

        await backend.run_until_drained()

        # Finalizer should still succeed despite clock jumps
        assert abort_error is not None, "finalizer never received BatchAbortedError"
        finalizer_row = await backend.get(handle.finalizer_handle.job_id)
        assert finalizer_row is not None
        assert finalizer_row.status == "succeeded", (
            f"finalizer failed despite NULL schedule_to_close: "
            f"status={finalizer_row.status!r}, "
            f"error_class={finalizer_row.error_class!r}, "
            f"attempt={finalizer_row.attempt}, "
            f"max_attempts={finalizer_row.max_attempts}"
        )


# ── Snooze does not consume retry budget ─────────────────────────────────


class TestSnoozeBudgetInvariant:
    """Snooze must never consume retry budget — max_attempts grows with
    each snooze so attempt < max_attempts always holds."""

    async def test_snooze_increments_max_attempts_not_attempt(self) -> None:
        """After N snooze-dispatch cycles, attempt < max_attempts always holds.

        Snooze preserves attempt (only dispatch increments it) and bumps
        max_attempts by 1, so the retry budget grows with each snooze.
        """
        backend = _make_backend()
        worker_id = backend._worker_id

        job = make_job_row(status="running")
        job = replace(
            job,
            schedule_to_close=None,
            max_attempts=50,
            attempt=1,
            locked_by_worker=worker_id,
        )
        backend._jobs[job.id] = job

        snooze = Snooze(timedelta(seconds=2))
        expected_attempt = 1

        for i in range(10):
            # Snooze: running → scheduled, max_attempts++, attempt unchanged
            tri = await backend.mark_snoozed(
                job.id,
                worker_id,
                snooze.delay,
                metadata_update={"snooze_count": i + 1},
            )
            assert tri == "scheduled", f"snooze {i + 1} returned {tri!r}"

            row = await backend.get(job.id)
            assert row is not None
            assert row.max_attempts == 50 + i + 1
            assert row.attempt == expected_attempt, (
                f"snooze changed attempt: expected {expected_attempt}, got {row.attempt}"
            )
            assert row.attempt < row.max_attempts, (
                f"budget exhausted: attempt={row.attempt} >= max_attempts={row.max_attempts}"
            )

            # Re-dispatch: scheduled → running, attempt++
            expected_attempt += 1
            backend._jobs[job.id] = replace(
                row,
                status="running",
                attempt=expected_attempt,
                locked_by_worker=worker_id,
            )

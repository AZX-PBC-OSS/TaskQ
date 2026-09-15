"""Tests for InMemoryBackend.retry_job and PostgresBackend.retry_job.

Verifies that retry_job:
- Re-pends a failed/crashed/cancelled job with cleared error fields
- Keeps attempt monotonic (never reset — the attempt counter is the epoch
  identifier for the job's attempt record) and raises max_attempts to
  GREATEST(max_attempts, attempt + 1) so the budget gates open
- Resets cancel_phase=0, scheduled_at=now()
- Returns True for retryable jobs, False for non-retryable ones
- Fires a wake NOTIFY (verified via wake subscriber on InMemoryBackend)
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend._protocol import JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend(clock: FakeClock | None = None) -> InMemoryBackend:
    clk = clock or FakeClock(_START)
    return InMemoryBackend(
        clock=clk,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _enqueue_job(backend: InMemoryBackend) -> JobId:
    from taskq.backend import EnqueueArgs

    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    return args.id


def _set_job_status(
    backend: InMemoryBackend, job_id: JobId, status: str, *, attempt: int = 1
) -> None:
    row = backend._jobs[job_id]
    backend._jobs[job_id] = replace(
        row,
        status=status,  # pyright: ignore[reportArgumentType]
        attempt=attempt,
        finished_at=_START + timedelta(seconds=10),
        error_class="SomeError",
        error_message="something broke",
        error_traceback="Traceback...",
    )


class TestInMemoryRetryJob:
    async def test_retry_failed_job(self) -> None:
        """retry_job resets a failed job to pending."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "failed")

        result = await backend.retry_job(job_id)

        assert result is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending"
        assert row.attempt == 1, (
            "MONOTONIC-ATTEMPT CONTRACT: retry_job must never reset the "
            f"attempt counter — observed {row.attempt!r}. A reset revisits "
            "the spent epoch's attempt numbers, causing a PRIMARY KEY "
            "collision on the attempt record (verified in "
            "tests/test_retry_job_attempt_epoch_pk.py)."
        )
        assert row.max_attempts == 3, (
            "CEILING-RAISE CONTRACT: a mid-budget re-run keeps the original "
            "ceiling (GREATEST(max_attempts, attempt + 1) — attempt 1 of 3 "
            f"stays 3) — observed {row.max_attempts!r}."
        )
        assert row.error_class is None
        assert row.error_message is None
        assert row.error_traceback is None
        assert row.finished_at is None
        assert row.result is None

    async def test_retry_crashed_job(self) -> None:
        """retry_job resets a crashed job to pending."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "crashed")

        result = await backend.retry_job(job_id)

        assert result is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending"

    async def test_retry_cancelled_job(self) -> None:
        """retry_job resets a cancelled job to pending."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "cancelled")

        result = await backend.retry_job(job_id)

        assert result is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending"

    async def test_retry_pending_returns_false(self) -> None:
        """retry_job returns False for a pending job (not retryable)."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)

        result = await backend.retry_job(job_id)

        assert result is False

    async def test_retry_running_returns_false(self) -> None:
        """retry_job returns False for a running job (not retryable)."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "running")

        result = await backend.retry_job(job_id)

        assert result is False

    async def test_retry_succeeded_returns_true(self) -> None:
        """retry_job re-pends a succeeded job for a replay.

        An operator re-run means "run this again": a succeeded job's status
        records that the actor returned without raising, not that the result
        was right, so a bug shipped and fixed afterward must still have a
        supported path to re-run the jobs that ran against it. See
        Backend.retry_job's docstring and
        tests/test_retry_job_source_states_parity.py for the full contract
        (every terminal state is a valid source; only 'running' is refused).
        """
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "succeeded")

        result = await backend.retry_job(job_id)

        assert result is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status in ("pending", "scheduled")

    async def test_retry_nonexistent_returns_false(self) -> None:
        """retry_job returns False for a non-existent job."""
        backend = _make_backend()
        fake_id = new_job_id()

        result = await backend.retry_job(fake_id)

        assert result is False

    async def test_retry_sets_scheduled_at_to_now(self) -> None:
        """retry_job sets scheduled_at to the current clock time."""
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "failed")

        clock.advance(timedelta(seconds=60))
        await backend.retry_job(job_id)

        row = await backend.get(job_id)
        assert row is not None
        assert row.scheduled_at == _START + timedelta(seconds=60)

    async def test_retry_fires_wake_subscriber(self) -> None:
        """retry_job fires the wake subscriber event."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "failed")

        async with backend.subscribe_wake() as wake_event:
            assert not wake_event.is_set()
            await backend.retry_job(job_id)
            assert wake_event.is_set()

    async def test_retry_resets_cancel_phase(self) -> None:
        """retry_job resets the whole cancel trail: cancel_phase to 0 (NONE)
        and cancel_requested_at to None — the re-run is a fresh epoch, so it
        must not inherit the spent epoch's request stamp (PG's SET clause
        clears both)."""
        from taskq.backend._protocol import CancelPhase

        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        row = backend._jobs[job_id]
        backend._jobs[job_id] = replace(
            row,
            status="cancelled",
            cancel_phase=CancelPhase.FORCED,
            cancel_requested_at=_START + timedelta(seconds=1),
        )

        await backend.retry_job(job_id)

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == CancelPhase.NONE
        assert row.cancel_requested_at is None, (
            "FRESH-EPOCH CONTRACT: retry_job clears cancel_requested_at "
            "alongside cancel_phase — a stale request stamp on a re-pended "
            "row breaks the cancel safety contract (verified in "
            "tests/test_rt_diff_terminal.py)."
        )

    async def test_retry_clears_result_fields(self) -> None:
        """retry_job clears result, result_size_bytes, and result_expires_at."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        row = backend._jobs[job_id]
        backend._jobs[job_id] = replace(
            row,
            status="failed",
            result={"key": "val"},
            result_size_bytes=42,
            result_expires_at=_START + timedelta(days=1),
        )

        await backend.retry_job(job_id)

        row = await backend.get(job_id)
        assert row is not None
        assert row.result is None
        assert row.result_size_bytes is None
        assert row.result_expires_at is None

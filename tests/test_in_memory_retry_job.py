"""Tests for InMemoryBackend.retry_job and PostgresBackend.retry_job.

Verifies that retry_job:
- Resets a failed/crashed/cancelled job to pending with cleared error fields
- Sets attempt=0, cancel_phase=0, scheduled_at=now()
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
        assert row.attempt == 0
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

    async def test_retry_succeeded_returns_false(self) -> None:
        """retry_job returns False for a succeeded job."""
        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        _set_job_status(backend, job_id, "succeeded")

        result = await backend.retry_job(job_id)

        assert result is False

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
        """retry_job resets cancel_phase to 0 (NONE)."""
        from taskq.backend._protocol import CancelPhase

        backend = _make_backend()
        job_id = await _enqueue_job(backend)
        row = backend._jobs[job_id]
        backend._jobs[job_id] = replace(row, status="cancelled", cancel_phase=CancelPhase.FORCED)

        await backend.retry_job(job_id)

        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == CancelPhase.NONE

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

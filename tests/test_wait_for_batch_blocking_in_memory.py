"""Blocking mode (``snooze_via_exception=False``) of the in-memory
``wait_for_batch`` mirror.

The PG variant blocks via ``asyncio.sleep`` and rescans until every
member is terminal; the mirror must honor the same flag rather than
always raising :class:`~taskq.exceptions.Snooze` - code written against
the blocking mode gets a returned status in memory exactly as it does
against PG. Both snooze modes are pinned here, including the
snooze-interval clamp, which must govern the blocking sleep exactly as
it governs the Snooze delay.

The sleep is patched rather than waited: what is pinned is the loop
shape (pending → sleep(interval) → rescan), not wall-clock latency.
"""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._protocol import BatchRow
from taskq.exceptions import BatchAbortedError, Snooze
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.in_memory import wait_for_batch as in_memory_wait_for_batch
from taskq.testing.jobs import make_job_row

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_CLOCK_START))


def _seed_batch(
    backend: InMemoryBackend,
    n: int,
    batch_id: UUID,
    status: str,
) -> list[UUID]:
    """Insert *n* member jobs with the given batch_id and status."""
    job_ids: list[UUID] = []
    for _i in range(n):
        row = make_job_row(status=status)  # type: ignore[arg-type]  # Why: helper accepts str; every caller passes a valid JobStatus literal at runtime
        row = replace(row, metadata={"batch_id": str(batch_id)})
        backend._jobs[row.id] = row  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access to seed member rows
        job_ids.append(row.id)
    return job_ids


def _seed_batch_row(
    backend: InMemoryBackend,
    batch_id: UUID,
    *,
    status: str = "active",
    consecutive_failures: int = 0,
    failure_threshold: int | None = None,
) -> None:
    backend._batches[batch_id] = BatchRow(  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access
        id=batch_id,
        queue="default",
        status=status,  # type: ignore[arg-type]  # Why: helper accepts str for ergonomics
        expected_size=0,
        consecutive_failures=consecutive_failures,
        failure_threshold=failure_threshold,
        finalizer_job_id=None,
        originating_actor=None,
        created_at=_CLOCK_START,
        completed_at=None,
        metadata={},
    )


def _settle(backend: InMemoryBackend, job_ids: list[UUID]) -> None:
    """The concurrent task's terminal write between two poll ticks."""
    for jid in job_ids:
        row = backend._jobs[jid]  # pyright: ignore[reportPrivateUsage]  # Why: test-only direct store access
        backend._jobs[jid] = replace(row, status="succeeded")


# ── Blocking mode: sleep and rescan, never raise Snooze ────────────


class TestBlockingMode:
    async def test_returns_status_once_members_settle(self) -> None:
        backend = _make_backend()
        batch_id = new_uuid()
        job_ids = _seed_batch(backend, 2, batch_id, status="pending")
        original_sleep = asyncio.sleep

        async def fake_sleep(delta: float) -> None:
            _settle(backend, job_ids)
            await original_sleep(0)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(asyncio, "sleep", fake_sleep)
            status = await in_memory_wait_for_batch(
                backend,
                batch_id,
                snooze_via_exception=False,
                snooze_interval=timedelta(seconds=1),
            )

        assert status.total == 2
        assert status.pending == 0
        assert status.succeeded == 2
        assert status.is_complete is True

    async def test_sleeps_the_clamped_interval_and_warns(self) -> None:
        """A sub-second snooze_interval must be clamped to the 1 s floor
        before it reaches the blocking sleep, and the clamp warning must
        fire in blocking mode exactly as it does in exception mode."""
        backend = _make_backend()
        batch_id = new_uuid()
        job_ids = _seed_batch(backend, 1, batch_id, status="pending")
        original_sleep = asyncio.sleep
        sleeps: list[float] = []

        async def fake_sleep(delta: float) -> None:
            sleeps.append(delta)
            _settle(backend, job_ids)
            await original_sleep(0)

        with (
            structlog.testing.capture_logs() as logs,
            pytest.MonkeyPatch.context() as mp,
        ):
            mp.setattr(asyncio, "sleep", fake_sleep)
            status = await in_memory_wait_for_batch(
                backend,
                batch_id,
                snooze_via_exception=False,
                snooze_interval=timedelta(milliseconds=500),
            )

        assert status.is_complete is True
        assert sleeps == [1.0], (
            "the clamp must govern the blocking sleep, not just the Snooze delay"
        )
        assert any(entry["event"] == "snooze-interval-clamped" for entry in logs)

    async def test_aborted_batch_raises_batch_aborted_error_once_settled(self) -> None:
        """Blocking mode on an aborted batch sleeps through the in-flight
        window and surfaces BatchAbortedError when the last member turns
        terminal - the same terminal state the PG poll loop reaches."""
        backend = _make_backend()
        batch_id = new_uuid()
        job_ids = _seed_batch(backend, 1, batch_id, status="pending")
        _seed_batch_row(
            backend,
            batch_id,
            status="aborted",
            consecutive_failures=3,
            failure_threshold=3,
        )
        original_sleep = asyncio.sleep

        async def fake_sleep(delta: float) -> None:
            _settle(backend, job_ids)
            await original_sleep(0)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(asyncio, "sleep", fake_sleep)
            with pytest.raises(BatchAbortedError) as exc_info:
                await in_memory_wait_for_batch(
                    backend,
                    batch_id,
                    snooze_via_exception=False,
                    snooze_interval=timedelta(seconds=1),
                )

        assert exc_info.value.batch_id == batch_id


# ── Exception mode: unchanged by the mode split ────────────────────


class TestExceptionModeUnchanged:
    async def test_still_raises_snooze_while_in_flight(self) -> None:
        backend = _make_backend()
        batch_id = new_uuid()
        _seed_batch(backend, 1, batch_id, status="pending")

        with pytest.raises(Snooze) as exc_info:
            await in_memory_wait_for_batch(
                backend,
                batch_id,
                snooze_via_exception=True,
                snooze_interval=timedelta(seconds=2),
            )

        assert exc_info.value.delay == timedelta(seconds=2)

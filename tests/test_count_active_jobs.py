"""Unit tests for Backend.count_active_jobs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from taskq.testing import FakeClock, InMemoryBackend, make_enqueue_args

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_count_active_jobs_empty_queues() -> None:
    """Empty queues list returns 0."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    assert await backend.count_active_jobs([]) == 0


async def test_count_active_jobs_no_jobs() -> None:
    """Queues with no jobs returns 0."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    assert await backend.count_active_jobs(["default"]) == 0


async def test_count_active_jobs_pending() -> None:
    """Pending jobs are counted."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="default"))
    assert await backend.count_active_jobs(["default"]) == 2


async def test_count_active_jobs_running() -> None:
    """Running jobs are counted."""
    from datetime import timedelta

    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(make_enqueue_args(queue="default", scheduled_at=_CLOCK_START))
    await backend.dispatch_batch(
        backend._worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert await backend.count_active_jobs(["default"]) == 1


async def test_count_active_jobs_scheduled() -> None:
    """Scheduled (future) jobs are counted."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(
        make_enqueue_args(queue="default", scheduled_at=datetime(2030, 1, 1, tzinfo=UTC))
    )
    assert await backend.count_active_jobs(["default"]) == 1


async def test_count_active_jobs_terminal_excluded() -> None:
    """Terminal jobs (succeeded, failed, etc.) are NOT counted."""
    from datetime import timedelta

    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(make_enqueue_args(queue="default", scheduled_at=_CLOCK_START))
    dispatched = await backend.dispatch_batch(
        backend._worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    await backend.mark_succeeded(dispatched[0].id, backend._worker_id, None)
    assert await backend.count_active_jobs(["default"]) == 0


async def test_count_active_jobs_multi_queue() -> None:
    """Counts across multiple queues."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="priority"))
    await backend.enqueue(make_enqueue_args(queue="other"))
    assert await backend.count_active_jobs(["default", "priority"]) == 2


async def test_count_active_jobs_queue_subset() -> None:
    """Only counts jobs in the specified queues."""
    backend = InMemoryBackend(FakeClock(_CLOCK_START))
    await backend.enqueue(make_enqueue_args(queue="default"))
    await backend.enqueue(make_enqueue_args(queue="priority"))
    assert await backend.count_active_jobs(["default"]) == 1


@pytest.mark.integration
async def test_pg_count_active_jobs(clean_jobs_app: JobsApp) -> None:
    """PostgresBackend.count_active_jobs matches inserted rows."""
    backend = clean_jobs_app.backend
    await backend.enqueue(make_enqueue_args(queue="default", actor="test_actor"))
    await backend.enqueue(make_enqueue_args(queue="default", actor="test_actor"))
    await backend.enqueue(make_enqueue_args(queue="priority", actor="test_actor"))
    assert await backend.count_active_jobs(["default"]) == 2
    assert await backend.count_active_jobs(["default", "priority"]) == 3
    assert await backend.count_active_jobs([]) == 0

"""Unit tests for JobFilter.order_by and JobSortField — latest-run queries.

Covers the order_by option added to JobFilter so callers can query
"latest run by business key" without paging through the default
priority/scheduled_at ordering. Uses the InMemoryBackend so ordering
behaviour is exercised end-to-end without a Postgres dependency.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import JobFilter, JobId, JobRow, JobSortField
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_T0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)


def _backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_T0))


def _job(
    *,
    actor: str = "test_actor",
    queue: str = "default",
    status: object = "succeeded",
    created_at: datetime = _T0,
    finished_at: datetime | None = None,
    priority: int = 0,
    scheduled_at: datetime = _T0,
) -> JobRow:
    row = make_job_row(actor=actor, queue=queue, status=status, priority=priority)  # type: ignore[arg-type]
    return replace(
        row,
        id=JobId(new_job_id()),
        created_at=created_at,
        scheduled_at=scheduled_at,
        finished_at=finished_at,
    )


def test_job_sort_field_has_expected_members() -> None:
    """JobSortField exposes SCHEDULED_AT_ASC, CREATED_AT_DESC, FINISHED_AT_DESC."""
    names = {m.name for m in JobSortField}
    assert names == {"SCHEDULED_AT_ASC", "CREATED_AT_DESC", "FINISHED_AT_DESC"}


def test_job_filter_order_by_defaults_to_none() -> None:
    """JobFilter.order_by defaults to None (preserve current ordering)."""
    f = JobFilter()
    assert f.order_by is None


async def test_list_jobs_default_ordering_is_scheduled_at_asc() -> None:
    """Without order_by, list_jobs preserves priority DESC, scheduled_at ASC."""
    backend = _backend()
    early = _job(scheduled_at=_T0, priority=0)
    late = _job(scheduled_at=_T0 + timedelta(minutes=5), priority=0)
    backend._jobs[early.id] = early
    backend._jobs[late.id] = late

    rows = await backend.list_jobs(JobFilter(actor="test_actor", limit=10))

    assert [r.id for r in rows] == [early.id, late.id]


async def test_list_jobs_order_by_created_at_desc() -> None:
    """order_by=CREATED_AT_DESC returns newest-created jobs first."""
    backend = _backend()
    oldest = _job(created_at=_T0)
    middle = _job(created_at=_T0 + timedelta(minutes=10))
    newest = _job(created_at=_T0 + timedelta(minutes=20))
    backend._jobs[oldest.id] = oldest
    backend._jobs[middle.id] = middle
    backend._jobs[newest.id] = newest

    rows = await backend.list_jobs(
        JobFilter(actor="test_actor", order_by=JobSortField.CREATED_AT_DESC, limit=10)
    )

    assert [r.id for r in rows] == [newest.id, middle.id, oldest.id]


async def test_list_jobs_order_by_finished_at_desc_nulls_last() -> None:
    """order_by=FINISHED_AT_DESC returns most-recently-finished first;
    jobs that have not finished (finished_at is None) sort last."""
    backend = _backend()
    pending = _job(status="pending", finished_at=None)
    first_done = _job(status="succeeded", finished_at=_T0 + timedelta(seconds=10))
    last_done = _job(status="succeeded", finished_at=_T0 + timedelta(seconds=50))
    backend._jobs[pending.id] = pending
    backend._jobs[first_done.id] = first_done
    backend._jobs[last_done.id] = last_done

    rows = await backend.list_jobs(
        JobFilter(actor="test_actor", order_by=JobSortField.FINISHED_AT_DESC, limit=10)
    )

    assert [r.id for r in rows] == [last_done.id, first_done.id, pending.id]


async def test_list_jobs_order_by_scheduled_at_asc_matches_default() -> None:
    """order_by=SCHEDULED_AT_ASC produces the same ordering as the default."""
    backend = _backend()
    early = _job(scheduled_at=_T0)
    late = _job(scheduled_at=_T0 + timedelta(minutes=5))
    backend._jobs[early.id] = early
    backend._jobs[late.id] = late

    explicit = await backend.list_jobs(
        JobFilter(actor="test_actor", order_by=JobSortField.SCHEDULED_AT_ASC, limit=10)
    )

    assert [r.id for r in explicit] == [early.id, late.id]


def test_job_filter_cursor_with_non_default_order_by_raises() -> None:
    """Cursor pagination is only valid with the default ordering; combining
    a cursor with a non-default order_by raises ValueError at the boundary."""
    with pytest.raises(ValueError, match="cursor pagination"):
        JobFilter(order_by=JobSortField.CREATED_AT_DESC, cursor="opaque")


def test_job_filter_cursor_with_default_order_by_allowed() -> None:
    """A cursor with order_by=None or SCHEDULED_AT_ASC is allowed."""
    JobFilter(cursor="opaque")
    JobFilter(order_by=JobSortField.SCHEDULED_AT_ASC, cursor="opaque")

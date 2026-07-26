"""Unit tests for JobFilter — ordering, multi-status, and active meta-filter.

Covers the order_by option, multi-status sequence support, and the
``active`` meta-filter added to JobFilter. Uses the InMemoryBackend so
behaviour is exercised end-to-end without a Postgres dependency.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend._cursor import encode_cursor
from taskq.backend._protocol import JobFilter, JobId, JobRow, JobSortField
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES
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


# ── Multi-status sequence support ────────────────────────────────────


async def test_job_filter_status_single_string_regression() -> None:
    """A single JobStatus string still works exactly as before."""
    backend = _backend()
    pending = _job(status="pending", priority=5)
    running = _job(status="running", priority=3)
    succeeded = _job(status="succeeded", priority=1)
    for r in (pending, running, succeeded):
        backend._jobs[r.id] = r

    rows = await backend.list_jobs(JobFilter(actor="test_actor", status="pending", limit=10))
    assert [r.id for r in rows] == [pending.id]


async def test_job_filter_status_list_returns_union() -> None:
    """A list of statuses returns the union of matching rows."""
    backend = _backend()
    pending = _job(status="pending", priority=5)
    running = _job(status="running", priority=3)
    succeeded = _job(status="succeeded", priority=1)
    for r in (pending, running, succeeded):
        backend._jobs[r.id] = r

    rows = await backend.list_jobs(
        JobFilter(actor="test_actor", status=["pending", "running"], limit=10)
    )
    ids = {r.id for r in rows}
    assert ids == {pending.id, running.id}


async def test_job_filter_status_tuple_returns_union() -> None:
    """A tuple of statuses also works."""
    backend = _backend()
    pending = _job(status="pending", priority=5)
    running = _job(status="running", priority=3)
    for r in (pending, running):
        backend._jobs[r.id] = r

    rows = await backend.list_jobs(
        JobFilter(actor="test_actor", status=("pending", "running"), limit=10)
    )
    ids = {r.id for r in rows}
    assert ids == {pending.id, running.id}


# ── active meta-filter ───────────────────────────────────────────────


async def test_job_filter_active_true_returns_non_terminal() -> None:
    """active=True returns exactly pending, scheduled, running — and
    excludes all 5 terminal statuses."""
    backend = _backend()
    jobs: dict[str, JobRow] = {}
    for s in (
        "pending",
        "scheduled",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "crashed",
        "abandoned",
    ):
        j = _job(status=s, priority=0)
        jobs[s] = j
        backend._jobs[j.id] = j

    rows = await backend.list_jobs(JobFilter(actor="test_actor", active=True, limit=100))
    returned_statuses = {r.status for r in rows}
    assert returned_statuses == {"pending", "scheduled", "running"}
    assert returned_statuses == ACTIVE_STATUSES


async def test_job_filter_active_false_returns_terminal() -> None:
    """active=False returns exactly the terminal statuses."""
    backend = _backend()
    for s in (
        "pending",
        "scheduled",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "crashed",
        "abandoned",
    ):
        j = _job(status=s, priority=0)
        backend._jobs[j.id] = j

    rows = await backend.list_jobs(JobFilter(actor="test_actor", active=False, limit=100))
    returned_statuses = {r.status for r in rows}
    assert returned_statuses == TERMINAL_STATUSES


def test_job_filter_status_and_active_raises() -> None:
    """Specifying both status and active raises ValueError."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        JobFilter(status="pending", active=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        JobFilter(status=["pending", "running"], active=False)


def test_job_filter_active_defaults_to_none() -> None:
    """active defaults to None (no terminality filter)."""
    f = JobFilter()
    assert f.active is None


# ── Multi-status cursor pagination ───────────────────────────────────


async def test_list_jobs_multi_status_cursor_pagination() -> None:
    """Cursor pagination with a multi-status filter produces a complete,
    non-overlapping, correctly-ordered traversal across multiple pages.

    Creates 5 jobs with statuses in {pending, running} and distinct
    priorities, then pages through with limit=2 using cursors. Asserts
    every matching row appears exactly once in priority-DESC order.
    """
    backend = _backend()
    priorities = [10, 8, 5, 3, 1]
    statuses = ["pending", "running", "pending", "running", "pending"]
    jobs: list[JobRow] = []
    for pri, st in zip(priorities, statuses, strict=True):
        j = _job(status=st, priority=pri)
        jobs.append(j)
        backend._jobs[j.id] = j

    expected_ids = [j.id for j in sorted(jobs, key=lambda r: (-r.priority, r.scheduled_at, r.id))]

    # Page 1
    page1 = await backend.list_jobs(
        JobFilter(actor="test_actor", status=["pending", "running"], limit=2)
    )
    assert len(page1) == 2
    assert [r.id for r in page1] == expected_ids[:2]

    cursor = encode_cursor(page1[-1].priority, page1[-1].scheduled_at, page1[-1].id)

    # Page 2
    page2 = await backend.list_jobs(
        JobFilter(actor="test_actor", status=["pending", "running"], limit=2, cursor=cursor)
    )
    assert len(page2) == 2
    assert [r.id for r in page2] == expected_ids[2:4]

    cursor = encode_cursor(page2[-1].priority, page2[-1].scheduled_at, page2[-1].id)

    # Page 3 — only 1 job left
    page3 = await backend.list_jobs(
        JobFilter(actor="test_actor", status=["pending", "running"], limit=2, cursor=cursor)
    )
    assert len(page3) == 1
    assert [r.id for r in page3] == expected_ids[4:]

    # Complete, non-overlapping
    all_returned = [r.id for r in page1 + page2 + page3]
    assert all_returned == expected_ids
    assert len(all_returned) == len(set(all_returned))

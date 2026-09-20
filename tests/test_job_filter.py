"""Unit tests for JobFilter - ordering, multi-status, and the unfinished
meta-filter.

Covers the order_by option, multi-status sequence support, and the
``unfinished`` meta-filter (renamed from ``active``; the deprecated alias
is exercised too). Uses the InMemoryBackend so behaviour is exercised
end-to-end without a Postgres dependency.
"""

import warnings
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend._cursor import encode_cursor
from taskq.backend._protocol import JOB_STATUS_VALUES, JobFilter, JobId, JobRow, JobSortField
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


def test_job_filter_accepts_a_cursor_with_every_ordering() -> None:
    """A cursor is valid with every ordering, not just the default.

    The boundary used to reject the combination outright because the two
    DESC orderings tie-broke on ``id`` in the opposite direction to their
    primary column, which no keyset comparison can express. Now that each
    ordering runs ``id`` with its primary column and carries its own
    cursor shape, the refusal has nothing left to protect against -
    ``test_backend_equivalence`` pages every ordering end to end on both
    backends.
    """
    for order_by in (None, *JobSortField):
        JobFilter(order_by=order_by, cursor="opaque")


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


# ── unfinished meta-filter ────────────────────────────────────────────


async def test_job_filter_unfinished_true_returns_non_terminal() -> None:
    """unfinished=True returns exactly pending, scheduled, running - and
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

    rows = await backend.list_jobs(JobFilter(actor="test_actor", unfinished=True, limit=100))
    returned_statuses = {r.status for r in rows}
    assert returned_statuses == {"pending", "scheduled", "running"}
    assert returned_statuses == ACTIVE_STATUSES


async def test_job_filter_unfinished_false_returns_terminal() -> None:
    """unfinished=False returns exactly the terminal statuses."""
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

    rows = await backend.list_jobs(JobFilter(actor="test_actor", unfinished=False, limit=100))
    returned_statuses = {r.status for r in rows}
    assert returned_statuses == TERMINAL_STATUSES


def test_job_filter_status_and_unfinished_raises() -> None:
    """Specifying both status and unfinished raises ValueError."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        JobFilter(status="pending", unfinished=True)
    with pytest.raises(ValueError, match="mutually exclusive"):
        JobFilter(status=["pending", "running"], unfinished=False)


# ── Deprecated `active` alias ──────────────────────────────────────────


def test_job_filter_active_alias_promotes_to_unfinished_and_warns() -> None:
    """The deprecated ``active`` kwarg still selects the same predicate:
    it warns, and the resulting filter equals the ``unfinished`` spelling."""
    with pytest.warns(DeprecationWarning, match="deprecated alias"):
        from_alias = JobFilter(active=True)
    assert from_alias == JobFilter(unfinished=True)
    assert from_alias.unfinished is True
    assert from_alias.has_predicates()


def test_job_filter_active_alias_false_pins_terminal_predicate() -> None:
    """``active=False`` promotes to ``unfinished=False``, the terminal
    half of the predicate, not a different one."""
    with pytest.warns(DeprecationWarning, match="deprecated alias"):
        from_alias = JobFilter(active=False)
    assert from_alias == JobFilter(unfinished=False)
    assert from_alias.unfinished is False


def test_job_filter_active_alias_conflict_with_unfinished_raises() -> None:
    """Passing the alias and the new name with different values is a
    misconfiguration, not a silently-won race between the two fields."""
    with pytest.raises(ValueError, match="disagree"):
        JobFilter(unfinished=True, active=False)  # type: ignore[arg-type]  # Why: exercising the deprecated alias deliberately


def test_job_filter_pre_rename_positional_layout_still_binds() -> None:
    """An 11-argument positional call written before the rename still
    binds its 10th argument to the terminality filter and its 11th to
    created_before: ``unfinished`` was appended after ``created_before``,
    not slotted into ``active``'s old position. Without this, a positional
    call would hand a datetime to the alias and fail on the alias/
    ``unfinished`` disagreement check.
    """
    created_before = datetime(2025, 6, 1, tzinfo=UTC)
    with pytest.warns(DeprecationWarning, match="deprecated alias"):
        positional = JobFilter(  # type: ignore[misc]  # Why: exercising the pre-rename positional call shape deliberately
            "q", None, "a", None, None, 50, "c", ("t",), None, True, created_before
        )
    assert positional.unfinished is True
    assert positional.created_before == created_before
    assert positional == JobFilter(
        queue="q",
        actor="a",
        limit=50,
        cursor="c",
        tags=("t",),
        unfinished=True,
        created_before=created_before,
    )


def test_job_filter_active_alias_warns_once_across_replace_copies() -> None:
    """A filter built with the deprecated alias warns exactly once, at
    construction: the promotion clears the alias, so the
    ``dataclasses.replace`` copies the client's list probe and the
    bulk-cancel sanitizer make do not re-trip the warning.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        from_alias = JobFilter(active=True)
        for _ in range(3):
            replace(from_alias, limit=1)
    assert from_alias.unfinished is True
    deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 1


async def test_cancel_where_with_active_alias_warns_once() -> None:
    """Realistic flow through the bulk-cancel sanitizer: the filter copy
    the sanitizer makes with ``dataclasses.replace`` must not re-trip the
    alias warning, so a cancel_where on an alias-built filter warns once,
    not once per internal copy.
    """
    backend = _backend()
    job = _job(status="pending")
    backend._jobs[job.id] = job

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = await backend.cancel_where(JobFilter(active=True), reason="done")
    assert result.cancelled_directly == 1
    deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 1


# ── Unknown status validation ──────────────────────────────────────────


def test_job_filter_unknown_single_status_raises() -> None:
    """An unknown single status raises ValueError at construction, before
    any backend is involved - identical behaviour for PG (which would
    otherwise fail with an enum-cast DataError) and in-memory (which
    would otherwise silently return nothing)."""
    with pytest.raises(ValueError, match="unknown job status"):
        JobFilter(status="bogus")  # type: ignore[arg-type]  # Why: untrusted runtime input, deliberately not a JobStatus


def test_job_filter_unknown_status_in_sequence_raises() -> None:
    """One bad member in a sequence is rejected, and the message names it."""
    with pytest.raises(ValueError, match=r"unknown job status value\(s\): \['bogus'\]"):
        JobFilter(status=["pending", "bogus"])  # type: ignore[list-item]  # Why: untrusted runtime input


def test_job_filter_unknown_status_message_lists_valid_values() -> None:
    """The error message tells the operator which statuses are valid."""
    with pytest.raises(ValueError, match="valid statuses are"):
        JobFilter(status=["bogus"])  # type: ignore[list-item]  # Why: untrusted runtime input


def test_job_filter_non_string_status_in_sequence_raises() -> None:
    """A non-string member (e.g. an int from a config file) is rejected by
    the same validation rather than passing through to a backend."""
    with pytest.raises(ValueError, match="unknown job status"):
        JobFilter(status=[42])  # type: ignore[list-item]  # Why: untrusted runtime input


def test_job_filter_all_known_statuses_accepted() -> None:
    """Every JobStatus literal value is accepted, single or in a sequence."""
    for status in JOB_STATUS_VALUES:
        JobFilter(status=status)  # type: ignore[arg-type]  # Why: JOB_STATUS_VALUES is frozenset[str]; members are all JobStatus
    JobFilter(status=list(JOB_STATUS_VALUES))  # type: ignore[arg-type]  # Why: same
    JobFilter(status=[])  # empty sequence is valid - matches no jobs


# ── limit validation ───────────────────────────────────────────────────


def test_job_filter_negative_limit_raises() -> None:
    """A negative limit raises ValueError at construction - PG would raise
    "LIMIT must not be negative" mid-query while the in-memory slice would
    silently drop rows; both backends must fail identically up front."""
    with pytest.raises(ValueError, match="limit must be >= 0"):
        JobFilter(limit=-1)


def test_job_filter_zero_limit_is_valid() -> None:
    """limit=0 is well-defined (returns no rows) and consistent across
    backends - it stays allowed."""
    assert JobFilter(limit=0).limit == 0


async def test_list_jobs_zero_limit_returns_no_rows() -> None:
    """limit=0 returns an empty page even with matching jobs present."""
    backend = _backend()
    job = _job(status="pending")
    backend._jobs[job.id] = job

    rows = await backend.list_jobs(JobFilter(actor="test_actor", limit=0))
    assert rows == []


def test_job_filter_unfinished_defaults_to_none() -> None:
    """unfinished defaults to None (no terminality filter)."""
    f = JobFilter()
    assert f.unfinished is None
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

    # Page 3 - only 1 job left
    page3 = await backend.list_jobs(
        JobFilter(actor="test_actor", status=["pending", "running"], limit=2, cursor=cursor)
    )
    assert len(page3) == 1
    assert [r.id for r in page3] == expected_ids[4:]

    # Complete, non-overlapping
    all_returned = [r.id for r in page1 + page2 + page3]
    assert all_returned == expected_ids
    assert len(all_returned) == len(set(all_returned))


# ── unfinished meta-filter + cursor pagination ────────────────────────


async def test_list_jobs_unfinished_true_cursor_pagination() -> None:
    """Cursor pagination with unfinished=True produces a complete,
    non-overlapping, correctly-ordered traversal across multiple pages
    that includes exactly the non-terminal jobs and excludes all
    terminal jobs.

    Creates 5 non-terminal jobs (mix of pending, scheduled, running)
    with distinct priorities and 3 terminal jobs (succeeded, failed,
    cancelled), then pages through with unfinished=True and limit=2 using
    cursors. Asserts every non-terminal row appears exactly once in
    priority-DESC order and no terminal row leaks through.
    """
    backend = _backend()
    priorities = [10, 8, 5, 3, 1]
    unfinished_statuses = ["pending", "scheduled", "running", "pending", "scheduled"]
    unfinished_jobs: list[JobRow] = []
    for pri, st in zip(priorities, unfinished_statuses, strict=True):
        j = _job(status=st, priority=pri)
        unfinished_jobs.append(j)
        backend._jobs[j.id] = j

    # Terminal jobs that must never appear in unfinished=True results.
    for st in ("succeeded", "failed", "cancelled"):
        j = _job(status=st, priority=99)
        backend._jobs[j.id] = j

    expected_ids = [
        j.id for j in sorted(unfinished_jobs, key=lambda r: (-r.priority, r.scheduled_at, r.id))
    ]

    # Page 1
    page1 = await backend.list_jobs(JobFilter(actor="test_actor", unfinished=True, limit=2))
    assert len(page1) == 2
    assert [r.id for r in page1] == expected_ids[:2]

    cursor = encode_cursor(page1[-1].priority, page1[-1].scheduled_at, page1[-1].id)

    # Page 2
    page2 = await backend.list_jobs(
        JobFilter(actor="test_actor", unfinished=True, limit=2, cursor=cursor)
    )
    assert len(page2) == 2
    assert [r.id for r in page2] == expected_ids[2:4]

    cursor = encode_cursor(page2[-1].priority, page2[-1].scheduled_at, page2[-1].id)

    # Page 3 - only 1 job left
    page3 = await backend.list_jobs(
        JobFilter(actor="test_actor", unfinished=True, limit=2, cursor=cursor)
    )
    assert len(page3) == 1
    assert [r.id for r in page3] == expected_ids[4:]

    # Complete, non-overlapping, no terminal jobs
    all_returned = [r.id for r in page1 + page2 + page3]
    assert all_returned == expected_ids
    assert len(all_returned) == len(set(all_returned))
    assert len(all_returned) == 5


# ── unfinished meta-filter + non-default order_by ─────────────────────


async def test_list_jobs_unfinished_true_with_created_at_desc() -> None:
    """unfinished=True combined with order_by=CREATED_AT_DESC returns only
    non-terminal jobs sorted by created_at descending.

    This combination is legal per JobFilter.__post_init__, which only
    rejects cursor + non-default order_by, not unfinished + non-default
    order_by.
    """
    backend = _backend()
    oldest_unfinished = _job(status="pending", created_at=_T0)
    middle_unfinished = _job(status="running", created_at=_T0 + timedelta(minutes=10))
    newest_unfinished = _job(status="scheduled", created_at=_T0 + timedelta(minutes=20))

    # Terminal jobs with created_at values that interleave - they must
    # be excluded entirely, not just sorted to the bottom.
    old_terminal = _job(status="succeeded", created_at=_T0 + timedelta(minutes=5))
    new_terminal = _job(status="failed", created_at=_T0 + timedelta(minutes=15))

    for j in (oldest_unfinished, middle_unfinished, newest_unfinished, old_terminal, new_terminal):
        backend._jobs[j.id] = j

    rows = await backend.list_jobs(
        JobFilter(
            actor="test_actor", unfinished=True, order_by=JobSortField.CREATED_AT_DESC, limit=10
        )
    )

    assert [r.id for r in rows] == [
        newest_unfinished.id,
        middle_unfinished.id,
        oldest_unfinished.id,
    ]
    assert all(r.status in ACTIVE_STATUSES for r in rows)

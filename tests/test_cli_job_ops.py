"""Tests for the CLI job write path: `taskq job cancel`, `taskq job retry`,
`taskq job cancel-where`, `taskq job events`, `taskq job show --traceback
/--payload`, and `taskq queues depth`.

The write commands run on the Backend the admin UI's POST routes call, so
their tests pin the route semantics at the ``taskq.cli`` boundary: a fake
client seam (``taskq.cli._job_ops_client``) stands in for the short-lived
``TaskQ`` client, the same boundary-faking pattern
``tests/test_cli_job.py`` applies to ``asyncpg.connect`` for the read
commands. The admin routes' 404/409 behaviors are the contract: a missing
job is a clean not-found, a terminal job refuses cancel, a non-terminal
job refuses retry, and cancel-where refuses an empty filter before any
connection is opened.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from typer.testing import CliRunner

from taskq._ids import new_uuid
from taskq.cli import app
from taskq.types import BulkCancelResult, CancelResult

runner = CliRunner()

_JOB_ID = UUID("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f")


class _FakeTaskQ:
    """The TaskQ surface the write commands use, with every call recorded.

    ``rows`` answers ``get_row`` calls in sequence (the write commands read
    before and after the write); ``cancel_result``/``bulk_result``/
    ``retry_result`` are the canned write outcomes.
    """

    def __init__(
        self,
        *,
        rows: list[Any] | None = None,
        cancel_result: CancelResult | None = None,
        bulk_result: BulkCancelResult | None = None,
        retry_result: bool = True,
    ) -> None:
        self.rows = list(rows or [])
        self.cancel_result = cancel_result
        self.bulk_result = bulk_result
        self.retry_result = retry_result
        self.cancelled: list[tuple[UUID, str | None]] = []
        self.cancel_where_calls: list[tuple[Any, str | None]] = []
        self.retry_calls: list[UUID] = []

    async def get_row(self, job_id: UUID) -> Any:
        if self.rows:
            return self.rows.pop(0)
        return None

    async def cancel(self, job_id: UUID, reason: str | None = None) -> CancelResult:
        self.cancelled.append((job_id, reason))
        assert self.cancel_result is not None, "cancel called without a canned result"
        return self.cancel_result

    async def cancel_where(
        self, job_filter: Any, reason: str | None = None, **kwargs: Any
    ) -> BulkCancelResult:
        self.cancel_where_calls.append((job_filter, reason))
        assert self.bulk_result is not None, "cancel_where called without a canned result"
        return self.bulk_result

    async def retry_job(self, job_id: UUID) -> bool:
        self.retry_calls.append(job_id)
        return self.retry_result


def _patch_ops_client(monkeypatch: pytest.MonkeyPatch, fake: _FakeTaskQ) -> list[_FakeTaskQ]:
    """Fake the write commands' client seam at the ``taskq.cli`` boundary.

    Returns the list the fake is appended to (one entry per open), so a
    test can assert the command never opened a client at all.
    """
    opened: list[_FakeTaskQ] = []

    @asynccontextmanager
    async def fake_client(settings: Any) -> AsyncGenerator[_FakeTaskQ]:
        opened.append(fake)
        yield fake

    monkeypatch.setattr("taskq.cli._job_ops_client", fake_client)
    return opened


def _patch_reads(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fetchrow: Any = None,
    fetchval: Any = None,
    fetch: Any = None,
) -> None:
    """Fake ``asyncpg.connect`` for the read commands (events, dry-run,
    depth, show). Each canned answer is a callable taking the sql string."""

    class _FakeConn:
        async def fetchrow(self, sql: str, *args: Any) -> Any:
            return fetchrow(sql) if fetchrow else None

        async def fetchval(self, sql: str, *args: Any) -> Any:
            return fetchval(sql) if fetchval else None

        async def fetch(self, sql: str, *args: Any) -> Any:
            return fetch(sql) if fetch else []

        async def close(self) -> None: ...

    async def fake_connect(dsn: str, **_kwargs: object) -> Any:
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)


# ── job cancel ─────────────────────────────────────────────────────────


def test_cancel_pending_reports_the_terminal_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pending job has no actor to cooperate with: the cancel applies
    directly and the printed outcome states say pending -> cancelled."""
    fake = _FakeTaskQ(
        rows=[SimpleNamespace(status="pending")],
        cancel_result=CancelResult(
            job_id=_JOB_ID,
            previous_status="pending",
            new_status="cancelled",
            cancellation_initiated=True,
        ),
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "previous_status: pending" in result.output
    assert "new_status: cancelled" in result.output
    assert "cancelled directly" in result.output
    assert fake.cancelled == [(_JOB_ID, None)]


def test_cancel_running_reports_the_cooperative_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A running job stays 'running' after the request: the worker owns the
    terminal write, so the outcome must read as requested, not cancelled."""
    fake = _FakeTaskQ(
        rows=[SimpleNamespace(status="running")],
        cancel_result=CancelResult(
            job_id=_JOB_ID,
            previous_status="running",
            new_status="running",
            cancellation_initiated=True,
        ),
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel", str(_JOB_ID), "--reason", "operator stop"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "previous_status: running" in result.output
    assert "new_status: running" in result.output
    assert "cooperative cancel requested" in result.output
    assert fake.cancelled == [(_JOB_ID, "operator stop")]


def test_cancel_terminal_job_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """The admin route's 409: a cancel that 'succeeds' against a terminal
    row would read as an action taken when the write applied to nothing."""
    fake = _FakeTaskQ(rows=[SimpleNamespace(status="succeeded")])
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel", str(_JOB_ID)])

    assert result.exit_code == 1
    assert "already in a terminal state" in result.stderr
    assert fake.cancelled == [], "no cancel write may be issued for a terminal job"
    # The refusal happens after the read but before any write; the client
    # was opened (the pre-check reads through it), nothing was written.


def test_cancel_unknown_job_is_a_clean_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """The admin route's 404: a missing id is named as such, not a bare
    KeyError from the client layer."""
    fake = _FakeTaskQ(rows=[])
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel", str(_JOB_ID)])

    assert result.exit_code == 1
    assert str(_JOB_ID) in result.stderr
    assert fake.cancelled == []


def test_cancel_rejects_a_malformed_job_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-UUID argument is a usage error before any I/O, the show
    command's contract."""
    opened = _patch_ops_client(monkeypatch, _FakeTaskQ())

    result = runner.invoke(app, ["job", "cancel", "not-a-uuid"])

    assert result.exit_code == 1
    assert "expected a UUID" in result.stderr
    assert opened == [], "a malformed id must not cost a connection"


# ── job retry ──────────────────────────────────────────────────────────


def test_retry_resting_job_re_pends_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """A terminal job (failed here; the route accepts every terminal
    status) is re-pended, and the printed new status is read back after
    the write, not assumed."""
    fake = _FakeTaskQ(
        rows=[SimpleNamespace(status="failed"), SimpleNamespace(status="pending")],
        retry_result=True,
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "retry", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "previous_status: failed" in result.output
    assert "new_status: pending" in result.output
    assert fake.retry_calls == [_JOB_ID]


def test_retry_running_job_is_refused_before_the_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The admin route's 409: retrying a running job would race that
    attempt's terminal write and can execute the job twice, so the
    refusal must happen before the backend write, not after it."""
    fake = _FakeTaskQ(rows=[SimpleNamespace(status="running")])
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "retry", str(_JOB_ID)])

    assert result.exit_code == 1
    assert "not in a retryable state" in result.stderr
    assert fake.retry_calls == [], "the write guard is not the first line of defence here"


def test_retry_lost_race_reports_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    """A False from the write means the row left a retryable state between
    the pre-check and the write (a concurrent claim won): the operator
    must hear conflict, not success."""
    fake = _FakeTaskQ(
        rows=[SimpleNamespace(status="failed")],
        retry_result=False,
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "retry", str(_JOB_ID)])

    assert result.exit_code == 1
    assert "not in a retryable state" in result.stderr
    assert "no rows were changed" in result.stderr


def test_retry_unknown_job_is_a_clean_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeTaskQ(rows=[])
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "retry", str(_JOB_ID)])

    assert result.exit_code == 1
    assert str(_JOB_ID) in result.stderr
    assert fake.retry_calls == []


# ── job cancel-where ───────────────────────────────────────────────────


def test_cancel_where_rejects_an_empty_filter_before_any_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backend's EmptyFilterError, mirrored at the CLI: a naked
    full-table cancel is refused before a connection is even opened, and
    the CLI offers no allow_empty_filter bypass."""
    fake = _FakeTaskQ()
    opened = _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel-where"])

    assert result.exit_code == 1
    assert "at least one filter predicate" in result.stderr
    assert "would cancel the entire table" in result.stderr
    assert opened == [], "the empty-filter refusal must not cost a connection"
    assert fake.cancel_where_calls == []


def test_cancel_where_dry_run_counts_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--dry-run prints the matching count and sample ids and never opens
    the write client: a preview that could write is not a preview."""
    sample = [new_uuid() for _ in range(3)]
    _patch_reads(
        monkeypatch,
        fetchrow=lambda sql: {"total": 3, "sample_ids": sample},
    )
    fake = _FakeTaskQ()
    opened = _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(
        app, ["job", "cancel-where", "--queue", "default", "--status", "pending", "--dry-run"]
    )

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "dry run" in result.output.lower()
    assert "3 matching job(s)" in result.output
    for job_id in sample:
        assert str(job_id) in result.output
    assert fake.cancel_where_calls == [], "a dry run must not write"
    assert opened == [], "a dry run must not open the write client"


def test_cancel_where_bulk_cancels_and_prints_the_split(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real run reports the two arms separately: pending/scheduled
    rows cancelled directly, running rows given a cooperative request."""
    direct = (new_uuid(), new_uuid())
    requested = (new_uuid(),)
    fake = _FakeTaskQ(
        bulk_result=BulkCancelResult(
            cancelled_directly=2,
            cancel_requested=1,
            cancelled_ids=direct,
            cancel_requested_ids=requested,
        ),
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(
        app,
        [
            "job",
            "cancel-where",
            "--queue",
            "default",
            "--status",
            "pending",
            "--status",
            "running",
            "--tag",
            "stale",
            "--reason",
            "bad deploy",
        ],
    )

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "cancelled directly: 2" in result.output
    assert "cooperative cancel requested: 1" in result.output
    assert "total affected: 3" in result.output
    for job_id in (*direct, *requested):
        assert str(job_id) in result.output
    ((job_filter, reason),) = fake.cancel_where_calls
    assert reason == "bad deploy"
    assert job_filter.queue == "default"
    assert job_filter.status == ("pending", "running")
    assert job_filter.tags == ("stale",)
    assert job_filter.has_predicates()


def test_cancel_where_older_than_becomes_a_created_before_predicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--older-than is translated to a created_before cutoff the backend's
    own filter machinery applies, so the write matches what a dry run of
    the same flag previews."""
    fake = _FakeTaskQ(
        bulk_result=BulkCancelResult(
            cancelled_directly=0,
            cancel_requested=0,
            cancelled_ids=(),
            cancel_requested_ids=(),
        ),
    )
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel-where", "--queue", "default", "--older-than", "2h"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    ((job_filter, _),) = fake.cancel_where_calls
    assert job_filter.created_before is not None


def test_cancel_where_rejects_an_unknown_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown --status is the filter validation failure named with the
    valid set, not a PG enum-cast error."""
    fake = _FakeTaskQ()
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(app, ["job", "cancel-where", "--queue", "default", "--status", "nope"])

    assert result.exit_code == 1
    assert "nope" in result.stderr
    assert fake.cancel_where_calls == []


def test_cancel_where_rejects_an_unparseable_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeTaskQ()
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(
        app, ["job", "cancel-where", "--queue", "default", "--older-than", "2parsecs"]
    )

    assert result.exit_code == 1
    assert "--older-than" in result.stderr
    assert fake.cancel_where_calls == []


def test_cancel_where_rejects_a_duration_that_outruns_timedelta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A regex-valid duration past timedelta's range is the same clean
    usage error, not a raw OverflowError traceback.

    ``999999999999999w`` matches the ``--older-than`` grammar and the
    integer multiplication is exact, so the rejection has to happen at
    the constructor: the OverflowError it raises must not escape the CLI
    as an untyped crash -- the boundary's contract is the one-line
    ``--older-than`` usage error and exit 1, whichever way the text fails.
    """
    fake = _FakeTaskQ()
    _patch_ops_client(monkeypatch, fake)

    result = runner.invoke(
        app,
        ["job", "cancel-where", "--queue", "default", "--older-than", "999999999999999w"],
    )

    assert result.exit_code == 1
    assert "--older-than" in result.stderr
    assert "999999999999999w" in result.stderr
    assert fake.cancel_where_calls == []


# ── job show --traceback / --payload ───────────────────────────────────


def _show_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": _JOB_ID,
        "actor": "send_email",
        "queue": "default",
        "status": "failed",
        "priority": 0,
        "attempt": 2,
        "max_attempts": 3,
        "retry_kind": "transient",
        "created_at": "2026-01-01 00:00:00+00:00",
        "scheduled_at": "2026-01-01 00:00:00+00:00",
        "started_at": "2026-01-01 00:00:01+00:00",
        "finished_at": "2026-01-01 00:00:02+00:00",
        "error_class": "ValueError",
        "error_message": "boom",
        "idempotency_key": None,
        "error_traceback": "Traceback (most recent call last):\n  boom",
        "payload": '{"to": "ops@example.com"}',
    }
    row.update(overrides)
    return row


def test_show_default_output_stays_blob_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """The columns are fetched only under the opt-in flags: without them
    neither blob name may appear in the output, the show contract the
    existing tests pin."""
    _patch_reads(monkeypatch, fetchrow=lambda sql: _show_row())

    result = runner.invoke(app, ["job", "show", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "error_traceback" not in result.output
    assert "payload" not in result.output


def test_show_traceback_flag_prints_the_stored_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_reads(monkeypatch, fetchrow=lambda sql: _show_row())

    result = runner.invoke(app, ["job", "show", str(_JOB_ID), "--traceback"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "error_traceback:" in result.output
    assert "Traceback (most recent call last):" in result.output
    assert "payload" not in result.output


def test_show_payload_flag_prints_the_stored_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_reads(monkeypatch, fetchrow=lambda sql: _show_row())

    result = runner.invoke(app, ["job", "show", str(_JOB_ID), "--payload"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "payload:" in result.output
    assert '{"to": "ops@example.com"}' in result.output
    assert "error_traceback" not in result.output


def test_show_flags_name_an_absent_blob(monkeypatch: pytest.MonkeyPatch) -> None:
    """A None blob prints a (none) placeholder: the operator asked the
    field by name, silence would be indistinguishable from a flag that
    did nothing."""
    _patch_reads(
        monkeypatch,
        fetchrow=lambda sql: _show_row(error_traceback=None, payload=None),
    )

    result = runner.invoke(app, ["job", "show", str(_JOB_ID), "--traceback", "--payload"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "error_traceback: (none)" in result.output
    assert "payload: (none)" in result.output


# ── job events ─────────────────────────────────────────────────────────


def test_job_events_lists_the_timeline(monkeypatch: pytest.MonkeyPatch) -> None:
    events = [
        {
            "occurred_at": "2026-01-01 00:00:00+00:00",
            "kind": "state_change",
            "detail": '{"from_state": "pending", "to_state": "running"}',
        },
        {
            "occurred_at": "2026-01-01 00:00:05+00:00",
            "kind": "cancel_request",
            "detail": '{"reason": "operator stop"}',
        },
    ]
    _patch_reads(
        monkeypatch,
        fetchval=lambda sql: True,  # the jobs/jobs_archive existence probe
        fetch=lambda sql: events,
    )

    result = runner.invoke(app, ["job", "events", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "state_change" in result.output
    assert "cancel_request" in result.output
    assert "operator stop" in result.output


def test_job_events_truncates_a_long_detail_to_one_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timeline must stay a timeline: a long detail is bounded on one
    line with the dropped-character count named."""
    events = [
        {"occurred_at": "2026-01-01 00:00:00+00:00", "kind": "progress", "detail": "x" * 500},
    ]
    _patch_reads(
        monkeypatch,
        fetchval=lambda sql: True,
        fetch=lambda sql: events,
    )

    result = runner.invoke(app, ["job", "events", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "+380 characters" in result.output


def test_job_events_unknown_job_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown id must read as 'no such job', not as the empty timeline
    a bare events query would return for it."""
    _patch_reads(monkeypatch, fetchval=lambda sql: None)

    result = runner.invoke(app, ["job", "events", str(_JOB_ID)])

    assert result.exit_code == 1
    assert str(_JOB_ID) in result.stderr


def test_job_events_job_with_no_events_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_reads(
        monkeypatch,
        fetchval=lambda sql: True,
        fetch=lambda sql: [],
    )

    result = runner.invoke(app, ["job", "events", str(_JOB_ID)])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "no job_events rows" in result.output


# ── queues depth ───────────────────────────────────────────────────────


def test_queues_depth_prints_counts_and_oldest_pending_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "queue": "default",
            "pending": 12,
            "scheduled": 2,
            "running": 3,
            "failed": 1,
            "oldest_pending_age": 252.0,
        },
        {
            "queue": "reports",
            "pending": 0,
            "scheduled": 0,
            "running": 1,
            "failed": 0,
            "oldest_pending_age": None,
        },
    ]
    _patch_reads(monkeypatch, fetch=lambda sql: rows)

    result = runner.invoke(app, ["queues", "depth"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "default" in result.output
    assert "12" in result.output
    assert "reports" in result.output
    # A queue with no pending rows has no oldest-pending age: it renders
    # as '-', not as a zero age or a crash.
    assert "-" in result.output


def test_queues_depth_empty_table_says_nothing_to_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_reads(monkeypatch, fetch=lambda sql: [])

    result = runner.invoke(app, ["queues", "depth"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "nothing to report" in result.output

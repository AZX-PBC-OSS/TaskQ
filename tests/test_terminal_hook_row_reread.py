"""Unit tests for the terminal-hook row re-read's degraded paths.

``_post_write_row`` (taskq/worker/_handlers.py) re-reads the job row a
terminal hook is handed, post-write, so ``on_retry_exhausted`` /
``error_reporter`` see the failed row rather than the dispatch-time
``running`` snapshot. When that re-read fails - an infra error, or the
row gone - the handler degrades to the stale snapshot *and must report
itself*: a stale row handed to hooks silently is a failure that looks
like a success. These tests pin both degraded branches (report +
fallback), the fresh-row control, and the narrow exception boundary.
"""

from datetime import UTC, datetime

import asyncpg
import pytest
import structlog

from taskq.backend._protocol import JobId, JobRow
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.worker._handlers import (  # pyright: ignore[reportPrivateUsage]  # Why: the re-read seam is the unit under test; staging a failed get mid-terminal-write through the full consumer stack is not expressible in the in-memory harness.
    _post_write_row,
)

_START = datetime(2025, 1, 1, tzinfo=UTC)

_REREAD_EVENTS = ("terminal-hook-row-reread-failed", "terminal-hook-row-reread-missing")


class _RereadStubBackend(InMemoryBackend):
    """A real backend whose ``get`` is rigged at the boundary seam.

    The terminal write already landed by the time the re-read runs, so
    the stub models only the re-read itself: a configured return row, or
    a configured failure.
    """

    def __init__(self, *, result: JobRow | None = None, error: BaseException | None = None) -> None:
        super().__init__(clock=FakeClock(_START))
        self._result = result
        self._error = error

    async def get(self, job_id: JobId) -> JobRow | None:
        if self._error is not None:
            raise self._error
        return self._result


def _reread_events(captured: list[structlog.typing.EventDict]) -> list[structlog.typing.EventDict]:
    return [e for e in captured if e.get("event") in _REREAD_EVENTS]


async def test_fresh_reread_replaces_the_snapshot_and_stays_quiet() -> None:
    """The re-read's purpose: hooks are handed the post-write row, not the
    dispatch-time snapshot - and a healthy re-read emits no report."""
    stale = make_job_row(status="running", attempt=1)
    fresh = make_job_row(status="failed", attempt=1, error_class="MaxAttemptsExceeded")
    backend = _RereadStubBackend(result=fresh)

    with structlog.testing.capture_logs() as captured:
        row = await _post_write_row(backend, stale)

    assert row is fresh, "the hook row must be the re-read row, not the dispatch-time snapshot"
    assert _reread_events(captured) == [], f"a healthy re-read must stay quiet; captured={captured}"


@pytest.mark.parametrize(
    "infra_error",
    [
        asyncpg.PostgresError("connection was closed"),
        OSError("socket reset"),
        TimeoutError("pool acquire timed out"),
    ],
    ids=["postgres_error", "os_error", "timeout_error"],
)
async def test_failed_reread_reports_itself_and_hands_hooks_the_stale_row(
    infra_error: BaseException,
) -> None:
    """Every infra failure class the seam is written to survive takes the
    same contract: the stale row goes to the hooks (the terminal write
    already landed; dropping the hooks would be worse), and the
    degradation is reported as a warning naming the job and the error
    class - never silently."""
    stale = make_job_row(status="running", attempt=1)
    backend = _RereadStubBackend(error=infra_error)

    with structlog.testing.capture_logs() as captured:
        row = await _post_write_row(backend, stale)

    assert row is stale, "a failed re-read must degrade to the dispatch-time row"

    hits = [e for e in captured if e.get("event") == "terminal-hook-row-reread-failed"]
    assert len(hits) == 1, f"the degraded re-read must report itself; captured={captured}"
    line = hits[0]
    assert line.get("log_level") == "warning"
    assert line.get("job_id") == str(stale.id)
    assert line.get("error_class") == type(infra_error).__name__


async def test_missing_row_reports_itself_and_hands_hooks_the_stale_row() -> None:
    """A row gone between the terminal write and the re-read (retention
    sweep, operator delete) degrades identically: stale row to the hooks,
    and a warning that says the row was *missing* - distinguishable from
    the infra-failure report, since the two have different causes."""
    stale = make_job_row(status="running", attempt=1)
    backend = _RereadStubBackend(result=None)

    with structlog.testing.capture_logs() as captured:
        row = await _post_write_row(backend, stale)

    assert row is stale, "a missing re-read must degrade to the dispatch-time row"

    hits = [e for e in captured if e.get("event") == "terminal-hook-row-reread-missing"]
    assert len(hits) == 1, f"the missing-row re-read must report itself; captured={captured}"
    line = hits[0]
    assert line.get("log_level") == "warning"
    assert line.get("job_id") == str(stale.id)
    assert "error_class" not in line, "no error occurred - the row was absent"


async def test_non_infra_error_propagates() -> None:
    """The degraded fallback exists for *infrastructure* failure only. A
    programming error from the backend (a bug, not an outage) must
    propagate - swallowing it would hide the defect behind a plausible
    stale row."""
    stale = make_job_row(status="running", attempt=1)
    backend = _RereadStubBackend(error=ValueError("deserialization bug"))

    with pytest.raises(ValueError, match="deserialization bug"):
        await _post_write_row(backend, stale)

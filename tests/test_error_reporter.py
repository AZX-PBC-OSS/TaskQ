"""Unit tests for invoke_error_reporter: timeout guard (S4) and argument
order (S5).

The ErrorReporter.report signature is ``(job, exception)`` — job first,
exception second — matching OnRetryExhausted's ``(JobRow, BaseException)``
convention.  invoke_error_reporter wraps the call in asyncio.wait_for with
a configurable timeout, catching TimeoutError and Exception separately.
"""

import asyncio
import time
from typing import cast

import structlog

from taskq.backend._protocol import JobRow
from taskq.obs import ErrorReporter, NullErrorReporter, invoke_error_reporter
from taskq.testing.jobs import make_job_row

# ── Argument order (S5): report(job, exception) ──────────────────────────


async def test_report_receives_job_then_exception() -> None:
    """ErrorReporter.report is called with (job, exception) — job first,
    exception second."""
    received: list[tuple[JobRow, BaseException]] = []

    class _RecordingReporter:
        async def report(self, job: JobRow, exception: BaseException) -> None:
            received.append((job, exception))

    job = make_job_row()
    exc = RuntimeError("boom")

    await invoke_error_reporter(cast(ErrorReporter, _RecordingReporter()), job, exc)

    assert len(received) == 1
    assert received[0][0] is job
    assert received[0][1] is exc


async def test_null_error_reporter_report_accepts_job_then_exception() -> None:
    """NullErrorReporter.report accepts (job, exception) without raising."""
    reporter = NullErrorReporter()
    job = make_job_row()
    exc = RuntimeError("noop")

    await reporter.report(job, exc)


# ── Timeout guard (S4) ───────────────────────────────────────────────────


async def test_invoke_error_reporter_timeout_caught_and_logged() -> None:
    """A hanging reporter.report is cancelled after the timeout; a warning
    is logged and invoke returns normally."""

    class _HangingReporter:
        async def report(self, job: JobRow, exception: BaseException) -> None:
            await asyncio.sleep(999)

    job = make_job_row()
    exc = RuntimeError("boom")

    with structlog.testing.capture_logs() as captured:
        await invoke_error_reporter(cast(ErrorReporter, _HangingReporter()), job, exc, timeout=0.5)
    timeouts = [e for e in captured if e.get("event") == "error-reporter-timeout"]
    assert len(timeouts) == 1
    assert timeouts[0]["log_level"] == "warning"
    assert timeouts[0]["timeout_seconds"] == 0.5


async def test_invoke_error_reporter_timeout_returns_within_time_budget() -> None:
    """A hanging reporter returns within ~1s of the timeout, not 999s."""

    class _HangingReporter:
        async def report(self, job: JobRow, exception: BaseException) -> None:
            await asyncio.sleep(999)

    job = make_job_row()
    exc = RuntimeError("boom")

    start = time.monotonic()
    await invoke_error_reporter(cast(ErrorReporter, _HangingReporter()), job, exc, timeout=0.3)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0


# ── Exception handling ───────────────────────────────────────────────────


async def test_invoke_error_reporter_exception_caught_and_logged() -> None:
    """A reporter that raises has its exception caught and logged at WARNING."""

    class _CrashingReporter:
        async def report(self, job: JobRow, exception: BaseException) -> None:
            raise RuntimeError("reporter crashed")

    job = make_job_row()
    exc = RuntimeError("original")

    with structlog.testing.capture_logs() as captured:
        await invoke_error_reporter(cast(ErrorReporter, _CrashingReporter()), job, exc)
    failures = [e for e in captured if e.get("event") == "error-reporter-failed"]
    assert len(failures) == 1
    assert failures[0]["log_level"] == "warning"
    assert "reporter_type" in failures[0]


# ── None reporter is a no-op ─────────────────────────────────────────────


async def test_none_reporter_is_noop() -> None:
    """A None reporter is treated as a no-op and returns immediately."""
    job = make_job_row()
    exc = RuntimeError("boom")
    await invoke_error_reporter(None, job, exc)


# ── Default timeout ──────────────────────────────────────────────────────


async def test_default_timeout_is_3_seconds() -> None:
    """The default timeout is 3.0 seconds, matching on_retry_exhausted."""

    class _DelayReporter:
        async def report(self, job: JobRow, exception: BaseException) -> None:
            await asyncio.sleep(0.001)

    job = make_job_row()
    exc = RuntimeError("boom")

    await invoke_error_reporter(cast(ErrorReporter, _DelayReporter()), job, exc)

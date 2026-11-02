"""Pin: ``wait_terminal_with_diagnostics``'s timeout carries its evidence.

The PG-restart chaos test's recovery wait (``handle.wait(timeout=180)``)
failed once in CI (run 35748283472) as a bare ``TimeoutError`` from
``JobHandle._fetch_row_bounded_by`` - and the failure carried nothing that
could attribute it: no worker logs, no last observed row, so "the reclaim
never happened", "the attempt starved mid-run", and "the replacement
re-isolated" were indistinguishable. The e2e module now reports the
timeout through :func:`tests.e2e._assertions.wait_terminal_with_diagnostics`;
this file pins that helper's contract in the fast tier, where the timeout
shape (a healthy poll loop returning a non-terminal row until the budget
expires) is reproducible deterministically over an in-process backend.

Every case here is the production shape, not a stubbed shortcut: the
backend always answers (``get`` returns the row immediately, the loop
stays healthy), the row never turns terminal, and the budget expires.
That is exactly the CI failure's signature (the raise came from the
pre-fetch deadline check, so every fetch up to the deadline succeeded).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import TypeAdapter

from taskq.client._handle import JobHandle
from taskq.testing.jobs import make_job_row
from tests.e2e._assertions import wait_terminal_with_diagnostics

_NONE_ADAPTER = TypeAdapter(type(None))


class _NonTerminalBackend:
    """Duck-typed Backend whose ``get`` always returns the same row.

    Mirrors the chaos test's recovery window: polls succeed, the job row
    stays non-terminal (the replacement worker's reclaim and re-dispatch
    are what would move it), and the wait budget expires against a
    healthy poll loop."""

    def __init__(self, row: Any) -> None:
        self._row = row

    async def get(self, job_id: Any) -> Any:  # type: ignore[override]  # Why: duck-typed Backend stand-in; the wait loop only calls get(job_id).
        return self._row


def _handle(row: Any) -> JobHandle[None]:  # type: ignore[type-var]  # Why: JobHandle[None] via the None adapter; R cannot be inferred through a duck-typed backend.
    return JobHandle(
        row=row,
        result_adapter=_NONE_ADAPTER,
        was_existing=False,
        backend=_NonTerminalBackend(row),  # type: ignore[arg-type]  # Why: duck-typed Backend seam - the wait loop only calls .get(job_id).
    )


async def test_timeout_reports_last_observed_row_and_context() -> None:
    """RED contract: the RuntimeError must name the row the poll loop last
    observed (status AND attempt - the reclaim-never-happened vs
    attempt-starved discriminator) and the caller's context dump."""
    handle = _handle(make_job_row(status="running", attempt=1))

    async def _context() -> str:
        return "CHAOS-DIAGNOSTIC-MARKER"

    with pytest.raises(RuntimeError) as exc_info:
        await wait_terminal_with_diagnostics(
            handle,
            timeout=0.3,
            description="the replacement worker to complete the reclaimed job",
            failure_context=_context,
        )
    msg = str(exc_info.value)
    assert "the replacement worker to complete the reclaimed job" in msg
    assert "status='running'" in msg, f"the last observed row's status must be in the report: {msg}"
    assert "attempt=1" in msg, f"the last observed row's attempt must be in the report: {msg}"
    assert "CHAOS-DIAGNOSTIC-MARKER" in msg, (
        f"the caller's context dump must be in the report: {msg}"
    )


async def test_diagnostics_failure_never_masks_the_timeout() -> None:
    """RED contract: a context dump that raises (PG pool still wedged,
    Docker daemon gone) is downgraded to report content - the re-raised
    error is still the wait timeout's RuntimeError, never the dump's
    exception."""
    handle = _handle(make_job_row(status="running", attempt=2))

    async def _broken_context() -> str:
        raise OSError("docker daemon unreachable")

    with pytest.raises(RuntimeError) as exc_info:
        await wait_terminal_with_diagnostics(
            handle,
            timeout=0.3,
            description="the replacement worker to complete the reclaimed job",
            failure_context=_broken_context,
        )
    msg = str(exc_info.value)
    assert "docker daemon unreachable" in msg, (
        f"the context failure is evidence and must be reported: {msg}"
    )
    assert "status='running'" in msg and "attempt=2" in msg, (
        f"the last observed row must survive the context failure: {msg}"
    )


async def test_happy_path_is_a_bare_wait() -> None:
    """RED contract: on terminal state the wrapper IS ``handle.wait`` -
    the result passes through unwrapped, no RuntimeError, no residue."""
    row = make_job_row(status="succeeded", attempt=1)
    handle = _handle(row)

    result = await wait_terminal_with_diagnostics(
        handle,
        timeout=5.0,
        description="the replacement worker to complete the reclaimed job",
        failure_context=asyncio_sleep_never,
    )
    assert result is None


async def asyncio_sleep_never() -> str:
    """A failure_context the happy path must never await (the wait returns
    terminal before any dump is built)."""
    raise AssertionError("failure_context must not run on the happy path")

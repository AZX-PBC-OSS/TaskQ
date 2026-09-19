"""The on_cancel hook's failure paths are observable, per the module's convention.

``taskq.retry._invoke_hook`` gives every actor-supplied lifecycle hook the
same contract: user code runs beside a terminal write that has already been
decided, so neither a raising hook nor a hanging one may change what the job
does - failures are logged at WARNING under a name-keyed event and never
propagate, and a hook that outlives its timeout is abandoned with a
``*-hook-timeout`` warning carrying the bound it exceeded.

The on_success sibling pins that event surface (``on-success-hook-timeout`` /
``on-success-hook-failed`` in tests/test_on_success_hook.py); the on_cancel
suite (tests/test_on_cancel_hook.py) pins the absorption and the bound
themselves - the row still terminalises, the call still returns - but not the
signal an operator reads when cleanup goes wrong. A hook that hangs or raises
and leaves no WARNING is a failure that looks like a success: the job went
``cancelled``, the external reservation the hook existed to release silently
never was, and nothing in the logs says the cleanup did not run.

These pins assert the events the cooperative-cancel path emits on its failure
paths: the timeout warning with the configured bound, the failure warning with
the hook's error, and - the no-noise direction - neither event for a hook that
returns inside its bound.
"""

import asyncio

import structlog.testing

from taskq.retry import invoke_on_cancel
from taskq.testing.jobs import make_job_row


async def test_timed_out_on_cancel_hook_emits_the_timeout_warning() -> None:
    """A hook abandoned at its bound logs ``on-cancel-hook-timeout`` at
    WARNING, naming the job, the actor, the hook, and the configured
    timeout - the same shape the on_success sibling's pin asserts."""
    from taskq.backend._protocol import JobRow

    fired_before_cutoff: list[JobRow] = []

    async def hanging_hook(job_row: JobRow) -> None:
        fired_before_cutoff.append(job_row)
        await asyncio.sleep(3600)

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_cancel(hanging_hook, job_row, 0.05)

    assert fired_before_cutoff == [job_row], (
        "the hook must have started (it is the cleanup); only its completion "
        "is abandoned at the bound"
    )
    timeouts = [e for e in captured if e.get("event") == "on-cancel-hook-timeout"]
    assert len(timeouts) == 1, (
        "a hanging on_cancel hook abandoned at its bound must emit exactly one "
        f"on-cancel-hook-timeout warning; captured={captured}"
    )
    event = timeouts[0]
    assert event["log_level"] == "warning"
    assert event["timeout_seconds"] == 0.05
    assert event["hook"] == "on_cancel"
    assert event["job_id"] == str(job_row.id)
    assert event["actor"] == job_row.actor


async def test_raising_on_cancel_hook_emits_the_failure_warning() -> None:
    """A hook that raises is logged ``on-cancel-hook-failed`` at WARNING with
    the error rendered, and never propagates - the terminal write beside it
    has already been decided."""
    from taskq.backend._protocol import JobRow

    async def bad_hook(job_row: JobRow) -> None:
        await asyncio.sleep(0)
        raise RuntimeError("cleanup blew up")

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_cancel(bad_hook, job_row, 3.0)

    failures = [e for e in captured if e.get("event") == "on-cancel-hook-failed"]
    assert len(failures) == 1, (
        "a raising on_cancel hook must emit exactly one on-cancel-hook-failed "
        f"warning; captured={captured}"
    )
    event = failures[0]
    assert event["log_level"] == "warning"
    assert event["hook"] == "on_cancel"
    assert event["job_id"] == str(job_row.id)
    assert event["actor"] == job_row.actor
    assert "cleanup blew up" in event["error"], (
        "the failure warning must carry the hook's error so the operator can "
        "tell which cleanup did not run"
    )


async def test_a_hook_within_its_bound_emits_no_warning() -> None:
    """The no-noise direction: a hook that returns inside its bound must
    emit neither the timeout nor the failure warning - a hook that logs on
    the happy path is pager noise on every cancelled job."""
    from taskq.backend._protocol import JobRow

    calls: list[JobRow] = []

    def quick_hook(job_row: JobRow) -> None:
        calls.append(job_row)

    job_row = make_job_row()
    with structlog.testing.capture_logs() as captured:
        await invoke_on_cancel(quick_hook, job_row, 3.0)

    assert calls == [job_row]
    hook_warnings = [
        e for e in captured if e.get("event") in ("on-cancel-hook-timeout", "on-cancel-hook-failed")
    ]
    assert hook_warnings == [], (
        f"a hook that completes within its bound must stay silent; captured={captured}"
    )

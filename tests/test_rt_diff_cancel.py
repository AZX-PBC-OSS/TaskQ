# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential attacks on the cancel protocol.

write_cancel_request on pending/scheduled/running, phase idempotence,
escalation to phase 2, the mark_abandoned phase-2 guard, and poll_cancel_flags.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_uuid

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


async def _cancel_pending_and_scheduled(side: DiffSide) -> None:
    await side.enqueue("pend", scheduled_in=-1.0)
    await side.enqueue("sched", scheduled_in=300.0)
    side.record("cancel_pending", await side.write_cancel_request("pend", "bulk-drain"))
    side.record("cancel_scheduled", await side.write_cancel_request("sched", "bulk-drain"))
    # Second requests on the now-terminal rows must be no-ops.
    side.record("recancel_pending", await side.write_cancel_request("pend", "again"))
    side.record("recancel_scheduled", await side.write_cancel_request("sched", "again"))


async def test_diff_cancel_pending_scheduled_missing(pg_dsn: str) -> None:
    """write_cancel_request terminalizes pending/scheduled rows identically
    (state_change + cancel_request events, finished_at set) and refuses
    already-terminal rows."""
    mem, pg = await run_differential(_cancel_pending_and_scheduled, pg_dsn=pg_dsn)
    assert_mirror(
        "write_cancel_request on a pending/scheduled row lands 'cancelled' "
        "with the state_change and cancel_request events in that order; a "
        "second request on the terminal row returns False — identically on "
        "both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "cancel_pending": True,
        "cancel_scheduled": True,
        "recancel_pending": False,
        "recancel_scheduled": False,
    }
    assert pg["jobs"]["pend"]["events"][-2:] == [
        {
            "kind": "state_change",
            "detail": {"from_state": "pending", "to_state": "cancelled"},
        },
        {"kind": "cancel_request", "detail": {"reason": "bulk-drain"}},
    ]


async def _cancel_running_phases(side: DiffSide) -> None:
    await side.enqueue("run", scheduled_in=-1.0)
    await side.dispatch("wholder", ["default"], limit=1)
    side.record("first", await side.write_cancel_request("run", "stop"))
    # Phase 1 is idempotent: a second request on the same running row refuses.
    side.record("second", await side.write_cancel_request("run", "stop-again"))
    flags = await side.poll_cancel_flags("wholder")
    side.record("flags", sorted(flags))
    # A different worker's poll sees nothing.
    other = await side.poll_cancel_flags("wother")
    side.record("other_worker_flags", sorted(other))


async def test_diff_cancel_running_phase_and_poll(pg_dsn: str) -> None:
    """write_cancel_request on a running phase-0 row lands phase 1 with the
    cancel_request event, is idempotent, and is visible to the holder's poll."""
    mem, pg = await run_differential(_cancel_running_phases, pg_dsn=pg_dsn)
    assert_mirror(
        "write_cancel_request on a running row sets cancel_phase 1 + "
        "cancel_requested_at and writes the cancel_request event; a second "
        "request refuses; only the lock holder's poll_cancel_flags reports "
        "the flag — identically on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "first": True,
        "second": False,
        "flags": [["run", 1]],
        "other_worker_flags": [],
    }
    assert pg["jobs"]["run"]["cancel_phase"] == 1


async def _escalation_and_abandon(side: DiffSide) -> None:
    await side.enqueue("job", scheduled_in=-1.0)
    await side.enqueue("guarded", scheduled_in=-1.0)
    await side.dispatch("wholder", ["default"], limit=2)
    # The guarded job never escalates: mark_abandoned must refuse it.
    await side.write_cancel_request("guarded", "guard-probe")

    await side.write_cancel_request("job", "escalate-probe")
    # Escalation by the WRONG worker refuses.
    side.record("escalate_wrong_worker", await side.write_cancel_escalation("job", "wother"))
    # By the holder: phase 1 -> 2.
    side.record("escalate_owner", await side.write_cancel_escalation("job", "wholder"))
    # Re-escalation refuses (phase is already 2).
    side.record("escalate_again", await side.write_cancel_escalation("job", "wholder"))
    # mark_abandoned lands ONLY on the phase-2 running row.
    side.record("abandon_escalated", await side.mark_abandoned("job"))
    side.record("abandon_phase1_only", await side.mark_abandoned("guarded"))


async def test_diff_cancel_escalation_and_abandon_guard(pg_dsn: str) -> None:
    """Escalation phases and the mark_abandoned phase-2 guard, including the
    holder-preserving abandoned write both backends perform."""
    mem, pg = await run_differential(_escalation_and_abandon, pg_dsn=pg_dsn)
    assert_mirror(
        "write_cancel_escalation moves phase 1 -> 2 only for the running "
        "holder, refuses re-escalation, and mark_abandoned lands only on a "
        "phase-2 running row — preserving the last holder for audit, "
        "identically on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "escalate_wrong_worker": False,
        "escalate_owner": True,
        "escalate_again": False,
        "abandon_escalated": True,
        "abandon_phase1_only": False,
    }
    assert pg["jobs"]["job"]["status"] == "abandoned"
    # Both backends keep the holder on the abandoned row (the audit trail).
    assert pg["jobs"]["job"]["locked_by_worker"] == "wholder"
    assert pg["jobs"]["guarded"]["status"] == "running"


async def _cancel_missing_job_id(side: DiffSide) -> None:
    missing = new_uuid()
    result = await side.backend.write_cancel_request(missing, "no-such-job")  # type: ignore[arg-type]  # Why: JobId is a runtime-transparent NewType over UUID; the scenario deliberately addresses an unregistered id.
    side.record("missing_returns", result)


async def test_diff_cancel_unregistered_job_id(pg_dsn: str) -> None:
    """A cancel request for a job id that was never stored refuses (False) on
    both backends — never raises, never writes an event."""
    mem, pg = await run_differential(_cancel_missing_job_id, pg_dsn=pg_dsn)
    assert_mirror(
        "write_cancel_request for an unknown job id returns False on both "
        "backends — no exception, no event rows",
        mem,
        pg,
    )
    assert pg["records"] == {"missing_returns": False}

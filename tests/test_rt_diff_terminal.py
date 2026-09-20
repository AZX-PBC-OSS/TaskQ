# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential attacks on terminal writes.

mark_succeeded (incl. the lock-bookkeeping clear PG performs), the
mark_failed_or_retry decision arms (retry / deadline / exhausted), fencing
no-ops and ownership mismatches, the mark_snoozed and mark_retry_after
deferral arms, and retry_job.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.exceptions import WorkerOwnershipMismatch

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


# ── mark_succeeded ─────────────────────────────────────────────────────


async def _succeeded_lock_bookkeeping(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0, result_ttl_s=300.0)
    await side.dispatch("w1", ["default"], limit=1)
    ok = await side.mark_succeeded("j1", "w1", result={"v": 1})
    side.record("succeeded", ok)


async def test_diff_mark_succeeded_clears_lock_bookkeeping(pg_dsn: str) -> None:
    """PG's mark_succeeded clears locked_by_worker and lock_expires_at with the
    terminal write; the mirror must not leave a succeeded row still claiming
    its worker and lease."""
    mem, pg = await run_differential(_succeeded_lock_bookkeeping, pg_dsn=pg_dsn)
    assert_mirror(
        "a terminal success write clears the lock holder and lease "
        "(locked_by_worker = NULL, lock_expires_at = NULL) exactly like PG's "
        "mark_succeeded SET clause - a succeeded row must not keep matching "
        "every locked_by_worker-scoped reader",
        mem,
        pg,
    )
    assert pg["jobs"]["j1"]["locked_by_worker"] is None
    assert pg["jobs"]["j1"]["lock_expires_at"] is None


async def _succeeded_fencing(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0)
    await side.enqueue("j2", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=2)
    # Wrong worker: no-op.
    side.record("wrong_worker", await side.mark_succeeded("j1", "w2", result={"v": 1}))
    # Then the owner succeeds; a second (already-terminal) write no-ops.
    side.record("owner", await side.mark_succeeded("j1", "w1", result={"v": 1}))
    side.record("already_terminal", await side.mark_succeeded("j1", "w1"))
    # A NULL result succeeds too (result/result_size stay NULL).
    side.record("null_result", await side.mark_succeeded("j2", "w1"))


async def test_diff_mark_succeeded_fencing_no_ops(pg_dsn: str) -> None:
    """mark_succeeded no-ops on a wrong worker and on an already-terminal row,
    and a NULL-result success stores NULL result fields."""
    mem, pg = await run_differential(_succeeded_fencing, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_succeeded's fencing UPDATE matches only a running row owned by "
        "the caller: wrong-worker and post-terminal writes return False and "
        "write nothing; a NULL result stores NULL result fields",
        mem,
        pg,
    )
    assert pg["records"] == {
        "wrong_worker": False,
        "owner": True,
        "already_terminal": False,
        "null_result": True,
    }


# ── mark_failed_or_retry ───────────────────────────────────────────────


async def _failed_or_retry_arms(side: DiffSide) -> None:
    # Retry arm, positive delay -> scheduled with a 10s backoff.
    await side.enqueue("retry", scheduled_in=-40.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    row = await side.mark_failed_or_retry("retry", "w1", retry_delay_s=10.0)
    side.record("retry_returned", [side.token_of(row.id), row.status])

    # Deadline arm: the next retry point passes schedule_to_close.
    await side.enqueue("late", scheduled_in=-38.0, stc_in=5.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    row = await side.mark_failed_or_retry("late", "w1", retry_delay_s=10.0)
    side.record("deadline_returned", [side.token_of(row.id), row.status])

    # Exhausted arm: no delay left -> terminal failed.
    await side.enqueue("dead", scheduled_in=-37.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=1)
    row = await side.mark_failed_or_retry("dead", "w1", retry_delay_s=None)
    side.record("failed_returned", [side.token_of(row.id), row.status])

    # Retry arm, zero delay -> floored to MIN_DEFERRAL_INTERVAL (1 s) ->
    # scheduled: the anti-monopolisation floor PG's mark_retry params CTE
    # pins (GREATEST($3, MIN_DEFERRAL_INTERVAL)), the same bound the
    # deferral arms and the decision layer (retry.py) apply. This block
    # runs LAST: the quick pin asserts a 1 s backoff against the
    # scenario-end reference, so its write must sit at that reference;
    # earlier placement let runner latency between it and the reference
    # eat the 0.5 s bucket fence and read the floor backoff as "now".
    await side.enqueue("quick", scheduled_in=-39.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    row = await side.mark_failed_or_retry("quick", "w1", retry_delay_s=0.0)
    side.record("zero_delay_returned", [side.token_of(row.id), row.status])


async def test_diff_mark_failed_or_retry_arms(pg_dsn: str) -> None:
    """The retry/deadline/exhausted decision table: statuses, backoffs, error
    fields, attempt rows, and events must match arm for arm."""
    mem, pg = await run_differential(_failed_or_retry_arms, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_failed_or_retry's arms: positive delay requeues scheduled with "
        "the delay as backoff; a sub-floor delay is floored to "
        "MIN_DEFERRAL_INTERVAL and requeues scheduled; a next-retry point "
        "past schedule_to_close fails DeadlineExceeded; no delay fails "
        "terminal - identical rows, attempts, and events on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "retry_returned": ["retry", "scheduled"],
        "zero_delay_returned": ["quick", "scheduled"],
        "deadline_returned": ["late", "failed"],
        "failed_returned": ["dead", "failed"],
    }
    assert pg["jobs"]["retry"]["scheduled_at"] == 10
    assert pg["jobs"]["quick"]["scheduled_at"] == 1
    assert pg["jobs"]["late"]["error_class"] == "DeadlineExceeded"


async def _failed_or_retry_mismatch(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=1)
    try:
        await side.mark_failed_or_retry("j1", "w2", retry_delay_s=None)
        side.record("wrong_worker", "no-raise")
    except WorkerOwnershipMismatch as exc:
        side.record(
            "wrong_worker",
            [
                "WorkerOwnershipMismatch",
                side.worker_token(exc.expected),
                side.worker_token(exc.actual),
            ],
        )
    # A terminal (no longer running) row raises the same mismatch.
    await side.mark_failed_or_retry("j1", "w1", retry_delay_s=None)
    try:
        await side.mark_failed_or_retry("j1", "w1", retry_delay_s=None)
        side.record("terminal_row", "no-raise")
    except WorkerOwnershipMismatch as exc:
        side.record(
            "terminal_row",
            [
                "WorkerOwnershipMismatch",
                side.worker_token(exc.expected),
                side.worker_token(exc.actual),
            ],
        )
    # A job id that was never stored.
    missing = new_uuid()
    try:
        await side.backend.mark_failed_or_retry(
            missing,  # type: ignore[arg-type]  # Why: JobId is a runtime-transparent NewType; the scenario deliberately addresses an unregistered id.
            new_uuid(),
            ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None),
            None,
        )
        side.record("missing_job", "no-raise")
    except Exception as exc:  # Why: the exception TYPE is the observable being compared.
        side.record("missing_job", type(exc).__name__)


async def test_diff_mark_failed_or_retry_ownership_and_missing(pg_dsn: str) -> None:
    """Ownership mismatches raise identically; a missing job id must produce
    the SAME typed outcome on both backends."""
    mem, pg = await run_differential(_failed_or_retry_mismatch, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_failed_or_retry raises WorkerOwnershipMismatch for a "
        "wrong-worker or non-running row on both backends - and for a job id "
        "that was never stored, the mirror must raise the same typed error "
        "PG raises, not a different exception class",
        mem,
        pg,
    )
    assert pg["records"]["wrong_worker"] == ["WorkerOwnershipMismatch", "w2", "w1"]
    assert pg["records"]["terminal_row"] == ["WorkerOwnershipMismatch", "w1", None]


# ── mark_cancelled ─────────────────────────────────────────────────────


async def _cancelled_fencing(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0)
    await side.enqueue("j2", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=2)
    side.record("wrong_worker", await side.mark_cancelled("j1", "w2"))
    side.record("owner", await side.mark_cancelled("j1", "w1"))
    side.record("already_terminal", await side.mark_cancelled("j1", "w1"))


async def test_diff_mark_cancelled_fencing(pg_dsn: str) -> None:
    """mark_cancelled no-ops on wrong worker and post-terminal, clears lock
    bookkeeping, and writes the cancelled attempt row."""
    mem, pg = await run_differential(_cancelled_fencing, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_cancelled's fencing matches only the running owner: the "
        "cancelled transition clears lock bookkeeping and writes the "
        "cancelled attempt row - no-ops otherwise, on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "wrong_worker": False,
        "owner": True,
        "already_terminal": False,
    }
    assert pg["jobs"]["j1"]["attempts"][-1]["outcome"] == "cancelled"


# ── mark_snoozed / mark_retry_after ────────────────────────────────────


async def _snoozed_arms(side: DiffSide) -> None:
    # Actor-requested deferral: attempt refunded, snooze_count bumped.
    await side.enqueue("snooze", scheduled_in=-1.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "snoozed",
        await side.mark_snoozed(
            "snooze", "w1", 10.0, outcome="snoozed", metadata_update={"k": "v"}
        ),
    )
    # Admission denial within budget: the claim's increment is refunded,
    # blocked_count bumps. (Pinned to the 429 contract: a denial is
    # admission control, not an execution, so it can neither spend retry
    # budget nor reach a terminal arm by itself.)
    await side.enqueue("denied", scheduled_in=-1.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "denied_in_budget",
        await side.mark_snoozed("denied", "w1", 10.0, outcome="reservation_denied"),
    )
    # Admission denial AT budget (no deadline): still only a deferral -
    # the attempt ceiling governs executions, and nothing executed.
    await side.enqueue("denied-dead", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "denied_at_budget",
        await side.mark_snoozed("denied-dead", "w1", 10.0, outcome="rate_limit_denied"),
    )
    # Deadline arm: the deferral point passes schedule_to_close.
    await side.enqueue("snooze-late", scheduled_in=-1.0, stc_in=5.0)
    await side.dispatch("w1", ["default"], limit=1)
    side.record("snoozed_deadline", await side.mark_snoozed("snooze-late", "w1", 10.0))
    # Denial-keyed deadline exit: the same deadline arm on a job whose last
    # deferral was an admission denial. The terminal state_change event
    # names WHICH starvation ended the job; a plain snooze's deadline event
    # carries no denial_reason key at all.
    await side.enqueue("denied-late", scheduled_in=-1.0, stc_in=5.0)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "denied_deadline",
        await side.mark_snoozed(
            "denied-late",
            "w1",
            10.0,
            outcome="reservation_denied",
            denial_reason="unavailable",
        ),
    )


async def test_diff_mark_snoozed_arms(pg_dsn: str) -> None:
    """The snooze decision table: the attempt refund, the outcome-keyed
    counters, and the deadline arm - plain and denial-keyed, the latter
    naming its starvation in the terminal event's detail - plus the
    metadata merge and the deferral floor."""
    mem, pg = await run_differential(_snoozed_arms, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_snoozed's arms: 'snoozed' refunds the claim's attempt and "
        "counts snooze_count; a denial refunds identically and counts "
        "rate_limit_blocked_count - within budget or AT budget alike, "
        "because a denial is admission control rather than an execution and "
        "only the job's own schedule_to_close ends it (DeadlineExceeded) - "
        "identical on both backends, floors and metadata merges included",
        mem,
        pg,
    )
    assert pg["records"] == {
        "snoozed": "scheduled",
        "denied_in_budget": "scheduled",
        "denied_at_budget": "scheduled",
        "snoozed_deadline": "failed",
        "denied_deadline": "failed",
    }
    assert pg["jobs"]["snooze"]["attempt"] == 0
    assert pg["jobs"]["snooze"]["snooze_count"] == 1
    assert pg["jobs"]["snooze"]["metadata"] == {"k": "v"}
    assert pg["jobs"]["denied"]["attempt"] == 0
    assert pg["jobs"]["denied"]["rate_limit_blocked_count"] == 1
    # The at-budget denial is the 429 contract's sharpest edge: no terminal
    # exit, the increment refunded, the denial counted, no error label -
    # MaxAttemptsExceeded would assert the actor ran and failed, which a
    # saturated bucket can never claim.
    assert pg["jobs"]["denied-dead"]["status"] == "scheduled"
    assert pg["jobs"]["denied-dead"]["error_class"] is None
    # The deadline arm counts the denial that ran the job out of road, and
    # the terminal state_change event names WHICH starvation ended it; the
    # plain snooze's deadline event stays shape-unchanged, no denial_reason
    # key at all (jsonb_strip_nulls drops the absent reason).
    assert pg["jobs"]["denied-late"]["rate_limit_blocked_count"] == 1
    denied_deadline_event = next(
        e for e in pg["jobs"]["denied-late"]["events"] if e["kind"] == "state_change"
    )
    assert denied_deadline_event["detail"]["denial_reason"] == "unavailable"
    plain_deadline_event = next(
        e for e in pg["jobs"]["snooze-late"]["events"] if e["kind"] == "state_change"
    )
    assert "denial_reason" not in plain_deadline_event["detail"]
    assert pg["jobs"]["denied-dead"]["attempt"] == 0
    assert pg["jobs"]["denied-dead"]["rate_limit_blocked_count"] == 1
    assert pg["jobs"]["denied-dead"]["attempts"] == []


async def _retry_after_arms(side: DiffSide) -> None:
    # Consuming RetryAfter: a real execution - attempt stands, attempt row
    # written (outcome snoozed / RetryAfter).
    await side.enqueue("consume", scheduled_in=-1.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "consuming",
        await side.mark_retry_after("consume", "w1", 10.0, consume_budget=True),
    )
    # Non-consuming RetryAfter: deferral - attempt refunded, snooze_count
    # bumped, no attempt/event rows.
    await side.enqueue("defer", scheduled_in=-1.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "non_consuming",
        await side.mark_retry_after("defer", "w1", 10.0, consume_budget=False),
    )
    # Consuming at budget: terminal MaxAttemptsExceeded.
    await side.enqueue("consume-dead", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "consuming_at_budget",
        await side.mark_retry_after("consume-dead", "w1", 10.0, consume_budget=True),
    )
    # Non-consuming past the deadline: terminal DeadlineExceeded.
    await side.enqueue("defer-late", scheduled_in=-1.0, stc_in=5.0)
    await side.dispatch("w1", ["default"], limit=1)
    side.record(
        "non_consuming_deadline",
        await side.mark_retry_after("defer-late", "w1", 10.0, consume_budget=False),
    )


async def test_diff_mark_retry_after_arms(pg_dsn: str) -> None:
    """The RetryAfter decision table: consuming vs non-consuming budget
    semantics, budget exhaustion, and the deadline arm."""
    mem, pg = await run_differential(_retry_after_arms, pg_dsn=pg_dsn)
    assert_mirror(
        "mark_retry_after: a consuming deferral is a real execution (attempt "
        "stands, attempt row written); a non-consuming one refunds the "
        "attempt and counts snooze_count with no attempt/event rows; budget "
        "exhaustion and deadline arms fail terminal - identically on both "
        "backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "consuming": "scheduled",
        "non_consuming": "scheduled",
        "consuming_at_budget": "failed:MaxAttemptsExceeded",
        "non_consuming_deadline": "failed:DeadlineExceeded",
    }
    assert pg["jobs"]["consume"]["attempt"] == 1
    assert pg["jobs"]["defer"]["attempt"] == 0
    assert pg["jobs"]["defer"]["snooze_count"] == 1
    assert pg["jobs"]["defer"]["attempts"] == []


# ── retry_job ──────────────────────────────────────────────────────────


async def _retry_job_gates(side: DiffSide) -> None:
    await side.enqueue("failed", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=1)
    await side.mark_failed_or_retry("failed", "w1", retry_delay_s=None)
    side.record("retry_failed", await side.retry_job("failed"))
    # A failed row carrying a still-pending cancel request: the re-run
    # opens a fresh epoch, so the whole cancel trail must go with the
    # spent one - PG's SET clause clears cancel_requested_at alongside
    # cancel_phase (the audit columns the TERMINAL writes deliberately
    # keep become the inherited state a re-pend must not carry).
    await side.plant(
        "failed-cancel",
        status="failed",
        cancel_phase=1,
        started_ago_s=30.0,
        lock_expired_ago_s=None,
    )
    side.record("retry_failed_cancel", await side.retry_job("failed-cancel"))
    # An abandoned row IS retryable: a deploy interrupted the job, it did
    # not fail - the operator re-run contract admits every resting state
    # (succeeded and abandoned included); only an actively-running or
    # already-queued row is off-limits. Planted directly: the escalation
    # path's event-detail divergence is pinned separately in
    # tests/test_rt_diff_cancel.py.
    await side.plant(
        "abandoned",
        status="abandoned",
        worker_token="w2",
        cancel_phase=2,
        started_ago_s=30.0,
        lock_expired_ago_s=None,
    )
    side.record("retry_abandoned", await side.retry_job("abandoned"))
    # A live pending row is not retryable.
    await side.enqueue("pending", scheduled_in=-1.0)
    side.record("retry_pending", await side.retry_job("pending"))


async def test_diff_retry_job_status_gates(pg_dsn: str) -> None:
    """retry_job revives failed/crashed/cancelled/abandoned rows - keeping
    attempt monotonic, raising the ceiling, clearing errors, result, and
    the cancel trail - and refuses only actively-running or already-queued
    rows."""
    mem, pg = await run_differential(_retry_job_gates, pg_dsn=pg_dsn)
    assert_mirror(
        "retry_job admits failed/crashed/cancelled rows AND abandoned ones "
        "(an operator re-run is 'run this again'; only a live attempt is "
        "off-limits), keeping attempt at its spent value (monotonic - the "
        "admin-retry precedent) and raising max_attempts to "
        "GREATEST(max_attempts, attempt + 1) so the budget gates open, "
        "clearing errors, result, and the whole cancel trail (cancel_phase "
        "AND cancel_requested_at - the re-run is a fresh epoch), and "
        "rescheduling at now; a live pending row refuses - identically on "
        "both backends",
        mem,
        pg,
    )
    # retry_abandoned is True on both backends: the operator re-run
    # contract admits every resting state including abandoned (a deploy
    # interrupted the job; it did not fail) - only 'running' (a live
    # attempt the re-pend would race) and 'pending'/'scheduled' (already
    # queued) refuse.
    assert pg["records"] == {
        "retry_failed": True,
        "retry_failed_cancel": True,
        "retry_abandoned": True,
        "retry_pending": False,
    }
    assert pg["jobs"]["failed"]["status"] == "pending"
    assert pg["jobs"]["failed"]["attempt"] == 1, (
        "MONOTONIC-ATTEMPT CONTRACT: retry_job must never reset the "
        "attempt counter - a reset revisits the spent epoch's attempt "
        "numbers and the next attempt-row write collides on "
        "job_attempts_pkey (pinned in tests/test_retry_job_attempt_epoch_pk.py)."
    )
    assert pg["jobs"]["failed"]["max_attempts"] == 2, (
        "CEILING-RAISE CONTRACT: retry_job must raise max_attempts to "
        "GREATEST(max_attempts, attempt + 1) so the budget gates open for "
        "the re-run."
    )
    assert pg["jobs"]["failed"]["error_class"] is None
    assert pg["jobs"]["failed-cancel"]["cancel_phase"] == 0
    assert pg["jobs"]["failed-cancel"]["cancel_requested_at"] is None, (
        "FRESH-EPOCH CONTRACT: PG's retry_job SET clause clears "
        "cancel_requested_at alongside cancel_phase - a re-run must not "
        "inherit the spent epoch's cancel trail."
    )
    # The abandoned re-run gets the same fresh epoch: re-pended pending,
    # its phase-2 cancel trail cleared with the spent one.
    assert pg["jobs"]["abandoned"]["status"] == "pending"
    assert pg["jobs"]["abandoned"]["cancel_phase"] == 0
    assert pg["jobs"]["abandoned"]["cancel_requested_at"] is None

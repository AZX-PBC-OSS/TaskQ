# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential attacks on the maintenance sweeps (S1 reclaim, S2 deadline, S3 promotion).

S1 branches (retry-exhausted / cancel-in-flight / crashed), the +60s cancel
carve-out margin, the bounded-batch cap, S2's DeadlineExceeded terminal,
and S3's due promotion — each driven through real dispatch where possible,
with row-timestamp mutation standing in for clock advance on the PG side.
"""

from __future__ import annotations

import pytest

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


async def _s1_branches_and_cap(side: DiffSide) -> None:
    # Real dispatch plants the running rows, then the locks are expired by
    # row mutation (the PG equivalent of FakeClock advancing past the lease).
    await side.enqueue("retry", scheduled_in=-30.0, max_attempts=3)
    await side.enqueue("crash", scheduled_in=-29.0, max_attempts=1, retry_kind="non_retryable")
    await side.enqueue("cancel", scheduled_in=-28.0, max_attempts=1, retry_kind="non_retryable")
    await side.dispatch("wholder", ["default"], limit=3)
    await side.write_cancel_request("cancel", "sweep-probe")
    await side.mutate("retry", lock_expired_ago_s=10.0)
    await side.mutate("crash", lock_expired_ago_s=10.0)
    # The cancel-in-flight row needs the deep margin (grace+grace+60s).
    await side.mutate("cancel", lock_expired_ago_s=130.0)

    first = await side.sweep_reclaim(batch_size=2)
    rest = await side.sweep_reclaim(batch_size=2)
    drained = await side.sweep_reclaim(batch_size=2)
    side.record("sweep_counts", [first, rest, drained])


async def test_diff_sweep1_branches_cap_and_drain(pg_dsn: str) -> None:
    """Sweep 1's three-way CASE, under a cap, drained to completion: identical
    per-branch states, retry backoff, cleared lock bookkeeping, and audit rows."""
    mem, pg = await run_differential(_s1_branches_and_cap, pg_dsn=pg_dsn)
    assert_mirror(
        "reclaim sweep: retry branch requeues with a 5s backoff and a clean "
        "cancel slate; exhausted branch lands crashed (or cancelled when a "
        "cancel was in-flight); every branch clears the holder and lease and "
        "writes the crashed attempt + lock_expired event — identically on "
        "both backends, one bounded batch at a time",
        mem,
        pg,
    )
    assert pg["records"]["sweep_counts"] == [2, 1, 0]
    assert pg["jobs"]["retry"]["status"] == "pending"
    assert pg["jobs"]["retry"]["scheduled_at"] == 5
    assert pg["jobs"]["crash"]["status"] == "crashed"
    assert pg["jobs"]["cancel"]["status"] == "cancelled"


async def _s1_cancel_margin_window(side: DiffSide) -> None:
    await side.enqueue("inflight", scheduled_in=-30.0, max_attempts=3)
    await side.dispatch("wholder", ["default"], limit=1)
    await side.write_cancel_request("inflight", "margin-probe")
    # Expired only 40s ago: past the lease, but NOT past the
    # cancel_grace + cleanup_grace + 60s deep margin — the carve-out holds.
    await side.mutate("inflight", lock_expired_ago_s=40.0)
    held = await side.sweep_reclaim()
    side.record("shallow_sweep", held)
    # Now past the deep margin (130s): the carve-out releases it.
    await side.mutate("inflight", lock_expired_ago_s=130.0)
    deep = await side.sweep_reclaim()
    side.record("deep_sweep", deep)


async def test_diff_sweep1_cancel_carveout_margin(pg_dsn: str) -> None:
    """The cancel carve-out: an in-flight cancel is left alone until the lock
    has been expired past grace+grace+60s, then reclaimed (retry branch for a
    retryable job: requeued pending with a clean cancel slate)."""
    mem, pg = await run_differential(_s1_cancel_margin_window, pg_dsn=pg_dsn)
    assert_mirror(
        "the reclaim sweep's cancel carve-out admits an in-flight-cancel job "
        "only once its lock has been expired past cancel_grace + "
        "cleanup_grace + a flat 60s margin, requeueing it with a clean "
        "cancel slate — on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {"shallow_sweep": 0, "deep_sweep": 1}
    assert pg["jobs"]["inflight"]["status"] == "pending"
    assert pg["jobs"]["inflight"]["cancel_phase"] == 0


async def _s2_deadline_pending_never_dispatched(side: DiffSide) -> None:
    # schedule_to_close already past at enqueue: the row lands pending and
    # S2 fails it without ever being dispatched.
    await side.enqueue("doomed", scheduled_in=-1.0, stc_in=-10.0)
    await side.enqueue("healthy", scheduled_in=-1.0, stc_in=300.0)
    swept = await side.sweep_deadline()
    side.record("swept", swept)
    again = await side.sweep_deadline()
    side.record("swept_again", again)


async def test_diff_sweep2_deadline_never_dispatched(pg_dsn: str) -> None:
    """S2 fails a never-dispatched past-deadline row with the DeadlineExceeded
    attempt row (worker NULL, duration NULL) and state_change event."""
    mem, pg = await run_differential(_s2_deadline_pending_never_dispatched, pg_dsn=pg_dsn)
    assert_mirror(
        "the deadline sweep fails pending/scheduled rows whose "
        "schedule_to_close has passed, writing the same terminal fields, "
        "attempt row, and event on both backends — and only those rows",
        mem,
        pg,
    )
    assert pg["records"] == {"swept": 1, "swept_again": 0}
    assert pg["jobs"]["doomed"]["status"] == "failed"
    assert pg["jobs"]["doomed"]["attempts"] == [
        {
            "attempt": 0,
            "outcome": "failed",
            "error_class": "DeadlineExceeded",
            "error_message": "schedule_to_close reached before next dispatch",
            "worker": None,
            "duration_s": None,
            "finished": True,
        }
    ]
    assert pg["jobs"]["healthy"]["status"] == "pending"


async def _s2_deadline_retried_job(side: DiffSide) -> None:
    # A job that RAN once (attempt row written at (job, 1) by the retry arm)
    # and rescheduled; its deadline then passes while it waits. S2 selects
    # it — and PG's batched attempt INSERT targets (job_id, attempt) again.
    await side.enqueue("retried", scheduled_in=-30.0, stc_in=300.0, max_attempts=3)
    await side.dispatch("w1", ["default"], limit=1)
    await side.mark_failed_or_retry("retried", "w1", retry_delay_s=10.0)
    await side.mutate("retried", stc_in_s=-10.0)
    try:
        swept = await side.sweep_deadline()
        side.record("sweep", swept)
    except Exception as exc:  # Why: the typed outcome IS the observable; recorded by name.
        side.record("sweep", type(exc).__name__)


async def test_diff_sweep2_deadline_after_retry_attempt_pk(pg_dsn: str) -> None:
    """S2 on a retried row: PG's attempt INSERT collides with the retry arm's
    existing (job_id, attempt) row and ABORTS the sweep; the mirror silently
    appends a second attempt row and fails the job.

    The mirror misleads here: a test driving this sequence in memory sees a
    clean DeadlineExceeded transition, while the same sequence on PG leaves
    the job 'scheduled' and tears the sweep down with a constraint violation
    (deliberately non-transient in the leader's error classification).
    """
    mem, pg = await run_differential(_s2_deadline_retried_job, pg_dsn=pg_dsn)
    assert_mirror(
        "the deadline sweep on a row whose (job_id, attempt) attempt row "
        "already exists (any retried row) must behave identically on both "
        "backends — today PG aborts on job_attempts_pkey while the mirror "
        "writes a duplicate attempt row and completes the transition",
        mem,
        pg,
    )


async def test_diff_sweep2_deadline_retried_job_does_not_wedge(pg_dsn: str) -> None:
    """Red-team test for issue #176: S2 must fail an overdue job that already
    ran an attempt (retry arm wrote a job_attempts row at the same attempt
    number) WITHOUT raising ``UniqueViolationError`` on the ``job_attempts_pkey``
    and without leaving the job wedged at 'scheduled' forever.

    Expected/correct behavior: the deadline sweep should terminally fail the
    job (status='failed') exactly as it does for a never-dispatched overdue
    job, on both backends, with no unhandled exception. Today on PG the sweep's
    batched INSERT collides with the (job_id, attempt) row the retry arm
    already wrote, raises UniqueViolationError, rolls back the whole sweep
    batch, and leaves the job stuck at 'scheduled' — reproducing exactly what
    issue #176 describes (and rewedging on every subsequent tick).
    """
    _mem, pg = await run_differential(_s2_deadline_retried_job, pg_dsn=pg_dsn)
    # The sweep must not have raised — 'sweep' should record the swept count
    # (an int), never an exception class name like "UniqueViolationError".
    assert pg["records"]["sweep"] != "UniqueViolationError", (
        f"deadline sweep raised UniqueViolationError on job_attempts_pkey "
        f"instead of terminally failing the retried-then-overdue job "
        f"(pg records: {pg['records']!r})"
    )
    # The job must actually leave 'scheduled' and land 'failed', matching the
    # never-dispatched-overdue-job contract (test_diff_sweep2_deadline_never_dispatched).
    assert pg["jobs"]["retried"]["status"] == "failed", (
        f"job left wedged at {pg['jobs']['retried']['status']!r} instead of "
        f"'failed' — sweep 2 never terminally wrote the overdue retried job "
        f"(this is the production wedge from issue #176: the job, and every "
        f"other overdue job batched behind it, never leaves pending/scheduled)"
    )


async def _s3_promotion(side: DiffSide) -> None:
    await side.enqueue("due", scheduled_in=10.0)
    await side.enqueue("notdue", scheduled_in=300.0)
    before = await side.sweep_promote()
    side.record("before_due", before)
    # Drive both clocks past 'due': memory moves its row, PG binds
    # clock_timestamp() arithmetic — the same logical instant.
    await side.mutate("due", scheduled_in_s=-1.0)
    after = await side.sweep_promote()
    side.record("after_due", after)
    drained = await side.sweep_promote()
    side.record("drained", drained)


async def test_diff_sweep3_promotion(pg_dsn: str) -> None:
    """S3 promotes only due scheduled rows to pending, leaving the rest scheduled."""
    mem, pg = await run_differential(_s3_promotion, pg_dsn=pg_dsn)
    assert_mirror(
        "the promotion sweep flips due scheduled rows to pending (writing no "
        "event rows — promotion is scheduler bookkeeping) and leaves "
        "not-yet-due rows untouched, on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {"before_due": 0, "after_due": 1, "drained": 0}
    assert pg["jobs"]["due"]["status"] == "pending"
    assert pg["jobs"]["notdue"]["status"] == "scheduled"

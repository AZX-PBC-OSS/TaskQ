# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Differential attacks on batch outcomes (the apply_batch_terminal_outcome table).

The shared policy hook runs against the Backend protocol on both sides, so
the differential isolates the backend batch operations themselves:
increment/reset counters, the NOT EXISTS completion guard, abort-wins-over-
complete, the abort members' terminal fields, and the noop/snoozed neutral
arms.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskq._ids import new_uuid

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

_BATCH_META_TEMPLATE = "batch-meta"


async def _member(side: DiffSide, token: str, batch_token: str, **kwargs: Any) -> None:
    """Enqueue one batch member carrying its batch_id metadata."""
    bid = side.batch_id_of(batch_token)
    await side.enqueue(token, metadata={"batch_id": str(bid)}, **kwargs)


async def _noop_neutral(side: DiffSide) -> None:
    await side.create_batch("b1", expected_size=2, failure_threshold=1)
    await _member(side, "m1", "b1", scheduled_in=-1.0)
    await _member(side, "m2", "b1", scheduled_in=-1.0)
    # The neutral outcomes: no counter, status, or completion movement.
    await side.apply_outcome("m1", "noop")
    await side.apply_outcome("m2", "snoozed")
    side.record("non_terminal", await side.batch_non_terminal("b1"))


async def test_diff_batch_outcome_noop_neutral(pg_dsn: str) -> None:
    """noop and the deferral outcomes leave every batch counter and status
    untouched (the consumer's no-op semantics, compared across backends)."""
    mem, pg = await run_differential(_noop_neutral, pg_dsn=pg_dsn)
    assert_mirror(
        "apply_batch_terminal_outcome treats 'noop' and the deferral "
        "outcomes as batch-neutral on both backends: no failure counter "
        "movement, no completion attempt effect, batch stays active",
        mem,
        pg,
    )
    assert pg["records"] == {"non_terminal": 2}
    assert pg["batches"]["b1"]["status"] == "active"
    assert pg["batches"]["b1"]["consecutive_failures"] == 0


async def _succeeded_completes(side: DiffSide) -> None:
    await side.create_batch("b1", expected_size=2, failure_threshold=None)
    await _member(side, "m1", "b1", scheduled_in=-2.0)
    await _member(side, "m2", "b1", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=2)
    await side.mark_succeeded("m1", "w1", result={"v": 1})
    await side.apply_outcome("m1", "succeeded")
    # m2 still pending: the optimistic completion attempt must be vetoed.
    mid_row = await side.batch_row("b1")
    side.record("mid_complete_status", mid_row.status if mid_row is not None else None)
    await side.mark_succeeded("m2", "w1", result={"v": 2})
    await side.apply_outcome("m2", "succeeded")
    side.record("final_non_terminal", await side.batch_non_terminal("b1"))


async def test_diff_batch_outcome_succeeded_completes(pg_dsn: str) -> None:
    """Success resets the failure counter and the last terminal member's
    completion attempt lands (earlier attempts are vetoed by the NOT EXISTS
    guard)."""
    mem, pg = await run_differential(_succeeded_completes, pg_dsn=pg_dsn)
    assert pg["records"] == {"mid_complete_status": "active", "final_non_terminal": 0}
    assert pg["batches"]["b1"]["status"] == "complete"
    assert pg["batches"]["b1"]["consecutive_failures"] == 0
    assert_mirror(
        "the succeeded outcome resets consecutive_failures and the "
        "self-arbitrating complete_batch lands exactly when the last member "
        "turns terminal — identical batch status and counters on both "
        "backends (the member lock-bookkeeping divergence this snapshot also "
        "surfaces is pinned in tests/test_rt_diff_terminal.py)",
        mem,
        pg,
    )


async def _failed_below_threshold_then_complete(side: DiffSide) -> None:
    await side.create_batch("b1", expected_size=2, failure_threshold=2)
    await _member(side, "m1", "b1", scheduled_in=-2.0, max_attempts=1)
    await _member(side, "m2", "b1", scheduled_in=-1.0)
    await side.dispatch("w1", ["default"], limit=2)
    await side.mark_failed_or_retry("m1", "w1", retry_delay_s=None)
    await side.apply_outcome("m1", "failed")
    row = await side.batch_row("b1")
    side.record("counter_after_fail", row.consecutive_failures if row else None)
    side.record("status_after_fail", row.status if row else None)
    # m1's failure raised the counter to 1; m2's success resets and completes.
    await side.mark_succeeded("m2", "w1", result={"v": 2})
    await side.apply_outcome("m2", "succeeded")
    side.record("non_terminal_after_success", await side.batch_non_terminal("b1"))


async def test_diff_batch_outcome_failed_below_threshold(pg_dsn: str) -> None:
    """A failure below the threshold increments the counter and still attempts
    completion; a later success resets the counter and completes."""
    mem, pg = await run_differential(_failed_below_threshold_then_complete, pg_dsn=pg_dsn)
    assert pg["records"] == {
        "counter_after_fail": 1,
        "status_after_fail": "active",
        "non_terminal_after_success": 0,
    }
    assert pg["batches"]["b1"]["status"] == "complete"
    assert_mirror(
        "the failed outcome increments consecutive_failures, attempts "
        "completion only when the member set is terminal, and a subsequent "
        "succeeded outcome resets the counter — identical on both backends "
        "(the member lock-bookkeeping divergence this snapshot also surfaces "
        "is pinned in tests/test_rt_diff_terminal.py)",
        mem,
        pg,
    )


async def _failed_at_threshold_aborts(side: DiffSide) -> None:
    await side.create_batch("b1", expected_size=3, failure_threshold=1)
    await _member(side, "victim1", "b1", scheduled_in=-3.0, max_attempts=1)
    await _member(side, "runner", "b1", scheduled_in=-2.0)
    # victim2 is enqueued LAST and never dispatched: it stays pending so the
    # abort's member-cancel arm has a row to act on.
    await _member(side, "victim2", "b1", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=2)
    # The runner succeeds first: its completion attempt is vetoed (failures
    # pending). Then one failure trips the threshold and ABORTS the batch —
    # abort wins over complete, and the pending member is cancelled.
    await side.mark_succeeded("runner", "w1", result={"v": 1})
    await side.apply_outcome("runner", "succeeded")
    await side.mark_failed_or_retry("victim1", "w1", retry_delay_s=None)
    await side.apply_outcome("victim1", "failed")
    side.record("non_terminal_after_abort", await side.batch_non_terminal("b1"))


async def test_diff_batch_outcome_failed_at_threshold_aborts(pg_dsn: str) -> None:
    """A failure reaching the threshold aborts the batch (winning over the
    completion attempt) and cancels every pending/scheduled member with the
    batch-abort terminal fields."""
    mem, pg = await run_differential(_failed_at_threshold_aborts, pg_dsn=pg_dsn)
    assert pg["records"] == {"non_terminal_after_abort": 0}
    assert pg["batches"]["b1"]["status"] == "aborted"
    assert pg["jobs"]["victim2"]["status"] == "cancelled"
    assert pg["jobs"]["victim2"]["error_class"] == "BatchAbortedError"
    assert pg["jobs"]["victim2"]["cancel_phase"] == 2
    assert_mirror(
        "the failed outcome at the threshold aborts the batch — cancelling "
        "pending/scheduled members with error_class BatchAbortedError, "
        "cancel_phase 2, cancel_requested_at and finished_at set — and the "
        "aborted batch is never also completed, identically on both backends "
        "(the member lock-bookkeeping divergence this snapshot also surfaces "
        "is pinned in tests/test_rt_diff_terminal.py)",
        mem,
        pg,
    )


async def _terminal_batch_counter_writes(side: DiffSide) -> None:
    await side.create_batch("b1", expected_size=1, failure_threshold=1)
    await _member(side, "m1", "b1", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=1)
    await side.mark_failed_or_retry("m1", "w1", retry_delay_s=None)
    await side.apply_outcome("m1", "failed")  # threshold 1 -> aborted
    # Counter writes against a non-active batch are inert on PG (the updated
    # CTE matches nothing) — the mirror must agree.
    side.record("increment_on_aborted", list(await side.batch_increment("b1")))
    side.record("reset_on_aborted", await side.batch_reset("b1"))
    # And a completion attempt against the aborted batch must not complete.
    await side.batch_complete("b1")
    final_row = await side.batch_row("b1")
    side.record("complete_on_aborted", final_row.status if final_row is not None else None)
    # Counter reads on a MISSING batch row.
    side.record("increment_missing", list(await side.backend.increment_batch_failures(new_uuid())))
    side.record("reset_missing", await side.backend.reset_batch_failures(new_uuid()))


async def test_diff_batch_counters_on_terminal_and_missing(pg_dsn: str) -> None:
    """increment/reset no-op on a terminal batch and return the missing-row
    shape on both backends; complete never un-aborts."""
    mem, pg = await run_differential(_terminal_batch_counter_writes, pg_dsn=pg_dsn)
    assert_mirror(
        "batch counter writes match only active rows: increment/reset "
        "against a terminal batch return the inert shape (0, None)/None, a "
        "missing batch returns the same, and complete_batch never flips an "
        "aborted row — identically on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "increment_on_aborted": [0, None],
        "reset_on_aborted": None,
        "complete_on_aborted": "aborted",
        "increment_missing": [0, None],
        "reset_missing": None,
    }


async def _finalizer_exclusion_from_counts(side: DiffSide) -> None:
    await side.enqueue("finalizer", scheduled_in=-1.0)
    await side.create_batch(
        "b1",
        expected_size=1,
        failure_threshold=None,
        finalizer_token="finalizer",
    )
    await _member(side, "m1", "b1", scheduled_in=-1.0, max_attempts=1)
    await side.dispatch("w1", ["default"], limit=2)
    await side.mark_failed_or_retry("m1", "w1", retry_delay_s=None)
    await side.apply_outcome("m1", "failed")
    # The batch row's finalizer_job_id must round-trip to the finalizer job.
    row = await side.batch_row("b1")
    side.record("finalizer_id", "missing" if row is None else row.finalizer_job_id)


async def test_diff_batch_finalizer_job_id_round_trip(pg_dsn: str) -> None:
    """create_batch stores the finalizer job id and it reads back through the
    protocol identically."""
    mem, pg = await run_differential(_finalizer_exclusion_from_counts, pg_dsn=pg_dsn)
    assert_mirror(
        "create_batch persists finalizer_job_id and get_batch reads it back on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {"finalizer_id": "finalizer"}

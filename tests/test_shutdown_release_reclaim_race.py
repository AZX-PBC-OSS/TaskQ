"""Red-team: RELEASING-phase release must not double-refund against a
concurrent reclaim sweep on the same row.

Hypothesis under test: ``mark_interrupted``'s fence
(``status='running' AND locked_by_worker=$worker_id AND attempt=$attempt AND
cancel_phase=0``) and the leader's crash-reclaim sweep
(``sweep_expired_locks``, eligible once the lease has expired) both operate
on rows with ``status='running'``. If a departing worker's lock lease has
already expired by the time the RELEASING phase reaches a row -- a slow
shutdown racing a leader's reclaim tick -- both writers could believe they
are the one entitled to release the row: the reclaim sweep would spend it as
a crash (``_ATTEMPT_REFUND_SQL`` is NOT used on the crash-reclaim retry arm;
the attempt is left as claimed) while ``mark_interrupted`` would leave the
attempt as claimed too (the interrupt arm carries no refund since #287: the
attempt started executing), and if both landed the job could be written
twice into job_events / job_attempts for the same transition.

This fires ``mark_interrupted`` and ``reclaim_expired_locks`` concurrently
against a single row whose lease has already expired, and pins that exactly
one writer wins the row (the other reads back a no-op / finds nothing to
reclaim) and the attempt only ever moves by the amount one writer is owed.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration


async def _claim_like_dispatch(
    deps: WorkerDeps, schema: str, job_id: JobId, worker_id: JobId
) -> int:
    """Claim a row the same shape dispatch_batch's CTE produces, with an
    ALREADY-EXPIRED lease -- the state a row is in when a departing worker's
    RELEASING phase is slow enough to overlap a leader's reclaim tick."""
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE \"{schema}\".jobs SET status='running', "  # noqa: S608
            f"locked_by_worker=$1, lock_expires_at=now() - interval '1 second', "
            f"started_at=clock_timestamp(), last_heartbeat_at=clock_timestamp(), "
            f"attempt = attempt + 1 "
            f"WHERE id = $2 RETURNING attempt",
            worker_id,
            job_id,
        )
    assert row is not None
    return int(row["attempt"])


async def test_releasing_phase_does_not_double_refund_against_a_concurrent_reclaim(
    clean_jobs_app: JobsApp,
) -> None:
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    job_id = JobId(new_uuid())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    enqueued = await backend.get(job_id)
    assert enqueued is not None
    baseline_attempt = enqueued.attempt

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    claimed_attempt = await _claim_like_dispatch(deps, schema, job_id, worker_id)
    assert claimed_attempt == baseline_attempt + 1

    # Fire the RELEASING-phase release and the leader's crash-reclaim sweep
    # concurrently against the SAME row: both see status='running' and an
    # expired lease, and both believe they may be the one to transition it.
    interrupted_result, reclaimed_count = await asyncio.gather(
        backend.mark_interrupted(
            job_id,
            worker_id,
            attempt=claimed_attempt,
            hold=timedelta(seconds=0),
        ),
        backend.reclaim_expired_locks(timedelta(seconds=0), timedelta(seconds=0)),
    )

    final = await backend.get(job_id)
    assert final is not None

    # Exactly one writer must have taken the row: either mark_interrupted
    # released it (reclaim then finds nothing), or the reclaim sweep beat it
    # to the row (mark_interrupted's fence then reads back "noop").
    released = interrupted_result != "noop"
    reclaimed = reclaimed_count >= 1
    assert released != reclaimed, (
        "exactly one of the RELEASING-phase release and the concurrent "
        f"reclaim sweep should have transitioned the row; got "
        f"mark_interrupted={interrupted_result!r}, reclaimed_count={reclaimed_count}, "
        f"final row status={final.status!r} attempt={final.attempt}"
    )

    if released:
        assert final.status in ("pending", "scheduled"), (
            f"a released row must come back to the fleet, not stay {final.status!r}"
        )
        assert final.attempt == claimed_attempt, (
            "the release must NOT refund the attempt (the attempt started "
            f"executing; issue #287): it stays at the claimed epoch "
            f"{claimed_attempt}; got {final.attempt}"
        )
    else:
        # The reclaim sweep won the row first: it is the crash-recovery
        # path's outcome that governs, not a second refund on top of it.
        assert final.attempt >= baseline_attempt, (
            "a reclaim-sweep-won row must never end up refunded BELOW the "
            f"claimed baseline by an extra mark_interrupted write; got "
            f"{final.attempt} < {baseline_attempt}"
        )

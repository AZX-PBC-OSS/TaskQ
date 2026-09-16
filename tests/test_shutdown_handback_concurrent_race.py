"""Red-team: concurrent handback calls for the same worker must not double-refund.

Hypothesis under test: the design doc for drain_local_queue_to_pending claims
"a second pass matches no rows, so the two passes together are exactly-once"
for the sequenced DRAINING-then-producer-exit case. But is that guarantee
still true under a genuine RACE -- two callers invoking the helper
concurrently for the same worker_id, rather than sequenced one-after-another?

If PG's row-level locking does not serialize the two UPDATEs the way the
doctrine comment assumes, a claimed-but-unstarted row could be refunded
twice, handing it extra retry budget it was never owed.

This test claims a row with a direct SQL UPDATE shaped exactly like the
production dispatch claim CTE (status='running', locked_by_worker=<id>,
attempt incremented by 1) -- dispatch_batch itself is not used here because
unrelated in-flight work elsewhere in this tree currently breaks it -- and
then fires two concurrent drain_local_queue_to_pending calls with
asyncio.gather.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import drain_local_queue_to_pending

pytestmark = pytest.mark.integration


async def _claim_like_dispatch(
    deps: WorkerDeps, schema: str, job_id: JobId, worker_id: UUID
) -> None:
    """Claim a row the same shape dispatch_batch's CTE produces.

    status='running', locked_by_worker=<worker>, lock_expires_at set,
    started_at stamped, attempt incremented by 1 -- everything
    drain_local_queue_to_pending's WHERE clause and refund logic look at.
    """
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status='running', "  # noqa: S608
            f"locked_by_worker=$1, lock_expires_at=now() + interval '60 seconds', "
            f"started_at=clock_timestamp(), attempt = attempt + 1 "
            f"WHERE id = $2",
            worker_id,
            job_id,
        )


async def test_concurrent_handback_calls_refund_exactly_once(
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

    await _claim_like_dispatch(deps, schema, job_id, worker_id)
    claimed = await backend.get(job_id)
    assert claimed is not None
    assert claimed.attempt == baseline_attempt + 1, "claim setup should have incremented attempt"
    assert claimed.status == "running"

    # Fire two handback calls concurrently for the SAME worker_id: a true
    # race, not the sequenced DRAINING-then-producer-exit ordering the
    # doctrine comment describes.
    results = await asyncio.gather(
        drain_local_queue_to_pending(deps, worker_id),
        drain_local_queue_to_pending(deps, worker_id),
    )

    total_rows_repended = sum(results)
    assert total_rows_repended == 1, (
        "exactly one of the two concurrent calls should have re-pended the "
        f"row (PG row-level locking should serialize the UPDATEs); got total "
        f"rowcount {total_rows_repended} from results {results}"
    )

    final = await backend.get(job_id)
    assert final is not None
    assert final.attempt == baseline_attempt, (
        "attempt must be refunded exactly once back to baseline "
        f"{baseline_attempt}; got {final.attempt} -- a double-refund from "
        "the race would show as attempt < baseline here"
    )
    assert final.status == "pending"

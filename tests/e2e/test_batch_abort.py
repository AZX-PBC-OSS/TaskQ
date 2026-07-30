"""Batch abort policy e2e — AbortBatchAfter(3) stops a 10-job all-fail batch.

Scenario:
10 jobs that always fail, enqueued as a batch with ``failure_policy=
AbortBatchAfter(3)``.  A serial worker (``TASKQ_MAX_CONCURRENCY=1``) ensures
deterministic dispatch ordering so the abort threshold is hit predictably.

After abort:
- 3-4 jobs reach ``failed`` (bounded: the dispatch/write-hook race means one
  extra job may be in-flight when the abort fires), the rest are ``cancelled``.
- Only dispatched jobs record ``attempt`` effects — cancelled jobs never ran.
- The ``batches`` row shows ``status = 'aborted'``.

Uses ``e2e_worker_serial`` for serialized dispatch.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import EnqueueItem
from taskq.batch_policy import AbortBatchAfter

from ._assertions import fetch_effects, fetch_job_rows, poll_until
from .actors import BatchAbortPayload, batch_abort_worker

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_NUM_JOBS = 10
_ABORT_THRESHOLD = 3


async def test_batch_abort_after_threshold(
    e2e_client: TaskQ,
    e2e_worker_serial: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """10 all-fail jobs with AbortBatchAfter(3) → 3-4 failed, rest cancelled."""
    batch_id = uuid4()

    items = [
        EnqueueItem(
            actor_ref=batch_abort_worker,
            payload=BatchAbortPayload(run_id=run_id),
            metadata={"run_id": run_id},
        )
        for _ in range(_NUM_JOBS)
    ]

    batch = await e2e_client.enqueue_batch(
        items,
        batch_id=batch_id,
        failure_policy=AbortBatchAfter(consecutive_failures=_ABORT_THRESHOLD),
    )
    assert batch.size == _NUM_JOBS

    job_ids = [handle.job_id for handle in batch.job_handles]

    await asyncio.gather(
        *(h.wait(timeout=60) for h in batch.job_handles),
        return_exceptions=True,
    )

    async def _all_terminal() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, job_ids)
        return all(r["status"] in ("failed", "cancelled") for r in rows)

    await poll_until(
        _all_terminal,
        timeout=60.0,
        description=f"all {_NUM_JOBS} batch jobs to reach terminal status",
    )

    rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, job_ids)
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    failed_count = status_counts.get("failed", 0)
    cancelled_count = status_counts.get("cancelled", 0)
    assert failed_count + cancelled_count == _NUM_JOBS

    assert _ABORT_THRESHOLD <= failed_count <= _ABORT_THRESHOLD + 1, (
        f"expected {_ABORT_THRESHOLD}-{_ABORT_THRESHOLD + 1} failed jobs, "
        f"got {failed_count} (statuses={status_counts})"
    )
    assert cancelled_count == _NUM_JOBS - failed_count

    attempt_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="attempt"
    )
    assert len(attempt_effects) == failed_count, (
        f"expected {failed_count} attempt effects (one per dispatched job), "
        f"got {len(attempt_effects)}"
    )

    batch_status = await e2e_pg_pool.fetchval(
        f'SELECT status FROM "{e2e_schema.schema_name}".batches WHERE id = $1',
        batch_id,
    )
    assert batch_status == "aborted", f"batch row status={batch_status!r}, expected 'aborted'"

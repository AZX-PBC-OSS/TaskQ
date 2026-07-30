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

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import EnqueueItem
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch_policy import AbortBatchAfter

from ._assertions import (
    fetch_batch_status,
    fetch_effects,
    fetch_job_rows,
    poll_until,
    wait_all_ignoring_failures,
    wait_for_effects,
)
from .actors import (
    AbortFinalizerPayload,
    BatchAbortPayload,
    batch_abort_finalizer,
    batch_abort_worker,
)

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

    await wait_all_ignoring_failures(batch.job_handles, timeout=60)

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

    batch_status = await fetch_batch_status(e2e_pg_pool, e2e_schema.schema_name, batch_id)
    assert batch_status == "aborted", f"batch row status={batch_status!r}, expected 'aborted'"


async def test_batch_abort_with_finalizer(
    e2e_client: TaskQ,
    e2e_worker_serial: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Abort policy + finalizer: batch aborts, finalizer is NOT cancelled
    (it's not stamped with batch_id) and reaches a terminal *succeeded*
    state by catching BatchAbortedError inside the actor."""
    batch_id = uuid4()
    _num_children = 5

    children = [
        EnqueueItem(
            actor_ref=batch_abort_worker,
            payload=BatchAbortPayload(run_id=run_id),
            metadata={"run_id": run_id},
        )
        for _ in range(_num_children)
    ]

    finalizer = EnqueueItem(
        actor_ref=batch_abort_finalizer,
        payload=AbortFinalizerPayload(
            run_id=run_id,
            batch_id=str(batch_id),
        ),
        metadata={"run_id": run_id},
    )

    batch = await e2e_client.enqueue_batch(
        children,
        batch_id=batch_id,
        failure_policy=AbortBatchAfter(consecutive_failures=_ABORT_THRESHOLD),
        finalizer=finalizer,
    )
    assert batch.size == _num_children
    assert batch.finalizer_handle is not None

    child_ids = [
        handle.job_id for handle in batch.job_handles if handle is not batch.finalizer_handle
    ]
    finalizer_id = batch.finalizer_handle.job_id

    # Wait for child jobs only — the finalizer snoozes (via wait_for_batch)
    # until all children reach terminal status, so including it in the gather
    # would always time out and waste the full 60 s budget.  The finalizer's
    # terminal status is verified separately via the poll below.
    child_handles = [h for h in batch.job_handles if h is not batch.finalizer_handle]
    await wait_all_ignoring_failures(child_handles, timeout=60)

    async def _all_terminal() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, child_ids)
        return all(r["status"] in ("failed", "cancelled") for r in rows)

    await poll_until(
        _all_terminal,
        timeout=30.0,
        description=f"all {_num_children} child jobs to reach terminal status",
    )

    # Batch is aborted
    batch_status = await fetch_batch_status(e2e_pg_pool, e2e_schema.schema_name, batch_id)
    assert batch_status == "aborted", f"batch row status={batch_status!r}, expected 'aborted'"

    # Finalizer should NOT be cancelled by the abort — it's not stamped
    # with batch_id. It should reach a terminal *succeeded* state (it
    # catches BatchAbortedError inside the actor).
    async def _finalizer_terminal() -> bool:
        row = await e2e_pg_pool.fetchrow(
            f'SELECT status FROM "{e2e_schema.schema_name}".jobs WHERE id = $1',
            finalizer_id,
        )
        return row is not None and row["status"] in TERMINAL_STATUSES

    # With a 2 s snooze interval and children already terminal, the
    # finalizer should reach its terminal state within a few snooze cycles.
    await poll_until(
        _finalizer_terminal,
        timeout=30.0,
        description=f"finalizer {finalizer_id} to reach terminal status",
    )

    finalizer_status = await e2e_pg_pool.fetchval(
        f'SELECT status FROM "{e2e_schema.schema_name}".jobs WHERE id = $1',
        finalizer_id,
    )
    assert finalizer_status == "succeeded", (
        f"finalizer should succeed (catching BatchAbortedError), got status={finalizer_status!r}"
    )

    # The finalizer should have recorded an 'aborted' effect
    aborted_effects = await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="aborted",
        min_count=1,
        timeout=30.0,
    )
    assert len(aborted_effects) >= 1

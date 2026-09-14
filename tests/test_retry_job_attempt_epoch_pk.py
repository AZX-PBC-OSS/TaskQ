# ruff: noqa: S608  # Why: schema is the fixture's validated identifier throughout; every value is $-bound.

"""KNOWN DEFECT, executably tracked: ``retry_job`` resets ``attempt`` to 0
and can revisit attempt numbers from a spent epoch, colliding on
``job_attempts``' PRIMARY KEY (job_id, attempt).

Reproduced by execution (a psql probe during this initiative's review):
a job whose attempt row was written at attempt N, re-run via the admin
``retry_job`` (which resets ``attempt = 0``), climbs back through N on
re-dispatch, and its NEXT terminal write inserts at (job_id, N) again —
``duplicate key value violates unique constraint "job_attempts_pkey"``.
The hazard is shared by every attempt-row writer, not any one arm: the
fence guarantees freshness within an epoch, and only the epoch reset
revisits numbers.

The fix is a semantics decision on ``retry_job``'s contract — keep
``attempt`` monotonic across re-runs (the Oban/River admin-retry
precedent: their retry APIs never reset the attempt counter), or give
the attempt-row inserts conflict handling — and that decision belongs to
the operator, not to this initiative's fix run. Until it is made, this
pin holds the defect visible: ``xfail(strict=True)`` fails the suite the
moment the defect is fixed, forcing this marker's removal, so it cannot
rot into silence. The in-memory twin cannot catch this class at all (its
attempt store is a list, not keyed), which is why the pin is PG-only.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


@pytest.mark.xfail(
    strict=True,
    reason="retry_job resets attempt to 0, so a re-run job revisits its "
    "spent epoch's attempt numbers and the next terminal write collides on "
    "job_attempts_pkey; fixed when retry_job keeps attempt monotonic (or "
    "attempt inserts gain conflict handling) — then remove this marker",
)
async def test_retry_job_epoch_reset_collides_on_attempt_pk(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Terminal at attempt N → retry_job (attempt → 0) → re-dispatch to N
    → terminal again: the second write at (job_id, N) must not collide."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    error = ErrorInfo(
        error_class="TransientError",
        error_message="boom",
        error_traceback=None,
    )

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the JobsApp shim, as throughout the suite.
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=1,
            attempt=1,
            lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            with_events=False,
        )

        # Epoch 1: the terminal write lands the attempt row at (job, 1).
        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None)
        row = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None and row["status"] == "failed" and row["attempt"] == 1

        # The admin re-run: resets attempt to 0 and re-queues the row.
        assert await backend.retry_job(job_id)

        # Epoch 2: dispatch claims it again — attempt walks back to 1.
        claimed = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(minutes=5))
        assert [r.id for r in claimed] == [job_id]

        # The second terminal write at (job, 1): today this raises
        # UniqueViolationError on job_attempts_pkey.
        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None)

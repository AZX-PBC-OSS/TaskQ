# ruff: noqa: S608  # Why: schema is the fixture's validated identifier throughout; every value is $-bound.

"""PIN: ``retry_job`` keeps ``attempt`` monotonic, so a re-run job never
revisits a spent epoch's attempt numbers and no ``job_attempts`` write
can collide on the table's PRIMARY KEY (job_id, attempt).

History (the defect this pin replaced): ``retry_job`` used to reset
``attempt`` to 0, so a re-dispatched job climbed back through its spent
numbers and the next terminal write inserted at (job_id, N) again -
``duplicate key value violates unique constraint "job_attempts_pkey"``
- a revisited-primary-key violation the consumer's terminal-write infra
family misclassified as transient, stranding the row ``running``. The
correct approach: leave ``attempt`` alone and raise the ceiling
(``GREATEST(max_attempts, attempt + 1)``) so the budget gates open on
re-run. This way a re-run climbs to fresh attempt numbers and the
collision is unrepresentable. Every assertion here fails if anyone
reintroduces the epoch reset. The in-memory twin mirrors the same
semantics (its attempt store is a list, not keyed, so the PK itself is
PG-only - the monotonic counter and ceiling are pinned for it in
tests/test_in_memory_retry_job.py).
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


async def test_retry_job_attempt_monotonic_no_duplicate_key_on_re_run(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Terminal at attempt N → retry_job → re-dispatch climbs past N →
    terminal again at the fresh number: the attempt counter never goes
    backwards and the second write lands without a PK collision."""
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
        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=1)
        row = await conn.fetchrow(
            f'SELECT status, attempt, max_attempts FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None and row["status"] == "failed" and row["attempt"] == 1

        # The admin re-run: the counter stays at its spent value and the
        # ceiling rises so the budget gates open.
        assert await backend.retry_job(job_id)
        repended = await conn.fetchrow(
            f'SELECT status, attempt, max_attempts FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert repended is not None and repended["status"] == "pending"
        assert repended["attempt"] == 1, (
            "MONOTONIC-ATTEMPT CONTRACT: retry_job must never reset the "
            f"attempt counter - observed {repended['attempt']!r}. A reset "
            "revisits the spent epoch's attempt numbers and the next "
            "terminal write collides on job_attempts_pkey (the wedge pinned "
            "in tests/test_retry_job_wedge_consequence.py)."
        )
        assert repended["max_attempts"] == 2, (
            "CEILING-RAISE CONTRACT: retry_job must raise max_attempts to "
            "GREATEST(max_attempts, attempt + 1) so the budget gates open "
            f"for the re-run - observed {repended['max_attempts']!r}."
        )

        # Epoch 2: dispatch claims it again - the counter climbs to the
        # fresh number 2; the spent 1 is never revisited.
        claimed = await backend.dispatch_batch(worker_id, ["default"], 1, timedelta(minutes=5))
        assert [r.id for r in claimed] == [job_id]
        assert claimed[0].attempt == 2, (
            "the re-dispatch must climb past the spent epoch's numbers - "
            f"observed attempt={claimed[0].attempt!r}; a walk back to 1 "
            "means the epoch reset is back."
        )

        # The second terminal write lands at the fresh (job, 2): under the
        # old epoch reset this write raised UniqueViolationError on
        # job_attempts_pkey.
        assert await backend.mark_failed_or_retry(job_id, worker_id, error, None, attempt=2)
        final = await conn.fetchrow(
            f'SELECT status, attempt FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert final is not None and final["status"] == "failed" and final["attempt"] == 2
        attempts = await conn.fetch(
            f'SELECT attempt FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            job_id,
        )
        assert [a["attempt"] for a in attempts] == [1, 2], (
            "the attempt history must hold one row per spent number - a "
            "duplicate-key write would have failed the statement above, and "
            "a revisited number means the epoch reset is back."
        )

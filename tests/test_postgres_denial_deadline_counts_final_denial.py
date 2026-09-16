"""Red-team check: on real Postgres, does the deadline arm that
terminalises a perpetually-denied job also count the denial that pushed
it past its deadline?

``tests/test_denial_observability.py::
test_every_admission_denial_is_counted_including_the_last_before_expiry``
pins this on the in-memory backend only. The existing Postgres coverage
(``test_admission_denial_past_schedule_to_close_fails_on_the_deadline_path``
in ``test_postgres_retry_after.py``) proves the DeadlineExceeded failure
shape but never reads back ``rate_limit_blocked_count`` on the terminal
row, so a Postgres-only regression in the deadline arm's counter
increment (``_sql_templates.py``'s ``deadline_failed`` CTE) would pass
every existing Postgres pin while still under-counting for an operator
reading the row. This test drives the real SQL statement end to end and
would fail if that arm's increment ever dropped or regressed relative to
the in-memory twin.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


async def test_final_denial_before_deadline_is_counted_on_postgres(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=1,
            retry_kind="non_retryable",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(seconds=2),
        )

    # First denial: still fits inside the deadline window -> rescheduled,
    # counted, budget untouched.
    result = await backend.mark_snoozed(
        JobId(job_id),
        worker_id,
        timedelta(seconds=1),
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT rate_limit_blocked_count, attempt, status FROM "{schema}".jobs '
            "WHERE id = $1",
            job_id,
        )
    assert row is not None
    assert row["rate_limit_blocked_count"] == 1
    assert row["status"] == "scheduled"

    # Re-claim exactly as the dispatcher would (attempt refunded to 0 by
    # the first denial, then re-incremented by the claim).
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"""UPDATE "{schema}".jobs
            SET status = 'running',
                attempt = 1,
                locked_by_worker = $1,
                lock_expires_at = now() + interval '60 seconds',
                started_at = now(),
                last_heartbeat_at = now()
            WHERE id = $2""",
            worker_id,
            job_id,
        )

    # Second denial: the reschedule point now lands past schedule_to_close
    # -> the deadline arm terminalises the job. This denial must still be
    # counted, because it is the very denial that starved the job out.
    result = await backend.mark_snoozed(
        JobId(job_id),
        worker_id,
        timedelta(seconds=5),
        outcome="reservation_denied",
        attempt=1,
    )
    assert result == "failed"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, rate_limit_blocked_count FROM "{schema}".jobs '
            "WHERE id = $1",
            job_id,
        )
    assert row is not None
    assert row["status"] == "failed"
    assert row["error_class"] == "DeadlineExceeded"
    assert row["rate_limit_blocked_count"] == 2, (
        "the final denial -- the one that pushed the job past its "
        "schedule_to_close -- must be counted on Postgres exactly as it "
        "is on the in-memory backend; an operator reading this row after "
        "the job dies must see every denial it absorbed, not all-but-one"
    )

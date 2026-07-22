"""Integration tests for PostgresBackend mark_retry_after() and mark_snoozed()
multi-arm CTE branch selection.

Covers:
- mark_retry_after with consume_budget=True (three-arm CTE): snoozed,
  max_attempts_failed, deadline_failed, noop
- mark_retry_after with consume_budget=False (two-arm CTE): snoozed,
  deadline_failed, attempt-not-incremented
- mark_snoozed (two-arm CTE): snoozed, deadline_failed, event/attempt
  row verification
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings.post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches existing integration test pattern

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration


# ── mark_retry_after consume_budget=True (three-arm CTE) ────────────────


async def test_mark_retry_after_consume_budget_true_snoozed(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """snoozed branch: job with retry budget remaining, schedule_to_close
    far in the future → transitions to 'scheduled'."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=5), consume_budget=True
    )
    assert result == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, attempt, locked_by_worker, lock_expires_at, finished_at, error_class FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "scheduled"
    assert row["attempt"] == 1  # attempt unchanged by snooze
    assert row["locked_by_worker"] is None
    assert row["lock_expires_at"] is None
    assert row["finished_at"] is None
    assert row["error_class"] is None

    # Two events: pending→running (from create_running_job) + running→scheduled (from mark_retry_after)
    assert len(events) == 2
    snooze_event = events[-1]
    assert snooze_event["kind"] == "state_change"
    detail = snooze_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "scheduled"

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "snoozed"
    assert attempts[0]["error_class"] == "RetryAfter"
    assert attempts[0]["worker_id"] == worker_id


async def test_mark_retry_after_consume_budget_true_max_attempts_failed(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """max_attempts_failed branch: transient job with attempt >= max_attempts
    → fails with MaxAttemptsExceeded."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=3,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=5), consume_budget=True
    )
    assert result == "failed:MaxAttemptsExceeded"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, error_message, finished_at, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "failed"
    assert row["error_class"] == "MaxAttemptsExceeded"
    assert row["error_message"] == "retry budget exhausted"
    assert row["finished_at"] is not None
    assert row["locked_by_worker"] is None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["error_class"] == "MaxAttemptsExceeded"
    assert attempts[0]["worker_id"] == worker_id

    assert len(events) == 2
    fail_event = events[-1]
    assert fail_event["kind"] == "state_change"
    detail = fail_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "failed"
    assert detail["error_class"] == "MaxAttemptsExceeded"


async def test_mark_retry_after_consume_budget_true_deadline_failed(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """deadline_failed branch: schedule_to_close in the past takes priority
    over retry budget → fails with DeadlineExceeded."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()

    # schedule_to_close is in the past; even though budget remains (attempt=1, max=3),
    # the deadline arm should fire because now() + delay > schedule_to_close
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        job_id = await create_running_job(
            conn,
            schema,
            worker_id,
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=60),
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=30), consume_budget=True
    )
    assert result == "failed:DeadlineExceeded"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, error_message, finished_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "failed"
    assert row["error_class"] == "DeadlineExceeded"
    assert row["error_message"] == "schedule_to_close reached before next dispatch"
    assert row["finished_at"] is not None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["error_class"] == "DeadlineExceeded"
    assert attempts[0]["worker_id"] == worker_id

    assert len(events) == 2
    fail_event = events[-1]
    assert fail_event["kind"] == "state_change"
    detail = fail_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "failed"
    assert detail["error_class"] == "DeadlineExceeded"


async def test_mark_retry_after_consume_budget_true_noop(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Concurrency guard: calling mark_retry_after on a job already moved
    to 'succeeded' by another worker returns 'noop'."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
        )
        # Simulate another worker having already completed the job
        await conn.execute(
            f'UPDATE "{schema}".jobs SET status = $1, locked_by_worker = NULL, lock_expires_at = NULL, finished_at = now() WHERE id = $2',
            "succeeded",
            job_id,
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=5), consume_budget=True
    )
    assert result == "noop"

    # Verify the job is still succeeded
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)

    assert row is not None
    assert row["status"] == "succeeded"


# ── mark_retry_after consume_budget=False (two-arm CTE) ──────────────────


async def test_mark_retry_after_no_consume_snoozed(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """snoozed branch with consume_budget=False: schedule_to_close in the
    future → transitions to 'scheduled'."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=5), consume_budget=False
    )
    assert result == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, attempt, max_attempts, error_class FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "scheduled"
    assert row["attempt"] == 1
    # consume_budget=False extends max_attempts (snooze budget extension)
    assert row["max_attempts"] == 4
    assert row["error_class"] is None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "snoozed"
    assert attempts[0]["error_class"] == "RetryAfter"

    assert len(events) == 2
    snooze_event = events[-1]
    assert snooze_event["kind"] == "state_change"
    detail = snooze_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "scheduled"


async def test_mark_retry_after_no_consume_deadline_failed(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """deadline_failed branch with consume_budget=False: schedule_to_close
    in the past → fails with DeadlineExceeded."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=60),
        )

    result = await backend.mark_retry_after(
        JobId(job_id), worker_id, timedelta(seconds=30), consume_budget=False
    )
    assert result == "failed:DeadlineExceeded"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, error_message, finished_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "failed"
    assert row["error_class"] == "DeadlineExceeded"
    assert row["error_message"] == "schedule_to_close reached before next dispatch"
    assert row["finished_at"] is not None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["error_class"] == "DeadlineExceeded"

    assert len(events) == 2
    fail_event = events[-1]
    assert fail_event["kind"] == "state_change"
    detail = fail_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "failed"
    assert detail["error_class"] == "DeadlineExceeded"


async def test_mark_retry_after_consume_budget_true_attempt_not_incremented(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """After snoozed branch with consume_budget=True, the attempt count on
    the jobs row is unchanged (dispatch CTE is the sole increment point)."""
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
            max_attempts=10,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

    for cycle in range(3):
        # Capture attempt before mark_retry_after
        async with deps.worker_pool.acquire() as conn:
            before = await conn.fetchrow(
                f'SELECT attempt FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert before is not None
            expected_attempt = before["attempt"]

        result = await backend.mark_retry_after(
            JobId(job_id), worker_id, timedelta(seconds=1), consume_budget=True
        )
        assert result == "scheduled"

        # Verify attempt unchanged by mark_retry_after itself
        async with deps.worker_pool.acquire() as conn:
            after = await conn.fetchrow(
                f'SELECT attempt FROM "{schema}".jobs WHERE id = $1', job_id
            )
            assert after is not None
            assert after["attempt"] == expected_attempt, (
                f"cycle {cycle}: attempt changed from {expected_attempt} to {after['attempt']}"
            )

        # Re-dispatch to move back to running for next cycle
        async with deps.worker_pool.acquire() as conn:
            await conn.execute(
                f"""UPDATE "{schema}".jobs
                SET status = 'running',
                    attempt = attempt + 1,
                    locked_by_worker = $1,
                    lock_expires_at = now() + interval '60 seconds',
                    started_at = now(),
                    last_heartbeat_at = now()
                WHERE id = $2""",
                worker_id,
                job_id,
            )

    # After 3 snooze + re-dispatch cycles, attempt should be 4
    # (initial=1 + 3 dispatch increments)
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT attempt FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["attempt"] == 4

    # Verify all attempts are recorded
    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 3
    for a in attempts:
        assert a.outcome == "snoozed"
        assert a.error_class == "RetryAfter"


# ── mark_snoozed two-arm CTE ─────────────────────────────────────────────


async def test_mark_snoozed_snoozed_branch(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """snoozed branch: schedule_to_close in the future → transitions to
    'scheduled'."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

    result = await backend.mark_snoozed(JobId(job_id), worker_id, timedelta(seconds=5))
    assert result == "scheduled"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, attempt, max_attempts, error_class, locked_by_worker FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "scheduled"
    assert row["attempt"] == 1  # attempt unchanged by snooze
    assert row["max_attempts"] == 4  # snooze budget extension: +1
    assert row["error_class"] is None
    assert row["locked_by_worker"] is None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "snoozed"
    assert attempts[0]["error_class"] is None
    assert attempts[0]["worker_id"] == worker_id

    assert len(events) == 2
    snooze_event = events[-1]
    assert snooze_event["kind"] == "state_change"
    detail = snooze_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "scheduled"


async def test_mark_snoozed_deadline_failed_branch(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """deadline_failed branch: schedule_to_close in the past → fails with
    DeadlineExceeded."""
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
            max_attempts=3,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=60),
        )

    result = await backend.mark_snoozed(JobId(job_id), worker_id, timedelta(seconds=5))
    assert result == "failed"

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, error_message, finished_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', job_id
        )
        events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            job_id,
        )

    assert row is not None
    assert row["status"] == "failed"
    assert row["error_class"] == "DeadlineExceeded"
    assert row["error_message"] == "schedule_to_close reached before next dispatch"
    assert row["finished_at"] is not None

    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["error_class"] == "DeadlineExceeded"
    assert attempts[0]["worker_id"] == worker_id

    assert len(events) == 2
    fail_event = events[-1]
    assert fail_event["kind"] == "state_change"
    detail = fail_event["detail"]
    if isinstance(detail, str):
        from taskq._json import loads

        detail = loads(detail)
    assert detail["from_state"] == "running"
    assert detail["to_state"] == "failed"
    assert detail["error_class"] == "DeadlineExceeded"


async def test_mark_snoozed_job_events_and_attempts_both_branches(
    clean_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Verification: job_events and job_attempts rows are written correctly
    for each branch of mark_snoozed (snoozed vs deadline_failed)."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name

    async with deps.worker_pool.acquire() as conn:
        # ── snoozed branch job ──
        worker_s = new_uuid()
        await create_worker(conn, schema, worker_s)
        snoozed_job_id = await create_running_job(
            conn,
            schema,
            worker_s,
            max_attempts=5,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) + timedelta(hours=1),
        )

        # ── deadline_failed branch job ──
        worker_d = new_uuid()
        await create_worker(conn, schema, worker_d)
        deadline_job_id = await create_running_job(
            conn,
            schema,
            worker_d,
            max_attempts=5,
            retry_kind="transient",
            attempt=1,
            schedule_to_close=datetime.now(UTC) - timedelta(seconds=60),
        )

    # Exercise both branches
    result_s = await backend.mark_snoozed(JobId(snoozed_job_id), worker_s, timedelta(seconds=5))
    result_d = await backend.mark_snoozed(JobId(deadline_job_id), worker_d, timedelta(seconds=5))
    assert result_s == "scheduled"
    assert result_d == "failed"

    async with deps.worker_pool.acquire() as conn:
        # ── Check snoozed branch rows ──
        s_row = await conn.fetchrow(
            f'SELECT status, attempt, max_attempts FROM "{schema}".jobs WHERE id = $1',
            snoozed_job_id,
        )
        s_attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', snoozed_job_id
        )
        s_events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            snoozed_job_id,
        )

        assert s_row is not None
        assert s_row["status"] == "scheduled"
        assert s_row["attempt"] == 1
        assert s_row["max_attempts"] == 6  # initial 5 + snooze extension

        assert len(s_attempts) == 1
        assert s_attempts[0]["outcome"] == "snoozed"
        assert s_attempts[0]["error_class"] is None
        assert s_attempts[0]["worker_id"] == worker_s
        assert s_attempts[0]["started_at"] is not None
        assert s_attempts[0]["duration_ms"] is not None

        assert len(s_events) == 2
        s_snooze_event = s_events[-1]
        assert s_snooze_event["kind"] == "state_change"
        s_detail = s_snooze_event["detail"]
        if isinstance(s_detail, str):
            from taskq._json import loads

            s_detail = loads(s_detail)
        assert s_detail["from_state"] == "running"
        assert s_detail["to_state"] == "scheduled"

        # ── Check deadline_failed branch rows ──
        d_row = await conn.fetchrow(
            f'SELECT status, error_class, finished_at FROM "{schema}".jobs WHERE id = $1',
            deadline_job_id,
        )
        d_attempts = await conn.fetch(
            f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1', deadline_job_id
        )
        d_events = await conn.fetch(
            f'SELECT * FROM "{schema}".job_events WHERE job_id = $1 ORDER BY occurred_at',
            deadline_job_id,
        )

        assert d_row is not None
        assert d_row["status"] == "failed"
        assert d_row["error_class"] == "DeadlineExceeded"
        assert d_row["finished_at"] is not None

        assert len(d_attempts) == 1
        assert d_attempts[0]["outcome"] == "failed"
        assert d_attempts[0]["error_class"] == "DeadlineExceeded"
        assert d_attempts[0]["error_message"] == "schedule_to_close reached before next dispatch"
        assert d_attempts[0]["worker_id"] == worker_d
        assert d_attempts[0]["started_at"] is not None
        assert d_attempts[0]["duration_ms"] is not None

        assert len(d_events) == 2
        d_fail_event = d_events[-1]
        assert d_fail_event["kind"] == "state_change"
        d_detail = d_fail_event["detail"]
        if isinstance(d_detail, str):
            from taskq._json import loads

            d_detail = loads(d_detail)
        assert d_detail["from_state"] == "running"
        assert d_detail["to_state"] == "failed"
        assert d_detail["error_class"] == "DeadlineExceeded"

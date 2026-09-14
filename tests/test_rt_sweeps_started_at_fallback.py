"""Sweep 1's attempt INSERT must satisfy job_attempts.started_at NOT NULL
for a running job whose started_at is NULL.

That shape is reachable only via direct SQL (dispatch always stamps
started_at = clock_timestamp() — see _dispatch_sql.py), but it is a
legal row, and the crash-reclaim sweep is exactly the path that meets
the first direct-SQL author's orphan: pre-fix, the batched attempt
INSERT writes a.started_at raw, the INSERT dies on a non-transient
NotNullViolation inside the sweep's transaction (deliberately
non-transient per taskq.worker._transient), and the orphan is left
unreclaimed with no live worker to reclaim it — while the in-memory
twin COALESCEs NULL started_at to its injected now
(taskq/testing/_sweeps.py), so the same corpus also silently diverges
the two backends' parity contract. The twin's COALESCE is therefore the
CONTRACT, not dead mirroring.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)
# Well past lock expiry, far inside the cancel carve-out (cancel_phase=0
# anyway), so the job is eligible on the sweep's first call.
_EXPIRED_AGO = timedelta(seconds=10)
_START = datetime(2025, 6, 1, tzinfo=UTC)


async def _seed_running_job_null_started_at(
    conn: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID
) -> None:
    """The only reachable NULL-started_at shape: a running row written by
    direct SQL (dispatch stamps started_at, so no TaskQ path produces
    it) with an expired lock held by a live workers row."""
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        "VALUES ($1, 'null-start-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        " id, actor, queue, payload, max_attempts, retry_kind, status,"
        " priority, attempt, scheduled_at, started_at, last_heartbeat_at,"
        " locked_by_worker, lock_expires_at, cancel_phase"
        ") VALUES ("
        " $1, 'test_actor', 'default', '{}'::jsonb, 1, 'transient',"
        " 'running', 0, 1, clock_timestamp(), NULL, clock_timestamp(),"
        " $2, clock_timestamp() - ($3::double precision * interval '1 second'), 0"
        ")",
        job_id,
        worker_id,
        _EXPIRED_AGO.total_seconds(),
    )


async def test_sweep1_lands_attempt_row_for_null_started_at_running_job(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Pre-fix red: the sweep raises NotNullViolationError on the batched
    attempt INSERT and the transaction rolls back — the job is left
    running (still unreclaimable), with no audit rows. Post-fix the
    sweep completes and the attempt row carries the per-row clock
    fallback for started_at, the in-memory twin's contract."""
    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    worker_id = new_uuid()
    await _seed_running_job_null_started_at(clean_pg_conn, schema, job_id, worker_id)

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _GRACE,
        _GRACE,
        schema=schema,
    )
    assert count == 1, "the NULL-started_at job must be reclaimed like any other expired lock"

    job = await clean_pg_conn.fetchrow(
        f"SELECT status::text, finished_at, locked_by_worker "  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert job is not None
    # max_attempts=1 with attempt=1: exhausted, no cancel in flight → crashed.
    assert job["status"] == "crashed"
    assert job["finished_at"] is not None
    assert job["locked_by_worker"] is None

    attempt = await clean_pg_conn.fetchrow(
        f"SELECT started_at, finished_at, duration_ms, outcome, worker_id "  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        f'FROM "{schema}".job_attempts WHERE job_id = $1',
        job_id,
    )
    assert attempt is not None, "the sweep must write the attempt row"
    assert attempt["started_at"] is not None, (
        "job_attempts.started_at is NOT NULL: a NULL-started_at running job "
        "must land the per-row clock fallback, not the raw NULL (pre-fix this "
        "sweep dies on NotNullViolationError before ever reaching this assert)"
    )
    # The fallback stamps started_at inside the same statement as
    # finished_at, so the span between them is the statement's own
    # execution time, not a measurable job duration.
    assert attempt["finished_at"] >= attempt["started_at"]
    # duration is unknowable for a never-started attempt: the twin records
    # NULL (no started_at to measure from), and the sweep computes
    # duration_ms in Python from the job's NULL started_at — None.
    assert attempt["duration_ms"] is None
    assert attempt["outcome"] == "crashed"
    assert attempt["worker_id"] == worker_id, "the live holder must be recorded"

    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        job_id,
    )
    assert events == 1, "the reclaim state_change event must land in the same transaction"


async def test_sweep1_null_started_at_fallback_is_per_row_distinct_within_a_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A batch of SEVERAL NULL-started_at running jobs must not collapse onto
    one fallback stamp.

    The fallback is ``clock_timestamp() + (ord - 1) * 1us`` — the per-row
    ladder.  A single-row test (the pin above) cannot see the ladder: ord
    is always 1.  With three NULL-started_at rows in one batch, a fallback
    written as a bare per-statement ``clock_timestamp()`` would pass the
    single-row test while stamping every attempt identically — the exact
    collapse the ladder exists to prevent (see the comment above
    _SWEEP_1_ATTEMPTS_BATCH_SQL)."""
    schema = module_pg_schema.schema_name
    # One workers row per job: the seeder INSERTs a worker each call, so a
    # shared worker_id would violate the workers PK on the second call.
    job_ids = [new_uuid() for _ in range(3)]
    for job_id in job_ids:
        await _seed_running_job_null_started_at(clean_pg_conn, schema, job_id, new_uuid())

    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
        _GRACE,
        _GRACE,
        schema=schema,
        batch_size=10,
    )
    assert count == 3, "one call must reclaim the whole three-row batch"

    attempts = await clean_pg_conn.fetch(
        f'SELECT job_id, started_at FROM "{schema}".job_attempts ORDER BY started_at',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
    )
    assert len(attempts) == 3
    assert {r["job_id"] for r in attempts} == set(job_ids)
    stamps = [r["started_at"] for r in attempts]
    assert len(set(stamps)) == 3, (
        f"the per-row fallback ladder collapsed: three NULL-started_at rows got "
        f"{len(set(stamps))} distinct started_at stamps — the ord term in the "
        "COALESCE fallback is what keeps the audit trail per-row distinct"
    )
    assert all(s is not None for s in stamps)


async def test_sweep1_null_started_at_contract_is_the_in_memory_twin() -> None:
    """The in-memory twin's COALESCE-to-now is the contract both backends
    must satisfy: for the same NULL-started_at corpus the twin leaves one
    attempt row with a non-NULL started_at (its injected now), NULL
    duration_ms, 'crashed' outcome, and the lock bookkeeping cleared —
    the observable the Postgres path is hardened to match (pinned by the
    test above)."""
    memory = InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    holder = new_uuid()
    args = make_enqueue_args(scheduled_at=_START, max_attempts=1)
    row = await memory.enqueue(args)
    memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the direct-SQL-reachable running shape, the established pattern from test_rt_sweeps_parity.py.
        row,
        status="running",
        attempt=1,
        started_at=None,
        locked_by_worker=holder,
        lock_expires_at=_START - _EXPIRED_AGO,
    )

    count = await memory.reclaim_expired_locks(_GRACE, _GRACE)
    assert count == 1

    job = await memory.get(args.id)
    assert job is not None
    assert job.status == "crashed"
    assert job.finished_at == _START
    assert job.locked_by_worker is None
    assert job.lock_expires_at is None

    attempts = await memory.get_attempts(args.id)
    assert len(attempts) == 1
    twin = attempts[0]
    assert twin.started_at == _START, (
        "the twin's COALESCE-to-now fallback is the contract the Postgres path matches"
    )
    assert twin.finished_at == _START
    assert twin.duration_ms is None
    assert twin.outcome == "crashed"
    assert twin.worker_id == holder

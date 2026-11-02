"""Sweep 1's reclaim of a NULL-started_at running job records NO attempt
row: the standing-claim fence.

A running row whose ``started_at`` is NULL carries NO standing claim.
Two writers produce the shape:

* the claim-loss reconcile's refund (worker/heartbeat.py's
  ``_RECONCILE_LOST_CLAIMS_SQL_TEMPLATE``) un-stamps ``started_at`` as it
  refunds the claim-time increment - NULL is that statement's durable
  "no claim stands on this number" marker, and the row is left running
  and locked FOR the reclaim sweep, so the sweep meets the shape in
  production on every refund-then-reclaim hand-off;
* a direct-SQL author's orphan (dispatch always stamps
  ``started_at = clock_timestamp()`` - see ``_dispatch_sql.py`` - so no
  TaskQ path but the refund produces it).

Pre-fence, the batched attempt INSERT wrote the row raw, died on the
NOT NULL (the original finding), and the per-row clock fallback fixed
the crash by fabricating a started stamp the attempt never had. That
fallback is the defect this module now pins the fence against: a
reclaimed row whose attempt never started owes the ledger NOTHING. A
crashed attempt row at that number would permanentise an epoch the
counter no longer carries - the claim-loss refund de-charged it - and
the next claim re-mints the number, closing the job's lineage with one
more attempt row than the counter: the soak's reconciliation red
(``attempt counter 1 vs 2 attempt rows``, run 36175331443, the
refund-then-reclaim ledger race pinned by
``tests/test_rt_refund_reclaim_ledger_race.py``). The reclaim itself is
still audited: the ``job_events`` state_change (the crash-reclaim outbox
channel) lands either way, and the in-memory twin mirrors the fence so
the two backends cannot drift.
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
    """A running row with a NULL started_at (the refund's void marker, or
    a direct-SQL author's orphan - dispatch always stamps it) with an
    expired lock held by a live workers row."""
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


async def test_sweep1_records_no_attempt_row_for_a_null_started_at_running_job(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The standing-claim fence: the sweep reclaims the row (the lease
    arm's eligibility is the lock, not the stamp) and lands the reclaim
    event, but records NO attempt row - the attempt never started, and a
    crashed row for it is the fabrication the claim-loss refund exists to
    keep out of the ledger. Pre-fence this shape died on
    NotNullViolationError, then recorded a fabricated clock-fallback
    stamp; both were wrong in the ledger's own terms."""
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

    attempts = await clean_pg_conn.fetch(
        f'SELECT * FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        job_id,
    )
    assert attempts == [], (
        f"the reclaim of a no-claim row must record NO attempt row, got "
        f"{[dict(a) for a in attempts]}: a crashed row at an epoch no "
        f"claim stood on permanentises a number the counter does not "
        f"carry, and the next claim's re-mint double-counts it"
    )

    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        job_id,
    )
    assert events == 1, "the reclaim state_change event must land in the same transaction"


async def test_sweep1_reclaims_a_full_batch_of_no_claim_rows_with_one_attempt_row_total(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A batch of SEVERAL no-claim rows reclaims whole and writes NO
    attempt rows: the fence is per-row inside the batched INSERT, so one
    refunded sibling cannot abort the batch's transaction (pre-fence the
    raw NULL stamp was a non-transient NotNullViolation that rolled back
    every sibling) and one recorded sibling cannot mask the others."""
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
        f'SELECT job_id FROM "{schema}".job_attempts',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
    )
    assert attempts == [], (
        "no row in the batch carried a standing claim: the fence must "
        "keep the batch's attempt ledger empty, not partially recorded"
    )

    events = await clean_pg_conn.fetch(
        f'SELECT job_id FROM "{schema}".job_events',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
    )
    assert {r["job_id"] for r in events} == set(job_ids), (
        "every reclaimed row's state_change audit must land, the fence "
        "withholds only the attempt LEDGER row"
    )


async def test_sweep1_no_claim_fence_is_the_in_memory_twin() -> None:
    """The twin mirrors the fence: for the same no-claim corpus the twin
    leaves ZERO attempt rows (never a fabricated one), terminalises the
    job 'crashed', and keeps the lock bookkeeping cleared - the observable
    the Postgres path is fenced to match."""
    memory = InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    holder = new_uuid()
    args = make_enqueue_args(scheduled_at=_START, max_attempts=1)
    row = await memory.enqueue(args)
    memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the refund's un-stamped running shape, the established pattern from test_rt_sweeps_parity.py.
        row,
        status="running",
        attempt=1,
        claim_epoch=1,
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
    assert attempts == [], (
        "the twin's fence mirrors the batched INSERT's: a reclaimed row "
        "with no standing claim records no attempt row - the COALESCE "
        "fallback this module pinned pre-fence fabricated a started stamp "
        "the attempt never had"
    )

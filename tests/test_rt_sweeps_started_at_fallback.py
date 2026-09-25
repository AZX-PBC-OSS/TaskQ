"""Sweep 1's reclaim of a NEVER-STARTED claim writes no attempt row.

A running row whose ``started_at`` is NULL is the heartbeat claim-loss
reconcile's refund output (worker/heartbeat.py): dispatch always stamps
``started_at = clock_timestamp()`` at claim, so the only TaskQ-reachable
NULL is the reconcile's un-stamp - the durable "never started" mark of a
claim that charged an attempt no actor ever saw (direct SQL can forge the
shape too; no system writer can).

The ledger records EXECUTIONS. The pre-fix doctrine coalesced the NULL
stamp to a clock and wrote a crashed row for the never-started claim -
a record asserting an execution that did not happen, indistinguishable
from a genuine mid-execution crash (the exact record issue 458 banned),
and worse, a conservation break the soak's reconcile pin reads: the
refund rewinds the counter, so a FIRST-attempt refund re-claims at
attempt 1 while the fabricated row sits at 0 - the settled job then
carries TWO attempt rows against a counter of 1 ("attempt counter 1 vs
2 attempt rows", the soak grand_mixin's ``assert 2 == 1`` red, run
36175331443 job 108204436302). The batched attempt INSERT now filters
``WHERE started_at IS NOT NULL``: the reclaim's state_change event still
records the hand-back, and the statement stays satisfied against
``job_attempts.started_at NOT NULL`` without any fallback stamp - the
original NotNullViolation concern is met by not inserting, not by
fabricating a stamp. The in-memory twin carries the identical guard, so
the two backends' parity contract holds on this shape too.
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
    """The only TaskQ-reachable NULL-started_at shape: the heartbeat
    reconcile's refunded claim (a running row, lock expired, held by a
    live workers row, no ledger row - nothing executed)."""
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


async def test_sweep1_writes_no_attempt_row_for_a_never_started_claim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The reclaim completes (the original concern: the batched INSERT must
    not die on a NotNullViolation for the raw NULL stamp - it does not,
    because it inserts nothing), the row transitions, the event lands, and
    NO attempt row is fabricated for the execution that never ran."""
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

    attempts = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        job_id,
    )
    assert attempts == 0, (
        f"a never-started claim (started_at NULL: the refund's durable mark) "
        f"must write NO attempt row - a crashed row fabricated at the "
        f"refunded attempt number asserts an execution that did not happen "
        f"and breaks the counter<->ledger conservation the soak pins, got "
        f"{attempts} row(s)"
    )

    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        job_id,
    )
    assert events == 1, "the reclaim state_change event must land in the same transaction"


async def test_sweep1_never_started_batch_writes_no_rows_and_survives(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A batch of SEVERAL NULL-started_at running jobs must reclaim cleanly.

    The pre-fix COALESCE fallback stamped each row via the per-row
    microsecond ladder; the doctrine is now no-fabrication, so the batch's
    attempt INSERT is empty. The statement must still complete (no
    NotNullViolation, no zero-row INSERT error) and reclaim every sibling,
    and it must not disturb GENUINE crashes swept in the same batch: a
    mixed batch filters only the never-started rows."""
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

    attempts = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts',  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
    )
    assert attempts == 0, (
        f"no never-started claim in the batch may gain a fabricated attempt row, got {attempts}"
    )
    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound.
        job_ids,
    )
    assert events == 3, "every reclaimed row's state_change event must still land"


async def test_sweep1_never_started_contract_is_the_in_memory_twin() -> None:
    """The in-memory twin carries the identical no-fabrication guard: for
    the same NULL-started_at corpus the twin writes NO attempt row, the
    hand-back recorded by the reclaim event alone - the observable the
    Postgres path is hardened to match (pinned by the test above)."""
    memory = InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    holder = new_uuid()
    args = make_enqueue_args(scheduled_at=_START, max_attempts=1)
    row = await memory.enqueue(args)
    memory._jobs[args.id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: test-only seeding of the refunded running shape, the established pattern from test_rt_sweeps_parity.py.
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
        f"the twin must write no attempt row for a never-started claim "
        f"(the PG batched INSERT's WHERE started_at IS NOT NULL, the "
        f"parity half), got {attempts}"
    )

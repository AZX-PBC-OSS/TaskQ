"""Issue: the shutdown drain refunds the claim of a disowned attempt that ran.

The chain, all on one real PostgreSQL:

1. The dispatch claim stamps ``attempt = j.attempt + 1`` and
   ``started_at = clock_timestamp()`` at claim (the real rendered
   dispatch CTE below).
2. A consumer takes the row, the attempt runs, and the attempt's
   terminal write spends its retry budget on infra errors without
   landing. The consumer disowns the id (``worker/_handlers.py``'s
   ``_disown_job``): the row stays ``running`` and locked, the
   disowned set is the process's own record that the attempt was IN
   FLIGHT, and the promised recovery is lease lapse, then Sweep 1's
   evidence-gated reclaim with its crashed attempt row.
3. The bug: ``drain_local_queue_to_pending`` (the shutdown hand-back)
   excluded only the live-consumer registries. A disowned id sits in
   no registry, so the drain re-pended the row AND refunded the
   claim-time increment through ``ATTEMPT_REFUND_SQL`` - a refund whose
   documented meaning is "this claim never reached an actor", applied
   to an attempt that did. The ledger keeps no trace of the execution
   (the write that would have recorded it is the one that failed), so
   a reconciliation of ``job_attempts`` against reality shows one
   execution where two happened: the fabricated-absence mirror of the
   claim-loss charge. The re-pend also erased the crashed attempt row
   sweep 1 would have written, the recovery the disown contract
   promises.

The fix: the drain folds ``deps.disowned_jobs`` into its exclusion set.
A disowned row stays running and locked; its lease lapses and sweep 1
owns the reclaim, writing the truthful crashed attempt row for the
attempt that ran.

No fakes: the real rendered dispatch CTE
(``_sql_templates.render`` -> ``_dispatch_sql.dispatch_batch``), the
real ``drain_local_queue_to_pending``, and the real
``sweep_expired_locks`` so the sweep's own attempt and event writes are
in scope. Timing is deterministic by seeding: the sweep's expiry is
anchored to the PG clock with an aged ``lock_expires_at`` stamp instead
of a wall-clock sleep.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import dispatch_batch
from taskq.backend._sql_templates import render
from taskq.backend._sweeps import sweep_expired_locks
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import drain_local_queue_to_pending

pytestmark = pytest.mark.integration

_QUEUE = "disown-drain-q"

#: The dispatch claim's lock lease; the sweep's lease arm is anchored to
#: the PG clock by ageing the stamp, so the value only bounds the claim.
_LOCK_LEASE = timedelta(seconds=30.0)


async def _seed_worker(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound or a module constant.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    # The dispatch CTE's candidate walk iterates the actor registry
    # (per_actor_capacity over actor_config); an unregistered actor's
    # pending rows are invisible to every round.
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608
        "VALUES ('disown_drain_actor', $1) ON CONFLICT (actor) DO NOTHING",
        _QUEUE,
    )


async def _seed_pending_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    max_attempts: int = 3,
) -> UUID:
    rows = await conn.fetch(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "attempt, scheduled_at) VALUES (gen_random_uuid(), 'disown_drain_actor', "
        f"'{_QUEUE}', '{{}}'::jsonb, 'pending', $1, 'transient', "
        "0, clock_timestamp()) RETURNING id",
        max_attempts,
    )
    job_id: UUID = rows[0]["id"]
    return job_id


async def _claim_via_real_dispatch_cte(
    conn: asyncpg.Connection, schema: str, worker_id: UUID
) -> asyncpg.Record:
    """The real rendered dispatch CTE, the real claim: autocommit, the row
    is ``running``, charged to attempt 1, and stamped before this helper
    returns."""
    sql = render(schema).dispatch_strict_fifo
    rows = await dispatch_batch(
        conn,
        sql=sql,
        queues=[_QUEUE],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=_LOCK_LEASE,
    )
    assert len(rows) == 1, f"the claim must land: got {len(rows)} rows"
    return rows[0]


def _drain_deps(pg_dsn: str, schema: str, pool: asyncpg.Pool) -> WorkerDeps:
    """Real deps on the real schema: empty registries, exactly the
    post-disown shape under test."""
    return WorkerDeps(
        settings=WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": schema,
            }
        ),
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: the drain reads this pool; a real pool is a drop-in.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, Any]:
    rows = await conn.fetch(
        f"SELECT status::text AS status, attempt, "  # noqa: S608
        f"started_at, locked_by_worker::text AS locked_by "
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert rows, f"job {job_id} vanished"
    return dict(rows[0])


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f"SELECT attempt, outcome::text AS outcome "  # noqa: S608
        f'FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
        job_id,
    )
    return [dict(r) for r in rows]


async def test_the_drain_leaves_a_disowned_row_and_its_charge_alone(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """A disowned id is evidence the attempt was in flight: the drain must
    neither re-pend the row nor refund the claim-time increment.

    Red on main: the drain re-pended the row and refunded the increment,
    charging back an execution whose terminal write (the thing that would
    have recorded it) is the very write that failed - a ledger with one
    execution where reality holds two.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 1

    # The disown: exactly what _disown_job records after a terminal
    # write exhausts its retry budget on infra errors. The consumer is
    # gone (its task exited), the row is running and locked to this
    # worker, held by nothing in any registry - and the attempt ran.
    deps = _drain_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    deps.disowned_jobs.add(job_id)

    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 0, (
        f"the drain must not hand back a disowned row (its recovery is "
        f"lease lapse, then sweep 1's reclaim), it re-pended {drained}"
    )

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "running", f"the disowned row must stay sweep 1's, got {row}"
    assert row["attempt"] == 1, (
        f"the drain must not refund the claim-time increment of a disowned "
        f"attempt (the attempt ran; the refund asserts it never reached an "
        f"actor), got attempt={row['attempt']}"
    )
    assert row["locked_by"] == str(worker_id), (
        f"the untouched row keeps its lock so the lease lapse reaches "
        f"sweep 1, got locked_by={row['locked_by_worker']}"
    )
    assert await _attempt_rows(clean_pg_conn, schema, job_id) == [], (
        "the drain writes no attempt row of its own"
    )


async def test_the_drain_still_refunds_the_never_started_claim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Control: the claimed-but-never-started row (no consumer ever took
    it, nothing disowned) keeps its hand-back and its refund. The fix
    narrows the drain's premise to the rows that never started; this pin
    holds that premise's original contract in place."""
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 1

    deps = _drain_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    assert not deps.disowned_jobs
    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 1, "the claimed-but-unstarted row must come back to pending"

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "pending", f"the drain re-pends, got {row}"
    assert row["attempt"] == 0, (
        f"the drain refunds the claim-time increment of a claim that never "
        f"reached an actor, got attempt={row['attempt']}"
    )


async def test_sweep_1_writes_the_disowned_attempt_row_the_drain_used_to_erase(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The disowned row's promised recovery, end to end: the drain leaves
    it running and locked, the lease lapses, and the real sweep_expired_locks
    writes the crashed attempt row for the attempt that ran - the ledger
    record the drain's refund used to make impossible. max_attempts=1 puts
    the row at its budget edge, so the reclaim terminalises 'crashed' and
    the attempt row is the job's whole history."""
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=1)

    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 1

    deps = _drain_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    deps.disowned_jobs.add(job_id)
    assert await drain_local_queue_to_pending(deps, worker_id) == 0

    # Anchor sweep 1's lease arm to the PG clock: the disowned row's
    # lease has lapsed (the heartbeat stopped renewing it at the disown).
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET lock_expires_at = '  # noqa: S608
        "clock_timestamp() - interval '1 second' WHERE id = $1",
        job_id,
    )
    reclaimed = await sweep_expired_locks(
        clean_pg_conn,
        cancel_grace=timedelta(seconds=0),
        cleanup_grace=timedelta(seconds=0),
        schema=schema,
    )
    assert reclaimed == 1, f"sweep 1 must reclaim the lapsed disowned row, got {reclaimed}"

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "crashed", f"the budget-edge reclaim terminalises, got {row}"
    assert row["attempt"] == 1, (
        f"the crashed record sits at the attempt that ran, got attempt={row['attempt']}"
    )
    attempts = await _attempt_rows(clean_pg_conn, schema, job_id)
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [(1, "crashed")], (
        f"sweep 1's crashed attempt row is the truthful 'in flight, outcome "
        f"lost' record for the attempt the drain must not refund, got {attempts}"
    )

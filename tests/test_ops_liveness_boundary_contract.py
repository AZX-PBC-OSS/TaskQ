"""The liveness-boundary contract: doctor and the leader sweep must agree on
WHERE the liveness edge sits, and the operator must be told which surfaces
DISAGREE on a worker dead-by-liveness whose leases are still alive.

Three arms read the same ``workers`` row and do not use one window:

- the liveness arm (``admin_worker_liveness_seconds``, default 30 s,
  strict ``>``): the leader's stranded-jobs detector
  (``_stranded_jobs_loop``), doctor's on-demand stranded scan
  (``taskq.cli._list_stranded_pending_jobs``), the ``taskq.queue.live_workers``
  gauge and the admin UI's unserved banner;
- the stale-worker REMOVAL arm (``heartbeat_interval *
  (max_heartbeat_failures + 3)``, 60 s at the defaults): the sweep that
  deletes the row;
- the RECOVERY arm (per-job ``lock_expires_at`` / ``last_heartbeat_at +
  heartbeat_timeout``): sweep 1's reclaim of a RUNNING row - it reads the
  job's own lease columns, never the worker row's liveness.

In the band between the liveness edge and the removal edge (30 s - 60 s at
the defaults, longer whenever an operator raises the heartbeat) the two
surfaces disagree BY DESIGN: doctor says "no live worker serves" while the
running row's reclaim still waits for its lease. That disagreement is the
operator's confusion mid-incident; these tests pin the verdict so it is
documented, executable truth rather than folklore.
"""

from __future__ import annotations

import ast
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sweeps import sweep_expired_locks
from taskq.cli import _list_stranded_pending_jobs
from taskq.worker._leader_shared import cleanup_stale_workers

pytestmark = pytest.mark.integration

_LIVENESS_WINDOW = 30  # admin_worker_liveness_seconds' default

_SWEEPS_SOURCE = (
    Path(__file__).resolve().parents[1] / "src" / "taskq" / "worker" / "_leader_sweeps.py"
)


def _sweep_stranded_sql() -> str:
    """The leader sweep's stranded-jobs SQL, extracted from its own source.

    Reading the statement out of ``_stranded_jobs_loop`` (instead of
    restating a copy here) is the point: if the sweep's liveness arm or
    routing discriminator changes, this test's comparison tracks the NEW
    statement, so a doctor/sweep divergence cannot hide behind a stale
    second copy. The marker asserts the extraction found the liveness
    arm - a refactor that moves or renames the statement must update this
    accessor, not silently unpin the boundary.
    """
    tree = ast.parse(_SWEEPS_SOURCE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
            node.name == "_stranded_jobs_loop"
        ):
            for stmt in ast.walk(node):
                if not isinstance(stmt, ast.Assign):
                    continue
                for target in stmt.targets:
                    if not (isinstance(target, ast.Name) and target.id == "_stranded_sql"):
                        continue
                    literal = stmt.value
                    if not (isinstance(literal, ast.Constant) and isinstance(literal.value, str)):
                        continue
                    sql = literal.value
                    marker = "last_seen_at > statement_timestamp() - make_interval(secs => $1)"
                    assert marker in sql, (
                        "the sweep's stranded SQL no longer carries the "
                        f"liveness arm at the expected spelling:\n{sql}"
                    )
                    return sql
    raise AssertionError(
        "could not extract _stranded_sql from _stranded_jobs_loop - the sweep's "
        "statement moved; re-point this accessor, do not restate a copy"
    )


async def _seed_pending_job(
    conn: asyncpg.Connection, schema: str, *, actor: str, queue: str
) -> object:
    job_id = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at
            ) VALUES ($1, $2, $3, '{{"k": "v"}}'::jsonb, 3, 'transient',
                      'pending', 0, statement_timestamp())""",  # noqa: S608
        job_id,
        actor,
        queue,
    )
    return job_id


async def _seed_worker(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    last_seen_sql: str,
) -> object:
    """One worker row whose heartbeat is computed IN the insert statement."""
    worker_id = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at)
            VALUES ($1, 'boundary-host', 1, ARRAY[$2::text],
                    {last_seen_sql})""",  # noqa: S608
        worker_id,
        queue,
    )
    return worker_id


async def _job_status(conn: asyncpg.Connection, schema: str, job_id: object) -> str | None:
    """One job row's status (the read-back `job show` prints)."""
    return await conn.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is identifier-validated upstream; the id is bound.
        job_id,
    )


async def _seed_actor_config(
    conn: asyncpg.Connection, schema: str, *, actor: str, queue: str
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        "ON CONFLICT (actor) DO NOTHING",
        actor,
        queue,
    )


async def _doctor_unserved(
    conn: asyncpg.Connection, schema: str, liveness: int
) -> dict[str, tuple[int, tuple[str, ...]]]:
    """Doctor's stranded arm, verbatim (the CLI function the doctor runs)."""
    rows = await _list_stranded_pending_jobs(conn, schema=schema, worker_liveness_seconds=liveness)
    return {r.actor: (r.unserved_queue, r.unserved_queues) for r in rows}


async def _sweep_unserved(
    conn: asyncpg.Connection, schema: str, liveness: int
) -> dict[str, tuple[int, tuple[str, ...]]]:
    """The leader sweep's stranded arm, verbatim SQL from its own source."""
    sql = _sweep_stranded_sql().format(schema=schema)
    rows = await conn.fetch(sql, liveness)
    return {
        str(r["actor"]): (int(r["unserved_queue_cnt"]), tuple(r["unserved_queues"])) for r in rows
    }


async def test_doctor_and_sweep_agree_at_the_exact_liveness_edge(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """A worker EXACTLY at the liveness edge: doctor and the sweep must give
    the SAME verdict, and the edge must move with the window (strict ``>``).

    Bracketed around the boundary: a heartbeat stamped
    ``statement_timestamp() - interval '30 seconds'`` is at the edge the
    moment it lands, and every later read sees an age strictly past it, so
    with the 30 s window it is DEAD (the predicate is ``>``, not ``>=``);
    the identical fleet read with a 31 s window is ALIVE. Both surfaces
    must bracket together - any window or clock-source drift between them
    lands an operator two verdicts for one fleet.
    """
    schema = module_pg_schema.schema_name

    # Two actors, one queue each: 'q_edge' held by the edge-stale row,
    # 'q_inside' by a row one second inside the window.
    await _seed_actor_config(clean_pg_conn, schema, actor="actor_edge", queue="q_edge")
    await _seed_actor_config(clean_pg_conn, schema, actor="actor_inside", queue="q_inside")
    await _seed_pending_job(clean_pg_conn, schema, actor="actor_edge", queue="q_edge")
    await _seed_pending_job(clean_pg_conn, schema, actor="actor_inside", queue="q_inside")
    await _seed_worker(
        clean_pg_conn,
        schema,
        queue="q_edge",
        last_seen_sql="statement_timestamp() - interval '30 seconds'",
    )
    await _seed_worker(
        clean_pg_conn,
        schema,
        queue="q_inside",
        last_seen_sql="statement_timestamp() - interval '29 seconds'",
    )

    # At the documented window: the edge row is DEAD (strict >), the inside
    # row is ALIVE - and both surfaces name exactly the same stranded actor.
    doctor = await _doctor_unserved(clean_pg_conn, schema, _LIVENESS_WINDOW)
    sweep = await _sweep_unserved(clean_pg_conn, schema, _LIVENESS_WINDOW)
    assert doctor == sweep, f"doctor and sweep disagree at the edge:\n{doctor}\n{sweep}"
    assert doctor == {"actor_edge": (1, ("q_edge",))}, f"the 30 s edge verdict drifted: {doctor}"

    # The edge MOVES with the window: one second wider resurrects the row on
    # BOTH surfaces; one second tighter kills the inside row on BOTH.
    for window in (29, 31):
        doctor_w = await _doctor_unserved(clean_pg_conn, schema, window)
        sweep_w = await _sweep_unserved(clean_pg_conn, schema, window)
        assert doctor_w == sweep_w, (
            f"doctor and sweep disagree at window={window}s:\n{doctor_w}\n{sweep_w}"
        )
    assert await _doctor_unserved(clean_pg_conn, schema, 29) == {
        "actor_edge": (1, ("q_edge",)),
        "actor_inside": (1, ("q_inside",)),
    }, "tightening the window to 29 s must strand both queues"
    assert await _doctor_unserved(clean_pg_conn, schema, 31) == {}, (
        "widening the window to 31 s must resurrect both workers"
    )


async def test_dead_by_liveness_with_live_lease_the_surfaces_disagree_by_design(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: Any,
) -> None:
    """The 30-60 s band pinned: a worker past the liveness edge but before
    the removal edge, holding a RUNNING row whose lease is still future.

    The verdict an operator must be able to trust:

    - the LIVENESS surfaces (doctor's stranded scan, the leader's stranded
      detector, the live_workers gauge) call the worker DEAD: its queue's
      pending work reads stranded-unserved;
    - the REMOVAL sweep has not deleted the row yet (its own window is the
      heartbeat budget, 60 s at the defaults);
    - the RECOVERY arm does NOT rescue the running row on the liveness
      verdict - it reads the row's own lease columns, and a fresh
      heartbeat/valid lease means sweep 1 comes back empty: the stuck job
      follows the LEASE, not the liveness window. The documented remedy is
      the lease wait (runbooks.md TaskQRunningLeaseExpired), optionally the
      cooperative cancel, which against a dead worker only REQUESTS - the
      reclaim sweep is what lands it, honouring the row to ``cancelled``.
    """
    schema = module_pg_schema.schema_name

    # The ghost: past the 30 s liveness edge (45 s old), well short of the
    # 60 s removal edge - the disagreement band.
    ghost = await _seed_worker(
        clean_pg_conn,
        schema,
        queue="default",
        last_seen_sql="statement_timestamp() - interval '45 seconds'",
    )
    other = new_uuid()  # the sweeping leader, never its own row's reaper
    await _seed_actor_config(clean_pg_conn, schema, actor="test_actor", queue="default")
    await _seed_pending_job(clean_pg_conn, schema, actor="test_actor", queue="default")

    # The ghost's in-flight job: lease still future, heartbeat fresh - the
    # worker died with the row mid-flight and its columns say ALIVE.
    running_id = new_uuid()
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, attempt, claim_epoch, scheduled_at,
                locked_by_worker, lock_expires_at, started_at,
                last_heartbeat_at, cancel_phase
            ) VALUES ($1, 'test_actor', 'default', '{{"k": "v"}}'::jsonb, 3,
                      'transient', 'running', 0, 1, 1, clock_timestamp(),
                      $2, clock_timestamp() + interval '60 seconds',
                      clock_timestamp(), clock_timestamp(), 0)""",  # noqa: S608
        running_id,
        ghost,
    )

    # Liveness verdict: the queue reads unserved - the pending work is named.
    doctor = await _doctor_unserved(clean_pg_conn, schema, _LIVENESS_WINDOW)
    sweep = await _sweep_unserved(clean_pg_conn, schema, _LIVENESS_WINDOW)
    assert doctor == sweep == {"test_actor": (1, ("default",))}, (
        f"the 45 s ghost must read unserved on both surfaces:\n{doctor}\n{sweep}"
    )

    # Removal verdict: NOT yet - the row (and administratively, its leases)
    # survives its own window.
    removed = await cleanup_stale_workers(
        clean_pg_conn, worker_id=other, staleness=timedelta(seconds=60), schema=schema
    )
    assert removed == 0, f"the 45 s row was removed before the 60 s edge: {removed}"

    # Recovery verdict: sweep 1 does NOT rescue the running row on the
    # liveness finding - the lease is still future and the heartbeat arm is
    # quiet. The stuck job waits the lease out; that IS the documented path.
    reclaimed = await sweep_expired_locks(clean_pg_conn, timedelta(0), timedelta(0), schema=schema)
    assert reclaimed == 0, f"sweep 1 reclaimed a valid-lease row on a liveness verdict: {reclaimed}"
    status = await _job_status(clean_pg_conn, schema, running_id)
    assert status == "running", f"the stuck row moved without a lease expiry: {status}"

    # The documented cooperative cancel against a dead worker: a REQUEST
    # only (cli.md), the status stays running - the ghost can never land it.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET cancel_phase = 1, cancel_requested_at = '  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        "clock_timestamp() WHERE id = $1",
        running_id,
    )
    status = await _job_status(clean_pg_conn, schema, running_id)
    assert status == "running", "a cancel request against a dead worker landed itself"

    # The reclaim sweep is what resolves it: past the lease AND the
    # cancelling row's grace ladder (cancel grace + cleanup grace + 60 s,
    # the runbook's carve-out), it honours the row to ``cancelled``.
    await clean_pg_conn.execute(
        f"""UPDATE "{schema}".jobs
            SET lock_expires_at = statement_timestamp() - interval '70 seconds'
            WHERE id = $1""",  # noqa: S608  # Why: schema is identifier-validated upstream; values are bound.
        running_id,
    )
    reclaimed = await sweep_expired_locks(clean_pg_conn, timedelta(0), timedelta(0), schema=schema)
    assert reclaimed == 1, f"an expired cancelling row was not reclaimed: {reclaimed}"
    status = await _job_status(clean_pg_conn, schema, running_id)
    assert status == "cancelled", (
        f"the reclaim must honour an in-flight cancel to 'cancelled', saw {status}"
    )

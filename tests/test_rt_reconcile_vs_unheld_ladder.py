"""Red-team: the claim-loss reconcile vs the cancel ladder's unheld walk.

Two writers now own the same row class on current main, and their premises
collide for one row shape:

* The claim-loss reconcile (``worker/heartbeat.py``) reads ``running`` rows
  locked to this worker that NO in-memory structure holds (no registry
  entry, no claim intent, not queued, not disowned) and whose claim-stamped
  ``started_at`` has aged past one full lease. Its premise: "no registry
  entry means no actor ever ran this claim", so it refunds the claim-time
  attempt increment, un-stamps ``started_at``, and disowns the row for
  Sweep 1.
* The cancel ladder's unheld walk (``worker/cancel.py``) reads the SAME
  rows when they carry an operator cancel flag (the poll's predicate:
  ``locked_by_worker = this worker AND cancel_requested_at IS NOT NULL AND
  status = 'running'``). Its premise: the poll's predicate is ownership,
  the walk escalates the flag to phase 2 and abandons the row.

The colliding shape: a body that ran LONGER than the lock lease and exited
through the cancel fence. The fence (``mark_retry``'s ``cancel_phase = 0``
arms) no-ops on a phase-carrying row, the consumer's unconditional finally
deregisters, and the row is left ``running`` at phase 1 with NOTHING
holding it. Its ``started_at`` is the CLAIM stamp, so its age is the body's
full duration, not the age of the unheld state: a body that outlived the
lease makes the reconcile's age test true on the very first tick after the
exit, while the ladder's graces (cancel grace + cleanup grace) are still
counting from the walk's first sight.

When both writers fire, the reconcile's premise is FALSE (the body ran; the
attempt was earned) and the walk's "my abandon beats the age test" timing
premise is FALSE too (the abandon is bounded by the GRACES from first
sight, the age test is bounded by the LEASE from the claim, and a long body
makes the second bound pass first). The refund erases the executed
attempt's charge: the row abandons at the REFUNDED attempt number, and
``mark_abandoned``'s ledger INSERT collides with the genuine earlier
attempt's row (``ON CONFLICT DO NOTHING``), so the attempt whose body
actually ran has NO ``job_attempts`` row anywhere.

The contract pinned here: the reconcile must never touch a row carrying an
operator cancel. The ladder owns flagged rows while the worker lives (the
poll returns every one of them, held or not), Sweep 1's cancel arm owns
them when it dies, and the reconcile's "a claim that never reached an
actor" premise is structurally false for a flagged row: ``cancel_running``
stamps the flag only on a ``status = 'running'`` row, so a flagged row's
claim DID reach a holder. No timing premise survives between the two
writers; the exclusion is structural instead.

Deterministic, all real: the real rendered dispatch CTE (the claim), the
real rendered ``cancel_running`` (the operator flag), the real rendered
``mark_retry`` (the fenced outcome write), the real ``heartbeat_loop`` with
the real cancel controller (reconcile + ladder + drain), the real rendered
``mark_abandoned`` (the ladder's terminal write), real ``job_attempts``.
Timing anchors to the PG clock with aged stamps, no wall-clock sleeps.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import dispatch_batch
from taskq.backend._sql_templates import render
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop

pytestmark = pytest.mark.integration

_QUEUE = "reconcile_ladder_q"
_ACTOR = "reconcile_ladder_actor"

#: The reconcile's grace is one full lease on ``started_at``; seeds age the
#: stamp by double that so the probe fires on the first tick. The aged stamp
#: IS the long-body premise: the body ran two leases, the heartbeat renewed
#: the lease in flight, and the claim stamp never moved.
_LEASE = timedelta(seconds=3.0)
_GRACE_AGING = "6 seconds"

_HEARTBEAT_INTERVAL = "0.5"
_COMMAND_TIMEOUT = "0.1"


async def _seed_worker(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound or a module constant.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608
        f"VALUES ('{_ACTOR}', $1) ON CONFLICT (actor) DO NOTHING",
        _QUEUE,
    )


async def _seed_pending_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    max_attempts: int = 3,
    attempt: int = 0,
) -> UUID:
    rows = await conn.fetch(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "attempt, scheduled_at) VALUES (gen_random_uuid(), "
        f"'{_ACTOR}', '{_QUEUE}', '{{}}'::jsonb, 'pending', $1, 'transient', "
        "$2, clock_timestamp()) RETURNING id",
        max_attempts,
        attempt,
    )
    job_id: UUID = rows[0]["id"]
    return job_id


async def _seed_genuine_attempt_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """The genuine ledger row of a PRIOR attempt: what the abandoned row's
    refunded attempt number collides with."""
    await conn.execute(
        f'INSERT INTO "{schema}".job_attempts '  # noqa: S608
        "(job_id, attempt, started_at, finished_at, outcome) "
        "VALUES ($1, 1, clock_timestamp(), clock_timestamp(), 'succeeded')",
        job_id,
    )


async def _claim_via_real_dispatch_cte(
    conn: asyncpg.Connection, schema: str, worker_id: UUID
) -> asyncpg.Record:
    sql = render(schema).dispatch_strict_fifo
    rows = await dispatch_batch(
        conn,
        sql=sql,
        queues=[_QUEUE],
        limit_n=1,
        worker_id=worker_id,
        lock_lease=_LEASE,
    )
    assert len(rows) == 1, f"the claim must land: got {len(rows)} rows"
    return rows[0]


async def _operator_cancel(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """The real rendered cancel_running: stamp the operator's request on the
    running row (phase 0 -> 1)."""
    tag = await conn.execute(
        render(schema).cancel_running,
        job_id,
    )
    assert tag.endswith("1"), (
        "fixture broken: the operator cancel must land on the running row "
        "(cancel_running's guard is status='running' AND cancel_phase=0)"
    )


async def _fenced_outcome_write(
    conn: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID, row: dict[str, Any]
) -> None:
    """The body's exit through the cancel fence: the real rendered
    ``mark_retry`` with the attempt's OWN correct fence values (attempt,
    claim_epoch, ownership), so the ONLY failing conjunct is
    ``cancel_phase = 0``. A phase-carrying row matches NO arm: no row comes
    back, the write no-ops, and the consumer's unconditional finally
    deregisters (the caller's registry is empty by construction)."""
    rec = await conn.fetchrow(
        render(schema).mark_retry,
        job_id,
        worker_id,
        timedelta(seconds=1.0),  # $3 retry delay (the retried arm's delay)
        "ValueError",  # $4 error_class
        "body failed",  # $5 error_message
        None,  # $6 error_traceback
        0,  # $7 progress_seq
        None,  # $8 progress_state
        row["attempt"],  # $9 the caller's own claim view's attempt
        row["claim_epoch"],  # $10 the caller's own claim view's epoch
    )
    assert rec is None, (
        "fixture broken: mark_retry must fence out on a phase-carrying row "
        f"(the cancel fence matches no arm), got {dict(rec) if rec else None}"
    )


async def _age_started_at(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """Anchor the reconcile's grace to the PG clock: the claim stamp aged
    past one full lease (the long-body premise)."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET started_at = '  # noqa: S608
        "clock_timestamp() - interval '" + _GRACE_AGING + "' WHERE id = $1",
        job_id,
    )


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, Any]:
    rows = await conn.fetch(
        f"SELECT id::text, status::text AS status, attempt, claim_epoch, "  # noqa: S608
        f"started_at, cancel_phase, locked_by_worker::text AS locked_by "
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


def _heartbeat_settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": schema,
            "heartbeat_interval": _HEARTBEAT_INTERVAL,
            "heartbeat_command_timeout": _COMMAND_TIMEOUT,
            "lock_lease": str(_LEASE.total_seconds()),
            "watchdog_loop_lag_budget": "1.2",
            "watchdog_loop_lag_warn_budget": "0.5",
            "cancellation_grace_period": "0.0",
            "cleanup_grace_period": "0.0",
        }
    )


def _heartbeat_deps(pg_dsn: str, schema: str, pool: asyncpg.Pool) -> WorkerDeps:
    """Real deps on the real schema. The registry is EMPTY: the body exited
    and its unconditional finally deregistered, the exact post-fence shape."""
    return WorkerDeps(
        settings=_heartbeat_settings(pg_dsn, schema),
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: the drain reads this pool; a real pool is a drop-in.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _one_tick(deps: WorkerDeps, worker_id: UUID) -> None:
    """Run the real ``heartbeat_loop`` for exactly one tick, synchronised on
    the tick-duration hook (the idiom the heartbeat pins use). The real
    cancel controller is wired: this file's seam is the reconcile AND the
    ladder's unheld walk in one tick, both writers present."""
    import taskq.worker.heartbeat as hb_mod
    from taskq.worker.cancel import make_cancel_controller

    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    controller = make_cancel_controller(deps, worker_id, _LadderBackend(deps))
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]  # Why: the tick-complete hook the heartbeat unit tests synchronise on.

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]  # Why: as above.
    try:
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )
        await asyncio.wait_for(tick_done.wait(), timeout=10.0)
        shutdown.set()
        await task
    finally:
        hb_mod._tick_duration.record = prev_record  # type: ignore[method-assign,reportPrivateUsage]


class _LadderBackend:
    """The backend surface the controller's drain needs: the real rendered
    ``mark_abandoned`` and ``get`` on the deps' heartbeat pool. The walk's
    in-tx statements use the controller's own SQL on the tick's connection;
    only the post-commit drain's two calls go through this object."""

    def __init__(self, deps: WorkerDeps) -> None:
        self._pool = deps.heartbeat_pool
        self._schema = deps.settings.schema_name

    async def mark_abandoned(self, job_id: object) -> bool:
        rec = await self._pool.fetchrow(render(self._schema).mark_abandoned, job_id, 0, None)
        return rec is not None

    async def get(self, job_id: object) -> object:
        rows = await self._pool.fetch(
            f'SELECT status::text AS status FROM "{self._schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
        if not rows:
            return None
        return SimpleNamespace(status=rows[0]["status"])


async def test_reconcile_never_refunds_an_executed_attempt_the_ladder_owns(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """A cancel-flagged unheld row must stay the LADDER's: no reconcile refund.

    RED on main: the reconcile refunds the executed attempt and un-stamps
    the claim on the very first tick after the fenced exit, the walk then
    abandons the row at the refunded number, and the executed attempt has
    no ``job_attempts`` row anywhere.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    # One spent attempt with a genuine ledger row: the number the refund
    # lands on must be VISIBLY stolen from a real execution.
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=3, attempt=1)
    await _seed_genuine_attempt_row(clean_pg_conn, schema, job_id)

    # 1. The claim commits: running, locked, attempt 1 -> 2, started_at stamped.
    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["status"] == "running"
    assert claimed["attempt"] == 2, "the claim charges the attempt at claim time"

    # 2. The body runs LONGER than the lease (the lease renewed in flight,
    #    the claim stamp never moved) and exits through the cancel fence.
    await _operator_cancel(clean_pg_conn, schema, job_id)
    await _age_started_at(clean_pg_conn, schema, job_id)
    row = await _job_row(clean_pg_conn, schema, job_id)
    await _fenced_outcome_write(clean_pg_conn, schema, job_id, worker_id, row)
    # The consumer's finally deregistered: the row is running, locked here,
    # phase 1, held by NOTHING.
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "running" and row["cancel_phase"] == 1
    assert row["locked_by"] == str(worker_id)

    # 3. The real heartbeat tick: the reconcile AND the ladder's unheld walk
    #    (a real cancel controller, both writers of this seam).
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id)

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 2, (
        f"the reconcile must not refund the attempt of a row the cancel "
        f"ladder owns (the flag proves the claim reached a holder and the "
        f"body ran), got attempt={row['attempt']} - the executed attempt's "
        f"charge was erased"
    )
    assert row["started_at"] is not None, (
        "the reconcile must not un-stamp the executed attempt's started_at: "
        "the claim stamp is the execution's evidence, and the ladder's "
        "mark_abandoned ledger row reads it"
    )
    assert row["cancel_phase"] == 2, (
        "fixture broken: the ladder's unheld walk must have escalated the "
        "flagged row in the same tick (zero graces)"
    )

    # 4. The ladder completes: the abandon applies, the row exits limbo.
    await _one_tick(deps, worker_id)
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "abandoned", (
        f"the ladder's abandon must terminalise the unheld row, got {row}"
    )

    # 5. THE LEDGER: the executed attempt's row must exist. On main the
    #    abandon's INSERT ran at the REFUNDED number, collided with the
    #    genuine attempt-1 row, and the body's attempt vanished.
    attempts = await _attempt_rows(clean_pg_conn, schema, job_id)
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [
        (1, "succeeded"),
        (2, "cancelled"),
    ], (
        f"the executed attempt (2, the body the cancel fence fenced) must "
        f"carry its own job_attempts row with the abandon's ledger write, "
        f"got {attempts} - a body ran with no attempt row behind it"
    )


async def test_reconcile_still_owns_the_unflagged_lost_claim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Control: the fix must not blind the reconcile to its own class.

    A running row locked here, held by nothing, NO cancel flag: the
    reconcile refunds and disowns it exactly as the issue-458 contract
    pins, and Sweep 1's budget predicate reads the restored budget."""
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=1, attempt=0)

    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 1

    await _age_started_at(clean_pg_conn, schema, job_id)
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id)

    assert job_id in deps.disowned_jobs, (
        "the reconcile must still disown the unflagged lost claim (the "
        "issue-458 contract this file's fix must not break)"
    )
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "running"
    assert row["attempt"] == 0, "the unflagged claim's increment must be refunded"

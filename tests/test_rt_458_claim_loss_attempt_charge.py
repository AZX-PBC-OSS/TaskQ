"""Issue 458: the claim-loss reconcile charges an attempt to a job that never ran.

The chain, all on one real PostgreSQL:

1. The dispatch claim increments ``attempt`` at claim time, before any
   actor sees the job (``backend/_dispatch_sql.py``: ``attempt =
   LEAST(j.attempt + 1, 32767)``), and the claim is autocommit.
2. A round that commits its claim and then fails (the #402 shape) leaves
   the row ``running``, locked, and held by no in-memory structure.
3. The heartbeat's claim-loss reconcile (``worker/heartbeat.py``) finds
   the orphan after one full lease on ``started_at`` and disowns it. The
   ONLY write is the in-memory ``disowned_jobs`` update: nothing refunds
   the claim-time increment.
4. The lease lapses and Sweep 1 evaluates its budget predicate
   (``backend/_sweeps.py``: ``WHEN {has_budget} THEN 'pending' ... ELSE
   'crashed'``). A ``max_attempts=1`` job (or a ``non_retryable`` one at
   its budget edge) lands terminal ``crashed`` having never executed, and
   the crashed arm writes a ``job_attempts`` row for the charged attempt:
   a record asserting an execution that did not happen, indistinguishable
   from a genuine mid-execution crash.

The rule the sibling sink already enforces (``worker/shutdown.py``'s
``drain_local_queue_to_pending``, which refunds through
``ATTEMPT_REFUND_SQL``): a claim that never reached an actor bought
nothing, so it spends nothing. The reconcile must refund the same way
while keeping the row ``running`` so Sweep 1 still owns the reclaim.

No fakes: the real rendered dispatch CTE (``_sql_templates.render`` →
``_dispatch_sql.dispatch_batch``), the real ``heartbeat_loop``, and the
real ``sweep_expired_locks`` so the sweep's own attempt and event writes
are in scope. Timing is deterministic by seeding: the reconcile's grace
and the sweep's expiry are anchored to the PG clock with aged
``started_at`` / ``lock_expires_at`` stamps instead of wall-clock sleeps
(the same discipline the sweep pins use).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import dispatch_batch
from taskq.backend._sql_templates import render
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE, heartbeat_loop
from taskq.worker.shutdown import drain_local_queue_to_pending

pytestmark = pytest.mark.integration

_QUEUE = "issue458_q"

#: The reconcile's grace is one full lease on ``started_at``; seeds age the
#: stamp by double that so the probe fires on the first tick.
_LEASE = timedelta(seconds=3.0)
_GRACE_AGING = "6 seconds"

#: Settings for the heartbeat deps. The interval only paces the loop's
#: wait between ticks: the test synchronises on the tick hook, never on
#: the clock.
_HEARTBEAT_INTERVAL = "0.5"
_COMMAND_TIMEOUT = "0.1"


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
        "VALUES ('issue458_actor', $1) ON CONFLICT (actor) DO NOTHING",
        _QUEUE,
    )


async def _seed_pending_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    max_attempts: int = 1,
    attempt: int = 0,
) -> UUID:
    """One pending job at the given counter: the default is the
    budget-edge shape the issue measures (``max_attempts=1``, any claim
    consumes the whole budget); ``attempt`` seeds prior spend for the pins
    that must observe a counter move."""
    rows = await conn.fetch(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "attempt, scheduled_at) VALUES (gen_random_uuid(), 'issue458_actor', "
        f"'{_QUEUE}', '{{}}'::jsonb, 'pending', $1, 'transient', "
        "$2, clock_timestamp()) RETURNING id",
        max_attempts,
        attempt,
    )
    job_id: UUID = rows[0]["id"]
    return job_id


async def _claim_via_real_dispatch_cte(
    conn: asyncpg.Connection, schema: str, worker_id: UUID
) -> asyncpg.Record:
    """The real rendered dispatch CTE, the real claim: autocommit, the row
    is ``running`` and charged before this helper returns."""
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


async def _age_started_at(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """Anchor the reconcile's grace to the PG clock (no wall-clock sleep):
    the claim's ``started_at`` ages past one full lease."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET started_at = '  # noqa: S608
        "clock_timestamp() - interval '" + _GRACE_AGING + "' WHERE id = $1",
        job_id,
    )


async def _age_lock_expiry(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """Anchor Sweep 1's lease-arm eligibility to the PG clock: the disowned
    row's lease has lapsed."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET lock_expires_at = '  # noqa: S608
        "clock_timestamp() - interval '1 second' WHERE id = $1",
        job_id,
    )


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, Any]:
    rows = await conn.fetch(
        f"SELECT id::text, status::text AS status, attempt, claim_epoch, "  # noqa: S608
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
        f"SELECT attempt, outcome::text AS outcome, error_message "  # noqa: S608
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
    """Real deps on the real schema: empty registries, the claimed row is
    held by nothing, exactly the post-claim-loss shape."""
    return WorkerDeps(
        settings=_heartbeat_settings(pg_dsn, schema),
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: the drain reads this pool; a real pool is a drop-in.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _one_tick(
    deps: WorkerDeps,
    worker_id: UUID,
    cancel_controller: object | None = None,
) -> None:
    """Run the real ``heartbeat_loop`` for exactly one tick against real
    PG, synchronised on the tick-duration hook (the idiom the disowned-jobs
    pins use, here over a live pool). ``cancel_controller`` optionally
    injects the tick's cancel-poll hook (the failing-hook shape drives the
    brownout-tick paths)."""
    import taskq.worker.heartbeat as hb_mod

    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]  # Why: the tick-complete hook the heartbeat unit tests synchronise on.

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]  # Why: as above.
    try:
        task = asyncio.create_task(
            heartbeat_loop(
                deps,
                worker_id,
                shutdown,
                cancel_controller=cancel_controller,  # type: ignore[arg-type]  # Why: the test hook satisfies the structural protocol at runtime.
            )
        )
        await asyncio.wait_for(tick_done.wait(), timeout=10.0)
        shutdown.set()
        await task
    finally:
        hb_mod._tick_duration.record = prev_record  # type: ignore[method-assign,reportPrivateUsage]


async def test_reconcile_refunds_the_claim_time_increment_of_a_job_that_never_ran(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The claim-loss reconcile must never charge an attempt to a job that
    never started executing.

    RED on main (the reconcile disowns without refunding): the sweep
    terminalises the never-executed job 'crashed' and writes a
    ``job_attempts`` row for the charged attempt, the record the issue
    measured:

    ``state-change attempt=1 cause=lock_expired to_state=crashed`` /
    ``job_attempts: [(1, 'crashed', 'lock expired before worker reported
    terminal state')]``
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    # 1. The claim commits (autocommit): running, locked, attempt charged.
    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["status"] == "running"
    assert claimed["attempt"] == 1, "the claim charges the attempt at claim time"
    assert claimed["locked_by_worker"] == worker_id

    # 2. The round dies after the commit (the #402 shape): the JobRow batch
    #    is discarded, the row is held by no in-memory structure.

    # 3. The real heartbeat_loop's claim-loss reconcile probes the orphan
    #    (started_at aged past one lease) and disowns it.
    await _age_started_at(clean_pg_conn, schema, job_id)
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id)
    assert job_id in deps.disowned_jobs, (
        "the reconcile must disown the orphan: without the disown the row is "
        "renewed for the worker's lifetime (the #402 defect)"
    )

    # The disown must NOT have charged the attempt: the row stays running,
    # locked to this worker (Sweep 1 owns the reclaim), at the attempt the
    # job actually reached, which for a never-executed claim is the
    # pre-claim counter.
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "running", (
        f"the reconcile must keep the row running so Sweep 1 owns the reclaim, got {row}"
    )
    assert row["locked_by"] == str(worker_id)
    assert row["attempt"] == 0, (
        f"the reconcile must refund the claim-time increment of a job that "
        f"never started executing (a claim that never reached an actor bought "
        f"nothing, so it spends nothing), got attempt={row['attempt']}"
    )

    # 4. The lease lapses; the real Sweep 1 reclaims. With the budget
    #    restored the job must re-pend, NOT terminalise.
    await _age_lock_expiry(clean_pg_conn, schema, job_id)
    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,
        timedelta(0),
        timedelta(0),
        schema=schema,
    )
    assert count == 1, f"the lapsed row must be reclaimed, sweep count={count}"

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "pending", (
        f"a never-executed job with its budget restored must re-pend, got "
        f"{row}: the sweep fabricated a terminal state for an execution that "
        f"did not happen"
    )

    # The record: the sweep's reclaim ledger must sit at the REFUNDED
    # attempt - which, the refund having de-charged the increment, is NO
    # attempt row at all. The reclaim's crashed-row INSERT is fenced on
    # the STANDING CLAIM (the un-stamped started_at is the refund's void
    # marker): a crashed row at the refunded number would permanentise an
    # epoch the counter no longer carries, and the next claim's re-mint
    # of the number would close the lineage with one more attempt row
    # than the counter - the soak's reconciliation red (``attempt counter
    # 1 vs 2 attempt rows``, run 36175331443). The reclaim is still
    # audited - by the job_events state_change, which fires either way.
    # On main pre-fence the unrefunded charge left [(1, 'crashed', ...)]
    # and the pre-fence fence left the phantom [(0, 'crashed')]: both
    # fabricated an execution that did not happen.
    attempts = await _attempt_rows(clean_pg_conn, schema, job_id)
    assert attempts == [], (
        f"the reclaim's attempt ledger must not claim the charged attempt "
        f"(the claim charged attempt 1 and nothing ever executed it) NOR "
        f"the refunded number (an epoch the counter no longer carries), "
        f"got {attempts}"
    )

    # 5. The budget was not consumed: the job re-dispatches. The reclaim's
    #    re-pend schedules the row on the retry backoff (the same hand-back
    #    delay every reclaim arm stamps); anchor it to the PG clock so the
    #    re-claim is deterministic.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET scheduled_at = '  # noqa: S608
        "clock_timestamp() - interval '1 second' WHERE id = $1",
        job_id,
    )
    reclaims = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert reclaims["attempt"] == 1, (
        f"the re-claim must be the job's FIRST attempt, got attempt="
        f"{reclaims['attempt']}: the reconcile's unrefunded charge spent a "
        f"retry this job never used"
    )


async def test_reconcile_refund_is_exactly_once_across_a_same_id_restart(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The refund must be exactly-once per claim.

    Within a process the reconcile's exclusion array folds the disowned
    set, so the refunded row stops matching. Across a restart
    ``disowned_jobs`` is empty and the row still satisfies the reconcile's
    own predicate, so a restarted worker with the same id would refund it
    twice: a double refund revisits an attempt number a genuine earlier
    execution may already hold a ``job_attempts`` row for, the exact
    PK-collision the refund idiom's safety argument rules out. The refund
    must therefore move the row out of its own predicate's match set.

    The seeded job carries one spent attempt (a genuine earlier
    execution's terminal row) so the double refund is observable: the
    GREATEST floor in the shared fragment would absorb a second refund of
    a fresh job's counter, and the pin would pass vacuously.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=3, attempt=1)
    # The spent attempt's genuine ledger row: what a second refund would
    # make the next claim's terminal write collide with.
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".job_attempts '  # noqa: S608
        "(job_id, attempt, started_at, finished_at, outcome) "
        "VALUES ($1, 1, clock_timestamp(), clock_timestamp(), 'succeeded')",
        job_id,
    )

    await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    await _age_started_at(clean_pg_conn, schema, job_id)

    # First process: the reconcile refunds once (the claim charged
    # attempt 2; the refund returns the counter to the spent attempt).
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id)
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 1, f"the first reconcile must refund once, got {row}"

    # The restart: same worker id, EMPTY disowned set (fresh process
    # memory), the row still running and locked to this id. One more tick.
    deps_after_restart = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    assert not deps_after_restart.disowned_jobs
    await _one_tick(deps_after_restart, worker_id)

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 1, (
        f"the refund must be exactly-once per claim: a restarted worker with "
        f"the same id re-probed the orphan and refunded a second time, "
        f"got attempt={row['attempt']} - the next claim would re-create the "
        f"spent attempt epoch its genuine ledger row already holds"
    )
    assert row["status"] == "running", f"the refunded row must stay Sweep 1's, got {row}"


async def test_drain_refunds_the_same_never_started_row_shape(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Control: the sibling sink (the shutdown drain) already refunds the
    identical row shape through the shared fragment. This passes before and
    after the fix; it is the semantic the reconcile must match."""
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)

    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    drained = await drain_local_queue_to_pending(deps, worker_id)
    assert drained == 1, "the drain must hand the claimed-but-unstarted row back"

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "pending", f"the drain re-pends, got {row}"
    assert row["attempt"] == 0, (
        f"the drain refunds the claim-time increment ('a claim that never "
        f"reached an actor bought nothing, so it spends nothing'), got "
        f"attempt={row['attempt']}"
    )


# ── The exactly-once refund invariant ACROSS the two refund writers ─────


async def _seed_spent_attempt(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """A genuine earlier execution's ledger row at attempt 1.

    The shared refund fragment floors at 0 (``GREATEST(j.attempt - 1,
    0)``), so a second refund of a fresh job's counter is invisible. The
    spent row defeats the floor: the counter stands at 1 before the
    claim, so the first refund lands on 1 and a SECOND refund drives the
    counter below the epoch the genuine ledger row holds - an observable
    counter corruption, not a floor absorption.
    """
    await conn.execute(
        f'INSERT INTO "{schema}".job_attempts '  # noqa: S608
        "(job_id, attempt, started_at, finished_at, outcome) "
        "VALUES ($1, 1, clock_timestamp(), clock_timestamp(), 'succeeded')",
        job_id,
    )


async def test_drain_never_re_refunds_a_row_the_reconcile_refunded(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The refund is exactly-once per claim ACROSS the two writers.

    The heartbeat's claim-loss reconcile refunds a lost claim and, by
    design, leaves the row ``running`` and locked (Sweep 1 owns the
    reclaim). The shutdown drain's exclusion folds only ``held_ids()``
    (registered consumers + claim intents), so the reconciled row still
    matches the drain's predicate: ``running``, locked to this worker,
    held by nothing, ``cancel_phase = 0``. The drain re-pends the row AND
    refunds the SAME claim a second time: the counter the reconcile
    restored is driven one further down, past the epoch a genuine
    execution's ``job_attempts`` row holds, and the row is yanked out of
    the Sweep-1 reclaim the reconcile deliberately preserved.

    Sequential form: the reconcile's tick completes, then the shutdown's
    DRAINING pass runs. This is the common rolling-deploy shape - a
    claim-loss window followed by a shutdown inside one lease.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=5, attempt=1)
    await _seed_spent_attempt(clean_pg_conn, schema, job_id)

    # The claim charges attempt 2 (1 + 1) and stamps started_at.
    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 2, "fixture broken: the claim must charge attempt 2"

    # The reconcile refunds once: attempt 2 -> 1, started_at un-stamped,
    # the row still running and locked, the id disowned.
    await _age_started_at(clean_pg_conn, schema, job_id)
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id)
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 1 and row["status"] == "running", (
        f"fixture broken: the reconcile must refund once and leave the row running, got {row}"
    )

    # The drain runs (the DRAINING pass, or the producer's exit pass).
    # The row is already refunded: the drain must not touch it at all -
    # no re-pend, no second refund. The row stays Sweep 1's.
    drained = await drain_local_queue_to_pending(deps, worker_id)

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert drained == 0, (
        f"the drain must not re-pend a row the reconcile already refunded "
        f"(Sweep 1 owns that reclaim), drained={drained}"
    )
    assert row["status"] == "running", (
        f"the refund is exactly-once per claim across the two writers: the "
        f"drain re-pended a row the reconcile had already refunded, got {row}"
    )
    assert row["attempt"] == 1, (
        f"the drain re-refunded the reconcile's claim: attempt {row['attempt']} "
        f"is below the epoch the genuine job_attempts row holds - the next "
        f"claim re-creates a spent attempt epoch"
    )


async def test_drain_concurrent_with_the_reconcile_refunds_exactly_once(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The race form: the drain's UPDATE blocks on the reconcile's row
    lock and re-evaluates the row when the reconcile commits.

    The reconcile's refund runs inside the heartbeat's open transaction;
    the drain's UPDATE on a second connection takes the row lock behind
    it. When the reconcile commits, PostgreSQL re-evaluates the blocked
    row against the drain's predicate on the NEW row version
    (EvalPlanQual): the new version is the refunded one, so the
    exactly-once guard must hold on the re-evaluation too. An exclusion
    set bound from process memory (a snapshot of ``disowned_jobs`` taken
    before the reconcile committed) cannot see the refund; a row-version
    conjunct can.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema, max_attempts=5, attempt=1)
    await _seed_spent_attempt(clean_pg_conn, schema, job_id)

    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 2, "fixture broken: the claim must charge attempt 2"
    await _age_started_at(clean_pg_conn, schema, job_id)

    reconcile_sql = _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema)
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)

    # The reconcile's open transaction: the refund is written and holds
    # the row lock, uncommitted. The exclusion array is empty - the
    # in-memory coverage is exactly the post-claim-loss shape.
    async with clean_pg_conn.transaction():
        await clean_pg_conn.execute(reconcile_sql, worker_id, [], _LEASE)

        # The drain races: its UPDATE blocks on the row lock the open
        # reconcile transaction holds. The task is left running while the
        # transaction commits underneath it.
        drain_task = asyncio.create_task(drain_local_queue_to_pending(deps, worker_id))
        await asyncio.sleep(0.5)

    # The reconcile committed; the drain's blocked statement re-evaluates
    # the refunded row version and completes.
    drained = await asyncio.wait_for(drain_task, timeout=10.0)

    row = await _job_row(clean_pg_conn, schema, job_id)
    assert drained == 0, (
        f"the drain must drop the row the committed reconcile just refunded, drained={drained}"
    )
    assert row["status"] == "running", f"the concurrent drain re-pended the refunded row, got {row}"
    assert row["attempt"] == 1, (
        f"the concurrent drain re-refunded the reconcile's claim after "
        f"EvalPlanQual re-evaluation, got attempt={row['attempt']} - the "
        f"exactly-once guard must hold on the re-checked row version"
    )


class _FailingInTxHook:
    """The cancel controller whose IN-TX phase fails: the OSError family
    the heartbeat loop treats as a transient tick failure (the brownout
    tick the disown-after-commit fix is pinned against)."""

    async def run_in_tx(self, conn: asyncpg.Connection) -> None:
        raise OSError("the in-tx cancel poll failed: the brownout tick")

    async def run_post_tx(self) -> None:
        return None


async def test_reconcile_disown_does_not_survive_a_failed_tick(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The disown is MEMORY state; the refund is DB state. A tick that
    matches a lost claim and then FAILS (the in-tx cancel hook's OSError,
    a cut commit, a dropped connection) rolls the refund back - and the
    disown, applied before the commit, would survive it: the row would
    then match neither the renewal nor the reconcile (the exclusion set
    folds the disowned), the refund could NEVER re-apply, the lease would
    lapse, and Sweep 1 would reclaim at the CHARGED attempt - issue 458's
    'crashed, never ran' record resurrected by one brownout tick.

    The pin: the failed tick leaves NO disown behind, and the next
    healthy tick re-reconciles the row and refunds it for real."""
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    # The claim commits; the round dies after it; the orphan ages past
    # one lease - the reconcile's exact prey.
    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["attempt"] == 1
    await _age_started_at(clean_pg_conn, schema, job_id)

    # The brownout tick: the reconcile matches (it logs the match), then
    # the in-tx hook fails and the tick rolls back.
    deps = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps, worker_id, cancel_controller=_FailingInTxHook())

    # THE PIN: the disown did not survive the rollback. (On the pre-fix
    # code this assertion fails - the disown was applied before the
    # commit - and the row below is then unreachable by every later
    # reconcile: the refund is lost permanently.)
    assert job_id not in deps.disowned_jobs, (
        "a failed tick's disown survived its rollback: the refund un-applied "
        "but the exclusion stayed - the row can never be re-reconciled, the "
        "refund is lost, and Sweep 1 will reclaim at the charged attempt"
    )
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 1, (
        f"the rolled-back refund must leave the charge standing for the "
        f"next tick to re-reconcile, got attempt={row['attempt']}"
    )

    # The next healthy tick re-reconciles: the refund lands for real.
    deps_healthy = _heartbeat_deps(module_pg_schema.pg_dsn, schema, module_pg_pool)
    await _one_tick(deps_healthy, worker_id)
    assert job_id in deps_healthy.disowned_jobs
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 0, (
        f"the re-reconcile must refund the claim-time increment, got "
        f"attempt={row['attempt']}: the fix must not merely delay the loss"
    )

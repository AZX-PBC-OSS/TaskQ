"""The refund-then-reclaim ledger race: the counter and the ledger must
move TOGETHER across the claim-loss reconcile's refund.

The soak's grand mixin red on CI (run 36175331443, ``grand_mixin[1]``) at
the reconciliation pin: ``attempt counter 1 vs 2 attempt rows``. The
mechanism, frozen here one writer at a time (the writers genuinely run
concurrently in production - the heartbeat tick on the worker's loop, the
leader's sweep on its own - and PostgreSQL's row lock admits exactly this
interleaving; the reverse order is the consistent schedule the ledger
guard already covers):

1. The dispatch claim charges ``attempt`` 0 -> 1 and stamps
   ``started_at`` (``backend/_dispatch_sql.py``). The attempt LEDGER row
   does not exist yet: it lands only at the attempt's terminal write.
2. The consumer is lost (the #402 shape: the claim committed, the
   in-memory grip did not survive). The heartbeat's claim-loss reconcile
   finds the orphan - running, locked here, held by nothing, its stamp
   aged past the lease - and its NOT EXISTS ledger guard passes (no
   ``job_attempts`` row at the charged number), so the refund fires BY
   DESIGN: the counter rolls 1 -> 0 and ``started_at`` is un-stamped
   (issue 458: a claim that never reached an actor bought nothing, so it
   spends nothing). The row stays ``running`` and locked for Sweep 1.
3. THE RACE: the reclaim path records the crash of the attempt it
   reclaims AT THE ROW'S CURRENT COUNTER - the number the refund just
   de-charged. The ledger gains a phantom row (attempt 0) for an
   execution that never happened, while the counter stands at 0.
4. The re-claim charges 0 -> 1 (the refund's whole point: the number is
   revisited) and its terminal write lands the ledger row for attempt 1.
5. The lineage closes at counter 1 with ledger rows {0, 1} - the soak's
   ``attempts == attempt`` reads 2 vs 1 and reds.

The fence the trace proves missing: the reclaim's attempt INSERT must be
fenced on the STANDING CLAIM. Dispatch stamps ``started_at`` at claim;
the refund un-stamps it as it de-charges; so a reclaimed running row
whose stamp is NULL carries no claim, and the honest ledger record for
its reclaim is NOTHING (the state-change event still lands - the reclaim
happened and is audited; the execution a crashed row would assert never
did). With the fence, step 3 records nothing, and the lineage closes at
counter 1 with ledger rows {1}: the counter and the ledger moved
together.

The same fence covers the sibling reclaim writer (``isolate_self``'s
attempt INSERT, worker/heartbeat.py), whose stale-snapshot write carries
the identical gap - and whose raw NULL stamp would additionally violate
``job_attempts.started_at NOT NULL`` on the refunded shape, aborting the
whole isolation transaction.

THE FENCE'S GAP, precisely (worker/heartbeat.py, pre-fix): the terminal
writes are airtight - their JOB_FENCE carries ``attempt = $k`` AND
``claim_epoch = $m`` and every dispatch claim bumps the epoch
(backend/_dispatch_sql.py's ``claim_epoch = j.claim_epoch + 1``), so a
stale consumer's terminal write no-ops on the re-minted epoch. The
isolate's attempt INSERT had NO such fence: it bound the SELECT
snapshot's (attempt, started_at), and the arbiter UPDATE in front of it
fenced only on ``(id, status='running', locked_by_worker)`` - a
conjunction the REFUND leaves true (the de-charged row stays running and
locked by the same worker). So the refund committing inside the
SELECT->UPDATE window - the heartbeat pool's tick, or the shutdown
drain's ATTEMPT_REFUND_SQL arm, both concurrent with the isolate's
dedicated connection - left the arbiter winning a row whose charge was
already refunded, and the INSERT minting the SNAPSHOT's epoch after the
refund. The fix reads the fence from the ARBITER's RETURNING (the row
lock means no refund can interleave after it; a refund before it shows
up as the un-stamped, de-charged row) - a snapshot-based check cannot
see the refund and does not close this gap.

No fakes: the real rendered dispatch CTE, the real reconcile statement,
the real ``sweep_expired_locks``, and the real fused ``mark_succeeded``
terminal write. Timing is deterministic by seeding: the reconcile's grace
and the sweep's expiry are anchored to the PG clock with aged
``started_at`` / ``lock_expires_at`` stamps instead of wall-clock sleeps
(the same discipline the sibling pins use).
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated by the backend; every value is $-bound or a module constant.

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._sql_templates import render
from taskq.backend._terminal import _mark_succeeded_on_conn
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import (
    _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE,
    _SELECT_RUNNING_JOBS_SQL_TEMPLATE,
    isolate_self,
)

pytestmark = pytest.mark.integration

_QUEUE = "ledger_race_q"

#: The reconcile's grace is one full lease on ``started_at``; seeds age the
#: stamp by double that so the refund fires deterministically.
_LEASE = timedelta(seconds=3.0)
_GRACE_AGING = "6 seconds"


async def _seed_worker(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    # The dispatch CTE's candidate walk iterates the actor registry
    # (per_actor_capacity over actor_config); an unregistered actor's
    # pending rows are invisible to every round.
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ('ledger_race_actor', $1) ON CONFLICT (actor) DO NOTHING",
        _QUEUE,
    )


async def _seed_pending_job(conn: asyncpg.Connection, schema: str) -> UUID:
    rows = await conn.fetch(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        f"scheduled_at) VALUES (gen_random_uuid(), 'ledger_race_actor', "
        f"'{_QUEUE}', '{{}}'::jsonb, 'pending', 5, 'transient', "
        "clock_timestamp()) RETURNING id",
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
        f'UPDATE "{schema}".jobs SET started_at = '
        "clock_timestamp() - interval '" + _GRACE_AGING + "' WHERE id = $1",
        job_id,
    )


async def _age_lock_expiry(conn: asyncpg.Connection, schema: str, job_id: UUID) -> None:
    """Anchor Sweep 1's lease-arm eligibility to the PG clock: the refunded
    row's lease has lapsed."""
    await conn.execute(
        f'UPDATE "{schema}".jobs SET lock_expires_at = '
        "clock_timestamp() - interval '1 second' WHERE id = $1",
        job_id,
    )


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, Any]:
    rows = await conn.fetch(
        f"SELECT status::text AS status, attempt, claim_epoch, "
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
        f"SELECT attempt, outcome::text AS outcome "
        f'FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
        job_id,
    )
    return [dict(r) for r in rows]


async def test_reclaim_records_no_attempt_row_for_a_refunded_epoch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The race pin, frozen in the racing order: refund FIRST (the
    reconcile's tick), reclaim SECOND (the leader's sweep). Red on main at
    the soak's own assert shape; green with the standing-claim fence.

    The claim's ledger row is written by the terminal write at the END of
    an attempt, never by the claim itself - which is exactly why the
    reconcile's NOT EXISTS guard passes for a claim whose consumer was
    lost mid-flight: the ledger has had nothing to say about the epoch
    YET. The refund's de-charge and the reclaim's crashed-row INSERT are
    the two writers that must move together.
    """
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    await _seed_worker(clean_pg_conn, schema, worker_id)
    job_id = await _seed_pending_job(clean_pg_conn, schema)

    # 1. The claim commits (autocommit): running, locked, attempt charged,
    #    started_at stamped, LEDGER EMPTY (the terminal write owns that row).
    claimed = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert claimed["status"] == "running"
    assert claimed["attempt"] == 1, "the claim charges the attempt at claim time"
    assert await _attempt_rows(clean_pg_conn, schema, job_id) == [], (
        "fixture broken: the claim must not write the ledger row"
    )

    # 2. The consumer is lost mid-attempt (the #402 shape). The reconcile's
    #    NOT EXISTS ledger guard passes - no row yet - and the refund fires
    #    by design: the counter rolls back, the stamp is un-stamped, the
    #    row stays running and locked for Sweep 1's reclaim.
    await _age_started_at(clean_pg_conn, schema, job_id)
    refunded = await clean_pg_conn.fetch(
        _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema),
        worker_id,
        [],
        _LEASE,
    )
    assert [r["id"] for r in refunded] == [job_id], (
        "fixture broken: the reconcile must refund the lost claim"
    )
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["attempt"] == 0 and row["started_at"] is None, (
        f"fixture broken: the refund must de-charge the increment and un-stamp the claim, got {row}"
    )

    # 3. THE RACE: the lease lapses and the leader's Sweep 1 reclaims the
    #    refunded row. The reclaim must re-pend the row AND record NO
    #    attempt row: the refunded epoch carries no standing claim (the
    #    un-stamped started_at is the durable void marker), and a crashed
    #    row at that number would permanentise an epoch the counter no
    #    longer carries. Pre-fix, the reclaim's batched INSERT writes the
    #    phantom row (job_id, 0, 'crashed') with a fabricated clock stamp.
    await _age_lock_expiry(clean_pg_conn, schema, job_id)
    count = await PostgresBackend.sweep_expired_locks(
        clean_pg_conn,
        timedelta(0),
        timedelta(0),
        schema=schema,
    )
    assert count == 1, f"the lapsed row must be reclaimed, sweep count={count}"
    row = await _job_row(clean_pg_conn, schema, job_id)
    assert row["status"] == "pending", f"the reclaim must re-pend the row, got {row}"

    # The reclaim EVENT still lands - the reclaim happened and is audited.
    events = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
        "AND kind = 'state_change' AND detail->>'reason' = 'lock_expired'",
        job_id,
    )
    assert events == 1, "the reclaim's state_change event must land either way"

    assert await _attempt_rows(clean_pg_conn, schema, job_id) == [], (
        "the reclaim must record NO attempt row for a refunded epoch: the "
        "un-stamped started_at is the refund's void marker, and a crashed "
        "row at the refunded number permanentises an epoch the counter no "
        "longer carries - the next claim re-mints that number and the "
        "lineage closes with one more attempt row than the counter"
    )

    # 4. The budget was restored, so the job re-dispatches (the refund's
    #    whole point: the number is revisited). Anchor the reclaim backoff
    #    to the PG clock so the re-claim is deterministic.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET scheduled_at = '
        "clock_timestamp() - interval '1 second' WHERE id = $1",
        job_id,
    )
    reclaims = await _claim_via_real_dispatch_cte(clean_pg_conn, schema, worker_id)
    assert reclaims["attempt"] == 1, (
        f"the re-claim must be the job's FIRST attempt, got attempt="
        f"{reclaims['attempt']}: the refund exists so a never-executed "
        f"claim spends nothing"
    )

    # 5. The re-claimed attempt runs and its terminal write lands the
    #    ledger row - the real fused mark_succeeded the consumer issues.
    row = await _job_row(clean_pg_conn, schema, job_id)
    applied = await _mark_succeeded_on_conn(
        clean_pg_conn,
        render(schema),
        JobId(job_id),
        worker_id,
        {"ok": True},
        attempt=1,
        claim_epoch=row["claim_epoch"],
    )
    assert applied is True, "fixture broken: the live attempt's terminal write must apply"

    # 6. THE SOAK'S EXACT INVARIANT (tests/test_rt_lost_job_soak.py's
    #    reconciliation pin, the assert shape the CI red carried): the
    #    attempt counter and the attempt ledger must agree at settle.
    lineage = await clean_pg_conn.fetchrow(
        f"SELECT j.attempt, "
        f'(SELECT count(*)::int FROM "{schema}".job_attempts a '
        f"WHERE a.job_id = j.id) AS attempts "
        f'FROM "{schema}".jobs j WHERE j.id = $1',
        job_id,
    )
    assert lineage is not None
    assert lineage["attempts"] == lineage["attempt"], (
        f"job {job_id}: attempt counter {lineage['attempt']} vs "
        f"{lineage['attempts']} attempt rows - a claim was double-applied "
        "or a claim's ledger row was lost"
    )
    attempts = await _attempt_rows(clean_pg_conn, schema, job_id)
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [(1, "succeeded")], (
        f"the ledger must hold exactly the epochs the counter charged, got "
        f"{attempts}: the phantom epoch the refund de-charged must not stand"
    )


# ── The isolate-self leg: the stale-snapshot writer ───────────────────


async def _open_isolate_deps(
    pg_dsn: str,
) -> tuple[AsyncExitStack, WorkerDeps]:
    """A real WorkerDeps on a fresh schema, the property test's shape
    (tests/test_leader_property.py): state assertions, not timing, so the
    heartbeat pool's command timeout is bounded loosely enough that a
    parallel ``-n 2`` runner's loop starvation cannot red it."""
    from taskq.testing.fixtures import (
        _open_pg_backend,  # pyright: ignore[reportPrivateUsage]  # Why: _open_pg_backend is a shared test helper published by the testing module; private prefix scopes it within the testing package.
    )

    stack, deps, _backend = await _open_pg_backend(
        pg_dsn,
        schema_name=f"tlr_{new_base62()}".lower(),
        settings_overrides={
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "5.0",
            "TASKQ_LOCK_LEASE": "60.0",
            # The isolate's dedicated connection binds THIS budget per
            # statement (worker/heartbeat.py's connect call), and the lock
            # dance below parks its arbiter UPDATE on the holder's FOR
            # UPDATE for as long as the freeze needs: the blitz default's
            # per-query bound would kill the parked UPDATE before the
            # refund commits inside the window. The setting's own budget
            # arithmetic caps it (timeout + the leader-loop period must
            # stay under the watchdog's staleness budget), hence 5s, and
            # the freeze's poll bound stays under it.
            "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "5.0",
        },
    )
    return stack, deps


async def _wait_for_lock_waiter(
    conn: asyncpg.Connection, schema: str, *, timeout_secs: float = 4.0
) -> None:
    """Poll until some session is queued on an ungranted lock: the
    isolate's arbiter UPDATE parked on the holder's FOR UPDATE. Waiting
    on a lock is a state, not a delay - once it is observable, the
    isolate's SELECT (which precedes the arbiter) is guaranteed to have
    taken its snapshot. pg_locks, not pg_stat_activity's wait_event
    columns: the parked arbiter's wait_event registration proved
    unreliable under the parallel runner, the ungranted pg_locks row is
    the arbiter's own queue entry and does not lag."""
    del schema
    waited = 0.0
    while waited < timeout_secs:
        waiters = await conn.fetchval(
            "SELECT count(*) FROM pg_locks WHERE pid <> pg_backend_pid() AND NOT granted",
        )
        assert waiters is not None and int(waiters) <= 1, (
            f"fixture broken: {waiters} unexpected lock waiters in the database"
        )
        if int(waiters) == 1:
            return
        await asyncio.sleep(0.01)
        waited += 0.01
    raise AssertionError(
        f"the isolate's arbiter never parked on the held row lock within "
        f"{timeout_secs}s - the SELECT->UPDATE window cannot be frozen"
    )


async def test_isolate_self_records_no_attempt_row_for_a_refunded_epoch(
    pg_dsn: str,
) -> None:
    """The isolate leg, end to end through the REAL ``isolate_self``: the
    refund has ALREADY de-charged the row when the isolate's SELECT runs
    (attempt 0, started_at NULL). Pre-fix the arbiter still won the
    transition (its WHERE: id + running + holder, all left true by the
    refund) and the attempt INSERT bound the refunded state raw -
    a phantom row at the refunded number, or the NotNullViolation the
    NULL stamp raises on job_attempts.started_at, either way the
    isolation transaction aborts. Post-fix the arbiter's RETURNING (the
    standing-claim fence's source of truth) shows the un-stamped row and
    the INSERT is skipped: the ledger stays empty, the re-pend stands
    (sweep parity - the sweep's fenced reclaim re-pends the same shape),
    and the lineage closes at the soak's invariant after the re-claim."""
    stack, deps = await _open_isolate_deps(pg_dsn)
    async with stack:
        schema = deps.settings.schema_name
        assert deps.settings.pg_dsn_direct is not None
        conn = await asyncpg.connect(deps.settings.pg_dsn_direct)
        try:
            worker_id = new_uuid()
            await _seed_worker(conn, schema, worker_id)
            job_id = await _seed_pending_job(conn, schema)

            # 1. The claim commits: running, attempt 1, stamped. Ledger empty.
            claimed = await _claim_via_real_dispatch_cte(conn, schema, worker_id)
            assert claimed["attempt"] == 1
            # 2. The reconcile refunds the lost claim: 1 -> 0, un-stamped,
            #    still running, still locked by this worker.
            await _age_started_at(conn, schema, job_id)
            refunded = await conn.fetch(
                _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema),
                worker_id,
                [],
                _LEASE,
            )
            assert [r["id"] for r in refunded] == [job_id]
            row = await _job_row(conn, schema, job_id)
            assert row["attempt"] == 0 and row["started_at"] is None

            # 3. The REAL isolate_self walks this refunded state.
            shutdown = asyncio.Event()
            await asyncio.wait_for(isolate_self(deps, worker_id, shutdown), timeout=30.0)

            # 4. THE LEDGER: no row at the refunded epoch - not a phantom
            #    'crashed', not a NotNullViolation aborting the transaction.
            row = await _job_row(conn, schema, job_id)
            assert row["status"] == "pending" and row["attempt"] == 0, (
                f"the isolate must re-pend the refunded row (sweep parity), got {row}"
            )
            assert await _attempt_rows(conn, schema, job_id) == [], (
                "the isolate must record NO attempt row for a refunded epoch: "
                "the un-stamped started_at is the refund's void marker"
            )
            # The reclaim EVENT still lands - audited either way.
            events = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
                "AND kind = 'state_change' AND detail->>'cause' = 'isolate_self'",
                job_id,
            )
            assert events == 1

            # 5. Closure: the re-claim visits the number (the refund's whole
            #    point) and the terminal write lands the ONE ledger row.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = '
                "clock_timestamp() - interval '1 second' WHERE id = $1",
                job_id,
            )
            reclaims = await _claim_via_real_dispatch_cte(conn, schema, worker_id)
            assert reclaims["attempt"] == 1
            job = await _job_row(conn, schema, job_id)
            applied = await _mark_succeeded_on_conn(
                conn,
                render(schema),
                JobId(job_id),
                worker_id,
                {"ok": True},
                attempt=1,
                claim_epoch=job["claim_epoch"],
            )
            assert applied is True

            # 6. THE SOAK'S EXACT INVARIANT.
            lineage = await conn.fetchrow(
                f"SELECT j.attempt, "
                f'(SELECT count(*)::int FROM "{schema}".job_attempts a '
                f"WHERE a.job_id = j.id) AS attempts "
                f'FROM "{schema}".jobs j WHERE j.id = $1',
                job_id,
            )
            assert lineage is not None
            assert lineage["attempts"] == lineage["attempt"], (
                f"job {job_id}: attempt counter {lineage['attempt']} vs "
                f"{lineage['attempts']} attempt rows"
            )
            assert [
                (a["attempt"], a["outcome"]) for a in await _attempt_rows(conn, schema, job_id)
            ] == [(1, "succeeded")]
        finally:
            await conn.close()


async def test_isolate_self_fences_the_refund_commit_inside_the_select_update_window(
    pg_dsn: str,
) -> None:
    """THE CI INTERLEAVING, frozen: the refund commits BETWEEN the
    isolate's SELECT and its arbiter UPDATE. The SELECT takes no row
    lock, so the reconcile's refund (the same worker's heartbeat pool,
    the shutdown drain's refund arm) serialises right there; the
    pre-fix arbiter - fenced on (id, running, holder), every conjunct of
    which the refund leaves TRUE - then won the transition on the
    de-charged row and the INSERT minted the SNAPSHOT's epoch (attempt 1,
    stamped) AFTER the refund rolled the counter to 0. The next claim
    re-mints the number and the lineage closes with more rows than the
    counter: the soak's red.

    Frozen with a held row lock: the holder's uncommitted FOR UPDATE
    parks the arbiter AFTER the snapshot is taken; the refund commits
    inside that window; the COMMIT releases the arbiter onto the REFUNDED
    row version. Post-fix the arbiter's RETURNING exposes the un-stamped
    row and no attempt row is recorded; a fence that read the SNAPSHOT
    (whose stamp is non-NULL here) could not see the refund and would
    still mint the phantom."""
    stack, deps = await _open_isolate_deps(pg_dsn)
    async with stack:
        schema = deps.settings.schema_name
        assert deps.settings.pg_dsn_direct is not None
        conn = await asyncpg.connect(deps.settings.pg_dsn_direct)
        holder = await asyncpg.connect(deps.settings.pg_dsn_direct)
        try:
            worker_id = new_uuid()
            await _seed_worker(conn, schema, worker_id)
            job_id = await _seed_pending_job(conn, schema)

            # 1. The claim: running, attempt 1, stamped, ledger empty.
            claimed = await _claim_via_real_dispatch_cte(conn, schema, worker_id)
            assert claimed["attempt"] == 1
            # The reconcile's grace is anchored to the PG clock.
            await _age_started_at(conn, schema, job_id)

            # 2. The snapshot, taken exactly as _inner takes it (the real
            #    rendered SELECT): attempt 1, stamp PRESENT - the stale view
            #    the pre-fix INSERT bound.
            snapshot = await conn.fetch(
                _SELECT_RUNNING_JOBS_SQL_TEMPLATE.format(schema=schema),
                worker_id,
                [],
            )
            assert [r["id"] for r in snapshot] == [job_id]
            assert snapshot[0]["attempt"] == 1
            assert snapshot[0]["started_at"] is not None, (
                "fixture broken: the snapshot must predate the refund"
            )

            # 3. Freeze the window: the holder parks the job row's lock
            #    uncommitted, the REAL isolate_self runs - its SELECT sees
            #    the stamped row, its arbiter PARKS on the holder's lock.
            await holder.execute("BEGIN")
            await holder.execute(f'SELECT id FROM "{schema}".jobs WHERE id = $1 FOR UPDATE', job_id)
            shutdown = asyncio.Event()
            isolate_task = asyncio.create_task(isolate_self(deps, worker_id, shutdown))
            try:
                await _wait_for_lock_waiter(holder, schema)

                # 4. THE REFUND, committed INSIDE the SELECT->UPDATE window:
                #    the counter rolls 1 -> 0 and the stamp is un-stamped
                #    while the isolate's snapshot already says attempt 1,
                #    stamped.
                refunded = await holder.fetch(
                    _RECONCILE_LOST_CLAIMS_SQL_TEMPLATE.format(schema=schema),
                    worker_id,
                    [],
                    _LEASE,
                )
                assert [r["id"] for r in refunded] == [job_id], (
                    "fixture broken: the reconcile must refund the lost claim"
                )
            finally:
                # 5. Release the arbiter onto the REFUNDED row version.
                await holder.execute("COMMIT")

            await asyncio.wait_for(isolate_task, timeout=30.0)

            # 6. THE FENCE: the arbiter won (the re-pend stands - sweep
            #    parity), but the attempt INSERT read the RETURNING - the
            #    refunded, un-stamped row - and recorded NOTHING. Pre-fix
            #    the phantom (job, 1, 'crashed') stands here: the snapshot's
            #    epoch, minted after the refund.
            row = await _job_row(conn, schema, job_id)
            assert row["status"] == "pending" and row["attempt"] == 0, (
                f"the arbiter must win the re-pend on the refunded row (sweep parity), got {row}"
            )
            assert await _attempt_rows(conn, schema, job_id) == [], (
                "the stale-snapshot INSERT must be fenced by the arbiter's "
                "RETURNING: the snapshot's epoch was already refunded, and a "
                "crashed row minted after the refund re-mints under the next "
                "claim - the soak's counter-vs-ledger red"
            )
            events = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1 '
                "AND kind = 'state_change' AND detail->>'cause' = 'isolate_self'",
                job_id,
            )
            assert events == 1, "the reclaim event lands either way - audited"

            # 7. Closure at the soak's exact invariant: the re-claim visits
            #    the refunded number, the terminal write lands the ONE row.
            await conn.execute(
                f'UPDATE "{schema}".jobs SET scheduled_at = '
                "clock_timestamp() - interval '1 second' WHERE id = $1",
                job_id,
            )
            reclaims = await _claim_via_real_dispatch_cte(conn, schema, worker_id)
            assert reclaims["attempt"] == 1, (
                "the refund exists so a never-executed claim spends nothing"
            )
            job = await _job_row(conn, schema, job_id)
            applied = await _mark_succeeded_on_conn(
                conn,
                render(schema),
                JobId(job_id),
                worker_id,
                {"ok": True},
                attempt=1,
                claim_epoch=job["claim_epoch"],
            )
            assert applied is True
            lineage = await conn.fetchrow(
                f"SELECT j.attempt, "
                f'(SELECT count(*)::int FROM "{schema}".job_attempts a '
                f"WHERE a.job_id = j.id) AS attempts "
                f'FROM "{schema}".jobs j WHERE j.id = $1',
                job_id,
            )
            assert lineage is not None
            assert lineage["attempts"] == lineage["attempt"], (
                f"job {job_id}: attempt counter {lineage['attempt']} vs "
                f"{lineage['attempts']} attempt rows - the refund and the "
                f"ledger writer did not move together"
            )
            assert [
                (a["attempt"], a["outcome"]) for a in await _attempt_rows(conn, schema, job_id)
            ] == [(1, "succeeded")]
        finally:
            await holder.close()
            await conn.close()

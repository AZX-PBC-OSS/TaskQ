# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""The bounded lease renewal: the deferral contract, pinned against real PG18.

SQL-audit F3: the heartbeat loop's gated jobs-lock renewal
(``UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE``) was O(the worker's running
fleet) per beat - the OR'd threshold predicate defeated every index, so
the statement visited every held row to decide that most rows need
nothing, and it rides the tick path. The fix restructures the gate as a
row-shortlist CTE whose arms are each index-bound (the expiry arm drives
``jobs_running_lock_expires_idx`` via a STABLE ``statement_timestamp()``
range bound - a VOLATILE ``clock_timestamp()`` bound cannot be an index
condition at all, the same trade backend/_sweeps.py's reclaim sweep
documented). These pins hold the two safety properties the bound risks:

* the non-reclaim invariant (the risk row): a job whose consumer is
  alive but whose renewal was DEFERRED by the gate must NOT be
  reclaimed - the sweep's lease arm only takes rows whose
  ``lock_expires_at`` has actually passed, and the gate's whole sizing
  contract (``_lease_renewal_threshold``) is that a deferred row's
  lease is still comfortably valid. Proven red-first: with the bound
  reverted to an unsafe one (a threshold of 0 - the degenerate
  "renew-nothing" gate), the same seed's lease lapses and the sweep
  DOES take the row, so these assertions fail on the unsafe shape.
* the starvation answer: a row the gate deferred past one beat MUST be
  picked up by a later beat - the first beat whose remaining lease is
  at or under the threshold - and the arithmetic guarantees that beat
  arrives at least one worst beat-gap before expiry (threshold = the
  enforced cascade floor ``max(interval, command_timeout) +
  (F+1) * (interval + command_timeout)``, lease 6s / interval 0.5s /
  F 3 / command timeout 0.1s gives 3.0s: a row deferred at remaining 6
  is renewed at remaining 3.0, four seconds before it would lapse).

A third pin holds the plan shape at depth: at 10,000 held rows in the
deferred regime (no row due) the statement's scans are ALL index-bound
- no Seq Scan over ``jobs`` - and the expiry arm's Index Cond is the
threshold range on ``jobs_running_lock_expires_idx``. Production tables
carry current statistics (autovacuum's analyze); the pin's explicit
``ANALYZE`` mirrors that, and the seed's own churn is what a live
fleet's stats look like.
"""

from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sql import build_heartbeat_sql
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.heartbeat import _lease_renewal_threshold

pytestmark = pytest.mark.integration

#: The pinned regime: a lease the gate can actually defer under (the
#: threshold must sit BELOW the lease for any row to ever be deferred;
#: at the 60s default lease the gate is inert - the floor 58s eats the
#: slack - so these pins use 6s, where the fast-suite arithmetic gives
#: threshold 3.0s and a two-beat defer-then-renew cycle).
_LEASE = timedelta(seconds=6.0)

#: The fast-suite tick settings (test_heartbeat_integration's constants):
#: the renewal threshold they derive is the floor
#: ``max(interval, command_timeout) + (F+1) * (interval +
#: command_timeout)`` = max(0.5, 0.1) + 4 * 0.6 = 2.9, raised by the
#: ``lock_lease / 2`` arm to exactly 3.0s.
_HEARTBEAT_INTERVAL = 0.5
_MAX_HEARTBEAT_FAILURES = 3
_COMMAND_TIMEOUT = 0.1
_THRESHOLD = _lease_renewal_threshold(
    lock_lease=_LEASE,
    heartbeat_interval=_HEARTBEAT_INTERVAL,
    max_heartbeat_failures=_MAX_HEARTBEAT_FAILURES,
    heartbeat_command_timeout=_COMMAND_TIMEOUT,
)

#: How much the aging step in the starvation pin removes between the two
#: beats: enough to carry the row from remaining 6s (deferred) to
#: remaining 2.5s - strictly under the 3.0s threshold, strictly above
#: zero (no lapse window).
_AGE_BETWEEN_BEATS = timedelta(seconds=3.5)


#: How much the aging steps in the walking pins remove per beat: one
#: tick's decay at the fast-suite interval (0.5s plus headroom), so the
#: walk reads as the real cadence compressed to test time.
_TICK_DECAY = timedelta(seconds=0.6)


async def _seed_worker_and_running_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    expires_in: timedelta,
    disowned_held: bool = False,
) -> tuple[UUID, UUID]:
    """One worker row + one running job, every stamp server-written.

    The stamps use ``clock_timestamp()`` (not Python's ``now``) so the
    gate's server-side comparison and the assertions' single-domain
    reads all judge the same clock domain - the same discipline
    test_heartbeat_integration's docstring states.
    """
    worker_id, job_id = new_uuid(), new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers '
        "(id, hostname, pid, queues) VALUES ($1, 'bound-host', 1, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, status, priority, attempt,
            scheduled_at, max_attempts, retry_kind, locked_by_worker,
            lock_expires_at, started_at, last_heartbeat_at
        ) VALUES (
            $1, 'bound_actor', 'default', '{{"v": 1}}'::jsonb, 'running', 0, 1,
            clock_timestamp(), 3, 'transient', $2,
            clock_timestamp() + $3::interval, clock_timestamp(), clock_timestamp()
        )""",
        job_id,
        worker_id,
        expires_in,
    )
    return worker_id, job_id


async def _read_job(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
) -> asyncpg.Record:
    """One statement reads the server clock and the row together."""
    row = await conn.fetchrow(
        f"SELECT statement_timestamp() AS now_ts, status, lock_expires_at, last_heartbeat_at "
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    assert row is not None, f"the seeded job {job_id} vanished from {schema}.jobs"
    return row


async def _renew(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> int:
    """Run one gated renewal beat exactly as the heartbeat loop binds it."""
    _liveness, jobs_sql, _slots = build_heartbeat_sql(schema, renewal_threshold=_THRESHOLD)
    tag = await conn.execute(
        jobs_sql,
        worker_id,
        _LEASE,
        [],  # nothing disowned here
        _THRESHOLD,
    )
    return int(tag.rsplit(" ", 1)[-1])


async def _reclaim_event_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    return await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events '
        "WHERE job_id = $1 AND kind = 'state_change' "
        "AND detail->>'reason' = 'lock_expired'",
        job_id,
    )


async def test_deferred_renewal_row_is_not_reclaimed(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The non-reclaim invariant: a deferral is not a reclaim.

    A running row whose consumer is alive (its beat runs every tick, it
    holds the row) whose renewal the gate DEFERS must survive the
    reclaim sweep for as long as the consumer keeps beating - across
    the row's WHOLE original lease lifetime and past it: the sweep's
    lease arm takes only rows whose ``lock_expires_at`` has passed, and
    the gate's sizing contract (``_lease_renewal_threshold``) is that
    the crossing beat renews the row a full cascade ahead of expiry.
    The pin walks the real cycle: age the row 0.6s (one tick's decay,
    server-side, single clock domain), run the gated beat, run the real
    Sweep 1, demand the row back running - repeated past the point
    where the ORIGINAL lease deadline has passed, so the row visibly
    outlives it on renewals alone.

    Red-first provenance: with the bound reverted to an unsafe one
    (threshold 0 - the degenerate "renew-nothing" gate the sizing
    exists to prevent), the same walk lets the lease lapse at the
    original deadline and Sweep 1 takes the row - every assertion here
    reds on the unsafe shape (captured in the fix's evidence notes).
    """
    schema = module_pg_schema.schema_name
    assert timedelta(seconds=3.0) == _THRESHOLD, (
        f"the pinned regime's threshold moved to {_THRESHOLD}: the seeds' "
        "defer/renew arithmetic below is calibrated to 3.0s - re-derive "
        "the aging interval before trusting the pins"
    )
    worker_id, job_id = await _seed_worker_and_running_job(clean_pg_conn, schema, expires_in=_LEASE)

    # Walk the row past its ORIGINAL lease deadline: 12 steps of one
    # tick's decay (0.6s) age it 7.2s against a lease of 6.0s. A
    # consumer alive this whole time holds a running job the whole
    # time - any reclaim inside the walk is the spurious-reclaim bug.
    aged = timedelta(seconds=0)
    steps = 0
    while aged <= _LEASE:
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = lock_expires_at - $2::interval '
            "WHERE id = $1",
            job_id,
            _TICK_DECAY,
        )
        aged += _TICK_DECAY
        steps += 1
        await _renew(clean_pg_conn, schema, worker_id)
        row = await _read_job(clean_pg_conn, schema, job_id)
        assert row["status"] == "running", (
            f"step {steps} (aged {aged}): the consumer's beats ran every tick "
            f"but the row left running state - the deferred renewal lapsed a "
            "lease the threshold arithmetic promises is valid (spurious reclaim)"
        )
        reclaimed = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn, timedelta(seconds=0), timedelta(seconds=0), schema=schema
        )
        assert reclaimed == 0, (
            f"step {steps} (aged {aged}): the reclaim sweep took a row whose "
            "consumer is alive and whose renewal was deferred - the gate's "
            "bound is smaller than the cascade floor it must size against"
        )
        assert await _reclaim_event_count(clean_pg_conn, schema, job_id) == 0, (
            f"step {steps} (aged {aged}): a reclaim event was written for a "
            "row whose consumer never stopped beating"
        )

    # The row outlived the original deadline (6.0s aged 7.2s) on the
    # gate's renewals alone, still running, lease still above zero.
    row = await _read_job(clean_pg_conn, schema, job_id)
    assert row["status"] == "running"
    assert row["lock_expires_at"] > row["now_ts"], (
        "the row walked past its original lease deadline but ended with a "
        "lapsed lease - the pickup beat did not re-stamp it"
    )


async def test_deferred_row_is_picked_up_by_a_later_beat(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The starvation answer: a deferred row is renewed at the crossing beat.

    The gate defers a row only while its remaining lease is above the
    threshold, so the row's next renewal is the first beat whose
    remaining is at or under it - and the threshold's sizing
    (``max(interval, command_timeout) + (F+1) * (interval +
    command_timeout)`` at the pinned settings, raised by the
    ``lock_lease / 2`` arm) puts that beat a full cascade ahead of
    expiry. The pin walks one real defer-then-pickup cycle on the
    server's clock: beat 1 defers at remaining 6.0s, the row ages
    3.5s (server-side, single clock domain) to remaining 2.5s -
    under the 3.0s threshold, above zero - and beat 2 MUST renew it.
    """
    schema = module_pg_schema.schema_name
    worker_id, job_id = await _seed_worker_and_running_job(clean_pg_conn, schema, expires_in=_LEASE)

    assert await _renew(clean_pg_conn, schema, worker_id) == 0, (
        "beat 1 must defer the fresh-lease row (remaining above the threshold)"
    )
    deferred = await _read_job(clean_pg_conn, schema, job_id)

    # Age the row between beats, server-side: the deferred regime's
    # normal decay, compressed to test time. The remaining lease stays
    # strictly positive - this is NOT a lapse, the gate is never
    # surprised by an expired row.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET lock_expires_at = lock_expires_at - $2::interval '
        "WHERE id = $1",
        job_id,
        _AGE_BETWEEN_BEATS,
    )
    aged = await _read_job(clean_pg_conn, schema, job_id)
    remaining = aged["lock_expires_at"] - aged["now_ts"]
    assert timedelta(seconds=0) < remaining <= _THRESHOLD, (
        f"the aged row's remaining lease is {remaining}: the pickup beat's "
        f"precondition is strictly between zero and the {_THRESHOLD} threshold"
    )

    assert await _renew(clean_pg_conn, schema, worker_id) == 1, (
        "the crossing beat must renew the deferred row: a row whose remaining "
        f"lease ({remaining}) sits under the {_THRESHOLD} threshold is "
        "due - a gate that keeps deferring it starves the row into a lapse"
    )
    renewed = await _read_job(clean_pg_conn, schema, job_id)
    new_remaining = renewed["lock_expires_at"] - renewed["now_ts"]
    assert new_remaining > _THRESHOLD, (
        f"the renewed row's remaining lease is {new_remaining}: the beat must "
        "re-stamp the full lease, pushing the row back above the threshold"
    )
    assert renewed["lock_expires_at"] > deferred["lock_expires_at"] - _AGE_BETWEEN_BEATS
    assert renewed["status"] == "running"
    assert await _reclaim_event_count(clean_pg_conn, schema, job_id) == 0, (
        "a defer-then-pickup cycle must never produce a reclaim event"
    )


async def test_renewal_plan_is_index_bound_at_depth(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """At 10k held rows, the deferred beat's plan has no whole-fleet scan.

    SQL-audit F3's bound, pinned at the plan level (wall time is
    load-fragile; the plan SHAPE is the contract): with 10,000 running
    rows held by one worker, every lease fresh (the deferred regime -
    nothing due), every scan node in the gated renewal's plan is an
    index scan, and the expiry arm's Index Cond is the threshold range
    on ``jobs_running_lock_expires_idx`` - the index bound the OR'd
    spelling could never produce (its plan was a Seq Scan over the
    whole jobs table, measured on this same seed).
    """
    schema = module_pg_schema.schema_name
    worker_id, _job_id = await _seed_worker_and_running_job(
        clean_pg_conn, schema, expires_in=_LEASE
    )
    # 9,999 more rows for the same worker: the depth regime. generate_series
    # in one statement; every stamp server-written, then ANALYZE - the
    # statistics state a production table's autovacuum maintains.
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, status, priority, attempt,
            scheduled_at, max_attempts, retry_kind, locked_by_worker,
            lock_expires_at, started_at, last_heartbeat_at
        )
        SELECT gen_random_uuid(), 'bound_actor', 'default', '{{"v": 1}}'::jsonb,
               'running', 0, 1, clock_timestamp(), 3, 'transient', $1,
               clock_timestamp() + $2::interval, clock_timestamp(), clock_timestamp()
        FROM generate_series(1, 9999)""",
        worker_id,
        _LEASE,
    )
    await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')

    _liveness, jobs_sql, _slots = build_heartbeat_sql(schema, renewal_threshold=_THRESHOLD)
    stmt = await clean_pg_conn.prepare(jobs_sql)
    # The deferred regime's parameters: every row's lease is fresh
    # (stamped _LEASE ago), so a threshold of half the lease makes
    # nothing due - the beat must decide that from an index bound, not
    # by visiting the fleet.
    plan = await stmt.explain(
        worker_id,
        _LEASE,
        [],
        _LEASE / 2,
        analyze=True,
    )

    nodes: list[dict[str, Any]] = []

    def _walk(node: dict[str, Any]) -> None:
        nodes.append(node)
        for child in node.get("Plans", []):
            _walk(child)

    for fragment in plan:
        _walk(fragment.get("Plan", fragment))

    seq_scans = [n for n in nodes if n.get("Node Type") == "Seq Scan"]
    assert not seq_scans, (
        "the deferred renewal's plan contains a Seq Scan over jobs at 10k "
        "held rows - the whole-fleet visit F3 exists to remove is back: "
        f"{[n.get('Relation Name') for n in seq_scans]}"
    )
    expiry_arms = [
        n
        for n in nodes
        if n.get("Index Name") == "jobs_running_lock_expires_idx"
        and "statement_timestamp()" in (n.get("Index Cond") or "")
    ]
    assert expiry_arms, (
        "the expiry arm's threshold range bound is not an Index Cond on "
        "jobs_running_lock_expires_idx - the statement lost the index bound "
        "the deferral contract rides"
    )
    modify = [n for n in nodes if n.get("Node Type") == "ModifyTable"]
    assert modify and modify[0].get("Actual Rows") == 0, (
        "a fully-deferred beat rewrote rows - the gate deferred nothing, "
        "which means the threshold bound is not what the plan is using"
    )

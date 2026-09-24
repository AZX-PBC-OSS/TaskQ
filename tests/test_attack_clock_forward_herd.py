"""The clock-forward thundering herd attack: every deadline lapses at once.

The fault: the clock jumps FORWARD (a VM resumed from a paused snapshot, a
bad NTP step). Two hours pass in an instant. Every lease lapses
simultaneously, every leader lease dies, every TTL row becomes prunable:
the thundering herd. The question for each mechanism is not "does it
recover" but "what does one recovery pass cost" - the herd must drain in
BOUNDED committed passes, never one unbounded transaction, and the audit
ledger must conserve through the flood (one attempt row per lapsed job,
no double reclaims, no lost re-pends).

The jump is seeded, not simulated: every row is stamped as it was stamped
before the jump (a 30 s lease written two hours ago reads as expired by
~1 h 59 m after it), and every server-side predicate in the sweeps reads
the database's own clock, so a cohort stamped pre-jump and a live clock
reproduce the post-jump state exactly - the same state a real +2 h step
presents to the sweep. Nothing in the herd path reads the Python clock
(pinned elsewhere: tests/test_clock_domain_isolation.py); the two-clock
split is why a seeded cohort IS the jump for these statements.

What each test pins, and the defect class each would catch:

1. ``test_reclaim_herd_batches_stay_bounded_and_ledger_conserved`` - the
   herd's SHAPE: 10k running rows lapse in one tick; each reclaim pass
   must stay inside the 2 x batch_size arm bound (the sweep's LIMIT-ed
   snaps), every pass commits (a stopped drain is a pause, not a
   rollback), the ledger conserves (exactly one crashed attempt row and
   one reclaim event per lapsed job, no double reclaim on a re-drain),
   and the re-pended cohort's wake instants are SPREAD (the per-row
   jitter band), not one synchronised instant.
2. ``test_reclaim_wake_is_one_notify_per_batch`` - the reclaim STORM's
   wake shape: one pg_notify per committed batch, never one per row. 200
   re-pends must wake the fleet once, not 200 times.
3. ``test_mass_election_single_winner_fence_holds`` - the LEADER mass
   election: with the lease row lapsed by the jump, N workers electing
   concurrently must produce exactly ONE winner, and an immediate
   re-election by a loser must keep losing against the fresh lease (the
   losers' retries ride the heartbeat tick; no takeover ping-pong).
4. ``test_deadline_herd_batches_stay_bounded_and_ledger_conserved`` -
   sweep 2's herd: 10k pending rows' schedule_to_close lapses at once;
   bounded passes, one failed attempt row each, no double sweep.
5. ``test_prune_herd_batches_stay_bounded_and_archive_conserves`` - the
   retention prune: 100k terminal rows pass retention at once; every
   prune batch stays inside its LIMIT (the 363 lesson: the lock window is
   the batch, never the population), the archive conserves (every
   deleted live row is in jobs_archive, every archived attempt in
   job_attempts_archive), and a re-run deletes nothing.
6. ``test_event_ttl_herd_batch_bounded_and_outbox_carveout_holds`` - the
   TTL'd event rows: 100k events become prunable in one instant; one
   call deletes at most its batch (the tick's cost is independent of the
   backlog) and the crash-reclaim outbox slice is carved out of the herd
   (an unconsumed reclaim event survives the ordinary retention age that
   just lapsed).

Every bound asserted here is the mechanism's own (the SQL LIMIT, the
atomic upsert fence, the batched INSERT), not a test-side throttle: a
mutation that unbinds any of them must turn the relevant pin red.
"""

from __future__ import annotations

# ruff: noqa: S608  # Why: every interpolated identifier is the module fixture's schema name, fixture-derived, not user input; all values are $-bound.
import asyncio
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._sweeps import sweep_expired_locks
from taskq.backend.postgres import PostgresBackend
from taskq.constants import wake_channel
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._leader_shared import prune_terminal_jobs
from taskq.worker.leader import build_leader_lease_sql

pytestmark = pytest.mark.integration

# The jump: +2 hours. Every pre-jump stamp in this module is written as
# clock_timestamp() - interval '2 hours' (+ the stamp's own validity).
_JUMP = timedelta(hours=2)
_LEASE = timedelta(seconds=30)

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)

# The sweep loop's production bounds (settings defaults): 100 rows per
# writer batch, 8 batches per tick. The drain helpers below reuse them so
# the measured drain is the drain the leader actually runs.
_BATCH = 100
_STATEMENT_TIMEOUT_MS = 1750

# The prune family's defaults.
_PRUNE_BATCH = 10_000
_PRUNE_TIMEOUT_MS = 4000

# Cohort sizes.
_HERD_BUDGET = 7_000  # re-pend arm
_HERD_EXHAUSTED = 3_000  # crashed arm
_HERD_CANCEL = 100  # cancel arm
_HERD_TOTAL = _HERD_BUDGET + _HERD_EXHAUSTED + _HERD_CANCEL
_DEADLINE_HERD = 10_000
_PRUNE_HERD = 100_000
_EVENTS_HERD = 100_000
_EVENTS_OUTBOX = 200


async def _reset(module: ModulePgSchema) -> asyncpg.Connection:
    """A fresh connection on a truncated schema (per-test blank slate)."""
    from taskq.testing.pg import reset_schema

    conn = await asyncpg.connect(module.pg_dsn)
    await reset_schema(conn, module.schema_name, actors=[])
    return conn


# ── Seeding: the herd, stamped the way the instant BEFORE the jump stamped it


async def _seed_herd_worker(conn: asyncpg.Connection, schema: str) -> UUID:
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # Why: schema is the module fixture's validated identifier, never user input.
        "VALUES ($1, 'herd-host', 4242, ARRAY['default'])",
        worker_id,
    )
    return worker_id


_HERD_INSERT_SQL = """\
INSERT INTO "{schema}".jobs
    (id, actor, queue, payload, status, attempt, max_attempts, retry_kind,
     scheduled_at, started_at, last_heartbeat_at, locked_by_worker,
     lock_expires_at, cancel_phase, cancel_requested_at,
     retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter)
SELECT substr(md5('{tag}' || n::text), 1, 32)::uuid,
       'herd_actor', 'default', '{{}}'::jsonb, 'running', 1, {max_attempts},
       'transient',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours',
       $1,
       -- The lease the worker stamped 2 h ago, valid for 30 s: lapsed by
       -- ~1 h 59 m 30 s of the jump.
       clock_timestamp() - interval '2 hours' + interval '30 seconds',
       {cancel_phase}, {cancel_requested_at},
       5.0, 10.0, 'exponential', 0.5
FROM generate_series(1, {n}) AS n"""


async def _seed_running_herd(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    """10,100 running rows, every stamp written the instant BEFORE the jump."""
    async with conn.transaction():
        # Budget remaining -> the re-pend arm.
        await conn.execute(
            _HERD_INSERT_SQL.format(
                schema=schema,
                tag="budget-",
                max_attempts=3,
                cancel_phase=0,
                cancel_requested_at="NULL",
                n=_HERD_BUDGET,
            ),
            worker_id,
        )
        # Attempts exhausted -> the crashed arm.
        await conn.execute(
            _HERD_INSERT_SQL.format(
                schema=schema,
                tag="exhausted-",
                max_attempts=1,
                cancel_phase=0,
                cancel_requested_at="NULL",
                n=_HERD_EXHAUSTED,
            ),
            worker_id,
        )
        # Cancel in flight -> the cancel arm (terminalised, never re-pended).
        await conn.execute(
            _HERD_INSERT_SQL.format(
                schema=schema,
                tag="cancel-",
                max_attempts=3,
                cancel_phase=1,
                cancel_requested_at="clock_timestamp() - interval '2 hours'",
                n=_HERD_CANCEL,
            ),
            worker_id,
        )


_DEADLINE_HERD_SQL = """\
INSERT INTO "{schema}".jobs
    (id, actor, queue, payload, status, max_attempts, retry_kind,
     scheduled_at, schedule_to_close)
SELECT substr(md5('deadline-' || n::text), 1, 32)::uuid,
       'herd_actor', 'default', '{{}}'::jsonb, 'pending', 3, 'transient',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours' + interval '5 minutes'
FROM generate_series(1, $1) AS n"""

_PRUNE_HERD_SQL = """\
WITH seed AS (
    SELECT substr(md5('prune-' || n::text), 1, 32)::uuid AS id,
           clock_timestamp() - interval '2 hours' AS finished
    FROM generate_series(1, $1) AS n
)
INSERT INTO "{schema}".jobs
    (id, actor, queue, payload, status, max_attempts, retry_kind,
     scheduled_at, finished_at, error_class)
SELECT id, 'herd_actor', 'default', '{{}}'::jsonb, 'succeeded', 3, 'transient',
       finished, finished, NULL
FROM seed"""

_PRUNE_ATTEMPTS_SQL = """\
INSERT INTO "{schema}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome, error_class,
     error_message, error_traceback, duration_ms, worker_id, metadata)
SELECT j.id, 1, j.finished_at - interval '1 minute', j.finished_at,
       'succeeded', NULL, NULL, NULL, 60000, NULL, '{{}}'::jsonb
FROM "{schema}".jobs j
WHERE j.actor = 'herd_actor'"""

# The events herd needs parent jobs (job_events.job_id is an FK): one
# minimal terminal job per event, created first.
_EVENTS_HERD_SQL = """\
INSERT INTO "{schema}".jobs
    (id, actor, queue, payload, status, max_attempts, retry_kind,
     scheduled_at, finished_at)
SELECT substr(md5('evt-' || n::text), 1, 32)::uuid,
       'herd_actor', 'default', '{{}}'::jsonb, 'succeeded', 3, 'transient',
       clock_timestamp() - interval '2 hours',
       clock_timestamp() - interval '2 hours'
FROM generate_series(1, $1) AS n"""

_EVENTS_HERD_EVENTS_SQL = """\
INSERT INTO "{schema}".job_events (job_id, kind, detail, occurred_at)
SELECT id, 'progress', '{{}}'::jsonb, clock_timestamp() - interval '2 hours'
FROM "{schema}".jobs WHERE actor = 'herd_actor'"""

_EVENTS_OUTBOX_SQL = """\
INSERT INTO "{schema}".job_events (job_id, kind, detail, occurred_at)
SELECT id, 'state_change',
       '{{"reason": "lock_expired", "cause": "lock_expired"}}'::jsonb,
       clock_timestamp() - interval '2 hours'
FROM "{schema}".jobs WHERE actor = 'herd_actor' LIMIT $1"""


# ── The leader's drain shape, instrumented ───────────────────────────────


class _PassLog:
    """One entry per committed sweep pass: rows transitioned, wall cost."""

    def __init__(self) -> None:
        self.rows: list[int] = []
        self.elapsed_ms: list[float] = []

    @property
    def total(self) -> int:
        return sum(self.rows)

    @property
    def worst_ms(self) -> float:
        return max(self.elapsed_ms, default=0.0)

    @property
    def worst_rows(self) -> int:
        return max(self.rows, default=0)


async def _drain_reclaim_herd(
    conn: asyncpg.Connection, schema: str, *, max_passes: int = 1_000
) -> _PassLog:
    """Drain the reclaim herd one committed batch per pass, like the leader
    loop's ``_drain_bounded`` does (minus its per-tick cap: this helper
    measures the FULL drain, pass by pass)."""
    log = _PassLog()
    for _ in range(max_passes):
        start = asyncio.get_running_loop().time()
        rows = await sweep_expired_locks(
            conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
            statement_timeout_ms=_STATEMENT_TIMEOUT_MS,
        )
        log.rows.append(rows)
        log.elapsed_ms.append((asyncio.get_running_loop().time() - start) * 1000.0)
        if rows == 0:
            break
    return log


# ── 1. The herd's shape: bounded passes, conserved ledger, spread re-pends


async def test_reclaim_herd_batches_stay_bounded_and_ledger_conserved(
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    try:
        worker_id = await _seed_herd_worker(conn, schema)
        await _seed_running_herd(conn, schema, worker_id)

        eligible = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'running' "
            "AND lock_expires_at < statement_timestamp()"
        )
        assert eligible == _HERD_TOTAL

        log = await _drain_reclaim_herd(conn, schema)

        # SHAPE: every pass inside the documented arm bound (each arm's
        # LIMIT-ed snap; only the lease arm is populated here). One pass
        # transitioning the whole 10k would be the unbounded-work defect.
        assert log.total == _HERD_TOTAL
        assert log.worst_rows <= 2 * _BATCH
        assert log.rows[-1] == 0  # the drain terminated on a clean tick
        # Each pass is a short transaction, server-side bounded: a pass
        # that ran past its statement_timeout would have been cancelled,
        # not slow. Wall bound = the server bound plus round-trip slack.
        assert log.worst_ms < 3_000.0

        status_rows = await conn.fetch(
            f'SELECT status::text AS s, count(*) AS c FROM "{schema}".jobs GROUP BY status'
        )
        statuses = {str(r["s"]): int(r["c"]) for r in status_rows}
        # Conservation: every lapsed row accounted for, exactly once,
        # under the arm the row's own state selected.
        assert statuses.get("pending", 0) == _HERD_BUDGET
        assert statuses.get("crashed", 0) == _HERD_EXHAUSTED
        assert statuses.get("cancelled", 0) == _HERD_CANCEL
        assert sum(statuses.values()) == _HERD_TOTAL

        # The attempt ledger: ONE crashed attempt row per lapsed job, no
        # duplicates (the batched INSERT's ON CONFLICT yields to nothing
        # here; a double reclaim would surface as a second write or a
        # rollback), every row naming the deadline that fired.
        attempt_rows = await conn.fetch(
            f'SELECT outcome, count(*) AS c FROM "{schema}".job_attempts GROUP BY outcome'
        )
        attempts = {str(r["outcome"]): int(r["c"]) for r in attempt_rows}
        assert attempts.get("crashed", 0) == _HERD_TOTAL
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts') == _HERD_TOTAL
        dupes = await conn.fetchval(
            "SELECT count(*) FROM (SELECT job_id, attempt, count(*) AS c "
            f'FROM "{schema}".job_attempts GROUP BY 1, 2 HAVING count(*) > 1) d'
        )
        assert dupes == 0

        # The reclaim events: one per job, all on the crash-reclaim
        # outbox channel, cause naming which deadline fired.
        events = await conn.fetch(
            "SELECT detail->>'reason' AS reason, detail->>'cause' AS cause, count(*) AS c "
            f'FROM "{schema}".job_events GROUP BY 1, 2'
        )
        assert sum(int(e["c"]) for e in events) == _HERD_TOTAL
        assert all(e["reason"] == "lock_expired" for e in events)
        assert all(e["cause"] == "lock_expired" for e in events)

        # No double reclaim: a full second drain reclaims nothing.
        assert (await _drain_reclaim_herd(conn, schema)).total == 0
        # And the terminal/re-pended rows carry no residual lock state.
        residual = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE locked_by_worker IS NOT NULL OR lock_expires_at IS NOT NULL"
        )
        assert residual == 0

        # HERD DAMPING: the re-pended cohort's wake instants are spread
        # across the jitter band [raw*(1-j), raw*(1+j)] = [2.5 s, 7.5 s]
        # for this seed's curve, not one synchronised instant (a flat
        # delay would land the whole cohort back on the fleet at once).
        # Measured spread must cover most of the band; distinct instants
        # must be ~every row.
        spread = await conn.fetchrow(
            "SELECT count(*) AS n, count(DISTINCT scheduled_at) AS distinct_n, "
            "EXTRACT(EPOCH FROM max(scheduled_at) - min(scheduled_at)) AS spread_s "
            f"FROM \"{schema}\".jobs WHERE status = 'pending'"
        )
        assert spread is not None
        assert spread["n"] == _HERD_BUDGET
        assert spread["distinct_n"] >= _HERD_BUDGET - 10
        assert float(spread["spread_s"]) >= 2.0
    finally:
        await conn.close()


# ── 2. The reclaim storm's wake shape: one NOTIFY per batch, not per row


async def test_reclaim_wake_is_one_notify_per_batch(
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    listener = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        worker_id = await _seed_herd_worker(conn, schema)
        # One full batch of lapsed rows (the cohort a single reclaim pass
        # transitions and wakes for).
        await conn.execute(
            _HERD_INSERT_SQL.format(
                schema=schema,
                tag="notify-",
                max_attempts=3,
                cancel_phase=0,
                cancel_requested_at="NULL",
                n=_BATCH,
            ),
            worker_id,
        )

        woken: asyncio.Queue[str] = asyncio.Queue()
        channel = wake_channel(schema)

        def _on_wake(
            connection: object, pid: int, channel: str, payload: str
        ) -> None:  # Why: asyncpg's listener callback signature is positional.
            woken.put_nowait(payload)

        await listener.add_listener(channel, _on_wake)

        rows = await sweep_expired_locks(
            conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
            batch_size=_BATCH,
            statement_timeout_ms=_STATEMENT_TIMEOUT_MS,
        )
        assert rows == _BATCH

        # Exactly one wake for the whole batch. A per-row NOTIFY would put
        # 100 payloads on this queue; the batch's ONE pg_notify puts one.
        first = await asyncio.wait_for(woken.get(), timeout=2.0)
        assert first == ""
        await asyncio.sleep(0.3)
        assert woken.empty()
    finally:
        await listener.close()
        await conn.close()


# ── 3. The leader mass election: the fence holds, losers do not take over


async def test_mass_election_single_winner_fence_holds(
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    try:
        elect_sql, _renew_sql, _resign_sql = build_leader_lease_sql(schema)

        n = 8
        worker_ids = [new_uuid() for _ in range(n)]
        for wid in worker_ids:
            await conn.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, 'elect-host', 1, ARRAY['default'])",
                wid,
            )
        conns = [await asyncpg.connect(module_pg_schema.pg_dsn) for _ in range(n)]

        async def _elect(worker_id: UUID, db: asyncpg.Connection) -> UUID | None:
            elected = await db.fetchval(elect_sql, worker_id, 30.0, 10.0)
            return worker_id if elected is not None else None

        try:
            # Round 1, from the void: N pods, no incumbent.
            winners = [
                w
                for w in await asyncio.gather(
                    *(_elect(w, c) for w, c in zip(worker_ids, conns, strict=True))
                )
                if w is not None
            ]
            assert len(winners) == 1

            holder = await conn.fetchrow(
                f'SELECT worker_id, worker_id = ANY($1::uuid[]) AS known FROM "{schema}".maintenance_leader',
                worker_ids,
            )
            assert holder is not None

            # An immediate loser re-run must keep losing: the fresh lease
            # fences it out, the loser's next try rides the heartbeat
            # tick, no spin, no takeover.
            loser = next(w for w in worker_ids if w != holder["worker_id"])
            loser_conn = next(c for c, w in zip(conns, worker_ids, strict=True) if w == loser)
            assert await _elect(loser, loser_conn) is None

            # Round 2, the jump: the lease row lapses (expires_at two
            # hours stale). All N elect at once.
            await conn.execute(
                f'UPDATE "{schema}".maintenance_leader SET '
                "expires_at = clock_timestamp() - interval '2 hours', "
                "last_seen_at = clock_timestamp() - interval '2 hours'"
            )
            winners2 = [
                w
                for w in await asyncio.gather(
                    *(_elect(w, c) for w, c in zip(worker_ids, conns, strict=True))
                )
                if w is not None
            ]
            assert len(winners2) == 1
            row = await conn.fetchrow(
                f'SELECT worker_id, EXTRACT(EPOCH FROM expires_at - clock_timestamp()) AS fresh FROM "{schema}".maintenance_leader'
            )
            assert row is not None
            assert row["worker_id"] == winners2[0]
            assert float(row["fresh"]) > 0.0  # the winner renewed the row
        finally:
            for c in conns:
                await c.close()
    finally:
        await conn.close()


# ── 4. Sweep 2's herd: the lapsed schedule_to_close cohort


async def test_deadline_herd_batches_stay_bounded_and_ledger_conserved(
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    try:
        await conn.execute(_DEADLINE_HERD_SQL.format(schema=schema), _DEADLINE_HERD)

        log = _PassLog()
        for _ in range(1_000):
            start = asyncio.get_running_loop().time()
            rows = await PostgresBackend.sweep_deadline_exceeded(
                conn,
                schema=schema,
                batch_size=_BATCH,
                statement_timeout_ms=_STATEMENT_TIMEOUT_MS,
            )
            log.rows.append(rows)
            log.elapsed_ms.append((asyncio.get_running_loop().time() - start) * 1000.0)
            if rows == 0:
                break

        assert log.total == _DEADLINE_HERD
        assert log.worst_rows <= _BATCH  # one arm, one LIMIT
        assert log.worst_ms < 3_000.0

        failed = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'failed' "
            "AND error_class = 'DeadlineExceeded'"
        )
        assert failed == _DEADLINE_HERD
        assert (
            await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts') == _DEADLINE_HERD
        )
        assert (
            await conn.fetchval(
                f"SELECT count(*) FROM \"{schema}\".job_events WHERE kind = 'state_change'"
            )
            == _DEADLINE_HERD
        )
        # No double sweep.
        rows = await PostgresBackend.sweep_deadline_exceeded(conn, schema=schema, batch_size=_BATCH)
        assert rows == 0
    finally:
        await conn.close()


# ── 5. The retention prune herd: 100k rows pass retention at once


async def test_prune_herd_batches_stay_bounded_and_archive_conserves(
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import taskq.worker._leader_shared as leader_shared

    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    try:
        await conn.execute(_PRUNE_HERD_SQL.format(schema=schema), _PRUNE_HERD)
        await conn.execute(_PRUNE_ATTEMPTS_SQL.format(schema=schema))

        batch_sizes: list[int] = []
        batch_rows: list[int] = []
        real_batch = leader_shared._run_prune_archive_batch

        async def _measured_batch(*args: object, **kwargs: object) -> object:
            size = kwargs["size"]
            assert isinstance(size, int)
            batch_sizes.append(size)
            rows = await real_batch(*args, **kwargs)  # type: ignore[arg-type]
            # The write statement returns deleted groups (actor, status,
            # cnt); the batch's moved-row total is the cnt sum.
            batch_rows.append(sum(int(r["cnt"]) for r in rows))  # type: ignore[union-attr,index-any-item]
            return rows

        monkeypatch.setattr(leader_shared, "_run_prune_archive_batch", _measured_batch)

        result = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(hours=1)},
            archive_retention=timedelta(days=30),
            batch_size=_PRUNE_BATCH,
            schema=schema,
            statement_timeout_ms=_PRUNE_TIMEOUT_MS,
        )

        # SHAPE: 100k rows drained in LIMIT-bounded batches (10 productive
        # batches at the default prune batch), never one population-wide
        # write - the 363 lesson: the lock window is the batch. (The other
        # terminal statuses' empty probes are recorded too; only batches
        # that moved rows count toward the drain's shape.)
        assert result.total_deleted == _PRUNE_HERD
        assert result.archived == _PRUNE_HERD
        assert batch_sizes and all(s <= _PRUNE_BATCH for s in batch_sizes)
        assert sum(batch_rows) == _PRUNE_HERD
        assert len([r for r in batch_rows if r > 0]) == _PRUNE_HERD // _PRUNE_BATCH

        # Conservation: every deleted live row is in the archive, every
        # archived attempt too, and the two sets are disjoint.
        live = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'succeeded'"
        )
        assert live == 0
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive') == _PRUNE_HERD
        assert (
            await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts_archive')
            == _PRUNE_HERD
        )
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts') == 0
        overlap = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs_archive a JOIN "{schema}".jobs j ON j.id = a.id'
        )
        assert overlap == 0

        # A re-run deletes nothing (no double prune, no re-archive).
        result2 = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(hours=1)},
            archive_retention=timedelta(days=30),
            batch_size=_PRUNE_BATCH,
            schema=schema,
            statement_timeout_ms=_PRUNE_TIMEOUT_MS,
        )
        assert result2.total_deleted == 0
    finally:
        await conn.close()


# ── 6. The TTL'd event rows: one bounded batch per tick, outbox spared


async def test_event_ttl_herd_batch_bounded_and_outbox_carveout_holds(
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    conn = await _reset(module_pg_schema)
    try:
        await conn.execute(_EVENTS_HERD_SQL.format(schema=schema), _EVENTS_HERD)
        await conn.execute(_EVENTS_HERD_EVENTS_SQL.format(schema=schema))
        await conn.execute(_EVENTS_OUTBOX_SQL.format(schema=schema), _EVENTS_OUTBOX)

        retention = timedelta(hours=1)
        batch = 10_000

        # ONE tick: at most one batch. The herd's first pass must not
        # become a 100k-row DELETE in one transaction.
        first = await PostgresBackend.sweep_expired_events(
            conn, schema=schema, retention=retention, batch_size=batch
        )
        assert first == batch

        # Drain the rest; every pass inside the batch bound.
        total = first
        while True:
            rows = await PostgresBackend.sweep_expired_events(
                conn, schema=schema, retention=retention, batch_size=batch
            )
            assert rows <= batch
            if rows == 0:
                break
            total += rows

        assert total == _EVENTS_HERD

        # The carve-out: the unconsumed crash-reclaim outbox slice
        # outlives the ordinary retention age that just lapsed (the
        # trailing-watermark consumer can still reach it).
        outbox_left = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events '
            "WHERE kind = 'state_change' AND detail->>'reason' = 'lock_expired'"
        )
        assert outbox_left == _EVENTS_OUTBOX
    finally:
        await conn.close()

"""Sweep 1 (expired locks → pending/cancelled/crashed) must be bounded.

``sweep_expired_locks`` (``src/taskq/backend/_sweeps.py``) reclaims at
most ``batch_size`` expired-lock running jobs per call, in one short
transaction whose writes are batched (a driving LIMIT-ed UPDATE, one
batched ``job_attempts`` INSERT, one batched ``job_events`` INSERT, one
``pg_notify``), and applies a server-side ``statement_timeout`` to the
whole batch.  Repeated calls drain the eligible backlog one committed
batch at a time.

Why boundedness is a correctness property, not merely a speed one
----------------------------------------------------------------
``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`` is 2 seconds, and its
docstring states the trailing-watermark guarantee of ``poll_reclaim_events``
holds only while "no ``job_events`` writer takes longer than this margin
between its INSERT and its COMMIT", naming "an abnormally large batch inserted
in one transaction" as a known way to violate it.  The consequence it names is
a **silently missed event**: a lower-``id`` ``job_events`` row commits after
the consumer's cursor has already advanced past its position, with no error
raised anywhere.  The crash backlog the sweep eats is itself unbounded (every
job held by a fleet-wide crash becomes eligible at once), so an unbounded
sweep is precisely the writer whose events ``poll_reclaim_events`` exists to
deliver — the miss lands on reclaim notifications themselves.

Why statements and not seconds
-------------------------------
The wall-clock threshold is RTT-dependent and therefore environment-dependent
and flaky.  The round-trip count is not: it is a small constant per sweep
call, bounded further by the number of committed batches, in any
environment.  Layer 1 counts statements.

Why the sweep-3 shape alone is not enough
-----------------------------------------
Sweep 3 batches with a single ``unnest($1::uuid[])`` because every promoted
row shares ``kind`` and ``detail``.  Here the per-row values are all DISTINCT:
``started_at``, ``attempt``, ``duration_ms``, ``locked_by_worker``, and the
event ``detail``'s ``to_state`` (a three-way CASE yielding
``pending``/``cancelled``/``crashed``).  The batch carries multi-column
arrays AND a row cap — so Layer 1 asserts both properties independently.

Invariant the batching must preserve
-------------------------------------
``job_events.occurred_at`` is stamped with per-row ``clock_timestamp()`` (see
``_sql.py``'s batched-insert ladder) and must stay co-monotonic with
``job_events.id`` -- that co-monotonicity is the whole basis of the watermark
above.  Layer 2 pins both the co-monotonicity and the per-row distinctness of
``occurred_at``, so a rewrite that collapses to a single transaction-wide
timestamp (``now()``) or reorders inserts relative to the bigserial fails here.

Layer 1 tests (``TestSweepExpiredLocksIsBounded``) pin the boundedness
contract the sweep enforces.  Layer 2 tests
(``TestSweepExpiredLocksBehaviourPinned``) pin the observable behaviour that
must not change -- the safety net over the SQL.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.integration

# Grace periods used by every call here.  The cancel carve-out in _SWEEP_1_SQL
# adds cancel_grace + cleanup_grace + 60s on top of lock expiry before a job
# with cancel_phase != 0 becomes eligible, so seeds that must be swept on the
# cancelled branch push lock_expires_at well past that sum.
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)
_CANCEL_CARVE_OUT_TOTAL_SECONDS = 30 + 30 + 60

# Large enough that a per-row loop is unmistakable in the statement count and
# small enough to keep the test fast.  The defect is linear, so the shape of
# the failure at 200 is identical to the shape measured at 6,000.
_BACKLOG = 200

# What a fixed sweep may spend per call.  A batched implementation needs a
# handful of statements regardless of N (the driving UPDATE, an attempt INSERT,
# an event INSERT, the pg_notify); a LIMIT+drain implementation needs a few per
# committed batch.  Anything proportional to _BACKLOG is the defect.
_MAX_STATEMENTS_PER_CALL = 32


class _CountingConn:
    """Delegates to a real connection, counting awaited statements.

    The statement count IS the property under test: correctness never differed
    between the per-row loop and a batched write, only the number of awaited
    round trips taken inside the transaction that holds every matched job row
    locked -- and, via ``RECLAIM_EVENT_VISIBILITY_DELAY``, how long the
    ``job_events`` writes sit uncommitted.

    Counting is by *table touched*, not by SQL spelling, so any batched form
    (multi-row VALUES, ``unnest`` over parallel arrays, a data-modifying CTE
    folded into the driving statement) satisfies the assertion while a loop
    respelled as ``enumerate``, a comprehension of awaits, or a helper does
    not.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.attempt_inserts = 0
        self.event_inserts = 0
        self.statements = 0

    def _tally(self, sql: str) -> None:
        self.statements += 1
        upper = sql.upper()
        if "INSERT INTO" in upper:
            if ".JOB_ATTEMPTS" in upper:
                self.attempt_inserts += 1
            if ".JOB_EVENTS" in upper:
                self.event_inserts += 1

    async def execute(self, sql: str, *args: object) -> str:
        self._tally(sql)
        result: str = await self._conn.execute(sql, *args)
        return result

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        self._tally(sql)
        rows: list[asyncpg.Record] = await self._conn.fetch(sql, *args)
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _as_dict(value: object) -> dict[str, Any]:
    """Decode a jsonb column, whether or not a codec is registered.

    ``clean_pg_conn`` registers no jsonb type codec, so asyncpg hands back the
    raw ``str``; a pooled connection elsewhere in the suite may decode for us.
    The existing sweep tests guard the same way (``test_postgres_sweeps.py``
    :500 and :543), so this mirrors the established convention rather than
    betting on one side of it.
    """
    if isinstance(value, str):
        decoded: dict[str, Any] = json.loads(value)
        return decoded
    assert isinstance(value, dict), f"expected a jsonb object, got {type(value)!r}"
    return value


async def _seed_worker(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )


async def _seed_running_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    worker_id: UUID | None,
    max_attempts: int,
    attempt: int,
    retry_kind: str,
    cancel_phase: int,
    lock_expired_seconds_ago: float,
    started_seconds_ago: float = 30.0,
) -> None:
    """Seed running jobs in ONE round trip via ``unnest``.

    Row-by-row seeding would itself take N round trips and would dominate the
    runtime of a 200-row test; it would also make the seed, rather than the
    sweep, the slow part of any timing observation.
    """
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608  # Why: schema is a test-fixture identifier validated upstream.
        "    id, actor, queue, payload, max_attempts, retry_kind,"
        "    status, priority, attempt, scheduled_at,"
        "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
        "    cancel_phase, cancel_requested_at"
        ") SELECT"
        "    t.id, 'test_actor', 'default', '{\"key\": \"value\"}'::jsonb,"
        "    $2::smallint, $3::text,"
        "    'running', 0, $4::smallint, clock_timestamp(),"
        # Explicit ::uuid so asyncpg can infer the type when worker_id is NULL
        # (the orphaned-holder seed), which it cannot do from context alone.
        "    $5::uuid, clock_timestamp() - ($6::double precision * interval '1 second'),"
        "    clock_timestamp() - ($7::double precision * interval '1 second'),"
        "    clock_timestamp() - ($7::double precision * interval '1 second'),"
        "    $8::smallint,"
        "    CASE WHEN $8::smallint = 0 THEN NULL ELSE clock_timestamp() END"
        " FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        max_attempts,
        retry_kind,
        attempt,
        worker_id,
        lock_expired_seconds_ago,
        started_seconds_ago,
        cancel_phase,
    )


# ══════════════════════════════════════════════════════════════════════
# LAYER 1 — the boundedness contract these tests enforce.
# ══════════════════════════════════════════════════════════════════════


class TestSweepExpiredLocksIsBounded:
    """Layer 1: pins the boundedness contract.

    Two independent properties, asserted by two independent tests so a
    partial regression cannot pass by accident:

    (a) statements-per-row is bounded -- no per-row attempt/event INSERT
        loop inside the batch transaction;
    (b) one call honours a row cap -- a single call cannot pull an
        unbounded backlog into one transaction, however cheap each row
        has become.

    (a) alone still opens a transaction over the entire backlog; (b) alone
    still takes 2N round trips inside it.  Both are required for the
    transaction to stay inside ``RECLAIM_EVENT_VISIBILITY_DELAY``.
    """

    async def test_statement_count_is_not_proportional_to_backlog(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Reclaiming N expired locks must not take 2N awaited round trips.

        Drives the real ``sweep_expired_locks`` and counts round trips rather
        than grepping the source for ``for rec in rows:``: a reintroduced loop
        spelled any other way -- ``enumerate``, a comprehension of awaits, a
        helper function -- is the same regression, and a regex would not see
        it.

        A bounded call spends a constant handful of statements (the
        driving UPDATE, one batched attempt INSERT, one batched event
        INSERT, one pg_notify) regardless of N.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        job_ids = [new_uuid() for _ in range(_BACKLOG)]

        await _seed_worker(clean_pg_conn, schema, worker_id)
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            job_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        counting = _CountingConn(clean_pg_conn)
        count = await PostgresBackend.sweep_expired_locks(
            counting,  # type: ignore[arg-type]  # Why: duck-typed connection; only execute/fetch/transaction are used.
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )

        assert count > 0, "the seeded backlog must be eligible for reclaim"

        assert counting.attempt_inserts <= _MAX_STATEMENTS_PER_CALL, (
            f"expected a bounded number of job_attempts INSERTs for {_BACKLOG} "
            f"reclaimed jobs, got {counting.attempt_inserts} — the per-row loop is "
            "still there, taking one awaited round trip per row inside the "
            "transaction that holds every matched job locked"
        )
        assert counting.event_inserts <= _MAX_STATEMENTS_PER_CALL, (
            f"expected a bounded number of job_events INSERTs for {_BACKLOG} "
            f"reclaimed jobs, got {counting.event_inserts} — every one of those "
            "round trips extends the window between the job_events INSERT and its "
            "COMMIT, which RECLAIM_EVENT_VISIBILITY_DELAY (2s) bounds at the cost "
            "of a silently missed reclaim event when exceeded"
        )
        assert counting.statements <= _MAX_STATEMENTS_PER_CALL, (
            f"expected at most {_MAX_STATEMENTS_PER_CALL} awaited statements for "
            f"{_BACKLOG} reclaimed jobs, got {counting.statements}; a per-row cost "
            "makes transaction duration linear in an unbounded crash backlog"
        )

    async def test_one_call_honours_a_row_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One sweep call must not pull an unbounded backlog into one
        transaction.

        ``_SWEEP_1_SQL``'s ``snap`` CTE is LIMIT-ed: it locks at most the
        batch's worth of running jobs whose locks have expired, in one
        ``FOR UPDATE SKIP LOCKED`` scan, in one transaction.  A
        fleet-wide crash makes the eligible set as large as the fleet's
        in-flight concurrency — exactly the case the cap keeps out of
        any single transaction.

        This asserts the cap exists and is honoured, without pinning its
        spelling: whatever bound applies, a single call must touch
        strictly fewer than the whole backlog when the backlog exceeds it.
        ``_MAX_STATEMENTS_PER_CALL`` is a deliberately loose stand-in for that
        bound -- the point is that SOME bound below ``_BACKLOG`` applies.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        job_ids = [new_uuid() for _ in range(_BACKLOG)]

        await _seed_worker(clean_pg_conn, schema, worker_id)
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            job_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )

        assert count < _BACKLOG, (
            f"one sweep call reclaimed all {count} of {_BACKLOG} expired locks — "
            "_SWEEP_1_SQL's snap CTE has no LIMIT, so a single transaction holds "
            "the entire crash backlog under FOR UPDATE SKIP LOCKED while it writes "
            "an attempt row and an event row for every one of them"
        )
        assert count > 0, "a capped sweep must still make progress on each call"

    async def test_repeated_calls_drain_the_whole_backlog(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A row cap must not lose work: repeated calls drain the backlog.

        The companion to the cap assertion.  A cap that leaves rows behind
        forever is not a bound, and a cap whose per-call progress is zero is a
        stall.  Together with ``test_one_call_honours_a_row_cap`` this pins
        "bounded per call, complete across calls" -- the LIMIT+drain shape.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        job_ids = [new_uuid() for _ in range(_BACKLOG)]

        await _seed_worker(clean_pg_conn, schema, worker_id)
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            job_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        calls = 0
        reclaimed = 0
        first_call_count = -1
        # Generous ceiling: any sane cap drains 200 rows in far fewer calls.
        while calls < 500:
            n = await PostgresBackend.sweep_expired_locks(
                clean_pg_conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )
            calls += 1
            if first_call_count < 0:
                first_call_count = n
            if n == 0:
                break
            reclaimed += n

        assert reclaimed == _BACKLOG, (
            f"repeated sweep calls must eventually reclaim every expired lock; "
            f"got {reclaimed} of {_BACKLOG} after {calls} calls"
        )
        assert first_call_count < _BACKLOG, (
            "the first call drained the entire backlog in one transaction — there "
            "is no row cap on _SWEEP_1_SQL"
        )

        still_running = await clean_pg_conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'running'"  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert still_running == 0, "no expired-lock job may be left running"


# ══════════════════════════════════════════════════════════════════════
# LAYER 2 — CORRECTNESS PINNING.  These PASS TODAY and MUST STILL PASS
# after the fix.  They are the safety net over the SQL rewrite.
# ══════════════════════════════════════════════════════════════════════


class TestSweepExpiredLocksBehaviourPinned:
    """Layer 2: pins existing behaviour — must pass BEFORE AND AFTER the fix.

    The fix rewrites SQL that mutates job state and writes audit rows, so
    every observable consequence of the sweep is pinned here: the three-way
    status CASE, the ``job_attempts`` row, the ``job_events`` row and its
    ``detail`` keys, non-matching rows staying untouched, the
    ``occurred_at``/``id`` co-monotonicity the reclaim watermark depends on,
    the per-row distinctness of ``occurred_at`` that proves ``clock_timestamp()``
    survived, and idempotency across repeated calls.
    """

    async def test_three_way_status_outcome(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        All three branches of ``_SWEEP_1_SQL``'s status CASE, seeded
        explicitly in one run so a rewrite cannot quietly collapse the CASE
        into a two-way one:

        * ``attempt < max_attempts AND retry_kind != 'non_retryable'``
          → ``pending``, ``scheduled_at`` pushed ~5s out, ``finished_at``
          left NULL;
        * ``cancel_phase != 0`` (and retries not available)
          → ``cancelled``, ``finished_at`` stamped;
        * otherwise → ``crashed``, ``finished_at`` stamped.

        All three branches clear ``locked_by_worker``/``lock_expires_at`` and
        reset ``cancel_phase``/``cancel_requested_at``.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await _seed_worker(clean_pg_conn, schema, worker_id)

        retry_ids = [new_uuid() for _ in range(3)]
        cancel_ids = [new_uuid() for _ in range(3)]
        crash_ids = [new_uuid() for _ in range(3)]

        # Branch 1: retries remain and retry is allowed → pending.
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            retry_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )
        # Branch 2: attempts exhausted AND a cancel was in flight → cancelled.
        # The lock must be expired past cancel_grace + cleanup_grace + 60s for
        # the cancel carve-out in the WHERE clause to admit it at all.
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            cancel_ids,
            worker_id=worker_id,
            max_attempts=1,
            attempt=1,
            retry_kind="transient",
            cancel_phase=1,
            lock_expired_seconds_ago=float(_CANCEL_CARVE_OUT_TOTAL_SECONDS + 30),
        )
        # Branch 3: attempts exhausted, no cancel in flight → crashed.
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            crash_ids,
            worker_id=worker_id,
            max_attempts=1,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count == 9, f"all nine seeded jobs must be reclaimed, got {count}"

        rows = await clean_pg_conn.fetch(
            "SELECT id, status, locked_by_worker, lock_expires_at, cancel_phase, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            "       cancel_requested_at, finished_at, scheduled_at, "
            "       clock_timestamp() AS pg_now "
            f'FROM "{schema}".jobs',
        )
        by_id = {row["id"]: row for row in rows}
        assert len(by_id) == 9

        for job_id in retry_ids:
            row = by_id[job_id]
            assert row["status"] == "pending", (
                "attempt < max_attempts and retry_kind != 'non_retryable' must "
                f"retry, got {row['status']}"
            )
            assert row["finished_at"] is None, "the retry branch must not finish the job"
            delta = row["scheduled_at"] - row["pg_now"]
            assert timedelta(seconds=2) <= delta <= timedelta(seconds=8), (
                f"retry branch must push scheduled_at ~5s out, got {delta}"
            )

        for job_id in cancel_ids:
            row = by_id[job_id]
            assert row["status"] == "cancelled", (
                "an exhausted job with a cancel in flight must land on 'cancelled', "
                f"not {row['status']} — the caller's explicit request is the honest "
                "terminal label"
            )
            assert row["finished_at"] is not None

        for job_id in crash_ids:
            row = by_id[job_id]
            assert row["status"] == "crashed", (
                f"an exhausted job with no cancel in flight must crash, got {row['status']}"
            )
            assert row["finished_at"] is not None

        for job_id in (*retry_ids, *cancel_ids, *crash_ids):
            row = by_id[job_id]
            assert row["locked_by_worker"] is None, "every branch must clear the lock holder"
            assert row["lock_expires_at"] is None, "every branch must clear the lock expiry"
            assert row["cancel_phase"] == 0, "every branch must reset cancel_phase"
            assert row["cancel_requested_at"] is None, "every branch must reset cancel_requested_at"

    async def test_job_attempts_row_shape(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        Exactly one ``job_attempts`` row per swept job, on EVERY branch, with
        ``outcome='crashed'`` and ``error_class='WorkerCrashed'`` regardless of
        the job's terminal label (that IS what happened to the attempt), the
        job's ``attempt`` number, the snapshotted ``worker_id``, and a
        ``duration_ms`` measured entirely in the database clock domain --
        ``started_at`` to the statement's own ``clock_timestamp()``, never the
        app's clock.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await _seed_worker(clean_pg_conn, schema, worker_id)

        retry_ids = [new_uuid() for _ in range(2)]
        crash_ids = [new_uuid() for _ in range(2)]
        started_seconds_ago = 30.0

        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            retry_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=2,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
            started_seconds_ago=started_seconds_ago,
        )
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            crash_ids,
            worker_id=worker_id,
            max_attempts=1,
            attempt=1,
            retry_kind="non_retryable",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
            started_seconds_ago=started_seconds_ago,
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count == 4

        attempts = await clean_pg_conn.fetch(
            "SELECT job_id, attempt, started_at, finished_at, outcome, error_class, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            "       error_message, error_traceback, duration_ms, worker_id, metadata "
            f'FROM "{schema}".job_attempts',
        )
        assert len(attempts) == 4, (
            f"exactly one job_attempts row per swept job, got {len(attempts)}"
        )

        expected_attempt = dict.fromkeys(retry_ids, 2) | dict.fromkeys(crash_ids, 1)
        for row in attempts:
            job_id = row["job_id"]
            assert row["outcome"] == "crashed", (
                "the attempt outcome is 'crashed' on every branch — that IS what "
                f"happened to the attempt; got {row['outcome']}"
            )
            assert row["error_class"] == "WorkerCrashed"
            assert row["error_message"] == "lock expired before worker reported terminal state"
            assert row["error_traceback"] is None
            assert row["worker_id"] == worker_id, (
                "the snapshotted lock holder must reach job_attempts.worker_id"
            )
            assert row["attempt"] == expected_attempt[job_id], (
                "the attempt row records the job's own attempt number"
            )
            assert row["finished_at"] is not None
            assert _as_dict(row["metadata"]) == {}
            duration_ms = row["duration_ms"]
            assert duration_ms is not None, (
                "duration_ms must be computed when started_at is present"
            )
            # started_at was seeded 30s back; the sweep measures to its own
            # clock_timestamp(). A value derived from the app clock instead
            # would be offset by app/database skew, and a now()-based one
            # would drift within a long transaction.
            assert 25_000 <= duration_ms <= 120_000, (
                f"duration_ms {duration_ms} is not a sane database-clock span for a "
                f"job started {started_seconds_ago}s ago"
            )

    async def test_job_events_row_shape_and_detail(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        Exactly one ``job_events`` row per swept job, ``kind='state_change'``,
        ``detail.from_state='running'``, ``detail.reason='lock_expired'``, and
        ``detail.to_state`` matching the job's own new status on each of the
        three branches.  ``detail.worker_id`` is present (as the string form of
        the snapshotted holder) when ``locked_by_worker`` was set and ABSENT
        when it was not -- an omitted key, not a null one.

        Jobs are seeded with no pre-existing events so the per-job counts here
        are exactly the sweep's own writes.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await _seed_worker(clean_pg_conn, schema, worker_id)

        retry_ids = [new_uuid() for _ in range(2)]
        cancel_ids = [new_uuid() for _ in range(2)]
        crash_ids = [new_uuid() for _ in range(2)]
        orphan_ids = [new_uuid() for _ in range(2)]

        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            retry_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            cancel_ids,
            worker_id=worker_id,
            max_attempts=1,
            attempt=1,
            retry_kind="transient",
            cancel_phase=2,
            lock_expired_seconds_ago=float(_CANCEL_CARVE_OUT_TOTAL_SECONDS + 30),
        )
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            crash_ids,
            worker_id=worker_id,
            max_attempts=1,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )
        # No lock holder at all: jobs.locked_by_worker is not an FK, so NULL is
        # a legal seed and exercises the "omit worker_id from detail" path.
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            orphan_ids,
            worker_id=None,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count == 8

        events = await clean_pg_conn.fetch(
            "SELECT e.id, e.job_id, e.kind, e.detail, j.status "  # noqa: S608  # Why: schema is a test-fixture identifier.
            f'FROM "{schema}".job_events e '
            f'JOIN "{schema}".jobs j ON j.id = e.job_id '
            "ORDER BY e.id",
        )
        assert len(events) == 8, f"exactly one job_events row per swept job, got {len(events)}"

        expected_to_state = (
            dict.fromkeys(retry_ids, "pending")
            | dict.fromkeys(cancel_ids, "cancelled")
            | dict.fromkeys(crash_ids, "crashed")
            | dict.fromkeys(orphan_ids, "pending")
        )
        seen: set[UUID] = set()
        for row in events:
            job_id = row["job_id"]
            seen.add(job_id)
            assert row["kind"] == "state_change"
            detail = _as_dict(row["detail"])
            assert detail["from_state"] == "running"
            assert detail["reason"] == "lock_expired", (
                "the reclaim event's reason is the audit discriminator for "
                "crash-reclaim; it must survive any rewrite"
            )
            assert detail["to_state"] == expected_to_state[job_id], (
                "detail.to_state must match the job's own new status"
            )
            assert detail["to_state"] == row["status"], (
                "detail.to_state must agree with the jobs row written in the same transaction"
            )
            if job_id in orphan_ids:
                assert "worker_id" not in detail, (
                    "worker_id must be OMITTED from detail (not null) when there was "
                    "no lock holder to snapshot"
                )
            else:
                assert detail["worker_id"] == str(worker_id), (
                    "detail.worker_id carries the last-known holder id, as a string"
                )

        assert seen == set(expected_to_state), "every swept job must get exactly one event"

    async def test_non_matching_rows_are_untouched(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        A running job whose lock has NOT expired must be left completely
        untouched: same status, same lock holder, same lock expiry, same
        ``cancel_phase``, and no ``job_attempts`` or ``job_events`` row
        written for it.  Also pins the cancel carve-out: a job with
        ``cancel_phase != 0`` whose lock expired only moments ago is inside the
        extra ``cancel_grace + cleanup_grace + 60s`` window and must NOT be
        swept yet.

        This is the predicate half of the rewrite risk: a fix that adds a LIMIT
        or restructures the CTE must not widen (or narrow) what it matches.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        await _seed_worker(clean_pg_conn, schema, worker_id)

        eligible_ids = [new_uuid() for _ in range(2)]
        live_lock_ids = [new_uuid() for _ in range(2)]
        carve_out_ids = [new_uuid() for _ in range(2)]

        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            eligible_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )
        # Lock still 5 minutes in the future — negative "seconds ago".
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            live_lock_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=-300.0,
        )
        # Cancel in flight, lock only just expired: inside the carve-out.
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            carve_out_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=1,
            lock_expired_seconds_ago=5.0,
        )

        before = await clean_pg_conn.fetch(
            "SELECT id, status, locked_by_worker, lock_expires_at, cancel_phase, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            "       attempt, scheduled_at, finished_at "
            f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY id',
            [*live_lock_ids, *carve_out_ids],
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count == len(eligible_ids), (
            f"only the {len(eligible_ids)} eligible jobs may be swept, got {count}"
        )

        after = await clean_pg_conn.fetch(
            "SELECT id, status, locked_by_worker, lock_expires_at, cancel_phase, "  # noqa: S608  # Why: schema is a test-fixture identifier.
            "       attempt, scheduled_at, finished_at "
            f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY id',
            [*live_lock_ids, *carve_out_ids],
        )
        assert [dict(r) for r in after] == [dict(r) for r in before], (
            "rows not matching the sweep predicate must be byte-for-byte unchanged"
        )

        untouched = [*live_lock_ids, *carve_out_ids]
        stray_attempts = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier.
            untouched,
        )
        assert stray_attempts == 0, "no job_attempts row for an unswept job"
        stray_events = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier.
            untouched,
        )
        assert stray_events == 0, "no job_events row for an unswept job"

    async def test_occurred_at_is_co_monotonic_with_id_and_distinct_per_row(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        ``job_events.occurred_at`` is stamped per row with
        ``clock_timestamp()`` and must stay co-monotonic with the bigserial
        ``id``: ordering by ``id`` must give non-decreasing ``occurred_at``.
        ``poll_reclaim_events``'s entire trailing-watermark scheme (see
        ``RECLAIM_EVENT_VISIBILITY_DELAY``) rests on that, and this sweep is
        the writer whose events that poll delivers.

        ``occurred_at`` must also be DISTINCT per row.  That is the observable
        signature of per-row ``clock_timestamp()``: a rewrite that reaches for
        ``now()`` (fixed at transaction start) or hoists a single timestamp
        across a batched INSERT collapses every row in the batch onto one
        instant, destroying the ordering information the watermark reads.
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        job_ids = [new_uuid() for _ in range(40)]

        await _seed_worker(clean_pg_conn, schema, worker_id)
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            job_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        count = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert count == len(job_ids)

        rows = await clean_pg_conn.fetch(
            f'SELECT id, occurred_at FROM "{schema}".job_events ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert len(rows) == len(job_ids)

        ids = [row["id"] for row in rows]
        stamps = [row["occurred_at"] for row in rows]

        inversions = [
            (ids[i - 1], stamps[i - 1], ids[i], stamps[i])
            for i in range(1, len(rows))
            if stamps[i] < stamps[i - 1]
        ]
        assert not inversions, (
            "job_events.occurred_at must be non-decreasing in id order — "
            f"found {len(inversions)} inversion(s): {inversions[:3]}. "
            "poll_reclaim_events' trailing watermark reads occurred_at as a proxy "
            "for id order; an inversion is a silently missed reclaim event."
        )

        assert len(set(stamps)) == len(stamps), (
            "occurred_at must be DISTINCT per row — identical values mean the "
            "per-row clock_timestamp() was replaced by a single transaction-wide "
            "timestamp (now(), or one hoisted across a batched INSERT)"
        )

    async def test_rerunning_the_sweep_reclaims_nothing_extra(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Pins the behaviour the bounded sweep must preserve.

        The sweep is idempotent over an already-drained backlog: once every
        expired lock has been reclaimed, a further call returns 0 and writes no
        additional ``job_attempts`` or ``job_events`` rows.

        Written as a drain loop rather than a single call so it holds for a
        capped implementation too: the assertion is about the state AFTER the
        backlog is drained, not about how many calls draining took.  (Note the
        retry branch pushes ``scheduled_at`` ~5s out but leaves the job
        ``pending``, so it is no longer a sweep-1 candidate at all --
        ``status='running'`` is the predicate.)
        """
        schema = module_pg_schema.schema_name
        worker_id = new_uuid()
        job_ids = [new_uuid() for _ in range(30)]

        await _seed_worker(clean_pg_conn, schema, worker_id)
        await _seed_running_jobs(
            clean_pg_conn,
            schema,
            job_ids,
            worker_id=worker_id,
            max_attempts=3,
            attempt=1,
            retry_kind="transient",
            cancel_phase=0,
            lock_expired_seconds_ago=10.0,
        )

        reclaimed = 0
        for _ in range(500):
            n = await PostgresBackend.sweep_expired_locks(
                clean_pg_conn,
                _CANCEL_GRACE,
                _CLEANUP_GRACE,
                schema=schema,
            )
            if n == 0:
                break
            reclaimed += n
        assert reclaimed == len(job_ids), (
            f"the drain must reclaim every seeded job, got {reclaimed}"
        )

        attempts_after_drain = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_attempts'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        events_after_drain = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert attempts_after_drain == len(job_ids)
        assert events_after_drain == len(job_ids)

        again = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,
            _CANCEL_GRACE,
            _CLEANUP_GRACE,
            schema=schema,
        )
        assert again == 0, "a drained backlog must reclaim nothing on the next call"

        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_attempts'  # noqa: S608  # Why: schema is a test-fixture identifier.
            )
            == attempts_after_drain
        ), "a no-op sweep must write no extra job_attempts rows"
        assert (
            await clean_pg_conn.fetchval(
                f'SELECT count(*) FROM "{schema}".job_events'  # noqa: S608  # Why: schema is a test-fixture identifier.
            )
            == events_after_drain
        ), "a no-op sweep must write no extra job_events rows"

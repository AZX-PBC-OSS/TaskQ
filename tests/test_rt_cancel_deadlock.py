"""Real-PG induced-deadlock attack on the bounded bulk cancel's retry loop.

``_drain_cancel_batches`` retries a batch on ``DeadlockDetectedError``
(3 attempts, exponential backoff with jitter) and promises that a
deadlocked batch contributes no phantom ids, no duplicate events, and
that the retried batch re-runs cleanly.  The pinned retry coverage
(``test_cancel_where_pg.py::TestDeadlockRetry``) is mock-based; this
file induces a GENUINE server-side 40P01 against the real drain, across
batch boundaries: batch 1 commits, batch 2 deadlocks, batch 2's retry
re-runs.

The trap is deliberately lock-ORDER-independent (an earlier design that
relied on the driving UPDATE locking rows in ``ORDER BY id`` sequence
proved unstable — the planner's join order, not the CTE's ORDER BY,
decides row-lock order):

* 102 matching pending jobs at ``batch_size=100`` → batch 1 (the 100
  lowest ids) commits; batch 2 = the two highest ids.
* The drain is gated INSIDE batch 2's transaction — its driving UPDATE
  has already locked both batch-2 rows, and the gate holds the batch's
  first event INSERT open.
* A second connection then takes ``LOCK TABLE job_events IN SHARE
  MODE``: the drain's event INSERT (RowExclusive on job_events) cannot
  proceed while the drain still holds both job-row locks.
* The gate opens; the drain's INSERT blocks on the table lock (its
  deadlock detector arms NOW), and only then — after the test observes
  the drain waiting — the second connection requests one of the
  drain-held rows ``FOR UPDATE``.  That closes a genuine cross-type
  cycle: drain (holds job row locks, wants the job_events table lock) ↔
  holder (holds the table lock, wants a job row lock).
* The drain's detector armed first, so Postgres aborts the DRAIN's
  batch — the real ``DeadlockDetectedError`` the retry loop exists for.
* The holder commits during the drain's backoff, so the retried batch
  re-runs against unlocked, still-pending rows.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

# The gap between the drain's INSERT blocking (its detector arming) and
# the holder's row request (the holder's detector arming): the drain
# arms strictly first, so it is deterministically the transaction
# Postgres aborts — the retry path under test.
_ARM_GAP = 0.1


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, 'pending'::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', ARRAY['bulk']::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
    )


def _is_event_insert(sql: str) -> bool:
    upper = sql.lstrip().upper()
    return upper.startswith("INSERT INTO") and ".JOB_EVENTS" in upper


class _DeadlockWitnessConn:
    """Delegates to a real connection: gates the drain inside batch 2's
    transaction (before its first event INSERT), and counts the genuine
    deadlocks the drain's event writes raise.

    The drain takes a fresh connection per batch AND per retry attempt,
    so the counters and the gate live on the pool-level state object.
    """

    def __init__(self, conn: Any, state: _DeadlockWitnessState) -> None:
        self._conn = conn
        self._state = state

    def transaction(self, **kwargs: object) -> Any:
        outer = self

        @asynccontextmanager
        async def _tx() -> AsyncGenerator[None]:
            async with outer._conn.transaction(**kwargs):
                yield

        return _tx()

    async def fetchrow(self, sql: str, *args: object) -> Any:
        if "cancelled_prev_statuses" in sql:
            self._state.ps_driving_calls += 1
        return await self._conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: object) -> Any:
        if _is_event_insert(sql):
            self._state.event_inserts += 1
            if self._state.event_inserts == self._state.gate_at:
                # The drain is inside batch 2's transaction: its driving
                # UPDATE has locked the batch rows, and this is the
                # batch's first event write.  Hold it open while the
                # holder arms the table lock.
                self._state.gate_entered.set()
                await self._state.gate_release.wait()
        try:
            return await self._conn.execute(sql, *args)
        except asyncpg.DeadlockDetectedError:
            if _is_event_insert(sql):
                self._state.event_insert_deadlocks += 1
            raise

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _DeadlockWitnessState:
    def __init__(self, gate_at: int) -> None:
        self.ps_driving_calls = 0
        self.event_inserts = 0
        self.event_insert_deadlocks = 0
        self.gate_at = gate_at
        self.gate_entered = asyncio.Event()
        self.gate_release = asyncio.Event()


class _WitnessPool:
    def __init__(self, pool: Any, state: _DeadlockWitnessState) -> None:
        self._pool = pool
        self.state = state

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[_DeadlockWitnessConn]:
        async with self._pool.acquire(**kwargs) as conn:
            yield _DeadlockWitnessConn(conn, self.state)


async def _wait_for_lock_waiter(
    conn: asyncpg.Connection,
    *,
    budget: float = 10.0,
) -> None:
    """Block until some OTHER backend is waiting on a lock — proof the
    drain's event INSERT is parked on the holder's table lock."""
    deadline = asyncio.get_running_loop().time() + budget
    while asyncio.get_running_loop().time() < deadline:
        waiters: int = await conn.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND state = 'active' "
            "AND pid <> pg_backend_pid()"
        )
        if waiters:
            return
        await asyncio.sleep(0.02)
    pytest.fail("the drain's batch-2 event INSERT never blocked on the table lock")


async def test_real_deadlock_mid_drain_retries_cleanly_with_no_phantoms(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A genuine server-side 40P01 on batch 2 of the drain, raised from
    the batch's own event INSERT: batch 1 stays committed, the aborted
    batch contributes nothing, the retry re-runs it against the
    now-unlocked rows, and the totals are exact — every job cancelled
    once, every event written once."""
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(102)]
    await _seed_jobs(clean_pg_conn, schema, job_ids)
    # Batch 2 = the two highest ids in the CTE's ORDER BY id; both rows
    # are locked by batch 2's driving UPDATE when the gate opens.
    batch2 = sorted(job_ids)[-2:]

    holder = await module_pg_pool.acquire()
    holder_tx = holder.transaction()
    await holder_tx.start()
    try:
        # Batch 1 = event INSERTs #1 (state_change) and #2
        # (cancel_request); the gate holds batch 2's first INSERT (#3)
        # open, inside the batch's transaction, after its driving UPDATE.
        state = _DeadlockWitnessState(gate_at=3)
        pool = _WitnessPool(module_pg_pool, state)
        cancel_task = asyncio.create_task(
            _cancel_where(
                pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
                schema,
                render(schema),
                JobFilter(tags=("bulk",)),
                "offboard",
                batch_size=100,
            )
        )
        await state.gate_entered.wait()

        # The holder's half of the cycle: the SHARE table lock makes the
        # drain's event INSERT (RowExclusive on job_events) wait.
        await holder.execute(
            f'LOCK TABLE "{schema}".job_events IN SHARE MODE'
        )  # Why: schema is a test-fixture identifier, validated by render() above.
        state.gate_release.set()
        await _wait_for_lock_waiter(holder)
        await asyncio.sleep(_ARM_GAP)

        # Close the cycle from the other side: the holder now wants a
        # row the drain's batch-2 driving UPDATE holds.  The drain's
        # detector armed first, so the DRAIN's batch is the transaction
        # Postgres aborts.
        await asyncio.wait_for(
            holder.execute(
                f'SELECT id FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) FOR UPDATE',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the ids are $-bound.
                batch2,
            ),
            timeout=15,
        )
        # The drain's abort released the batch-2 rows (that is what let
        # the SELECT above return); committing releases the table lock
        # before the drain's backoff ends.
        await holder_tx.commit()
    finally:
        await module_pg_pool.release(holder)

    result, _notify = await asyncio.wait_for(cancel_task, timeout=30)

    assert state.event_insert_deadlocks == 1, (
        f"expected exactly one genuine DeadlockDetectedError from the drain's batch-2 "
        f"event INSERT; saw {state.event_insert_deadlocks}"
    )
    assert state.ps_driving_calls == 3, (
        f"batch 1 + deadlocked batch 2 + retried batch 2 = three ps driving executions; "
        f"saw {state.ps_driving_calls} — the retry did not re-run the batch"
    )

    # Exact totals, no phantom ids, no duplicates.
    assert result.cancelled_directly == 102
    assert result.cancel_requested == 0
    assert len(result.cancelled_ids) == len(set(result.cancelled_ids)) == 102
    assert set(result.cancelled_ids) == set(job_ids)

    statuses = await clean_pg_conn.fetch(
        f'SELECT status::text AS status, finished_at FROM "{schema}".jobs',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
    )
    assert len(statuses) == 102
    assert {r["status"] for r in statuses} == {"cancelled"}, (
        "batch 1's committed rows AND the retried batch 2's rows must all be cancelled"
    )
    assert all(r["finished_at"] is not None for r in statuses)

    # Exactly-once events across the aborted attempt and its retry.
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert {r["kind"] for r in events} == {"state_change", "cancel_request"}
    assert all(r["n"] == 1 for r in events), (
        "the deadlocked attempt's rolled-back event writes must not resurface as "
        "duplicates when the retry re-runs the batch"
    )
    assert len(events) == 2 * 102

    # The holder's lock-only touches left no trace of their own.
    holder_rows = await clean_pg_conn.fetch(
        f'SELECT status::text AS status, max_attempts FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "WHERE id = ANY($1::uuid[])",
        batch2,
    )
    assert {(r["status"], r["max_attempts"]) for r in holder_rows} == {("cancelled", 3)}

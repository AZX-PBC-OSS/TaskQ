# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""The batch terminal path's per-job reset cost is not a batches-row lock queue.

The latency red-team's batch-shape profile (`attack-hotpath-findings.md`)
measured `reset_batch_failures` - the `UPDATE batches SET
consecutive_failures = 0` every successful job of a batch pays through
`apply_batch_terminal_outcome` - at **8.2 ms/job, 24x the terminal UPDATE's
0.34 ms**, with `Lock:transactionid`/`Lock:tuple` waits and ungranted
`pg_locks` counts up to 5.

The arithmetic of the defect, stated so the pins below have a derivation to
cite: the reset's UPDATE matched the row on `id` and `status` ALONE, so a
healthy batch - the overwhelmingly common case, where no failure was ever
recorded and the counter is already 0 - still wrote the row, still took the
batches-row tuple lock, and still paid the statement's LATERAL member count.
Every one of a batch's concurrent terminal transactions then queued on that
row lock for the rest of the holder's transaction (terminal write + hook +
COMMIT), so the per-job cost was the lock QUEUE, not the statement's ~0.1 ms
of work. The fleet profile: 2,000 jobs x 200-member batches x 8 workers put
34.5 ms of mean `pg_stat_statements` exec time on the reset - 68.9 s of
statement time across a 10.3 s drain - while `complete_batch`, which touches
the same row through `FOR UPDATE SKIP LOCKED` (never waits), measured
0.032 ms: the row is not the cost; WAITING for it is.

The fix is the guard the arithmetic predicts: `AND consecutive_failures <> 0`
in the UPDATE's WHERE. A zero counter matches no row, so no tuple lock is
taken (nothing queues) and the LATERAL count never executes (the LEFT JOIN's
left side is empty); the failure-counter contract is untouched - a non-zero
counter resets exactly as before, proven by the pins here and the batch
families (`test_batch_pg.py`, `test_batch_completion_cost_pg.py`,
`test_in_memory_batch.py`, `test_rt_diff_batch.py`).
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._batch_sql import render_batch_sql
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.batch import apply_batch_terminal_outcome
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

# The contended-shape sizes: C terminal transactions in flight, R rounds
# each. 8 matches the fleet profile's actor count.
_CONCURRENCY = 8
_ROUNDS = 6
# The in-transaction hold (seconds) each terminal transaction pays after
# its statements, modeling the fleet's post-hook work (event appends +
# WAL commit) that keeps the holder's row locks until COMMIT. BOTH arms
# pay it, so the ratio isolates the batches-row lock queue.
_HOLD_S = 0.005
# The cost budget: the hook's per-job contended wall may exceed the bare
# terminal UPDATE's by at most HOOK_BUDGET_MULTIPLE. Derived arithmetic:
# the fleet profile measured the reset alone at 24x the terminal UPDATE
# (the defect); post-fix the hook adds only the guarded no-write probe
# (~0.05 ms) plus complete_batch's never-waiting SKIP LOCKED probe
# (0.032 ms measured) to the terminal write, so a same-run budget of 4x
# the terminal UPDATE holds with ~6x headroom against the documented 24x
# defect while tolerating any same-run noise that inflates both arms
# together.
_HOOK_BUDGET_MULTIPLE = 4


def _member_args(actor: str, batch_id: UUID) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        metadata={"batch_id": str(batch_id)},
    )


def _loose_args(actor: str) -> EnqueueArgs:
    """A job with NO batch - its terminal write is the bare-reference arm."""
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
    )


async def _seed_running_batch(
    deps: Any,
    backend: Any,
    schema: str,
    members: int,
) -> tuple[UUID, list[JobRow]]:
    """A real batch row + `members` running member jobs, the production
    pre-terminal state."""
    bid = new_uuid()
    actor = "hotspot_actor"
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
        )
        await backend.create_batch(bid, "default", members, None, None, None)
        args = [_member_args(actor, bid) for _ in range(members)]
        rows = await backend.enqueue_batch(args, connection=conn)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'running', "
            "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
            "locked_by_worker = gen_random_uuid(), "
            "lock_expires_at = clock_timestamp() + interval '600 seconds' "
            "WHERE id = ANY($1::uuid[])",
            [a.id for a in args],
        )
    assert len(rows) == members
    return bid, rows


# ── the mechanism pin ────────────────────────────────────────────────


async def test_reset_on_a_zero_counter_active_batch_neither_locks_nor_probes(
    pg_dsn: str,
) -> None:
    """The defect's mechanism: against an ACTIVE batch whose counter is
    already 0, the reset must not write the batches row (no tuple lock for
    concurrent terminal writes to queue on) and must not run the member
    count (the LATERAL hangs off the UPDATE's returned rows). EXPLAIN
    ANALYZE pins both: on the unguarded statement the CTE updates a row and
    the probe executes; guarded, the CTE returns 0 rows and the probe is
    "(never executed)"."""
    schema = f"reset_hotspot_mech_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await migrate_mod.apply_pending(conn, schema=schema)
        sql = render_batch_sql(schema)
        bid = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".batches (id, queue, expected_size) '
            "VALUES ($1, 'default', 8)",
            bid,
        )
        meta = f'{{"batch_id": "{bid}"}}'
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs (id, queue, actor, payload, max_attempts, '
            "retry_kind, metadata, status) VALUES ($1, 'default', 'seed', '{}'::jsonb, "
            "1, 'non_retryable', $2::jsonb, 'running')",
            [(new_job_id(), meta) for _ in range(8)],
        )
        await conn.execute(f'ANALYZE "{schema}".jobs')

        rows = await conn.fetch(
            f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {sql.reset_batch_failures}",
            bid,
            str(bid),
        )
        plan = "\n".join(r["QUERY PLAN"] for r in rows)

        update_nodes = [
            line for line in plan.splitlines() if "Update on batches" in line and "actual" in line
        ]
        assert update_nodes, f"the batches UPDATE is missing from the plan:\n{plan}"
        assert any("rows=0" in line for line in update_nodes), (
            "a reset against a zero counter WROTE the batches row - the unguarded "
            f"UPDATE took the tuple lock and queued every concurrent terminal "
            f"write behind it:\n{plan}"
        )
        probe_nodes = [line for line in plan.splitlines() if " on jobs" in line]
        assert probe_nodes, f"the member probe is missing from the plan:\n{plan}"
        assert all("(never executed)" in line for line in probe_nodes), (
            "a reset against a zero counter ran the member count - the LATERAL "
            f"probe executed for a batch nothing was reset on:\n{plan}"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── the cost pin ─────────────────────────────────────────────────────


async def test_hook_per_job_wall_is_terminal_update_class_under_contention(
    pg_dsn: str,
) -> None:
    """The fleet profile's cost, pinned as a same-run ratio: drive C
    concurrent terminal transactions through the REAL hook (terminal job
    UPDATE + apply_batch_terminal_outcome('succeeded') + COMMIT) against a
    real batch whose counter is 0, and the same C against batch-free jobs
    running only the terminal UPDATE. The hook's per-job wall must stay
    within HOOK_BUDGET_MULTIPLE x the bare terminal UPDATE's - both arms
    measured in this run on this PG, so runner noise inflates both together.
    On the unguarded statement the hook arm queues 8-deep on the batches
    row (the fleet profile's 24x) and blows the budget."""
    schema = f"reset_hotspot_cost_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid, batch_rows = await _seed_running_batch(deps, backend, schema, _CONCURRENCY * _ROUNDS)

        loose: list[JobRow] = []
        async with deps.worker_pool.acquire() as conn:
            args = [_loose_args("hotspot_actor") for _ in range(_CONCURRENCY * _ROUNDS)]
            loose = await backend.enqueue_batch(args, connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = gen_random_uuid(), "
                "lock_expires_at = clock_timestamp() + interval '600 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [a.id for a in args],
            )

        async def _hook_job(row: JobRow, bid: UUID, conn: Any) -> None:
            async with conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "succeeded", transaction_conn=conn)
                # The fleet condition: a terminal transaction keeps working
                # (and holding whatever rows it touched) until COMMIT - the
                # profile's drains carried event appends + a WAL commit
                # between the hook and the release. Both arms pay this hold,
                # so the ratio isolates the batches-row lock queue.
                await conn.execute("SELECT pg_sleep($1)", _HOLD_S)

        async def _bare_job(row: JobRow, conn: Any) -> None:
            async with conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await conn.execute("SELECT pg_sleep($1)", _HOLD_S)

        async def _timed_arm(jobs: list[Any], fn: Any) -> float:
            queue: asyncio.Queue[Any] = asyncio.Queue()
            for row in jobs:
                queue.put_nowait(row)

            # One DEDICATED connection per worker, like a fleet worker's
            # pool slice: a shared pool's checkout queue would tax both
            # arms' wall equally and dilute the ratio this pin measures.
            conns = [await asyncpg.connect(pg_dsn) for _ in range(_CONCURRENCY)]

            async def _worker(conn: asyncpg.Connection) -> None:
                while not queue.empty():
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await fn(*item, conn)
                    queue.task_done()

            t0 = time.perf_counter()
            await asyncio.gather(*(_worker(c) for c in conns))
            drain = time.perf_counter() - t0
            for c in conns:
                await c.close()
            return drain / len(jobs) * 1000

        bare_ms = await _timed_arm([(r,) for r in loose], _bare_job)
        hook_ms = await _timed_arm([(r, bid) for r in batch_rows], _hook_job)
        ratio = hook_ms / bare_ms
        print(
            f"[reset-hotspot] bare_terminal_update={bare_ms:.3f} ms/job "
            f"hook_path={hook_ms:.3f} ms/job ratio={ratio:.2f}x "
            f"budget={_HOOK_BUDGET_MULTIPLE}x"
        )

        assert ratio <= _HOOK_BUDGET_MULTIPLE, (
            f"the batch hook's contended per-job wall is {ratio:.1f}x the bare "
            f"terminal UPDATE's (hook {hook_ms:.3f} ms vs bare {bare_ms:.3f} ms, "
            f"budget {_HOOK_BUDGET_MULTIPLE}x) - the reset is writing the batches "
            "row per job and queueing every concurrent terminal transaction on "
            "its tuple lock"
        )
    finally:
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()


# ── the contract pins ────────────────────────────────────────────────


async def test_a_batch_with_real_failures_still_counts_and_resets(pg_dsn: str) -> None:
    """The trap the guard must not spring: a batch with REAL recorded
    failures still counts them and the successful outcome still resets the
    counter - through the production hook, on both backend shapes."""
    schema = f"reset_hotspot_fail_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid, rows = await _seed_running_batch(deps, backend, schema, 4)

        # Two real failures: the counter must see both.
        for row in rows[:2]:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "failed", transaction_conn=conn)
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.consecutive_failures == 2, (
            f"the guard ate a real increment: expected 2, got {batch.consecutive_failures}"
        )

        # Then two successes: each resets, the counter must land at 0 and
        # stay there.
        for row in rows[2:]:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "succeeded", transaction_conn=conn)
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.consecutive_failures == 0, (
            f"the guard skipped a NEEDED reset: expected 0, got {batch.consecutive_failures}"
        )
        assert await backend.count_batch_non_terminal(bid) == 0

        # And the reset still returns the member count when it fires for
        # real: re-fail then reset through the protocol.
        await backend.increment_batch_failures(bid)
        remaining = await backend.reset_batch_failures(bid)
        assert remaining == 0  # every member is terminal by now
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.consecutive_failures == 0
    finally:
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()


async def test_concurrent_failures_still_increment_and_a_success_resets(
    pg_dsn: str,
) -> None:
    """Under contention the counter contract holds: N concurrent failures
    each increment (the counter ends at N), and a concurrent success racing
    them leaves the counter at whatever the serialized order decided - never
    losing a recorded failure by SKIPPING a needed reset. The guarded
    statement's re-check under READ COMMITTED evaluates the latest row
    version, so a success that snapshots 0 but locks behind a failure still
    resets it - pinned here against the production statements."""
    schema = f"reset_hotspot_race_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid, rows = await _seed_running_batch(deps, backend, schema, _CONCURRENCY)

        async def _fail(row: JobRow) -> None:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "failed", transaction_conn=conn)

        await asyncio.wait_for(asyncio.gather(*(_fail(row) for row in rows)), timeout=30.0)
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.consecutive_failures == _CONCURRENCY, (
            f"concurrent failures lost increments: expected {_CONCURRENCY}, "
            f"got {batch.consecutive_failures}"
        )
    finally:
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()


async def test_concurrent_all_succeeded_tail_completes_the_batch(pg_dsn: str) -> None:
    """The invariant the guarded reset's de-serialization exposed: with the
    batch's terminal writers no longer queued on the batches row, the LAST
    member's completion attempt can skip behind a premature peer's row
    hold - and a single-shot skip left an all-terminal batch 'active' until
    the leader sweep. The bounded re-arbitration must land it: after every
    member's succeeded hook returns, the batch is 'complete' with no sweep
    in sight, and it got there exactly once (one completed_at)."""
    schema = f"reset_hotspot_tail_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid, rows = await _seed_running_batch(deps, backend, schema, 24)

        async def _succeed(row: JobRow) -> None:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "succeeded", transaction_conn=conn)

        await asyncio.wait_for(asyncio.gather(*(_succeed(row) for row in rows)), timeout=30.0)

        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.status == "complete", (
            f"an all-terminal batch stayed {batch.status!r} after every member's "
            "completion attempt returned - the tail's completion attempt was "
            "skipped on the batches-row lock and never re-arbitrated"
        )
        assert batch.completed_at is not None
        assert await backend.count_batch_non_terminal(bid) == 0
    finally:
        await stack.aclose()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()

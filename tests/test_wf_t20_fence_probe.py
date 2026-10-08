"""T20's FENCE PROBE — the source-terminal fence + the premature-terminal
fence (the spike's 30-sweep probe's shape, PROOF.md §2, translated).

The no-fan-in design has NO join, so the original premature-FIRE fence
has no subject; its successor is the PREMATURE TERMINAL: the run's root
must never finalize while ANY live row exists — not mid-stream (the
source running), not mid-drain (chains in flight). The fence is NOT a
new fire-guard: it is the shipped rows-only derivation itself — the root
finalizes only when the rows are terminal (or resolved-blocked), and a
derivation cannot fire early because it is not a fire at all.

The probe drives the REAL sweep arms + the maintenance leg with NO WORKER
AT ALL after a mid-stream death: nothing may terminalize, 30 passes
running. Also asserted at every pass: the sweep finds NOTHING to fire
(``firable=0``, zero join-wait rows) — the join machinery is inert in
this design.

The mutation drill is the red-first tooth: the maintenance leg with its
LIVENESS GATE MUTATED OUT finalizes the root mid-stream on the same
shape of rows — the fence is load-bearing, not decorative.

Captured: ``.measurements/t20-probe-*.txt``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import RedLog, seed_flow
from tests.test_wf_t20_emit_pins import (
    _LEASE,
    claim_source,
    close_storm_pool,
    committed_children,
    emit_batch,
    make_source,
    page_children,
    read_cursor,
    reclaim_until_claim,
)

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

#: the pages the probe streams (3 pages x 4 chains — at least 3 pages, the
#: ticket's DH1 obligation's shape)
_PROBE_PAGES: list[list[int]] = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]

TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


async def finalize_source(
    pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    flow_id: JobId,
    source_id: JobId,
    view: tuple[JobId, int, int],
) -> None:
    """The source's own terminal (its pager exhausted): the REAL two-tx
    finalize — the cursor's last page is the last checkpoint."""
    worker, attempt, epoch = view
    result = await finalize_node(
        pool,
        wf_sql,
        flow_id=flow_id,
        job_id=source_id,
        step_key="source",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"value": {"pages": len(_PROBE_PAGES)}},
    )
    assert result.applied


async def claim_chain_and_finalize(
    pool: asyncpg.Pool, wf_sql: WorkflowSql, flow_id: JobId, job_id: JobId
) -> None:
    """One chain row's worker pass: the claim (the REAL dispatch-claim's
    fence shape) + the REAL finalize (tx1's fenced terminal mark + the
    ledger terminal + tx2's decrement — no fork: the probe's chains are
    single-step)."""
    worker = JobId(new_uuid())
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f"UPDATE \"{wf_sql.schema}\".jobs SET status = 'running', started_at = now(), "
            "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
            "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
            "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
            "AND deps_pending = 0 RETURNING attempt, claim_epoch",
            job_id,
            worker,
            _LEASE,
        )
        assert rec is not None, f"chain row {job_id} did not claim"
        attempt, epoch = int(rec["attempt"]), int(rec["claim_epoch"])
    result = await finalize_node(
        pool,
        wf_sql,
        flow_id=flow_id,
        job_id=job_id,
        step_key="screen",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"value": {"screened": True}},
    )
    assert result.applied


async def drain_chains(pool: asyncpg.Pool, wf_sql: WorkflowSql, flow_id: JobId) -> int:
    """The worker's chain drain: claim + finalize every pending chain row
    (the probe's chains are single-step 'screen' rows)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT id FROM "{wf_sql.schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'screen' "
            "AND status = 'pending'",
            flow_id,
        )
    for r in rows:
        await claim_chain_and_finalize(pool, wf_sql, flow_id, JobId(r["id"]))
    return len(rows)


async def root_state(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> dict[str, Any]:
    rec = await conn.fetchrow(
        f'SELECT status, finished_at FROM "{schema}".jobs WHERE id = $1', flow_id
    )
    assert rec is not None
    return dict(rec)


async def hard_sweep_pass(
    pool: asyncpg.Pool, wf_sql: WorkflowSql, flow_id: JobId
) -> tuple[int, int]:
    """One hard sweep pass with NO worker: the certified re-derive + fire
    arm, then the maintenance leg. Returns (firable, join_wait_rows)."""
    from taskq.workflows._sweep import sweep_join_rederive

    s = await sweep_join_rederive(pool, wf_sql, batch_size=200)
    async with pool.acquire() as conn:
        await conn.execute(wf_sql.workflow_root_maintain, 200)
        join_waits = int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{wf_sql.schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 "
                "AND metadata->>'blocking_reason' = 'join'",
                flow_id,
            )
        )
    return s.firable, join_waits


async def reclaim_pass(pool: asyncpg.Pool, schema: str) -> None:
    """One REAL certified reclaim + promotion pass (the storm's heal)."""
    from taskq.backend._sweeps import sweep_expired_locks, sweep_scheduled_to_pending

    async with pool.acquire() as conn:
        await sweep_expired_locks(
            conn,
            timedelta(0),
            timedelta(0),
            schema=schema,
            batch_size=100,
            max_retry_backoff=timedelta(milliseconds=10),
        )
        await sweep_scheduled_to_pending(conn, schema=schema)


@pytest.mark.integration
async def test_t20_premature_terminal_fence_30_sweeps(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: Any,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    t20_redlog: RedLog,
) -> None:
    """THE 30-SWEEP PROBE (the spike's shape): a mid-stream death, then 30
    hard sweep passes with NO worker — the root NEVER finalizes (the
    premature-terminal fence holds), the join machinery is inert
    (firable=0, zero join-wait rows — with no fan-in there is nothing to
    fire). The resumed run then completes; the root terminalizes ONLY
    after everything drained, and every row is terminal under it.

    THE MUTATION DRILL (the red-first tooth): the maintenance leg with
    its liveness gate mutated out finalizes the root MID-STREAM on a
    fresh mid-stream flow — the fence's teeth, observed red."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)

    # The claim + page 0's emit run on a DEDICATED pool conn — the
    # spike's driver — whose backend then dies a REAL server-side death
    # (pg_terminate_backend; no graceful path). The lease (250 ms)
    # lapses; the row is the reclaim's subject.
    dsn = module_pg_schema.pg_dsn
    storm_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    try:
        async with storm_pool.acquire() as conn:
            worker, attempt, epoch = await claim_source(conn, wf_schema, source_id)
        await emit_batch(
            storm_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children(_PROBE_PAGES[0], flow_id),
            cursor={"page": 0},
        )
        async with storm_pool.acquire() as conn:
            conn_pid = await conn.fetchval("SELECT pg_backend_pid()")
        killer = await asyncpg.connect(dsn)
        try:
            await killer.execute("SELECT pg_terminate_backend($1)", conn_pid)
        finally:
            await killer.close()
    finally:
        await close_storm_pool(storm_pool)

    # THE MID-STREAM SHAPE: page 0 committed (its chains PENDING — live
    # work), the cursor at the last committed page, the source
    # running-with-a-dead-holder.
    assert await committed_children(wf_conn, wf_schema, source_id) == 4
    assert await read_cursor(wf_conn, wf_schema, source_id) == {"page": 0}

    # NO WORKER: only the certified sweeps + the maintenance legs, hard,
    # 30 passes. NOTHING may terminalize.
    ever_firable = 0
    for _ in range(30):
        firable, join_waits = await hard_sweep_pass(module_pg_pool, wf_sql, flow_id)
        ever_firable += firable
        assert join_waits == 0, (
            "the join machinery must be INERT in the no-fan-in design — a "
            "join-wait row exists (the design was violated)"
        )
        root = await root_state(wf_conn, wf_schema, flow_id)
        assert root["status"] not in TERMINAL, (
            f"THE PREMATURE TERMINAL: the root finalized mid-stream "
            f"({root}) while page 0's chains are live — the rows-only "
            "derivation must never terminalize over live rows"
        )
        await asyncio.sleep(0.02)

    t20_redlog.red(
        "t20-premature-terminal",
        "the maintenance leg WITHOUT its liveness gate (the WHERE's arms "
        "mutated to true — every pass finalizes the root): the root "
        "finalizes mid-stream while page 0's chains are pending live "
        "work — the dispatch fence then strands them forever",
        {"firable_total_30_passes": ever_firable},
    )
    assert ever_firable == 0, "nothing may fire in the no-fan-in design"

    # ── THE MUTATION DRILL (live, on a FRESH mid-stream flow) ─────────
    mutated = wf_sql.workflow_root_maintain.replace(
        "(pf.has_failed AND NOT COALESCE(pf.has_unresolved, false))\n"
        "          -- THE COMPLETED/CANCELLED ROOT: every row terminal (rows 4-5).\n"
        "          OR NOT COALESCE(pf.has_live, true)",
        "true",
    )
    assert mutated != wf_sql.workflow_root_maintain, "the mutation drill did not arm"
    flow2 = await seed_flow(wf_conn, wf_schema)
    src2 = await make_source(wf_conn, wf_schema, flow2, wf_sql)
    w2, a2, e2 = await claim_source(wf_conn, wf_schema, src2)
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow2,
        source_id=src2,
        worker_id=w2,
        attempt=a2,
        claim_epoch=e2,
        children=page_children(_PROBE_PAGES[0], flow2),
        cursor={"page": 0},
    )
    await wf_conn.execute(mutated, 200)
    root2 = await root_state(wf_conn, wf_schema, flow2)
    assert root2["status"] == "succeeded", (
        f"the mutation drill did not reproduce the premature terminal "
        f"(the root reports {root2['status']!r}) — the drill is inert"
    )
    live2 = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending'",
        flow2,
    )
    assert int(live2 or 0) == 4, (
        "the drill's flow must have live rows under the premature terminal "
        "(the mid-stream shape the fence exists to prevent)"
    )
    # THE DRILL'S UNDO: the mutated leg terminalized EVERY running root
    # in the schema (no flow scoping) — the lie is restored to the honest
    # live state before the resume (the always-on G7 assertion reads the
    # roots at teardown; the drill's evidence is the assertion above).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', finished_at = NULL "
        "WHERE id = ANY($1)",
        [flow_id, flow2],
    )

    # ── THE RESUME: the run completes; the root terminals ONLY at the end ─
    await reclaim_pass(module_pg_pool, wf_schema)
    worker, attempt, epoch = await reclaim_until_claim(module_pg_pool, wf_sql, wf_schema, source_id)
    # The remaining pages stream under the resumed claim.
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PROBE_PAGES[1], flow_id),
        cursor={"page": 1},
    )
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PROBE_PAGES[2], flow_id),
        cursor={"page": 2},
    )
    # The source's pager exhausts: the source terminalizes (the
    # source-terminal fence's subject — the run's completion is the
    # rows-only derivation, and the source's own row must be terminal
    # under it).
    await finalize_source(module_pg_pool, wf_sql, flow_id, source_id, (worker, attempt, epoch))

    # The chains drain; THEN (and only then) the root terminals.
    drained = await drain_chains(module_pg_pool, wf_sql, flow_id)
    assert drained == 12, f"12 chains must drain, got {drained}"

    # One maintenance pass with every row terminal: the root finalizes NOW.
    async with module_pg_pool.acquire() as conn:
        await conn.execute(wf_sql.workflow_root_maintain, 200)
    root = await root_state(wf_conn, wf_schema, flow_id)
    assert root["status"] == "succeeded", (
        f"the root must terminalize ONLY after everything drained, got {root!r}"
    )
    assert root["finished_at"] is not None
    live = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 "
        f"AND status NOT IN {TERMINAL}",
        flow_id,
    )
    assert int(live or 0) == 0, "zero live rows at the root's terminal"

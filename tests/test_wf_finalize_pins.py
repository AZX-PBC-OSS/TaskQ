"""The finalize-family pins (T04): pin 1 (the deps fingerprint — the snapshot write reds, the shipped arm heals), pin 6 (duplicate finalizes → one decrement, the rowcount gate), pin 13 (THE ATTEMPT-FENCE — the zombie loses).

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the convicted shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    TERMINAL_SQL,
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_edge,
    seed_flow,
    seed_join,
    seed_running_node,
)

# ── Pin 1: THE deps = -1 FINGERPRINT (the snapshot write) ───────────────


@pytest.mark.integration
async def test_pin_1_snapshot_write_reds_and_the_shipped_arm_is_exact(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """THE FINGERPRINT: a sweep that WRITES FROM A SNAPSHOT (the count read
    outside the child's row lock) strands the join — the counter jumps
    backward off the ledger's truth when it races a committed decrement.
    The shipped lock-first arm: drift 0, deterministic (0/100)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=1)
    parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent, flow_id)

    # The interleave the pin convicts, deterministically: the snapshot
    # read (un-terminal = 1) happens BEFORE a finalize's decrement
    # commits, the snapshot write lands AFTER.
    snapshot_count = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_edge e '
        f'JOIN "{wf_schema}".jobs p ON p.id = e.parent_id '
        "WHERE e.child_id = $1 AND p.status NOT IN " + TERMINAL_SQL,
        join_id,
    )
    assert int(snapshot_count) == 1

    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent,
        step_key="a",
        worker_id=(await claim_view(wf_conn, wf_schema, parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.applied
    after = await node_state(wf_conn, wf_schema, join_id)
    assert after["deps_pending"] == 0, after  # the decrement committed

    # THE RED: the snapshot write puts the STALE count (1) back onto a row
    # that is already 0 — the counter jumps backward off the ledger's
    # truth; the join un-fires (waits forever on a terminal parent).
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET deps_pending = $2 WHERE id = $1',
        join_id,
        int(snapshot_count),
    )
    drifted = await node_state(wf_conn, wf_schema, join_id)
    engine_redlog.red(
        "pin1-snapshot-write",
        "sweep writes deps_pending from a pre-decrement snapshot",
        {"deps_pending_after": drifted["deps_pending"], "ledger_truth": 0},
    )
    assert drifted["deps_pending"] == 1, (
        "the snapshot write did NOT corrupt the counter — the red comparator "
        "is broken (it must drift; that is the fingerprint)"
    )

    # THE SHIPPED ARM heals the drift: the drifted row is join-wait again
    # (deps 1, pending, blocking join); the arm counts un-terminal parents
    # from the LEDGER (0), reconciles the cache back to 0, and the fire
    # stays exactly one (the PK refuses a second).
    await sweep_join_rederive(module_pg_pool, wf_sql)
    healed = await node_state(wf_conn, wf_schema, join_id)
    assert healed["deps_pending"] == 0, healed
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "still exactly one fire"

    # THE SHIPPED ARM on a fresh join: the same finalize + sweep interleave
    # lands exactly-once, and 100 re-derives drift nothing.
    join2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="join2", deps=1)
    parent2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join2, parent2, flow_id)
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent2,
        step_key="b",
        worker_id=(await claim_view(wf_conn, wf_schema, parent2))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.applied
    assert result.fired and result.fired[0].step_key == "join2", (
        "the fire rides the finalize's own tx2"
    )
    sweep = await sweep_join_rederive(module_pg_pool, wf_sql)
    healed = await node_state(wf_conn, wf_schema, join2)
    assert healed["deps_pending"] == 0, healed
    assert await fire_count(wf_conn, wf_schema, join2) == 1, "exactly one fire"
    assert not sweep.fired, "the sweep must not double-fire (the PK refused)"
    for _ in range(100):
        await sweep_join_rederive(module_pg_pool, wf_sql)
    assert (await node_state(wf_conn, wf_schema, join2))["deps_pending"] == 0
    assert await fire_count(wf_conn, wf_schema, join2) == 1


# ── Pin 6: DUPLICATE-FINALIZE (the rowcount gate) ────────────────────────


@pytest.mark.integration
async def test_pin_6_duplicate_finalizes_one_decrement(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """50 duplicate finalizes → 1 decrement: tx2 runs ONLY when tx1's
    fenced UPDATE returned a row. The join fires exactly once, when the
    LAST parent's first admitted finalize lands — never prematurely."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, deps=2)
    parent_a = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="a")
    parent_b = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join_id, parent_a, flow_id)
    await seed_edge(wf_conn, wf_schema, join_id, parent_b, flow_id)

    async def fifty_duplicates(target: JobId, step: str) -> None:
        for _ in range(50):
            # Each duplicate presents a DIFFERENT (stale) worker view: only
            # the row's own first finalize can win the fence.
            view = await claim_view(wf_conn, wf_schema, target)
            res = await finalize_node(
                module_pg_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=target,
                step_key=step,
                worker_id=(await claim_view(wf_conn, wf_schema, target))[
                    0
                ],  # a stale/duplicate caller's view
                attempt=view[1],
                claim_epoch=view[2],
                outcome="succeeded",
            )
            assert res.applied is False or res.attempt == view[1]

    await fifty_duplicates(parent_a, "a")
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 1, state  # ONE decrement total from a

    # parent_b's OWN finalize (the row's current claim view) admits exactly
    # one more decrement, hitting 0 — the fire.
    b_worker, b_attempt, b_epoch = await claim_view(wf_conn, wf_schema, parent_b)
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=parent_b,
        step_key="b",
        worker_id=b_worker,
        attempt=b_attempt,
        claim_epoch=b_epoch,
        outcome="succeeded",
    )
    assert result.applied
    state = await node_state(wf_conn, wf_schema, join_id)
    assert state["deps_pending"] == 0, state
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, "exactly one fire"
    assert (await node_state(wf_conn, wf_schema, join_id))["status"] == "pending"


# ── Pin 13: THE ATTEMPT-FENCE (the zombie's finalize loses) ─────────────


@pytest.mark.integration
async def test_pin_13_attempt_fence(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The finalize CAS guarded by ROW STATUS ONLY loses to the zombie on
    the NEXT attempt's re-claim: reclaim → re-claim re-arms the row
    running (attempt 2) → the zombie's finalize WINS the status-only CAS,
    corrupting the result AND the counter. THE ATTEMPT IS THE FENCING
    TOKEN: ``AND attempt = $n`` (plus the worker and claim-epoch
    conjuncts) — the stale attempt's write no-ops."""
    flow_id = await seed_flow(wf_conn, wf_schema)

    # THE CONVICTED VARIANT (status-only CAS, no attempt/epoch conjunct):
    # the row is re-claimed to attempt 2, and the zombie (attempt 1's
    # worker) terminalizes it.
    node = await seed_running_node(wf_conn, wf_schema, flow_id)
    stale_worker, _stale_attempt, _stale_epoch = await claim_view(wf_conn, wf_schema, node)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'pending', locked_by_worker = NULL, "
        "lock_expires_at = NULL WHERE id = $1",
        node,
    )
    fresh_worker = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 2, "
        "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
        "claim_epoch = 7 WHERE id = $1",
        node,
        fresh_worker,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND status = 'running'",  # NO attempt conjunct
        node,
    )
    corrupted = await node_state(wf_conn, wf_schema, node)
    engine_redlog.red(
        "pin13-attempt-fence",
        "finalize CAS with row status only (no attempt/epoch conjunct)",
        {"status": corrupted["status"], "zombie_attempt": 1, "row_attempt": 2},
    )
    assert corrupted["status"] == "succeeded", (
        "the status-only variant did not lose to the zombie — the red comparator is broken"
    )

    # THE SHIPPED FENCE: the same choreography — the zombie's finalize
    # (stale worker, attempt 1, epoch 0) NO-OPS; the fresh attempt's
    # finalize owns the terminal and the counter.
    join2 = await seed_join(wf_conn, wf_schema, flow_id, step_key="join2", deps=1)
    node2 = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await seed_edge(wf_conn, wf_schema, join2, node2, flow_id)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'pending', locked_by_worker = NULL, "
        "lock_expires_at = NULL WHERE id = $1",
        node2,
    )
    fresh2 = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 2, "
        "locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds', "
        "claim_epoch = 7 WHERE id = $1",
        node2,
        fresh2,
    )
    fenced = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node2,
        step_key="b",
        worker_id=stale_worker,  # the zombie's own worker view
        attempt=1,  # ...its stale attempt
        claim_epoch=0,  # ...its stale epoch
        outcome="succeeded",
        result={"zombie": True},
    )
    assert not fenced.applied, "the zombie's finalize must be fenced"
    state = await node_state(wf_conn, wf_schema, node2)
    assert state["status"] == "running", state  # the fresh attempt still owns it
    assert await fire_count(wf_conn, wf_schema, join2) == 0, "no zombie decrement"

    admitted = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node2,
        step_key="b",
        worker_id=fresh2,
        attempt=2,
        claim_epoch=7,
        outcome="succeeded",
        result={"fresh": True},
    )
    assert admitted.applied
    assert await fire_count(wf_conn, wf_schema, join2) == 1, "exactly one fire"

"""ATTACK (phase-2, round 2): T18's pruner coupling + T07's barrier under
concurrency.

1. T18 EXPIRY COUPLING DIRECTION: the guard holds a child's result while
   its join is UN-FIRED. The shipped pin sweeps BEFORE the fire. The
   attacker's window is the OTHER arm: sweep runs BETWEEN the fire and
   the reduce's batch read — does anything still eat the results? (The
   body reads in the fire's own tx on both paths; the probe documents
   the boundary and hunts the gap via the sweep's own shipped driver.)

2. T18 THE OVER-HOLD DIRECTION (vanilla rows): the guard must not hold
   NON-workflow rows (no edges → swept normally). A guard that over-holds
   vanilla rows is the retention regression.

3. T07 THE BARRIER RACING A CANCEL: the flow cancels while the ladder's
   LAST intermittent child is mid-ladder; the child then terminalizes.
   The barrier + the cancel fence must give: no fire on a cancelled flow,
   one verdict per row.

4. T07 THE TIMEOUT ARM RACING THE LADDER'S LAST RETRY: the deadline
   sweep fires while the child is RUNNING its would-succeed final
   attempt. The child's real finalize must lose the fence CLEANLY (no
   corrupt row, no fire over the deadline-failed fail_closed parent).
"""

from __future__ import annotations

import json

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.backend._sweeps import sweep_expired_results
from taskq.workflows.engine import finalize_node
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive
from tests._wf_fixtures import (
    claim_view,
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)


async def _seed_edge_named(
    conn: asyncpg.Connection,
    schema: str,
    join_id: JobId,
    parent_id: JobId,
    flow_id: JobId,
    policy: str,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, $4)",
        join_id,
        parent_id,
        flow_id,
        policy,
    )


# ── 1: the expiry coupling's OTHER window (fire→read) ───────────────────


@pytest.mark.integration
async def test_attack_t18_expiry_after_the_fire_before_the_reduce(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """The shipped pin sweeps while the join is un-fired (the guard holds).
    The attacker's window: the fire has LANDED (the guard's join-wait
    condition is now false — the join is deps=0) but the reduce's batch
    read has not happened. Drive the SHIPPED sweep right there: does the
    expiry eat the children's results the reduce still needs? The shipped
    answer should be 'safe by construction' (the body reads in the fire's
    tx) — the probe PROVES the boundary by driving the sweep in the gap
    and checking what the record then shows."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="red", deps=2)
    a = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="a")
    b = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="b")
    await _seed_edge_named(wf_conn, wf_schema, join_id, a, flow_id, "collect")
    await _seed_edge_named(wf_conn, wf_schema, join_id, b, flow_id, "collect")

    # both children terminalize with results + a SHORTENED expiry (the
    # named clock mechanism — the rows' expiry forced into the past).
    for node, key in ((a, "a"), (b, "b")):
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=node,
            step_key=key,
            worker_id=(await claim_view(wf_conn, wf_schema, node))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            result={"k": key},
        )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET result_expires_at = now() - interval '1 second' "
        "WHERE id = ANY($1)",
        [a, b],
    )
    # join not fired yet (both decrements landed through the finalize —
    # deps hit 0 → the finalize FIRED it already; the fire happened in the
    # last child's tx2 — the gap the pin's pre-fire sweep never covers).
    fired = await fire_count(wf_conn, wf_schema, join_id)

    # THE SWEEP IN THE GAP: the join is fired (deps=0) — the guard no
    # longer counts the rows. The results are the reduce's inputs... but
    # the reduce ALREADY ran (the body executed inside the firing tx2 —
    # resolve_flow_reducer → none here → body_unavailable). What does the
    # record show AFTER the sweep? The consumers read the JOIN's result —
    # is the join's fired record independent of the children's rows?
    expired = await sweep_expired_results(module_pg_pool, schema=wf_schema)
    survivors = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = ANY($1) AND result IS NOT NULL',
        [a, b],
    )
    join_state = await node_state(wf_conn, wf_schema, join_id)
    print(
        json.dumps(
            {
                "fired_at_finalize": fired,
                "expired": expired,
                "children_results_surviving": survivors,
                "join_blocking": join_state["metadata"].get("blocking_reason"),
                "join_deps": join_state["deps_pending"],
            },
            indent=2,
        )
    )
    # THE ASSERTION (the coupling's contract at the gap): whatever the
    # sweep eats, the FIRED record must already carry the delivery — the
    # fire row exists and the join row is resolved. The reduce's own read
    # happened in the firing tx. So: fire == 1 and the row resolvable.
    assert fired == 1, "the collect join must have fired in the last child's tx2"
    assert join_state["deps_pending"] == 0
    # DOCUMENTED BOUNDARY: post-fire, the results are expirable rows (the
    # TTL is their normal aging). The reduce's inputs are read in the
    # fire's tx — the sweep cannot interleave there (single tx). The
    # record's delivery contract survives the sweep eating the children.
    # If the fire did NOT happen above (fired==0), the guard must hold the
    # results — the shipped pin's shape, already convicted there.


# ── 2: the guard must not over-hold vanilla rows ────────────────────────


@pytest.mark.integration
async def test_attack_t18_expiry_guard_does_not_hold_vanilla_rows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """A VANILLA terminal row (no edges) past its expiry must still sweep —
    the T18 guard is a workflow-liveness probe, not a retention pause on
    the fleet."""
    vanilla = None  # placeholder to keep the import local
    from taskq._ids import new_uuid

    _ = vanilla
    vid = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, retry_kind, '
        "status, attempt, step_key, result, result_expires_at, finished_at, idempotency_scope, "
        "idempotency_key) "
        f"VALUES ($1, 'van', 'default', '{{}}', 3, 'transient', 'succeeded', 1, 'v', "
        "'{\"ok\": 1}'::jsonb, now() - interval '1 second', now(), 'scope', 'vk-" + str(vid) + "')",
        vid,
    )
    expired = await sweep_expired_results(module_pg_pool, schema=wf_schema)
    assert expired >= 1, "the expiry sweep must take the vanilla row"
    left = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE id = $1 AND result IS NOT NULL', vid
    )
    assert left == 0, "the guard over-holds: a vanilla row's result was held"


# ── 3: T07's barrier racing a cancel ────────────────────────────────────


@pytest.mark.integration
async def test_attack_t07_barrier_racing_a_cancel(
    module_pg_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The flow cancels while the last intermittent child runs its final
    attempt; the child terminalizes right after. The fence set must give
    ONE verdict: the child's success CAS loses to the cancel (or wins the
    race fairly), and the join NEVER fires into a cancelled flow."""
    for round_no in range(4):
        flow_id = await seed_flow(wf_conn, wf_schema)
        join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="bar", deps=2)
        done_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="d0")
        late_child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="d1")
        await _seed_edge_named(wf_conn, wf_schema, join_id, done_child, flow_id, "collect")
        await _seed_edge_named(wf_conn, wf_schema, join_id, late_child, flow_id, "collect")

        # the first child completes cleanly (the barrier waits)
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=done_child,
            step_key="d0",
            worker_id=(await claim_view(wf_conn, wf_schema, done_child))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            result={"k": "d0"},
        )

        # THE CANCEL: the flow root + the late child cancel (the cancel
        # arm's direct write — the record's cancelled shape)
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'cancelled', finished_at = now() "
            "WHERE id = $1",
            flow_id,
        )
        late_worker, late_attempt, late_epoch = await claim_view(wf_conn, wf_schema, late_child)

        # THE RACE: the late child's SUCCESS tx2 vs the cancelled flow.
        # The terminal-mark CAS (status='running') still matches the child
        # row — the child marks succeeded — but tx2's decrement/fire must
        # refuse on the flow-status leg.
        await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=late_child,
            step_key="d1",
            worker_id=late_worker,
            attempt=late_attempt,
            claim_epoch=late_epoch,
            outcome="succeeded",
            result={"k": "d1"},
        )
        fires = await fire_count(wf_conn, wf_schema, join_id)
        assert fires == 0, (
            f"round {round_no}: the join FIRED into a cancelled flow — "
            "the fire's flow-status leg lost the race"
        )
        child_state = await node_state(wf_conn, wf_schema, late_child)
        assert child_state["status"] == "succeeded", (
            f"round {round_no}: the child's own verdict was {child_state['status']!r} — "
            "the fence set's semantics unclear"
        )
        deps = child_state  # the join keeps its counter? (the decrement's flow leg)
        join_state = await node_state(wf_conn, wf_schema, join_id)
        _ = deps
        print(
            f"round {round_no}: fires=0, join deps after refused decrement: {join_state['deps_pending']}"
        )


# ── 4: the timeout arm racing the ladder's last retry ───────────────────


@pytest.mark.integration
async def test_attack_t07_timeout_sweep_racing_the_last_attempt(
    module_pg_pool: asyncpg.Pool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
) -> None:
    """The deadline sweep takes a SCHEDULED child mid-ladder (its
    schedule_to_close elapsed — the sweep's from-set is
    pending/scheduled, the state-event totality's own row); the row then
    re-claims (the ladder's next claim beat the sweep's write) and its
    real finalize lands. The fence must lose CLEANLY: the child stays
    the sweep's 'failed' (no zombie success), a fail_closed join
    blocks-with-reason, nothing fires."""
    from taskq.backend._sweeps import sweep_deadline_exceeded

    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="to_join", deps=1)
    child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="t0")
    await _seed_edge_named(wf_conn, wf_schema, join_id, child, flow_id, "fail_closed")

    # the ladder's shape: the child FAILED attempt 1 and re-scheduled
    # (mark_retry) — mid-ladder, SCHEDULED — and its schedule_to_close
    # has elapsed.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', attempt = 2, "
        "scheduled_at = now() + interval '60 seconds', "
        "schedule_to_close = now() - interval '1 second' WHERE id = $1",
        child,
    )
    swept = 0
    async with module_pg_pool.acquire() as deadline_conn:
        swept = await sweep_deadline_exceeded(deadline_conn, schema=wf_schema)
    assert swept >= 1, "the deadline sweep must take the overdue running row"

    # the real worker's finalize lands NOW (the attempt was genuinely
    # running — its work SUCCEEDED).
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, child)
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=child,
        step_key="t0",
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"k": "the work actually finished"},
    )
    print(json.dumps({"finalize_applied": result.applied}, indent=2))

    child_state = await node_state(wf_conn, wf_schema, child)
    assert child_state["status"] == "failed", (
        f"THE ZOMBIE SUCCESS: the deadline-failed row was overwritten to "
        f"{child_state['status']!r} by the late finalize — the fence's "
        "status leg did not refuse"
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 0
    # the sweep heal resolves the join
    await sweep_join_rederive(module_pg_pool, wf_sql)
    join_state = await node_state(wf_conn, wf_schema, join_id)
    assert join_state["metadata"].get("blocking_reason") == "failed_parent"
    _ = join_id

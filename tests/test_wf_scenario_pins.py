"""The two maintainer-ordered scenario pins (T07) — MAP-RETRY-BARRIER and
JOIN-FAILURE-POLICY — the integration tier, the real PG lane.

MAP-RETRY-BARRIER: a list map over N=8 children; K=3 fail intermittently
(each failing its first M attempts - DISTINCT M per child: 1, 2, 3 - then
succeeding via the normal retry ladder); the other N-K succeed first try.
Five assertions, each its own check: (a) NO EARLY FIRE, (b)
ATTEMPT-FAILURE != NODE-FAILURE, (c) RETRY ISOLATION, (d) THE BARRIER, (e)
THE COLLECT IS THE SUCCESSFUL PAYLOADS. The red-first fingerprints (the
convicted shapes, each pinned by its own drill): the join firing on
first-success-count mismatch (counting ATTEMPTS instead of CHILDREN); the
over-eager parent-fail; the retry-storm contagion.

JOIN-FAILURE-POLICY: the duality - TERMINALITY x POLICY. A join/collect
fails ONLY on a child's TERMINAL state (the ladder exhausted, a terminal
cancel, or the join's own timeout) - NEVER on an attempt-failure;
required = fail-closed composing with T06's peer-cascade; maybe =
proceed-with-subset SURFACED (the envelope names the policy that
absorbed it - it must not lie); the timeout arm rides the node-level
deadline (the estate's deadline-exceeded sweep - this series gives the
join NO separate timer), with the bound outliving the ladder's worst
case, computed in the pin.

Driven through the REAL engine (the ladder simulated at the vanilla
mark_retry/claim boundary - the same shapes the proofs' harnesses used).
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import finalize_node
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import sweep_join_rederive
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    fire_count,
    node_state,
    seed_flow,
    seed_join,
    seed_running_node,
)

N_CHILDREN = 8
#: The intermittent children: index -> the M attempts that fail first.
INTERMITTENT: dict[int, int] = {0: 1, 3: 2, 6: 3}


# ── the ladder harness ──────────────────────────────────────────────────


async def _seed_map_with_indexes(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    flow_id: JobId,
    *,
    policy: str,
    prefix: str = "c",
) -> tuple[JobId, list[JobId]]:
    """The N-child list map: children carry map_index (the gather's child
    order); the join's edges declare *policy*."""
    join_id = await seed_join(wf_conn, wf_schema, flow_id, step_key="map_join", deps=N_CHILDREN)
    children: list[JobId] = []
    for i in range(N_CHILDREN):
        child = await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"{prefix}{i}")
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET map_index = $2 WHERE id = $1', child, i
        )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
            "VALUES ($1, $2, $3, $4)",
            join_id,
            child,
            flow_id,
            policy,
        )
        children.append(child)
    return join_id, children


async def _flow_of(wf_conn: asyncpg.Connection, wf_schema: str, child: JobId) -> JobId:
    return JobId(
        await wf_conn.fetchval(
            f"SELECT (metadata->>'flow_id')::uuid FROM \"{wf_schema}\".jobs WHERE id = $1",
            child,
        )
    )


async def _fail_attempt(
    wf_conn: asyncpg.Connection, wf_schema: str, child: JobId, *, attempt: int, err: str
) -> None:
    """One ladder ATTEMPT failure (the vanilla mark_retry shape): the
    failed attempt lands on the ledger, the row goes back to scheduled —
    NO workflow terminal (P3 decision 7)."""
    meta = await wf_conn.fetchrow(
        f"SELECT (metadata->>'flow_id')::uuid AS flow_id, step_key, map_index "
        f'FROM "{wf_schema}".jobs WHERE id = $1',
        child,
    )
    assert meta is not None
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger '
        "(id, flow_id, job_id, step_key, map_index, attempt, status, error_class, error_message) "
        "VALUES ($1, $2, $3, $4, $5, $6, 'failed', 'ValueError', $7)",
        new_uuid(),
        meta["flow_id"],
        child,
        meta["step_key"],
        meta["map_index"],
        attempt,
        err,
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', "
        "scheduled_at = now() + interval '1 millisecond' WHERE id = $1",
        child,
    )


async def _reclaim(
    wf_conn: asyncpg.Connection, wf_schema: str, child: JobId, *, attempt: int
) -> None:
    """The ladder's next claim: the promoted row re-claims (a new worker
    view, attempt advanced) — the SAME row (retry-in-place)."""
    worker = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = $2, "
        "locked_by_worker = $3, claim_epoch = claim_epoch + 1, "
        "lock_expires_at = now() + interval '90 seconds' WHERE id = $1",
        child,
        attempt,
        worker,
    )


async def _succeed_now(
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    child: JobId,
    step_key: str,
) -> None:
    """The child's terminal SUCCESS from its CURRENT claim view (the
    fence reads the row's own attempt/epoch/worker — always consistent)."""
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, child)
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=await _flow_of(wf_conn, wf_schema, child),
        job_id=child,
        step_key=step_key,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="succeeded",
        result={"child": step_key},
    )


async def _fail_terminal_now(
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    child: JobId,
    step_key: str,
    err: str,
) -> None:
    """The child's terminal FAILURE from its CURRENT claim view — the
    ladder EXHAUSTED (the terminal state the duality's fail arm reads)."""
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, child)
    await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=await _flow_of(wf_conn, wf_schema, child),
        job_id=child,
        step_key=step_key,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        outcome="failed",
        error_class="ValueError",
        error_message=err,
    )


async def _run_child_ladder(
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    child: JobId,
    step_key: str,
    *,
    m: int,
    first_attempt: int = 1,
) -> None:
    """The child's ladder: attempts first_attempt..m fail (each: the
    ledger row + the mark_retry + the re-claim), then the success — the
    probes fire BETWEEN the attempts (the barrier's observation windows)."""
    for attempt in range(first_attempt, m + 1):
        await _fail_attempt(wf_conn, wf_schema, child, attempt=attempt, err=f"boom-{attempt}")
        await _reclaim(wf_conn, wf_schema, child, attempt=attempt + 1)
    await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, child, step_key)


# ── MAP-RETRY-BARRIER ───────────────────────────────────────────────────


@pytest.mark.integration
async def test_map_retry_barrier(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """N=8; K=3 intermittent (M = 1, 2, 3); the five assertions, each its
    own check. The convicted shapes are the drills inside: the
    attempt-counting join (fires K attempts early), the over-eager
    parent-fail, the retry-storm contagion."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map_with_indexes(wf_conn, wf_schema, flow_id, policy="collect")

    # (a) NO EARLY FIRE — after the FIRST child's first failure the
    # join/parent is still pending with deps_pending > 0 (the join counts
    # CHILDREN, not attempts): the first failure + the ledger's failed
    # attempt row must not move the join at all.
    await _fail_attempt(wf_conn, wf_schema, children[0], attempt=1, err="boom-1")
    state_after_first_failure = await node_state(wf_conn, wf_schema, join_id)
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    child_state = await node_state(wf_conn, wf_schema, children[0])
    propagation_redlog.red(
        "map-retry-barrier-(a)-early-fire",
        "the attempt-counting join — fires on the first-success-count "
        "mismatch (counting ATTEMPTS instead of CHILDREN)",
        state_after_first_failure,
    )
    assert state_after_first_failure["status"] == "pending"
    assert state_after_first_failure["deps_pending"] == N_CHILDREN, (
        "no decrement mid-ladder: the join counts children, not attempts"
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 0, (
        "the attempt-counting variant fires here (the conviction)"
    )

    # (b) ATTEMPT-FAILURE != NODE-FAILURE — the parent does NOT fail when a
    # child fails an attempt (the retry ladder owns it; the asymmetry
    # doctrine's over-rejection applied to maps): the flow runs, the child
    # is NOT terminal — its ladder owns the retry (scheduled, pending its
    # re-claim).
    assert flow_state["status"] == "running", (
        "the over-eager parent-fail: one child's attempt-failure must not "
        "propagate as a map failure"
    )
    assert child_state["status"] == "scheduled", "the ladder owns the retry"

    # The ladder re-claims the row (attempt 2 — the failure was attempt 1).
    await _reclaim(wf_conn, wf_schema, children[0], attempt=2)

    # (c) RETRY ISOLATION — run c0's ladder to success; the SIBLINGS'
    # rows must be untouched (their attempt counts AND outputs unchanged —
    # a retry must not restart, re-enqueue, or re-run its siblings).
    sibling_before = await node_state(wf_conn, wf_schema, children[7])
    await _run_child_ladder(
        module_pg_pool,
        wf_sql,
        wf_conn,
        wf_schema,
        children[0],
        "c0",
        m=INTERMITTENT[0],
        first_attempt=2,
    )
    sibling_after = await node_state(wf_conn, wf_schema, children[7])
    propagation_redlog.red(
        "map-retry-barrier-(c)-contagion",
        "the retry-storm contagion — a child's retry re-triggers the map body for the WHOLE list",
        {"before": sibling_before, "after": sibling_after},
    )
    assert sibling_after == sibling_before, (
        "the retried child's retries must not touch its siblings"
    )
    sib_ledger = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1',
        children[7],
    )
    assert sib_ledger == 0, "a sibling's ledger must not grow from another's retry"

    # The observed sibling is itself a straight child: it succeeds first
    # try (its isolation is the assert above — untouched until now).
    await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, children[7], "c7")

    # The first successful child decremented the join (children, not
    # attempts): the join is still waiting on the mid-ladder children.
    state_after_c0 = await node_state(wf_conn, wf_schema, join_id)
    assert state_after_c0["deps_pending"] == N_CHILDREN - 2, (
        "two children terminalized (c0 through its retries, c7 first try) "
        "— the counter moved TWICE, never per attempt"
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 0

    # The two remaining intermittent children run their ladders; the
    # straight children succeed first try, interleaved.
    for i in (1, 2):  # the straight ones
        await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, children[i], f"c{i}")
    await _run_child_ladder(
        module_pg_pool, wf_sql, wf_conn, wf_schema, children[3], "c3", m=INTERMITTENT[3]
    )
    for i in (4, 5):
        await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, children[i], f"c{i}")
    assert await fire_count(wf_conn, wf_schema, join_id) == 0, (
        "the barrier holds while any child is still mid-ladder"
    )

    # (d) THE BARRIER — c6 (M=3, the LAST intermittent child) runs its
    # ladder; once it succeeds through its retries, and ONLY then, the
    # join fires and the gather collects ALL N results in child order.
    await _run_child_ladder(
        module_pg_pool, wf_sql, wf_conn, wf_schema, children[6], "c6", m=INTERMITTENT[6]
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, (
        "the join fires once the LAST intermittent child succeeds"
    )
    # THE GATHER (child order): the join's parents' results, ORDER BY
    # map_index — the reduce's batch read shape.
    gather_rows = await wf_conn.fetch(
        f"""
        SELECT c.map_index, c.step_key, c.result
        FROM "{wf_schema}".wf_edge e
        JOIN "{wf_schema}".jobs c ON c.id = e.parent_id
        WHERE e.child_id = $1
        ORDER BY c.map_index
        """,
        join_id,
    )
    assert [r["step_key"] for r in gather_rows] == [f"c{i}" for i in range(N_CHILDREN)], (
        "ALL N results, in child order"
    )
    assert all(r["result"] is not None for r in gather_rows)

    # (e) THE COLLECT IS THE SUCCESSFUL PAYLOADS — the failed attempts'
    # errors appear nowhere in the join's record (at most in the
    # ledger/debug surface): the join row carries NO failure item (nobody
    # failed TERMINALLY) and no error text.
    join_state = await node_state(wf_conn, wf_schema, join_id)
    propagation_redlog.red(
        "map-retry-barrier-(e)-errors-in-collect",
        "the failed attempts' errors leaking into the join's result",
        {"failures": join_state["metadata"].get("failures")},
    )
    assert not join_state["metadata"].get("failures"), (
        "attempt-failures are not terminal failures: the collect carries "
        "the successful payloads only"
    )
    ledger_errors = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND status = 'failed'",
        flow_id,
    )
    assert ledger_errors == sum(INTERMITTENT.values()), (
        "the failed attempts live in the LEDGER (the debug surface), all "
        f"{sum(INTERMITTENT.values())} of them"
    )
    _ = flow_id


# ── JOIN-FAILURE-POLICY (the duality) ───────────────────────────────────


@pytest.mark.integration
async def test_join_failure_policy_terminality_one_number_apart(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """A. TERMINALITY: the barrier's companion cases, ONE NUMBER APART —
    the child failing M-1 attempts then succeeding MUST still collect;
    the SAME setup with the ladder exhausted (M+1 failures) MUST fail the
    collect. The pin says which number differs."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    collect_join, collect_children = await _seed_map_with_indexes(
        wf_conn, wf_schema, flow_id, policy="collect"
    )
    # The M-1-then-succeed case (c0 fails one attempt, succeeds on 2): the
    # collect fires when the map completes.
    await _fail_attempt(wf_conn, wf_schema, collect_children[0], attempt=1, err="boom-1")
    await _reclaim(wf_conn, wf_schema, collect_children[0], attempt=2)
    await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, collect_children[0], "c0")
    assert await fire_count(wf_conn, wf_schema, collect_join) == 0, (
        "M-1 attempt failures do not fail the collect (terminality): the "
        "map is not complete yet — and nothing failed"
    )
    # Complete the collect map: the join FIRES (the M-1 child collected).
    for i in range(1, N_CHILDREN):
        await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, collect_children[i], f"c{i}")
    assert await fire_count(wf_conn, wf_schema, collect_join) == 1, (
        "the child that failed M-1 attempts then succeeded MUST collect"
    )

    # THE M+1 case, adjacent (a second map under the SAME flow): the
    # child exhausts its ladder — the REQUIRED variant blocks + cascades.
    required_join, required_children = await _seed_map_with_indexes(
        wf_conn, wf_schema, flow_id, policy="fail_closed", prefix="r"
    )
    await _fail_attempt(wf_conn, wf_schema, required_children[0], attempt=1, err="boom-1")
    await _reclaim(wf_conn, wf_schema, required_children[0], attempt=2)
    await _fail_attempt(wf_conn, wf_schema, required_children[0], attempt=2, err="boom-2")
    await _reclaim(wf_conn, wf_schema, required_children[0], attempt=3)
    await _fail_terminal_now(
        module_pg_pool, wf_sql, wf_conn, wf_schema, required_children[0], "r0", "boom-3"
    )
    propagation_redlog.red(
        "join-failure-policy-terminality",
        "the attempt-counting join — the M+1 case treated as M-1 (the "
        "collect fires where the policy says fail-closed)",
        {"required_join_fires": await fire_count(wf_conn, wf_schema, required_join)},
    )
    # B. REQUIRED fan-in = FAIL-CLOSED, composing with T06's rule: the
    # join blocks, the peers peer-cancel, the flow fails.
    assert await fire_count(wf_conn, wf_schema, required_join) == 0, (
        "a required child's terminal failure FAILS THE JOIN CLOSED"
    )
    req_state = await node_state(wf_conn, wf_schema, required_join)
    assert req_state["metadata"].get("blocking_reason") == "failed_parent"
    peer = await wf_conn.fetchrow(
        f'SELECT status, error_class, metadata FROM "{wf_schema}".jobs WHERE id = $1',
        required_children[1],
    )
    assert peer is not None and peer["status"] == "cancelled", (
        "T06's decided rule: a required-join failure peer-cancels the running peers"
    )
    peer_meta = (
        peer["metadata"]
        if isinstance(peer["metadata"], dict)
        else json.loads(peer["metadata"] or "{}")
    )
    assert peer_meta.get("peer_cancel", {}).get("by") == "peer_failure"
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "failed", "the flow fails closed"
    _ = collect_join


@pytest.mark.integration
async def test_join_failure_policy_maybe_surfaced_and_the_zero_collect(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """C. MAYBE semantics: a maybe child's terminal failure does NOT fail
    the collect — the collect proceeds with the successful subset; the
    failure is SURFACED (the envelope marks the absent child + its
    terminal error, naming the policy that ran); the parent SUCCEEDS.
    THE EDGE CASE N5: a maybe edge that is the join's ONLY parent — the
    collect of ZERO -> the parent SUCCEEDS with the empty result + the
    envelope carrying the absence. The silently-failing variant (a
    zero-collect that wedges or fails the parent) reds."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    join_id, children = await _seed_map_with_indexes(wf_conn, wf_schema, flow_id, policy="maybe")
    # c0 succeeds; c1 (the maybe child) TERMINAL-fails.
    await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, children[0], "c0")
    await _fail_terminal_now(
        module_pg_pool,
        wf_sql,
        wf_conn,
        wf_schema,
        children[1],
        "c1",
        "the maybe child's terminal error",
    )
    assert await fire_count(wf_conn, wf_schema, join_id) == 0, (
        "the third child still pending: no fire yet"
    )
    # The remaining children succeed -> the join fires with the subset.
    for i in range(2, N_CHILDREN):
        await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, children[i], f"c{i}")
    assert await fire_count(wf_conn, wf_schema, join_id) == 1, (
        "the maybe child's terminal failure does NOT fail the collect — "
        "it proceeds with the successful subset"
    )
    join_state = await node_state(wf_conn, wf_schema, join_id)
    failures: list[dict[str, Any]] = join_state["metadata"].get("failures") or []
    propagation_redlog.red(
        "join-failure-policy-maybe-silent",
        "the maybe child's failure SILENTLY DROPPED (an un-surfaced "
        "absence — the collect succeeds and the envelope says NOTHING)",
        {"failures": failures},
    )
    assert len(failures) == 1, "the absence must be SURFACED"
    assert failures[0]["node_key"] == "c1"
    assert failures[0]["policy"] == "maybe", "the envelope must not lie about which policy ran"
    assert failures[0]["error"]["error_class"] == "ValueError"
    flow_state = await node_state(wf_conn, wf_schema, flow_id)
    assert flow_state["status"] == "running", "the parent SUCCEEDS (never failed)"

    # N5: the maybe edge that is the join's ONLY parent — the collect of
    # ZERO is a DEFINED OUTCOME: the parent SUCCEEDS with the empty
    # result + the envelope carrying the absence.
    flow2 = await seed_flow(wf_conn, wf_schema)
    only_join = await seed_join(wf_conn, wf_schema, flow2, step_key="only_join", deps=1)
    maybe_child = await seed_running_node(wf_conn, wf_schema, flow2, step_key="m0")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'maybe')",
        only_join,
        maybe_child,
        flow2,
    )
    await _fail_terminal_now(
        module_pg_pool,
        wf_sql,
        wf_conn,
        wf_schema,
        maybe_child,
        "m0",
        "the only maybe child terminals",
    )
    assert await fire_count(wf_conn, wf_schema, only_join) == 1, (
        "the zero-collect is a defined outcome: the join FIRES (never "
        "wedges, never fails the parent)"
    )
    only_state = await node_state(wf_conn, wf_schema, only_join)
    only_failures: list[dict[str, Any]] = only_state["metadata"].get("failures") or []
    assert len(only_failures) == 1 and only_failures[0]["policy"] == "maybe", (
        "the envelope carries the absence on the empty collect"
    )


@pytest.mark.integration
async def test_join_failure_policy_timeout_arm_both_policies(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    propagation_redlog: RedLog,
) -> None:
    """D. THE TIMEOUT ARM: a child that never reaches terminality inside
    the join's bound — the bound is the NODE-LEVEL deadline
    (schedule_to_close; the estate's deadline-exceeded sweep — this
    series gives the join NO separate timer) — resolves per the edge's
    policy: required -> fail; maybe -> proceed-without. One test, both
    arms. THE BOUND OUTLIVES THE LADDER'S WORST CASE (computed here): the
    declared deadline must be >= the ladder's max backoff sum + the
    attempts' own runtime — a tighter deadline racing the retry ladder is
    the dragon (the timeout would fail a would-succeed child)."""
    # THE COMPUTED BOUND: the retry curve (retry.py: base x 2^(attempt-1),
    # capped at cap, with jitter) — the ladder's worst-case wall clock for
    # M attempts is the SUM of its delays + each attempt's own runtime
    # (start_to_close). A join's child carrying a deadline below this sum
    # is the convicted race (the documented interaction, named in the
    # docs): the deadline must be SET ABOVE it.
    retry_base_s = 5.0
    retry_cap_s = 3600.0
    max_attempts_declared = 3
    start_to_close_s = 90.0
    ladder_worst_case_s = (
        sum(min(retry_base_s * 2 ** (k - 1), retry_cap_s) for k in range(1, max_attempts_declared))
        + max_attempts_declared * start_to_close_s
    )
    assert ladder_worst_case_s > 0
    # THE PIN'S OPERATIONAL RULE: the join's children carry
    # schedule_to_close >= ladder_worst_case_s (+ the join's own margin).
    # The estate's deadline sweep does the rest: a child whose deadline
    # elapses NEVER wedges the join.

    # ── THE REQUIRED ARM: the deadline sweep terminal-fails the stuck
    # child -> the sweep's rederive resolves the fail_closed join (T06's
    # heal): blocked-with-reason, never fired.
    flow_id = await seed_flow(wf_conn, wf_schema)
    req_join, req_children = await _seed_map_with_indexes(
        wf_conn, wf_schema, flow_id, policy="fail_closed"
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', scheduled_at = now(), "
        "schedule_to_close = now() - interval '1 second' WHERE id = $1",
        req_children[0],
    )
    from taskq.backend._sweeps import sweep_deadline_exceeded

    swept = 0
    async with module_pg_pool.acquire() as deadline_conn:
        swept = await sweep_deadline_exceeded(deadline_conn, schema=wf_schema)
    assert swept >= 1, "the deadline sweep must take the overdue child"
    summary = await sweep_join_rederive(module_pg_pool, wf_sql)
    propagation_redlog.red(
        "join-failure-policy-timeout-required",
        "the timeout arm wedged: the deadline-failed child leaves the "
        "join in join-wait (a join with no timer of its own, waiting on a "
        "child that can never terminalize)",
        {
            "swept": swept,
            "blocked_required": summary.blocked_required,
            "fires": await fire_count(wf_conn, wf_schema, req_join),
        },
    )
    assert await fire_count(wf_conn, wf_schema, req_join) == 0
    req_state = await node_state(wf_conn, wf_schema, req_join)
    assert req_state["metadata"].get("blocking_reason") == "failed_parent", (
        "required x timeout = fail-closed (the deadline-failed child IS a terminal failure)"
    )

    # ── THE MAYBE ARM: the deadline-failed maybe child -> proceed-without.
    flow2 = await seed_flow(wf_conn, wf_schema)
    may_join, may_children = await _seed_map_with_indexes(wf_conn, wf_schema, flow2, policy="maybe")
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'scheduled', scheduled_at = now(), "
        "schedule_to_close = now() - interval '1 second' WHERE id = $1",
        may_children[0],
    )
    async with module_pg_pool.acquire() as deadline_conn:
        await sweep_deadline_exceeded(deadline_conn, schema=wf_schema)
    # The remaining children succeed; the recount then resolves the join
    # (the deadline-failed maybe child's side: terminal = resolved).
    for i in range(1, N_CHILDREN):
        await _succeed_now(module_pg_pool, wf_sql, wf_conn, wf_schema, may_children[i], f"c{i}")
    await sweep_join_rederive(module_pg_pool, wf_sql)
    assert await fire_count(wf_conn, wf_schema, may_join) == 1, (
        "maybe x timeout = proceed-without: the join fires (the ledger "
        "records the deadline-failed child as the terminal failure it is)"
    )
    may_state = await node_state(wf_conn, wf_schema, may_join)
    assert may_state["metadata"].get("blocking_reason") != "failed_parent", (
        "the maybe join never blocks on the timeout arm (a body_unavailable "
        "stamp is the R2-2 loudness record — the sweep fired it with no "
        "registered body, the delivery contract held — never a policy block)"
    )
    assert may_state["deps_pending"] == 0, "the maybe join fired: resolved"

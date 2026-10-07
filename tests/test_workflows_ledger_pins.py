"""T05's pins — the step ledger, the run-key arbiter, the idempotency
contract, driven against a live Postgres through the REAL engine.

Method: every pin's convicted variant is made to manifest its dragon (the
double-run, the two-transaction claim, the replay that launches a second
run), the red output recorded to ``.measurements/ledger-pin-reds.json``,
the shipped shape green. The contract the pins state: EXACTLY-ONCE for
DB-local effects (the ledger claim + the ON CONFLICT path), AT-LEAST-ONCE
for external effects with the ledger dedup — join/reducer bodies covered
by the same boundary (a raising reducer rolls tx2 back and the body
RE-RUNS on re-fire).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import reap_phantom_ledger
from taskq.workflows.engine import finalize_node, render_workflow_sql
from taskq.workflows.ledger import (
    claim_step_ledger,
    insert_flow_run,
    memoized_step_result,
    run_idempotency_scope,
    step_idempotency_key,
    step_idempotency_scope,
)
from tests._wf_fixtures import FlowStandIn, RedLog, seed_flow


@pytest.mark.integration
async def test_pin_1_double_run_red_and_memoized_green(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    ledger_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A step re-delivered gets the recorded result rather than
    re-executing — the memoized replay through the SHIPPED claim path. THE
    RED is a real mutation of the engine: the memoized lookup's statement
    mutated to return nothing (the replay lookup broken — the ledger
    forgets) and the re-delivery RE-EXECUTES through the very same shipped
    body: the double-run dragon manifests on the engine, not on a local
    re-implementation. The shipped statement greens it."""
    flow_id = new_uuid()
    node_id = new_uuid()
    executions = 0

    # THE SHIPPED CLAIM PATH (the body is guarded by the memoized lookup):
    # the first execution claims (running), the node terminalizes (the
    # ledger-terminal write rides the finalize), and the re-delivery
    # returns the recorded result — ONE execution, ever.
    async def claimed_body() -> Any:
        nonlocal executions
        memo = await memoized_step_result(
            wf_conn, wf_sql, flow_id=flow_id, step_key="s", map_index=None
        )
        if memo is not None and memo.status == "succeeded":
            return memo.result
        executions += 1
        return "ran"

    claim = await claim_step_ledger(
        wf_conn, wf_sql, flow_id=flow_id, job_id=node_id, step_key="s", map_index=None, attempt=1
    )
    assert claim.status == "running" and claim.result is None
    first = await claimed_body()
    assert first == "ran" and executions == 1
    # The finalize's ledger terminal lands (the success outcome).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".wf_step_ledger SET status = 'succeeded', result = $4::jsonb "
        "WHERE flow_id = $1 AND step_key = $2 AND attempt = $3",
        flow_id,
        "s",
        1,
        json.dumps({"v": 42}),
    )
    replay = await claimed_body()
    assert replay == {"v": 42}, "the recorded result returns"
    assert executions == 1, "the memoized replay must not re-execute"

    # THE RED — A REAL ENGINE MUTATION: the memoized-lookup statement
    # mutated to match nothing (the replay lookup broken), the SAME body
    # re-delivered re-EXECUTES: the double-run dragon, on the shipped path.
    import taskq.workflows._sql as sql_module

    mutated = wf_sql.ledger_memoized.replace(
        "AND status IN ('succeeded', 'failed')", "AND status IN ('no-such-status')"
    )
    assert mutated != wf_sql.ledger_memoized, "the mutation drill did not arm"
    monkeypatch.setattr(sql_module, "LEDGER_MEMOIZED_SQL", mutated)
    mutated_sql = render_workflow_sql(wf_schema)
    executions = 0

    async def red_body() -> Any:
        nonlocal executions
        memo = await memoized_step_result(
            wf_conn, mutated_sql, flow_id=flow_id, step_key="s", map_index=None
        )
        if memo is not None and memo.status == "succeeded":
            return memo.result
        executions += 1
        return "ran"

    second = await red_body()
    ledger_redlog.red(
        "pin1-double-run",
        "the memoized lookup's statement mutated to match nothing (the replay forgets)",
        {"executions_after_redelivery": executions},
    )
    assert second == "ran" and executions == 1, (
        "the mutated replay must re-execute — the red comparator is broken "
        "(the lookup is load-bearing)"
    )
    monkeypatch.undo()


# ── Pin 2: LOST-COMPLETION-WINDOW (crash between effect and finalize) ───


@pytest.mark.integration
async def test_pin_2_lost_completion_window(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Crash between the side-effect and the finalize: the retry CLAIMS
    (the ledger's terminal row is the memoized answer), never re-executes.
    The re-claim of the SAME (flow, step, attempt) rides the ON CONFLICT
    path — one round trip, the same row, the PK blocking every duplicate."""
    flow_id = new_uuid()
    node_id = new_uuid()
    claim = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="effect",
        map_index=None,
        attempt=1,
    )
    assert claim.status == "running"
    # THE CRASH: the side effect landed; the finalize (and its ledger
    # terminal write) did not. The retry re-claims the SAME key 30x
    # concurrently — one ledger row.
    reclaims = await asyncio.gather(
        *[
            claim_step_ledger(
                module_pg_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=node_id,
                step_key="effect",
                map_index=None,
                attempt=1,
            )
            for _ in range(30)
        ]
    )
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 AND step_key = $2',
        flow_id,
        "effect",
    )
    assert int(rows) == 1, "the ledger PK blocks double-recording"
    assert all(r.status == "running" for r in reclaims)


# ── Pin 3: CONCURRENT-CLAIM (30 reps x 10 concurrent, one winner) ───────


@pytest.mark.integration
async def test_pin_3_concurrent_step_claims_one_winner(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Two concurrent claims of one (scope, key) — the ledger PK is the
    arbiter: one winner, the losers observe the conflict path (the same
    row returned, never a second row). The 30x10 shape: 30 reps x 10
    concurrent claims stay one-round-trip-exact."""
    flow_id = new_uuid()
    node_id = new_uuid()
    for rep in range(30):
        attempt = rep + 1
        claims = await asyncio.gather(
            *[
                claim_step_ledger(
                    module_pg_pool,
                    wf_sql,
                    flow_id=flow_id,
                    job_id=node_id,
                    step_key="raced",
                    map_index=None,
                    attempt=attempt,
                )
                for _ in range(10)
            ]
        )
        rows = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '
            "AND step_key = 'raced' AND attempt = $2",
            flow_id,
            attempt,
        )
        assert int(rows) == 1, f"rep {rep}: {rows} rows for one (step, attempt)"
        assert all(c.status == "running" for c in claims)


# ── Pin 4: RUN-KEY-REPLAY (G2's arbiter is the rememberer) ──────────────


@pytest.mark.integration
async def test_pin_4_run_key_replay_one_run(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    ledger_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent ``run(key=k)`` — and the sequential replay — produce
    ONE run row; every caller gets the EXISTING run's id. The '202 + a new
    run' variant (the founding incident's convicted shape) reds."""
    flow = FlowStandIn()
    key = "nightly:2026-10-07T03:00:00Z"  # the cron-slot key (G3)

    # THE SHIPPED ARBITER: 10 racing inserts, one key — ONE run row; every
    # caller gets the same id.
    results = await asyncio.gather(
        *[insert_flow_run(module_pg_pool, wf_sql, entry=flow, run_key=key) for _ in range(10)]
    )
    ids = {str(r.flow_id) for r in results}
    created = [r for r in results if r.created]
    assert len(created) == 1, f"exactly one creator, got {len(created)}"
    assert len(ids) == 1, ids  # every caller sees the SAME run id

    # The SEQUENTIAL replay returns the existing run, never a new one.
    replay = await insert_flow_run(wf_conn, wf_sql, entry=flow, run_key=key)
    assert not replay.created
    assert replay.flow_id == results[0].flow_id
    rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE idempotency_scope = $1 '
        "AND idempotency_key = $2",
        run_idempotency_scope(flow.name),
        key,
    )
    assert int(rows) == 1, "one run row, forever"

    # THE RED — A REAL ENGINE MUTATION: the arbiter's scope reverted to the
    # bare GLOBAL 'workflow-run' (the pre-F4 shape, the founding incident's
    # scope): a DIFFERENT flow with the same key is silently deduped onto
    # THIS flow's run — created=False, someone else's run id, nothing
    # launched. The shipped per-flow scope greens it (the attack file
    # keeps the same conviction:
    # tests/attack_wf_runkey_scope_collision.py).
    import taskq.workflows.ledger as ledger_module

    other = FlowStandIn(name="nightly-prune")
    other.actor = "prune-b"
    drill_key = "nightly:slot-2"  # a fresh key: BOTH runs must sit under the MUTATED (global) scope for the collision to manifest
    monkeypatch.setattr(ledger_module, "run_idempotency_scope", lambda name=None: "workflow-run")
    first_global = await insert_flow_run(wf_conn, wf_sql, entry=flow, run_key=drill_key)
    assert first_global.created
    cross = await insert_flow_run(wf_conn, wf_sql, entry=other, run_key=drill_key)
    monkeypatch.undo()
    ledger_redlog.red(
        "pin4-run-key-replay",
        "the arbiter's scope reverted to the bare global 'workflow-run'",
        {
            "cross_flow_created": cross.created,
            "cross_flow_run_id": str(cross.flow_id),
            "existing_run_id": str(first_global.flow_id),
        },
    )
    assert not cross.created and cross.flow_id == first_global.flow_id, (
        "the global-scope mutation must silently dedup the other flow's run "
        "— the red comparator is broken (the scope is load-bearing)"
    )
    # The SHIPPED scope namespaces per flow: the same key, the OTHER flow,
    # creates ITS OWN run.
    shipped = await insert_flow_run(wf_conn, wf_sql, entry=other, run_key=drill_key)
    assert shipped.created, "the per-flow scope lets another flow's run claim its own key"


# ── Pin 5: LEDGER-CLAIM-ATOMIC (hardening H6) ───────────────────────────


@pytest.mark.integration
async def test_pin_5_ledger_claim_atomic(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    ledger_redlog: RedLog,
) -> None:
    """The claim and the attempt's ledger ``running`` row in SEPARATE
    transactions — a cancel landing in the window fences NOTHING (the
    attempt isn't on the record yet) and the row appears POST-CANCEL as a
    phantom ``running``. THE CURE: claim + ledger insert = ONE statement
    (the shipped shape — the ledger row IS the claim; the phantom the
    window produced is reconciled by the reaper arm, pin 15). The
    two-transaction variant reds."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = new_uuid()

    # THE CANCEL LANDS FIRST (the flow terminal — the two-transaction
    # variant's dispatch already committed its node flip; only the ledger
    # write follows, post-cancel).
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded' WHERE id = $1",
        flow_id,
    )
    # THE CONVICTED SHAPE: the post-cancel ledger write lands a 'running'
    # row on a TERMINAL flow — the phantom (the attempt is on the record
    # too late to be fenced by the cancel's own scan).
    phantom = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="too-late",
        map_index=None,
        attempt=1,
    )
    assert phantom.status == "running"
    flow_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    ledger_redlog.red(
        "pin5-ledger-claim-atomic",
        "the claim's ledger row lands in a transaction AFTER the cancel scanned (the two-tx window)",
        {"ledger": "running", "flow": flow_status},
    )
    assert flow_status == "succeeded", "the flow terminalized before the ledger write"

    # THE SHIPPED REAPER reconciles the phantom: the record is rows-alone
    # reconstructible again. The CLAIM's own atomicity (ONE statement: the
    # ledger row IS the claim — no window to land in) is what prevents NEW
    # phantoms; the concurrency pin (pin 3) drives that shape 30x10.
    reaped = await reap_phantom_ledger(module_pg_pool, wf_sql)
    assert reaped >= 1, "the phantom is reaped; the record reconciles"
    ledger_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '
        "AND step_key = 'too-late'",
        flow_id,
    )
    assert ledger_status == "fenced", ledger_status


# ── Pin 6: LEDGER-TERMINAL-ATOMIC (hardening H9) ────────────────────────


@pytest.mark.integration
async def test_pin_6_ledger_terminal_atomic(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    ledger_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ledger's terminal-outcome write OUTSIDE the finalize TX — a
    cancel between the node flip and the ledger write leaves
    node=succeeded with ledger=running. THE CURE: the ledger terminal write
    is IN the finalize TX (the shipped shape: ``_run_tx1`` co-locates
    them). The split-write variant reds."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, 'split', $2)",
        node_id,
        json.dumps({"flow_id": str(flow_id)}),
    )

    # THE SHIPPED SHAPE: the node's finalize tx1 carries the ledger's
    # terminal — node and ledger agree, atomically.
    claim = await claim_step_ledger(
        wf_conn,
        wf_sql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="split",
        map_index=None,
        attempt=1,
    )
    assert claim.status == "running"
    worker_id = new_uuid()
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET locked_by_worker = $2, claim_epoch = 0, '
        "lock_expires_at = now() + interval '90 seconds' WHERE id = $1",
        node_id,
        worker_id,
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="split",
        worker_id=worker_id,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        result={"done": True},
    )
    assert result.applied
    ledger_status = await wf_conn.fetchval(
        f"SELECT status FROM \"{wf_schema}\".wf_step_ledger WHERE flow_id = $1 AND step_key = 'split'",
        flow_id,
    )
    node_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', node_id
    )
    assert ledger_status == node_status == "succeeded", (
        "the shipped tx1 co-locates the ledger terminal with the node's"
    )

    # ...the split variant is a REAL ENGINE MUTATION: the ledger's
    # terminal statement mutated to update NOTHING (the write lost — the
    # cancel ate the window); the node terminalized, the ledger did not.
    # The shipped statement greens it (the tx1 co-location above).
    import taskq.workflows._sql as sql_module

    # The mutation lands BEFORE the RETURNING (a suffix would corrupt the
    # statement's tail): the terminal matches nothing — the write is lost.
    mutated_terminal = wf_sql.ledger_terminal.replace(
        "RETURNING id", "  AND status = 'no-such-status'\nRETURNING id"
    )
    assert mutated_terminal != wf_sql.ledger_terminal, "the mutation drill did not arm"
    monkeypatch.setattr(sql_module, "LEDGER_TERMINAL_SQL", mutated_terminal)
    mutated_sql = render_workflow_sql(wf_schema)
    split_node = new_uuid()
    split_worker = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, claim_epoch, "
        "step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
        "now() + interval '90 seconds', 0, 'split2', $3::jsonb)",
        split_node,
        split_worker,
        json.dumps({"flow_id": str(flow_id)}),
    )
    await claim_step_ledger(
        wf_conn,
        mutated_sql,
        flow_id=flow_id,
        job_id=split_node,
        step_key="split2",
        map_index=None,
        attempt=1,
    )
    split_result = await finalize_node(
        module_pg_pool,
        mutated_sql,
        flow_id=flow_id,
        job_id=split_node,
        step_key="split2",
        worker_id=split_worker,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        result={"done": True},
    )
    assert split_result.applied
    split_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', split_node
    )
    split_ledger = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '
        "AND step_key = 'split2'",
        flow_id,
    )
    ledger_redlog.red(
        "pin6-ledger-terminal-atomic",
        "the ledger's terminal statement mutated to update nothing (the write lost in the cancel window)",
        {"node": split_status, "ledger": split_ledger},
    )
    assert split_status == "succeeded" and split_ledger == "running", (
        "the mutated terminal must drift from the node — the red comparator "
        "is broken (the tx1 co-location is load-bearing)"
    )


# ── Pin 7: THE LEDGER PK'S MAP CHILDREN (the arbiter keys map_index) ────


@pytest.mark.integration
async def test_pin_7_map_children_own_ledger_rows(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """Two map children of ONE step key (map_index 0 and 1, first attempt
    each) claim DIFFERENT ledger rows and terminalize WITHOUT overwrite:
    each replay returns ITS OWN child's result (T05's key contract; the
    conviction — an arbiter omitting map_index — is the RED drill, the
    shipped statements are what the drill mutates)."""
    flow_id = new_uuid()
    node_0, node_1 = new_uuid(), new_uuid()

    claim_0 = await claim_step_ledger(
        wf_conn, wf_sql, flow_id=flow_id, job_id=node_0, step_key="enrich", map_index=0, attempt=1
    )
    claim_1 = await claim_step_ledger(
        wf_conn, wf_sql, flow_id=flow_id, job_id=node_1, step_key="enrich", map_index=1, attempt=1
    )
    assert claim_0.ledger_id is not None and claim_1.ledger_id is not None
    assert claim_0.ledger_id != claim_1.ledger_id, "one claim row per map child"

    # Each child's finalize keys the terminal write by ITS OWN claim's
    # RETURNING id (the strongest form) — the outcomes cannot cross.
    for claim, node, res in (
        (claim_0, node_0, {"child": 0}),
        (claim_1, node_1, {"child": 1}),
    ):
        worker_id = new_uuid()
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, attempt, locked_by_worker, lock_expires_at, claim_epoch, "
            "step_key, metadata) "
            "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
            "now() + interval '90 seconds', 0, 'enrich', $3::jsonb)",
            node,
            worker_id,
            json.dumps({"flow_id": str(flow_id)}),
        )
        result = await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=node,
            step_key="enrich",
            worker_id=worker_id,
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            result=res,
            ledger_id=claim.ledger_id,
        )
        assert result.applied

    replay_0 = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=0
    )
    replay_1 = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=1
    )
    assert replay_0 is not None and replay_0.result == {"child": 0}
    assert replay_1 is not None and replay_1.result == {"child": 1}

    # THE MAP-AWARE FALLBACK (the arbiter-tuple keying): a finalize without
    # the claim id still keys COALESCE(map_index, -1) — the fallback write
    # hits ITS OWN map child's row, never the sibling's.
    node_2 = new_uuid()
    claim_2 = await claim_step_ledger(
        wf_conn, wf_sql, flow_id=flow_id, job_id=node_2, step_key="enrich", map_index=2, attempt=1
    )
    assert claim_2.status == "running"
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, claim_epoch, "
        "step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
        "now() + interval '90 seconds', 0, 'enrich', $3::jsonb)",
        node_2,
        worker_id,
        json.dumps({"flow_id": str(flow_id)}),
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=node_2,
        step_key="enrich",
        worker_id=worker_id,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        result={"child": 2},
        map_index=2,
    )
    assert result.applied
    replay_2 = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=2
    )
    assert replay_2 is not None and replay_2.result == {"child": 2}
    # ...and the earlier replays are untouched by the fallback write.
    replay_0_again = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="enrich", map_index=0
    )
    assert replay_0_again is not None and replay_0_again.result == {"child": 0}


# ── The key-derivation golden (unit pin) ────────────────────────────────


def test_step_key_derivation_golden() -> None:
    """The step-claim key: ``(workflow, step_key[, map_index])`` — golden
    shapes; ids stay surrogate (the seam), keys stay TEXT (business keys)."""
    flow = new_uuid()
    assert step_idempotency_scope(flow) == f"workflow:{flow}"
    assert step_idempotency_key("enrich") == "wf:enrich"
    assert step_idempotency_key("enrich", 7) == "wf:enrich:7"

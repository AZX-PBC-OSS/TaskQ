# ruff: noqa: S608  # Why: every schema interpolation is the pack's own fixture-minted chp_* test identifier (never user input); every value is $-bound.
"""RV5 — THE CHILD-FLOWS HOSTILE REVIEW'S PINS (feat/taskqflow-child-flows
@ f8bfc5033 — the T28 child-flows lane under the rv5 hostile review).

Provenance: the rv5 reviewer's live-convicted findings, this lane's pack.
Per the doctrine, a LANDED finding is pinned asserting the SAFE behavior
under ``pytest.mark.xfail(strict=True)`` — the cure flips the pin to
XPASS-strict (a red that says: remove the marker WITH the cure); the pin
then stands as the green guard. Every pin ran UNMARKED first (the red
captured verbatim in the pack's RECEIPTS.md) before the marker went on.

The findings (each convicted live at the pin head; the attack scripts'
harness shapes at the convictions' root):

* F-C1 (SEVERE) THE GATHER DRAGON — ``asyncio.gather`` over K
  ``ctx.spawn_flow`` in one body mints ONE child run, not K: the spawn
  cursor read (_childflow.py:414-423) and write (:457-467) are separate
  autocommit statements, so the concurrent spawns all read slot 0, mint
  the identical run key, and the arbiter ADOPTS — inputs d1-1/d1-2
  silently dropped, the K handles alias one flow id, the fan-in returns
  tenant-0's verdict K times (silent fabricated results).
* F-C2 (SEVERE) THE RETENTION-RESPAWN — purge the child's rows while the
  parent waits and the resume silently RE-CREATES and re-runs the child
  (double execution): the run-key arbiter's memory IS the child root
  row, so the purge frees the key; the ticket's "missing root →
  fail-close ChildFailed" is dead code behind spawn-then-wait
  (_childflow.py:224's missing-root path is unreachable).
* F-C3 (SEVERE) THE RECURSION BOMB — a flow whose child is ITSELF: no
  cycle check, no depth bound; 12 dev ticks minted 14 runs, unbounded
  (every tick mints the next generation forever).
* F-C4 THE CASCADE'S STACK — ``cancel_workflow_run`` recurses in Python
  (_runner_exit.py:316-323): a depth-1200 chain died RecursionError with
  984/1200 cancelled and 216 left RUNNING (an admin 500 over a
  half-cancelled tree).
* F-C5 THE ORPHAN WINDOW — kill the worker between the child-create
  commit and the receipt write, then cancel the parent: the child stays
  running FOREVER — the cascade walks ``metadata.child_flows`` receipts
  only (_childflow.py:474-479); the crash-safe ``parent_flow_id``
  direction rides the child's create tx but is never walked.
* F-C6 THE DEV-TICK EXPIRY GAP — ``wait(timeout_s=…)``'s Expired arm
  depends on ``sweep_expired_signals``, wired only in the fleet leader
  sweeps, never in ``FlowRunner.tick`` (_runner.py:430-456): on the
  dev-loop drive the join's deadline is dead code.
* F-C7 THE NO-OP RESOLVE BOUNCE — ``flows resolve <hold>`` /
  ``HitlClient.resolve`` on a child-join hold DELIVERS it; the resumed
  wait site re-holds on the still-live child (the resolve consumed
  nothing; the operator's decision is a dead letter).

THE FIXTURE STACK (the pack's own, per the lane's prefix law): the
module-minted ``chp_rv5_*`` schema on ``TASKQ_TEST_PG_DSN``, truncated +
reseeded per test (the house ``clean_pg_conn`` pattern), dropped CASCADE
at module teardown. The module-local ``wf_conn`` / ``wf_schema`` /
``wf_sql`` / ``wf_pool`` names deliberately shadow the conftest-registered
``tests/_wf_fixtures.py`` fixtures so the always-on G7 rows↔status
teardown (auto-registered on every ``test_wf_*`` async test by
``tests/conftest.py``) runs against THIS pack's schema. Every red state
below was audited G7-quiet (wedged/live roots reconstruct
running/blocked/pending — agreement, no false teeth).
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from taskq.testing.pg import reset_schema
from taskq.workflows import (
    ChildCancelled,
    ChildFailed,
    ChildOk,
    Expired,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    cancel_workflow_run,
    step,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import render_workflow_sql

#: The lane's DSN (the review cluster; the fixture stack owns the
#: chp_-prefixed schema's lifecycle on it).
CHP_DSN = os.environ.get("TASKQ_TEST_PG_DSN", "postgres://taskq:taskq@localhost:5891/taskq")


# ── the pack's fixture stack (the chp_ prefix law; house idioms) ────────


@pytest_asyncio.fixture(scope="module")
async def chp_schema() -> AsyncIterator[str]:
    """The pack's own schema: ``chp_rv5_<token>`` on the lane's DSN —
    created migrated, dropped CASCADE at module teardown (the residue
    is attributable and self-cleaning)."""
    name = "chp_rv5_" + new_uuid().hex[:10]
    conn = await asyncpg.connect(CHP_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        await apply_pending(conn, schema=name)
    finally:
        await conn.close()
    yield name
    conn = await asyncpg.connect(CHP_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def wf_schema(chp_schema: str) -> AsyncIterator[str]:
    """Per-test clean slate on the pack's schema (the house
    ``clean_pg_conn`` pattern: truncate every dynamic table, reseed the
    default actors — migration state intact)."""
    conn = await asyncpg.connect(CHP_DSN)
    try:
        await reset_schema(conn, chp_schema)
    finally:
        await conn.close()
    yield chp_schema


@pytest_asyncio.fixture
async def wf_pool(wf_schema: str) -> AsyncIterator[asyncpg.Pool]:
    """The runner's pool on the lane's DSN (one per test — the house
    ``wf_pool`` pattern). Teardown TERMINATES, never graceful-closes:
    the F-C4 red's RecursionError destroys a holder-release task
    mid-flight, and ``close()`` waits on that never-released holder
    forever (the pack's own seam — a red must never wedge the suite)."""
    pool = await asyncpg.create_pool(CHP_DSN)
    try:
        yield pool
    finally:
        pool.terminate()


@pytest_asyncio.fixture
async def wf_conn(wf_schema: str) -> AsyncIterator[asyncpg.Connection]:
    """A bare connection on the lane's DSN (the row reads; per-test)."""
    conn = await asyncpg.connect(CHP_DSN)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def wf_sql(wf_schema: str) -> WorkflowSql:
    """The workflow statement bundle rendered for the pack's schema."""
    return render_workflow_sql(wf_schema)


# ── the shared models + app builders (the T28 lane's shapes) ────────────


class SubProbe(BaseModel):
    doc_id: str


class SubReport(BaseModel):
    findings: list[str]


class ParentIn(BaseModel):
    doc_id: str


def _child_app(name: str) -> WorkflowApp:
    """The CHILD workflow's app: one step reading ``ctx.input`` (the
    spawn's input riding the child root's payload), returning the typed
    report. The name is UNIQUE PER TEST (the D1 registry is global)."""
    app = WorkflowApp()

    @app.actor(queue="cpu", max_attempts=1)
    async def probe(ctx: StepContext) -> SubReport:
        probe_input = SubProbe.model_validate(ctx.input)
        return SubReport(findings=[f"found:{probe_input.doc_id}"])

    @app.workflow(name)
    def child_wf() -> Promise[object]:
        return build(step(probe, key="probe"))

    return app


async def _spawn_and_hold(runner: FlowRunner, flow_id: JobId) -> None:
    """ONE body execution of the parent's spawn_join node: the spawn +
    the join hold (the T28 pin file's manual staging idiom)."""
    rows = await runner._claimable(flow_id)
    assert len(rows) == 1, f"expected exactly the spawn_join node, got {rows}"
    await runner._run_node(flow_id, rows[0])


async def _node_row(
    wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId, step_key: str
) -> dict[str, Any]:
    row = await wf_conn.fetchrow(
        f'SELECT id, status, metadata FROM "{wf_schema}".jobs '
        "WHERE step_key = $1 AND (metadata->>'flow_id')::uuid = $2",
        step_key,
        flow_id,
    )
    assert row is not None, f"no {step_key!r} row under {flow_id}"
    node = dict(row)
    raw_meta = node["metadata"]
    # asyncpg hands jsonb back as str on un-coded connections.
    node["metadata"] = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
    return node


async def _child_census(wf_conn: asyncpg.Connection, wf_schema: str, child_name: str) -> int:
    """How many REAL child runs the rows hold for the child workflow
    (the flow roots named by their registered workflow)."""
    return int(
        await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND metadata->>'workflow' = $1",
            child_name,
        )
    )


# ── F-C1: THE GATHER DRAGON ─────────────────────────────────────────────


class GatherOut(BaseModel):
    verdicts: list[str]
    distinct_children: int


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c1: the spawn cursor's read (_childflow.py:414-423) and write "
        "(:457-467) are separate autocommit statements — K concurrent spawn_flow in one "
        "body all read slot 0, mint the identical run key, and the arbiter adopts: ONE "
        "child run for K spawns (observed: 3 spawns → 1 run, inputs d1-1/d1-2 dropped, "
        "the fan-in returned tenant-0's verdict 3 times)"
    ),
)
async def test_rv5_c1_concurrent_spawns_in_one_body_each_mint_their_own_child(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: K concurrent spawns in one body mint K DISTINCT
    child runs, each carrying its own input; the fan-in returns each
    child's own verdict. (The alternative cure — a LOUD refusal of
    concurrent spawns naming the sequential spelling — is flagged in
    PINMAP.md; the pin asserts the distinct-children shape.)"""
    _child_app("rv5c1_child").get("rv5c1_child")
    app = WorkflowApp()

    async def gather_body(ctx: StepContext, params: ParentIn) -> GatherOut:
        probes = [SubProbe(doc_id=f"{params.doc_id}-{i}") for i in range(3)]
        # THE CONCURRENT FAN-OUT: the natural asyncio spelling.
        handles = await asyncio.gather(
            *[ctx.spawn_flow("rv5c1_child", input=p, result=SubReport) for p in probes]
        )
        outcomes = [await h.wait() for h in handles]
        verdicts: list[str] = []
        for outcome in outcomes:
            match outcome:
                case ChildOk(result=report):
                    verdicts.append(report.findings[0])
                case ChildFailed():
                    verdicts.append("failed")
                case ChildCancelled():
                    verdicts.append("cancelled")
                case Expired():
                    verdicts.append("expired")
        return GatherOut(
            verdicts=verdicts,
            distinct_children=len({str(h.flow_id) for h in handles}),
        )

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> GatherOut:
        return await gather_body(ctx, params)

    @app.workflow("rv5c1_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(app.get("rv5c1_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal"
    result = GatherOut.model_validate(await runner.result(flow_id))
    assert result.distinct_children == 3, (
        "F-C1: 3 concurrent spawns aliased "
        f"{result.distinct_children} child flow id(s) — the inputs of the dropped spawns "
        "were silently discarded (the gather dragon)"
    )
    assert result.verdicts == ["found:d1-0", "found:d1-1", "found:d1-2"], (
        f"F-C1: the fan-in returned {result.verdicts} — a child run's verdict "
        "fabricated for a sibling's wait (each spawn's own input must come back)"
    )
    assert await _child_census(wf_conn, wf_schema, "rv5c1_child") == 3


# ── F-C2: THE RETENTION-RESPAWN ─────────────────────────────────────────


class PurgeOut(BaseModel):
    verdict: str
    error_class: str


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c2: the run-key arbiter's memory IS the child root row — "
        "purge the waiting child's rows and the resume silently RE-CREATES and re-runs "
        "the child (observed: child census 0 → 1 after the purge, parent verdict 'ok'); "
        "the missing-root fail-close at _childflow.py:224 is unreachable behind "
        "spawn-then-wait (double execution under the retention policy)"
    ),
)
async def test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: the child the parent joined is GONE at the wake
    (the retention purge's shape) → the wait's ``ChildFailed`` arm fires
    with a NAMED error class, and NO new child run is minted (never a
    silent respawn)."""
    _child_app("rv5c2_child").get("rv5c2_child")
    app = WorkflowApp()

    async def join_body(ctx: StepContext, params: ParentIn) -> PurgeOut:
        handle = await ctx.spawn_flow(
            "rv5c2_child", input=SubProbe(doc_id=params.doc_id), result=SubReport
        )
        outcome = await handle.wait()
        match outcome:
            case ChildOk():
                return PurgeOut(verdict="ok", error_class="")
            case ChildFailed(error_class=error_class):
                return PurgeOut(verdict="failed", error_class=error_class)
            case ChildCancelled():
                return PurgeOut(verdict="cancelled", error_class="")
            case Expired():
                return PurgeOut(verdict="expired", error_class="")
        raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> PurgeOut:
        return await join_body(ctx, params)

    @app.workflow("rv5c2_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(app.get("rv5c2_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await _spawn_and_hold(runner, flow_id)
    parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
    receipts = parent_node["metadata"]["child_flows"]
    assert len(receipts) == 1
    child_flow_id = JobId(str(receipts[0]))
    assert await _child_census(wf_conn, wf_schema, "rv5c2_child") == 1

    # THE RETENTION PURGE: the child's rows are gone while the parent
    # waits (the edges first, then the run's rows — the pruner's shape).
    await wf_conn.execute(
        f'DELETE FROM "{wf_schema}".wf_edge WHERE parent_id IN '
        f"(SELECT id FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1) "
        "OR child_id IN "
        f"(SELECT id FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1)",
        child_flow_id,
    )
    await wf_conn.execute(
        f"DELETE FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 OR id = $1",
        child_flow_id,
    )
    assert await _child_census(wf_conn, wf_schema, "rv5c2_child") == 0

    outcome = await runner.drive(flow_id, until="terminal", max_ticks=4000)
    assert outcome == "terminal"
    result = PurgeOut.model_validate(await runner.result(flow_id))
    assert result.verdict == "failed", (
        f"F-C2: the purged child's join resolved {result.verdict!r} — the wait's "
        "ChildFailed fail-close arm is the LOUD answer to a missing child root "
        "(never a silent respawn-and-join)"
    )
    assert result.error_class != "", (
        "F-C2: the fail-close must carry a NAMED error class (the operator reads WHY "
        "the child is gone), not an empty one"
    )
    assert await _child_census(wf_conn, wf_schema, "rv5c2_child") == 0, (
        "F-C2: the resume RE-CREATED the purged child — the double execution the "
        "fail-close exists to prevent"
    )


# ── F-C3: THE RECURSION BOMB ────────────────────────────────────────────


class GenOut(BaseModel):
    verdict: str


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c3: no cycle check and no depth bound on spawn_flow — a flow "
        "whose child is ITSELF minted 14 runs in 12 dev ticks (observed), one new "
        "generation per tick, unbounded; each generation's run key is fresh (the "
        "parent_flow_id leg) so the arbiter never adopts"
    ),
)
async def test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: a self/ancestor-spawning flow is REFUSED — the
    runtime loud refusal (the receipt walk makes the ancestor check
    cheap): the spawn raises, the run terminal-FAILS with a named error
    class, and the flow census stays at the ONE run the caller created."""
    app = WorkflowApp()

    @app.actor(queue="cpu", max_attempts=1)
    async def ouroboros(ctx: StepContext, params: SubProbe) -> GenOut:
        handle = await ctx.spawn_flow(
            "rv5c3_self", input=SubProbe(doc_id=params.doc_id), result=SubReport
        )
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=report):
                return GenOut(verdict=f"ok:{report.findings}")
            case ChildFailed():
                return GenOut(verdict="failed")
            case ChildCancelled():
                return GenOut(verdict="cancelled")
            case Expired():
                return GenOut(verdict="expired")
        raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    @app.workflow("rv5c3_self")
    def self_wf() -> Promise[object]:
        return build(step(ouroboros, SubProbe(doc_id="x"), key="self_spawn"))

    runner = FlowRunner(app.get("rv5c3_self"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    # The attack's own drive: 12 ticks. The unguarded engine mints ~1
    # generation per tick (observed: 14 runs); the refused engine fails
    # the first spawn and terminalizes the ONE run.
    for _ in range(12):
        await runner.tick(flow_id, execute=True)

    census = await _child_census(wf_conn, wf_schema, "rv5c3_self")
    assert census == 1, (
        f"F-C3: a self-spawning flow minted {census} runs in 12 ticks — unbounded "
        "flow growth from ONE create_flow (the recursion bomb; no cycle check, no "
        "depth bound)"
    )
    root = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root is not None
    assert root["status"] == "failed", (
        "F-C3: the refusal must be LOUD — the self-spawning run terminal-fails, "
        f"never parks or succeeds (observed: {root['status']!r})"
    )
    assert root["error_class"], "F-C3: the refusal names itself (a non-empty error class)"


# ── F-C4: THE CASCADE'S STACK ───────────────────────────────────────────

#: The convicted depth: the Python-recursive cascade died RecursionError
#: at ~984 of 1200 (observed live), leaving 216 roots RUNNING.
C4_DEPTH = 1200


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c4: cancel_workflow_run recurses in Python "
        "(_runner_exit.py:316-323) — a depth-1200 receipt chain died RecursionError "
        "with 984/1200 cancelled and 216 left RUNNING (observed live): an admin 500 "
        "over a half-cancelled tree, the survivors' names unreported"
    ),
)
async def test_rv5_c4_the_cancel_cascade_is_iterative_over_a_deep_chain(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: the cascade is ITERATIVE (or depth-bounded with
    a loud partial-failure report naming the survivors — the alternative
    flagged in PINMAP.md): cancelling the head of a depth-1200 child
    chain cancels EVERY link and returns the count, never raising the
    stack. The chain is staged as raw root rows (the q8b attack's
    staging: each root's receipt names the next flow — the exact rows
    the cascade's walk reads)."""
    ids = [new_uuid() for _ in range(C4_DEPTH)]
    rows = []
    for i, fid in enumerate(ids):
        meta: dict[str, object] = {"flow_id": str(fid), "workflow": "rv5c4_chain"}
        if i + 1 < C4_DEPTH:
            meta["child_flows"] = [str(ids[i + 1])]
        rows.append((fid, json.dumps(meta)))
    await wf_conn.executemany(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, step_key, status, metadata, created_at) "
        f"VALUES ($1, 'wf', 'default', '{{}}'::jsonb, 1, 'transient', '__flow__', "
        "'running', $2::jsonb, now())",
        rows,
    )

    cancelled: int | None
    try:
        cancelled = await cancel_workflow_run(
            wf_pool, schema=wf_schema, flow_id=JobId(ids[0]), reason="rv5-c4 depth"
        )
    except RecursionError:
        cancelled = None
    survivors = int(
        await wf_conn.fetchval(
            f"SELECT count(*) FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')"
        )
    )
    assert cancelled is not None, (
        f"F-C4: the cancel cascade blew the Python stack at depth {C4_DEPTH} "
        f"(RecursionError) — {survivors} of {C4_DEPTH} runs left NON-terminal, the "
        "call raising instead of reporting a bounded partial failure"
    )
    assert survivors == 0, (
        f"F-C4: {survivors} of {C4_DEPTH} runs survived the cascade still non-terminal"
    )
    assert cancelled == C4_DEPTH


# ── F-C5: THE ORPHAN WINDOW ─────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c5: the cancel cascade walks metadata.child_flows receipts "
        "only (_childflow.py:474-479) — a child whose create committed (parent_flow_id "
        "provenance riding the create's own tx) but whose receipt write was lost (the "
        "kill window) escapes the parent's cancel FOREVER (observed: parent cancelled, "
        "child root still running; the cascade returned 1)"
    ),
)
async def test_rv5_c5_the_cascade_walks_both_directions_no_child_escapes(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: the cascade walks BOTH directions — the parent
    node's receipts AND the children's ``parent_flow_id`` provenance — so
    no child escapes a parent's cancel. The orphan window is staged
    exactly: the child's create is the spawn's own create call (the
    provenance riding the create tx, byte-identical to _childflow.py's
    ``root_metadata_extra`` write), and the receipt write — the
    statement the kill-window aborts — never runs."""
    _child_app("rv5c5_child").get("rv5c5_child")
    app = WorkflowApp()

    async def join_body(ctx: StepContext, params: ParentIn) -> GenOut:
        handle = await ctx.spawn_flow(
            "rv5c5_child", input=SubProbe(doc_id=params.doc_id), result=SubReport
        )
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=report):
                return GenOut(verdict=f"ok:{report.findings}")
            case ChildFailed():
                return GenOut(verdict="failed")
            case ChildCancelled():
                return GenOut(verdict="cancelled")
            case Expired():
                return GenOut(verdict="expired")
        raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> GenOut:
        return await join_body(ctx, params)

    @app.workflow("rv5c5_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(app.get("rv5c5_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")

    # THE WINDOW'S STATE, staged through the spawn's OWN create path
    # (FlowRunner.create_flow with the spawn's run key + provenance) —
    # minus the receipt write the kill aborts.
    from taskq.workflows._worker_execution import get_compiled_workflow

    child_runner = FlowRunner(get_compiled_workflow("rv5c5_child"), wf_pool, wf_schema)
    claim = await child_runner.create_flow(
        input=SubProbe(doc_id="d1"),
        run_key=f"childflow:{flow_id}:spawn_join:0",
        root_metadata_extra={
            "parent_flow_id": str(flow_id),
            "parent_node_id": str(parent_node["id"]),
            "parent_step_key": "spawn_join",
        },
    )
    child_flow_id = claim.flow_id

    # The staging IS the convicted state: the child's provenance names
    # the parent (the crash-safe direction's data IS there)...
    provenance = await wf_conn.fetchval(
        f"SELECT metadata->>'parent_flow_id' FROM \"{wf_schema}\".jobs WHERE id = $1",
        child_flow_id,
    )
    assert provenance == str(flow_id)
    # ...and the parent's receipt is ABSENT (the cascade's only walk
    # today reads nothing).
    fresh_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
    assert "child_flows" not in fresh_node["metadata"]

    await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=flow_id, reason="rv5-c5")

    child_root = await wf_conn.fetchrow(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', child_flow_id
    )
    assert child_root is not None
    assert child_root["status"] == "cancelled", (
        f"F-C5: the unreceipted child escaped the parent's cancel (child root "
        f"{child_root['status']!r}) — the cascade walked the receipts only and never "
        "the parent_flow_id provenance it could see; the orphan runs on with its "
        "parent dead"
    )
    child_work = await _node_row(wf_conn, wf_schema, child_flow_id, "probe")
    assert child_work["status"] == "cancelled"


# ── F-C6: THE DEV-TICK EXPIRY GAP ───────────────────────────────────────


class ExpireOut(BaseModel):
    verdict: str


class NeverGate(BaseModel):
    ok: bool


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c6: wait(timeout_s=)'s Expired arm depends on "
        "sweep_expired_signals, wired only in the fleet leader sweeps — never in "
        "FlowRunner.tick (_runner.py:430-456). On the dev-loop drive the join's "
        "deadline is dead code (observed: a child outliving timeout_s=0.2 across "
        "ticks never abandons the hold; the drive runs to max_ticks with the "
        "parent held past its own deadline)"
    ),
)
async def test_rv5_c6_the_dev_drive_s_tick_runs_the_child_join_expiry_arm(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """THE SAFE BEHAVIOR: the dev driver's tick runs the expiry arm — a
    child that OUTLIVES its join deadline across ticks yields the
    ``Expired`` member to the resumed parent (the Expired face works in
    dev loops). The child parks on a never-resolved signal (the honest
    staging: a child that completes INSIDE one tick is legitimately
    ChildOk under the deadline's at-least semantics — the conviction is
    the arm's ABSENCE, not the race; see PINMAP.md). The alternative
    cure — refusing timeout_s loudly on the dev path — is flagged in
    PINMAP.md."""
    capp = WorkflowApp()

    @capp.actor(queue="cpu", max_attempts=1)
    async def parked_probe(ctx: StepContext) -> Any:
        return await ctx.wait_signal(NeverGate, timeout_s=120.0)

    @capp.workflow("rv5c6_child")
    def child_wf() -> Promise[object]:
        return build(step(parked_probe, key="probe"))

    capp.get("rv5c6_child")
    papp = WorkflowApp()

    @papp.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> ExpireOut:
        handle = await ctx.spawn_flow(
            "rv5c6_child", input=SubProbe(doc_id=params.doc_id), result=SubReport
        )
        outcome = await handle.wait(timeout_s=0.2)
        match outcome:
            case ChildOk(result=report):
                return ExpireOut(verdict=f"ok:{report.findings}")
            case ChildFailed():
                return ExpireOut(verdict="failed")
            case ChildCancelled():
                return ExpireOut(verdict="cancelled")
            case Expired():
                return ExpireOut(verdict="expired")
        raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    @papp.workflow("rv5c6_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(papp.get("rv5c6_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id, until="terminal", tick=0.02, max_ticks=120)
    assert outcome == "terminal", (
        "F-C6: the dev drive never delivered the join's Expired arm — the parent "
        "held past its own timeout_s=0.2 deadline for the whole drive "
        f"({outcome!r}); sweep_expired_signals is wired in the fleet leader sweeps "
        "only, never in FlowRunner.tick"
    )
    result = ExpireOut.model_validate(await runner.result(flow_id))
    assert result.verdict == "expired", (
        f"F-C6: the join outlived its deadline and the body saw {result.verdict!r} "
        "— the Expired face must fire in the dev loop"
    )


# ── F-C7: THE NO-OP RESOLVE BOUNCE ──────────────────────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIVE FINDING rv5-c7: resolving a child-join hold is a no-op bounce — "
        "HitlClient.resolve DELIVERED the join hold (observed), the resumed wait "
        "site re-held on the still-live child (hold epoch 2 registered), and the "
        "operator's decision payload is a dead letter (the join resolves off the "
        "child's terminal, never off a delivered payload). The boundary never "
        "names the hold's kind"
    ),
)
async def test_rv5_c7_resolving_a_child_join_hold_is_a_named_boundary_refusal(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: the resolve boundary REFUSES a child-join hold
    with the hold's KIND named (the timer boundary's own pattern — a
    machine-owned hold is not operator-answerable), the hold SURVIVES
    'held', and the node keeps its held representation (nothing
    consumed, nothing bounced)."""
    _child_app("rv5c7_child").get("rv5c7_child")
    app = WorkflowApp()

    async def join_body(ctx: StepContext, params: ParentIn) -> GenOut:
        handle = await ctx.spawn_flow(
            "rv5c7_child", input=SubProbe(doc_id=params.doc_id), result=SubReport
        )
        outcome = await handle.wait()
        match outcome:
            case ChildOk(result=report):
                return GenOut(verdict=f"ok:{report.findings}")
            case ChildFailed():
                return GenOut(verdict="failed")
            case ChildCancelled():
                return GenOut(verdict="cancelled")
            case Expired():
                return GenOut(verdict="expired")
        raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> GenOut:
        return await join_body(ctx, params)

    @app.workflow("rv5c7_parent")
    def parent_wf() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    runner = FlowRunner(app.get("rv5c7_parent"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await _spawn_and_hold(runner, flow_id)

    hold = await wf_conn.fetchrow(
        f'SELECT id, status FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 AND signal_name LIKE '__child__:%'",
        flow_id,
    )
    assert hold is not None and hold["status"] == "held"

    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(wf_pool, schema=wf_schema)
    # The decision payload FITS the hold's declared contract (the child's
    # result model) — the boundary's shape-check alone cannot refuse it.
    result = await client.resolve(
        str(hold["id"]), {"findings": ["injected-by-hand"]}, reason="rv5-c7"
    )
    assert result.status == "refused", (
        f"F-C7: resolving a child-join hold returned {result.status!r} — the "
        "delivery resolves NOTHING (the wait site re-holds on the still-live "
        "child; the operator's decision is a dead letter). The boundary must "
        "refuse the machine-owned hold, naming its kind"
    )
    reason = (result.reason or "").lower()
    assert "child" in reason, (
        f"F-C7: the refusal must name the hold's KIND (child_join) — got {result.reason!r}"
    )
    # THE HOLD SURVIVES: nothing consumed, nothing bounced.
    hold_after = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold["id"]
    )
    assert hold_after == "held"
    node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
    assert "hold" in node["metadata"]

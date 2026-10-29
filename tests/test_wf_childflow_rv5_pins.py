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

THE RESIDUAL-CURES LANE'S RECONCILED FLIPS (the adoption-with-flip-
receipts law — the receipt rows are in RECEIPTS.md; every flip green x2
at the lane head): C1, C4, C5, C6, C7's markers are REMOVED WITH THE
CURES (the child-cures lane's landed shapes — the pins were xpass-strict
at the reconciliation ground); C2 and C3 are RECONCILED to the cured
shapes (C2: the fail-close AT THE RESPAWN ATTEMPT — the typed
``ChildFlowLostError`` on the parent node, the root failed, the census
zero; C3: the DEPTH BOUND — the ancestor-chain walk, the typed
``SpawnDepthExceededError``, the bounded tree terminalizing). Neither
reconciliation weakens the pin: the cured shape carries every property
the safe behavior asked, and the reconciled assertions read the ROWS.

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

#: THE PACK'S DSN SEAM IS THE HOUSE ``pg_dsn`` FIXTURE (module-scoped
#: database on the invocation's shared container). The dev loop's
#: external cluster keeps working: the shared fixture honors
#: ``TASKQ_TEST_PG_DSN`` as-is when it is set. The dead
#: ``localhost:5891`` literal a previous constant defaulted to was the
#: pack's never-executed-in-CI face (run 38351644136: 7 connection-
#: refused errors per leg — the review cluster the pack was authored
#: against does not exist on a runner, and the import-time read
#: bypassed the fixture seam no matter what CI set).


# ── the pack's fixture stack (the chp_ prefix law; house idioms) ────────


@pytest_asyncio.fixture(scope="module")
async def chp_schema(pg_dsn: str) -> AsyncIterator[str]:
    """The pack's own schema: ``chp_rv5_<token>`` on the lane's DSN —
    created migrated, dropped CASCADE at module teardown (the residue
    is attributable and self-cleaning). The token mixes the house's
    RUN-ISOLATION token (the invocation-unique basetemp path —
    invocation-unique AND worker-distinct: two xdist workers of one
    ``-n 8`` battery each run this module fixture, and the second
    worker's CREATE SCHEMA must never collide with the first's — the
    pg_namespace_nspname_index collision the untokenized uuid7 prefix
    carried when the battery first ran the pack at -n 8)."""
    import hashlib

    from taskq.testing.fixtures import run_isolation_token

    name = "chp_rv5_" + hashlib.sha256(f"chp_rv5:{run_isolation_token()}".encode()).hexdigest()[:10]
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        await apply_pending(conn, schema=name)
    finally:
        await conn.close()
    yield name
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def wf_schema(chp_schema: str, pg_dsn: str) -> AsyncIterator[str]:
    """Per-test clean slate on the pack's schema (the house
    ``clean_pg_conn`` pattern: truncate every dynamic table, reseed the
    default actors — migration state intact)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await reset_schema(conn, chp_schema)
    finally:
        await conn.close()
    yield chp_schema


@pytest_asyncio.fixture
async def wf_pool(wf_schema: str, pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    """The runner's pool on the lane's DSN (one per test — the house
    ``wf_pool`` pattern). Teardown TERMINATES, never graceful-closes:
    the F-C4 red's RecursionError destroys a holder-release task
    mid-flight, and ``close()`` waits on that never-released holder
    forever (the pack's own seam — a red must never wedge the suite)."""
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        yield pool
    finally:
        pool.terminate()


@pytest_asyncio.fixture
async def wf_conn(wf_schema: str, pg_dsn: str) -> AsyncIterator[asyncpg.Connection]:
    """A bare connection on the lane's DSN (the row reads; per-test)."""
    conn = await asyncpg.connect(pg_dsn)
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
    assert sorted(result.verdicts) == ["found:d1-0", "found:d1-1", "found:d1-2"], (
        f"F-C1: the fan-in returned {result.verdicts} — a child run's verdict "
        "fabricated for a sibling's wait or a dropped input (each spawn's OWN "
        "input must come back, exactly once)"
    )
    # THE ORDER LEG'S RECONCILIATION (the residual-cures lane's flip): the
    # strict order is NOT the unkeyed fan-out's law — the spawn docstring's
    # own contract ('a concurrent fan-out that must join STABLY across the
    # resume passes key= — the slot's allocation order is the scheduler's'):
    # the hold-resume re-runs the body from the top, the per-attempt slot
    # race re-associates the unkeyed handles, and the fan-in's verdict
    # order is the scheduler's. The LAW is the no-fabrication SET (above) +
    # the distinct children (below); the keyed stable-join face is the
    # child-cures lane's own pin.
    assert await _child_census(wf_conn, wf_schema, "rv5c1_child") == 3


# ── F-C2: THE RETENTION-RESPAWN ─────────────────────────────────────────


class PurgeOut(BaseModel):
    verdict: str
    error_class: str


@pytest.mark.integration
async def test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: the child the parent joined is GONE at the wake
    (the retention purge's shape) → the fail-close fires — NO new child
    run is minted (never a silent respawn) and the parent's run
    terminal-FAILS LOUDLY with the typed class on the record.

    THE RECONCILED SHAPE (the residual-cures lane's flip — the
    child-cures lane's cure landed the fail-close AT THE RESPAWN
    ATTEMPT, not at the wait's arms): the resume re-runs the spawn site,
    the spawn's dangling-receipt walk finds the parent's receipt naming
    a child whose rows are GONE, and raises the TYPED
    ``ChildFlowLostError`` — the deterministic route terminal-fails the
    node with the class on the record, T06's propagation fails the
    root. The original pin's wait-arm shape (the ``ChildFailed`` member
    through ``handle.wait()``) is unreachable behind spawn-then-wait —
    the conviction's own words — so the cured pin reads the ROWS: the
    node's error class names the wedge, the root followed it, the
    census stayed at zero."""
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
    # THE RECONCILED ASSERTIONS (the cured shape — the fail-close AT THE
    # RESPAWN ATTEMPT): the parent's node row CARRIES the typed
    # ChildFlowLostError (the dangling-receipt refusal — the operator
    # reads WHY the child is gone), the root followed it (the loud
    # terminal), and NO child was re-minted.
    parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
    node_class = await wf_conn.fetchval(
        f'SELECT error_class FROM "{wf_schema}".jobs WHERE id = $1', parent_node["id"]
    )
    assert node_class == "ChildFlowLostError", (
        f"F-C2: the purged child's respawn attempt did not fail-close typed — "
        f"the spawn_join node carries error_class {node_class!r} (the cured "
        "shape: ChildFlowLostError on the record, the run failed LOUDLY, never "
        "a silent respawn-and-join)"
    )
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root == "failed", (
        f"F-C2: the parent's run is {root!r} — the fail-close must take the "
        "run to a LOUD terminal (the rows tell the operator the truth)"
    )
    assert await _child_census(wf_conn, wf_schema, "rv5c2_child") == 0, (
        "F-C2: the resume RE-CREATED the purged child — the double execution the "
        "fail-close exists to prevent"
    )


# ── F-C3: THE RECURSION BOMB ────────────────────────────────────────────


class GenOut(BaseModel):
    verdict: str


@pytest.mark.integration
async def test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """THE SAFE BEHAVIOR: a self/ancestor-spawning flow is BOUNDED and
    the bound's refusal is LOUD — the bomb's tree TERMINALIZES with the
    typed ``SpawnDepthExceededError`` on the record (never an unbounded
    mint).

    THE RECONCILED SHAPE (the residual-cures lane's flip — the
    child-cures lane's cure landed the DEPTH BOUND, not the
    first-spawn refusal): the ancestor chain is read from the rows'
    parent links — ONE walk serves the bound AND the cycle check — and
    the walk past the bound (:data:`DEFAULT_SPAWN_MAX_DEPTH` = 32)
    raises the TYPED ``SpawnDepthExceededError``, a DETERMINISTIC
    authoring failure (the ladder unburned, the class on the record).
    The original pin's first-spawn-refusal shape (census == 1) is NOT
    the cured contract — the bomb's generations run UNTIL the bound,
    then the bound's refusal names itself. The reconciled pin asserts
    the cured shape's whole substance: the tree terminalizes, the
    census is bounded at the depth arithmetic, and the typed refusal
    fired."""
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
    # The cured engine's drive: the dev drive walks the descendant flows,
    # the generations run until the depth bound fires, and the BOUNDED
    # tree terminalizes (the unguarded engine minted a generation per
    # tick FOREVER — the drive never returned terminal).
    outcome = await runner.drive(flow_id, until="terminal", max_ticks=600)
    assert outcome == "terminal", (
        f"F-C3: the self-spawning bomb never terminalized (drive returned "
        f"{outcome!r}) — the depth bound must stop the mint and let the "
        "bounded tree reach terminal"
    )
    census = await _child_census(wf_conn, wf_schema, "rv5c3_self")
    from taskq.workflows import DEFAULT_SPAWN_MAX_DEPTH

    assert census <= 1 + DEFAULT_SPAWN_MAX_DEPTH, (
        f"F-C3: a self-spawning flow minted {census} runs — past the depth "
        "bound's arithmetic (1 + DEFAULT_SPAWN_MAX_DEPTH); the recursion bomb "
        "is unbounded"
    )
    assert census > 1, (
        f"F-C3: the bomb never spawned at all (census {census}) — the bound's "
        "generations must run first; the pin would be green on a spawn that "
        "never fires (the muted engine is the OTHER lie)"
    )
    # THE TYPED REFUSAL ON THE RECORD: the bound's failure NAMES itself —
    # at least one run in the family terminal-failed with the
    # SpawnDepthExceededError class (the deterministic route, the ladder
    # unburned). The family is the ROWS' parent-link walk (the node rows
    # carry no workflow metadata — the tree walk is the honest scope).
    family = await wf_conn.fetch(
        f"""
        WITH RECURSIVE tree AS (
            SELECT $1::uuid AS fid
            UNION
            SELECT (j.metadata->>'flow_id')::uuid
            FROM tree t
            JOIN "{wf_schema}".jobs j
              ON (j.metadata->>'parent_flow_id')::uuid = t.fid
             AND j.step_key = '__flow__'
        )
        SELECT fid FROM tree
        """,
        flow_id,
    )
    typed = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE error_class = 'SpawnDepthExceededError' "
        "AND (metadata->>'flow_id')::uuid = ANY($1::uuid[])",
        [str(r["fid"]) for r in family],
    )
    assert int(typed or 0) >= 1, (
        "F-C3: the depth bound never fired typed — the refusal must NAME "
        "itself on the record (the loud law), not stop the mint silently"
    )


# ── F-C4: THE CASCADE'S STACK ───────────────────────────────────────────

#: The convicted depth: the Python-recursive cascade died RecursionError
#: at ~984 of 1200 (observed live), leaving 216 roots RUNNING.
C4_DEPTH = 1200


@pytest.mark.integration
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

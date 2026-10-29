"""THE CHILD-FLOW CURES' PINS — the rv5-child pin pack (the child-flows
attack front's convictions, cured red-first on the
``feat/taskqflow-child-cures`` branch off the child line's head
``f8bfc5033``).

THE RED RECEIPTS (all captured live BEFORE the cures, on the base head
against the lane's :5812 — the capture rides
``.measurements/child-cures/rv5-child-red-first.txt``):

* RV5-C1 (THE ASYNCIO.GATHER DRAGON — GRAND-MERGE BLOCKER): the natural
  fan-out spelling ``gather(*[ctx.spawn_flow(...) for t in tenants])``
  minted ONE child and returned tenant-0's verdict K times wearing K
  labels — the per-attempt spawn cursor's read/write was not atomic, so
  K concurrent spawns all adopted the same slot (run key), and the
  arbiter's conflict path handed every caller THE ONE child's handle.
  ZERO errors, green pins, silent fabricated results.
* RV5-C2 (THE RECURSION BOMB + THE RECURSIVE CASCADE — GRAND-MERGE
  BLOCKER): (a) a self-spawning flow minted a generation per sweep tick
  forever — no depth bound, no cycle check; (b) the cancel cascade was
  Python-recursive — a 1200-deep cascade raised ``RecursionError`` at
  the operator and left hundreds of runs running.
* RV5-C3 (THE RETENTION-RESPAWN FAIL-CLOSE): purging a child's rows +
  resuming the parent SILENTLY RE-EXECUTED the child (the arbiter's row
  went with the purge → the spawn re-minted a fresh child → the child's
  side effects ran twice, the parent SUCCEEDED — the worst face). The
  fail-close is now the typed arm: the dangling child pointer raises.
* RV5-C4 (THE ORPHAN WINDOW): the crash between child-create and
  receipt-write, then a parent cancel, let the child ESCAPE the cascade
  forever — the cascade walked the crash-unsafe receipts and never read
  the crash-safe ``parent_flow_id`` direction the rows already carry.
* RV5-C5 (THE SMALLER FACES): (a) the dev-loop tick never ran the
  expiry arm — ``timeout_s`` was inert in dev drives; (b) the admin run
  explorer was child-blind — the parent↔child links the rows carry
  never rendered; (c) resolving a child-join hold no-op-bounced (the
  resolve delivered, the body re-executed, the live child re-held) —
  the typed refusal now teaches where the resolution actually lives.

The cure choices are the reads' choices, and the pins hold them. Pin
names carry the rv5 stem (the attack front's pack: these ARE the flip
targets — the rv5-child branch's pins were not visible at cure time,
so the cures' pins name themselves rv5-consistently).
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import (
    DEFAULT_SPAWN_MAX_DEPTH,
    ChildOk,
    Expired,
    FlowRunner,
    GateDecl,
    HitlClient,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    cancel_workflow_run,
    step,
)

#: The cascade's depth the recursion pin drives (the convicted shape's
#: own number — the pre-cure Python-recursive walk raised
#: ``RecursionError`` inside this depth).
_CASCADE_DEPTH = 1200


# ── the app pairs: every test builds its OWN (the D1 registry is
# global — the workflow names are UNIQUE PER TEST). ──────────────────────


class SubProbe(BaseModel):
    doc_id: str


class SubReport(BaseModel):
    findings: list[str]


class ParentIn(BaseModel):
    doc_id: str


class ParentOut(BaseModel):
    findings: list[str]
    verdict: str


def _child_app(name: str) -> WorkflowApp:
    """The child workflow's app: one step echoing the flow input."""
    app = WorkflowApp()

    @app.actor(queue="cpu", max_attempts=1)
    async def probe(ctx: StepContext) -> SubReport:
        probe_input = SubProbe.model_validate(ctx.input)
        return SubReport(findings=[f"found:{probe_input.doc_id}"])

    @app.workflow(name)
    def rv5_child() -> Promise[object]:
        return build(step(probe, key="probe"))

    return app


def _parent_app(child_name: str, name: str, *, body: Any = None) -> WorkflowApp:
    """The parent workflow's app: one step spawning + joining the child."""
    app = WorkflowApp()

    if body is None:

        async def join_body(ctx: StepContext, params: ParentIn) -> ParentOut:
            handle = await ctx.spawn_flow(
                child_name,
                input=SubProbe(doc_id=params.doc_id),
                result=SubReport,
            )
            outcome = await handle.wait()
            match outcome:
                case ChildOk(result=report):
                    return ParentOut(findings=report.findings, verdict="ok")
                case Expired():
                    return ParentOut(findings=[], verdict="expired")
                case _:
                    return ParentOut(findings=[], verdict="not-ok")

    else:
        join_body = body

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> ParentOut:
        return await join_body(ctx, params)  # type: ignore[operator]

    @app.workflow(name)
    def rv5_parent() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    return app


async def _run_parent_to_terminal(
    parent_app: WorkflowApp,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    name: str,
    *,
    max_ticks: int = 4000,
) -> tuple[JobId, str, FlowRunner]:
    """The FULL dev-loop drive: the child is a real flow — the driver's
    tick walks the receipts and executes the descendant flows."""
    runner = FlowRunner(parent_app.get(name), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=max_ticks)
    return claim.flow_id, outcome, runner


async def _node_row(
    wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId, step_key: str
) -> dict[str, Any]:
    row = await wf_conn.fetchrow(
        f'SELECT id, status, metadata, error_class FROM "{wf_schema}".jobs '
        "WHERE step_key = $1 AND (metadata->>'flow_id')::uuid = $2",
        step_key,
        flow_id,
    )
    assert row is not None, f"no {step_key!r} row under {flow_id}"
    node = dict(row)
    raw_meta = node["metadata"]
    node["metadata"] = json.loads(raw_meta) if isinstance(raw_meta, str) else raw_meta
    return node


async def _child_roots(
    wf_conn: asyncpg.Connection, wf_schema: str, parent_flow_id: JobId
) -> list[dict[str, Any]]:
    """The child ROOT rows the rows' parent pointer names (the
    crash-safe direction — the receipts' shape is read separately)."""
    rows = await wf_conn.fetch(
        f"SELECT id, payload FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
        "AND (metadata->>'parent_flow_id')::uuid = $1 ORDER BY id",
        parent_flow_id,
    )
    out = []
    for r in rows:
        d = dict(r)
        raw = d["payload"]
        d["payload"] = json.loads(raw) if isinstance(raw, str) else raw
        out.append(d)
    return out


async def _spawn_and_hold(runner: FlowRunner, flow_id: JobId) -> None:
    """ONE body execution of the parent's spawn_join node: the spawn +
    the join hold (the body re-executes from the top on the later
    resume)."""
    rows = await runner._claimable(flow_id)
    assert len(rows) == 1, f"expected exactly the spawn_join node, got {rows}"
    await runner._run_node(flow_id, rows[0])


async def _descendants(
    wf_conn: asyncpg.Connection, wf_schema: str, root_flow_id: JobId
) -> list[JobId]:
    """Every descendant flow root of *root_flow_id* (the rows'
    parent_flow_id walk — the same direction the depth bound reads)."""
    rows = await wf_conn.fetch(
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
        SELECT fid FROM tree WHERE fid <> $1
        """,
        root_flow_id,
    )
    return [JobId(str(r["fid"])) for r in rows]


# ── RV5-C1: THE GATHER DRAGON (the spawn's atomicity face) ──────────────


class TestRv5C1GatherDragonAtomicity:
    async def test_gather_spawns_mint_k_distinct_children_never_tenant_zero_k_times(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE DRAGON'S KILL PIN: ``gather`` over K spawn_flow calls —
        the natural fan-out spelling — must mint K DISTINCT children,
        each carrying ITS OWN input, and every receipt must name its own
        child (no tenant-0 x K, no dropped tenants, no fabricated
        per-caller verdicts)."""
        _child_app("rv5_c1_child").get("rv5_c1_child")

        async def gather_body(ctx: StepContext, params: ParentIn) -> ParentOut:
            probes = [SubProbe(doc_id=f"{params.doc_id}-{i}") for i in range(5)]
            handles = await asyncio.gather(
                *[ctx.spawn_flow("rv5_c1_child", input=p, result=SubReport) for p in probes]
            )
            verdicts: list[str] = []
            for handle in handles:
                outcome = await handle.wait()
                match outcome:
                    case ChildOk(result=report):
                        verdicts.append(report.findings[0])
                    case Expired():
                        verdicts.append("expired")
                    case _:
                        verdicts.append("not-ok")
            return ParentOut(findings=verdicts, verdict="ok")

        parent_app = _parent_app("rv5_c1_child", "rv5_c1_parent", body=gather_body)
        flow_id, outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "rv5_c1_parent"
        )
        assert outcome == "terminal"

        # THE ROWS' LAW: K DISTINCT child roots under this parent, each
        # carrying its OWN input (the receipts prove no cross-served
        # verdict, no dropped tenant).
        roots = await _child_roots(wf_conn, wf_schema, flow_id)
        assert len(roots) == 5, f"expected 5 children, got {len(roots)}"
        inputs = sorted(r["payload"]["wf_input"]["doc_id"] for r in roots)
        assert inputs == [f"d1-{i}" for i in range(5)], inputs

        # THE RECEIPTS: one per child, no dupes (the K labels each name
        # their own child).
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        receipts = parent_node["metadata"]["child_flows"]
        assert sorted(receipts) == sorted(str(r["id"]) for r in roots)

    async def test_the_same_key_concurrent_spawn_adopts_one_child_all_callers_agree(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE HONEST SHARED FACE: a SAME-key concurrent spawn ADOPTS the
        same child and EVERY caller receives THAT child's handle/outcome
        — never a fabricated per-caller verdict."""
        _child_app("rv5_c1_same_child").get("rv5_c1_same_child")

        async def same_key_body(ctx: StepContext, params: ParentIn) -> ParentOut:
            probes = [SubProbe(doc_id=f"{params.doc_id}-{i}") for i in range(5)]
            handles = await asyncio.gather(
                *[
                    ctx.spawn_flow(
                        "rv5_c1_same_child", input=p, result=SubReport, key="the-shared-site"
                    )
                    for p in probes
                ]
            )
            flow_ids = {str(h.flow_id) for h in handles}
            assert len(flow_ids) == 1, f"the same key must adopt ONE child, got {flow_ids}"
            outcome = await handles[0].wait()
            match outcome:
                case ChildOk(result=report):
                    return ParentOut(findings=report.findings, verdict="ok")
                case _:
                    return ParentOut(findings=[], verdict="not-ok")

        parent_app = _parent_app("rv5_c1_same_child", "rv5_c1_same_parent", body=same_key_body)
        flow_id, outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "rv5_c1_same_parent"
        )
        assert outcome == "terminal"
        roots = await _child_roots(wf_conn, wf_schema, flow_id)
        assert len(roots) == 1, f"the same key must mint ONE child, got {len(roots)}"

    async def test_input_fidelity_two_input_shapes_each_child_carries_its_own(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE INPUT-FIDELITY PIN: two DIFFERENT input shapes through one
        gather fan-out — each child's rows carry ITS OWN input (shape +
        values), never a sibling's."""
        app = WorkflowApp()

        class ShapeA(BaseModel):
            doc_id: str
            shape: str
            only_a: int

        class ShapeB(BaseModel):
            doc_id: str
            shape: str
            only_b: str

        class Echo(BaseModel):
            echo: dict[str, object]

        @app.actor(queue="cpu", max_attempts=1)
        async def echo_step(ctx: StepContext) -> Echo:
            raw = cast("dict[str, object]", ctx.input)
            return Echo(echo=raw)

        @app.workflow("rv5_c1_fid_child")
        def rv5_fid_child() -> Promise[object]:
            return build(step(echo_step, key="echo"))

        async def fid_body(ctx: StepContext, params: ParentIn) -> dict[str, object]:
            _ = params
            inputs: list[object] = [
                ShapeA(doc_id="ta", shape="A", only_a=7),
                ShapeB(doc_id="tb", shape="B", only_b="bee"),
            ]
            handles = await asyncio.gather(
                *[ctx.spawn_flow("rv5_c1_fid_child", input=i, result=Echo) for i in inputs]
            )
            echoes: list[dict[str, object]] = []
            for handle in handles:
                outcome = await handle.wait()
                assert isinstance(outcome, ChildOk)
                echoes.append(outcome.result.echo)
            return {"echoes": echoes}

        @app.actor(queue="cpu", max_attempts=3)
        async def fid_parent(ctx: StepContext, params: ParentIn) -> dict[str, object]:
            return await fid_body(ctx, params)  # type: ignore[operator]

        @app.workflow("rv5_c1_fid_parent")
        def rv5_fid_parent() -> Promise[object]:
            return build(step(fid_parent, ParentIn(doc_id="d1"), key="spawn_join"))

        app.get("rv5_c1_fid_child")  # the registration door (D1)
        runner = FlowRunner(app.get("rv5_c1_fid_parent"), wf_pool, wf_schema)
        claim = await runner.create_flow()
        outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
        assert outcome == "terminal"
        roots = await _child_roots(wf_conn, wf_schema, claim.flow_id)
        assert len(roots) == 2
        payloads = [r["payload"]["wf_input"] for r in roots]
        shapes = sorted(p["shape"] for p in payloads)
        assert shapes == ["A", "B"], shapes
        # Each child's rows carry ITS OWN shape-specific field — the
        # cross-served verdict is unrepresentable.
        a = next(p for p in payloads if p["shape"] == "A")
        b = next(p for p in payloads if p["shape"] == "B")
        assert a["only_a"] == 7 and "only_b" not in a
        assert b["only_b"] == "bee" and "only_a" not in b


# ── RV5-C2a: THE RECURSION BOMB (the depth bound + the cycle check) ─────


class GenIn(BaseModel):
    gen: int = 0


class GenOut(BaseModel):
    gen: int


class TestRv5C2SpawnDepthBound:
    async def test_the_self_spawning_flow_is_bounded_the_typed_error_names_the_fix(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE BOMB'S KILL PIN: a self-spawning flow must be STOPPED by
        the declared depth bound — the typed
        ``SpawnDepthExceededError`` (``FanInBoundExceededError``'s
        sibling) on the record, the child census bounded, the run
        terminal. The pre-cure bomb minted a generation per sweep tick
        forever."""
        app = WorkflowApp()

        @app.actor(queue="cpu", max_attempts=1)
        async def bomb_step(ctx: StepContext) -> GenOut:
            gen_in = GenIn.model_validate(ctx.input)
            handle = await ctx.spawn_flow(
                "rv5_c2_bomb", input=GenIn(gen=gen_in.gen + 1), result=GenOut
            )
            outcome = await handle.wait()
            assert isinstance(outcome, ChildOk)
            return GenOut(gen=outcome.result.gen)

        @app.workflow("rv5_c2_bomb")
        def rv5_bomb() -> Promise[object]:
            return build(step(bomb_step, key="bomb"))

        runner = FlowRunner(app.get("rv5_c2_bomb"), wf_pool, wf_schema)
        claim = await runner.create_flow(input=GenIn(gen=0))
        outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=600)
        assert outcome == "terminal", (
            "the bomb never terminalized — the generations grew past the drive"
        )

        # THE CENSUS IS BOUNDED at the declared depth (each generation
        # is one child root of the bomb's tree).
        descendants = await _descendants(wf_conn, wf_schema, claim.flow_id)
        assert len(descendants) <= DEFAULT_SPAWN_MAX_DEPTH, (
            f"the bomb minted {len(descendants)} children past the bound"
        )

        # THE TYPED ERROR ON THE RECORD: the bound's refusal names the
        # fix (the error class rides the deepest node's row — the bomb's
        # whole tree walked from the original root via the rows' parent
        # links).
        tree_ids = await _descendants(wf_conn, wf_schema, claim.flow_id)
        typed = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE error_class = 'SpawnDepthExceededError' "
            "AND (metadata->>'flow_id')::uuid = ANY($1::uuid[])",
            [str(t) for t in tree_ids],
        )
        assert int(typed or 0) >= 1, "the depth bound never fired typed"

    async def test_the_ancestor_cycle_fires_the_same_bound(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE CYCLE CHECK (folded into the same ancestor walk): a cycle
        in the rows' parent links cannot run the walk forever — the same
        typed bound fires."""
        _child_app("rv5_c2_cycle_child").get("rv5_c2_cycle_child")

        async def cycle_body(ctx: StepContext, params: ParentIn) -> ParentOut:
            handle = await ctx.spawn_flow(
                "rv5_c2_cycle_child",
                input=SubProbe(doc_id=params.doc_id),
                result=SubReport,
            )
            outcome = await handle.wait()
            match outcome:
                case ChildOk(result=report):
                    return ParentOut(findings=report.findings, verdict="ok")
                case _:
                    return ParentOut(findings=[], verdict="not-ok")

        parent_app = _parent_app("rv5_c2_cycle_child", "rv5_c2_cycle_parent", body=cycle_body)
        runner = FlowRunner(parent_app.get("rv5_c2_cycle_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        # THE FABRICATED CYCLE: the parent's root points at ITSELF (the
        # rows' parent links are operator-writable truth — a corrupted
        # link must be a typed refusal, never a walk that never ends).
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs '
            "SET metadata = metadata || jsonb_build_object('parent_flow_id', id::text) "
            "WHERE id = $1",
            flow_id,
        )

        rows = await runner._claimable(flow_id)
        await runner._run_node(flow_id, rows[0])

        # THE TYPED REFUSAL ON THE RECORD: the deterministic failure
        # terminal-fails the node with the bound's own class (the walk
        # refused; the ladder unburned).
        node_class = await wf_conn.fetchval(
            f'SELECT error_class FROM "{wf_schema}".jobs '
            "WHERE step_key = 'spawn_join' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node_class == "SpawnDepthExceededError", node_class


# ── RV5-C2b + RV5-C4: THE CANCEL CASCADE (iterative + the rows' law) ────


async def _chain_rows(wf_conn: asyncpg.Connection, wf_schema: str, depth: int) -> list[JobId]:
    """A depth-long chain of fabricated flow roots: each level's row is
    its own root (``metadata.flow_id``) + the next level's parent link
    (``metadata.parent_flow_id``) + the next level's receipt
    (``metadata.child_flows``). One row per level serves both cascade
    directions."""
    ids = [JobId(new_uuid()) for _ in range(depth)]
    for i, fid in enumerate(ids):
        meta: dict[str, object] = {"flow_id": str(fid), "workflow": "rv5_chain"}
        if i > 0:
            meta["parent_flow_id"] = str(ids[i - 1])
        if i < depth - 1:
            meta["child_flows"] = [str(ids[i + 1])]
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, step_key, status, metadata) "
            "VALUES ($1, 'wf', 'cpu', '{}', 1, 'transient', '__flow__', 'running', $2::jsonb)",
            fid,
            json.dumps(meta),
        )
    return ids


class TestRv5C2bCancelCascadeIterative:
    async def test_the_1200_deep_cascade_cancels_clean_no_recursion_error(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE CASCADE'S DEPTH LAW: a 1200-deep cascade cancels CLEAN —
        the worklist walk, never Python recursion; every run terminal;
        the operator's face honest (no RecursionError, no 500)."""
        ids = await _chain_rows(wf_conn, wf_schema, _CASCADE_DEPTH)

        await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=ids[0], reason="rv5-c2b")

        left_running = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE step_key = '__flow__' AND id = ANY($1::uuid[]) "
            "AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')",
            [str(i) for i in ids],
        )
        assert int(left_running or 0) == 0, (
            f"{left_running} of {_CASCADE_DEPTH} runs escaped the cascade "
            "(the operator's face lied)"
        )


class TestRv5C4OrphanWindow:
    async def test_the_crash_orphan_child_is_cancelled_by_the_rows_law(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE ORPHAN WINDOW'S KILL PIN: the crash between child-create
        and receipt-write (simulated: the receipt removed from the
        parent's row — the crash window's committed state), then a
        parent cancel — the child must STILL be cancelled by the rows'
        parent_flow_id direction (the crash-safe truth), never escape
        the cascade forever."""
        _child_app("rv5_c4_child").get("rv5_c4_child")
        parent_app = _parent_app("rv5_c4_child", "rv5_c4_parent")
        runner = FlowRunner(parent_app.get("rv5_c4_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        await _spawn_and_hold(runner, flow_id)
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_id = str(parent_node["metadata"]["child_flows"][0])

        # THE CRASH WINDOW'S COMMITTED STATE: the child exists (its rows
        # carry the parent pointers — written in the create's ONE tx),
        # the receipt NEVER landed.
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET metadata = metadata - 'child_flows' WHERE id = $1",
            parent_node["id"],
        )
        child_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1::uuid', child_id
        )
        assert child_status == "running", child_status

        await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=flow_id, reason="rv5-c4")

        child_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1::uuid', child_id
        )
        assert child_status == "cancelled", (
            f"the orphan child escaped the cascade (status {child_status!r})"
        )


# ── RV5-C3: THE RETENTION-RESPAWN FAIL-CLOSE ────────────────────────────


class TestRv5C3ChildLostFailClose:
    async def test_the_purged_child_never_silently_respawns_the_typed_arm(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """THE FAIL-CLOSE'S KILL PIN: purging a held child's rows +
        resuming the parent must NEVER silently re-execute the child —
        the dangling child pointer raises the typed arm, the parent's
        run FAILS honestly (the pre-cure shape re-minted a fresh child:
        emails/payments twice, the parent SUCCEEDED)."""
        _child_app("rv5_c3_child").get("rv5_c3_child")
        parent_app = _parent_app("rv5_c3_child", "rv5_c3_parent")
        runner = FlowRunner(parent_app.get("rv5_c3_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        await _spawn_and_hold(runner, flow_id)
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_id = str(parent_node["metadata"]["child_flows"][0])

        # THE RETENTION PURGE: the child's rows go (the operator's law:
        # the purge must walk the parent links — the docs name it).
        await wf_conn.execute(
            f"DELETE FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid",
            child_id,
        )

        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal"

        # THE RESPAWN'S CENSUS: NO second child was ever minted.
        roots = await _child_roots(wf_conn, wf_schema, flow_id)
        assert len(roots) == 0, (
            f"the resume silently re-executed the purged child ({len(roots)} child rows)"
        )

        # THE TYPED ARM ON THE RECORD: the parent's run FAILED with the
        # dangling-pointer refusal — never a silent success. The NODE
        # row carries the body's error class (the failed run's first
        # question's answer); the root's propagation verdict wraps it.
        root = await wf_conn.fetchrow(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        assert root is not None, "the parent's own root row vanished"
        assert root["status"] == "failed", (
            f"the parent's face after the purge is {root['status']!r} — the silent respawn"
        )
        node_class = await wf_conn.fetchval(
            f'SELECT error_class FROM "{wf_schema}".jobs '
            "WHERE step_key = 'spawn_join' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        assert node_class == "ChildFlowLostError", node_class


# ── RV5-C5a: THE DEV-DRIVE PARITY (the tick runs the expiry arm) ────────


class ContinueApproval(BaseModel):
    verdict: str


class Verdict(BaseModel):
    v: str


class TestRv5C5DevDriveParity:
    async def test_a_dev_drive_runs_the_expiry_arm_timeout_s_fires_expired(
        self, wf_pool: asyncpg.Pool, wf_schema: str
    ) -> None:
        """THE DEV FACE MATCHES THE LEADER'S: a ``timeout_s`` wait in a
        dev drive fires the ``Expired`` member (the tick runs the expiry
        arm) — the pre-cure dev face parked on the hold until
        ``max_ticks`` (the deadline inert)."""
        app = WorkflowApp()

        @app.actor(queue="cpu", max_attempts=3)
        async def waiter(ctx: StepContext, params: ParentIn) -> Verdict:
            _ = params
            outcome = await ctx.wait_signal((ContinueApproval,), reason="rv5-c5a", timeout_s=0.05)
            match outcome:
                case ContinueApproval():
                    return Verdict(v="approved")
                case Expired():
                    return Verdict(v="expired")
            raise AssertionError("unreachable: the union is closed")  # pragma: no cover

        gate = GateDecl(name="ContinueApproval", payload_models=(ContinueApproval,))

        @app.workflow("rv5_c5a_parent")
        def rv5_c5a() -> Promise[object]:
            return build(step(waiter, ParentIn(doc_id="d1"), key="hold", gates=(gate,)))

        runner = FlowRunner(app.get("rv5_c5a_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id, until="terminal", max_ticks=400)
        assert outcome == "terminal", (
            "the dev drive never ran the expiry arm — the timeout_s wait is inert"
        )
        result = Verdict.model_validate(await runner.result(flow_id))
        assert result.v == "expired", result


# ── RV5-C5b + RV5-C5c: THE ADMIN FACES ──────────────────────────────────


class TestRv5C5AdminFaces:
    async def test_the_run_explorer_shows_the_parent_child_links(
        self,
        wf_pool: asyncpg.Pool,
        wf_schema: str,
        wf_conn: asyncpg.Connection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """THE ADMIN IS NOT CHILD-BLIND: the run explorer's page renders
        the parent↔child links the rows already carry (the child flows'
        links on the parent's page, the parent's link on the child's)."""
        import httpx
        from fastapi import FastAPI

        from taskq.web.admin import create_router, setup_admin_state

        monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
        monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
        monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")

        _child_app("rv5_c5b_child").get("rv5_c5b_child")
        parent_app = _parent_app("rv5_c5b_child", "rv5_c5b_parent")
        runner = FlowRunner(parent_app.get("rv5_c5b_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        await _spawn_and_hold(runner, flow_id)
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_id = str(parent_node["metadata"]["child_flows"][0])

        bundle = create_router(wf_pool, schema=wf_schema, base_path="")
        fast_app = FastAPI()
        setup_admin_state(fast_app, bundle)
        fast_app.include_router(bundle.router)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=fast_app), base_url="http://test"
        ) as http:
            parent_html = (await http.get(f"/workflows/{flow_id}")).text
            assert child_id in parent_html, (
                "the parent's run page names no child flow — the admin is child-blind"
            )
            child_html = (await http.get(f"/workflows/{child_id}")).text
            assert str(flow_id) in child_html, (
                "the child's run page names no parent flow — the identity faces are one-way"
            )

    async def test_resolving_a_child_join_hold_is_the_typed_refusal(
        self, wf_pool: asyncpg.Pool, wf_schema: str
    ) -> None:
        """THE HONEST RESOLVE FACE: resolving a child-join hold is the
        TYPED refusal teaching where the resolution actually lives (the
        child's terminal) — the hold SURVIVES; the pre-cure face
        delivered, the body re-executed, the live child re-held (the
        no-op bounce wearing a 200)."""
        _child_app("rv5_c5c_child").get("rv5_c5c_child")
        parent_app = _parent_app("rv5_c5c_child", "rv5_c5c_parent")
        runner = FlowRunner(parent_app.get("rv5_c5c_parent"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        await _spawn_and_hold(runner, flow_id)

        client = HitlClient(wf_pool, schema=wf_schema)
        (hold,) = await client.list(flow_id)
        assert hold.signal_name.startswith("__child__:"), hold.signal_name

        result = await client.resolve(hold.hold_id, {"findings": ["the operator's fake"]})
        assert result.status == "refused", result
        assert "child" in (result.reason or "").lower(), result.reason

        # THE HOLD SURVIVES: nothing moved (the wake arm is the only
        # resolver — the child's terminal).
        (still,) = await client.list(flow_id)
        assert still.hold_id == hold.hold_id and still.status == "held"

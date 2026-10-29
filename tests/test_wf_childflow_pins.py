"""T28 — THE CHILD FLOWS' PINS: the spawn→join happy path (the parent's
body gets the child's typed result; the parent↔child links BOTH
directions), the child-fail face (the parent's ``ChildFailed`` arm —
never a silent swallow), the cancel cascade (the parent cancelled → the
child's rows terminal), the retry's idempotency (the retry ADOPTS the
same child — the two-claims law at the flow level), the fan-out (K
children → the fan-in → the typed results list), the slot law (the
parent's node rests in the held representation — no failed attempt, no
slot held — while the child's work sits in its OWN rows), and the
crash-mid-wait (the wake arm delivers; the successor's reclaim joins;
the hold-stamp reconcile is the belt for a lost resume).

The red-first receipt: this suite ran against the PRE-T28 tree — every
import of the child-flow surface reds (``ChildHandle``/``ChildOk``/
``ChildFailed``/``ChildCancelled`` do not exist; ``ctx.spawn_flow`` is
not on the context); the capture rides
``.measurements/t28-childflow-red-first.txt``.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    ChildCancelled,
    ChildFailed,
    ChildHandle,
    ChildOk,
    Expired,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.engine import render_workflow_sql

# ── the app pairs: every test builds its OWN (the D1 registry is
# global — the workflow names are UNIQUE PER TEST... and ONE name per
# app pair here: the child is a REAL flow, registered like any other). ─


class SubProbe(BaseModel):
    doc_id: str


class SubReport(BaseModel):
    findings: list[str]


class ParentIn(BaseModel):
    doc_id: str


class ParentOut(BaseModel):
    findings: list[str]
    verdict: str


def _child_app(name: str = "t28_child", *, fail: bool = False) -> WorkflowApp:
    """The CHILD workflow's app: one step reading ``ctx.input`` (the
    flow's own input — the spawn's), returning the typed report. The
    name is UNIQUE PER TEST (the D1 registry is global)."""
    app = WorkflowApp()

    @app.actor(queue="cpu", max_attempts=1)
    async def probe(ctx: StepContext) -> SubReport:
        # THE FLOW INPUT'S FACE (the house pattern — the jsonb value
        # re-validates through the declared model's own codec): the
        # spawn's ``input=`` rides the child root's payload; the body
        # re-hydrates it.
        probe_input = SubProbe.model_validate(ctx.input)
        if fail:
            raise RuntimeError("the sub-investigation blew up")
        return SubReport(findings=[f"found:{probe_input.doc_id}"])

    @app.workflow(name)
    def t28_child() -> Promise[object]:
        return build(step(probe, key="probe"))

    return app


def _parent_app(
    child_name: str = "t28_child", *, body: Any = None, name: str = "t28_parent"
) -> WorkflowApp:
    """The PARENT workflow's app: one step spawning + joining the child.
    The body is the DEFAULT happy-path join (the full four-arm match —
    the checker's forcing, spelled) unless overridden. The names are
    UNIQUE PER TEST (the D1 registry is global)."""
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
                case ChildFailed():
                    return ParentOut(findings=[], verdict="failed")
                case ChildCancelled():
                    return ParentOut(findings=[], verdict="cancelled")
                case Expired():
                    return ParentOut(findings=[], verdict="expired")
            raise AssertionError("unreachable: the checker forces the arms")  # pragma: no cover

    else:
        join_body = body

    @app.actor(queue="cpu", max_attempts=3)
    async def parent_actor(ctx: StepContext, params: ParentIn) -> ParentOut:
        return await join_body(ctx, params)  # type: ignore[operator]

    @app.workflow(name)
    def t28_parent() -> Promise[object]:
        return build(step(parent_actor, ParentIn(doc_id="d1"), key="spawn_join"))

    return app


async def _run_parent_to_terminal(
    parent_app: WorkflowApp,
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    name: str = "t28_parent",
) -> tuple[JobId, Any, FlowRunner]:
    """The FULL dev-loop drive: the child is a real flow — the driver's
    tick walks the receipts and executes the descendant flows."""
    runner = FlowRunner(parent_app.get(name), wf_pool, wf_schema)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=4000)
    return claim.flow_id, outcome, runner


async def _spawn_and_hold(runner: FlowRunner, flow_id: JobId) -> None:
    """ONE body execution of the parent's spawn_join node: the spawn +
    the join hold (the body re-executes from the top on the later
    resume). Direct claim+run — the manual staging the crash/slot faces
    need (the dev driver's tick would drive the child in the same
    pass)."""
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


def _child_flow_id(parent_node: dict[str, Any]) -> JobId:
    receipts = parent_node["metadata"].get("child_flows")
    assert isinstance(receipts, list) and len(receipts) == 1, receipts
    return JobId(str(receipts[0]))


class TestSpawnJoinHappyPath:
    async def test_the_parent_body_gets_the_childs_typed_result(
        self, wf_pool: asyncpg.Pool, wf_schema: str
    ) -> None:
        _child_app("t28_child_hp1").get(
            "t28_child_hp1"
        )  # the registration door — the spawn's registry read
        parent_app = _parent_app("t28_child_hp1", name="t28_parent_hp1")
        flow_id, outcome, runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_hp1"
        )
        assert outcome == "terminal"
        # The flow result is the jsonb-decoded value — re-hydrated
        # through the declared model (the house pattern).
        result = ParentOut.model_validate(await runner.result(flow_id))
        assert result.verdict == "ok"
        assert result.findings == ["found:d1"]

    async def test_the_links_carry_both_directions(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        _child_app("t28_child_hp2").get("t28_child_hp2")
        parent_app = _parent_app("t28_child_hp2", name="t28_parent_hp2")
        flow_id, _outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_hp2"
        )

        # THE PARENT'S RECEIPT: the spawning node's row carries the
        # child's flow id.
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_flow_id = _child_flow_id(parent_node)

        # THE CHILD'S PROVENANCE: the child's ROOT row names its parent.
        root = await wf_conn.fetchrow(
            f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', child_flow_id
        )
        assert root is not None, "the child's root row is missing — the child is not a REAL flow"
        raw_root_meta = root["metadata"]
        meta = json.loads(raw_root_meta) if isinstance(raw_root_meta, str) else dict(raw_root_meta)
        assert meta.get("parent_flow_id") == str(flow_id)
        assert meta.get("parent_node_id") == str(parent_node["id"])
        assert meta.get("parent_step_key") == "spawn_join"

    async def test_the_child_is_a_real_flow_its_own_rows_and_terminal(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        _child_app("t28_child_hp3").get("t28_child_hp3")
        parent_app = _parent_app("t28_child_hp3", name="t28_parent_hp3")
        flow_id, _outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_hp3"
        )

        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_flow_id = _child_flow_id(parent_node)
        root = await wf_conn.fetchrow(
            f'SELECT step_key, status FROM "{wf_schema}".jobs WHERE id = $1', child_flow_id
        )
        assert root is not None
        assert root["step_key"] == "__flow__"  # the REAL flow root's own shape
        assert root["status"] == "succeeded"
        # The child has its OWN node rows — a separate flow's census,
        # never the parent's.
        probe = await _node_row(wf_conn, wf_schema, child_flow_id, "probe")
        assert probe["status"] == "succeeded"


class TestChildFailFace:
    async def test_the_failed_child_arrives_as_the_typed_member(
        self, wf_pool: asyncpg.Pool, wf_schema: str
    ) -> None:
        _child_app("t28_child_fail1", fail=True).get("t28_child_fail1")
        parent_app = _parent_app("t28_child_fail1", name="t28_parent_fail1")
        flow_id, outcome, runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_fail1"
        )
        assert outcome == "terminal"
        result = ParentOut.model_validate(await runner.result(flow_id))
        # NEVER a silent swallow: the parent's body DECIDED off the
        # ChildFailed member — the verdict is the body's own arm.
        assert result.verdict == "failed"
        assert result.findings == []


class TestCancelCascade:
    async def test_the_parent_cancelled_cancels_the_childs_rows(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        from taskq.workflows import cancel_workflow_run

        _child_app("t28_child_casc1").get("t28_child_casc1")
        parent_app = _parent_app("t28_child_casc1", name="t28_parent_casc1")
        runner = FlowRunner(parent_app.get("t28_parent_casc1"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        # The spawn + the join hold — the child EXISTS, its work still
        # pending (the manual staging: the dev driver never drove it).
        await _spawn_and_hold(runner, flow_id)
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_flow_id = _child_flow_id(parent_node)

        cancelled = await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=flow_id)
        assert cancelled >= 1

        # THE CASCADE: the child's root AND its nodes terminal.
        child_root = await wf_conn.fetchrow(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', child_flow_id
        )
        assert child_root is not None
        assert child_root["status"] == "cancelled"
        probe = await _node_row(wf_conn, wf_schema, child_flow_id, "probe")
        assert probe["status"] == "cancelled"

        # IDEMPOTENT: a second cancel cancels nothing.
        assert await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=flow_id) == 0


class TestRetryIdempotency:
    async def test_the_resume_adopts_the_same_child_never_duplicates(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        _child_app("t28_child_adopt1").get("t28_child_adopt1")
        parent_app = _parent_app("t28_child_adopt1", name="t28_parent_adopt1")
        flow_id, _outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_adopt1"
        )

        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        receipts = parent_node["metadata"]["child_flows"]

        # THE CHILD CENSUS: exactly ONE child root row exists for the
        # spawn — the body re-executed from the top (the hold's resume)
        # and every re-execution ADOPTED the same child by the run key.
        child_roots = await wf_conn.fetch(
            f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        assert len(child_roots) == 1
        assert str(child_roots[0]["id"]) == receipts[0]
        # And the receipt has no duplicates (the ensure-append's dedup).
        assert len(receipts) == len(set(receipts))

    async def test_a_second_spawn_site_under_an_explicit_key_adopts_too(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The explicit ``key=`` face: the SAME key ADOPTS the same
        child (the caller's identity is the arbiter's rememberer)."""
        _child_app("t28_child_adopt2").get("t28_child_adopt2")

        spawns: list[JobId] = []

        async def twice_body(ctx: StepContext, params: ParentIn) -> ParentOut:
            first = await ctx.spawn_flow(
                "t28_child_adopt2",
                input=SubProbe(doc_id=params.doc_id),
                result=SubReport,
                key="the-probe",
            )
            spawns.append(first.flow_id)
            outcome = await first.wait()
            match outcome:
                case ChildOk():
                    return ParentOut(findings=[], verdict="ok")
                case ChildFailed() | ChildCancelled() | Expired():
                    return ParentOut(findings=[], verdict="not-ok")
            raise AssertionError("unreachable")  # pragma: no cover

        parent_app = _parent_app("t28_child_adopt2", body=twice_body, name="t28_parent_adopt2")
        flow_id, _outcome, _runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_adopt2"
        )
        # The body ran TWICE (the hold's resume re-executes from the
        # top) — both executions recorded the SAME child id.
        assert len(spawns) == 2
        assert spawns[0] == spawns[1]
        child_roots = await wf_conn.fetch(
            f"SELECT id FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' "
            "AND (metadata->>'parent_flow_id')::uuid = $1",
            flow_id,
        )
        assert len(child_roots) == 1


class TestFanOut:
    async def test_k_children_fan_in_to_the_typed_results_list(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        _child_app("t28_child_fan1").get("t28_child_fan1")

        class Bundle(BaseModel):
            verdicts: list[str]
            n: int

        async def fanout_body(ctx: StepContext, params: ParentIn) -> Bundle:
            probes = [SubProbe(doc_id=f"{params.doc_id}-{i}") for i in range(3)]
            handles: list[ChildHandle[SubReport]] = [
                await ctx.spawn_flow("t28_child_fan1", input=p, result=SubReport) for p in probes
            ]
            outcomes = [await h.wait() for h in handles]
            print("T28DEBUG", [repr(o)[:160] for o in outcomes])
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
            return Bundle(verdicts=verdicts, n=len(handles))

        parent_app = _parent_app("t28_child_fan1", body=fanout_body, name="t28_parent_fan1")
        flow_id, outcome, runner = await _run_parent_to_terminal(
            parent_app, wf_pool, wf_schema, "t28_parent_fan1"
        )
        assert outcome == "terminal"
        result = Bundle.model_validate(await runner.result(flow_id))
        assert result.n == 3
        assert result.verdicts == ["found:d1-0", "found:d1-1", "found:d1-2"]

        # K receipts on the parent row, K distinct children, zero dupes.
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        receipts = parent_node["metadata"]["child_flows"]
        assert sorted(receipts) == sorted(set(receipts))
        assert len(receipts) == 3


class TestSlotLaw:
    async def test_the_parent_holds_without_an_attempt_burned_while_the_child_is_its_own_rows(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        from taskq.workflows.api._hitl import sweep_child_joins

        _child_app("t28_child_slot1").get("t28_child_slot1")
        parent_app = _parent_app("t28_child_slot1", name="t28_parent_slot1")
        runner = FlowRunner(parent_app.get("t28_parent_slot1"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        await _spawn_and_hold(runner, flow_id)

        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_flow_id = _child_flow_id(parent_node)

        # THE HELD REPRESENTATION: pending + the hold marker (the slot
        # law — the row is invisible to dispatch, no slot held).
        assert parent_node["status"] == "pending"
        assert "hold" in parent_node["metadata"]

        # THE LEDGER'S FACE: the attempt recorded 'awaited' — never a
        # failed attempt for the hold.
        ledger = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'spawn_join'",
            flow_id,
        )
        assert ledger == "awaited"

        # The child's work sits in its OWN rows, pending — the child is
        # a real flow, executable by ANY driver while the parent holds.
        probe = await _node_row(wf_conn, wf_schema, child_flow_id, "probe")
        assert probe["status"] == "pending"

        # The wake arm on a still-running child: NO wake (the hold
        # stands — the arm is the child-terminal's resolver, never a
        # poll that guesses).
        assert await sweep_child_joins(wf_pool, render_workflow_sql(wf_schema)) == 0


class TestCrashMidWait:
    async def test_the_wake_arm_delivers_and_the_successor_joins(
        self, wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
    ) -> None:
        """The crash-mid-wait face: the parent's attempt dies mid-hold —
        the hold row STANDS (durable); the child terminalizes; the wake
        arm delivers + resumes; the successor's re-claim re-executes the
        body (the spawn adopts; the wait reads the child's terminal) and
        lands the typed result. The belt: a hold stamp that SURVIVED a
        lost resume (the crash between the CAS and the resume) is
        cleared by the hold-stamp reconcile — nothing wedges."""
        from taskq.workflows._sweep import sweep_hold_stamps
        from taskq.workflows.api._hitl import sweep_child_joins

        child_app = _child_app("t28_child_crash1").get("t28_child_crash1")
        parent_app = _parent_app("t28_child_crash1", name="t28_parent_crash1")
        runner = FlowRunner(parent_app.get("t28_parent_crash1"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id

        await _spawn_and_hold(runner, flow_id)
        parent_node = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        child_flow_id = _child_flow_id(parent_node)

        # The attempt "dies" here (no finalize, no resume — the process
        # image ends). The child's driver (any process's) runs it to
        # terminal.
        child_runner = FlowRunner(child_app, wf_pool, wf_schema)
        assert await child_runner.drive(child_flow_id, until="terminal", max_ticks=2000) == (
            "terminal"
        )

        # THE WAKE: the child terminal → the join hold DELIVERED (the
        # child's own driver's tick runs the wake arm — ANY process's
        # sweep resolves the waiter; the explicit pass below is the
        # belt) and the parent's held marker cleared.
        hold_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_signals '
            "WHERE signal_name = $1 ORDER BY hold_epoch DESC LIMIT 1",
            f"__child__:{child_flow_id}",
        )
        assert hold_status == "delivered", hold_status
        assert await sweep_child_joins(wf_pool, render_workflow_sql(wf_schema)) == 0

        # THE SUCCESSOR'S RECLAIM: the parent's node re-claims; the body
        # re-executes from the top (the spawn ADOPTS; the wait reads the
        # child's terminal) — the typed result lands.
        rows = await runner._claimable(flow_id)
        assert len(rows) == 1
        await runner._run_node(flow_id, rows[0])
        finished = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        assert finished["status"] == "succeeded"

        # THE BELT (the lost-resume replica): a hold stamp that survived
        # the resolution is cleared + the row re-claims — the reconcile
        # arm's law, drilled on the delivered hold's row.
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET metadata = metadata || '
            "'{\"hold\": \"orphan-stamp\"}'::jsonb, status = 'pending' "
            "WHERE step_key = 'spawn_join' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        cleared = await sweep_hold_stamps(wf_pool, render_workflow_sql(wf_schema))
        assert cleared >= 1
        row = await _node_row(wf_conn, wf_schema, flow_id, "spawn_join")
        assert "hold" not in row["metadata"]

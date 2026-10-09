# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""THE COVERAGE-CLOSING PINS (the estate floor — 90 branch per module
under taskq/workflows/): the ctx.step surface's branches (the
idempotent opt-out, the step's own terminal on failure, the memoized
replay), the capture policies (none | errors-only | all — the failing
node's capture jsonb), and the CLI analysis's unhit lines. Every test
drives the REAL surfaces — no mocks (a mocked step pins the mock)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import FlowRunner, WorkflowApp, build, loop, step


class Ingest(BaseModel):
    doc_id: str


# ── the ctx.step surface (context.py's branches) ─────────────────────────


async def test_ctx_step_opts_out_of_idempotency_and_reruns(wf_pool: Any, wf_schema: str) -> None:
    """``idempotent=False`` re-runs on redelivery (the body's declared
    harmlessness) — the step ledger records NOTHING for it."""
    calls = {"n": 0}

    async def body(ctx: Any, params: Ingest) -> int:
        inner = await ctx.substep("side-effect", _count, idempotent=False)
        return inner

    async def _count() -> int:
        calls["n"] += 1
        return calls["n"]

    app = WorkflowApp()

    @app.workflow("step_optout")
    def step_optout() -> object:
        return build(step(body, Ingest(doc_id="d1"), key="solo"))

    runner = FlowRunner(app.get("step_optout"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert calls["n"] == 1
    # THE LEDGER: no claim row for the opted-out STEP (the NODE's own
    # claim row exists — the filter names the step).
    rows = await wf_pool.fetch(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 '
        "AND step_key = 'side-effect'",
        flow_id,
    )
    assert int(rows[0][0]) == 0


async def test_ctx_step_the_failing_step_ledgers_failed_and_reraises(
    wf_pool: Any, wf_schema: str
) -> None:
    """A raising step's claim terminalizes 'failed' (the ladder's ledger
    shape) and the exception propagates to the node's own failure
    handling."""
    calls = {"n": 0}

    async def body(ctx: Any, params: Ingest) -> int:
        return await ctx.substep("doomed-step", _explode)

    async def _explode() -> int:
        calls["n"] += 1
        raise RuntimeError("the step's own boom")

    app = WorkflowApp()

    @app.workflow("step_fail")
    def step_fail() -> object:
        return build(step(body, Ingest(doc_id="d1"), key="solo", max_attempts=2))

    runner = FlowRunner(app.get("step_fail"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    rows = await wf_pool.fetch(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 ORDER BY id',
        flow_id,
    )
    statuses = [r["status"] for r in rows]
    assert "failed" in statuses, statuses  # the step's OWN terminal


async def test_ctx_step_the_memoized_replay_returns_the_record(
    wf_pool: Any, wf_schema: str
) -> None:
    """The memoized replay: the retried body's step returns the
    RECORDED result (the cheap side of the re-execution doctrine)."""
    calls = {"n": 0}

    async def body(ctx: Any, params: Ingest) -> dict[str, int]:
        value = await ctx.substep("once", _count)
        if calls["once_seen"] == 0:
            calls["once_seen"] = 1
            raise RuntimeError("the transient between the step and the node's terminal")
        return {"v": value}

    async def _count() -> int:
        calls["n"] += 1
        return calls["n"]

    calls["once_seen"] = 0
    app = WorkflowApp()

    @app.workflow("step_memo")
    def step_memo() -> object:
        return build(step(body, Ingest(doc_id="d1"), key="solo", max_attempts=3))

    runner = FlowRunner(app.get("step_memo"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert calls["n"] == 1, calls  # the step RAN ONCE (the replay returned the record)


# ── the capture policies (capture.py + the finalize's capture writer) ────


async def test_capture_policies_none_and_errors_only(
    wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    """The declared capture policy decides the failing node's capture
    jsonb: none → NO capture; errors-only → the error text, never the
    input."""
    for policy, want_capture in (("none", False), ("errors-only", True)):
        app = WorkflowApp()

        async def fails(ctx: Any, params: Ingest) -> str:
            raise ValueError("the capture probe's boom")

        @app.workflow(f"cap_{policy}", capture=policy)
        def cap_wf() -> object:
            return build(step(fails, Ingest(doc_id="the input text"), key="solo"))

        runner = FlowRunner(app.get(f"cap_{policy}"), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        assert await runner.drive(flow_id) == "terminal"
        rows = await wf_pool.fetch(
            f'SELECT capture FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1',
            flow_id,
        )
        captures = [r["capture"] for r in rows if r["capture"] is not None]
        if want_capture:
            assert captures, f"{policy}: the error capture must ride the ledger"
            text = str(captures[-1])
            assert "boom" in text
        else:
            assert not captures, f"{policy}: the capture is REFUSED by the policy"


# ── the CLI analysis's unhit lines (the honest-zero + the join-wait) ─────


def test_cli_the_join_wait_and_blocked_remedies() -> None:
    """The analysis's remedy lines for the join-wait and the
    blocked-with-reason rows (the why-stuck arm's remaining shapes)."""
    from taskq.workflows._cli import FlowNodeRow, stuck_lines

    join_wait = stuck_lines(
        FlowNodeRow(step_key="the_join", status="pending", deps_pending=3), "r-1"
    )
    assert any("JOIN-WAIT" in line for line in join_wait)
    assert any("remedy: none" in line for line in join_wait)

    blocked = stuck_lines(
        FlowNodeRow(step_key="blocked_one", status="pending", blocking_reason="orphan_parent"),
        "r-1",
    )
    assert any("orphan_parent" in line for line in blocked)


def test_cli_the_undeadlined_holds_line_and_the_status_counts() -> None:
    from taskq.workflows._cli import FlowNodeRow, format_flow_status

    rows = [
        FlowNodeRow(step_key="a", status="pending"),
        FlowNodeRow(step_key="b", status="succeeded"),
        FlowNodeRow(step_key="c", status="pending"),
    ]
    lines = format_flow_status(run_id="r-1", workflow="wf", root_status="running", nodes=rows)
    joined = "\n".join(lines)
    assert "nodes: 3" in joined
    assert "nothing is stuck" in joined
    # The cancel-in-flight line:
    lines = format_flow_status(
        run_id="r-1", workflow="wf", root_status="running", nodes=rows, cancel_in_flight=True
    )
    assert any("cancel: IN FLIGHT" in line for line in lines)


def test_cli_parse_decision_rejects_the_non_object() -> None:
    from taskq.workflows._cli import parse_decision

    with pytest.raises(ValueError, match="JSON object"):
        parse_decision('"a string"')
    assert parse_decision('{"k": [1, 2]}') == {"k": [1, 2]}


def test_cli_the_status_lines_for_the_empty_nodes_and_the_holds_reason() -> None:
    from taskq.workflows._cli import FlowNodeRow, format_flow_status

    # THE EMPTY NODES (the honest degraded line + the remedy):
    lines = format_flow_status(run_id="r-1", workflow="wf", root_status="running", nodes=[])
    joined = "\n".join(lines)
    assert "nodes: none yet" in joined
    assert "taskq flows list" in joined
    # The HOLD's REASON line (the context's reason rides the listing):
    from taskq.workflows.api._hitl import HoldContext

    hold = HoldContext(
        hold_id="h-9",
        run_id="r-1",
        node_key="review",
        signal_name="Approval",
        hold_epoch=2,
        call_id="c",
        payload=None,
        payload_schema=None,
        reason="waiting for the editor",
        created_at=None,
        expires_at="soon",
        status="held",
    )
    lines = format_flow_status(
        run_id="r-1",
        workflow="wf",
        root_status="running",
        nodes=[FlowNodeRow(step_key="review", status="pending", hold=hold)],
    )
    joined = "\n".join(lines)
    assert "waiting for the editor" in joined
    assert "deadline soon" in joined


# ── the split's modules' remaining branches (my surfaces' homes) ─────────


def test_the_version_module_is_importable() -> None:
    """The version module (the version constants' home): the import is
    the pin (the module rides the package's metadata surface)."""
    from taskq.workflows import _version

    assert hasattr(_version, "__doc__")


def test_capture_the_halving_loop_floors_at_one_char() -> None:
    """The truncation's floor branch: a cap so small even ONE character
    overflows — the loop bottoms at chars==1 and returns (the cut
    recorded), never an infinite loop."""
    from taskq.workflows._capture import build_capture, utf8_byte_len

    capture = build_capture(
        policy="errors-only",
        node_input="冰" * 50,
        error="e" * 10,
        max_bytes=8,
        redact=lambda text: text,
    )
    assert capture is not None
    for key in ("input", "error"):
        if key in capture:
            assert utf8_byte_len(str(capture[key])) <= max(8, 4), key


def test_capture_the_marker_survives_the_truncation() -> None:
    """The truncation marker (the dropped-char count) rides the row
    when the cut happened — the record never lies silently."""
    from taskq.workflows._capture import TRUNCATED_MARKER, build_capture

    capture = build_capture(
        policy="errors-only",
        node_input="x" * 2000,
        error="the error text",
        max_bytes=64,
        redact=lambda text: text,
    )
    assert capture is not None
    assert TRUNCATED_MARKER in capture, "the truncation was silent (the record lies)"


async def test_ctx_the_runtime_fields_on_the_step_and_the_loop_paths(
    wf_pool: Any, wf_schema: str
) -> None:
    """The runtime fields' CONSTRUCTION paths: the step ctx (the queue +
    the claim ts + the flow name) and the loop ctx (the budget's
    remaining read — the first-claim arm's own value)."""
    from taskq.workflows import Done

    seen: dict[str, Any] = {}

    app = WorkflowApp()

    async def step_body(ctx: Any, params: Ingest) -> list[str]:
        seen["step_ctx"] = (ctx.flow_name, ctx.queue, ctx.claimed_at is not None)
        return ["a", "b"]

    async def loop_body(ctx: Any, carry: int) -> Done[str]:
        seen["loop_ctx"] = (ctx.flow_name, ctx.budget_remaining_ms)
        return Done("x")

    @app.workflow("ctx_runtime_fields")
    def ctx_runtime_fields() -> object:
        first = step(step_body, Ingest(doc_id="d1"), key="first", queue="q1")
        the_loop = loop("the_loop", loop_body, initial=0, budget_s=1800.0)
        return build(the_loop, first)

    runner = FlowRunner(app.get("ctx_runtime_fields"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    assert seen["step_ctx"] == ("ctx_runtime_fields", "q1", True), seen
    assert seen["loop_ctx"] == ("ctx_runtime_fields", 1_800_000), seen


def test_cli_the_gate_models_the_empty_gates_path() -> None:
    """gate_models_for on a node with NO declared gates → the empty
    tuple (the typed door's 422 names it)."""
    from taskq.workflows import WorkflowApp, build, step
    from taskq.workflows._cli import gate_models_for

    app = WorkflowApp()

    async def body(ctx: Any, params: Ingest) -> str:
        return "ok"

    @app.workflow("gate_empty")
    def gate_empty() -> object:
        return build(step(body, Ingest(doc_id="d"), key="solo"))

    assert gate_models_for(app, "gate_empty", "solo") == ()

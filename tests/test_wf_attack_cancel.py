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
    flow_id = await runner.create_flow()
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

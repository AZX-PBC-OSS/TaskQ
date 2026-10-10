# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""TEARDOWN-REVIEW PINS — the rv3 cut-and-cure lane's convictions
(feat/taskqflow @ 512f7a91; the teardown review of the workflows
interface).

Provenance: the teardown reviewer's offered cut, this lane's pack. Per
the doctrine, a LANDED finding is pinned asserting the SAFE behavior
under ``pytest.mark.xfail(strict=True)`` — the cure flips the pin to
XPASS-strict (a red that says: remove the marker WITH the cure); the
pin then stands as the green guard. Every finding here was run
UNMARKED first (the red captured in RECEIPTS — the
``tests/test_wf_teardown_rv3_RECEIPTS.md`` protocol) before the marker
went on.

The findings (each convicted live at the pin head):

* F-RV3-1 — ``map_source``'s ``key=`` param is the can-only-error seat:
  every value but the derived key raises ``WorkflowBuildError`` (the
  accept-and-ignore class's guard), so the param's only possible
  outcome is the author's own refusal — the param should not exist.
  THE CURE IS A REMOVAL (documented in the changelog + the guide).
* F-RV3-2 — the packaged run's first contact on an unmigrated schema
  raises the RAW ``asyncpg.exceptions.UndefinedTableError`` — the
  client arm's typed ``SchemaNotMigratedError`` translation exists and
  the packaged door does not use it.
* F-RV3-3 — ``apply_pending`` takes a raw ``asyncpg.Connection``; a
  Pool arrives at the first statement's convenience method and dies
  the untyped ``AttributeError`` (the Pool has no ``transaction()``).
* F-RV3-4 — THE E2-ANALOG HOLE IN THE GATE WIRING: a DECLARED gate the
  body never waits on, and a body's ``wait_signal`` with NO declared
  gate, both validate CLEAN — the compile-visible gate seat and the
  bodies' waits are never walked against each other (E2 walks the
  promise wiring; the gate wiring has no walk).
* F-RV3-5 — ``cli.py``'s ``if __name__ == "__main__":`` block sits
  MID-MODULE (before the workgroup/flows tails): ``python -m
  taskq.cli`` runs ``main()`` with 18 commands unregistered — the
  entry-point help advertises verbs the ``-m`` path dies on.
* F-RV3-6 — the ``schema`` param carries TWO conventions among the
  read-face siblings: keyword-only on ``HitlClient``, positional on
  ``FlowRunner`` / ``run()`` (the majority). ONE convention: positional
  (``HitlClient`` is unreleased — the fix is free).
* F-RV3-7 — ``FlowRunner.result()`` on a FAILED run returns the
  terminal node's empty read — failed and running indistinguishable at
  the read face; ``WorkflowRunError`` exists and is never raised by the
  read.
* F-RV3-8 — the ``@app.actor`` canonical path defeats the code-version
  stamper: the node's body is the ``WorkflowActor`` WRAPPER, so the
  stamp hashes the wrapper's identity (every node the same constant —
  §22.1's deploy-drift feature dead where the docs teach).
* F-RV3-9 — the hold's reason has TWO homes: the wait site's
  ``reason=`` lands in the row's ``payload['reason']`` (populated) and
  ``HoldContext.reason`` reads ``None`` — always.
* F-RV3-10 — the gate timeout is DOUBLE-SOURCED (the wait site's
  ``timeout_s=`` arms the runtime; ``GateDecl.timeout_s`` feeds the
  compile surfaces) with the precedence UNDOCUMENTED on the
  declaration, and the two sources never cross-checked where they are
  cross-checkable (both literals, disagreeing) — plus the ``GateDecl``
  docstring's stale raise-face text.
* F-RV3-12 — the loop's shape law ("a loop body declares ONE wait per
  iteration") is DOCSTRING-ENFORCED: a conditional-interior
  ``wait_signal`` in a loop body mis-indexes the answer cursor (the
  T26 review's C9 is the same question) and nothing names it.

CURE ORDER (the reviewer's): F-RV3-4 (E14 — the headline), F-RV3-7,
F-RV3-5, F-RV3-2/F-RV3-3, F-RV3-8/F-RV3-6/F-RV3-9, F-RV3-10/F-RV3-1/
F-RV3-12. Each cure lands WITH its pin (the marker removed — the pin
becomes the green guard) in the same commit.
"""

from __future__ import annotations

import inspect
import subprocess  # Why: the -m pin IS a subprocess question — the -m path only exists in a fresh interpreter.
import sys
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.exceptions import SchemaNotMigratedError
from taskq.migrate import apply_pending
from taskq.workflows import (
    Done,
    GateDecl,
    HitlClient,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    WorkflowRunError,
    build,
    loop,
    map_source,
    run,
    sink,
    step,
)
from taskq.workflows.api._runner import FlowRunner
from taskq.workflows.api._validate import (
    WorkflowValidationError,
    validate_compiled,
)


class _Ingest(BaseModel):
    doc_id: str


class _Approval(BaseModel):
    verdict: str


class _Decision(BaseModel):
    ok: bool


class _Carry(BaseModel):
    n: int = 0


def _compile_quietly(app: WorkflowApp, name: str) -> Any:
    """The validator's own probe seam (``WorkflowApp._compile``): compile
    WITHOUT the registration door — the pins read the DIAGNOSTICS (the
    W-rules' faces), which the door's raise-on-error hides."""
    return app._compile(name)


# ── F-RV3-4: the gate wiring's E2-analog (THE HEADLINE) ────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-4a: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_4a_a_declared_gate_the_body_never_waits_is_a_build_refusal() -> None:
    """E14 (the gate-wiring walk — E2's analog): a node declaring a
    ``GateDecl`` whose body carries NO ``wait_signal`` reference AT ALL
    is the provable wiring lie — the compile-visible hold seat with no
    waiter (work that waits for nobody). The refusal is at BUILD, like
    E2's produced-never-consumed."""
    app = WorkflowApp()

    async def reviewer(ctx: StepContext, params: _Ingest) -> _Decision:
        return _Decision(ok=True)

    @app.workflow("rv3-4a-gate-never-waited")
    def build_wf() -> Promise[_Decision]:
        p = step(
            reviewer,
            _Ingest(doc_id="d"),
            gates=(GateDecl(name="Review", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    with pytest.raises(WorkflowValidationError, match="E14"):
        app.get("rv3-4a-gate-never-waited")


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-4b: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_4b_a_wait_with_no_declared_gate_is_a_build_refusal() -> None:
    """E14's other provable face: a body whose ``wait_signal`` has NO
    declared gate — the hold the admin's resolve/deliver doors cannot
    see (the declaration is the compile's visibility). Refused at
    build, like E2."""
    app = WorkflowApp()

    async def approver(ctx: StepContext, params: _Ingest) -> _Decision:
        verdict = await ctx.wait_signal((_Approval,), timeout_s=60.0)
        return _Decision(ok=verdict.verdict == "go")

    @app.workflow("rv3-4b-wait-never-declared")
    def build_wf() -> Promise[_Decision]:
        p = step(approver, _Ingest(doc_id="d"))
        return build(p)

    with pytest.raises(WorkflowValidationError, match="E14"):
        app.get("rv3-4b-wait-never-declared")


def test_rv3_4c_the_conditional_interior_wait_is_not_the_static_refusal() -> None:
    """E14's ZERO-FALSE-POSITIVE bound (the reviewer's own rule): a
    declared gate whose body DOES reference ``wait_signal`` is never
    E14-convicted — the static walk cannot prove a conditional-interior
    wait never fires (that face is the documented C9/W-face, F-RV3-12's
    rule). The gate declared + the wait referenced validates past E14."""
    app = WorkflowApp()

    async def maybe_gated(ctx: StepContext, params: _Ingest) -> _Decision:
        if params.doc_id:
            approval = await ctx.wait_signal((_Approval,), timeout_s=30.0)
            return _Decision(ok=approval.verdict == "go")
        return _Decision(ok=True)

    @app.workflow("rv3-4c-conditional-wait-declared")
    def build_wf() -> Promise[_Decision]:
        p = step(
            maybe_gated,
            _Ingest(doc_id="d"),
            gates=(GateDecl(name="Approval", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    compiled = _compile_quietly(app, "rv3-4c-conditional-wait-declared")
    diagnostics = validate_compiled(compiled)
    assert not [d for d in diagnostics if d.rule.startswith("E14")], (
        "E14 convicted a conditional-interior wait — the static walk "
        "cannot prove that wait never fires (the zero-false-positive bound)"
    )


# ── F-RV3-7: result()'s failure face ────────────────────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-7: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
async def test_rv3_7_a_failed_run_s_result_read_names_the_failure(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The failed run's first question — WHAT failed, and why — answered
    at the read face: ``FlowRunner.result()`` raises the typed
    ``WorkflowRunError`` carrying the row's error class, never the
    empty read that makes failed and running indistinguishable."""
    app = WorkflowApp()

    async def doomed(ctx: StepContext, params: _Ingest) -> _Decision:
        raise RuntimeError("boom-the-teardown")

    @app.workflow("rv3-7-failed-read")
    def build_wf() -> Promise[_Decision]:
        p = step(doomed, _Ingest(doc_id="d"), max_attempts=1, retry_kind="permanent")
        return build(p)

    compiled = app.get("rv3-7-failed-read")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    claim = await runner.create_flow(input=_Ingest(doc_id="d"))
    await runner.drive(claim.flow_id)
    with pytest.raises(WorkflowRunError, match="RuntimeError"):
        await runner.result(claim.flow_id)


# ── F-RV3-5: the -m path's dead verbs ───────────────────────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-5: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_5_the_dash_m_path_serves_the_whole_verb_inventory() -> None:
    """``python -m taskq.cli --help`` must advertise EVERY verb the
    imported app registers (the console script's face): the __main__
    block runs main() mid-module, so every command registered after it
    is dead on the -m path while the group help (the fully-loaded
    module) advertises it."""
    from typer.main import get_command

    from taskq.cli import app as cli_app

    proc = subprocess.run(
        [sys.executable, "-m", "taskq.cli", "--help"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, f"the -m help itself failed: {proc.stderr[-400:]}"
    group = get_command(cli_app)
    assert group is not None and hasattr(group, "commands")
    dead = [verb for verb in group.commands if verb not in proc.stdout]
    assert not dead, (
        f"verbs the imported app registers that the -m path's own help "
        f"does not serve: {dead} — the __main__ block runs main() "
        "mid-module; every command defined after it is dead on the -m path"
    )


# ── F-RV3-6: ONE schema convention ──────────────────────────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-6: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_6_the_schema_param_carries_one_convention() -> None:
    """The read-face siblings carry the schema param under ONE
    convention — POSITIONAL (the majority: ``FlowRunner``,
    ``run()``). ``HitlClient``'s keyword-only seat is the minority
    convention (and the class is unreleased — the fix is free)."""
    from taskq.workflows.api._hitl import HitlClient as Client
    from taskq.workflows.api._runner import FlowRunner as Runner

    client_kind = inspect.signature(Client.__init__).parameters["schema"].kind
    runner_kind = inspect.signature(Runner.__init__).parameters["schema"].kind
    assert client_kind is inspect.Parameter.POSITIONAL_OR_KEYWORD, (
        f"HitlClient's schema is {client_kind.name} while FlowRunner's is "
        "positional — two conventions among the read-face siblings; the "
        "positional majority is the convention (HitlClient is unreleased)"
    )
    assert runner_kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


# ── F-RV3-2: the packaged run's unmigrated-schema contact ───────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-2: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
async def test_rv3_2_the_packaged_run_names_the_unmigrated_schema(
    wf_pool: asyncpg.Pool, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The packaged run's first contact on a schema that EXISTS but was
    never migrated is the TYPED ``SchemaNotMigratedError`` (the client
    arm's translation, extended to the packaged door) — never the raw
    asyncpg ``UndefinedTableError``."""
    app = WorkflowApp()

    async def ok_body(ctx: StepContext, params: _Ingest) -> _Decision:
        return _Decision(ok=True)

    @app.workflow("rv3-2-unmigrated-contact")
    def build_wf() -> Promise[_Decision]:
        p = step(ok_body, _Ingest(doc_id="d"))
        return build(p)

    compiled = app.get("rv3-2-unmigrated-contact")
    bare = f"{wf_schema}_rv3_bare"
    await wf_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{bare}"')
    try:
        with pytest.raises(SchemaNotMigratedError, match=bare):
            await run(compiled, wf_pool, bare)
    finally:
        await wf_conn.execute(f'DROP SCHEMA IF EXISTS "{bare}" CASCADE')


# ── F-RV3-3: apply_pending's Pool is the typed refusal ─────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-3: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
async def test_rv3_3_apply_pending_names_the_connection_it_refuses() -> None:
    """``apply_pending`` takes ONE ``asyncpg.Connection`` (the caller's
    transaction scope); a POOL is the typed refusal naming the
    Connection — never the untyped ``AttributeError`` the Pool's
    missing ``transaction()`` dies with at the first statement."""
    pool = await asyncpg.create_pool(
        "postgresql://taskq-refuses-pools@localhost:1/nothing", min_size=0
    )
    try:
        with pytest.raises(Exception, match="Connection"):
            await apply_pending(pool, schema="taskq")
    finally:
        await pool.close()


# ── F-RV3-8: the stamper reads the wrapper's INNER fn ──────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-8: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
async def test_rv3_8_the_actor_wrapped_bodies_stamp_their_own_code(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """§22.1's deploy-drift record: a node claimed through the
    ``@app.actor`` canonical path stamps the INNER function's canonical
    hash — two different bodies stamp DIFFERENT versions, each equal to
    the body's own ``compute_code_version``. The wrapper's identity
    (one constant hash for every node) is the finding."""
    from taskq.workflows._version import compute_code_version

    app = WorkflowApp()

    @app.actor(queue="default")
    async def alpha_body(ctx: StepContext, params: _Ingest) -> _Decision:
        return _Decision(ok=True)

    @app.actor(queue="default")
    async def beta_body(ctx: StepContext, params: _Ingest) -> _Decision:
        return _Decision(ok=False)

    @app.workflow("rv3-8-inner-stamps")
    def build_wf() -> Promise[_Decision]:
        a = step(alpha_body, _Ingest(doc_id="1"))
        p = step(beta_body, _Ingest(doc_id="2"))
        sink(a)
        return build(p)

    compiled = app.get("rv3-8-inner-stamps")
    outcome = await run(compiled, wf_pool, wf_schema)
    assert outcome.outcome == "terminal"
    rows = await wf_pool.fetch(
        f'SELECT step_key, code_version FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        outcome.flow_id,
    )
    stamps = {r["step_key"]: r["code_version"] for r in rows}
    assert set(stamps) == {"alpha_body", "beta_body"}, f"the run's nodes: {sorted(stamps)}"
    expected = {
        "alpha_body": compute_code_version(
            alpha_body.fn.__module__,
            alpha_body.fn.__qualname__,
            inspect.getsource(alpha_body.fn),
        ),
        "beta_body": compute_code_version(
            beta_body.fn.__module__,
            beta_body.fn.__qualname__,
            inspect.getsource(beta_body.fn),
        ),
    }
    assert stamps["alpha_body"] == expected["alpha_body"], (
        "alpha's stamp is not the inner body's canonical hash — the "
        "stamper hashed the WorkflowActor wrapper's identity"
    )
    assert stamps["beta_body"] == expected["beta_body"], (
        "beta's stamp is not the inner body's canonical hash"
    )
    assert stamps["alpha_body"] != stamps["beta_body"], (
        "two different bodies stamped the SAME code_version — the "
        "deploy-drift record cannot distinguish them (the wrapper's "
        "constant identity is the finding)"
    )


# ── F-RV3-9: the hold's reason — ONE home ───────────────────────────────


@pytest.mark.integration
@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-9: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
async def test_rv3_9_the_hold_s_reason_rides_its_own_field(
    wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """The wait site's ``reason=`` is readable at the TYPED field:
    ``HoldContext.reason`` carries what the insert wrote (the row's
    payload is the home; ``.reason`` is the read face fed from it) —
    never the hardcoded ``None`` that made the reason two homes."""
    app = WorkflowApp()

    async def gated(ctx: StepContext, params: _Ingest) -> _Decision:
        approval = await ctx.wait_signal(
            (_Approval,), timeout_s=30.0, reason="awaiting compliance sign-off"
        )
        return _Decision(ok=approval.verdict == "go")

    @app.workflow("rv3-9-hold-reason")
    def build_wf() -> Promise[_Decision]:
        p = step(
            gated,
            _Ingest(doc_id="d"),
            gates=(GateDecl(name="Approval", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    compiled = app.get("rv3-9-hold-reason")
    outcome = await run(compiled, wf_pool, wf_schema, until="held")
    assert outcome.outcome == "held"
    client = HitlClient(wf_pool, wf_schema)
    holds = await client.list(run=outcome.flow_id)
    assert holds, "the held run must carry its hold"
    reason = holds[0].reason
    assert reason is not None, (
        "HoldContext.reason is None while the row's payload carries the "
        "wait site's reason — two homes; the typed field is the read face"
    )
    assert "compliance sign-off" in reason


# ── F-RV3-10: the gate timeout's one precedence + the cross-check ───────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-10: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_10_the_gate_timeout_sources_agree_or_the_drift_is_named() -> None:
    """The timeout's cross-checkable drift is NAMED at validate: a
    ``GateDecl.timeout_s`` and the body's statically-readable literal
    ``timeout_s=`` that DISAGREE are the W-rule's subject (the split's
    documented precedence: the wait site's value arms the runtime, the
    declaration feeds the compile surfaces — and where both are
    literals they must agree or the drift is named)."""
    app = WorkflowApp()

    async def gated(ctx: StepContext, params: _Ingest) -> _Decision:
        approval = await ctx.wait_signal((_Approval,), timeout_s=45.0)
        return _Decision(ok=approval.verdict == "go")

    @app.workflow("rv3-10-timeout-drift")
    def build_wf() -> Promise[_Decision]:
        p = step(
            gated,
            _Ingest(doc_id="d"),
            gates=(GateDecl(name="Approval", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    compiled = _compile_quietly(app, "rv3-10-timeout-drift")
    diagnostics = validate_compiled(compiled)
    drift = [d for d in diagnostics if d.rule == "W4-gate-timeout-split"]
    assert drift, (
        "the GateDecl's timeout_s=30.0 and the wait site's timeout_s=45.0 "
        "disagree and validate() says nothing — the double-sourced "
        "timeout's cross-checkable drift is never named"
    )
    assert "45" in drift[0].message and "30" in drift[0].message, (
        "the drift diagnostic must name BOTH values (the operator reads "
        f"which side is which): {drift[0].message}"
    )


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-10b: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_10b_the_gate_decl_docstring_names_the_union_face() -> None:
    """The declaration's own documentation states the UNION face: what
    ``GateDecl.timeout_s`` feeds (the compile surfaces) AND what arms
    the runtime (the wait site's value) — the stale raise-face text is
    rewritten, the precedence readable where the author declares."""
    doc = GateDecl.__doc__ or ""
    import re

    # The union face, BOTH directions: what the Decl's timeout_s feeds
    # (the compile surfaces) and what arms the runtime (the wait site's
    # own value) — either alone is the stale half-face.
    feeds_compile = re.search(r"compile surfaces|Mermaid|W1", doc) is not None
    arms_runtime = (
        re.search(r"wait site[^\n]*(arms|the runtime|the deadline)", doc, re.IGNORECASE) is not None
    )
    assert feeds_compile and arms_runtime, (
        "GateDecl's docstring does not state the timeout's UNION face "
        f"(feeds-the-compile-surfaces: {feeds_compile}, the wait site "
        f"arms-the-runtime: {arms_runtime}) — the stale raise-face text "
        "documents neither the split's precedence nor the cross-check"
    )


# ── F-RV3-12: the loop's shape law — named, never prose ────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-12: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_12_a_conditional_loop_wait_is_a_named_warning() -> None:
    """The loop's shape law ("ONE wait per iteration") as a NAMED W-rule:
    a loop body whose ``wait_signal`` sits in a CONDITIONAL interior
    mis-indexes the answer cursor (the T26 review's C9 — the iteration
    counter is the cursor, so an iteration that does not wait drifts the
    sequence). The mis-index shape is the warning's subject; the
    docstring's prose is not the enforcement."""
    app = WorkflowApp()

    async def looping(ctx: StepContext, carry: _Carry):
        if carry.n > 0:  # the conditional-interior wait — the mis-index shape
            approval = await ctx.wait_signal((_Approval,), timeout_s=30.0)
            if approval.verdict != "go":
                return Done(_Carry(n=carry.n))
        return Refine(_Carry(n=carry.n + 1))

    async def stop() -> bool:
        return False

    @app.workflow("rv3-12-conditional-loop-wait")
    def build_wf() -> Promise[_Carry]:
        p = loop(
            "march",
            looping,
            initial=_Carry(),
            max_iterations=3,
            until=stop,
            gates=(GateDecl(name="Approval", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    compiled = _compile_quietly(app, "rv3-12-conditional-loop-wait")
    diagnostics = validate_compiled(compiled)
    shape = [d for d in diagnostics if d.rule == "W5-loop-wait-shape"]
    assert shape, (
        "the loop body's conditional-interior wait validates clean — the "
        "shape law is docstring-enforced and the mis-index (the T26 "
        "review's C9) has no name at the validator"
    )


def test_rv3_12b_the_unconditional_loop_wait_stays_clean() -> None:
    """The zero-false-positive bound: the loop body's UNCONDITIONAL wait
    (the shape law kept) is never convicted."""
    app = WorkflowApp()

    async def looping(ctx: StepContext, carry: _Carry):
        approval = await ctx.wait_signal((_Approval,), timeout_s=30.0)
        if approval.verdict == "go":
            return Done(_Carry(n=carry.n))
        return Refine(_Carry(n=carry.n + 1))

    async def stop() -> bool:
        return False

    @app.workflow("rv3-12b-unconditional-loop-wait")
    def build_wf() -> Promise[_Carry]:
        p = loop(
            "march",
            looping,
            initial=_Carry(),
            max_iterations=3,
            until=stop,
            gates=(GateDecl(name="Approval", payload_models=(_Approval,), timeout_s=30.0),),
        )
        return build(p)

    compiled = _compile_quietly(app, "rv3-12b-unconditional-loop-wait")
    diagnostics = validate_compiled(compiled)
    assert not [d for d in diagnostics if d.rule == "W5-loop-wait-shape"], (
        "the unconditional loop wait was convicted — the shape law only "
        "names the conditional-interior mis-index"
    )


# ── F-RV3-1: map_source's can-only-error param is GONE ─────────────────


@pytest.mark.xfail(
    strict=True,
    reason="LIVE FINDING F-RV3-1: the teardown review's conviction at 512f7a91 — the red receipt in test_wf_teardown_rv3_RECEIPTS.md; the cure removes this marker WITH the rule",
)
def test_rv3_1_map_source_takes_no_key_param() -> None:
    """The removal: ``map_source``'s ``key=`` existed only to raise (the
    map's join key is DERIVED — the engine addresses it by the source's
    own key). A parameter whose every use is the author's own refusal is
    not API, it's a trap — the param is GONE from the signature, and
    the removal is DOCUMENTED (the changelog + the guide's line)."""
    sig = inspect.signature(map_source)
    assert "key" not in sig.parameters, (
        f"map_source still carries key= (params: {sorted(sig.parameters)}) — "
        "a param whose every value raises is the accept-and-ignore "
        "class's cousin; delete it and document the removal"
    )


def test_rv3_1b_the_map_source_removal_is_documented() -> None:
    """The preserve law: the removal is readable — the changelog (or the
    guide) names the ``key=`` param's deletion, not a silent signature
    edit."""
    import re
    from pathlib import Path

    repo = Path(__file__).resolve().parent.parent
    changelog = (repo / "CHANGELOG.md").read_text()
    guide_hits = []
    guide = repo / "docs" / "workflows.md"
    if guide.exists():
        guide_hits.append(guide.read_text())
    for other in (repo / "docs").glob("**/*.md"):
        text = other.read_text()
        if "map_source" in text and "key" in text:
            guide_hits.append(text)
    pattern = re.compile(r"map_source[^\n]*key", re.IGNORECASE)
    assert any(pattern.search(text) for text in [changelog, *guide_hits]), (
        "the map_source key= removal is documented nowhere readable — "
        "the changelog (or the guide's map section) must name the "
        "deleted param (the preserve law: a removal is a documented "
        "event, never a silent edit)"
    )

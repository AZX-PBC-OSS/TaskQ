"""T09 — THE VALIDATE PINS: the checker-independent validator's mutation
matrix. A valid graph pins CLEAN; ONE mutation at a time flips EXACTLY
the intended rule and nothing else (§15.6's discipline) — the
zero-false-positive budget is the pin, not an aspiration.

Every pin here RED-then-GREENs by construction: the mutation IS the red
(the diagnostic fires), the clean graph the green — both verdicts read
back from the captured run (BUILD-PROTOCOL §2/§3).
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    gather,
    sink,
    step,
)
from taskq.workflows.api._app import CompiledWorkflow
from taskq.workflows.api._validate import WorkflowValidationError
from tests._wf_fixtures import runtime_refusal_builder

# ── the clean corpus (the SHARED graph — one mutation per pin) ──────────


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _annotated_body(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _consumer_body(ctx: StepContext, report: Report) -> dict[str, int]:
    return {"n": 1}


def _clean_app() -> tuple[WorkflowApp, str]:
    """The clean graph: two sequenced nodes, the terminal spelled."""
    app = WorkflowApp()

    @app.workflow("clean")
    def clean() -> Promise[object]:
        produced = step(_annotated_body, Ingest(doc_id="d1"), key="produce")
        consumed = step(_consumer_body, produced, key="consume")
        return build(consumed)

    app.get("clean")  # compile + register (idempotent)
    return app, "clean"


def _compiled(name: str, app: WorkflowApp) -> CompiledWorkflow:
    # The PROBE SEAM (app._compile): the door (app.get) validates — the
    # mutation pins' subject IS the invalid graph's diagnostics, so the
    # probes compile without the door's raise.
    return app._compile(name)


def _rules(compiled: CompiledWorkflow) -> set[str]:
    from taskq.workflows.api._validate import _run_rules

    return {d.rule for d in _run_rules(compiled)}


# ── the mutation matrix: ONE mutation → EXACTLY one rule ────────────────


def test_clean_graph_validates_clean() -> None:
    """The zero-false-positive baseline: the clean graph produces ZERO
    diagnostics of either class."""
    app, name = _clean_app()
    assert _rules(_compiled(name, app)) == set()


def test_cycle_mutation_flips_only_acyclicity() -> None:
    """E1 (the unconditional owner): a self-referential wiring → the
    cycle error; pyright is absent at runtime — the validator owns it."""
    app = WorkflowApp()

    @app.workflow("cyclic")
    def cyclic() -> Promise[object]:
        a = step(_annotated_body, Ingest(doc_id="d"), key="a")
        b = step(_consumer_body, a, key="b")
        return build(b)

    compiled = _compiled("cyclic", app)
    # THE MUTATION: the back-edge — 'a' consumes 'b' (the wiring's cycle).
    node = compiled.nodes["a"]
    node.parents = (*node.parents, "b")
    rules = _rules(compiled)
    assert rules == {"E1-acyclicity"}, rules


def test_unconsumed_residual_mutation() -> None:
    """E2: a promise nobody consumes/sinks/terminals — the residual red."""
    app = WorkflowApp()

    @app.workflow("residual")
    def residual() -> Promise[object]:
        step(_annotated_body, Ingest(doc_id="d"), key="orphan")  # nobody consumes it
        fed = step(_annotated_body, Ingest(doc_id="d"), key="fed")
        return build(step(_consumer_body, fed, key="consumer"))

    rules = _rules(_compiled("residual", app))
    assert rules == {"E2-produced-never-consumed"}, rules


def test_sink_clears_the_residual() -> None:
    """The explicit fire-and-forget: a SUNK promise is recorded, never
    flagged — the metadata carries the drop."""
    app = WorkflowApp()

    @app.workflow("sunk")
    def sunk() -> Promise[object]:
        orphaned = step(_annotated_body, Ingest(doc_id="d"), key="orphan")
        sink(orphaned)
        fed = step(_annotated_body, Ingest(doc_id="d"), key="fed")
        return build(step(_consumer_body, fed, key="consumer"))

    assert _rules(_compiled("sunk", app)) == set()


def test_edgeless_join_mutation() -> None:
    """E3: a gather over zero upstreams is the stranded invisible join —
    refused at the VERB (the compile's door, before any row)."""
    with pytest.raises(WorkflowBuildError, match="stranded invisible join"):
        gather([])


def test_unannotated_step_mutation() -> None:
    """E4: the return annotation IS the wiring — an unannotated body
    reds."""

    async def unannotated(ctx: StepContext, params: Ingest):  # pyright: ignore[reportMissingTypeStubs, reportMissingParameterType, reportUnknownParameterType, reportReturnType]  # Why: THE PROBE — the unannotated body is the mutation under test; the root pyproject's tests relaxation would mute it.
        return params

    app = WorkflowApp()

    @app.workflow("unannotated")
    def unannotated_wf() -> Promise[object]:
        produced = step(unannotated, Ingest(doc_id="d"), key="produce")
        return build(produced)

    rules = _rules(_compiled("unannotated", app))
    assert rules == {"E4-unannotated-step"}, rules


class Unrelated(BaseModel):
    """The E5 probe's consumer model — deliberately unrelated to Report
    (module-level: the annotations must resolve — a function-local model
    is invisible to the compile's hint resolution by construction)."""

    other: str


async def _unrelated_consumer(ctx: StepContext, u: Unrelated) -> dict[str, int]:
    return {"n": 1}


def test_incompatible_consumer_mutation() -> None:
    """E5: an unrelated payload model consumed — the compile refuses what
    the checker would flag (the checker-independent half of the story)."""
    app = WorkflowApp()

    @app.workflow("incompatible")
    def incompatible() -> Promise[object]:
        produced = step(_annotated_body, Ingest(doc_id="d"), key="produce")
        consumed = step(_unrelated_consumer, produced, key="consume")
        return build(consumed)

    rules = _rules(_compiled("incompatible", app))
    assert rules == {"E5-incompatible-consumer"}, rules


def test_fan_in_bound_mutation() -> None:
    """E6: the T07 bound — past MAX_FAN_IN_PER_JOIN the compile refuses
    (the error names the bound)."""
    from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN

    app = WorkflowApp()

    @app.workflow("overbound")
    def overbound() -> Promise[object]:
        promises = [
            step(_annotated_body, Ingest(doc_id=f"d{i}"), key=f"p{i}")
            for i in range(MAX_FAN_IN_PER_JOIN + 1)
        ]
        return build(step(_consumer_body, gather(promises), key="join"))

    rules = _rules(_compiled("overbound", app))
    assert rules == {"E6-fan-in-bound"}, rules


def test_eternal_wait_is_a_warning_never_a_refusal() -> None:
    """W1: a gate with no timeout — the warning class (over-refusing
    valid graphs is the compile's over-rejection)."""
    from taskq.workflows.api._graph import GateDecl

    async def gated_body(ctx: StepContext, params: Ingest) -> object:
        # E14's law: the declared gate's body WAITS on it (a declared
        # seat with no waiter is the E14 build refusal, not W1's subject).
        return await ctx.wait_signal((Report,), timeout_s=None)

    app = WorkflowApp()

    @app.workflow("eternal")
    def eternal() -> Promise[object]:
        produced = step(
            gated_body,
            Ingest(doc_id="d"),
            key="gate",
            gates=(GateDecl(name="Approval", payload_models=(Report,), timeout_s=None),),
        )
        return build(produced)

    rules = _rules(_compiled("eternal", app))
    assert rules == {"W1-eternal-wait"}, rules
    compiled = _compiled("eternal", app)
    compiled.validate()  # the WARNING does not refuse — the report passes


# ── the error REPORT is one-pass (tsc-style): every rule's verdict ──────


def test_error_report_names_every_error() -> None:
    """The report carries EVERY error, not the first alone."""
    app = WorkflowApp()

    def multi_error() -> None:
        step(_annotated_body, Ingest(doc_id="d"), key="orphan1")
        step(_annotated_body, Ingest(doc_id="d"), key="orphan2")
        return None

    app.workflow("multi_error")(runtime_refusal_builder(multi_error))

    with pytest.raises(WorkflowValidationError) as excinfo:
        _compiled("multi_error", app).validate()
    message = str(excinfo.value)
    assert "orphan1" in message and "orphan2" in message


# ── the typing pins: the Never-residual / the redefinition door ─────────


def test_differing_redefinition_refused() -> None:
    """The registry's idempotence is EXACT: the same bodies re-register
    (the deterministic recompile); a DIFFERING body map is the shadow —
    refused."""
    app, name = _clean_app()
    _compiled(name, app)  # recompile — identical, idempotent

    async def shadow_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref="shadow")

    app2 = WorkflowApp()

    @app2.workflow("shadow")
    def shadow() -> Promise[object]:
        return build(step(shadow_body, Ingest(doc_id="d"), key="produce"))

    app2.get("shadow")
    # A differing re-registration through the RAW registry:
    from taskq.workflows.definitions import DuplicateWorkflowError, WorkflowDef, get_registry

    with pytest.raises(DuplicateWorkflowError):
        get_registry().register(WorkflowDef(name="shadow", bodies={"produce": _annotated_body}))
    _ = (Promise, step, gather, build, sink, WorkflowBuildError)

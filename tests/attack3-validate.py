"""ATTACK-3 (phase-3 red team) — the compile-time surfaces: wf.validate()'s
rules, the wiring verbs' doors, the Mermaid byte-stability.

No PG: every probe compiles or validates. RED = the observed half
contradicts the documented contract (the capture file names it).

Captured to .measurements/attack3/ per BUILD-PROTOCOL §7b.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, gather, step
from taskq.workflows.api._graph import NodeDecl
from taskq.workflows.api._validate import _run_rules


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    n: int


class Unrelated(BaseModel):
    other: str


async def _annotated(ctx: StepContext, params: Ingest) -> Report:
    return Report(n=1)


async def _consumer(ctx: StepContext, r: Report) -> Report:
    return r


# ── A3-V1: the E3 rule is DEAD CODE ─────────────────────────────────────
# _validate.py::_rule_edgeless_join places `diagnostics.append(...)` AFTER
# `continue` inside the `if` — unreachable. No public verb can wire an
# edge-less join (gather([]) is refused at the verb), so the dead rule is
# unobservable through the verbs — but the RULE is the promised refusal,
# and the compiled graph is public data a plugin/extension writes.
# Proof: inject the shape the rule exists to convict (a join-shaped node
# with zero parents) and validate says NOTHING.


def test_a3_e3_rule_is_dead_code() -> None:
    app = WorkflowApp()

    @app.workflow("a3_e3_dead")
    def a3_e3() -> Promise[object]:
        produced = step(_annotated, Ingest(doc_id="d"), key="produce")
        return build(produced)

    compiled = app.get("a3_e3_dead")
    # THE CONVICTED SHAPE, injected post-compile: the verbs refuse it at
    # the door, but the validator's own E3 rule (the report's "E1-E6")
    # must own the shape — it owns NOTHING (the append is unreachable).
    compiled.nodes["stranded"] = NodeDecl(
        key="stranded", actor="wf", queue="default", body=None, parents=(), kind="gather"
    )
    rules = [d.rule for d in _run_rules(compiled)]
    assert "E3-edgeless-join" in rules, (
        f"E3 did NOT convict the edge-less join — the rule is dead code "
        f"(the diagnostics.append sits after the continue): {rules}"
    )


# ── A3-V2: the cross-graph promise smuggle ──────────────────────────────
# step() / gather() never check that a promise's graph IS the active
# recorder (map_source DOES). A promise from app B wired into app A
# resolves its KEY against app A's nodes — with a colliding key that is
# a silently WRONG edge to A's own node; clean at build AND at validate.


def test_a3_cross_graph_promise_smuggle_builds_a_silently_wrong_edge() -> None:
    app_a = WorkflowApp()
    app_b = WorkflowApp()

    @app_b.workflow("a3_b")
    def a3_b() -> Promise[object]:
        foreign = step(_annotated, Ingest(doc_id="d"), key="fetch")  # the SAME key
        return build(step(_consumer, foreign, key="consume"))

    compiled_b = app_b.get("a3_b")
    smuggled_key = compiled_b.nodes["consume"].args[0][1]  # 'fetch'

    @app_a.workflow("a3_a")
    def a3_a() -> Promise[object]:
        mine = step(_annotated, Ingest(doc_id="d"), key="fetch")
        forged = Promise(smuggled_key, Report, _a3_discard_graph())  # the foreign handle
        consume_mine = step(_consumer, mine, key="consume")
        consume_smuggled = step(_consumer, forged, key="consume2")
        return build(consume_mine, consume_smuggled)

    # the PROBE SEAM (app._compile): the smuggle's graph is INVALID on
    # purpose (the silently-wrong edge is the pin's subject) — the door
    # would raise before the diagnostics could be read.
    compiled = app_a._compile("a3_a")
    rules = [d.rule for d in _run_rules(compiled)]
    # The WRONG EDGE: consume2's parent is app A's own 'fetch', not app
    # B's node. validate must refuse (there is no rule that even looks):
    assert rules, (
        "the cross-graph smuggle (a foreign promise wired under a "
        "colliding key) built a WRONG edge to app A's own 'fetch' and "
        "validate said nothing — the silently-wrong-edge hole ships"
    )


def _a3_discard_graph() -> Any:
    """A throwaway recorder for the forged handle (the graph the promise
    CARRIES is never consulted by step/gather — that is the hole)."""
    from taskq.workflows.api._graph import BuildGraph

    return BuildGraph()


# ── A3-V3: the Mermaid byte-stability (two compiles + wiring order) ─────


def test_a3_mermaid_byte_stable_across_compiles_and_wiring_order() -> None:
    app = WorkflowApp()

    @app.workflow("a3_mermaid")
    def a3_mermaid() -> Promise[object]:
        produced = step(_annotated, Ingest(doc_id="d"), key="produce")
        produced2 = step(_annotated, Ingest(doc_id="d"), key="produce2")
        joined = gather([produced, produced2])
        return build(step(_consumer, joined, key="consume"))

    first = app.get("a3_mermaid").mermaid()

    app2 = WorkflowApp()

    @app2.workflow("a3_mermaid")
    def a3_mermaid2() -> Promise[object]:
        produced2 = step(_annotated, Ingest(doc_id="d"), key="produce2")
        produced = step(_annotated, Ingest(doc_id="d"), key="produce")
        joined2 = gather([produced, produced2])
        return build(step(_consumer, joined2, key="consume"))

    second = app2.get("a3_mermaid").mermaid()
    assert first == second, (
        "the same wiring spelled in a DIFFERENT order rendered different "
        "bytes — the emission depends on insertion order, not the graph"
    )
    assert first.startswith("flowchart TD\n")


# ── A3-V4: the unknown-queue seam (the report's own unspecification #7) ─


def test_a3_actor_on_a_nonexistent_queue_is_not_refused_at_build() -> None:
    app = WorkflowApp()

    @app.workflow("a3_queue")
    def a3_queue() -> Promise[object]:
        produced = step(_annotated, Ingest(doc_id="d"), key="produce", queue="no-such-queue")
        return build(produced)

    compiled = app.get("a3_queue")
    rules = [d.rule for d in _run_rules(compiled)]
    try:
        compiled.validate()
    except Exception as exc:  # the probe refuses nothing — any raise is the pin's failure
        raise AssertionError(f"unexpected refusal: {exc}") from exc
    assert any("queue" in r.lower() for r in rules), (
        "an actor projected onto a nonexistent queue validates CLEAN at "
        "build time — the TASKQ_QUEUES_STRICT fail-fast is the only door "
        "left (the worker-boot seam, the report's own #7)"
    )


# ── A3-V5: the E5 rule's blind spots (the unannotated param + the hook) ─


async def _untyped_consumer(ctx: StepContext, r: Any) -> Unrelated:
    return Unrelated(other="x")


def test_a3_e5_blind_to_untyped_and_duck_shapes() -> None:
    """A consumer whose param carries NO annotation (or a plain dict)
    consumes ANY producer — E5 is silent (the checker half would need
    the corpus; the checker-independent half promises totality)."""
    app = WorkflowApp()

    @app.workflow("a3_e5_blind")
    def a3_e5() -> Promise[object]:
        produced = step(_annotated, Ingest(doc_id="d"), key="produce")
        return build(step(_untyped_consumer, produced, key="consume"))

    # the PROBE SEAM (app._compile): the untyped consumer's graph is
    # INVALID on purpose (E5's conviction is the pin's subject) — the
    # door would raise before the diagnostics could be read.
    compiled = app._compile("a3_e5_blind")
    rules = [d.rule for d in _run_rules(compiled)]
    assert "E5-incompatible-consumer" in rules, (
        f"an UNANNOTATED consumer param consumes Report unseen — E5's "
        f"totality claim does not hold for the duck-typed face: {rules}"
    )

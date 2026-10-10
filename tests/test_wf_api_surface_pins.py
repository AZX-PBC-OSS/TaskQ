"""T09 — THE API-SURFACE PINS: the decorator's composition contract
(F3 — no second, invisible actor population), the channel/gate
declaration doors, and the Mermaid emission's shape vocabulary (the
golden's grammar).

Coverage note: these pins exist because the estate floor (90 branch)
APPLIES to the API modules — every branch below is a behavior, and a
behavior without a pin is a defect on layaway.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, sink, step
from taskq.workflows.api._graph import GateDecl
from tests._wf_fixtures import runtime_refusal_builder


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Approval(BaseModel):
    verdict: str


class Escalate(BaseModel):
    reason: str


async def _body(ctx: StepContext, params: Ingest) -> Report:
    """The report body (the direct-call form's probe)."""
    return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies


# ── the F3 composition: the workflow actor is VISIBLE to the estate ────


def test_actor_projects_the_estate_carrier() -> None:
    """Pin 9's composition assertion (the registry-omits variant reds):
    the workflow actor projects the ``ActorConfig``-compatible carrier —
    the same shape the config sync + the guards + ``TASKQ_QUEUES_STRICT``
    consume. The projection carries the placement + the retry contract;
    the actor NAME is workflow-namespaced (one registry, no shadows)."""
    app = WorkflowApp()
    decorate = app.actor(queue="gpu", max_attempts=5, retry_kind="permanent")
    handle = decorate(_body)
    config = handle.actor_config("doc_ingest")
    assert config == {
        "actor": "doc_ingest._body",
        "queue": "gpu",
        "max_attempts": 5,
        "retry_kind": "permanent",
    }


def test_actor_preserves_the_function_identity() -> None:
    """The decorator is SUGAR: the handle preserves ``__name__``/
    ``__doc__`` (attribute reads fall through) and is DIRECTLY callable
    for unit tests (the body's own signature)."""
    app = WorkflowApp()
    decorate = app.actor()
    handle = decorate(_body)
    assert handle.name == "_body"
    assert handle.__name__ == "_body"  # type: ignore[attr-defined]  # Why: the fall-through IS the pin (the callable-preserving decorator).
    assert "workflow actor handle" in handle.__doc__  # type: ignore[attr-defined]
    # The DIRECT call (the unit-test form) runs the body.
    report = asyncio_run(handle(None, Ingest(doc_id="d1")))
    assert isinstance(report, Report)


def asyncio_run(coro: Any) -> Any:
    import asyncio

    return asyncio.new_event_loop().run_until_complete(coro)


# ── the declaration doors ───────────────────────────────────────────────


def test_duplicate_declaration_refused() -> None:
    """Two declarations of one workflow name on one app — a coding
    error, refused (the same no-shadow law the registry enforces)."""
    app = WorkflowApp()

    @app.workflow("dup")
    def first() -> Promise[object]:
        return build(step(_body, Ingest(doc_id="d"), key="a"))

    with pytest.raises(Exception, match="already declared"):

        @app.workflow("dup")
        def second() -> Promise[object]:
            return build(step(_body, Ingest(doc_id="d"), key="b"))


def test_unknown_workflow_get_refused_loudly() -> None:
    """``get`` of an undeclared name — the LOUD KeyError (never a silent
    empty compile)."""
    app = WorkflowApp()
    with pytest.raises(KeyError, match="not declared"):
        app.get("never-declared")


def test_non_promise_build_return_refused() -> None:
    """The build function's return IS the terminal promise — a bare dict
    (the closure-shaped mistake) is the loud build error."""
    app = WorkflowApp()

    def bad_return() -> object:
        step(_body, Ingest(doc_id="d"), key="a")
        return {"not": "a promise"}

    app.workflow("bad_return")(runtime_refusal_builder(bad_return))

    with pytest.raises(TypeError, match="terminal promise"):
        app.get("bad_return")


def test_channel_gate_binding_doors() -> None:
    """The channel's gates: a gate binds at least ONE payload model
    (the empty binding refused); the registry is keyed by the bound
    gate's own name; a second gate with the same model REPLACES (the
    channel is the workflow definition's lifetime)."""
    channel = WorkflowApp().channel()
    with pytest.raises(TypeError, match="at least one payload model"):
        channel.gate()
    approval = channel.gate(Approval)
    assert channel._gates["Approval"] is approval  # pyright: ignore[reportPrivateUsage]  # Why: the registry IS the door's backing map — the pin reads it.
    escalate = channel.gate(Escalate)
    assert channel._gates["Escalate"] is escalate  # pyright: ignore[reportPrivateUsage]
    _ = approval, escalate


def test_gate_declaration_rides_the_compile() -> None:
    """The gate is COMPILE-VISIBLE: the node's declared gates ride the
    compiled graph (the mermaid render's hold nodes + the W1 warning
    read them from here)."""
    from taskq.workflows import gather

    async def gated(ctx: StepContext, params: Ingest) -> object:
        # E14's law: the declared gate's body WAITS on it (a declared
        # seat with no waiter is the build refusal).
        return await ctx.wait_signal(
            (Approval, Escalate), timeout_s=30.0
        )  # pragma: no cover - the runner drives real bodies

    app = WorkflowApp()

    @app.workflow("gated")
    def gated_wf() -> Promise[object]:
        gate = GateDecl(
            name="Approval",
            payload_models=(Approval, Escalate),
            timeout_s=30.0,
            on_timeout="fail",
        )
        return build(step(gated, Ingest(doc_id="d"), key="g", gates=(gate,)))

    compiled = app.get("gated")
    node = compiled.nodes["g"]
    assert node.gates[0].payload_models == (Approval, Escalate)
    assert node.gates[0].timeout_s == 30.0
    _ = gather


# ── the Mermaid shape vocabulary ────────────────────────────────────────


def test_mermaid_shape_vocabulary() -> None:
    """The emission's grammar: rectangle = plain actor, ``[[...]]`` =
    the gather join, ``{{...}}`` = the collapsed map's join, ``([(...)])``
    = the HOLD node (its gate + timeout policy ON the node), and the
    label fallback for an unresolvable type."""
    from taskq.workflows import gather, map_source

    async def map_item(ctx: StepContext, report: Report) -> dict[str, int]:
        return {"n": 1}  # pragma: no cover - the runner drives real bodies

    async def hold_body(ctx: StepContext, params: Ingest) -> object:
        # E14's law: the declared gate's body WAITS on it.
        return await ctx.wait_signal((Approval,), timeout_s=None)  # pragma: no cover

    app = WorkflowApp()

    @app.workflow("shapes")
    def shapes() -> Promise[object]:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=None)
        plain = step(_body, Ingest(doc_id="d"), key="plain")
        sink(plain)
        joined = gather([plain])
        sink(joined)
        mapped = map_source(plain, map_item)
        sink(mapped)
        return build(
            step(
                hold_body,
                Ingest(doc_id="d"),
                key="hold",
                gates=(gate,),
            )
        )

    text = app.get("shapes").mermaid()
    assert (
        'plain(["plain' in text
    )  # the map source's stadium (it forks children — the rv4 arms-render cure)
    assert "⇢ map:" in text  # the map's item body ON the label
    assert 'gather[["gather"]]' in text  # the gather join
    assert 'plain.join{{"plain.join"}}' in text  # the collapsed map join
    assert "hold([(" in text  # the HOLD node
    assert "⏳" in text and "Approval" in text  # the gate's policy rides it


def test_mermaid_label_and_shape_edges() -> None:
    """The emission's edge branches (the pure functions' unit lane): the
    unresolvable label's fallback, the class-path shortening, and the
    non-gated map shapes."""
    from taskq.workflows.api._mermaid import _label_of, _shape

    assert _label_of(None) == "?"  # the fallback (a bodyless parent)
    assert _label_of(Report) == "Report"  # the class path shortens
    assert _label_of("list[Report]") == "list[Report]"  # the string spelling passes through
    assert _shape("step", False) == ("[", "]")
    assert _shape("step", True) == ("([(", ")])")  # the HOLD shape
    assert _shape("map_source", False) == ("([", "])")
    assert _shape("map_join", False) == ("{{", "}}")
    assert _shape("gather", False) == ("[[", "]]")

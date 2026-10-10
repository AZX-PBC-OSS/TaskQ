# Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""THE API LANE'S FIX-ROUND PINS (the fresh reviewer's F3 findings):

* F3-1 → **E9-ctx-annotation**: a LYING ctx annotation (a fabricated
  stand-in) is the BUILD refusal (the PICKED door: the annotation is
  verification, not documentation — the docs' claim "the checker
  verifies the body's ctx.* reads" is now unconditional). RED-FIRST:
  the convicted shape validate()s clean on the pre-rule tree (the pin's
  red is the base's behavior, the finding's receipt).
* F3-2 → **E10-arity**: a body taking MORE params than wired is the
  BUILD refusal (the mismatch rode the retry ladder mid-flow; the
  ladder never sees it now). RED-FIRST: the same.
* F3-4 → **E5's signature-ordered map**: the LEGITIMATE mixed signature
  (``step(body, p, 3)`` for ``body(ctx, item: Report, page: int)``)
  GREENS (the cross-product's false positive is dead) AND the genuine
  mismatch still refuses (both faces pinned).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api._validate import WorkflowValidationError


class Report(BaseModel):
    ref: str


class Ingest(BaseModel):
    doc_id: str


class Other(BaseModel):
    n: int


class _FabricatedCtx:
    """THE LIE (F3-1's convicted stand-in): an anything-goes context —
    the checker verifies the body's ctx.* reads against THIS, every face
    reports clean, nothing verified."""


# ── F3-1: E9 — the ctx annotation's conformance ─────────────────────────


def test_e9_the_fabricated_ctx_annotation_is_the_build_refusal() -> None:
    """THE LIE IS THE BUILD REFUSAL: a body whose ctx annotation is a
    fabricated stand-in never compiles (the docs' verification claim is
    unconditional now)."""
    app = WorkflowApp()

    async def lying_body(ctx: _FabricatedCtx, params: Ingest) -> Report:  # type: ignore[reportInvalidTypeForm]  # Why: THE LIE ITSELF — the probe's subject.
        return Report(ref=params.doc_id)  # type: ignore[return-value]  # Why: the lie's body reads nothing.

    @app.workflow("e9_lying_ctx")
    def e9_lying_ctx() -> Promise[object]:
        return build(step(lying_body, Ingest(doc_id="d1"), key="solo"))

    # the PROBE SEAM (app._compile): the door (app.get) raises on the
    # graph's error — the pin's subject is the compiled graph's OWN
    # validate() report, so the probe compiles without the door.
    compiled = app._compile("e9_lying_ctx")
    from taskq.workflows.api._validate import _run_rules

    e9 = [d for d in _run_rules(compiled) if d.rule == "E9-ctx-annotation"]
    assert e9, "the fabricated ctx annotation was not convicted (the erasure ships)"
    assert "StepContext" in e9[0].message
    # And the BUILD door refuses (the errors raise):
    with pytest.raises(WorkflowValidationError, match="E9-ctx-annotation"):
        compiled.validate()


def test_e9_the_honest_annotation_greens() -> None:
    """The HONEST annotation: ``ctx: StepContext`` (the documented
    annotation) greens — the rule refuses lies, never the truth."""
    app = WorkflowApp()

    async def honest_body(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)

    @app.workflow("e9_honest_ctx")
    def e9_honest_ctx() -> Promise[object]:
        return build(step(honest_body, Ingest(doc_id="d1"), key="solo"))

    compiled = app.get("e9_honest_ctx")
    compiled.validate()  # zero findings


# ── F3-2: E10 — the arity gap ────────────────────────────────────────────


def test_e10_the_over_arity_body_is_the_build_refusal() -> None:
    """THE ARITY GAP'S CURE: a body taking MORE params than wired is the
    BUILD refusal — the mismatch never reaches the ladder."""
    app = WorkflowApp()

    async def over_arity(ctx: Any, params: Ingest, extra: str, more: int) -> Report:
        return Report(ref=params.doc_id)

    @app.workflow("e10_over_arity")
    def e10_over_arity() -> Promise[object]:
        return build(step(over_arity, Ingest(doc_id="d1"), key="solo"))

    # the PROBE SEAM (app._compile): the door would raise — the pin's
    # subject is the graph's own validate() report.
    compiled = app._compile("e10_over_arity")
    with pytest.raises(WorkflowValidationError, match="E10-arity"):
        compiled.validate()


def test_e10_the_exact_arity_greens() -> None:
    """The exact arity greens (the rule refuses the mismatch, never the
    wiring)."""
    app = WorkflowApp()

    async def exact(ctx: Any, params: Ingest) -> Report:
        return Report(ref=params.doc_id)

    @app.workflow("e10_exact")
    def e10_exact() -> Promise[object]:
        return build(step(exact, Ingest(doc_id="d1"), key="solo"))

    app.get("e10_exact").validate()


# ── F3-4: E5's signature-ordered map ────────────────────────────────────


def test_e5_the_legitimate_mixed_signature_greens() -> None:
    """THE ZERO-FALSE-POSITIVE PIN: ``step(body, p, 3)`` for
    ``body(ctx, item: Report, page: int)`` — the mixed promise/data
    signature GREENS (the cross-product convicted it before; the map is
    by signature order now)."""
    app = WorkflowApp()

    async def source_body(ctx: Any, params: Ingest) -> Report:
        return Report(ref=params.doc_id)

    async def mixed_body(ctx: Any, item: Report, page: int) -> str:
        return f"{item.ref}:{page}"

    @app.workflow("e5_mixed")
    def e5_mixed() -> Promise[object]:
        p = step(source_body, Ingest(doc_id="d1"), key="src")
        return build(step(mixed_body, p, 3, key="tail"))

    app.get("e5_mixed").validate()  # the mixed signature: GREEN


def test_e5_the_genuine_mismatch_still_refuses() -> None:
    """The genuine mismatch still refuses (the signature-ordered
    comparison sees it): the consumer's param model is unrelated to its
    OWN wired parent's product."""
    app = WorkflowApp()

    async def source_body(ctx: Any, params: Ingest) -> Report:
        return Report(ref=params.doc_id)

    async def mismatched(ctx: Any, item: Other) -> str:
        return item.n.__str__()

    @app.workflow("e5_mismatch")
    def e5_mismatch() -> Promise[object]:
        p = step(source_body, Ingest(doc_id="d1"), key="src")
        return build(step(mismatched, p, key="tail"))

    with pytest.raises(WorkflowValidationError, match="E5-incompatible-consumer"):
        app.get("e5_mismatch").validate()


# ── finding 13: E10's message states the SIGNATURE's counts ─────────────


def test_e10_the_message_states_the_signature_actually_declared() -> None:
    """FINDING 13's PIN: the arity message's numbers are the numbers the
    SIGNATURE it just read declares — never the resolved-hints count.
    The pre-cure message counted ANNOTATED params only: a
    partially-annotated body (``def body(ctx, params: Ingest, page)``)
    reported ``takes 1 param(s)`` while the signature declares TWO, and
    the unannotated extra param slipped the rule entirely (the runtime
    TypeError this rule exists to refuse). The pin asserts the message's
    numbers against the body's real signature."""
    import inspect

    app = WorkflowApp()

    async def partial(ctx: Any, params: Ingest, page: object) -> Ingest:
        # `page` is UNANNOTATED-in-kind on purpose: the pin's subject is
        # the SIGNATURE COUNT (2 params beyond ctx), and the pre-cure
        # rule counted only the ANNOTATED ones. `object` keeps pyright
        # quiet without adding the annotation the old message counted.
        return params

    @app.workflow("e10_partial_arity_message")
    def e10_partial_arity_message() -> object:
        return build(step(partial, key="solo"))

    compiled = app._compile("e10_partial_arity_message")
    declared = [
        name
        for name, p in inspect.signature(partial).parameters.items()
        if name not in ("ctx", "return")
        and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    assert len(declared) == 2, "the fixture's body declares 2 params beyond ctx"
    with pytest.raises(WorkflowValidationError, match="E10-arity") as exc_info:
        compiled.validate()
    message = str(exc_info.value)
    # THE MESSAGE'S NUMBERS: the ACTUAL declared count (2 — not the
    # hints-derived 1), the param NAMES the signature declares (both),
    # and the ACTUAL wired count (0).
    assert "takes 2 param(s)" in message, f"the message under-counts: {message}"
    assert "params, page" in message, f"the message omits the signature's own names: {message}"
    assert "wired 0 argument(s)" in message, f"the message miscounts the wiring: {message}"


def test_e10_the_message_counts_the_wiring_actually_provided() -> None:
    """The provided-count face: a body of 3 declared params wired 2
    sources reports ``wired 2 argument(s)`` — the message's second
    number is the wiring's OWN count, and the pin asserts it."""
    app = WorkflowApp()

    async def three(ctx: Any, a: Ingest, b: Ingest, c: Ingest) -> Ingest:
        return a

    @app.workflow("e10_provided_count")
    def e10_provided_count() -> object:
        return build(step(three, Ingest(doc_id="1"), Ingest(doc_id="2"), key="solo"))

    with pytest.raises(WorkflowValidationError, match="E10-arity") as exc_info:
        app._compile("e10_provided_count").validate()
    message = str(exc_info.value)
    assert "takes 3 param(s)" in message, f"the message under-counts the signature: {message}"
    assert "wired 2 argument(s)" in message, f"the message miscounts the wiring: {message}"


# ── finding 12: E11 walks the carry's STRUCTURE ──────────────────────────


def test_e11_the_nested_promise_carry_is_refused() -> None:
    """FINDING 12's PIN (RED-FIRST): a promise handle NESTED inside the
    initial carry — a dict's value — died the same mid-flow death the
    bare handle does (the rehydration's walk passes the handle through
    unchanged into the jsonb write; the claim→crash→reclaim loop died
    UnencodableValue AFTER the rows existed, untyped by any compile
    rule). THE CURE: the validator walks the carry's STRUCTURE — the
    same walk the rehydration does — and refuses the nested handle at
    the construction door, E11 at any depth."""
    from taskq.workflows import loop

    class Carry(BaseModel):
        n: int

    async def loop_body(ctx: Any, carry: Carry) -> Carry:
        return carry

    app = WorkflowApp()

    @app.workflow("e11_nested_carry")
    def e11_nested_carry() -> object:
        parent = step(lambda ctx: None, key="parent")
        return build(loop("l1", loop_body, initial={"parent": parent}))

    with pytest.raises(WorkflowValidationError, match="E11-loop-promise-carry") as exc_info:
        app._compile("e11_nested_carry").validate()
    assert "NESTED" in str(exc_info.value), (
        "the message must name the nested face (the bare-handle message "
        "says nothing about a structure walk)"
    )


def test_e11_the_nested_promise_deep_in_generics_is_refused() -> None:
    """The structure walk's depth face: a handle inside a list inside a
    dict is refused — the walk reaches every shape the rehydration's
    codec can reach."""
    from taskq.workflows import loop

    class Carry(BaseModel):
        n: int

    async def loop_body(ctx: Any, carry: Carry) -> Carry:
        return carry

    app = WorkflowApp()

    @app.workflow("e11_deep_carry")
    def e11_deep_carry() -> object:
        parent = step(lambda ctx: None, key="parent")
        return build(loop("l1", loop_body, initial=[Carry(n=0), {"deep": parent}]))

    with pytest.raises(WorkflowValidationError, match="E11-loop-promise-carry"):
        app._compile("e11_deep_carry").validate()


def test_e11_the_honest_value_carry_still_greens() -> None:
    """The walk refuses HANDLES, never values: an honest dict/list/model
    carry validates clean (the zero-false-positive doctrine's own
    balance)."""
    from taskq.workflows import loop

    class Carry(BaseModel):
        n: int

    async def loop_body(ctx: Any, carry: Carry) -> Carry:
        return carry

    app = WorkflowApp()

    @app.workflow("e11_honest_carry")
    def e11_honest_carry() -> object:
        return build(loop("l1", loop_body, initial=[Carry(n=0), {"deep": "value"}]))

    app._compile("e11_honest_carry").validate()

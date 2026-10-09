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

from taskq.workflows import StepContext, WorkflowApp, build, step
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
    def e9_lying_ctx() -> object:
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
    def e9_honest_ctx() -> object:
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
    def e10_over_arity() -> object:
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
    def e10_exact() -> object:
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
    def e5_mixed() -> object:
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
    def e5_mismatch() -> object:
        p = step(source_body, Ingest(doc_id="d1"), key="src")
        return build(step(mismatched, p, key="tail"))

    with pytest.raises(WorkflowValidationError, match="E5-incompatible-consumer"):
        app.get("e5_mismatch").validate()

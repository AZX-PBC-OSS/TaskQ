"""POSITIVE TYPE PROBE — the typed context (the §7b hygiene round's
complement to the MUST_ERROR corpus): a workflow body declaring
``ctx: StepContext`` and reading the DOCUMENTED surface must pyright
CLEAN — zero errors, both faces (this file is run by
``tests/test_wf_ctx_annotation_pins.py::test_the_typed_ctx_type_checks``
under THIS directory's strict pyrightconfig; it is deliberately NOT in
``_gate.py``'s corpus — the gate asserts REDS, this probe asserts the
GREEN door the docs promise).

The reads the docs teach (every one must resolve against the real
frozen dataclass, no ``Any`` laundering):

* ``ctx.input`` — the run's carried input (cut #7);
* ``await ctx.substep(name, fn, *args)`` — the ledger's replay contract;
* ``await ctx.progress(pct, message, data)`` — the emission op (T21);
* ``await ctx.wait_signal((Model,), timeout_s=...)`` — the typed wait (T10);
* ``await ctx.cursor()`` + ``await ctx.emit_batch(children, cursor=...)``
  — the streaming source's pair (T20).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import StepContext


class Approval(BaseModel):
    verdict: str


class Params(BaseModel):
    doc_id: str


class Outcome(BaseModel):
    ref: str


async def documented_body(ctx: StepContext, params: Params) -> Outcome:
    """THE DOCS' BODY SHAPE — the annotation IS the type story."""
    carried: object = ctx.input

    async def side_effect(_carried: object) -> int:
        return 1

    n: int = await ctx.substep("the-step", side_effect, carried)
    await ctx.progress(50, "half", {"page": 1})
    approval = await ctx.wait_signal((Approval,), timeout_s=30.0, tool="review")
    _ = n, approval
    return Outcome(ref=params.doc_id)


async def streaming_source(ctx: StepContext) -> None:
    """The T20 pair (the paged generator body's reads)."""
    from taskq.workflows._types import EmitChild

    cursor: dict[str, object] = await ctx.cursor()
    children: list[EmitChild] = []
    _ = await ctx.emit_batch(children, cursor=cursor)


_ = Any  # the probe's surface is the ANNOTATED ctx — Any appears only here, unused

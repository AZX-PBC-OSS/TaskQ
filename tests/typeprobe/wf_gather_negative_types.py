"""THE API LANE'S FIX-ROUND PROBES (the fresh reviewer's findings F1-1 +
F1-2) — the honest shapes:

* F1-1: the HETEROGENEOUS gather's truth — the elements unify to the
  UNION (`Promise[list[Report | Config]]` — sound, the element type
  preserved as the union); the doc's "degrades to Promise[list[object]]"
  claim was FALSE in both directions (the union is what infers, and a
  `list[object]` consumer never reds through the wiring's codec walk —
  the coerce boundary re-validates). The GREEN probe pins the union
  shape; the DOC carries the corrected claim.
* F1-2: the residual slot's door — a REAL promise in the residual slot
  is the checker's MUST_ERROR (the residual wants produces-nothing;
  ``Exit[DoneT] <= Never`` is false). The exited promise's accounting is
  ``sink(p)`` — the docstring's truth (the runner_exit module's).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import Promise, WorkflowApp, build, gather, sink, step


class Report(BaseModel):
    ref: str


class Config(BaseModel):
    name: str


class Ingest(BaseModel):
    doc_id: str


async def report_body(ctx: Any, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def config_body(ctx: Any) -> Config:
    return Config(name="c")


# ── F1-1: the heterogeneous gather — THE UNION IS THE SHAPE (GREEN) ─────


async def probe_heterogeneous_gather_infers_the_union() -> None:
    """The heterogeneous list infers the UNION element type (sound — the
    element type preserved as the union; the doc's 'degrades to
    Promise[list[object]]' claim is cut)."""
    app = WorkflowApp()

    @app.workflow("probe_hetero_union")
    def probe_hetero_union() -> Promise[object]:
        p_report = step(report_body, Ingest(doc_id="d"), key="r")
        p_config = step(config_body, key="c")
        joined = gather([p_report, p_config])  # Promise[list[Report | Config]] — the union infers
        return build(joined)


# ── F1-1: the EXPLICIT UPCAST — the documented form (GREEN) ─────────────


async def probe_heterogeneous_gather_explicit_upcast() -> None:
    """The documented upcast form (for a consumer that wants the
    object-typed list): the upcast is the CALLER'S, at the list."""
    app = WorkflowApp()

    @app.workflow("probe_hetero_explicit")
    def probe_hetero_explicit() -> Promise[object]:
        p_report = step(report_body, Ingest(doc_id="d"), key="r")
        p_config = step(config_body, key="c")
        mixed: list[Promise[object]] = [p_report, p_config]
        joined = gather(mixed)
        return build(joined)


# ── F1-2: the residual slot NEVER takes a real promise (RED) ────────────


async def probe_residual_takes_real_promise() -> None:
    """The residual slot's door: a REAL promise in the residual slot is
    the checker's error (the residual wants produces-nothing —
    ``Exit[DoneT] <= Never`` is false). The honest accounting is
    ``sink(p)``. The trailing marker BELOW asserts the red — this prose
    names the convention without carrying the marker's literal (a
    docstring line is never a marker; the gate asserts trailing-code
    comments only)."""
    app = WorkflowApp()

    @app.workflow("probe_residual")
    def probe_residual() -> Promise[object]:
        p_report = step(report_body, Ingest(doc_id="d"), key="r")
        other = step(config_body, key="c")
        sink(other)
        return build(
            p_report, other
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the residual slot wants a produces-nothing handle — a real promise never fits


async def main() -> None:
    await probe_heterogeneous_gather_infers_the_union()
    await probe_heterogeneous_gather_explicit_upcast()
    await probe_residual_takes_real_promise()

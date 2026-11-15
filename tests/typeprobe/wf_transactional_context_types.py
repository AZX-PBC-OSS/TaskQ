"""POSITIVE TYPE PROBES — the transactional step's public context subtype
(pyright 1.1.414 + ty 0.0.85, via ``tests/typeprobe/_gate.py``).

The contract ``transactional_step`` documents, made checker-true:

* a body declaring ``ctx: TransactionalStepContext`` dereferences
  ``ctx.connection`` DIRECTLY — no assert, no cast, no narrowing: the
  subtype's ``connection`` is the non-optional ``ConnLike`` (never
  ``None``);
* a transactional node's body may also declare the ordinary
  ``StepContext`` base (compatibility) — it just cannot dereference the
  connection unguarded there;
* the wiring verb's generic ``R`` flows through the ``Promise[R]`` and
  the result's Pydantic encoding stays typed.

This file carries NO ``MUST_ERROR`` markers: the gate asserts only that
both checkers emit no error anywhere in it (the positive face must stay
clean)."""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import (
    Chain,
    Promise,
    Step,
    StepContext,
    TransactionalStepContext,
    WorkflowApp,
    build,
    chain_source,
    transactional_step,
)


class SettlementReport(BaseModel):
    applied: int
    token: str


class Ingest(BaseModel):
    doc_id: str


def _chain() -> Chain:
    return Chain(
        name="transactional_positive_probe_records",
        start="screen",
        steps={"screen": Step(_leaf, Ingest)},
    )


async def _leaf(ctx: StepContext, item: Ingest) -> Ingest:
    return item


async def settle(ctx: TransactionalStepContext, source_result: object) -> SettlementReport:
    """The documented settlement body: the held connection dereferenced
    WITHOUT narrowing — the subtype's non-optional face is the contract."""
    record = await ctx.connection.fetchval("SELECT 1")
    await ctx.connection.execute("SELECT $1::int", 1)
    _ = record
    return SettlementReport(applied=1, token="ok")


async def settle_via_base(ctx: StepContext, source_result: object) -> SettlementReport:
    """The compatible ordinary-base annotation on a transactional node —
    the body does NOT dereference the optional connection here (the
    ordinary optional face), so the file stays clean."""
    return SettlementReport(applied=1, token="base-ok")


def probe_transactional_context_connection_is_non_optional() -> None:
    app = WorkflowApp()

    @app.workflow("transactional_positive_probe")
    def flow() -> Promise[SettlementReport]:
        source = chain_source(_chain(), _stream, key="source")
        return transactional_step(settle, source, key="settle")

    _ = app


async def _stream(ctx: StepContext) -> list[Ingest]:
    return [Ingest(doc_id="d1")]


def probe_transactional_generic_result_infers() -> None:
    """The wiring's generic ``R`` is the body's declared return: the
    promise's payload type follows the body's own annotation (the
    ``Promise[R]`` inference face), and the encode path stays typed."""
    app = WorkflowApp()

    @app.workflow("transactional_positive_probe_generic")
    def flow() -> Promise[SettlementReport]:
        source = chain_source(_chain(), _stream, key="source")
        promise: Promise[SettlementReport] = transactional_step(
            settle_via_base, source, key="settle"
        )
        return promise

    _ = app


_ = build

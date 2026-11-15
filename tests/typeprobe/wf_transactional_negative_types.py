"""NEGATIVE TYPE PROBES — the transactional execution doors (pyright 1.1.414 +
ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py

The joined page door's connection parameter is the actual stable asyncpg
session union (a real Connection or the pool's connection proxy), never a
string/session-facade object; the transactional_step wiring verb carries
step's placement/policy surface WITHOUT the cache face (the settlement's
terminal is its own boundary, never a cacheable address)."""

from __future__ import annotations

import asyncio

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
    step,
    transactional_step,
)


class Ingest(BaseModel):
    doc_id: str


async def _leaf(ctx: StepContext, item: object) -> str:
    return "ok"


def _chain() -> Chain:
    return Chain(
        name="transactional_probe_records", start="screen", steps={"screen": Step(_leaf, str)}
    )


async def _sync_body(ctx: StepContext) -> object:
    return asyncio.sleep(0)


def _plain_body(ctx: StepContext) -> object:
    """A deliberately SYNC body: the transactional wiring refuses it."""
    return None


async def settle_body(ctx: StepContext, source_result: object) -> None:
    return None


def probe_joined_connection_is_the_session_union() -> None:
    """The joined page door takes an actual asyncpg session: a bare object
    (the session/ORM-facade shape) reds."""
    app = WorkflowApp()

    @app.workflow("probe_joined_session")
    def probe_joined_session() -> Promise[object]:
        async def source(ctx: StepContext) -> object:
            facade: object = object()
            await ctx.emit_batch(
                [], cursor={}, connection=facade, schema="taskq"
            )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the joined door takes an actual asyncpg session, never a bare object
            return {}

        return build(chain_source(_chain(), source, key="source"))

    _ = app


def probe_joined_cursor_is_the_session_union() -> None:
    """The joined cursor read takes the same session union."""
    app = WorkflowApp()

    @app.workflow("probe_joined_cursor")
    def probe_joined_cursor() -> Promise[object]:
        async def source(ctx: StepContext) -> object:
            facade: object = object()
            await ctx.cursor(
                connection=facade
            )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the joined cursor read takes the same session union
            return {}

        return build(chain_source(_chain(), source, key="source"))

    _ = app


def probe_transactional_body_must_be_awaitable() -> None:
    """The settlement body is awaitable — a plain sync function reds (the
    wiring is the checker's door)."""
    app = WorkflowApp()

    @app.workflow("probe_transactional_body")
    def probe_transactional_body() -> Promise[object]:
        source = chain_source(_chain(), _sync_body, key="source")
        return transactional_step(
            _plain_body, source, key="settle"
        )  # MUST_ERROR(reportArgumentType, reportUnknownVariableType, invalid-argument-type): the settlement body must be awaitable

    _ = app


def probe_transactional_has_no_cache_face() -> None:
    """The settlement's terminal is its own boundary — the cache face is
    deliberately absent from the transactional wiring."""
    app = WorkflowApp()

    @app.workflow("probe_transactional_cache")
    def probe_transactional_cache() -> Promise[object]:
        source = chain_source(_chain(), _sync_body, key="source")
        return transactional_step(
            settle_body, source, key="settle", cache=True
        )  # MUST_ERROR(reportCallIssue, no-matching-overload, unknown-argument, invalid-argument-type): the transactional wiring has no cache face

    _ = app


def probe_transactional_key_is_a_str() -> None:
    """The wiring key is a str (the ledger's identity) — an int key reds."""
    app = WorkflowApp()

    @app.workflow("probe_transactional_key")
    def probe_transactional_key() -> Promise[object]:
        source = chain_source(_chain(), _sync_body, key="source")
        return transactional_step(
            settle_body, source, key=404
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): the node key is a str

    _ = app


def probe_transactional_queue_is_a_str() -> None:
    """The placement queue is a str — a None queue reds."""
    app = WorkflowApp()

    @app.workflow("probe_transactional_runtime")
    def probe_transactional_runtime() -> Promise[object]:
        source = chain_source(_chain(), _sync_body, key="source")
        return transactional_step(
            settle_body, source, key="settle", queue=None
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): queue is a str placement, never None

    _ = app


def probe_ordinary_context_connection_is_optional() -> None:
    """The ORDINARY body face: ``StepContext.connection`` is the optional
    claim-session union (``None`` on every pool-owned context) — a body
    that dereferences it without narrowing reds on both checkers. The
    non-optional face is the ``TransactionalStepContext`` subtype's alone."""
    app = WorkflowApp()

    @app.workflow("probe_ordinary_optional_connection")
    def probe_ordinary_optional_connection() -> Promise[object]:
        async def ordinary(ctx: StepContext) -> object:
            await ctx.connection.execute(
                "SELECT 1"
            )  # MUST_ERROR(reportOptionalMemberAccess, unresolved-attribute): the ordinary connection is optional — narrow or declare the transactional subtype
            return {}

        return build(chain_source(_chain(), ordinary, key="source"))

    _ = app


def probe_ordinary_step_refuses_the_transactional_demand() -> None:
    """A body wired through the ordinary ``step()`` that DEMANDS
    ``TransactionalStepContext`` could never be satisfied: the ordinary
    runner hands a plain ``StepContext`` (its ``connection`` is ``None``).
    The wiring door's body contract is the static face of E9's law — the
    demand reds at the wiring site, before execution."""
    app = WorkflowApp()

    @app.workflow("probe_ordinary_step_demands_subtype")
    def probe_ordinary_step_demands_subtype() -> Promise[object]:
        solo = step(
            demanding_body,  # MUST_ERROR(reportArgumentType, invalid-argument-type): the ordinary step's body contract accepts a StepContext (or wider) first parameter — the transactional subtype demand is unsatisfiable on a non-transactional node
            key="solo",
        )
        return build(solo)

    _ = app


async def demanding_body(ctx: TransactionalStepContext) -> object:
    return await ctx.connection.fetchval("SELECT 1")


_ = build

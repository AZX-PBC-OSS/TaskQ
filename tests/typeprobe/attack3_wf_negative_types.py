"""ATTACK-3 — the phase-3 public surface's NEGATIVE type probes.

Each ``MUST_ERROR`` marker names a wrong-shape call that must RED on
BOTH pinned checkers (pyright 1.1.414, ty 0.0.85) — the typed-doors law
(BUILD-PROTOCOL §7b). This file is run DIRECTLY by the attacker's gate
invocation (the shipped ``_gate.py`` corpus is untouched); the capture
goes to ``.measurements/attack3/``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

from pydantic import BaseModel

from taskq.workflows import WorkflowApp, build, loop, step


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    n: int


class Approval(BaseModel):
    verdict: str


async def _body(ctx: Any, params: Ingest) -> Report:
    return Report(n=1)


def probe_exhaustion_policy_literal() -> None:
    """The ExhaustionPolicy Literal: an off-vocabulary policy must red
    (statically AND at runtime the shipped surface never checks it —
    the static door is the only one)."""
    app = WorkflowApp()

    @app.workflow("a3t1")
    def a3t1() -> object:
        return build(
            loop("l1", _body, on_exhausted="abort")
        )  # MUST_ERROR: "abort" is not in Literal["escalate", "fail"]


def probe_sync_until_predicate() -> None:
    """The until= predicate's type: a SYNC closure (returning bool) is
    the convicted dragon — the API demands Callable[[], Awaitable[bool]]."""
    app = WorkflowApp()

    def sync_until() -> bool:
        return True

    @app.workflow("a3t2")
    def a3t2() -> object:
        return build(
            loop("l2", _body, until=sync_until)
        )  # MUST_ERROR: a sync bool-returning closure is not Awaitable[bool]


def probe_resolve_takes_a_dict_not_a_model() -> None:
    """HitlClient.resolve's decision: the shipped signature wants
    ``dict[str, object]`` — a model INSTANCE (the natural thing an
    operator holds) must red, naming the door's own shape gap."""
    from taskq.workflows.api._hitl import HitlClient

    async def probe(client: HitlClient) -> None:
        await client.resolve(
            "00000000-0000-0000-0000-000000000000", Approval(verdict="ok")
        )  # MUST_ERROR: an Approval instance is not a dict[str, object]


def probe_awaiting_a_promise_again() -> None:
    """The wiring handle is not awaitable (the T01 regression guard on
    the new surface's verbs)."""
    app = WorkflowApp()

    @app.workflow("a3t4")
    def a3t4() -> object:
        produced = step(_body, Ingest(doc_id="d"), key="produce")
        _pending = asyncio.ensure_future(
            produced
        )  # MUST_ERROR: a Promise is not a future/coroutine
        assert _pending is not None  # the reference is STORED (RUF006) and never awaited
        return build(produced)


def probe_loop_body_wrong_union() -> None:
    """The loop body must return Done/Refine — a body returning the raw
    payload type is the shape error (the residual machinery's static
    face: BodyFn's object return makes this invisible to the checker —
    this probe NAMES the gap; it is intentionally NOT a MUST_ERROR)."""
    app = WorkflowApp()

    async def raw_body(ctx: Any, carry: Any) -> Report:
        return Report(n=1)

    @app.workflow("a3t5")
    def a3t5() -> object:
        return build(loop("l5", raw_body, max_iterations=2))


_ = Awaitable

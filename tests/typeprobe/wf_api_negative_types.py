"""NEGATIVE TYPE PROBES — the T09 flow API's wiring (pyright 1.1.414 +
ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

Each ``MUST_ERROR(rules)`` marker names a wrong-shape call the typed-doors
law (BUILD-PROTOCOL §7b: "the negative probes (wrong-shape inputs RED on
both checkers) ship WITH the API") requires to be a checker error, PLUS
the EXACT rule-ids the pinned checkers must emit (the gate asserts the
ids — a stray unrelated error must not satisfy a probe). A probe the
checkers do NOT flag is a finding: the Any leak the probe demonstrates.

T09's doors probed here: the Promise is WIRING, not a future (awaiting
it — the wrong-nesting mistake — reds); a promise does not satisfy a
plain-data parameter (the direct unit-call form wants the DATA —
passing the wiring handle reds). The TypedGate's wrong-model delivery
door ships with T10's runtime (its probe lands with that surface).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel

from taskq.workflows import Exit, Promise, WorkflowApp, build, map_source, step


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _body(ctx: Any, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def probe_awaiting_a_promise() -> None:
    """The wrong-nesting mistake: a Promise is compile-time WIRING — it
    is not awaitable (the runtime enforces nothing here; the CHECKER is
    the door)."""
    app = WorkflowApp()

    @app.workflow("probe_await")
    def probe_await() -> Promise[object]:
        fetched = step(_body, Ingest(doc_id="d"), key="fetch")
        return asyncio.ensure_future(
            fetched
        )  # MUST_ERROR(reportCallIssue, reportArgumentType, no-matching-overload): not a future

    _ = app


async def probe_promise_where_data_is_wanted() -> None:
    """The DIRECT (unit-call) form takes the DATA: passing the wiring
    handle where the body's declared model is expected must red."""
    app = WorkflowApp()

    @app.workflow("probe_direct")
    def probe_direct() -> Promise[object]:
        fetched = step(_body, Ingest(doc_id="d"), key="fetch")
        return _body(
            None, fetched
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): promise, not the data

    _ = app


def probe_exit_bare_return() -> None:
    """THE EXIT SENTINEL'S RETURN POSITION (the §17.1 sentinel's type
    surface): a body whose annotation promises ``Exit[Report]`` that
    returns a BARE value is the checker error (the sentinel is the
    annotation's only valid return; a plain ``T`` return is the DATA
    result under a ``T`` annotation — sentinels appear only in control
    unions)."""
    app = WorkflowApp()

    @app.workflow("probe_exit_bare")
    def probe_exit_bare() -> Promise[object]:
        exit_body = _exit_body()
        return build(step(exit_body, Ingest(doc_id="d"), key="exit_bare"))

    _ = app


def _exit_body():
    async def exit_body(ctx: Any, params: Ingest) -> Exit[Report]:
        return Report(
            ref=params.doc_id
        )  # MUST_ERROR(reportReturnType, invalid-return-type): a bare Report is not the Exit payload

    return exit_body


def probe_exit_bare_return_none() -> None:
    """A BARE ``return`` under an ``Exit[Report]`` annotation — the
    None-end is not the sentinel's payload; the checker must red it
    (the asserted marker is on the return line below)."""
    app = WorkflowApp()

    @app.workflow("probe_exit_bare_none")
    def probe_exit_bare_none() -> Promise[object]:
        exit_body = _exit_body_none()
        return build(step(exit_body, Ingest(doc_id="d"), key="exit_bare_none"))

    _ = app


def _map_tail_exit_body() -> Callable[[Any, list[Report]], Awaitable[Exit[Report]]]:
    """THE MAP-JOIN'S PROMISE-VS-DATA DOOR (the consumption contract's
    type surface): the join's promise is WIRING — the direct unit-call
    form wants the COLLECTED DATA (the list), never the promise object.
    (The factory's return is ANNOTATED: ty infers an unannotated
    factory's inner closure as Unknown and goes silent — the annotation
    is what keeps the door checker-visible on BOTH checkers.)"""

    async def map_tail_exit(ctx: Any, items: list[Report]) -> Exit[Report]:
        return Exit(Report(ref=f"{len(items)}"))

    return map_tail_exit


async def probe_map_promise_where_data_is_wanted() -> None:
    """The DIRECT (unit-call) form on a map's join promise: passing the
    wiring handle where the collected data is expected must red (the
    map-join consumption contract's type surface)."""
    app = WorkflowApp()

    @app.workflow("probe_map_direct")
    def probe_map_direct() -> Promise[object]:
        src = step(_body, Ingest(doc_id="d"), key="src")
        mapped = map_source(src, _map_body)
        return await _map_tail_exit_body()(
            None, mapped
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): not the list

    _ = app


async def _map_body(ctx: Any, item: Report) -> Report:
    return item


def _exit_body_none():
    async def exit_body_none(ctx: Any, params: Ingest) -> Exit[Report]:
        return  # MUST_ERROR(reportReturnType, invalid-return-type): a bare return carries no Exit payload — the sentinel's return position

    return exit_body_none

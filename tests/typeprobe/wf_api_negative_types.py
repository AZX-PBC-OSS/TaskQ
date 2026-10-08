"""NEGATIVE TYPE PROBES — the T09 flow API's wiring (pyright 1.1.414 +
ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

Each ``MUST_ERROR`` marker names a wrong-shape call the typed-doors law
(BUILD-PROTOCOL §7b: "the negative probes (wrong-shape inputs RED on both
checkers) ship WITH the API") requires to be a checker error. A probe the
checkers do NOT flag is a finding: the Any leak the probe demonstrates.

T09's doors probed here: the Promise is WIRING, not a future (awaiting
it — the wrong-nesting mistake — reds); a promise does not satisfy a
plain-data parameter (the direct unit-call form wants the DATA —
passing the wiring handle reds). The TypedGate's wrong-model delivery
door ships with T10's runtime (its probe lands with that surface).
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from taskq.workflows import Promise, WorkflowApp, build, step


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
    def probe_await() -> object:
        fetched = step(_body, Ingest(doc_id="d"), key="fetch")
        return asyncio.ensure_future(fetched)  # MUST_ERROR: a Promise is not a future/coroutine — the wrong-nesting call must red

    _ = app


async def probe_promise_where_data_is_wanted() -> None:
    """The DIRECT (unit-call) form takes the DATA: passing the wiring
    handle where the body's declared model is expected must red."""
    app = WorkflowApp()

    @app.workflow("probe_direct")
    def probe_direct() -> object:
        fetched = step(_body, Ingest(doc_id="d"), key="fetch")
        # MUST_ERROR: the wiring handle is not the declared Ingest data —
        # the direct-call form's promise-vs-data confusion must red.
        return await _body(None, fetched)  # pyright: ignore[reportUnusedCoroutine]

    _ = app

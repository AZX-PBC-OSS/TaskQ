"""NEGATIVE TYPE PROBES — the workflows public API (pyright 1.1.414 + ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

Each ``MUST_ERROR`` marker names a call the typed-doors law (BUILD-PROTOCOL
§7b: "the negative probes (wrong-shape inputs RED on both checkers) ship
WITH the API") requires to be a checker ERROR. A probe the checkers do NOT
flag is a finding: the Any leak the probe demonstrates.

TODO(T01) — TICKET HOME for the probe-config finding: the root pyproject's
``tests`` executionEnvironment sets ``reportArgumentType = false`` (the
suite's duck-typed-seam relaxation), so a probe file under ``tests/`` is
MUTE for exactly the violations it asserts when checked by the root
config. The corpus therefore ships with its own pyrightconfig.json (this
directory) and the CI gate runs it against the PINNED checkers; the
root-config relaxation should eventually be narrowed to the files that
need it (the seam-fixture modules), not the whole ``tests/`` tree — until
then this corpus + gate is the typed-doors law's enforcement.
"""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg

from taskq.workflows import WorkflowSteps
from taskq.workflows.engine import finalize_node, render_workflow_sql
from taskq.workflows.ledger import insert_flow_run


async def probe_entry_is_any(conn: asyncpg.Connection) -> None:
    wsql = render_workflow_sql("public")
    # MUST_ERROR: *entry* is the flow definition's registered shape — the
    # core needs actor/queue/max_attempts/retry_kind/payload/trace_id. A
    # bare int (or any attribute-less object) must be a checker error.
    # SHIPPED: ``entry: Any`` (ledger.py:200) — not flagged by either checker.
    await insert_flow_run(conn, wsql, entry=42, run_key="k")  # MUST_ERROR


async def probe_workflow_steps_conn_is_any(conn: asyncpg.Connection) -> None:
    wsql = render_workflow_sql("public")  # noqa: F841  # Why: the probe binds the surface's shape; the bundle arg is exercised by the constructor's second positional.
    # MUST_ERROR: the step surface binds a DB connection + statement bundle;
    # arbitrary objects must not type-check.
    # SHIPPED: ``conn: Any, wsql: Any`` (context.py:37-38) — not flagged.
    steps = WorkflowSteps(42, "not-a-bundle", flow_id=conn, job_id=conn)  # MUST_ERROR
    await steps.step("s", _body)


async def _body() -> str:
    return "ran"


async def probe_sync_reducer_accepted(pool: Any, wsql: Any) -> None:
    # MUST_ERROR: reducers are ``dict[str, Callable[[], Awaitable[None]]]``;
    # a SYNC callable is the wrong shape (the body's await is the
    # tx2-boundary contract).
    await finalize_node(
        pool,  # pyright: ignore[reportArgumentType]
        wsql,  # pyright: ignore[reportArgumentType]
        flow_id=1,  # pyright: ignore[reportArgumentType]
        job_id=1,  # pyright: ignore[reportArgumentType]
        step_key="s",
        worker_id=1,  # pyright: ignore[reportArgumentType]
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        reducers={"join": _sync_body},  # MUST_ERROR (sync, not Awaitable)
    )


def _sync_body() -> None:
    return None


async def probe_bad_outcome(pool: Any, wsql: Any) -> None:
    # MUST_ERROR: outcome is Literal['succeeded','failed','cancelled',
    # 'crashed','abandoned'].
    await finalize_node(
        pool,  # pyright: ignore[reportArgumentType]
        wsql,  # pyright: ignore[reportArgumentType]
        flow_id=1,  # pyright: ignore[reportArgumentType]
        job_id=1,  # pyright: ignore[reportArgumentType]
        step_key="s",
        worker_id=1,  # pyright: ignore[reportArgumentType]
        attempt=1,
        claim_epoch=0,
        outcome="skipped",  # MUST_ERROR
    )


async def probe_envelope_any_result(pool: Any, wsql: Any) -> None:
    # MUST_ERROR: the LedgerClaim envelope's ``result: Any`` leaks — a
    # caller can call ANY method on the memoized result with no checker
    # complaint (the envelope is the typed door; its payload must be
    # JSONValue-typed, not Any).
    from taskq.workflows.ledger import memoized_step_result

    memo = await memoized_step_result(
        conn=pool,
        wsql=wsql,
        flow_id=1,
        step_key="s",
        map_index=None,  # pyright: ignore[reportArgumentType]
    )
    if memo is not None:
        memo.result.anything_at_all()  # MUST_ERROR (Any leak through the envelope)


def _unused_asyncio() -> None:
    asyncio.sleep(0)

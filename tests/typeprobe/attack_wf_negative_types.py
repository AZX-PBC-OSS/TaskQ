"""NEGATIVE TYPE PROBES — the workflows public API (pyright 1.1.414 + ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

Each ``MUST_ERROR(rules)`` marker names a call the typed-doors law
(BUILD-PROTOCOL §7b: "the negative probes (wrong-shape inputs RED on both
checkers) ship WITH the API") requires to be a checker error, PLUS the
EXACT rule-ids the pinned checkers must emit (the gate asserts the ids —
a stray unrelated error must not satisfy a probe). A probe the checkers
do NOT flag is a finding: the Any leak the probe demonstrates.

The probes pass WELL-TYPED values everywhere but the asserted violation —
the noise lines must stay clean on BOTH checkers (the gate fails on any
unmarked error, so a probe's own sloppiness would red the gate, not the
probed door).

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

import uuid

import asyncpg

from taskq.backend._protocol import JobId
from taskq.workflows import WorkflowSteps
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import finalize_node, render_workflow_sql
from taskq.workflows.ledger import insert_flow_run

#: A stand-in identity for the probes' id-shaped kwargs (JobId is a UUID
#: NewType — the probe binds the SHAPE, it never runs).
_JID = JobId(uuid.UUID(int=0))


async def probe_entry_is_any(conn: asyncpg.Connection) -> None:
    wsql = render_workflow_sql("public")
    # MUST_ERROR(reportArgumentType, invalid-argument-type): *entry* is the
    # flow definition's registered shape — the core wants the FlowEntry
    # protocol (actor/queue/max_attempts/retry_kind/payload/trace_id); a
    # bare int (or any attribute-less object) must be a checker error.
    # SHIPPED: ``entry: FlowEntry`` (ledger.py) — the typed door holds.
    await insert_flow_run(
        conn, wsql, entry=42, run_key="k"
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type)


async def probe_workflow_steps_conn_is_any(conn: asyncpg.Connection) -> None:
    # MUST_ERROR(reportArgumentType, invalid-argument-type): the step
    # surface binds a DB connection (ConnLike) + statement bundle
    # (WorkflowSql); arbitrary objects must not type-check.
    steps = WorkflowSteps(
        42, "not-a-bundle", flow_id=conn, job_id=conn
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type)
    await steps.step("s", _body)


async def _body() -> str:
    return "ran"


async def probe_sync_reducer_accepted(pool: asyncpg.Pool, wsql: WorkflowSql) -> None:
    # MUST_ERROR(reportArgumentType, invalid-argument-type): reducers are
    # ``dict[str, Callable[[], Awaitable[None]]]``; a SYNC callable is the
    # wrong shape (the body's await is the tx2-boundary contract). Every
    # other kwarg is WELL-TYPED — only the violation reds (both checkers).
    await finalize_node(
        pool,
        wsql,
        flow_id=_JID,
        job_id=_JID,
        step_key="s",
        worker_id=_JID,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        reducers={
            "join": _sync_body
        },  # MUST_ERROR(reportArgumentType, invalid-argument-type) (sync, not Awaitable)
    )


def _sync_body() -> None:
    return None


async def probe_bad_outcome(pool: asyncpg.Pool, wsql: WorkflowSql) -> None:
    # MUST_ERROR(reportArgumentType, invalid-argument-type): outcome is
    # Literal['succeeded','failed','cancelled','crashed','abandoned'].
    await finalize_node(
        pool,
        wsql,
        flow_id=_JID,
        job_id=_JID,
        step_key="s",
        worker_id=_JID,
        attempt=1,
        claim_epoch=0,
        outcome="skipped",  # MUST_ERROR(reportArgumentType, invalid-argument-type)
    )


async def probe_envelope_payload_is_not_any(conn: asyncpg.Connection, wsql: WorkflowSql) -> None:
    # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): the
    # LedgerClaim envelope's result payload is JSONValue-typed — an
    # ARBITRARY member access on it must red on BOTH checkers (the
    # envelope is the typed door; its payload is not an Any-shaped
    # anything-goes).
    from taskq.workflows.ledger import memoized_step_result

    memo = await memoized_step_result(
        conn=conn,
        wsql=wsql,
        flow_id=_JID,
        step_key="s",
        map_index=None,
    )
    if memo is not None:
        memo.result.anything_at_all()  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute) (arbitrary member on the typed payload)

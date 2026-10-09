"""The typed EARLY-EXIT + the manual retry (the split of ``api/_runner``
— §7b's concerns-separate law): §17.1's exit (the sentinel ends the
flow, the downstream marked skipped-with-the-record) and §17.2's manual
resume (the operator's audited, CAS-guarded re-arm of a terminal-failed
node's ladder).

Module homes: the ``Exit`` sentinel's declaration lives in
``api/_graph``; the engine's ``finalize_node``/``fan_in_skip`` own the
row mechanics; the runner's exit face here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import asyncpg
import structlog

from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._types import _jsonb
from taskq.workflows.api._graph import Exit
from taskq.workflows.api._runner_codec import jsonable
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.api._sql_runner import (
    EXIT_SKIP_SQL_TEMPLATE,
    RETRY_FLOW_REOPEN_SQL_TEMPLATE,
    RETRY_NODE_CAS_SQL_TEMPLATE,
    RETRY_REOPEN_CLOSURE_SQL_TEMPLATE,
    render_sql,
)
from taskq.workflows.engine import finalize_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

__all__ = ["ExitOps"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


class _ExitHost(Protocol):
    """The runner surface the exit machinery reads (the split's typed
    seam — the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    schema: str
    compiled: Any
    _worker_id: JobId

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None: ...


class ExitOps(_ExitHost):
    """The runner's exit/resume mixin (the machinery's home; the runner
    composes it)."""

    __slots__ = ()

    async def _finalize_exit(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        exit_value: Exit[object],
        *,
        claim_epoch: int = 0,
    ) -> None:
        """THE TYPED EARLY-EXIT (§17.1, attack-audit's Missing #1): the
        body returned ``Exit(payload)`` — the node TERMINAL-SUCCEEDS
        with the typed payload (the envelope records the exit — it never
        lies about what ran), and every non-terminal DOWNSTREAM node is
        marked SKIPPED-WITH-THE-RECORD (``{"skipped": true,
        "exit_from": <node>}`` — zero ledger rows, a skip is not an
        attempt; the join-firing machinery never runs for rows the exit
        resolved). All rows terminal → the derivation reports the
        workflow COMPLETE. The LEDGER records the exit on the exited
        node's own terminal row (the result carries the marker)."""
        step_key: str = row["step_key"]
        await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=step_key,
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            outcome="succeeded",
            result={"value": jsonable(exit_value.payload), "exit": True},
            map_index=row["map_index"],
        )
        # THE DOWNSTREAM MARK: the compiled graph's descendants (the
        # wiring's own edges — never a runtime guess), skipped with the
        # record. Bounded by the graph's size; a downstream row that is
        # already terminal (or does not exist — an unspawned map join)
        # is untouched.
        descendants = self._descendants_of(step_key)
        if descendants:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    render_sql(EXIT_SKIP_SQL_TEMPLATE, self.schema),
                    flow_id,
                    _jsonb({"skipped": True, "exit_from": step_key}),
                    sorted(descendants),
                )

    def _descendants_of(self, node_key: str) -> set[str]:
        """The compiled graph's transitive downstream of one node (the
        wiring's children adjacency — the exit's marked set)."""
        children: dict[str, list[str]] = {}
        for key, node in self.compiled.nodes.items():
            for parent in node.parents:
                children.setdefault(parent, []).append(key)
        seen: set[str] = set()
        frontier = [node_key]
        while frontier:
            current = frontier.pop()
            for child in children.get(current, ()):
                if child not in seen:
                    seen.add(child)
                    frontier.append(child)
        return seen

    async def retry_node(
        self,
        flow_id: JobId,
        node_key: str,
        *,
        principal: Any = None,
        reason: str | None = None,
    ) -> bool:
        """THE MANUAL RESUME (§17.2, the §22.6 redispatch row 3 — "the
        ladder then manual"; attack-audit's Missing #1): the operator's
        audited, CAS-guarded re-arm of a TERMINAL-FAILED node's ladder.

        * THE ATTEMPT ORDINAL CONTINUES — never resets: the claim's
          counter is untouched, the ladder's 'failed' ledger rows are the
          count, so each manual retry buys EXACTLY ONE more attempt (a
          failure re-fails the node; the operator retries again — the
          manual arm is the ladder's continuation, not a fresh budget).
        * SAFE BY THE STEP LEDGER (T05): the re-run's ``ctx.step`` calls
          return the recorded results — the replay is the ledger's own
          contract.
        * THE BLOCKED CLOSURE RE-OPENS: the failed node's downstream
          rows the cascade BLOCKED (``blocking_reason='failed_parent'``)
          return to join-wait (``'join'``) — the sweep's re-derive
          re-derives them from the edge ledger on the next pass (the
          counter-as-cache law; a stamp is the cache, never the truth).
        * THE FLOW RE-OPENS: a terminal-FAILED flow root returns to
          ``running`` (the manual resume's own linearization — a
          cancelled flow stays closed: the operator's cancel is
          deliberate, the resume does not second-guess it).
        * THE AUDIT (G4): "who retried this" is a ROW.

        Returns ``True`` when the CAS granted the re-arm. Surfaced as
        the ``taskq flows retry`` verb (T12's ticket)."""
        from taskq.audit import record_admin_action

        descendants = sorted(self._descendants_of(node_key))
        async with self.pool.acquire() as conn, conn.transaction():
            # THE NODE'S CAS (one grant): terminal-FAILED → pending; the
            # attempt ordinal untouched (CONTINUES — the ladder's own
            # count is the budget).
            node_id = await conn.fetchval(
                render_sql(RETRY_NODE_CAS_SQL_TEMPLATE, self.schema),
                flow_id,
                node_key,
            )
            if node_id is None:
                return False
            # THE CLOSURE RE-OPENS: the blocked downstream rows return
            # to join-wait (the re-derive re-derives them — a stamp is
            # the cache).
            if descendants:
                await conn.execute(
                    render_sql(RETRY_REOPEN_CLOSURE_SQL_TEMPLATE, self.schema),
                    flow_id,
                    descendants,
                )
            # THE FLOW RE-OPENS: a terminal-FAILED root → running (the
            # maintenance leg owns the verdict from the rows again).
            await conn.execute(render_sql(RETRY_FLOW_REOPEN_SQL_TEMPLATE, self.schema), flow_id)
            await record_admin_action(
                conn,
                schema=self.schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{flow_id}:{node_key}",
                reason=reason,
                detail={"node_key": node_key, "closure_reopened": len(descendants)},
            )
        return True


_EXIT_CANCEL_ROOT_SQL_TEMPLATE = """
WITH flipped AS (
    UPDATE {schema}.jobs SET status = 'cancelled',
        error_class = 'WorkflowCancelled',
        error_message = $2,
        finished_at = clock_timestamp()
    WHERE id = $1
      AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
    RETURNING id, status::text AS to_state
), evt AS (
    -- THE AUDIT LEG (the same cure CANCEL_ROOT carries): the cancel's
    -- flip is a mutation — the event names it.
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', f.to_state,
                              'error_class', 'WorkflowCancelled')
    FROM flipped f
)
SELECT id FROM flipped
"""

_EXIT_CANCEL_NODES_SQL_TEMPLATE = """
WITH flipped AS (
    UPDATE {schema}.jobs
    SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
        finished_at = CASE WHEN status = 'running' THEN finished_at ELSE clock_timestamp() END,
        error_class = CASE WHEN status = 'running' THEN error_class ELSE 'WorkflowCancelled' END,
        metadata = metadata || '{"cancel_phase": "cooperative"}'::jsonb
    WHERE (metadata->>'flow_id')::uuid = $1
      AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
    RETURNING id, (status::text) AS to_state
), evt AS (
    -- THE AUDIT LEG: only the rows THIS statement terminalised (the
    -- running row's cooperative phase-1 keeps it running — its own
    -- terminal write carries its event).
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'pending', 'to_state', f.to_state,
                              'error_class', 'WorkflowCancelled')
    FROM flipped f
    WHERE f.to_state = 'cancelled'
)
SELECT count(*) FROM flipped
"""


# ── the run-operator verbs (the id-addressed forms — T12) ───────────────
#
# The runner's methods own the rows work for a COMPILED flow; the CLI's
# ``flows cancel`` / ``flows retry`` address a RUN BY ID (no compiled
# definition to import) — these forms carry the SAME statements for the
# id-addressed caller. One engine, two surfaces: the rows work lives
# once per shape.


async def cancel_workflow_run(
    pool: asyncpg.Pool,
    *,
    schema: str,
    flow_id: JobId,
    reason: str | None = None,
    principal: Any = None,
) -> int:
    """P3 rule 4's cancel (T10): ONE transaction — the flow flip is
    the linearization point; every non-terminal node + held signal
    resolves in the same snapshot; running children take the
    existing two-phase cooperative cancel; terminal rows untouched;
    IDEMPOTENT (cancel twice = one cancel — a terminal root updates
    nothing). THE AUDIT (G4): "who cancelled this" is a ROW, not a
    log line."""
    from taskq.workflows.api._hitl import cancel_run_signals

    async with pool.acquire() as conn, conn.transaction():
        flipped = await conn.fetchval(
            render_sql(_EXIT_CANCEL_ROOT_SQL_TEMPLATE, schema),
            flow_id,
            (reason or "cancel_workflow")[:500],
        )
        if flipped is None:
            return 0  # already terminal — idempotent
        await conn.execute(render_sql(_EXIT_CANCEL_NODES_SQL_TEMPLATE, schema), flow_id)
        held = await cancel_run_signals(conn, schema=schema, workflow_id=flow_id)
        # THE AUDIT ROW (the caller owns the tx — the same-tx
        # guarantee; the lazy import keeps the layering).
        from taskq.audit import record_admin_action

        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action="workflow.cancel",
            target_type="workflow_run",
            target_id=str(flow_id),
            reason=reason,
            detail={"held_signals_cancelled": held},
        )
    return held + 1


async def retry_workflow_node(
    pool: asyncpg.Pool,
    *,
    schema: str,
    flow_id: JobId,
    node_key: str,
    reason: str | None = None,
    principal: Any = None,
) -> int:
    """THE MANUAL RESUME (T12), id-addressed: the ExitOps' own CAS
    statements for the caller that addresses the run BY ID (no compiled
    definition to import). A cancelled run REFUSES (the cancel was
    deliberate — re-opening it is a second decision, not a resume).
    THE AUDIT (G4): the row.

    Returns 1 when the CAS granted the re-arm, 0 when nothing was
    resumable (the node is live, unknown, or not terminal-failed)."""
    async with pool.acquire() as conn, conn.transaction():
        root = await conn.fetchval(
            render_sql("SELECT status FROM {schema}.jobs WHERE id = $1", schema),
            flow_id,
        )
        if root == "cancelled":
            raise WorkflowRunError(
                f"run {flow_id} is cancelled — re-opening it is a second decision, "
                "not a resume (create a new run, or un-cancel deliberately)"
            )
        node_id = await conn.fetchval(
            render_sql(RETRY_NODE_CAS_SQL_TEMPLATE, schema), flow_id, node_key
        )
        if node_id is None:
            return 0
        descendants = await conn.fetch(render_sql(_DESCENDANTS_SQL, schema), flow_id, [node_key])
        keys = [r["step_key"] for r in descendants]
        if keys:
            await conn.execute(render_sql(RETRY_REOPEN_CLOSURE_SQL_TEMPLATE, schema), flow_id, keys)
        await conn.execute(render_sql(RETRY_FLOW_REOPEN_SQL_TEMPLATE, schema), flow_id)
        # THE AUDIT ROW (the caller owns the tx — the same-tx guarantee).
        from taskq.audit import record_admin_action

        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action="workflow.retry_node",
            target_type="workflow_node",
            target_id=f"{flow_id}:{node_key}",
            reason=reason,
            detail={"run_id": str(flow_id), "node_key": node_key},
        )
    return 1


_DESCENDANTS_SQL = """
SELECT DISTINCT c.step_key
FROM {schema}.wf_edge e
JOIN {schema}.jobs c ON c.id = e.child_id
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE (p.metadata->>'flow_id')::uuid = $1 AND p.step_key = ANY($2)
"""

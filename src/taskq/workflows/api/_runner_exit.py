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
    RETRY_SKIP_REARM_SQL_TEMPLATE,
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
        """THE COMPILED FACE'S DELEGATE (the round-2 conviction 5a): the
        id-addressed verb ``retry_workflow_node`` is the ONE manual-resume
        implementation — this module's own parallel re-derivation (a
        second CAS + closure + audit walk) was dead in production AND
        STALE (it lacked the H-C3 FOR-UPDATE serialization, the H-C4
        skip re-arm, and the cancelled-run refusal the verb carries —
        the exact drift a copy invites). The runner's historical call
        shape stays (the phase-3 pins compose it), riding the verb's
        law. The import is the LAZY PUBLIC-SEAM form (the routes' own
        shape) so every caller patches ONE name. Returns ``True`` when
        the CAS granted the re-arm (the verb's row count > 0)."""
        from taskq.workflows import retry_workflow_node

        granted = await retry_workflow_node(
            self.pool,
            schema=self.schema,
            flow_id=flow_id,
            node_key=node_key,
            reason=reason,
            principal=principal,
        )
        return granted > 0


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


async def _cancel_one_run(
    pool: asyncpg.Pool,
    *,
    schema: str,
    flow_id: JobId,
    reason: str | None,
    principal: Any,
) -> int:
    """ONE run's cancel (the tx the two verbs share): the root flip is
    the linearization point; every non-terminal node + held signal
    resolves in the same snapshot; terminal rows untouched; IDEMPOTENT.
    Returns the held signals cancelled (0 when the run was already
    terminal — the idempotent no-op)."""
    from taskq.audit import bound_reason
    from taskq.workflows.api._hitl import cancel_run_signals

    bounded_reason = bound_reason(reason or "cancel_workflow", 500)

    async with pool.acquire() as conn, conn.transaction():
        flipped = await conn.fetchval(
            render_sql(_EXIT_CANCEL_ROOT_SQL_TEMPLATE, schema),
            flow_id,
            bounded_reason,
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
    return int(held) + 1


async def cancel_cascade(
    pool: asyncpg.Pool,
    *,
    schema: str,
    flow_id: JobId,
    reason: str | None,
    principal: Any,
) -> int:
    """THE CANCEL CASCADE (rv5-C2b/C4's cure): the WORKLIST WALK, never
    Python recursion (the pre-cure shape recursed per child — a
    1200-deep cascade raised ``RecursionError`` at the operator and
    left hundreds of runs running), and the children are read from the
    ROWS' crash-safe ``parent_flow_id`` direction FIRST — the receipts
    are the accelerator, the rows the law (the pre-cure shape walked
    the receipts alone: a child created but never receipted — the crash
    between create and receipt-write — ESCAPED the cascade forever, the
    orphan window). Each run's cancel is atomic in its own tx
    (eventually-consistent per child, idempotent by the terminal
    refusal); the walk is bounded by the run tree's own size."""
    from taskq.workflows._childflow import CHILD_FLOWS_OF_RUN_SQL_TEMPLATE

    frontier: list[JobId] = [flow_id]
    seen: set[str] = set()
    held = 0
    while frontier:
        current = frontier.pop()
        current_key = str(current)
        if current_key in seen:
            continue
        seen.add(current_key)
        cascaded = current_key != str(flow_id)
        held += await _cancel_one_run(
            pool,
            schema=schema,
            flow_id=current,
            reason=(
                f"cascaded from {flow_id}: {reason or 'cancel_workflow'}" if cascaded else reason
            ),
            principal=principal,
        )
        # THE CHILDREN (both directions — the rows' law + the receipts'
        # accelerator), collected AFTER the run's own cancel committed:
        # the parent's cancel is the linearization point, the children's
        # cancels follow it.
        async with pool.acquire() as conn:
            child_rows = await conn.fetch(
                render_sql(CHILD_FLOWS_OF_RUN_SQL_TEMPLATE, schema), current
            )
        frontier.extend(JobId(row["cid"]) for row in child_rows)
    return held


async def cancel_workflow_run(
    pool: asyncpg.Pool,
    *,
    schema: str,
    flow_id: JobId,
    reason: str | None = None,
    principal: Any = None,
) -> int:
    """P3 rule 4's cancel (T10): the flow flip is the linearization
    point; every non-terminal node + held signal resolves in the same
    snapshot; the CASCADE walks the children ITERATIVELY (the worklist
    — rv5-C2b: depth 1200 cancels clean, no RecursionError) from the
    ROWS' parent links first (rv5-C4: the orphan window is closed —
    the crash-safe direction is the law, the receipts the accelerator).
    Running children take the existing two-phase cooperative cancel;
    terminal rows untouched; IDEMPOTENT (cancel twice = one cancel — a
    terminal root updates nothing). THE AUDIT (G4): "who cancelled
    this" is a ROW, not a log line."""
    return await cancel_cascade(
        pool, schema=schema, flow_id=flow_id, reason=reason, principal=principal
    )


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

    * THE ROOT KEY IS NOT A NODE (H-C1): ``node_key='__flow__'`` matches
      nothing (the CAS's own guard) — returns 0, the root's bytes stay
      put; the run's own re-run is the RUN-rerun's concern (a fresh run,
      or the run-rerun composition — never the per-node CAS).
    * THE CANCEL/RETRY SERIALIZATION (H-C3): the root's status read
      takes ``FOR UPDATE`` — a cancel committing between the read and
      the CAS can no longer strand a PENDING node under a CANCELLED
      root: the refusal is race-free (the read sees the cancel's flip),
      and a retry that wins the lock serializes the cancel BEHIND its
      own commit (the cancel's cascade then flips the re-armed node —
      never a pending row under a cancelled root, the wedged-forever
      shape).
    * THE EXIT-SKIPPED BRANCHES RE-ARM (H-C4): the grant re-opens the
      run's skip-envelope rows (``result @> '{"skipped": true}'`` — the
      engine's own SKIPPED_RESULT contract, the typed early-exit's
      downstream mark) — pending, the envelope CLEARED — so the resumed
      run re-runs the cut-short work the resume promised (the count
      rides the audit row).

    Returns the number of NODE ROWS the CAS re-armed (0 when nothing
    was resumable — the node is live, unknown, not terminal-failed, or
    the run's own root key). THE ROW COUNT, NOT A GRANT BIT (the
    round-2 conviction): a MAP's failed children share one step key —
    one key's grant re-arms every failed row of the key at once, and
    the caller's applied-count counts ROWS (the audit row carries the
    count beside the key's)."""
    from taskq.audit import record_admin_action

    async with pool.acquire() as conn, conn.transaction():
        # THE ROOT'S STATUS READ — FOR UPDATE (H-C3): the lock is the
        # serialization point. A cancel's root flip now either landed
        # BEFORE this read (the refusal below sees it) or waits BEHIND
        # this transaction's commit (the cancel's cascade then flips the
        # re-armed node). The bare read's gap was the wedge: a pending
        # node under a cancelled root, forever.
        root = await conn.fetchval(
            render_sql("SELECT status FROM {schema}.jobs WHERE id = $1 FOR UPDATE", schema),
            flow_id,
        )
        if root == "cancelled":
            raise WorkflowRunError(
                f"run {flow_id} is cancelled — re-opening it is a second decision, "
                "not a resume (create a new run, or un-cancel deliberately)"
            )
        rows = await conn.fetch(render_sql(RETRY_NODE_CAS_SQL_TEMPLATE, schema), flow_id, node_key)
        if not rows:
            return 0
        descendants = await conn.fetch(render_sql(_DESCENDANTS_SQL, schema), flow_id, [node_key])
        keys = [r["step_key"] for r in descendants]
        if keys:
            await conn.execute(render_sql(RETRY_REOPEN_CLOSURE_SQL_TEMPLATE, schema), flow_id, keys)
        # THE SKIP ENVELOPE'S RE-ARM (H-C4): the run's exit-skipped rows
        # (the typed early-exit's downstream mark — the engine's own
        # SKIPPED_RESULT contract, ``{"skipped": true}`` containment)
        # return to pending with the envelope CLEARED — the resumed run
        # re-runs the cut-short work (a skip is not an attempt; the
        # envelope must never outlive the resume it would lie about).
        # Same transaction as the grant: the run cannot re-open WITHOUT
        # its skipped branches re-arming.
        skip_rows = await conn.fetch(render_sql(RETRY_SKIP_REARM_SQL_TEMPLATE, schema), flow_id)
        skips_rearmed = len(skip_rows)
        await conn.execute(render_sql(RETRY_FLOW_REOPEN_SQL_TEMPLATE, schema), flow_id)
        # THE AUDIT ROW (the caller owns the tx — the same-tx guarantee);
        # the ROWS count rides the detail (the map-key retry's census).
        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action="workflow.retry_node",
            target_type="workflow_node",
            target_id=f"{flow_id}:{node_key}",
            reason=reason,
            detail={
                "run_id": str(flow_id),
                "node_key": node_key,
                "rows_reopened": len(rows),
                "skips_rearmed": skips_rearmed,
            },
        )
    return len(rows)


_DESCENDANTS_SQL = """
SELECT DISTINCT c.step_key
FROM {schema}.wf_edge e
JOIN {schema}.jobs c ON c.id = e.child_id
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE (p.metadata->>'flow_id')::uuid = $1 AND p.step_key = ANY($2)
"""

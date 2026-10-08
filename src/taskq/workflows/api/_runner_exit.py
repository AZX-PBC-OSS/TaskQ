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
        from taskq.web.admin._audit import record_admin_action

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

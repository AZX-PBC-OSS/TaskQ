"""The chain step's ROUTER SURFACE (the split of ``api/_runner`` — §7b's
concerns-separate law): the T20 chain step's finalize — the body's typed
outcome IS the router's decision, at most one forked child, no fan-in.

Module homes: the chain's declaration verbs live in
``taskq.workflows.chain`` (``chain_start``/``chain_source``/``Route``);
the routing machinery (the fork-at-finalize) in the engine; the runner's
routed-finalize face here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import asyncpg
import structlog

from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows.api._runner_codec import encode_result
from taskq.workflows.engine import finalize_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

__all__ = ["ChainOps"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


class _ChainHost(Protocol):
    """The runner surface the chain finalize reads (the split's typed
    seam — the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    _worker_id: JobId


class ChainOps(_ChainHost):
    """The runner's chain/router mixin (the machinery's home; the runner
    composes it)."""

    __slots__ = ()

    async def _finalize_chain_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        step_key: str,
        chain: Any,
        outcome: object,
        payload_doc: dict[str, object],
        *,
        claim_epoch: int = 0,
    ) -> None:
        """THE CHAIN STEP'S ROUTED FINALIZE (T20): the body's typed
        outcome is the router's decision —

        * an arm to a step key → the certified fork-at-finalize carries
          AT MOST ONE child (no fan-in, no join): the record's payload
          rides the row's own (verbatim), its identity (``map_index``)
          and trace ride forward (the refuted-claim discipline);
        * DONE → no fork (the chain ends here);
        * an outcome with no arm → the LOUD refusal: the step
          terminal-FAILS with ``error_class='RouterNotTotal'`` — the
          record names the defect, the chain visibly dies, never
          silently drops (the declaration-time totality check makes this
          door unreachable for a truth-telling body; it exists for the
          body that lied about its type)."""
        from taskq.workflows.chain import RouterNotTotal, chain_fork

        try:
            child = chain.next_child(
                step_key,
                outcome,
                payload=payload_doc,
                map_index=row["map_index"],
                trace_id=row.get("trace_id"),
            )
        except RouterNotTotal as exc:
            logger.warning(
                "node.router_not_total",
                run_id=str(flow_id),
                node=step_key,
                outcome=repr(outcome),
            )
            await finalize_node(
                self.pool,
                self.wsql,
                flow_id=flow_id,
                job_id=JobId(row["id"]),
                step_key=step_key,
                worker_id=self._worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                outcome="failed",
                error_class="RouterNotTotal",
                error_message=str(exc)[:500],
                map_index=row["map_index"],
            )
            return
        fork = chain_fork(child, trace_id=row.get("trace_id"))
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
            result=encode_result(outcome),
            fork=fork,
            map_index=row["map_index"],
        )

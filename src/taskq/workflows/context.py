"""``ctx.step`` — the workflow job context's memoized step runner (T05).

``ctx.step("name", fn)`` in the workflow job context: the step-ledger
claim's user-facing shape. Default idempotent (ON) — silent double-runs are
the failure class this layer exists to prevent; ``idempotent=False`` opts a
step out (harmless redelivery, or a payload too large to key).

The replay contract: the retried step's key is
``(workflow, step_key[, map_index])`` → the claim path returns the
recorded result rather than re-executing. The node's own finalize records
the ledger terminal (riding tx1 — the ledger-terminal-atomic rule);
``ctx.step``'s claim is the attempt's grant of work.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from taskq.backend._protocol import JobId
from taskq.workflows.ledger import (
    LedgerClaim,
    claim_step_ledger,
    memoized_step_result,
)

__all__ = ["WorkflowSteps"]

StepFn = Callable[..., Awaitable[Any]]


class WorkflowSteps:
    """The step surface bound to one (flow run, node attempt)."""

    def __init__(
        self,
        conn: Any,
        wsql: Any,
        *,
        flow_id: JobId,
        job_id: JobId,
        map_index: int | None = None,
        attempt: int = 1,
    ) -> None:
        self._conn = conn
        self._wsql = wsql
        self._flow_id = flow_id
        self._job_id = job_id
        self._map_index = map_index
        self._attempt = attempt

    async def step(
        self,
        name: str,
        fn: StepFn,
        *args: Any,
        idempotent: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Run *fn* once per (flow, step key); replay returns the recorded
        result. ``idempotent=False`` opts out (the body re-runs on
        redelivery — its redelivery is declared harmless)."""
        if not idempotent:
            return await fn(*args, **kwargs)

        memo = await memoized_step_result(
            self._conn,
            self._wsql,
            flow_id=self._flow_id,
            step_key=name,
            map_index=self._map_index,
        )
        if memo is not None and memo.status == "succeeded":
            return memo.result

        claim: LedgerClaim = await claim_step_ledger(
            self._conn,
            self._wsql,
            flow_id=self._flow_id,
            job_id=self._job_id,
            step_key=name,
            map_index=self._map_index,
            attempt=self._attempt,
        )
        if claim.status == "succeeded" and claim.result is not None:
            # The ON CONFLICT path: the recorded result returns rather than
            # re-executing (the map-child retry's semantics).
            return claim.result
        return await fn(*args, **kwargs)

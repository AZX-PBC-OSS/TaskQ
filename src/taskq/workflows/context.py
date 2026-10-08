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

from pydantic import BaseModel

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.ledger import (
    LedgerClaim,
    claim_step_ledger,
    memoized_step_result,
)

__all__ = ["WorkflowSteps"]

StepFn = Callable[..., Awaitable[Any]]


class WorkflowSteps:
    """The step surface bound to one (flow run, node attempt). The TYPED
    doors: a live DB connection (``ConnLike``) + the schema's statement
    bundle (``WorkflowSql``) — arbitrary objects are checker errors (the
    T01 negative probes pin it)."""

    def __init__(
        self,
        conn: ConnLike,
        wsql: WorkflowSql,
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
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            await self._record_terminal(
                name, claim, "failed", None, type(exc).__name__, str(exc)[:500]
            )
            raise
        # THE STEP'S OWN TERMINAL (the re-execution doctrine's cheap side):
        # the memoized replay reads TERMINAL rows — a claim left 'running'
        # would re-execute on every resume (the defect the runner pins
        # convict: pre-wait side effects replay cheap, or not at all). The
        # FAILURE path records too: a raising step's claim terminalizes
        # 'failed' (the ladder's ledger shape), then the exception
        # propagates to the node's own failure handling.
        await self._record_terminal(name, claim, "succeeded", result, None, None)
        return result

    @staticmethod
    def _encode_step_result(result: Any) -> str:
        """The step result's jsonb form — the RAW value (no envelope: the
        memoized replay returns exactly what the step returned)."""
        if isinstance(result, BaseModel):
            return dumps_jsonb_str(result.model_dump(mode="json"))
        return dumps_jsonb_str(result)

    async def _record_terminal(
        self,
        name: str,
        claim: LedgerClaim,
        status: str,
        result: Any,
        error_class: str | None,
        error_message: str | None,
    ) -> None:
        """The step ledger's terminal write (the claim's own row when the
        id is held — the strongest key — else the arbiter tuple)."""
        if claim.ledger_id is not None:
            await self._conn.execute(
                self._wsql.ledger_terminal_by_id,
                claim.ledger_id,
                status,
                self._encode_step_result(result),
                error_class,
                error_message,
                None,
            )
        else:
            await self._conn.execute(
                self._wsql.ledger_terminal,
                self._flow_id,
                name,
                self._attempt,
                status,
                self._encode_step_result(result),
                error_class,
                error_message,
                None,
                self._map_index,
            )

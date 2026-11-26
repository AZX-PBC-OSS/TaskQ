"""``ctx.substep`` — the workflow job context's memoized step runner.

``ctx.substep("name", fn)`` in the workflow job context: the step-ledger
claim's user-facing shape. Default idempotent (ON) — silent double-runs are
the failure class this layer exists to prevent; ``idempotent=False`` opts a
step out (harmless redelivery, or a payload too large to key).

The replay contract: a record belongs to the calling node execution (its
job row and, in a loop body, its iteration) under its own name — the
ledger key is ``<job id>[.iter<n>]/<name>`` (#743) → a retry of that unit
replays the recorded ``succeeded`` result rather than re-executing. The
same name in another node, map child or iteration is a different record.
The node's own finalize records the ledger terminal (riding tx1 — the
ledger-terminal-atomic rule); ``ctx.substep``'s claim is the attempt's
grant of work.

the claim and the terminal write are ~1 ms statements, each on its own
acquisition — the body runs with no pool connection held. The pre-fix
shape held one connection from the claim to the terminal, so the
execution door's pool (the dispatcher pool, 4 connections at the
shipped default) throttled every concurrent map child to the conn
count: the team's A/B measured a 300-child fan-out collapse from 32
in-flight to 4 (10.1 s → 27.1 s at their profile; 11.1 s → 42.2 s at
0.5 s/call on this tree). The claim's exactly-once law never needed the
held connection: the claim arbiter
(``wf_step_ledger_claim_uniq``) linearizes the grant, and the terminal
keys the claim's own row id — the strongest write key there is.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
from asyncpg import Pool
from pydantic import BaseModel

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.ledger import (
    LedgerClaim,
    claim_substep_ledger,
    memoized_substep_result,
)

__all__ = ["WorkflowSteps"]

logger = get_logger(__name__)

StepFn = Callable[..., Awaitable[Any]]


class WorkflowSteps:
    """The step surface bound to one (flow run, node attempt). The TYPED
    doors: a live DB connection POOL (``Pool`` — the acquisition seam;
    the claim and the terminal each take their own connection, never
    one held across the body) + the schema's statement bundle
    (``WorkflowSql``) — arbitrary objects are checker errors (the type
    negative probes pin it)."""

    def __init__(
        self,
        pool: Pool,
        wsql: WorkflowSql,
        *,
        flow_id: JobId,
        job_id: JobId,
        map_index: int | None = None,
        attempt: int = 1,
        claim_epoch: int = 0,
        legacy_step_key: str | None = None,
        legacy_iteration_key: str | None = None,
    ) -> None:
        self._pool = pool
        self._wsql = wsql
        self._flow_id = flow_id
        self._job_id = job_id
        self._map_index = map_index
        self._attempt = attempt
        self._claim_epoch = claim_epoch
        self._legacy_step_key = legacy_step_key
        self._legacy_iteration_key = legacy_iteration_key

    async def step(
        self,
        name: str,
        fn: StepFn,
        *args: Any,
        idempotent: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Run *fn* once per *name*, the owning unit's scoped ledger key; a
        ``succeeded`` record replays. Re-calling a name whose previous call
        in the same attempt failed re-runs *fn* and records ITS outcome: the
        claim re-opens the failed row, so a later call, retry or resume
        replays a new ``succeeded`` result. If the re-call fails, its newest
        error replaces the old one and the next invocation runs *fn* again.
        ``idempotent=False`` opts out
        (the body re-runs on redelivery — its redelivery is declared
        harmless)."""
        if not idempotent:
            return await fn(*args, **kwargs)

        # the memoized read + the claim are ~1 ms statements; the connection
        # releases the moment they land — the body runs unwired.
        async with self._pool.acquire() as conn:
            memo = await memoized_substep_result(
                conn,
                self._wsql,
                flow_id=self._flow_id,
                job_id=self._job_id,
                step_keys=[name]
                + ([] if self._legacy_step_key is None else [self._legacy_step_key]),
                map_index=self._map_index,
                legacy_iteration_key=self._legacy_iteration_key,
            )
            if memo is not None and memo.status == "succeeded":
                if memo.step_key != name:
                    # only a pre-#743 executor writes a bare-name substep row
                    logger.warning(
                        "workflow-substep-legacy-replay",
                        flow_id=str(self._flow_id),
                        job_id=str(self._job_id),
                        step_key=memo.step_key,
                    )
                return memo.result

            claim = await claim_substep_ledger(
                conn,
                self._wsql,
                flow_id=self._flow_id,
                job_id=self._job_id,
                step_key=name,
                map_index=self._map_index,
                attempt=self._attempt,
                claim_epoch=self._claim_epoch,
            )
        if (
            memo is not None
            and memo.status == "failed"
            and memo.ledger_id is not None
            and memo.ledger_id == claim.ledger_id
            and claim.status == "running"
        ):
            # The claim collided with THIS caller's own failed row and
            # re-opened it: the outcome it cleared is gone from the ledger,
            # so name it before the row moves on (the recovery path — the
            # body caught the failure and re-called — is otherwise
            # indistinguishable from a first-try success). The id equality
            # makes the claim exact: the memo answers the HIGHEST-RANKED
            # terminal row, which can be a newer generation's failure while
            # the claim lands on this caller's own succeeded row — then
            # nothing was retired, and an unconditional log would announce
            # a retirement that never happened.
            logger.warning(
                "workflow-substep-failure-retired",
                flow_id=str(self._flow_id),
                job_id=str(self._job_id),
                step_key=claim.step_key,
                attempt=claim.attempt,
                claim_epoch=self._claim_epoch,
                error_class=memo.error_class,
                error_message=memo.error_message,
            )
        if claim.status == "succeeded":
            # The ON CONFLICT path: the recorded result returns rather than
            # re-executing (the map-child retry's semantics). A recorded
            # JSON null decodes to None and still replays: the status is the
            # record's marker, never the value's non-nullness (the memo leg
            # above tests status alone).
            return claim.result
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            await self._record_terminal(
                name, claim, "failed", None, type(exc).__name__, str(exc)[:500]
            )
            raise
        # the memoized replay reads terminal rows — a claim left 'running'
        # would re-execute on every resume (the defect the runner pins reject:
        # pre-wait side effects replay cheap, or not at all). The failure path
        # records too: a raising step's claim terminalizes 'failed' (the
        # ladder's ledger shape), then the exception propagates to the node's
        # own failure handling.
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
        """The step ledger's terminal write on ITS own acquisition (the
        LedgerWatch F4 fix, leg 2: the terminal never rides a connection held from
        the claim). The write keys the claim's OWN row id AND its own claim
        instant — the strongest identity there is
        (``claim_substep_ledger`` returns a nonnull id and a nonnull
        ``updated_at``; ``claim_step_ledger`` returns the same pair for the
        node/loop claim this statement also serves; no four-expression
        fallback exists).

        ``ledger_terminal_by_id`` is ``UPDATE ... RETURNING id``: the write
        returns the row it touched, so a ``None`` here means the guard
        matched nothing — another writer re-stamped the claim while this
        execution was still inside ``fn``. The refusal record separates the
        three causes by reading the row back by id:

        * ``"missing"`` — retention deleted the aged row
          (``STEP_LEDGER_RETENTION_SQL``);
        * ``"fenced"`` — a genuine by-id fence took the row
          (``LEDGER_FENCE_BY_ID_SQL``);
        * ``"superseded"`` — a newer claim of the same arbitration row
          re-stamped it (a duplicate execution).

        The phantom reap's fence is none of them as a refusal cause: it keeps
        a substep row's claim instant, and this write lands over it (#743)."""
        if claim.ledger_id is None or claim.claimed_at is None:
            # A memo/read projection (no actual claim behind it) has no
            # terminal of its own to record — manufacturing an identity
            # would rewrite some other claim's row.
            raise WorkflowRunError(
                "the step ledger's terminal write requires the actual "
                "claim's row id and claim instant; this LedgerClaim is a "
                "read projection and records no terminal"
            )
        row: asyncpg.Record | None = None
        async with self._pool.acquire() as conn:
            written = await conn.fetchval(
                self._wsql.ledger_terminal_by_id,
                claim.ledger_id,
                status,
                self._encode_step_result(result),
                error_class,
                error_message,
                None,
                claim.claimed_at,
            )
            if written is None:
                # The guarded write matched no row (#743 W-3): read the row
                # back so the record names the CAUSE, not just the outcome.
                row = await conn.fetchrow(self._wsql.ledger_row_status_by_id, claim.ledger_id)
        if written is None:
            # Silence here records a completed effect as abandoned with no
            # trace; the record's own value is already lost, so at least name
            # it — with the flow, the job, the row, the outcome the caller
            # asked to record, and which writer took the row.
            if row is None:
                reason = "missing"
            elif row["status"] == "fenced":
                reason = "fenced"
            else:
                reason = "superseded"
            logger.warning(
                "workflow-substep-terminal-refused",
                flow_id=str(self._flow_id),
                job_id=str(self._job_id),
                step_key=name,
                ledger_id=str(claim.ledger_id),
                status=status,
                error_class=error_class,
                reason=reason,
            )

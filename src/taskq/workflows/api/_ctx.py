"""The body's RUNTIME CONTEXT (the split of ``api/_runner`` — §7b's
concerns-separate law): :class:`StepContext` is the annotation every
workflow body declares — bodies read ``ctx.input`` and run side effects
through ``ctx.step`` (the ledger's replay contract), report progress
through ``ctx.progress``, and stream pages through ``ctx.emit_batch``.

The wait/deliver face (``wait_signal``/``signal`` — the HITL machinery)
lives in ``_ctx_wait`` and composes in; the runner builds the context at
the claim seam (``_runner``).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

import asyncpg

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.workflows._progress import ProgressEmitter, validate_emission
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild
from taskq.workflows.api._ctx_wait import CtxWaitOps
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.api._sql_runner import SOURCE_CURSOR_SQL_TEMPLATE, render_sql
from taskq.workflows.context import WorkflowSteps

__all__ = ["StepContext", "build_step_context"]


def build_step_context(
    *,
    flow_id: JobId,
    job_id: JobId,
    node_key: str,
    attempt: int,
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    input: object,
    map_index: int | None = None,
    is_loop: bool = False,
    ledger_id: JobId | None = None,
    workflow_name: str = "",
    redact: Callable[[str], str] | None = None,
    worker_id: JobId | None = None,
    progress: ProgressEmitter | None = None,
    claim_epoch: int = 0,
    flow_name: str | None = None,
    queue: str | None = None,
    claimed_at: datetime | None = None,
    budget_remaining_ms: int | None = None,
) -> StepContext:
    """The ONE context construction (the claim seam's factory): the
    runner's step path and the loop driver's iteration path build the
    same field set through here — the two call sites never drift."""
    return StepContext(
        flow_id=flow_id,
        job_id=job_id,
        node_key=node_key,
        attempt=attempt,
        input=input,
        _pool=pool,
        _wsql=wsql,
        _map_index=map_index,
        _is_loop=is_loop,
        _ledger_id=ledger_id,
        _workflow_name=workflow_name,
        _redact=redact,
        _worker_id=worker_id,
        _progress=progress,
        claim_epoch=claim_epoch,
        flow_name=flow_name,
        queue=queue,
        claimed_at=claimed_at,
        budget_remaining_ms=budget_remaining_ms,
    )


@dataclass(frozen=True, slots=True)
class StepContext(CtxWaitOps):
    """The body's runtime context: the run's identity + the input (cut
    #7) + the memoized step runner. Bodies read ``ctx.input`` and run
    side effects through ``ctx.step`` (the ledger's replay contract).

    THE DOCUMENTED ANNOTATION: a workflow body declares its context as
    ``ctx: StepContext`` — the checker verifies the body's ``ctx.*``
    reads against the real surface (users copy examples; the typed
    context is the story the docs tell)."""

    flow_id: JobId
    job_id: JobId
    node_key: str
    attempt: int
    input: object
    _pool: asyncpg.Pool
    _wsql: WorkflowSql
    _map_index: int | None = None
    _is_loop: bool = False
    _ledger_id: JobId | None = None
    #: The REGISTERED workflow's name (D1 — the signal-model catalog's
    #: key; the hold-context redact hook's resolution).
    _workflow_name: str = ""
    #: The workflow's redact hook (chain → hook — the hold context's
    #: persist-time redact; attack-3 H3's cure).
    _redact: Callable[[str], str] | None = None
    #: This node's claim view's worker (the emit's cursor checkpoint
    #: fences on it — T20). ``None`` = the context was built without a
    #: claim (a unit-test direct call) — the emit refuses loudly.
    _worker_id: JobId | None = None
    # THE EMISSION OP's buffer (T21): the attempt's own ProgressEmitter —
    # the runner wires it at the claim seam. None when unwired (direct
    # testing): ctx.progress then VALIDATES the shape (the typed door is
    # the authoring contract) and drops the emission (the deliberate
    # no-op — the jobs ctx's unwired pattern), never a crash.
    _progress: ProgressEmitter | None = None
    #: This attempt's claim epoch (the emit's cursor-fence's value — the
    #: runner's claim wrote it; the emit's checkpoint fences on the pair).
    #: The in-process driver's own claim stamps 0
    #: (NODE_CLAIM_SQL_TEMPLATE); a fleet-claimed row carries its
    #: dispatch claim's epoch — the emit's cursor checkpoint fences on
    #: the epoch beside the worker and the attempt (the fence must match
    #: the row, or the checkpoint updates nothing and the emit refuses).
    claim_epoch: int = 0
    # ── the runtime info (the context contract — the observability
    # primitive for body authors) ────────────────────────────────────
    flow_name: str | None = None
    queue: str | None = None
    claimed_at: datetime | None = None
    budget_remaining_ms: int | None = None
    _runtime: dict[str, int] = field(default_factory=lambda: dict[str, int]())  # pyright: ignore[reportUnknownVariableType]  # Why: pyright's inference for the slots+frozen dataclass's mutable default degrades; the annotation is the truth.

    @property
    def map_index(self) -> int | None:
        """The map item's index (the contract's public name; the private
        field is the runner's wiring)."""
        return self._map_index

    @property
    def hold_epoch(self) -> int | None:
        """The LAST CONSUMED hold's epoch (a resumed body's answer
        identity; None until a wait consumed an answer). The FROZEN
        ctx's mutable escape: the answer queue's consumption records it
        in the runtime dict (the identity never changes; the answer
        ledger does)."""
        return self._runtime.get("hold_epoch")

    async def cursor(self) -> dict[str, object]:
        """THE STREAMING SOURCE'S CHECKPOINTED CURSOR (T20): read from
        THIS source row's own metadata — the emit tx's checkpoint, under
        the ``emit_cursor`` key. ``{}`` before the first emit; the
        resume's body re-reads it to continue from the last COMMITTED
        page (never N-1, never N+1)."""
        from taskq.workflows._emit import EMIT_CURSOR_KEY

        async with self._pool.acquire() as conn:
            raw = await conn.fetchval(
                render_sql(SOURCE_CURSOR_SQL_TEMPLATE, self._wsql.schema),
                self.job_id,
                EMIT_CURSOR_KEY,
            )
        if raw is None:
            return {}
        decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
        assert isinstance(decoded, dict), "the cursor checkpoint is a jsonb object"
        return cast("dict[str, object]", decoded)

    async def emit_batch(
        self,
        children: Sequence[EmitChild],
        *,
        cursor: dict[str, object],
    ) -> tuple[JobId, ...]:
        """THE STREAMING SOURCE'S EMIT (T20): this page's chain starts +
        the edges + THIS node's cursor checkpoint, ONE transaction, while
        the source stays ``running`` (see ``taskq.workflows._emit`` — the
        fork-atomicity law at page granularity; a kill at any statement
        window rolls the whole page back). Each yield of the paged
        generator body is ONE ``ctx.emit_batch`` call."""
        from taskq.workflows._emit import emit_batch as _emit_batch

        if self._worker_id is None:
            raise WorkflowRunError(
                "ctx.emit_batch ran without this node's claim view — the "
                "emit's cursor checkpoint fences on the claim (worker, "
                "attempt, epoch), which this context does not carry"
            )
        return await _emit_batch(
            self._pool,
            self._wsql,
            flow_id=self.flow_id,
            source_id=self.job_id,
            worker_id=self._worker_id,
            attempt=self.attempt,
            claim_epoch=self.claim_epoch,  # the claim view's fence epoch
            children=children,
            cursor=cursor,
        )

    async def progress(
        self,
        pct: int | None = None,
        message: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """THE EMISSION OP (T21 decision a): report the node's progress —
        ``await ctx.progress(75, "page 3/4", {"page": 3})``.

        TYPED + BOUNDED: ``pct`` is an int 0..100 or None; ``message`` is
        chars-capped; ``data`` is jsonb, capped with the
        ``__truncated__`` marker (T18's D5 shape) and — when the node
        DECLARED a payload schema (``step(body, ..., progress_schema=
        Model)``, the TypedGate-door pattern) — validated against it, a
        wrong shape refused with :class:`taskq.workflows.ProgressRefusedError`.

        BEST-EFFORT BY CONSTRUCTION (decision e — THE LAW: observability
        degrades FIRST, never correctness): the call updates an in-memory
        buffer latest-wins and arms the cadence flush (~20 deltas/s at
        the 50 ms cadence); it NEVER awaits the network and NEVER
        touches the finalize path — the attempt's final flush runs
        bounded, BEFORE the node's finalize transactions. A flush that
        fails is counted on the record (the emitter's ``write_errors``,
        the first loss warned); a node whose every flush fails still
        terminalizes normally. A lost emission costs FRESHNESS; a
        blocked node costs CORRECTNESS.
        """
        emitter = self._progress
        schema = emitter.schema_decl if emitter is not None else None
        pct_v, message_v, data_v = validate_emission(pct, message, data, schema)
        if emitter is None:
            return  # unwired (direct testing): the deliberate no-op
        emitter.submit(pct_v, message_v, data_v)

    async def step(self, name: str, fn: Any, *args: Any, idempotent: bool = True) -> Any:
        """Run *fn* once per (flow, step key); replay returns the recorded
        result (the re-execution doctrine's cheap side: pre-wait side
        effects are ctx.step-ledgered and replay cheap)."""
        async with self._pool.acquire() as conn:
            steps = WorkflowSteps(
                conn,
                self._wsql,
                flow_id=self.flow_id,
                job_id=self.job_id,
                map_index=self._map_index,
                attempt=self.attempt,
            )
            return await steps.step(name, fn, *args, idempotent=idempotent)

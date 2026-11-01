"""The body's RUNTIME CONTEXT (the split of ``api/_runner`` — §7b's
concerns-separate law): :class:`StepContext` is the annotation every
workflow body declares — bodies read ``ctx.input`` and run side effects
through ``ctx.step`` (the ledger's replay contract), report progress
through ``ctx.progress``, and stream pages through ``ctx.emit_batch``.

The wait/deliver face (``wait_signal``/``signal`` — the HITL machinery)
lives in ``_ctx_wait`` and composes in; the DURABLE TIMER
(``wait_until`` — T28, the hold whose resolver is the clock) lives in
``_ctx_timer`` beside it; the runner builds the context at the claim
seam (``_runner``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

import asyncpg

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.workflows._childflow import CtxSpawnOps
from taskq.workflows._progress import ProgressEmitter, validate_emission
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild
from taskq.workflows.api._ctx_timer import CtxTimerOps
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
    max_in_flight: int | None = None,
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
        _max_in_flight=max_in_flight,
        claim_epoch=claim_epoch,
        flow_name=flow_name,
        queue=queue,
        claimed_at=claimed_at,
        budget_remaining_ms=budget_remaining_ms,
    )


@dataclass(frozen=True, slots=True)
class StepContext(CtxSpawnOps, CtxWaitOps, CtxTimerOps):
    """The body's runtime context: the run's identity + the input (cut
    #7) + the memoized step runner. Bodies read ``ctx.input`` and run
    side effects through ``ctx.step`` (the ledger's replay contract);
    spawn + join sub-flows through ``ctx.spawn_flow`` (T28 — the child
    flows' face, the join the HITL hold's own shape).

    The two WAIT faces compose in: the HUMAN hold (``ctx.wait_signal``
    — the HITL machinery) and the DURABLE TIMER (``ctx.wait_until`` —
    T28, the hold whose resolver is the clock).

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
    #: THE MAX-IN-FLIGHT BOUND (T20 / DH9): the workflow's DECLARED
    #: admission control, handed to every ``ctx.emit_batch`` (``None`` =
    #: the explicit unbounded).
    _max_in_flight: int | None = None
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
    # THE DEADLINE ENFORCEMENT'S CTX STATE (the consumer's
    # ``_enforce_start_to_close`` designated write — the SAME ctx face
    # the vanilla ``JobContext`` carries): the detached body task's
    # handle, written when the deadline enforcement runs the body in its
    # own task. The enforcement's OTHER designated write — the tx
    # rollback-collision window stamp — is the tx carrier's alone: the
    # wf body runs in NO transaction (each statement takes its own
    # acquisition), so there is no ``__aexit__`` ROLLBACK the marker
    # could collide with, no capture boundary, no reader here — the
    # enforcement's protocol split keeps that write off this face.
    _actor_body_task: asyncio.Task[object] | None = None

    def _set_actor_body_task(self, task: asyncio.Task[object]) -> None:
        """Record the deadline-enforced body task. Called by the
        start_to_close enforcement (the consumer's deadline helper, the
        same designated writer the vanilla ``JobContext`` documents) when
        the body runs in its own task, never by body code.

        The start_to_close machinery runs the body in a task so the
        deadline can win over whatever the body does after expiry (a
        cancellation-absorbing return, a hostile ``finally``); the task
        then unwinds ASYNCHRONOUSLY — the handle is the wf face's own
        record of that detached unwind. ``object.__setattr__`` because
        the dataclass is frozen; the handle is enforcement state that
        arrives after construction, exactly as the vanilla ctx's
        handles do.
        """
        object.__setattr__(self, "_actor_body_task", task)

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
        backpressure_timeout_s: float = 30.0,
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
            max_in_flight=self._max_in_flight,
            backpressure_timeout_s=backpressure_timeout_s,
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
        if emitter is None:
            # Unwired (direct testing): the deliberate no-op — but the
            # TYPED DOOR IS STILL THE AUTHORING CONTRACT (a wrong shape is
            # refused here too, never silently swallowed: the author's
            # test then sees the refusal their production run will get).
            validate_emission(pct, message, data, None)
            return
        # THE PUBLIC OP VALIDATES (the gate-hole cure): the emitter's own
        # validating door — the buffer's private write is never reached
        # around the gate.
        await emitter.emit(pct, message, data)

    async def substep(self, name: str, fn: Any, *args: Any, idempotent: bool = True) -> Any:
        """Run *fn* once per (flow, step key); replay returns the recorded
        result (the re-execution doctrine's cheap side: pre-wait side
        effects are substep-ledgered and replay cheap).

        THE CONNECTION IS NEVER YOURS, BODY (the LedgerWatch F4 cure):
        the claim and the terminal write each take their own
        acquisition; between them the body runs with NO pool connection
        held — a hot map fan-out's concurrent substeps are bounded by
        the workers, never by the execution pool's conn count (the
        pre-cure shape held ONE connection across *fn* and throttled a
        300-child fan-out to the dispatcher pool's 4).

        THE GLOSSARY LAW (the ergonomic tail's cure): this was
        ``ctx.step`` — the wiring verb ``step()`` already owns that name
        with DIFFERENT semantics (a graph NODE's declaration vs a node's
        INNER ledgered effect). The lesser noun renamed: the substep is
        the step-ledger's own vocabulary (``wf_step_ledger`` rows are
        what make it exactly-once)."""
        steps = WorkflowSteps(
            self._pool,
            self._wsql,
            flow_id=self.flow_id,
            job_id=self.job_id,
            map_index=self._map_index,
            attempt=self.attempt,
        )
        return await steps.step(name, fn, *args, idempotent=idempotent)

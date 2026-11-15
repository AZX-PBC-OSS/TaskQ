"""The body's RUNTIME CONTEXT (the split of ``api/_runner`` — §7b's
concerns-separate law): :class:`StepContext` is the annotation every
workflow body declares — bodies read ``ctx.input`` and run side effects
through ``ctx.step`` (the ledger's replay contract), report progress
through ``ctx.progress``, and stream pages through ``ctx.emit_batch``.

The wait/deliver face (``wait_signal``/``signal`` — the HITL machinery)
lives in ``_ctx_wait`` and composes in; the DURABLE TIMER
(``wait_until`` — the hold whose resolver is the clock) lives in
``_ctx_timer`` beside it; the runner builds the context at the claim
seam (``_runner``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import asyncpg

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.context import CancelOrigin
from taskq.workflows._cancellation import CancellationFace, WorkflowCancelDetail
from taskq.workflows._childflow import CtxSpawnOps
from taskq.workflows._progress import ProgressEmitter, validate_emission
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import EmitChild
from taskq.workflows.api._ctx_timer import CtxTimerOps
from taskq.workflows.api._ctx_wait import CtxWaitOps
from taskq.workflows.api._runner_errors import (
    TransactionalStepUnsupportedError,
    WorkflowRunError,
)
from taskq.workflows.api._sql_runner import SOURCE_CURSOR_SQL_TEMPLATE, render_sql
from taskq.workflows.context import WorkflowSteps

if TYPE_CHECKING:
    from pydantic import BaseModel

    from taskq.backend._protocol import ConnLike
    from taskq.workflows._childflow import ChildHandle
    from taskq.workflows.api._ctx_timer import TimerWake
    from taskq.workflows.api._ctx_wait import Expired

__all__ = [
    "StepContext",
    "TransactionalStepContext",
    "build_step_context",
    "build_transactional_step_context",
]


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
    physical_attempt: int | None = None,
    run_priority: int | None = None,
    cancel_face: CancellationFace | None = None,
    cancel_detail: WorkflowCancelDetail | None = None,
    connection: ConnLike | None = None,
) -> StepContext:
    """The one context construction (the claim seam's factory): the
    runner's step path and the loop driver's iteration path build the
    same field set through here — the two call sites never drift.

    ``connection`` is the claim-owned connection the transactional-step
    execution holds for this attempt (the subtype's non-optional body
    face); ordinary construction leaves it ``None``."""
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
        _claim_attempt=attempt if physical_attempt is None else physical_attempt,
        _run_priority=run_priority,
        _cancel_face=cancel_face,
        _cancel_detail=(cancel_detail if cancel_detail is not None else WorkflowCancelDetail()),
        _unwired_cancel_event=None if cancel_face is not None else asyncio.Event(),
        _connection=connection,
    )


@dataclass(frozen=True, slots=True)
class StepContext(CtxSpawnOps, CtxWaitOps, CtxTimerOps):
    """The body's runtime context: the run's identity + the input (cut
    #7) + the memoized step runner. Bodies read ``ctx.input`` and run
    side effects through ``ctx.step`` (the ledger's replay contract);
    spawn + join sub-flows through ``ctx.spawn_flow`` (the child
    flows' face, the join the HITL hold's own shape).

    The two WAIT faces compose in: the HUMAN hold (``ctx.wait_signal``
    — the HITL machinery) and the DURABLE TIMER (``ctx.wait_until`` —
    the hold whose resolver is the clock).

    A workflow body declares its context as
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
    #: The registered workflow's name (D1 — the signal-model catalog's
    #: key; the hold-context redact hook's resolution).
    _workflow_name: str = ""
    #: The workflow's redact hook (chain → hook — the hold context's
    #: persist-time redact; the crash-window fix).
    _redact: Callable[[str], str] | None = None
    #: This node's claim view's worker (the emit's cursor checkpoint
    #: fences on it ). ``None`` = the context was built without a
    #: claim (a unit-test direct call) — the emit refuses loudly.
    _worker_id: JobId | None = None
    #: the workflow's declared admission control, handed to every
    #: ``ctx.emit_batch`` (``None`` = the explicit unbounded).
    _max_in_flight: int | None = None
    # the emission op's buffer: the attempt's own ProgressEmitter — the runner
    # wires it at the claim seam. None when unwired (direct testing):
    # ctx.progress then validates the shape (the typed door is the authoring
    # contract) and drops the emission (the deliberate no-op — the jobs ctx's
    # unwired pattern), never a crash.
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
    #: The per-attempt wait-call counter (the wait site's deterministic call
    #: identity — ``_ctx_wait.wait_signal`` mints ``call:{attempt}:{seq}``
    #: from it): the body replays the same call order within an attempt, so
    #: the Nth wait call's id is stable across the resume's re-execution.
    #: The frozen dataclass's counter: ``object.__setattr__`` at the wait
    #: site, the same escape ``_set_actor_body_task`` keeps.
    _wait_call_seq: int = 0
    # the detached body task's handle, written when the deadline enforcement
    # runs the body in its own task. The enforcement's other designated write —
    # the tx rollback-collision window stamp — is the tx carrier's alone: the
    # wf body runs in no transaction (each statement takes its own
    # acquisition), so there is no ``__aexit__`` ROLLBACK the marker could
    # collide with, no capture boundary, no reader here — the enforcement's
    # protocol split keeps that write off this face.
    _actor_body_task: asyncio.Task[object] | None = None
    # the body's real finish time on the loop's monotonic clock, the
    # rescue's per-attempt stamp. The wf body has no executor thread, so
    # only the enforcement's body frame writes it here.
    _actor_body_finished_at: float | None = None
    # the body's own outcome, (value, exc). The sync dispatch's thread
    # body writes it on the vanilla face only; the enforcement's body
    # frame writes it on both faces, before its finish stamp.
    # Earliest-wins, and the rescue's cancelled arm reads it on either
    # face.
    _actor_body_outcome: tuple[object, BaseException | None] | None = None
    #: Tail-appended to avoid changing every existing positional field. Chain
    #: sources expose durable logical ``attempt`` publicly; this remains the
    #: physical jobs.attempt (including the park's 1 -> 0 -> 1 remint).
    _claim_attempt: int = 0
    #: Durable run-priority override for chain starts emitted by this attempt.
    _run_priority: int | None = None
    #: The worker's vanilla JobContext is the one carrier for every body kind.
    #: Direct/dev contexts deliberately have no worker observation wiring.
    _cancel_face: CancellationFace | None = None
    _cancel_detail: WorkflowCancelDetail = field(default_factory=WorkflowCancelDetail)
    _unwired_cancel_event: asyncio.Event | None = None
    #: The claim-owned connection the EXECUTION holds for this attempt —
    #: wired only for the transactional-step subtype (the runner builds the
    #: context with it; ordinary pool-owned contexts leave it None and the
    #: ordinary doors behave byte-identically). The body reads it through
    #: the ``connection`` property and joins the execution's own managed
    #: transaction with it.
    _connection: ConnLike | None = None
    #: The connection a joined ``emit_batch(connection=…)`` last used —
    #: recorded at the joined door so a plain ``ctx.cursor()`` cannot
    #: silently read the COMMITTED checkpoint through a different session
    #: while that caller transaction is still open (the stale-read
    #: refusal). ``object.__setattr__`` writes it (the frozen dataclass's
    #: escape, the same one the enforcement handles use).
    _joined_connection: ConnLike | None = None

    @property
    def connection(self) -> ConnLike | None:
        """The attempt's claim-owned connection (the transactional-step
        subtype's non-optional body face); ``None`` on ordinary pool-owned
        contexts — those bodies acquire from the pool like they always
        have."""
        return self._connection

    @property
    def cancellation_requested(self) -> bool:
        """Whether this attempt's existing vanilla carrier has been signalled."""
        face = self._cancel_face
        return False if face is None else face.cancel_event.is_set()

    @property
    def cancel_origin(self) -> CancelOrigin:
        """OPERATOR, SHUTDOWN, or NONE, using vanilla JobContext vocabulary."""
        face = self._cancel_face
        return CancelOrigin.NONE if face is None else face.cancel_origin

    @property
    def cancel_event(self) -> asyncio.Event:
        """The actual attempt event; direct/dev construction owns an unset one."""
        face = self._cancel_face
        if face is not None:
            return face.cancel_event
        assert self._unwired_cancel_event is not None
        return self._unwired_cancel_event

    @property
    def cancel_reason(self) -> str | None:
        """The cancelled root's bounded reason once durably observed."""
        return self._cancel_detail.reason

    @property
    def cancel_principal(self) -> str | None:
        """The same transaction's normalized admin-audit principal subject."""
        return self._cancel_detail.principal

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

    def _record_actor_body_finished_at(self, when: float) -> None:
        """Record the body's real finish time on the loop's monotonic
        clock, keeping the earliest stamp. The enforcement's body frame
        is the only writer here (the sync dispatch's thread-side stamp
        has no wf carrier), never body code. The rescue judges the body
        by when it finished, not by when the enforcement's continuation
        resumed. The enforcement resets it before arming.
        ``object.__setattr__`` because the dataclass is frozen.
        """
        if self._actor_body_finished_at is None or when < self._actor_body_finished_at:
            object.__setattr__(self, "_actor_body_finished_at", when)

    def _record_actor_body_outcome(self, value: object, exc: BaseException | None) -> None:
        """Record the body's own outcome, (value, exc). The enforcement's
        body frame is the only writer here (the sync thread's write is
        vanilla-only), never body code. Per attempt, reset with the stamp
        before arming, earliest-wins. ``object.__setattr__`` because the
        dataclass is frozen.
        """
        if self._actor_body_outcome is None:
            object.__setattr__(self, "_actor_body_outcome", (value, exc))

    def _reset_actor_body_attempt_record(self) -> None:
        """Clear the body's finish stamp and outcome record. Called by
        the start_to_close enforcement before arming a fresh deadline,
        never by body code. ``object.__setattr__`` because the dataclass
        is frozen.
        """
        object.__setattr__(self, "_actor_body_finished_at", None)
        object.__setattr__(self, "_actor_body_outcome", None)

    @property
    def map_index(self) -> int | None:
        """The map item's index (the contract's public name; the private
        field is the runner's wiring)."""
        return self._map_index

    @property
    def hold_epoch(self) -> int | None:
        """The last CONSUMED hold's epoch (a resumed body's answer
        identity; None until a wait consumed an answer). The FROZEN
        ctx's mutable escape: the answer queue's consumption records it
        in the runtime dict (the identity never changes; the answer
        ledger does)."""
        return self._runtime.get("hold_epoch")

    async def cursor(
        self,
        *,
        connection: ConnLike | None = None,
        schema: str | None = None,
    ) -> dict[str, object]:
        """Read from
        THIS source row's own metadata — the emit tx's checkpoint, under
        the ``emit_cursor`` key. ``{}`` before the first emit; the
        resume's body re-reads it to continue from the last COMMITTED
        page (never N-1, never N+1).

        ``connection=`` (+ ``schema=``) is the JOINED read: the checkpoint
        is read through THAT connection, so the caller's own uncommitted
        checkpoint is visible to it. A plain ``ctx.cursor()`` (the pool
        read) is REFUSED while this attempt's joined emit holds an open
        caller transaction — the committed-state read through a different
        session would silently return the pre-transaction cursor (the
        stale read behind the re-emit collision); pass the same
        connection for the same-transaction read."""
        from taskq.workflows._emit import EMIT_CURSOR_KEY

        if connection is None:
            joined = self._joined_connection
            if joined is not None:
                import asyncpg as _asyncpg

                try:
                    joined_open = joined.is_in_transaction()
                except _asyncpg.InterfaceError:
                    # The caller released its connection since the joined
                    # emit; there is no open caller transaction to be
                    # stale against, so the committed read is honest.
                    joined_open = False
                if joined_open:
                    raise WorkflowRunError(
                        "ctx.cursor() refused: this attempt's joined emit holds an "
                        "open caller transaction, so the pool read here would return "
                        "the last COMMITTED checkpoint through a different session — "
                        "a stale cursor. Read through the same connection that "
                        "emitted (ctx.cursor(connection=…, schema=…)) to see the "
                        "uncommitted checkpoint, or read after your transaction "
                        "commits."
                    )
            async with self._pool.acquire() as conn:
                raw = await conn.fetchval(
                    render_sql(SOURCE_CURSOR_SQL_TEMPLATE, self._wsql.schema),
                    self.job_id,
                    EMIT_CURSOR_KEY,
                )
        else:
            from taskq.workflows._emit import validate_joined_connection

            await validate_joined_connection(
                connection,
                self._wsql,
                connection_schema=schema,
                source_id=self.job_id,
            )
            raw = await connection.fetchval(
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
        connection: ConnLike | None = None,
        schema: str | None = None,
    ) -> tuple[JobId, ...]:
        """This page's chain starts +
        the edges + THIS node's cursor checkpoint, one transaction, while
        the source stays ``running`` (see ``taskq.workflows._emit`` — the
        fork-atomicity law at page granularity; a kill at any statement
        window rolls the whole page back). Each yield of the paged
        generator body is one ``ctx.emit_batch`` call. Saturation raises typed
        capacity control and parks the source; ``backpressure_timeout_s`` is
        diagnostic-age compatibility input, never a retry/failure deadline.

        ``connection=`` (+ ``schema=``) is the JOINED door: the page's
        writes go to THAT actual asyncpg connection inside the caller's
        already-open transaction — no pool acquisition, no second database
        session, no engine-owned commit. Every refusal (admission, stale
        claim, cancellation, divergence, poison) propagates out of the
        caller's ``async with conn.transaction()`` so the caller's WHOLE
        unit — application writes included — rolls back physically before
        any source park. The engine never commits or rolls back the
        caller's transaction; if the caller catches a refusal and commits
        anyway, that is outside the supported contract and earlier writes
        of the unit can then commit (documented, deliberately not
        synthetic-poisoned). Same-connection ``ctx.cursor(connection=…)``
        reads see this door's own uncommitted checkpoints."""
        from taskq.workflows._emit import emit_batch as _emit_batch

        if self._worker_id is None:
            raise WorkflowRunError(
                "ctx.emit_batch ran without this node's claim view — the "
                "emit's cursor checkpoint fences on the claim (worker, "
                "attempt, epoch), which this context does not carry"
            )
        if connection is not None:
            # Record the joined session before any statement: a plain
            # ctx.cursor() while THIS transaction is still open is the
            # stale read the refusal exists for.
            object.__setattr__(self, "_joined_connection", connection)
        return await _emit_batch(
            self._pool,
            self._wsql,
            flow_id=self.flow_id,
            source_id=self.job_id,
            worker_id=self._worker_id,
            attempt=self._claim_attempt,
            claim_epoch=self.claim_epoch,  # the claim view's fence epoch
            children=children,
            cursor=cursor,
            max_in_flight=self._max_in_flight,
            backpressure_timeout_s=backpressure_timeout_s,
            run_priority=self._run_priority,
            connection=connection,
            connection_schema=schema,
        )

    async def progress(
        self,
        pct: int | None = None,
        message: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """Report the node's progress —
        ``await ctx.progress(75, "page 3/4", {"page": 3})``.

        TYPED + BOUNDED: ``pct`` is an int 0..100 or None; ``message`` is
        chars-capped; ``data`` is jsonb, capped with the
        ``__truncated__`` marker (the truncation shape) and — when the node
        DECLARED a payload schema (``step(body, ..., progress_schema=
        Model)``, the TypedGate-door pattern) — validated against it, a
        wrong shape refused with :class:`taskq.workflows.ProgressRefusedError`.

        BEST-EFFORT BY CONSTRUCTION (decision e — the LAW: observability
        degrades first, never correctness): the call updates an in-memory
        buffer latest-wins and arms the cadence flush (~20 deltas/s at
        the 50 ms cadence); it never awaits the network and never
        touches the finalize path — the attempt's final flush runs
        bounded, before the node's finalize transactions. A flush that
        fails is counted on the record (the emitter's ``write_errors``,
        the first loss warned); a node whose every flush fails still
        terminalizes normally. A lost emission costs FRESHNESS; a
        blocked node costs CORRECTNESS.
        """
        emitter = self._progress
        if emitter is None:
            # Unwired (direct testing): the deliberate no-op — but the
            # typed door is still the authoring contract (a wrong shape is
            # refused here too, never silently swallowed: the author's
            # test then sees the refusal their production run will get).
            validate_emission(pct, message, data, None)
            return
        # the emitter's own validating door — the buffer's private write is
        # never reached around the gate.
        await emitter.emit(pct, message, data)

    async def substep(self, name: str, fn: Any, *args: Any, idempotent: bool = True) -> Any:
        """Run *fn* once per (flow, step key); replay returns the recorded
        result (the re-execution doctrine's cheap side: pre-wait side
        effects are substep-ledgered and replay cheap).

        The connection is never yours, body: the claim and the terminal
        write each take their own
        acquisition; between them the body runs with no pool connection
        held — a hot map fan-out's concurrent substeps are bounded by
        the workers, never by the execution pool's conn count (holding
        one connection across *fn* would throttle a 300-child fan-out
        to the dispatcher pool's 4).

        The name is ``substep``, not ``step``: the wiring verb
        ``step()`` already owns that name
        with DIFFERENT semantics (a graph NODE's declaration vs a node's
        INNER ledgered effect). The substep is
        the step-ledger's own vocabulary (``wf_step_ledger`` rows are
        what make it exactly-once)."""
        steps = WorkflowSteps(
            self._pool,
            self._wsql,
            flow_id=self.flow_id,
            job_id=self.job_id,
            map_index=self._map_index,
            attempt=self.attempt,
            claim_epoch=self.claim_epoch,
        )
        return await steps.step(name, fn, *args, idempotent=idempotent)


@dataclass(frozen=True, slots=True)
class TransactionalStepContext(StepContext):
    """The TRANSACTIONAL step's runtime context — the public subtype the
    ``transactional_step`` body declares (``ctx: TransactionalStepContext``).
    The execution built it at the claim seam with THIS attempt's actual
    claim-owned connection, and the managed unit's one transaction is open
    on it: every supported helper joins that exact session — the same
    physical connection, the same uncommitted state, one commit owned by
    the engine's settlement.

    The contract ``transactional_step`` docs, made type-true:

    * :attr:`connection` is the claim-owned session, NEVER ``None`` — a
      body may dereference it without narrowing (the positive type probe
      pins that face). Ordinary :class:`StepContext` bodies keep the
      optional property and the pool door.
    * :meth:`cursor` / :meth:`emit_batch` / :meth:`substep` join the held
      connection by default — they read and write THIS unit's uncommitted
      state and commit only when the settlement commits. An explicitly
      supplied DIFFERENT session is refused before any data statement.
    * :meth:`progress` buffers only: no pool acquisition and no DB
      operation while the managed transaction is open; the attempt's one
      ordered close after the connection's release flushes it (best-effort,
      never part of the commit).
    * the wait/spawn helpers (:meth:`ctx.spawn_flow` / ``wait_signal`` /
      ``wait_until``) refuse at the helper's entry — before any pool
      acquisition, data statement, signal write, or nested callback — with
      :class:`TransactionalStepUnsupportedError` naming the remedy.

    The deadline-enforcement carrier: the rollback-collision window stamp
    (the enforcement's own designated write, read by the transactional
    capture boundary) lives here — on the subtype that actually runs
    inside a transaction, never on the ordinary context."""

    #: The transactional-only deadline machinery: the ROLLBACK-COLLISION
    #: window's stamp. Set by :func:`_enforce_start_to_close` when the
    #: unwind budget expired (the marker is about to reach the managed
    #: ``__aexit__``'s ROLLBACK while a hostile unwind may still hold the
    #: connection); taken (one-shot) by the capture boundary that
    #: translates ONLY that rollback's own collision back to the truthful
    #: deadline. Never set or read by body code.
    _tx_rollback_collision_window: bool = False

    @property
    def connection(self) -> ConnLike:
        """The attempt's claim-owned connection — the managed unit's own
        session, never ``None``.

        FAIL-CLOSED: a context of this subtype manually constructed with
        no connection (bypassing the factory) raises rather than handing
        the body a ``None`` to trip over mid-transaction — the
        non-optional contract is runtime-true, not only checker-true."""
        conn: ConnLike | None = getattr(self, "_connection", None)
        if conn is None:
            raise WorkflowRunError(
                "this TransactionalStepContext was constructed without its "
                "claim-owned connection — the transactional execution binds "
                "the actual held session at the claim seam (build via the "
                "runner's transactional factory; a bare dataclass call is "
                "not a supported construction)"
            )
        return conn

    def _set_tx_rollback_collision_window(self) -> None:
        """Open the ROLLBACK-COLLISION window (the deadline enforcement's
        designated write, reached only on the unwind-budget path).
        ``object.__setattr__`` because the dataclass is frozen — the same
        escape the enforcement handles use."""
        object.__setattr__(self, "_tx_rollback_collision_window", True)

    def _take_tx_rollback_collision_window(self) -> bool:
        """Read and consume the window one-shot: ``True`` exactly when THIS
        attempt's deadline enforcement had stamped it. Reading is taking —
        a window justifies at most one re-label and no stale stamp
        survives the attempt."""
        taken = self._tx_rollback_collision_window
        if taken:
            object.__setattr__(self, "_tx_rollback_collision_window", False)
        return taken

    async def cursor(
        self,
        *,
        connection: ConnLike | None = None,
        schema: str | None = None,
    ) -> dict[str, object]:
        """Read THIS unit's own checkpoint — the held connection is the
        default (and the only) session: the read sees the body's own
        uncommitted cursor state through the same transaction the emit
        wrote it in. An explicitly supplied DIFFERENT session is refused
        before any statement (a second session cannot see this unit's
        uncommitted checkpoint; the pool read would silently return the
        last COMMITTED one)."""
        if connection is not None and connection is not self._connection:
            raise TransactionalStepUnsupportedError(
                "ctx.cursor(connection=…) refused: a transactional step's "
                "cursor read joins the held claim connection — an explicitly "
                "supplied different session cannot see this unit's "
                "uncommitted checkpoint and is refused before any statement"
            )
        return await super().cursor(connection=self.connection, schema=schema)

    async def emit_batch(
        self,
        children: Sequence[EmitChild],
        *,
        cursor: dict[str, object],
        backpressure_timeout_s: float = 30.0,
        connection: ConnLike | None = None,
        schema: str | None = None,
    ) -> tuple[JobId, ...]:
        """Emit one page JOINED to the settlement unit — the held claim
        connection is the default (and the only) session: the children,
        the edges and the cursor checkpoint join the body's application
        writes and the fenced terminal in the ONE managed commit.
        ``connection=ctx.connection`` is accepted as the equivalent
        explicit spelling; a different session is refused before any data
        statement. The same ``_emit_tx`` kernel and the same
        admission/fence/poison/TTL/capacity semantics run — only the
        session's ownership differs from the ordinary pool-owned door."""
        if connection is not None and connection is not self._connection:
            raise TransactionalStepUnsupportedError(
                "ctx.emit_batch(connection=…) refused: a transactional "
                "step's page joins the held claim connection — a different "
                "session would break the settlement's one-commit atomicity "
                "and is refused before any statement"
            )
        return await super().emit_batch(
            children,
            cursor=cursor,
            backpressure_timeout_s=backpressure_timeout_s,
            connection=self.connection,
            schema=schema,
        )

    async def substep(self, name: str, fn: Any, *args: Any, idempotent: bool = True) -> Any:
        """Run *fn* once per (flow, step key) JOINED to the settlement
        unit: the memo read, the exact ledger claim, the callback and the
        exact ledger terminal all run on the held claim connection — no
        nested transaction, no second acquisition, no release of the
        caller's session (the enclosing unit's commit owns them all). The
        callback may use ``ctx.connection`` like the body does. A terminal
        failure escapes into the enclosing unit, which decides the
        rollback. Ordinary :meth:`StepContext.substep` keeps its released-
        acquisition shape unchanged."""
        steps = WorkflowSteps(
            self._pool,
            self._wsql,
            flow_id=self.flow_id,
            job_id=self.job_id,
            map_index=self._map_index,
            attempt=self.attempt,
            claim_epoch=self.claim_epoch,
            connection=self.connection,
        )
        return await steps.step(name, fn, *args, idempotent=idempotent)

    async def progress(
        self,
        pct: int | None = None,
        message: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """Buffer one progress emission — NOTHING else: no pool
        acquisition and no DB operation while the managed transaction is
        open (the transactional face of the best-effort law). The same
        typed gate validates the shape (a wrong shape is still
        :class:`ProgressRefusedError`); the attempt's one ordered close
        after the connection's release flushes the buffer best-effort and
        never participates in the commit."""
        emitter = self._progress
        if emitter is None:
            validate_emission(pct, message, data, None)
            return
        await emitter.emit_buffered(pct, message, data)

    async def spawn_flow[G: BaseModel](
        self,
        name: str,
        *,
        input: object = None,
        result: type[G],
        key: str | None = None,
        max_depth: int | None = None,
    ) -> ChildHandle[G]:
        """Refused at the helper's entry — before any pool acquisition,
        data statement, signal write, or nested callback. A child flow
        spawn is an independent durable run whose rows commit in their own
        transactions; it cannot join this unit's one commit, and waiting
        for it inside a managed transaction would hold the claim
        connection across an unbounded external wait. Emit ordinary child
        work with :meth:`emit_batch` instead."""
        raise TransactionalStepUnsupportedError(
            "ctx.spawn_flow is unsupported inside a transactional step: a "
            "child flow commits its own rows and would hold this unit's "
            "claim connection across an unbounded wait. Emit ordinary child "
            "work with ctx.emit_batch(...) — it joins the settlement's one "
            "commit — or wire the child flow outside the transactional node."
        )

    async def wait_signal[M: BaseModel](
        self,
        signals: type[M] | tuple[type[M], ...],
        *,
        timeout_s: timedelta | float | None = None,
        reason: str | None = None,
        tool: str | None = None,
        args: dict[str, object] | None = None,
        discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
    ) -> M | Expired:
        """Refused at the helper's entry — before any pool acquisition,
        data statement, signal write, or nested callback. A human hold
        releases the node's claim and parks the row: the exact lifecycle
        the managed unit's one open transaction cannot survive (the claim
        connection would be held across the hold, and the unit's
        uncommitted state could never commit under a released claim).
        Wait OUTSIDE the settlement: hold on an ordinary step, then wire
        the settlement as its successor."""
        raise TransactionalStepUnsupportedError(
            "ctx.wait_signal is unsupported inside a transactional step: "
            "a human hold releases the node's claim and parks the row while "
            "this unit's transaction is still open. Hold on an ordinary "
            "step and wire the settlement as its successor."
        )

    async def wait_until(
        self,
        when: datetime | float,
        *,
        key: str | None = None,
    ) -> TimerWake:
        """Refused at the helper's entry — before any pool acquisition,
        data statement, signal write, or nested callback. A durable timer
        hold releases the node's claim and parks the row: the exact
        lifecycle the managed unit's one open transaction cannot survive.
        Time the work on an ordinary step, then wire the settlement as
        its successor."""
        raise TransactionalStepUnsupportedError(
            "ctx.wait_until is unsupported inside a transactional step: a "
            "durable timer hold releases the node's claim and parks the row "
            "while this unit's transaction is still open. Hold on an "
            "ordinary step and wire the settlement as its successor."
        )


def build_transactional_step_context(
    *,
    flow_id: JobId,
    job_id: JobId,
    node_key: str,
    attempt: int,
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    input: object,
    map_index: int | None = None,
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
    run_priority: int | None = None,
    cancel_face: CancellationFace | None = None,
    cancel_detail: WorkflowCancelDetail | None = None,
    connection: ConnLike,
) -> TransactionalStepContext:
    """The TRANSACTIONAL claim seam's factory (the one construction of
    :class:`TransactionalStepContext`): identical field wiring to
    :func:`build_step_context`, but the claim-owned ``connection`` is
    REQUIRED — the subtype's non-optional body face is bound here, from
    the actual held session, never via an optional-then-narrow hope. The
    typed ``ConnLike`` parameter refuses a ``None`` statically, and the
    :attr:`TransactionalStepContext.connection` property fails closed at
    runtime for any context that reached construction without one (the
    manual-construction face — the checker cannot see every caller)."""
    return TransactionalStepContext(
        flow_id=flow_id,
        job_id=job_id,
        node_key=node_key,
        attempt=attempt,
        input=input,
        _pool=pool,
        _wsql=wsql,
        _map_index=map_index,
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
        _run_priority=run_priority,
        _cancel_face=cancel_face,
        _cancel_detail=(cancel_detail if cancel_detail is not None else WorkflowCancelDetail()),
        _unwired_cancel_event=None if cancel_face is not None else asyncio.Event(),
        _connection=connection,
    )

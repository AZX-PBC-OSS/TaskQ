"""The TRANSACTIONAL STEP's execution (the split of ``api/_runner`` —
§7b's concerns-separate law): the all-emitted settlement body runs on the
claim-owned connection inside ONE engine-managed transaction — the
application writes, the fenced node terminal and the claim-instant ledger
record commit as one unit; the successor decrement (tx2) runs in a fresh
transaction on the SAME still-held connection after that commit.

The ordering law this module implements (the accepted #721 architecture):

* the parents' results are resolved and the dependency product pinned
  before the managed unit's final critical section — no application
  call, no pool acquisition, no progress flush runs between the terminal
  CAS and the COMMIT;
* the critical section locks the run root (live status) then the node —
  the same root→node order operator cancel uses, so the two
  serializations are defined: a cancel committed first makes this unit
  refuse and roll back (the cooperative-cancel terminal owns the node),
  and a unit that wins the root lock commits before a later cancel
  proceeds;
* the body's deadline uses the consumer's ONE shared enforcement with
  the SAME transactional unwind budget the vanilla transactional
  consumer arms: the deadline's expiry cancels the body task exactly
  once, bound-waits its cooperative unwind on the shared connection, and
  only past that budget stamps the rollback-collision window the
  capture boundary translates — an ``InterfaceError`` arriving under the
  one-shot stamp is the managed ROLLBACK colliding with the hostile
  unwind, never a body class;
* a body that is STILL unwinding past the budget never gets the shared
  session handed back to the pool: the connection is terminated
  out-of-pool (PostgreSQL disconnect supplies the rollback), the task
  stays tracked with its outcome retrieved, and the dependency pin
  rides the task's done callback — the pool's other tasks never receive
  that session;
* the connection's release/reset (or the trap) completes BEFORE the
  progress emitter's exactly-once close and before the runner's
  dependency-pin release (the caller's ``finally``), so a pool of one
  cannot deadlock;
* every refusal (a claim-fence no-match, a terminal root) and every
  ambiguous outcome (a backend death, a lost commit acknowledgement)
  converges through the durable rows: the fence no-match records the
  attempt 'fenced' OUTSIDE the rolled-back unit, and a recovery re-reads
  the settled state instead of reapplying any application effect.

The escape-point classifier is PHASE-EXPLICIT (never a ``body_done``
catch-all): the transaction-enter failure, the body's own failure, the
deadline, the deterministic outcome refusals (an ``Exit`` return, an
unencodable result), the terminal-write/COMMIT machinery failures and
the post-commit tx2 failure are distinguished by WHERE they escaped, and
each routes to its own owner — the body's exceptions to the disposition
vocabulary (the ladder/deferral/reclaim/deadline seats), the machinery's
escapes to the reclaim/recovery boundary, the deterministic refusals to
the one-rollback one-terminal authoring owner.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._emit import EmitBackpressure
from taskq.workflows._progress import KIND_NODE_TERMINAL
from taskq.workflows.api._runner_codec import encode_result
from taskq.workflows.api._runner_ladder import is_infra_fault
from taskq.workflows.engine import (
    _run_tx1_statements,  # pyright: ignore[reportPrivateUsage]  # Why: the terminal kernel's same-connection claim-owned use — the pool-owning wrapper stays the engine's; no second implementation exists.
    _run_tx2_statements,  # pyright: ignore[reportPrivateUsage]  # Why: the same kernel the pool finalize's tx2 wraps — this post-commit pass owns the transaction on the still-held connection instead.
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import asyncpg

    from taskq.backend._protocol import ConnLike
    from taskq.exceptions import RetryAfter, Snooze
    from taskq.workflows._cancellation import CancellationFace, WorkflowCancelDetail
    from taskq.workflows._progress import ProgressEmitter
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._app import CompiledWorkflow
    from taskq.workflows.api._ctx import StepContext, TransactionalStepContext
    from taskq.workflows.api._graph import NodeDecl

__all__ = ["BodyTaskRef", "TransactionalOps"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


@dataclass(slots=True)
class BodyTaskRef:
    """The one-attempt holder for the deadline-enforced body task (the
    enforcement writes the ctx handle; the runner records it here for the
    caller's release seat). The dependency-pin handoff reads ``task`` in
    the door's own ``finally``: ``None`` (no deadline, the body ran
    inline) or a done task releases synchronously; a still-live task
    transfers the lease to that task's done callback — never released
    while the body can still touch its product."""

    task: asyncio.Task[object] | None = None


class _SettlementRefusedError(Exception):
    """The settlement's final critical section refused the unit.

    Raised INSIDE the managed transaction so the whole unit — the
    application prefix included — rolls back physically. ``claim_fence``
    marks the claim-fence no-match (this attempt no longer owns the row;
    the fenced ledger record is written outside the rolled-back unit);
    the terminal-root refusal routes through the ladder's
    cancellation/deferral machinery like a body failure."""

    def __init__(self, message: str, *, claim_fence: bool) -> None:
        super().__init__(message)
        self.claim_fence = claim_fence


class _TransactionalHost:
    """The runner surface the transactional tail reads (the split's typed
    seam — the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    compiled: CompiledWorkflow
    _worker_id: JobId
    _wake_backpressure: bool

    async def _parent_results(
        self, node_id: JobId
    ) -> tuple[list[tuple[str, object]], dict[str, object]]: ...

    def _resolve_args(
        self,
        node: NodeDecl | None,
        body: Callable[..., object],
        parent_results: dict[str, object],
        payload: dict[str, object],
        *,
        node_key: str | None = None,
    ) -> tuple[object, ...]: ...

    async def _deps_tail(
        self, body: Callable[..., object], wired_count: int
    ) -> tuple[object, ...]: ...

    def _body_deadline_seconds(self, row: dict[str, Any]) -> float | None: ...

    def _body_queue(self, node: NodeDecl | None, row: dict[str, Any]) -> str | None: ...

    async def _flow_input(self, flow_id: JobId) -> object: ...

    def _redact_hook(self) -> Callable[[str], str] | None: ...

    async def _reclaim_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        exc: Exception,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: datetime | None = None,
    ) -> None: ...

    async def _defer_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        exc: EmitBackpressure | Snooze | RetryAfter,
        *,
        claim_epoch: int = 0,
        ledger_claimed_at: datetime | None = None,
    ) -> str: ...

    async def _deadline_disposition(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: NodeDecl | None,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: datetime | None = None,
    ) -> str: ...

    async def _ladder_or_fail(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: NodeDecl | None,
        exc: Exception,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: datetime | None = None,
    ) -> str: ...

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None: ...


class TransactionalOps(_TransactionalHost):
    """The runner's transactional-settlement mixin (the machinery's home;
    the runner composes it)."""

    __slots__ = ()

    async def _execute_transactional_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        node: NodeDecl | None,
        body: Callable[..., Awaitable[object]],
        emitter: ProgressEmitter,
        *,
        claim_epoch: int = 0,
        run_priority: int | None = None,
        ledger_claimed_at: datetime | None = None,
        cancel_face: CancellationFace | None = None,
        cancel_detail: WorkflowCancelDetail | None = None,
        body_task_ref: BodyTaskRef | None = None,
    ) -> str:
        """The settlement attempt's full lifecycle on ONE claim-owned
        connection: the resolution (every pool acquisition released — a
        pool of one must be free when the claim connection is taken) →
        the managed unit (body → final critical section → COMMIT) → the
        post-commit tx2 on the same connection → the connection's
        release (or the hostile unwind's out-of-pool trap) → the
        emitter's exactly-once close → the failure routes (which need
        the pool) last."""
        from taskq.connections import (
            _POOL_RELEASE_RESET_TIMEOUT_SECS,  # pyright: ignore[reportPrivateUsage]  # Why: the pool's own release/reset budget — the one constant every release seat uses; a local copy would drift from it.
        )
        from taskq.worker._consumer import (
            _TX_UNWIND_WAIT_BUDGET,  # pyright: ignore[reportPrivateUsage]  # Why: the consumer module owns the one derived unwind budget; re-declaring it here would drift.
            _enforce_start_to_close,  # pyright: ignore[reportPrivateUsage]  # Why: the deadline enforcement is the consumer module's shared machinery by design (the wf face imports the same function — the ordinary tail's precedent).
        )
        from taskq.workflows.api._ctx import build_transactional_step_context

        resolution_failure: Exception | None = None
        input_value: object = None
        args: tuple[object, ...] = ()
        deps_tail: tuple[object, ...] = ()
        try:
            input_value = await self._flow_input(flow_id)
            _parents_ordered, parents_by_key = await self._parent_results(row["id"])
            payload_raw = row["payload"]
            payload = _json_loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            assert isinstance(payload, dict)  # the Any-contract walk (the seed wrote the shape)
            payload_doc: dict[str, object] = payload  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk's boundary — the seed wrote the shape; the assert is the runtime check.
            args = self._resolve_args(
                node, body, parents_by_key, payload_doc, node_key=row["step_key"]
            )
            deps_tail = await self._deps_tail(body, len(args))
        except Exception as exc:  # Why: the resolution boundary — the same ladder-routed escape the ordinary tail's resolution failures take; nothing is held yet.
            resolution_failure = exc

        # The typed closure: the enforcement's callback face is opaque
        # (its ctx parameter is an object), but THIS closure captures the
        # already-typed transactional context below — no re-type, no
        # free cast between the machinery and the body.
        ctx: TransactionalStepContext | None = None

        async def _run_settlement_body(_job: object, _run_ctx: object) -> object:
            assert (
                ctx is not None
            )  # the closure is only awaited inside the unit, after the factory below
            return await body(ctx, *args, *deps_tail)

        body_failure: Exception | None = resolution_failure
        machinery_fault: Exception | None = None
        outcome: str | None = None
        conn: ConnLike | None = None
        if resolution_failure is None:
            conn = await self.pool.acquire()
            try:
                try:
                    ctx = build_transactional_step_context(
                        flow_id=flow_id,
                        job_id=JobId(row["id"]),
                        node_key=row["step_key"],
                        attempt=attempt,
                        pool=self.pool,
                        wsql=self.wsql,
                        input=input_value,
                        map_index=row["map_index"],
                        ledger_id=ledger_id,
                        worker_id=self._worker_id,
                        workflow_name=self.compiled.name,
                        redact=self._redact_hook(),
                        progress=emitter,
                        claim_epoch=claim_epoch,
                        max_in_flight=self.compiled.max_in_flight,
                        flow_name=self.compiled.name,
                        queue=self._body_queue(node, row),
                        claimed_at=datetime.now(UTC),
                        run_priority=run_priority,
                        cancel_face=cancel_face,
                        cancel_detail=cancel_detail,
                        connection=conn,
                    )
                    (
                        unit_outcome,
                        unit_body_failure,
                        unit_machinery,
                    ) = await self._transactional_unit(
                        ctx,
                        conn,
                        flow_id,
                        row,
                        attempt,
                        ledger_id,
                        run_body=_run_settlement_body,
                        enforce=_enforce_start_to_close,
                        unwind_budget=_TX_UNWIND_WAIT_BUDGET,
                        claim_epoch=claim_epoch,
                        ledger_claimed_at=ledger_claimed_at,
                    )
                    outcome = unit_outcome
                    body_failure = unit_body_failure
                    machinery_fault = unit_machinery
                except Exception as exc:  # Why: the ctx construction's own statements — nothing has run yet, so the escape routes through the dispositions like a resolution failure.
                    if body_failure is None and machinery_fault is None:
                        body_failure = exc
            finally:
                # The ordered teardown (§3.2 steps 9-10): the body task's
                # quiescence decides release-vs-trap ONCE; the emitter's
                # exactly-once close is nested so a failed release cannot
                # skip it; the disposition's fresh acquisitions only run
                # after both.
                assert conn is not None
                body_task = None if ctx is None else ctx._actor_body_task  # pyright: ignore[reportPrivateUsage]  # Why: the enforcement's designated body-task handle — the transactional tail is the handle's designated reader (the same contract the vanilla consumer's exit-proof hold reads).
                if body_task_ref is not None and body_task is not None:
                    # The door's release seat reads this after the
                    # dispositional tail — the live-task handoff's source.
                    body_task_ref.task = body_task
                if body_task is not None and not body_task.done():
                    # The HOSTILE unwind (§3.3): the body outlived the
                    # unwind budget still holding the shared session. The
                    # connection is NEVER released to the general pool
                    # (another task could receive a session with an open
                    # transaction): terminate() out-of-pool — the driver's
                    # holder cleanup detaches it and PostgreSQL's
                    # disconnect supplies the rollback. The task stays
                    # detached/tracked (the enforcement's registry) with
                    # its outcome retrieved by its own callback; the
                    # dependency pin rides the done callback (§3.4).
                    conn.terminate()
                    logger.warning(
                        "connection_terminated_for_unwind",
                        kind="connection_terminated_for_unwind",
                        run_id=str(flow_id),
                        node=row["step_key"],
                        attempt=attempt,
                        deadline_budget_s=_TX_UNWIND_WAIT_BUDGET,
                    )
                else:
                    try:
                        await self.pool.release(conn, timeout=_POOL_RELEASE_RESET_TIMEOUT_SECS)
                    except Exception as release_exc:  # Why: the reset failure is a resource fact, never the attempt's outcome — the driver terminated the session (it is not reusable), and the disposition below runs on a fresh acquisition.
                        logger.warning(
                            "claim_connection_release_failed",
                            kind="claim_connection_release_failed",
                            run_id=str(flow_id),
                            node=row["step_key"],
                            attempt=attempt,
                            error=str(release_exc)[:200],
                        )
                await emitter.aclose()
        else:
            await emitter.aclose()
        if machinery_fault is not None:
            if is_infra_fault(machinery_fault):
                # The reclaim is a no-op against an already-terminal row:
                # the durable settlement is the record, and the recovery
                # re-reads it instead of reapplying any application effect.
                await self._reclaim_step(
                    flow_id,
                    row,
                    attempt,
                    machinery_fault,
                    claim_epoch=claim_epoch,
                    ledger_id=ledger_id,
                    ledger_claimed_at=ledger_claimed_at,
                )
            raise machinery_fault
        if body_failure is not None:
            if ctx is None:
                # The context never existed (the run input read or the
                # build failed before any body ran): the machinery-level
                # escape, the ordinary tail's own raw propagation shape.
                raise body_failure
            return await self._transactional_disposition(
                ctx,
                flow_id,
                row,
                attempt,
                ledger_id,
                node,
                body_failure,
                claim_epoch=claim_epoch,
                ledger_claimed_at=ledger_claimed_at,
            )
        assert outcome is not None
        if outcome == "succeeded":
            # The ordinary KIND_NODE_TERMINAL auto-projection, once, after
            # the release (a pool of one stays free for its fresh
            # acquisition) and only on the genuinely committed success —
            # additive best-effort ``wf_node_stream`` publication, never a
            # second authority over row state and never another
            # ``job_events`` write.
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {"outcome": "succeeded", "step_key": row["step_key"]},
            )
            if self._wake_backpressure:
                from taskq.workflows._emit import wake_emit_backpressure_safely

                # The same common completion seat the ordinary tail uses —
                # its fresh pool acquisition is safe here: the claim
                # connection is already released.
                await wake_emit_backpressure_safely(self.pool, self.wsql, flow_id=flow_id)
        return outcome

    async def _transactional_unit(
        self,
        ctx: TransactionalStepContext,
        conn: ConnLike,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        *,
        run_body: Callable[[object, object], Awaitable[object]],
        enforce: Callable[..., Awaitable[object]],
        unwind_budget: float,
        claim_epoch: int = 0,
        ledger_claimed_at: datetime | None = None,
    ) -> tuple[str, Exception | None, Exception | None]:
        """The managed unit and the post-commit tx2 on the held connection
        (the resolution ran in the caller, before this connection was
        taken). Returns ``(verdict, body_failure, machinery_fault)`` —
        exactly one of the latter two is set when the verdict is not a
        clean return; the caller owns the routes.

        The PHASE-EXPLICIT classifier (§3.5): the transaction-enter
        failure, the body's own exception, the deadline marker, the
        deterministic outcome refusals and the machinery statements'
        escapes are distinguished by WHERE they escaped — never by a
        ``body_done`` catch-all."""
        import asyncpg

        from taskq.worker._handlers import _StartToCloseExceededError
        from taskq.workflows.api._graph import Exit
        from taskq.workflows.api._runner_errors import TransactionalStepUnsupportedError

        unit_entered = False
        body_done = False
        try:
            try:
                async with conn.transaction():
                    unit_entered = True
                    # The body IS the unit's prefix: its application writes,
                    # staged publication and the settlement guard below commit
                    # together or not at all. The deadline shares the
                    # consumer's ONE enforcement with the unwind budget — the
                    # expiry cancels the body task, bound-waits its
                    # cooperative unwind on this shared connection, and only
                    # past the budget stamps the rollback-collision window.
                    outcome_value = await enforce(
                        run_body,
                        None,
                        ctx,
                        self._body_deadline_seconds(row),
                        unwind_wait=unwind_budget,
                    )
                    body_done = True
                    if isinstance(outcome_value, Exit):
                        # The deterministic authoring refusal (F4): raised
                        # INSIDE the managed transaction so the whole unit
                        # rolls back; the class is the registered
                        # deterministic one — one terminal, once, below.
                        raise TransactionalStepUnsupportedError(
                            "a transactional step's body cannot return the Exit sentinel — "
                            "the settlement owns its one terminal; return the data result "
                            "or wire an Exit through an ordinary step"
                        )
                    try:
                        result = encode_result(outcome_value)
                    except Exception as encode_exc:  # Why: the unencodable result is a deterministic output/configuration defect — the same one rollback and one deterministic terminal as the Exit refusal; a genuine provider/IO exception raised by the BODY never reaches this arm (it escapes the enforce boundary above as the body failure it is).
                        raise TransactionalStepUnsupportedError(
                            "a transactional step's result could not be encoded — the "
                            "settlement's one terminal carries the truthful failure; "
                            "return an encodable (jsonb-typed) result"
                        ) from encode_exc
                    tx1 = await _run_tx1_statements(
                        conn,
                        self.wsql,
                        flow_id=flow_id,
                        job_id=JobId(row["id"]),
                        worker_id=self._worker_id,
                        attempt=attempt,
                        claim_epoch=claim_epoch,
                        outcome="succeeded",
                        result=result,
                        error_class=None,
                        error_message=None,
                        error_traceback=None,
                        fork=None,
                        capture=None,
                        step_key=row["step_key"],
                        map_index=row["map_index"],
                        ledger_id=ledger_id,
                        ledger_claimed_at=ledger_claimed_at,
                    )
                    if not tx1.applied:
                        raise _SettlementRefusedError(
                            "the settlement's terminal fence no longer matches this claim",
                            claim_fence=True,
                        )
                    if tx1.continuation_fenced:
                        raise _SettlementRefusedError(
                            "the run root is already terminal; the settlement refuses",
                            claim_fence=False,
                        )
                    # The critical section ends at the context's COMMIT: no
                    # helper, application call, or progress flush runs between
                    # the terminal CAS above and the commit.
            except asyncpg.InterfaceError as exc:
                # The capture boundary of the ROLLBACK-COLLISION window (the
                # vanilla transactional consumer's own shape): under the
                # one-shot stamp the deadline's marker was the exception in
                # flight through the managed ``__aexit__``, whose ROLLBACK is
                # the only statement this path issues on the shared
                # connection from there — an InterfaceError arriving now can
                # only be that ROLLBACK colliding with the hostile unwind.
                # Without the stamp (a pre-deadline body InterfaceError, a
                # later unrelated one) the error propagates unchanged — the
                # re-label never launders a genuine body class.
                if ctx._take_tx_rollback_collision_window():  # pyright: ignore[reportPrivateUsage]  # Why: the capture boundary is the window's one designated reader (the setter's contract on the subtype).
                    raise _StartToCloseExceededError(
                        "the start_to_close deadline fired; the managed rollback"
                        " collided with the body's still-hostile unwind on the shared"
                        " connection and the collision was re-labeled to the deadline"
                    ) from exc
                raise
            finally:
                # The window is one-shot per attempt: consume a stale stamp
                # even when no interface error arrived, so no later failure
                # of this attempt is ever re-labelled by a window it does
                # not belong to.
                ctx._take_tx_rollback_collision_window()  # pyright: ignore[reportPrivateUsage]  # Why: the same designated-reader contract — the one-shot consumption.
        except _SettlementRefusedError as refused:
            if not refused.claim_fence:
                return "", refused, None
            # The claim-fence no-match: the unit (application prefix
            # included) rolled back with the context exit. Record the
            # attempt's fence durably OUTSIDE the rolled-back unit — the
            # ordinary kernel's fenced arm writes it inside tx1, where no
            # application prefix needs protecting. The fence targets the
            # exact claim identity (id + instant).
            if ledger_id is not None:
                async with conn.transaction():
                    await conn.execute(
                        self.wsql.ledger_fence_by_id,
                        ledger_id,
                        None,
                        ledger_claimed_at,
                    )
            return "fenced", None, None
        except TransactionalStepUnsupportedError as refused:
            # The deterministic outcome refusal escaped the unit (rolled
            # back with it): it is the BODY-VISIBLE authoring channel's
            # class — the caller's disposition routes it through the
            # registered deterministic owner (one terminal, the ladder
            # unburned, no Exit finalizer, no second commit).
            return "", refused, None
        except Exception as exc:
            # The phase-explicit escape-point classifier: the transaction
            # ENTER failed before the body could run (machinery); the
            # body's own exceptions and the deadline marker route to the
            # dispositions; the machinery statements' escapes after the
            # body (the terminal write, the COMMIT, this unit's own SQL)
            # are the machinery boundary — reclaimed (infra class) or
            # re-raised raw by the caller, never a body verdict.
            if not unit_entered:
                return "", None, exc
            if body_done:
                return "", None, exc
            return "", exc, None

        # The COMMIT is acknowledged: the successor decrement runs in its
        # own fresh transaction on the SAME still-held connection (a pool
        # of one never deadlocks, and no second pool acquisition exists).
        # An ambiguous commit cannot reapply the application effect: the
        # durable terminal above is the settlement's record, and the
        # recovery re-reads it.
        try:
            hits, fired = await _run_tx2_statements(
                conn,
                self.wsql,
                flow_id=flow_id,
                parent_id=JobId(row["id"]),
                reducers=None,
                outcome="succeeded",
                step_key=row["step_key"],
                map_index=row["map_index"],
            )
        except Exception as exc:
            return "", None, exc
        if fired:
            logger.info(
                "wf.transactional_join_fired",
                run_id=str(flow_id),
                node=row["step_key"],
                joins=[fired_join.step_key for fired_join in fired],
            )
        _ = hits
        return "succeeded", None, None

    async def _transactional_disposition(
        self,
        ctx: StepContext,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        node: NodeDecl | None,
        exc: Exception,
        *,
        claim_epoch: int = 0,
        ledger_claimed_at: datetime | None = None,
    ) -> str:
        """The failure routes AFTER the connection's release (or trap) and
        the emitter's close — the same disposition owners, vocabulary and
        fences the ordinary tail uses (the connection they acquire is a
        fresh pool acquisition, never the released claim connection)."""
        from taskq.context import CancelOrigin
        from taskq.exceptions import RetryAfter, Snooze
        from taskq.worker._handlers import _StartToCloseExceededError
        from taskq.workflows.api._ctx_wait import NodeHeldError

        if ctx.cancellation_requested and ctx.cancel_origin is CancelOrigin.SHUTDOWN:
            # A started SHUTDOWN claim is requeued without exhausting the
            # ordinary body ladder (the ordinary tail's own law).
            await self._reclaim_step(
                flow_id,
                row,
                attempt,
                RuntimeError("workflow worker shutdown interrupted the started attempt"),
                claim_epoch=claim_epoch,
                ledger_id=ledger_id,
                ledger_claimed_at=ledger_claimed_at,
            )
            return "interrupted"
        if isinstance(exc, NodeHeldError):
            return "held"
        if isinstance(exc, EmitBackpressure | Snooze | RetryAfter):
            return await self._defer_step(
                flow_id,
                row,
                attempt,
                ledger_id,
                exc,
                claim_epoch=claim_epoch,
                ledger_claimed_at=ledger_claimed_at,
            )
        if isinstance(exc, _StartToCloseExceededError):
            return await self._deadline_disposition(
                flow_id,
                row,
                attempt,
                node,
                claim_epoch=claim_epoch,
                ledger_id=ledger_id,
                ledger_claimed_at=ledger_claimed_at,
            )
        route = await self._ladder_or_fail(
            flow_id,
            row,
            attempt,
            node,
            exc,
            claim_epoch=claim_epoch,
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
        )
        if route == "deferred":
            return "deferred"
        if route in ("cancelled", "fenced"):
            return route
        return "laddered"

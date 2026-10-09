"""The LOOP machinery (the split of ``api/_runner`` — §7b's
concerns-separate law): the T19 loop driver — fresh jobs per iteration,
the carry frozen at spawn and advanced exactly once atomically, the
control union consumed, the exhaustion arms (the cap, the body failure),
and the machinery reclaim (the escape-point contract).

Module homes: the loop's declaration verbs (``loop``/``Done``/``Refine``)
live in ``api/_loop``; the loop's named statements in ``api/_sql_loop``;
the ladder/failure-class classifier in ``_runner_ladder``; the runner's
driver here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._progress import ProgressEmitter
from taskq.workflows.api._ctx import build_step_context
from taskq.workflows.api._ctx_wait import NodeHeldError
from taskq.workflows.api._runner_codec import encode_result, jsonable
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.api._runner_ladder import is_infra_fault
from taskq.workflows.api._sql_runner import NODE_REPEND_SQL_TEMPLATE, render_sql

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

__all__ = ["LoopOps"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


class _LoopHost(Protocol):
    """The runner surface the loop driver reads (the split's typed seam
    — the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    schema: str
    compiled: Any
    #: THE DRIVER'S IDENTITY (the claim identity's fence): the id the
    # node claim stamped on the row — the loop's advance/exhaust bind it
    # (a zombie driver's write is refused by its own legs).
    _worker_id: JobId

    async def _flow_input(self, flow_id: JobId) -> object: ...

    def _redact_hook(self) -> Any: ...

    async def _finalize_success(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        result: dict[str, object] | None,
        emitter: ProgressEmitter | None = None,
        *,
        claim_epoch: int = 0,
    ) -> None: ...


class LoopOps(_LoopHost):
    """The runner's loop-driver mixin (the machinery's home; the runner
    composes it)."""

    __slots__ = ()

    async def _run_loop_node(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        ledger_id: JobId | None = None,
        emitter: ProgressEmitter | None = None,
        *,
        claim_epoch: int = 0,
    ) -> None:
        """THE LOOP DRIVER (T19): fresh jobs per iteration
        (``<loop>.iter<i>`` — the ledger's per-iteration identity); the
        carry FROZEN AT SPAWN (read once per iteration from the row) and
        advanced EXACTLY ONCE per iteration IN THE ADVANCE STATEMENT
        (atomic with the cap guard — a refused advance IS the
        exhaustion); the control union consumed (``Done`` stops,
        ``Refine`` threads); ``until=`` AWAITED per iteration. The
        budget deadline initializes FROM PG'S CLOCK at the first claim
        (the DB-clock doctrine); ``budget_remaining_ms`` is the on-wake
        read of the wall (holds are free).

        THE ESCAPE-POINT CONTRACT (attack-3 H5's cure): the driver's
        drive runs under the machinery boundary — an INFRA fault raised
        by the driver's own statements reclaims (the ledger says
        'crashed', the ladder untouched); anything else re-raises. The
        BODY's exceptions never reach this wrapper — the body boundary
        routes them first (a body cannot forge an infra fault by raising
        ConnectionError)."""
        from taskq.workflows.api._loop import LoopSpec

        spec = cast("LoopSpec", node.loop_spec)  # the driver's own declaration

        try:
            return await self._drive_loop(
                flow_id, row, attempt, node, spec, ledger_id, emitter, claim_epoch=claim_epoch
            )
        except Exception as exc:  # Why: the MACHINERY boundary — the classifier reads WHERE the error escaped, never its type (attack-3 H5's cure). BLE001 is the boundary's shape: ANY machinery exception is classified, then re-raised or reclaimed.
            if is_infra_fault(exc):
                return await self._reclaim_loop(flow_id, row, attempt, exc)
            raise

    async def _drive_loop(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        spec: Any,
        ledger_id: JobId | None,
        emitter: ProgressEmitter | None = None,
        *,
        claim_epoch: int = 0,
    ) -> None:
        """The loop driver's own drive (the machinery side of the
        escape-point contract — attack-3 H5's cure): every exception
        raised by THIS method's own statements (the state reads, the
        ledger claims, the advance, the exhaust) is a TRANSPORT/INFRA
        fault of the machinery — the caller's reclaim owns it, the
        ladder untouched. The BODY's exceptions never reach here: the
        body boundary routes them (the failure-class rules) before
        returning — a body CANNOT forge an infra fault by raising
        ConnectionError (its exceptions exhaust the loop as the body
        failure they are)."""
        from taskq._json import loads as _loads
        from taskq.workflows.api._loop import Done, Refine
        from taskq.workflows.api._runner_codec import rehydrate_carry
        from taskq.workflows.api._sql_loop import (
            LOOP_ADVANCE_SQL,
            LOOP_ERROR_BODY,
            LOOP_ERROR_SHAPE,
            LOOP_INIT_SQL,
            LOOP_NODE_STATE_SQL,
            LOOP_REMAINING_SQL,
            render_loop_sql,
        )
        from taskq.workflows.ledger import claim_step_ledger, memoized_step_result

        async with self.pool.acquire() as conn:
            state = await conn.fetchrow(
                render_loop_sql(LOOP_NODE_STATE_SQL, self.schema), row["id"]
            )
            assert state is not None
        meta_raw = state["metadata"]
        meta = _loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
        assert isinstance(meta, dict)  # the Any-contract walk (the seed wrote the shape)
        meta_doc = cast(dict[str, object], meta)  # the walk's boundary
        # THE INIT (first claim): the budget wall starts HERE (PG's
        # clock); the iteration counter + the initial carry are set.
        # THE CARRY'S TYPED SPLIT: the initial VALUE rides the spec's
        # own field (the declared split — no isinstance re-derivation
        # here; the type check is the validator's subject). It is
        # serialized through the codec (a model dumps to its dict) and
        # the DECLARED TYPE is re-applied at the read below (the carry's
        # typed contract — the body receives the declared type at
        # iteration 0 AND after every resume).
        initial_carry: object = (
            jsonable(cast(object, spec.initial_carry)) if spec.initial_carry is not None else None
        )
        if "iteration" not in meta_doc:
            init_meta: dict[str, object] = {
                **meta_doc,
                "kind": "loop",
                "iteration": 0,
                "carry": initial_carry,
                "max_iterations": spec.max_iterations,
            }
            async with self.pool.acquire() as conn:
                await conn.execute(
                    render_loop_sql(LOOP_INIT_SQL, self.schema),
                    row["id"],
                    spec.budget_s,  # NULL = the no-wall shape (the sweep reads NULL as no deadline)
                    dumps_jsonb_str(init_meta),
                )
            meta_doc = init_meta
        # THE ON-WAKE REMAINING (holds are free — a PG-clock read): the
        # number the loop ctx carries (the context contract's
        # budget_remaining_ms). The FIRST claim's init (above) just set
        # the deadline — the remaining is the budget's own value (the
        # clock hasn't spent anything).
        remaining: int | None = None
        if "iteration" not in meta and spec.budget_s is not None:
            remaining = int(spec.budget_s * 1000)
        elif state["budget_deadline"] is not None:
            async with self.pool.acquire() as conn:
                remaining = await conn.fetchval(
                    render_loop_sql(LOOP_REMAINING_SQL, self.schema), row["id"]
                )
            logger.debug("loop.wake", loop=row["step_key"], remaining_ms=remaining)

        iteration = int(str(meta_doc.get("iteration", 0)))
        carry: object = meta_doc.get("carry")

        max_wall = 10_000  # the driver's own bound (a hang is a defect with no stack)
        for _ in range(max_wall):
            # THE UNTIL PREDICATE — awaited (a sync closure returning a
            # coroutine object is the convicted dragon).
            if node.loop_until is not None and await node.loop_until():
                await self._finalize_success(
                    flow_id,
                    row,
                    attempt,
                    node,
                    encode_result(carry),
                    emitter=emitter,
                    claim_epoch=claim_epoch,
                )
                return

            # THE CAP CHECK (the driver's top-of-loop face — the advance
            # guard now lets the FINAL iteration run and the counter
            # REACH the cap, so the live path exhausts HERE, at the top
            # of the next pass; the crash window — a worker death after
            # the final advance — leaves exactly this state on the row
            # for the SWEEP's arm to own: the sweep's cap predicate is
            # REACHABLE in production, never vacuous). The named state +
            # the flow terminalized in the same tx: STRANDED-FLOW.
            if spec.max_iterations is not None and iteration >= spec.max_iterations:
                await self._exhaust_loop(
                    flow_id,
                    row,
                    None,
                    None,
                    worker_id=self._worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                )
                return

            iter_key = f"{row['step_key']}.iter{iteration}"
            # FRESH JOBS: the iteration-scoped ledger claim (the
            # idempotency ledger per-iteration — T05's contract
            # unchanged); the MEMO: a terminal iteration replays (the
            # crash recovery reads the row — never a re-run body).
            async with self.pool.acquire() as memo_conn:
                memo = await memoized_step_result(
                    memo_conn,
                    self.wsql,
                    flow_id=flow_id,
                    step_key=iter_key,
                    map_index=None,
                )
            if memo is not None and memo.status == "succeeded":
                # the memo replay's decoded face (the ledger's jsonb)
                memo_doc = cast(dict[str, object], memo.result)
                if memo_doc.get("done"):
                    payload: object = memo_doc.get("payload")
                    await self._finalize_success(
                        flow_id,
                        row,
                        attempt,
                        node,
                        encode_result(payload),
                        emitter=emitter,
                        claim_epoch=claim_epoch,
                    )
                    return
                carry = cast(object, memo_doc.get("feedback"))  # the walk's boundary
            elif memo is not None and memo.status == "failed":
                # THE REPLAY HONORS THE RECORDED TRUTH (the shape error's
                # ledger law): a FAILED iteration's terminal is the named
                # typed error the recording made — the loop exhausts with
                # THAT class and message (the diagnosis names it), the
                # body is never re-run (one invocation per iteration),
                # and the raw return is never re-threaded AS A REFINE
                # (the fabrication the pre-cure ledger committed: a
                # SUCCEEDED row laundering a wrong-shape return).
                await self._exhaust_loop(
                    flow_id,
                    row,
                    memo.error_class or LOOP_ERROR_BODY,
                    memo.error_message,
                    worker_id=self._worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                )
                return
            else:
                async with self.pool.acquire() as conn:
                    await claim_step_ledger(
                        conn,
                        self.wsql,
                        flow_id=flow_id,
                        job_id=JobId(row["id"]),
                        step_key=iter_key,
                        map_index=None,
                        attempt=attempt,
                    )
                loop_ctx = build_step_context(
                    flow_id=flow_id,
                    job_id=JobId(row["id"]),
                    node_key=row["step_key"],
                    attempt=attempt,
                    pool=self.pool,
                    wsql=self.wsql,
                    input=await self._flow_input(flow_id),
                    is_loop=True,
                    ledger_id=ledger_id,
                    workflow_name=self.compiled.name,
                    redact=self._redact_hook(),
                    progress=emitter,
                    # THE RUNTIME INFO: the loop ctx carries the wall's
                    # remaining read (the budget PAUSES while held — the
                    # number is the on-wake read).
                    flow_name=self.compiled.name,
                    queue=node.queue,
                    claimed_at=datetime.now(UTC),
                    budget_remaining_ms=remaining,
                )
                # THE CARRY'S TYPED CONTRACT, at the ONE point a carry
                # reaches a body: re-hydrated through the DECLARED type's
                # validator — at iteration 0 (the init's dump), after
                # EVERY resume (the row read), on the memo replay (the
                # recorded feedback), and on the in-memory Refine
                # (idempotent). A dict is NEVER the body's carry when a
                # type is declared. (BEFORE the body boundary: a contract
                # refusal is the DECLARATION's bug — the machinery's own
                # loud error, never a body failure to absorb.)
                carry = rehydrate_carry(spec.carry_type, carry)
                try:
                    outcome = await node.loop_body(loop_ctx, carry)
                except NodeHeldError:
                    # THE HOLD INSIDE THE ITERATION (T10 x T19's
                    # composition): the loop node rests in the held
                    # representation (the budget PAUSED — register_hold's
                    # loop case); the resume re-runs the iteration (the
                    # ledger's 'awaited' row — the ladder unburned). The
                    # driver's walls stay blind while paused (the arm's
                    # heart) — holds are free.
                    return
                except Exception as exc:
                    # THE BODY BOUNDARY (attack-3 H5's cure): EVERY
                    # exception crossing it is a BODY failure — the
                    # loop's typed failure (the named class, the flow
                    # terminalized in the same tx). The escape-point
                    # contract: the classifier never reads the TYPE of
                    # an exception the body raised, so a body cannot
                    # forge an infra fault by raising ConnectionError —
                    # the STRANDED-FLOW shape is unconstructible from
                    # body code. (SignalTimeoutError — the timed-out
                    # hold — is a body exception exactly like this.)
                    await self._exhaust_loop(
                        flow_id,
                        row,
                        LOOP_ERROR_BODY,
                        f"the loop body failed: {type(exc).__name__}: {str(exc)[:200]}",
                        worker_id=self._worker_id,
                        attempt=attempt,
                        claim_epoch=claim_epoch,
                    )
                    return
                await self._record_iteration_terminal(flow_id, iter_key, attempt, outcome)

                # THE CONTROL UNION (consumed with the residual machinery):
                if isinstance(outcome, Done):
                    done_payload = cast(object, outcome.payload)  # the union's boundary
                    await self._finalize_success(
                        flow_id,
                        row,
                        attempt,
                        node,
                        encode_result(done_payload),
                        emitter=emitter,
                        claim_epoch=claim_epoch,
                    )
                    return
                if not isinstance(outcome, Refine):
                    # THE SHAPE ERROR NAMED (the ledger's truth law): a
                    # body returning neither Done nor Refine is the TYPED
                    # shape error — the iteration's terminal ALREADY
                    # recorded it (above), and the loop exhausts with the
                    # named class so the diagnosis names it. The pre-cure
                    # bare assert laundered the wrong-shape return into a
                    # SUCCEEDED iteration whose memo replayed AS A REFINE.
                    await self._exhaust_loop(
                        flow_id,
                        row,
                        LOOP_ERROR_SHAPE,
                        f"the loop body returned {type(outcome).__name__!r} — "
                        "the body must return Done(...) or Refine(...)",
                        worker_id=self._worker_id,
                        attempt=attempt,
                        claim_epoch=claim_epoch,
                    )
                    return
                carry = cast(object, outcome.feedback)  # the walk's boundary

            # THE ADVANCE STATEMENT — the carry + THE CAP GUARD, one
            # atomic write (the guard lets the advance reach EXACTLY the
            # cap so the final iteration runs; a refused advance — a
            # concurrent terminal — stays the backstop exhaustion). THE
            # CLAIM IDENTITY'S FENCE rides the statement (the
            # one-tx-finalize doctrine's legs): a zombie driver's advance
            # — its claim lapsed, the loop reclaimed — is REFUSED, its
            # stale payload never moves the counter backward.
            async with self.pool.acquire() as conn:
                advanced = await conn.fetchval(
                    render_loop_sql(LOOP_ADVANCE_SQL, self.schema),
                    row["id"],
                    dumps_jsonb_str({"carry": jsonable(carry), "iteration": iteration + 1}),
                    spec.max_iterations if spec.max_iterations is not None else 2**31 - 1,
                    self._worker_id,
                    attempt,
                    claim_epoch,
                )
            if advanced is None:
                await self._exhaust_loop(
                    flow_id,
                    row,
                    None,
                    None,
                    worker_id=self._worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                )
                return
            iteration += 1

        # the driver's own bound exhausted — the loud refusal (never a
        # silent wedge)
        raise WorkflowRunError(
            f"loop {row['step_key']!r} exceeded the driver's iteration bound "
            f"({max_wall}) — the walls never fired; a bug, refused loudly"
        )

    async def _record_iteration_terminal(
        self, flow_id: JobId, iter_key: str, attempt: int, outcome: object
    ) -> None:
        """The iteration's ledger terminal (the §13.3 trace shape: the
        iteration index + the Done/Refine kind per iteration).

        THE LEDGER RECORDS THE TRUTH (the shape error's law): a body
        returning neither Done nor Refine records a FAILED terminal with
        the TYPED ``LoopBodyShapeError`` class — never a SUCCEEDED row
        whose feedback-shaped payload the memo replay would re-thread as
        a Refine (the fabrication the pre-cure recorder committed: the
        'other' kind was laundered into ``{"done": false, "feedback":
        …}`` and its computed kind deleted)."""
        from taskq.workflows.api._loop import Done, Refine
        from taskq.workflows.api._sql_loop import LOOP_ERROR_SHAPE

        payload: dict[str, object]
        status = "succeeded"
        error_class: str | None = None
        error_message: str | None = None
        if isinstance(outcome, Done):
            payload = {"done": True, "payload": jsonable(cast(object, outcome.payload))}
        elif isinstance(outcome, Refine):
            payload = {"done": False, "feedback": jsonable(cast(object, outcome.feedback))}
        else:
            status = "failed"
            error_class = LOOP_ERROR_SHAPE
            error_message = (
                f"the loop body returned {type(outcome).__name__!r} — the "
                "body must return Done(...) or Refine(...); the iteration "
                "is the shape error it is, never a replayable refine"
            )
            payload = {"shape": jsonable(outcome)}
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                iter_key,
                attempt,
                status,
                dumps_jsonb_str(payload),
                error_class,
                error_message,
                None,
                None,
            )

    async def _reclaim_loop(
        self, flow_id: JobId, row: dict[str, Any], attempt: int, exc: Exception
    ) -> None:
        """The MACHINERY reclaim (attack-3 H5's other half): an infra
        fault raised by the DRIVER'S OWN statements — never by the body
        — records 'crashed' and re-pends WITHOUT burning the ladder
        (the vanilla lease machinery re-claims from the ledger). The
        escape-point contract's beneficiary: the body cannot reach this
        arm (its exceptions exhaust the loop as the body failure they
        are), so reclaim stays UNBOUNDED-BY-THE-BODY — a poison body
        cannot wedge the flow through the classifier."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                row["step_key"],
                attempt,
                "crashed",
                None,
                type(exc).__name__,
                str(exc)[:500],
                None,
                None,
            )
            await conn.execute(
                render_sql(NODE_REPEND_SQL_TEMPLATE, self.schema),
                row["id"],
                0.05,
            )

    async def _exhaust_loop(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        error_class: str | None,
        message: str | None,
        *,
        worker_id: JobId,
        attempt: int,
        claim_epoch: int,
    ) -> None:
        """The NAMED exhaustion (the driver's own arms — the cap check
        and the body failure; the SWEEP's arm runs the same statement):
        the named state + the FLOW TERMINALIZED in the SAME tx
        (STRANDED-FLOW) — and, when the REGISTERED policy says
        ``escalate`` (attack-3 H1's cure: the policy is READ by the
        driver AND the sweep), the ESCALATION ENQUEUES through the same
        outbox in the SAME tx, addressed to the workflow's REGISTERED
        escalation step (the body resolves from the definition registry
        at claim — never a dead letter). ``fail`` enqueues NOTHING: the
        flow terminal-fails and the record is the named state.

        THE CLAIM IDENTITY'S FENCE: the exhaust statement carries the
        SAME legs every other terminal write carries (worker + attempt +
        claim_epoch) — a zombie driver's exhaust (its claim lapsed, the
        loop reclaimed and healthy under a new driver) updates nothing:
        no killed loop, no forged named state, no escalation row."""
        from taskq.workflows.api._loop import (
            ESCALATION_STEP_KEY,
            escalation_bindings,
        )
        from taskq.workflows.api._sql_loop import (
            ITERATION_STATE_CAP_EXHAUSTED,
            LOOP_ERROR_BODY,
            LOOP_ERROR_CAP,
            LOOP_ERROR_SHAPE,
            LOOP_ESCALATION_OUTBOX_SQL,
            LOOP_EXHAUST_SQL,
            render_loop_sql,
        )

        error_class = error_class or LOOP_ERROR_CAP
        if error_class == LOOP_ERROR_BODY:
            state_name = "loop_body_failed"
        elif error_class == LOOP_ERROR_SHAPE:
            state_name = "loop_body_shape_error"
        else:
            state_name = ITERATION_STATE_CAP_EXHAUSTED
        policy = "escalate"
        if node_spec := self.compiled.nodes.get(row["step_key"]):
            loop_spec = getattr(node_spec, "loop_spec", None)
            if loop_spec is not None:
                policy = loop_spec.on_exhausted
        async with self.pool.acquire() as conn, conn.transaction():
            result = await conn.fetchrow(
                render_loop_sql(LOOP_EXHAUST_SQL, self.schema),
                row["id"],
                error_class,
                f'{{"iteration_state": "{state_name}", "kind": "loop"}}',
                message or f"the loop's wall fired (the named state: {state_name})",
                worker_id,
                attempt,
                claim_epoch,
            )
            if result is None or not result["loop_exhausted"]:
                return  # another writer got there first — the CAS held
            if policy != "escalate":
                return  # the fail policy: the named state is the record, no enqueue
            bindings = escalation_bindings(
                self.compiled.name,
                row["step_key"],
                flow_id=flow_id,
                error_class=error_class,
                message=message,
            )
            if bindings is None:
                logger.warning(
                    "loop.escalation_unregistered",
                    loop=row["step_key"],
                    run_id=str(flow_id),
                    why="the escalation policy declared but the workflow's "
                    "escalation step is not registered in this process (D1) — "
                    "the named state stands, no outbox row is written",
                )
                return
            await conn.execute(
                render_loop_sql(LOOP_ESCALATION_OUTBOX_SQL, self.schema),
                new_uuid(),
                row["id"],
                flow_id,
                ESCALATION_STEP_KEY,
                dumps_jsonb_str(bindings),
            )

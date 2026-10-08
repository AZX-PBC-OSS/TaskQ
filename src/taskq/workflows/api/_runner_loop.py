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
            return await self._drive_loop(flow_id, row, attempt, node, spec, ledger_id, emitter)
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
        from taskq.workflows.api._sql_loop import (
            LOOP_ADVANCE_SQL,
            LOOP_ERROR_BODY,
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
        initial_carry: object
        if isinstance(spec.carry_type, dict | list | str | int | float | bool):
            initial_carry = jsonable(cast(object, spec.carry_type))
        else:
            initial_carry = None
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
                    spec.budget_s or 0.0,
                    dumps_jsonb_str(init_meta),
                )
            meta_doc = init_meta
        # THE ON-WAKE REMAINING (holds are free — a PG-clock read).
        if state["budget_deadline"] is not None:
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
                    flow_id, row, attempt, node, encode_result(carry), emitter=emitter
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
                await self._exhaust_loop(flow_id, row, None, None)
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
                        flow_id, row, attempt, node, encode_result(payload), emitter=emitter
                    )
                    return
                carry = cast(object, memo_doc.get("feedback"))  # the walk's boundary
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
                )
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
                    )
                    return
                await self._record_iteration_terminal(flow_id, iter_key, attempt, outcome)

                # THE CONTROL UNION (consumed with the residual machinery):
                if isinstance(outcome, Done):
                    done_payload = cast(object, outcome.payload)  # the union's boundary
                    await self._finalize_success(
                        flow_id, row, attempt, node, encode_result(done_payload), emitter=emitter
                    )
                    return
                assert isinstance(outcome, Refine), (
                    "the loop body must return Done(...) or Refine(...) — "
                    "anything else is the shape error (the control union's "
                    "residual machinery refuses the unconsumed member)"
                )
                carry = cast(object, outcome.feedback)  # the walk's boundary

            # THE ADVANCE STATEMENT — the carry + THE CAP GUARD, one
            # atomic write (the guard lets the advance reach EXACTLY the
            # cap so the final iteration runs; a refused advance — a
            # concurrent terminal — stays the backstop exhaustion).
            async with self.pool.acquire() as conn:
                advanced = await conn.fetchval(
                    render_loop_sql(LOOP_ADVANCE_SQL, self.schema),
                    row["id"],
                    dumps_jsonb_str({"carry": jsonable(carry), "iteration": iteration + 1}),
                    spec.max_iterations if spec.max_iterations is not None else 2**31 - 1,
                )
            if advanced is None:
                await self._exhaust_loop(flow_id, row, None, None)
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
        iteration index + the Done/Refine kind per iteration)."""
        from taskq.workflows.api._loop import Done, Refine

        kind = (
            "done"
            if isinstance(outcome, Done)
            else "refine"
            if isinstance(outcome, Refine)
            else "other"
        )
        # The generic wrappers' payloads launder through the declared
        # object boundary (the walk's cast).
        payload: dict[str, object]
        if isinstance(outcome, Done):
            payload = {"done": True, "payload": jsonable(cast(object, outcome.payload))}
        elif isinstance(outcome, Refine):
            payload = {"done": False, "feedback": jsonable(cast(object, outcome.feedback))}
        else:
            payload = {"done": False, "feedback": jsonable(outcome)}
        del kind
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                iter_key,
                attempt,
                "succeeded",
                dumps_jsonb_str(payload),
                None,
                None,
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
        flow terminal-fails and the record is the named state."""
        from taskq.workflows.api._loop import (
            ESCALATION_STEP_KEY,
            escalation_bindings,
        )
        from taskq.workflows.api._sql_loop import (
            ITERATION_STATE_CAP_EXHAUSTED,
            LOOP_ERROR_BODY,
            LOOP_ERROR_CAP,
            LOOP_ESCALATION_OUTBOX_SQL,
            LOOP_EXHAUST_SQL,
            render_loop_sql,
        )

        error_class = error_class or LOOP_ERROR_CAP
        state_name = (
            ITERATION_STATE_CAP_EXHAUSTED if error_class != LOOP_ERROR_BODY else "loop_body_failed"
        )
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

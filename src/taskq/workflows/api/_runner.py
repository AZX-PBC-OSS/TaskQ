"""The flow RUNNER (T09): lowers a :class:`CompiledWorkflow` into the
engine's rows and drives them — ``create_flow(spec, input)`` (cut #7: the
run carries its input, cross-flow data never dies in a Python closure),
the dispatch loop whose bodies resolve FROM THE REGISTERED DEFINITION
(D1 — the registry is the only body source; a per-call body map is the
convicted double-dispatch shape), and the test/demo driver
``drive(flow_id, until=…)`` (cut #10 — no consumer hand-rolls a
dispatch-poll loop).

THE DISPATCH CONTRACT: a claimable node is ``pending`` + ``deps_pending
= 0`` on a live flow; the claim is ONE fenced statement (the row goes
``running`` under this worker's id, the attempt increments — the only
grant of work, P3 rule 2); the body runs with the DECODED parent results
(jsonb decoded ONCE, cut #14); the finalize is the engine's own
``finalize_node`` (the two-tx shape unchanged). A body exception ladders:
attempt failures emit NO terminal (P3 rule 7) — the node re-pends with
backoff until ``max_attempts``, then terminal-fails and T06's propagation
takes over (the cascade / the collect fan-in, per the edges' declared
policy).

The runner is the API's test/demo driver and the reference worker shape —
a fleet worker replaces only the claim loop's concurrency, never the
semantics.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.obs import get_logger
from taskq.workflows._progress import (
    KIND_NODE_STARTED,
    KIND_NODE_TERMINAL,
    MESSAGE_TERMINAL_MAX,
    ProgressEmitter,
    project_auto_event,
    validate_emission,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sql_finalize import NODE_INSERT_SQL
from taskq.workflows._types import ChildSpec, EmitChild, ForkSpec, JoinSpec, NodeSpec, _jsonb
from taskq.workflows.api._graph import Exit
from taskq.workflows.context import WorkflowSteps
from taskq.workflows.engine import fan_in_skip, finalize_node
from taskq.workflows.ledger import (
    RunClaim,
    insert_flow_run,
    step_idempotency_key,
    step_idempotency_scope,
)

__all__ = ["FlowRunner", "StepContext", "WorkflowRunError"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The flow input key on the root row's payload (cut #7: the input rides
#: the ROW — a restart between stages reads it back).
_INPUT_KEY = "wf_input"

#: The skip record's result marker (the v1 skip semantics: the node
#: succeeds WITH the record — the envelope never lies about what ran).
_SKIPPED_RESULT: dict[str, object] = {"skipped": True}


class WorkflowRunError(RuntimeError):
    """The run cannot proceed — the graph is not registered, or the flow
    row is missing (a caller bug, refused loudly)."""


class _NodeHeld(Exception):
    """The wait site's control-flow unwinding (INTERNAL): the body
    raised it after registering the hold — the runner catches it, leaves
    the node in the held representation (pending + the deadline + the
    signal row as truth; NO terminal, NO ledger failure — the resume
    consumes no attempt), and the worker releases the slot. The body
    re-executes FROM THE TOP on resume (the re-execution doctrine)."""

    def __init__(self, hold_id: str, signal_names: tuple[str, ...]) -> None:
        super().__init__(f"node held on {signal_names} (hold {hold_id})")
        self.hold_id = hold_id
        self.signal_names = signal_names


class SignalUnavailableError(RuntimeError):
    """``ctx.signal(name)`` read a signal that has no delivered payload
    (the read-before-delivery mistake — the typed refusal, never a
    silent None)."""


@dataclass(frozen=True, slots=True)
class StepContext:
    """The body's runtime context: the run's identity + the input (cut
    #7) + the memoized step runner. Bodies read ``ctx.input`` and run
    side effects through ``ctx.step`` (the ledger's replay contract)."""

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

    async def cursor(self) -> dict[str, object]:
        """THE STREAMING SOURCE'S CHECKPOINTED CURSOR (T20): read from
        THIS source row's own metadata — the emit tx's checkpoint, under
        the ``emit_cursor`` key. ``{}`` before the first emit; the
        resume's body re-reads it to continue from the last COMMITTED
        page (never N-1, never N+1)."""
        from taskq.workflows._emit import EMIT_CURSOR_KEY

        async with self._pool.acquire() as conn:
            raw = await conn.fetchval(
                _stmt(_SOURCE_CURSOR_SQL_TEMPLATE, self._wsql.schema),
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
            claim_epoch=0,  # the runner's own claim writes epoch 0 (_NODE_CLAIM_SQL_TEMPLATE)
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

    async def wait_signal(
        self,
        signals: Any,
        *,
        timeout_s: float | None = None,
        reason: str | None = None,
        tool: str | None = None,
        args: dict[str, object] | None = None,
        discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
    ) -> Any:
        """THE TYPED WAIT (T10): the TUPLE FORM is the typed wait —
        ``await ctx.wait_signal((Approval, Escalate))`` (PEP 604 unions
        in value position are ``UnionType`` and carry no static payload
        information — Package B, finding 2); the single-payload form is
        the one-member overload. THE RESUME CONTRACT (documented ON the
        method — cut #18's disposition): **the body re-executes FROM
        THE TOP on resume** — make it idempotent; pre-wait side effects
        are ``ctx.step``-ledgered and replay cheap.

        No unresolved signal row → the node HOLDS (the held
        representation: pending + the deadline + the signal row as
        truth; the worker releases — no slot held; a LOOP node's budget
        PAUSES). A DELIVERED row (the resume) returns the payload.
        ``timeout=None`` must be EXPLICIT — the bare form is the W1
        validate warning ("a workflow that waits forever on a human is
        a support ticket").

        THE TIMEOUT FACE (attack-3 B1's cure): an ABANDONED hold (the
        expiry sweep fired on this wait site, no held row stands) RAISES
        :class:`taskq.exceptions.SignalTimeoutError` — the glossary
        exception, raised at the wait site; the body's ladder/except
        owns it from there (a step ladders + terminal-fails; a loop's
        failure-class rules route it as the BODY failure it is). The
        re-execution NEVER automatically mints a new epoch — hold →
        expire → re-hold → ∞ is the convicted dragon. A DELIBERATE
        re-wait (the body CAUGHT the timeout face and waits again within
        the same attempt) is a NEW body decision: it registers a NEW
        hold with a NEW epoch.

        ``discriminator=`` (attack-3 B2's small honest API): when the
        payload fits MORE than one declared model, the gate's explicit
        picker resolves the union — its pick must be one of the fitting
        candidates. Without one, an ambiguous payload is the typed
        refusal at the deliver boundary."""
        models: tuple[type[BaseModel], ...] = cast(
            tuple[type[BaseModel], ...],
            signals if isinstance(signals, tuple) else (signals,),
        )
        names = tuple(m.__name__ for m in models)
        signal_name = names[0] if len(names) == 1 else "|".join(names)
        from taskq.exceptions import SignalTimeoutError
        from taskq.workflows.api._hitl import (
            mark_awaited,
            register_hold,
            register_signal_models,
        )

        # THE CATALOG (the typed door's runtime registry): this process
        # ran the wait site — the deliver boundary validates against
        # THESE models (D1's discipline, the deliver path's face).
        register_signal_models(
            self._workflow_name, self.node_key, signal_name, models, discriminator
        )

        # RESUME-NOT-RETRY's ledger face: THIS attempt's ledger row
        # records 'awaited' (never 'failed' — the ladder counts failed
        # only).
        if self._ledger_id is not None:
            async with self._pool.acquire() as conn:
                await mark_awaited(conn, self._wsql.schema, self._ledger_id)

        # THE STALE-PAYLOAD DRAGON'S KILL SITES (two, both structural):
        # (1) THE ANSWER QUEUE — the delivered holds are the node's
        # durable answers, consumed IN EPOCH ORDER by each attempt's wait
        # sequence (the per-attempt CURSOR): a RETRY replays the answers
        # (they are the attempt's inputs — the operator never
        # re-answers, the retry is deterministic); a wait past the
        # queue's end registers a NEW hold (a NEW epoch — multi-hold).
        # (2) the CONSUMED hold can never answer twice.
        async with self._pool.acquire() as conn:
            cursor_meta = await conn.fetchval(
                _stmt(
                    "SELECT (metadata ->> $2)::int FROM {schema}.jobs WHERE id = $1",
                    self._wsql.schema,
                ),
                self.job_id,
                f"hold_cursor_{self.attempt}",
            )
            cursor = int(cursor_meta or 0)
            queue = await conn.fetch(
                _stmt(
                    "SELECT id, payload, hold_epoch FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND status = 'delivered' ORDER BY hold_epoch",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
            )
            if cursor < len(queue):
                # THE REPLAY/CONSUME: this attempt's wait takes the
                # queue's next answer (the cursor advances — the same
                # hold can never answer the same attempt twice).
                answer = queue[cursor]
                payload = (
                    _json_loads(answer["payload"])
                    if isinstance(answer["payload"], str)
                    else answer["payload"]
                )
                await conn.execute(
                    _stmt(
                        "UPDATE {schema}.jobs SET metadata = metadata || $2::jsonb WHERE id = $1",
                        self._wsql.schema,
                    ),
                    self.job_id,
                    dumps_jsonb_str({f"hold_cursor_{self.attempt}": cursor + 1}),
                )
                return self._coerce_signal(models, payload, discriminator=discriminator)
            # PAST THE QUEUE: the node's PENDING hold (if any) is THIS
            # wait's wait — the held row stands (idempotent re-hold,
            # never a second registration of one wait).
            held_row = await conn.fetchrow(
                _stmt(
                    "SELECT id FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND status = 'held' ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
            )
            if held_row is not None:
                raise _NodeHeld(hold_id=str(held_row["id"]), signal_names=names)
            # THE TIMEOUT FACE (attack-3 B1's cure): the sweep marked
            # THIS wait site's hold 'abandoned' and no held row stands —
            # the wait site RAISES the glossary exception; the body's
            # ladder/except owns it from there. NO automatic new epoch:
            # a re-execution after abandonment never re-holds (the
            # hold→expire→re-hold→∞ dragon's kill site). The DELIBERATE
            # re-wait — the body caught the face and waits again within
            # THIS attempt — is a NEW body decision: the face marker
            # (per-attempt, beside the answer cursor) lets it register a
            # NEW hold with a NEW epoch.
            abandoned_row = await conn.fetchrow(
                _stmt(
                    "SELECT id, hold_epoch FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND status = 'abandoned' ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
            )
            if abandoned_row is not None:
                face_key = f"timeout_face_{self.attempt}"
                face_seen = await conn.fetchval(
                    _stmt(
                        "SELECT (metadata ->> $2)::int FROM {schema}.jobs WHERE id = $1",
                        self._wsql.schema,
                    ),
                    self.job_id,
                    face_key,
                )
                if face_seen != int(abandoned_row["hold_epoch"]):
                    await conn.execute(
                        _stmt(
                            "UPDATE {schema}.jobs SET metadata = metadata || $2::jsonb "
                            "WHERE id = $1",
                            self._wsql.schema,
                        ),
                        self.job_id,
                        dumps_jsonb_str({face_key: int(abandoned_row["hold_epoch"])}),
                    )
                    raise SignalTimeoutError(
                        f"the hold on signal {signal_name!r} (node "
                        f"{self.node_key!r}, epoch {abandoned_row['hold_epoch']}) "
                        "timed out — the expiry sweep abandoned it and the wait "
                        "site does not re-hold: the body's ladder/except owns "
                        "the typed face from here"
                    )
            # A NEW HOLD: a NEW epoch (the count of this name's holds —
            # the identity's mint).
            epoch = await conn.fetchval(
                _stmt(
                    "SELECT COALESCE(MAX(hold_epoch), 0) + 1 FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
            )
            schema_ref: dict[str, object] = {
                m.__name__: m.model_json_schema()
                for m in models  # pyright: ignore[reportAttributeAccessIssue]
            }
            hold_id = await register_hold(
                conn,
                schema=self._wsql.schema,
                workflow_id=self.flow_id,
                node_id=self.job_id,
                node_key=self.node_key,
                signal_name=signal_name,
                hold_epoch=int(epoch),  # pyright: ignore[reportAny, reportArgumentType]
                call_id=f"call:{new_uuid()}",
                payload_schema=schema_ref,
                timeout_s=timeout_s,
                is_loop_node=self._is_loop,
                context={"reason": reason, "tool": tool, "args": args},
                redact=self._redact,
            )
        raise _NodeHeld(hold_id=str(hold_id), signal_names=names)

    @staticmethod
    def _coerce_signal(
        models: Any,
        payload: Any,
        *,
        discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
    ) -> Any:
        """The delivered payload re-validates into the declared model
        (the typed wait's return — narrows with ``isinstance``). BY
        SHAPE, never by declaration order (attack-3 B2's cure): every
        declared model is tried STRICTLY; exactly one must fit (it is
        returned); zero or more than one is the typed refusal —
        :class:`taskq.exceptions.SignalPayloadError` /
        :class:`taskq.exceptions.SignalPayloadAmbiguousError` (on the
        deliver path this refusal happens BEFORE the hold is consumed;
        here at the replay it is the body's typed failure)."""
        from taskq.workflows.api._hitl import resolve_payload_fit

        if not isinstance(payload, dict):
            return payload
        payload_doc = cast("dict[str, object]", payload)  # pyright: ignore[reportUnknownVariableType]  # Why: the answer-queue's jsonb decode — the Any-contract walk's boundary (the isinstance guard above is the runtime shape check).
        fitted = resolve_payload_fit(models, payload_doc, discriminator)
        return cast(Any, fitted.model_validate(payload_doc))

    async def signal(self, name: str) -> Any:
        """Read a DELIVERED signal's payload on resume (cut #3's cure —
        the payload rides the ROW; the author never hand-rolls a side
        table)."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                _stmt(
                    "SELECT payload, status FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                name,
            )
        if row is None or row["status"] != "delivered":
            raise SignalUnavailableError(
                f"signal {name!r} has no delivered payload on "
                f"{self.node_key!r} (status: {row['status'] if row else 'none'})"
            )
        return _json_loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]


# ── the runner's own statements (named constants — never inline SQL) ────

_NODE_CLAIM_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'running', locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds',
    attempt = attempt + 1, claim_epoch = 0
WHERE id = $1
  AND (
    status = 'pending'
    OR (status = 'scheduled' AND scheduled_at <= now())
  )
  AND deps_pending = 0
RETURNING attempt
"""

_NODE_REPEND_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'scheduled', locked_by_worker = NULL, scheduled_at = now() + ($2::double precision * interval '1 second')
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

_PARENT_RESULTS_BY_KEY_SQL_TEMPLATE = """
SELECT p.step_key, p.result
FROM {schema}.wf_edge e
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE e.child_id = $1
ORDER BY p.id
"""

#: The data-args key on the node row's payload (the wiring's plain data
#: rides the ROW — the restart reads it back; never a closure).
_WF_ARGS_KEY = "wf_args"

_ITEM_KEY = "wf_item"

_NODE_BY_STEP_KEY_SQL_TEMPLATE = """
SELECT id FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: The streaming source's checkpointed cursor read (T20): the key is the
#: bound EMIT_CURSOR_KEY constant — never an f-string SQL (the S608 rule);
#: the row is the SOURCE's own.
_SOURCE_CURSOR_SQL_TEMPLATE = """
SELECT metadata -> $2 FROM {schema}.jobs WHERE id = $1
"""

_EDGE_INSERT_SQL_TEMPLATE = """
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
VALUES ($1, $2, $3, $4)
"""

_INCREMENT_DEPS_SQL_TEMPLATE = """
UPDATE {schema}.jobs SET deps_pending = deps_pending + 1 WHERE id = $1
"""

_CLAIMABLE_NODES_SQL_TEMPLATE = """
SELECT id, step_key, map_index, attempt, trace_id, payload FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND status IN ('pending', 'scheduled')
  AND (scheduled_at IS NULL OR scheduled_at <= now())
  AND deps_pending = 0
  AND step_key <> '__flow__'
  AND NOT metadata ? 'hold'
ORDER BY id
"""

_FLOW_STATUS_SQL_TEMPLATE = """
SELECT status FROM {schema}.jobs WHERE id = $1
"""

_FLOW_PAYLOAD_SQL_TEMPLATE = """
SELECT payload FROM {schema}.jobs WHERE id = $1
"""

_SUCCEEDED_RESULTS_SQL_TEMPLATE = """
SELECT step_key, result FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'succeeded'
"""

_HELD_COUNT_SQL_TEMPLATE = """
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending'
  AND metadata ? 'hold'
"""

_ROOT_START_SQL_TEMPLATE = """
UPDATE {schema}.jobs SET status = 'running' WHERE id = $1 AND status = 'pending'
"""

_CANCEL_ROOT_SQL_TEMPLATE = """
UPDATE {schema}.jobs SET status = 'cancelled',
    error_class = 'WorkflowCancelled',
    error_message = $2,
    finished_at = clock_timestamp()
WHERE id = $1
  AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
RETURNING id
"""

_CANCEL_NODES_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
    finished_at = CASE WHEN status = 'running' THEN finished_at ELSE clock_timestamp() END,
    error_class = CASE WHEN status = 'running' THEN error_class ELSE 'WorkflowCancelled' END,
    metadata = metadata || '{"cancel_phase": "cooperative"}'::jsonb
WHERE (metadata->>'flow_id')::uuid = $1
  AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
"""

_TERMINAL_RESULT_SQL_TEMPLATE = """
SELECT result FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: THE EXIT'S DOWNSTREAM MARK (§17.1): the compiled descendants the exit
#: resolved — skipped WITH the record (the envelope never lies about the
#: nodes that didn't get to run), zero ledger rows. Terminal rows and
#: unspawned rows are untouched.
_EXIT_SKIP_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'succeeded',
    result = $2::jsonb,
    finished_at = clock_timestamp()
WHERE (metadata->>'flow_id')::uuid = $1
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
  AND step_key = ANY($3)
"""

#: THE MANUAL RESUME'S NODE CAS (§17.2): terminal-FAILED → pending — ONE
#: statement, the only grant; the attempt ordinal untouched (CONTINUES —
#: the ladder's own count is the budget).
_RETRY_NODE_CAS_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'pending',
    error_class = NULL,
    error_message = NULL,
    scheduled_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL
WHERE step_key = $2
  AND (metadata->>'flow_id')::uuid = $1
  AND status = 'failed'
RETURNING id
"""

#: THE CLOSURE RE-OPENS: the cascade's blocked rows return to join-wait —
#: the sweep's re-derive re-derives them from the edge ledger (a stamp is
#: the cache, never the truth).
_RETRY_REOPEN_CLOSURE_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(metadata, '{blocking_reason}', '"join"'::jsonb, true)
WHERE (metadata->>'flow_id')::uuid = $1
  AND step_key = ANY($2)
  AND metadata @> '{"blocking_reason": "failed_parent"}'::jsonb
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
"""

#: THE FLOW RE-OPENS: a terminal-FAILED root returns to running (the
#: manual resume's own linearization; a CANCELLED root stays closed).
_RETRY_FLOW_REOPEN_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'running', finished_at = NULL
WHERE id = $1
  AND status = 'failed'
"""


async def wf_conn_fetchval(pool: asyncpg.Pool, schema: str, query: str, *args: object) -> Any:
    """One rendered statement's single value (the runner's ad-hoc read
    seam — the schema rendered via the estate's validator)."""
    async with pool.acquire() as conn:
        return await conn.fetchval(_stmt(query, schema), *args)


def _stmt(template: str, schema: str) -> str:
    return template.replace("{schema}", schema)


class FlowRunner:
    """The API's runner: create, drive, read. One engine, many surfaces —
    the runner OWNS no state; every fact is a row."""

    def __init__(self, compiled: Any, pool: asyncpg.Pool, schema: str) -> None:
        from taskq.workflows.api._validate import validate_compiled

        validate_compiled(compiled)  # a graph with errors does not run
        self.compiled = compiled
        self.pool = pool
        self.wsql: WorkflowSql = WorkflowSql.build(schema)
        self.schema = schema
        self._worker_id = JobId(new_uuid())
        # THE ROUTER'S RESOLUTION (T20): the workflow's declared chains —
        # a chain STEP row (fork-spawned or emitted) has no compiled
        # NodeDecl; its route resolves from HERE (the compiled chain),
        # its body from the registry (D1).
        self._chain_steps: dict[str, Any] = {
            step_key: chain for chain in getattr(compiled, "chains", ()) for step_key in chain.steps
        }

    # ── create ───────────────────────────────────────────────────────

    async def create_flow(
        self,
        *,
        input: object = None,
        run_key: str | None = None,
    ) -> JobId:
        """Create the run: the flow root (its payload CARRIES the input —
        cut #7) + every non-map node upfront (the join-wait rows among
        them, edges declared in the same call). The map joins are
        runtime-spawned by their source's fork (the engine's FORK
        ATOMICITY) — the wiring's downstream consumers ride the join's
        declared consumers (the outbox)."""
        claim: RunClaim = await self._insert_root(input, run_key)
        if not claim.created:
            return claim.flow_id
        async with self.pool.acquire() as conn:
            await self._insert_static_nodes(conn, claim.flow_id, input)
            # The run is LIVE: the root flips pending → running (the
            # maintenance leg's derivation owns the TERMINAL verdict
            # from the rows — the root is a cache, never the decider).
            await conn.execute(_stmt(_ROOT_START_SQL_TEMPLATE, self.schema), claim.flow_id)
        return claim.flow_id

    async def _insert_root(self, input: object, run_key: str | None) -> RunClaim:
        key = run_key or f"flow:{new_uuid()}"
        entry_payload: dict[str, object] = {_INPUT_KEY: input}
        async with self.pool.acquire() as conn:
            return await insert_flow_run(
                conn,
                self.wsql,
                entry=_FlowEntryShim(self.compiled.name, entry_payload),
                run_key=key,
            )

    async def _insert_static_nodes(self, conn: ConnLike, flow_id: JobId, input: object) -> None:
        for key in sorted(self.compiled.nodes):
            node = self.compiled.nodes[key]
            if node.kind == "map_join":
                continue  # spawned by the source's fork
            # THE MAP-JOIN'S DOWNSTREAM IS A STATIC ROW (the ecosystem
            # mapper's consumption defect's structural half): every
            # non-join node is inserted upfront — a node whose parent is
            # a fork-spawned map join carries the RESERVED dep (below),
            # so it can never dispatch before the join's terminal. (The
            # convicted shape: the descendant was NOT inserted and rode
            # only the outbox's side-channel — no edge row, an arg
            # resolution with no result to read, and a transitive
            # downstream that the side-channel never creates at all.)
            data_args = [v for kind, v in node.args if kind == "d"]
            payload: dict[str, object] = {_INPUT_KEY: input} if not node.parents else {}
            if data_args:
                payload[_WF_ARGS_KEY] = [_encode_data_arg(v) for v in data_args]
            spec = NodeSpec(
                flow_id=flow_id,
                step_key=key,
                actor=node.actor,
                queue=node.queue,
                payload=payload,
                parents=(),
                deps_pending=0,
                max_attempts=node.max_attempts,
            )
            await conn.execute(
                _stmt(NODE_INSERT_SQL, self.schema),
                new_uuid(),
                spec.actor,
                spec.queue,
                _jsonb(spec.payload),
                spec.max_attempts,
                "transient",
                None,
                None,
                spec.step_key,
                spec.deps_pending,
                None,
                _jsonb({"flow_id": str(flow_id)}),
                step_idempotency_scope(flow_id),
                step_idempotency_key(spec.step_key),
            )
        # The EDGES in a second pass (both endpoints exist).
        for key in sorted(self.compiled.nodes):
            node = self.compiled.nodes[key]
            if node.kind == "map_join":
                continue
            for parent_key in node.parents:
                parent_node = self.compiled.nodes.get(parent_key)
                parent_row = await conn.fetchval(
                    _stmt(_NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), parent_key, flow_id
                )
                child_row = await conn.fetchval(
                    _stmt(_NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), key, flow_id
                )
                if child_row is None:
                    continue  # the fork's outbox writes these edges at fire
                if parent_row is None:
                    if parent_node is not None and parent_node.kind == "map_join":
                        # THE MAP JOIN'S EDGE — the fork writes the edge
                        # row at the source's finalize (the join's id is
                        # minted there). The COUNTER IS RESERVED NOW: the
                        # consumer can never dispatch before the join's
                        # terminal (the consumption defect's dispatch-
                        # ordering half — the arg resolution must never
                        # run before the edge's terminal has a result).
                        await conn.execute(
                            _stmt(_INCREMENT_DEPS_SQL_TEMPLATE, self.schema), child_row
                        )
                    continue  # the map join's edge — the fork writes it
                await conn.execute(_stmt(_INCREMENT_DEPS_SQL_TEMPLATE, self.schema), child_row)
                await conn.execute(
                    _stmt(_EDGE_INSERT_SQL_TEMPLATE, self.schema),
                    child_row,
                    parent_row,
                    flow_id,
                    node.on_failure,
                )

    # ── drive ────────────────────────────────────────────────────────

    async def drive(
        self,
        flow_id: JobId,
        *,
        until: Literal["held", "terminal"] = "terminal",
        tick: float = 0.02,
        max_ticks: int = 5000,
    ) -> str:
        """The driver (cut #10): dispatch + sweeps until ``until`` —
        ``"terminal"`` (the flow row is terminal) or ``"held"`` (a node
        waits on a human / a deadline). BOUNDED: ``max_ticks`` caps the
        loop (a hang is a defect with no stack trace); the bound is
        tested by the timeout pins."""
        for _ in range(max_ticks):
            if await self._flow_status(flow_id) in ("succeeded", "failed", "cancelled"):
                return "terminal"
            if until == "held" and await self._any_held(flow_id):
                return "held"
            ran = await self.tick(flow_id)
            if not ran:
                await asyncio.sleep(tick)
        return "max_ticks"

    async def tick(self, flow_id: JobId) -> bool:
        """One dispatch pass: claim + run + finalize every claimable node;
        then the sweep arms (the crash-window heal + the outbox drain).
        Returns whether ANY node ran."""
        ran = False
        for row in await self._claimable(flow_id):
            ran = True
            await self._run_node(flow_id, row)
        from taskq.workflows._sweep import drain_outbox, sweep_join_rederive

        await sweep_join_rederive(self.pool, self.wsql)
        await drain_outbox(self.pool, self.wsql)
        return ran

    async def _claimable(self, flow_id: JobId) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(_stmt(_CLAIMABLE_NODES_SQL_TEMPLATE, self.schema), flow_id)
        return [dict(r) for r in rows]

    async def _run_node(self, flow_id: JobId, row: dict[str, Any]) -> None:
        node_key = row["step_key"]
        node = self.compiled.nodes.get(node_key)
        body = self._resolve_body(node_key, node)
        # THE CLAIM (two fenced writes, one grant of work): the JOB row
        # goes running under this worker (the attempt increments — the
        # fence's token), and the LEDGER claim inserts the attempt's
        # running row (T05's contract — the terminal write keys the
        # claim's own row; a skipped ledger claim strands the terminal).
        ledger_id: JobId | None = None
        async with self.pool.acquire() as conn:
            claimed = await conn.fetchval(
                _stmt(_NODE_CLAIM_SQL_TEMPLATE, self.schema), row["id"], self._worker_id
            )
            if claimed is not None:
                from taskq.workflows.ledger import claim_step_ledger

                claim = await claim_step_ledger(
                    conn,
                    self.wsql,
                    flow_id=flow_id,
                    job_id=JobId(row["id"]),
                    step_key=row["step_key"],
                    map_index=row["map_index"],
                    attempt=int(claimed),
                )
                ledger_id = claim.ledger_id
        if claimed is None:
            return  # someone else claimed it (or the flow died) — no error
        attempt = int(claimed)

        # THE AUTO PROJECTION — STARTED (T21 decision b): the claim seam's
        # additive write, class='auto', the ONE stream. Best-effort: a
        # lost projection is a logged freshness loss, never a node
        # failure (the asymmetry — the projection never runs inside the
        # claim's statements).
        await self._project_auto(
            flow_id,
            JobId(row["id"]),
            KIND_NODE_STARTED,
            {"step_key": row["step_key"], "attempt": attempt, "map_index": row["map_index"]},
        )

        # THE DISPATCH-TIME PREDICATE (cut #4): decided NOW, against the
        # flow's state — never at create time.
        if (
            node is not None
            and node.skip is not None
            and node.skip(await self._flow_state(flow_id))
        ):
            await self._finalize_skipped(flow_id, row, node)
            return

        # THE EMISSION OP's BUFFER (T21): the attempt's own emitter — the
        # declared progress schema rides the node's decl (a MAP CHILD
        # inherits its source's: the fork-spawned keys have no decl of
        # their own).
        emitter = ProgressEmitter(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            node_id=JobId(row["id"]),
            schema_decl=self._progress_schema(node_key, node),
        )

        # THE LOOP NODE (T19): the driver owns the node's lifecycle
        # (the iterations, the walls) — the caller's body path never
        # runs for it. The emitter rides in; the driver's ctx gets it,
        # and the close is THIS call's finally (bounded, best-effort —
        # the driver's internal finalizes never wait on the buffer).
        if node is not None and node.loop_spec is not None:
            try:
                await self._run_loop_node(flow_id, row, attempt, node, ledger_id, emitter)
            finally:
                await emitter.aclose()
            return

        parents_ordered, parents_by_key = await self._parent_results(row["id"])
        payload_raw = row["payload"]
        payload = _json_loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
        assert isinstance(payload, dict)  # the Any-contract walk (the seed wrote the shape)
        payload_doc: dict[str, object] = payload  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk's boundary — the seed wrote the shape; the assert is the runtime check.
        args = self._resolve_args(node, body, parents_by_key, payload_doc)

        ctx = StepContext(
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            node_key=node_key,
            attempt=attempt,
            input=await self._flow_input(flow_id),
            _pool=self.pool,
            _wsql=self.wsql,
            _map_index=row["map_index"],
            _ledger_id=ledger_id,
            _worker_id=self._worker_id,
            _workflow_name=self.compiled.name,
            _redact=self._redact_hook(),
            _progress=emitter,
        )
        try:
            if body is None:
                # THE DEFAULT IDENTITY PACKER (the join/gather kinds): the
                # join's result IS the decoded parents' list — the flat
                # shape the downstream body's list param declares; the
                # EDGE ORDER (map children distinct, the gather's wiring
                # order).
                result: dict[str, object] | None = {"value": [r for _key, r in parents_ordered]}
            else:
                outcome_value = await body(ctx, *args)
                chain = self._chain_steps.get(node_key)
                if chain is not None:
                    # THE CHAIN STEP (T20): the body's typed outcome IS
                    # the router's decision — the finalize routes it (at
                    # most one forked child, no join). The engine's
                    # fork-at-finalize machinery owns the child row; the
                    # ROUTE is this runner's decision.
                    await self._finalize_chain_step(
                        flow_id, row, attempt, node_key, chain, outcome_value, payload_doc
                    )
                    return
                if isinstance(outcome_value, Exit):
                    # THE TYPED EARLY-EXIT (§17.1): the sentinel ends the
                    # node NOW — terminal-succeed with the typed payload,
                    # the downstream graph marked skipped-with-the-record.
                    exit_value = cast("Exit[object]", outcome_value)  # pyright: ignore[reportUnknownArgumentType]  # Why: the body's object-typed return — the isinstance guard IS the runtime shape check; the sentinel's payload is the walk's boundary.
                    await self._finalize_exit(flow_id, row, attempt, node, exit_value)
                    return
                result = _encode_result(outcome_value)
        except _NodeHeld as held:
            # THE HOLD (T10): the node rests in the held representation
            # (pending + the deadline + the signal row as truth) — NO
            # terminal, NO ledger failure; the resume re-executes the
            # body from the top. The runner's tick reports it ran. The
            # buffer's final flush runs FIRST (bounded, best-effort —
            # the hold is not a terminal; the progress survives).
            await emitter.aclose()
            logger.info(
                "node.held",
                run_id=str(flow_id),
                node=node_key,
                hold_id=held.hold_id,
                signals=list(held.signal_names),
            )
            return
        except Exception as exc:  # Why: the ladder's boundary — ANY body failure routes through the retry classification.
            # THE BUFFER CLOSES BEFORE THE LADDER (the asymmetry's ordering
            # law): the final flush is bounded and best-effort — it can
            # cost freshness, never the ladder's correctness.
            await emitter.aclose()
            await self._ladder_or_fail(flow_id, row, attempt, node, exc)
            return
        await emitter.aclose()
        await self._finalize_success(flow_id, row, attempt, node, result)

    def _redact_hook(self) -> Callable[[str], str] | None:
        """The workflow's OWN redact hook from the REGISTERED DEFINITION
        (attack-3 H3's cure — nothing wired the workflow's hook into the
        hold surface): the hold context's chain-then-hook composition
        reads it from here (the registry is the source, D1)."""
        from taskq.workflows.definitions import get_registry

        try:
            return get_registry().get(self.compiled.name).redact
        except KeyError:
            return None

    def _progress_schema(self, node_key: str, node: Any) -> type[BaseModel] | None:
        """The node's DECLARED progress payload schema (T21 decision a):
        the node's own decl; a MAP CHILD (the fork-spawned ``<src>.item``
        keys — no decl of their own) inherits its SOURCE's declaration."""
        if node is not None and getattr(node, "progress_schema", None) is not None:
            return node.progress_schema
        if node_key.endswith(".item"):
            source = self.compiled.nodes.get(node_key[: -len(".item")])
            if source is not None and source.progress_schema is not None:
                return source.progress_schema
        return None

    def _resolve_body(self, node_key: str, node: Any) -> Any:
        """D1: the body resolves from the REGISTERED DEFINITION — never
        from a per-call map (the double-dispatch dragon's cure). A node
        that declares NO body (the join/gather kinds) returns ``None`` —
        the runner's default IDENTITY packer (the join's result IS the
        decoded parents' list). The registry answers FIRST: the
        fork-spawned keys (``<src>.item``, the outbox consumers) have no
        compiled NodeDecl — their bodies live in the definition only."""
        from taskq.workflows.definitions import resolve_step_body

        try:
            return resolve_step_body(self.compiled.name, node_key)
        except KeyError:
            pass
        if node is None:
            raise WorkflowRunError(
                f"step {node_key!r} is not in workflow "
                f"{self.compiled.name!r}'s compiled graph nor in its "
                "registered definition — a foreign step key (the registry "
                "is the only body source, D1)"
            )
        return node.body

    async def _parent_results(
        self, node_id: Any
    ) -> tuple[list[tuple[str, object]], dict[str, object]]:
        """The parents' decoded results (decoded ONCE, cut #14): the
        ORDERED list — edge order (uuid7 = wiring creation order), the
        default packer's input, map children DISTINCT (they share one
        step key) — and the step-key dict for the NAMED-arg lookup (a
        named arg can never reference a map child: it references the
        node that produced the list, or the map's join)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                _stmt(_PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, self.schema), node_id
            )
        ordered: list[tuple[str, object]] = []
        for r in rows:
            result: object = _decode_result(r["result"])  # pyright: ignore[reportUnknownVariableType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
            key: str = r["step_key"]  # pyright: ignore[reportUnknownVariableType]  # Why: same walk.
            ordered.append((key, result))
        return ordered, dict(ordered)

    def _resolve_args(
        self,
        node: Any,
        body: Any,
        parent_results: dict[str, object],
        payload: dict[str, object],
    ) -> tuple[object, ...]:
        """THE WIRING RULE: argument order is signature order. A promise
        position resolves to its parent's decoded result; a data position
        to the value RE-VALIDATED into its declared model (cut #8's typed
        boundary: the body sees the TYPE it declared, never a raw dict —
        the payload codec hook). The map child's item key is the fork's
        single-arg form."""
        if _ITEM_KEY in payload:
            item_raw: object = payload[_ITEM_KEY]  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk — the fork wrote the shape.
            return (self._coerce(item_raw, node, body, 0),)
        if node is None:
            # THE OUTBOX CONSUMER with no compiled NodeDecl (the
            # escalation step — attack-3 H1's cure): its declared
            # payload rides the row as data args (the bindings'
            # ``wf_args``), the body's OWN contract with the enqueue's
            # context. No parents exist for such a consumer — the
            # parent-results walk has nothing to say.
            wf_args_raw: object = payload.get(_WF_ARGS_KEY, [])
            if wf_args_raw:
                return tuple(cast(list[object], wf_args_raw))
            return tuple(parent_results.values())
        if not node.args:
            return tuple(parent_results.values())
        wf_args_raw: object = payload.get(_WF_ARGS_KEY, [])
        wf_args = cast(list[object], wf_args_raw)  # the seed wrote the list
        data_iter = iter(wf_args)
        parts: list[object] = []
        for position, (kind, value) in enumerate(node.args):
            if kind == "p":
                parent_result = parent_results.get(value)
                assert parent_result is not None, (
                    f"the parent {value!r} has no result yet — the arg "
                    "resolution ran before the edge's terminal (a dispatch bug)"
                )
                parts.append(self._coerce(parent_result, node, body, position))
            else:
                parts.append(self._coerce(_next_data(data_iter), node, body, position))
        return tuple(parts)

    def _coerce(self, raw: object, node: Any, body: Any, position: int) -> object:
        """The payload codec hook: the body's declared param annotation —
        a pydantic model — re-validates the jsonb round-trip's dict (the
        decode once, typed end to end)."""
        if body is None:
            return raw
        if not isinstance(raw, dict):
            return raw  # the non-dict JSON values need no codec
        from taskq.workflows.api._hints import body_hints

        hints = body_hints(body)
        params = [v for k, v in hints.items() if k not in ("return", "ctx")]
        if position >= len(params):
            return cast(object, raw)  # the narrowing's laundering — the declared return is object
        param = params[position]
        raw_doc = cast(dict[str, object], raw)  # the codec's declared input
        if isinstance(param, type) and issubclass(param, BaseModel):
            return param.model_validate(raw_doc)
        return cast(object, raw)  # the narrowing's laundering

    async def cancel_workflow(
        self,
        flow_id: JobId,
        *,
        reason: str | None = None,
        principal: Any = None,
    ) -> int:
        """P3 rule 4's cancel (T10): ONE transaction — the flow flip is
        the linearization point; every non-terminal node + held signal
        resolves in the same snapshot; running children take the
        existing two-phase cooperative cancel; terminal rows untouched;
        IDEMPOTENT (cancel twice = one cancel — a terminal root updates
        nothing). THE AUDIT (G4): "who cancelled this" is a ROW, not a
        log line."""
        from taskq.workflows.api._hitl import cancel_run_signals

        async with self.pool.acquire() as conn, conn.transaction():
            flipped = await conn.fetchval(
                _stmt(_CANCEL_ROOT_SQL_TEMPLATE, self.schema),
                flow_id,
                (reason or "cancel_workflow")[:500],
            )
            if flipped is None:
                return 0  # already terminal — idempotent
            await conn.execute(_stmt(_CANCEL_NODES_SQL_TEMPLATE, self.schema), flow_id)
            held = await cancel_run_signals(self.pool, schema=self.schema, workflow_id=flow_id)
            # THE AUDIT ROW (the caller owns the tx — the same-tx
            # guarantee; the lazy import keeps the layering).
            from taskq.web.admin._audit import record_admin_action

            await record_admin_action(
                conn,
                schema=self.schema,
                principal=principal,
                action="workflow.cancel",
                target_type="workflow_run",
                target_id=str(flow_id),
                reason=reason,
                detail={"held_signals_cancelled": held},
            )
        return held + 1

    async def _finalize_chain_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        step_key: str,
        chain: Any,
        outcome: object,
        payload_doc: dict[str, object],
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
                claim_epoch=0,
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
            claim_epoch=0,
            outcome="succeeded",
            result=_encode_result(outcome),
            fork=fork,
            map_index=row["map_index"],
        )

    async def _finalize_success(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        result: dict[str, object] | None,
        emitter: ProgressEmitter | None = None,
    ) -> None:
        fork: ForkSpec | None = None
        if node is not None and node.map_item is not None:
            fork = self._map_fork(row, node, result)
        final = await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=row["step_key"],
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=0,
            outcome="succeeded",
            result=result,
            fork=fork,
            map_index=row["map_index"],
        )
        # THE AUTO PROJECTION — TERMINAL (T21 decision b): the finalize
        # seam's additive write, AFTER the two-tx finalize returned (the
        # projection is never inside the finalize's transactions — the
        # zero-finalize-changes probe pins the terminal-mark statement's
        # blindness to the progress substrate). The LEDGER owns the
        # state; the projection only publishes it to the stream.
        if final.applied:
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {"outcome": "succeeded", "step_key": row["step_key"]},
            )

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None:
        """One auto-class projection, best-effort (T21 decision e): a lost
        projection is a FRESHNESS loss — counted and warned, never raised
        into the node's path."""
        try:
            await project_auto_event(
                self.pool, self.wsql, flow_id=flow_id, node_id=node_id, kind=kind, payload=payload
            )
        except (
            Exception
        ) as exc:  # Why: the asymmetry — the projection degrades first, never correctness.
            logger.warning(
                "progress_projection_lost",
                kind="progress_projection_lost",
                run_id=str(flow_id),
                node=str(node_id),
                projection=kind,
                error=str(exc)[:200],
            )

    def _map_fork(
        self, row: dict[str, Any], node: Any, result: dict[str, object] | None
    ) -> ForkSpec | None:
        """The map source's finalize fork: N children (the item body,
        one per list element — fresh jobs, per-item ledger identity) +
        the join whose consumers are the wiring's downstream nodes (the
        outbox carries the consumer bindings — the cascade, cut #1's
        cure). ``None`` when the body returned no list (the map over
        nothing — the join fires empty, the collect states it)."""
        from taskq.workflows._types import ConsumerBinding

        raw_items = result.get("value") if result is not None else None
        items: list[object] = (
            list(raw_items) if isinstance(raw_items, list) else []  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk — the envelope's value is the body's own return, jsonb-round-tripped.
        )
        join_key = f"{row['step_key']}.join"
        bindings = tuple(
            ConsumerBinding(
                step_key=downstream,
                actor=self.compiled.nodes[downstream].actor,
                queue=self.compiled.nodes[downstream].queue,
                payload={},
                # THE CONSUMER'S OWN EDGE POLICY (the map-join consumption
                # cure): the fork writes the join→consumer edge with it —
                # T06's propagation reads the policy off the ledger when
                # the JOIN terminal-fails.
                failure_policy=self.compiled.nodes[downstream].on_failure,
            )
            for downstream in sorted(self.compiled.nodes)
            if join_key in self.compiled.nodes[downstream].parents
        )
        return ForkSpec(
            children=tuple(
                ChildSpec(
                    step_key=f"{row['step_key']}.item",
                    actor=node.actor,
                    queue=node.map_queue,
                    payload={_ITEM_KEY: _encode_data_arg(item)},
                    map_index=i,
                )
                for i, item in enumerate(items)
            ),
            join=JoinSpec(
                step_key=join_key,
                actor=node.actor,
                queue=node.map_queue,
                consumers=bindings,
                failure_policy=node.map_on_failure,  # type: ignore[arg-type]  # Why: the policy vocabulary is the compile's own — validated by the engine's validators.
            ),
            max_attempts=node.map_max_attempts,
        )

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
            return await self._drive_loop(
                flow_id, row, attempt, node, spec, ledger_id, emitter
            )
        except Exception as exc:  # Why: the MACHINERY boundary — the classifier reads WHERE the error escaped, never its type (attack-3 H5's cure). BLE001 is the boundary's shape: ANY machinery exception is classified, then re-raised or reclaimed.
            if _is_infra_fault(exc):
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
            initial_carry = _jsonable(cast(object, spec.carry_type))
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
                    _json_dumps(init_meta),
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
                    flow_id, row, attempt, node, _encode_result(carry), emitter=emitter
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
                        flow_id, row, attempt, node, _encode_result(payload), emitter=emitter
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
                loop_ctx = StepContext(
                    flow_id=flow_id,
                    job_id=JobId(row["id"]),
                    node_key=row["step_key"],
                    attempt=attempt,
                    input=await self._flow_input(flow_id),
                    _pool=self.pool,
                    _wsql=self.wsql,
                    _is_loop=True,
                    _ledger_id=ledger_id,
                    _workflow_name=self.compiled.name,
                    _redact=self._redact_hook(),
                    _progress=emitter,
                )
                try:
                    outcome = await node.loop_body(loop_ctx, carry)
                except _NodeHeld:
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
                        flow_id, row, attempt, node, _encode_result(done_payload), emitter=emitter
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
                    _json_dumps({"carry": _jsonable(carry), "iteration": iteration + 1}),
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
            payload = {"done": True, "payload": _jsonable(cast(object, outcome.payload))}
        elif isinstance(outcome, Refine):
            payload = {"done": False, "feedback": _jsonable(cast(object, outcome.feedback))}
        else:
            payload = {"done": False, "feedback": _jsonable(outcome)}
        del kind
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                iter_key,
                attempt,
                "succeeded",
                _json_dumps(payload),
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
                _stmt(_NODE_REPEND_SQL_TEMPLATE, self.schema),
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
                _json_dumps(bindings),
            )

    async def _ladder_or_fail(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        exc: Exception,
    ) -> None:
        """The ladder: an attempt failure emits NO terminal (P3 rule 7) —
        the node re-pends with backoff until ``max_attempts``, THEN
        terminal-fails (T06's propagation takes over). ``permanent`` retry
        kinds fail immediately (cut #12's classifier knob)."""
        max_attempts = node.max_attempts if node is not None else 3
        retry_kind = node.retry_kind if node is not None else "transient"
        # RESUME-NOT-RETRY (cut #5): the LADDER counts the ledger's
        # 'failed' rows — the claim's attempt ordinal increments on EVERY
        # claim (holds' resumes included), so a hold-heavy node keeps its
        # full retry curve (the shared-counter variant — 2 holds +
        # max_attempts=3 = terminal failure with ZERO retries — is pin
        # 7's RED forever).
        failed_count = int(
            await wf_conn_fetchval(
                self.pool,
                self.schema,
                "SELECT count(*) FROM {schema}.wf_step_ledger WHERE flow_id = $1 "
                "AND step_key = $2 AND COALESCE(map_index, -1) = COALESCE($3::smallint, -1) "
                "AND status = 'failed'",
                flow_id,
                row["step_key"],
                row["map_index"],
            )
        )
        # The attempt's OWN ledger row terminalizes 'failed' (the claim
        # inserted it — the arbiter's key is the attempt's identity): an
        # INSERT here would collide with the claim (the UniqueViolation
        # the runner pins convict). Ladder retries emit NO terminal on
        # the JOB row (P3 rule 7) — the node re-pends with backoff.
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                row["step_key"],
                attempt,
                "failed",
                None,
                type(exc).__name__,
                str(exc)[:500],
                None,
                row["map_index"],
            )
            if retry_kind != "permanent" and failed_count + 1 < max_attempts:
                await conn.execute(
                    _stmt(_NODE_REPEND_SQL_TEMPLATE, self.schema),
                    row["id"],
                    0.05 * (2 ** (failed_count)),
                )
                return
        final = await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=row["step_key"],
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=0,
            outcome="failed",
            error_class=type(exc).__name__,
            error_message=str(exc)[:MESSAGE_TERMINAL_MAX],
            map_index=row["map_index"],
        )
        if final.applied:
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {
                    "outcome": "failed",
                    "step_key": row["step_key"],
                    "error_class": type(exc).__name__,
                },
            )

    # ── the typed early-exit + the manual retry ──────────────────────

    async def _finalize_exit(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        exit_value: Exit[object],
    ) -> None:
        """THE TYPED EARLY-EXIT (§17.1, attack-audit's Missing #1): the
        body returned ``Exit(payload)`` — the node TERMINAL-SUCCEEDS
        with the typed payload (the envelope records the exit — it never
        lies about what ran), and every non-terminal DOWNSTREAM node is
        marked SKIPPED-WITH-THE-RECORD (``{"skipped": true,
        "exit_from": <node>}`` — zero ledger rows, a skip is not an
        attempt; the join-firing machinery never runs for rows the exit
        resolved). All rows terminal → the derivation reports the
        workflow COMPLETE. The LEDGER records the exit on the exited
        node's own terminal row (the result carries the marker)."""
        step_key: str = row["step_key"]
        await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=step_key,
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=0,
            outcome="succeeded",
            result={"value": _jsonable(exit_value.payload), "exit": True},
            map_index=row["map_index"],
        )
        # THE DOWNSTREAM MARK: the compiled graph's descendants (the
        # wiring's own edges — never a runtime guess), skipped with the
        # record. Bounded by the graph's size; a downstream row that is
        # already terminal (or does not exist — an unspawned map join)
        # is untouched.
        descendants = self._descendants_of(step_key)
        if descendants:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    _stmt(_EXIT_SKIP_SQL_TEMPLATE, self.schema),
                    flow_id,
                    _jsonb({"skipped": True, "exit_from": step_key}),
                    sorted(descendants),
                )

    def _descendants_of(self, node_key: str) -> set[str]:
        """The compiled graph's transitive downstream of one node (the
        wiring's children adjacency — the exit's marked set)."""
        children: dict[str, list[str]] = {}
        for key, node in self.compiled.nodes.items():
            for parent in node.parents:
                children.setdefault(parent, []).append(key)
        seen: set[str] = set()
        frontier = [node_key]
        while frontier:
            current = frontier.pop()
            for child in children.get(current, ()):
                if child not in seen:
                    seen.add(child)
                    frontier.append(child)
        return seen

    async def retry_node(
        self,
        flow_id: JobId,
        node_key: str,
        *,
        principal: Any = None,
        reason: str | None = None,
    ) -> bool:
        """THE MANUAL RESUME (§17.2, the §22.6 redispatch row 3 — "the
        ladder then manual"; attack-audit's Missing #1): the operator's
        audited, CAS-guarded re-arm of a TERMINAL-FAILED node's ladder.

        * THE ATTEMPT ORDINAL CONTINUES — never resets: the claim's
          counter is untouched, the ladder's 'failed' ledger rows are the
          count, so each manual retry buys EXACTLY ONE more attempt (a
          failure re-fails the node; the operator retries again — the
          manual arm is the ladder's continuation, not a fresh budget).
        * SAFE BY THE STEP LEDGER (T05): the re-run's ``ctx.step`` calls
          return the recorded results — the replay is the ledger's own
          contract.
        * THE BLOCKED CLOSURE RE-OPENS: the failed node's downstream
          rows the cascade BLOCKED (``blocking_reason='failed_parent'``)
          return to join-wait (``'join'``) — the sweep's re-derive
          re-derives them from the edge ledger on the next pass (the
          counter-as-cache law; a stamp is the cache, never the truth).
        * THE FLOW RE-OPENS: a terminal-FAILED flow root returns to
          ``running`` (the manual resume's own linearization — a
          cancelled flow stays closed: the operator's cancel is
          deliberate, the resume does not second-guess it).
        * THE AUDIT (G4): "who retried this" is a ROW.

        Returns ``True`` when the CAS granted the re-arm. Surfaced as
        the ``taskq flows retry`` verb (T12's ticket)."""
        from taskq.web.admin._audit import record_admin_action

        descendants = sorted(self._descendants_of(node_key))
        async with self.pool.acquire() as conn, conn.transaction():
            # THE NODE'S CAS (one grant): terminal-FAILED → pending; the
            # attempt ordinal untouched (CONTINUES — the ladder's own
            # count is the budget).
            node_id = await conn.fetchval(
                _stmt(_RETRY_NODE_CAS_SQL_TEMPLATE, self.schema),
                flow_id,
                node_key,
            )
            if node_id is None:
                return False
            # THE CLOSURE RE-OPENS: the blocked downstream rows return
            # to join-wait (the re-derive re-derives them — a stamp is
            # the cache).
            if descendants:
                await conn.execute(
                    _stmt(_RETRY_REOPEN_CLOSURE_SQL_TEMPLATE, self.schema),
                    flow_id,
                    descendants,
                )
            # THE FLOW RE-OPENS: a terminal-FAILED root → running (the
            # maintenance leg owns the verdict from the rows again).
            await conn.execute(_stmt(_RETRY_FLOW_REOPEN_SQL_TEMPLATE, self.schema), flow_id)
            await record_admin_action(
                conn,
                schema=self.schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{flow_id}:{node_key}",
                reason=reason,
                detail={"node_key": node_key, "closure_reopened": len(descendants)},
            )
        return True

    async def _finalize_skipped(self, flow_id: JobId, row: dict[str, Any], node: Any) -> None:
        """The v1 SKIP semantics: the node succeeds WITH the skip record
        (the envelope never lies about what ran); its ABSORBING joins
        (collect | maybe) receive the typed skip item — a skip is not an
        attempt (zero ledger rows)."""
        async with self.pool.acquire() as conn:
            await fan_in_skip(
                conn,
                self.wsql,
                flow_id=flow_id,
                parent_id=JobId(row["id"]),
                step_key=row["step_key"],
                map_index=row["map_index"],
            )
        await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=row["step_key"],
            worker_id=self._worker_id,
            attempt=int(row["attempt"]) + 1,
            claim_epoch=0,
            outcome="succeeded",
            result=dict(_SKIPPED_RESULT),
            map_index=row["map_index"],
        )
        await self._project_auto(
            flow_id,
            JobId(row["id"]),
            KIND_NODE_TERMINAL,
            {"outcome": "succeeded", "step_key": row["step_key"], "skipped": True},
        )

    # ── reads ────────────────────────────────────────────────────────

    async def result(self, flow_id: JobId) -> object:
        """The flow's answer (cut #19): the terminal node's result,
        DECODED — never a raw jsonb string. ``None`` before the terminal
        node finalizes."""
        terminal = self.compiled.terminal
        if terminal is None:
            raise WorkflowRunError(
                f"workflow {self.compiled.name!r} names no terminal — "
                "wire one (wf.build(p)) to read a result"
            )
        async with self.pool.acquire() as conn:
            raw = await conn.fetchval(
                _stmt(_TERMINAL_RESULT_SQL_TEMPLATE, self.schema), terminal, flow_id
            )
        return _decode_result(raw)

    async def _flow_status(self, flow_id: JobId) -> str:
        async with self.pool.acquire() as conn:
            status = await conn.fetchval(_stmt(_FLOW_STATUS_SQL_TEMPLATE, self.schema), flow_id)
        return status or "missing"

    async def _flow_input(self, flow_id: JobId) -> object:
        async with self.pool.acquire() as conn:
            raw = await conn.fetchval(_stmt(_FLOW_PAYLOAD_SQL_TEMPLATE, self.schema), flow_id)
        decoded = _decode_result(raw)
        if isinstance(decoded, dict) and _INPUT_KEY in decoded:
            return cast(object, decoded[_INPUT_KEY])  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk's boundary — the flow input's key is the runner's own.
        return None

    async def _flow_state(self, flow_id: JobId) -> dict[str, object]:
        """The dispatch-time predicate's view (cut #4): the input + the
        completed results so far — the state a sibling-reading guard
        needs."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(_stmt(_SUCCEEDED_RESULTS_SQL_TEMPLATE, self.schema), flow_id)
        return {
            "input": await self._flow_input(flow_id),
            "results": {
                r["step_key"]: _decode_result(r["result"])  # pyright: ignore[reportUnknownArgumentType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
                for r in rows
            },
        }

    async def _any_held(self, flow_id: JobId) -> bool:
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(_stmt(_HELD_COUNT_SQL_TEMPLATE, self.schema), flow_id)
        return bool(count)


@dataclass(slots=True)
class _FlowEntryShim:
    """The FlowEntry shape the run-key claim needs, built from the
    compiled workflow (the typed door's runtime form). Mutable: the
    protocol's members are writable (the estate's carrier convention)."""

    name: str
    payload: dict[str, object] | str | None
    actor: str = "wf"
    queue: str = "default"
    max_attempts: int = 3
    retry_kind: str = "transient"
    trace_id: str | None = None


def _next_data(iterator: Any) -> object:
    """One data-arg position's value (the payload's wf_args array is the
    wiring's recorded data, in order)."""
    return cast(object, next(iterator))


def _encode_data_arg(value: object) -> object:
    """The jsonb-safe form of one wiring value (a pydantic model dumps
    through its own codec — the typed boundary; lists and dicts walk).
    Everything else is already a JSON value by the wiring's contract."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        items = cast(list[object], value)  # the walk's declared members
        return [_encode_data_arg(v) for v in items]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)  # the walk's declared members
        walked_map: dict[str, object] = {}
        for k, v in mapping.items():
            walked_map[str(k)] = _encode_data_arg(v)
        return walked_map
    return value


def _json_dumps(value: object) -> str:
    """The metadata round-trip's write side (the estate's dumps)."""
    from taskq._json import dumps_jsonb_str

    return dumps_jsonb_str(value)


def _jsonable(value: object) -> object:
    """The carry/feedback's jsonb-safe form (the walk's boundary)."""
    return _encode_data_arg(value)


def _is_infra_fault(exc: BaseException) -> bool:
    """THE LADDER-ROUTES-BY-FAILURE-CLASS classifier (T19's semantics
    decision, stated once) — attack-3 H5's cure sharpened it to the
    ESCAPE-POINT contract: the classifier reads WHERE the error escaped,
    never merely its type. It is consulted ONLY at the MACHINERY
    boundary (a driver/sweep/client statement failed — the loop's own
    state reads, the ledger claims, the advance, the exhaust): a
    RECLAIM-ELIGIBLE transport fault (connection loss, admin shutdown,
    interface failure — the storm's ConnectionDoesNotExist class) routes
    to RECLAIM and NEVER burns the retry ladder; the vanilla lease
    machinery re-claims from the ledger — the ledger row says 'crashed',
    the ladder counts 'failed'.

    The BODY boundary NEVER consults this function: an exception the
    BODY raised is a body failure by WHERE it escaped — a body-raised
    ``ConnectionError`` exhausts the loop as the body failure it is (the
    body cannot forge an infra fault; the STRANDED-FLOW wedge — 20
    crashed rows, a ``running`` flow forever — is the convicted variant,
    kept red by the attack probe)."""
    import asyncpg as _asyncpg

    infra: tuple[type[BaseException], ...] = (
        _asyncpg.exceptions.ConnectionDoesNotExistError,
        _asyncpg.exceptions.InterfaceError,
        _asyncpg.exceptions.AdminShutdownError,
        _asyncpg.exceptions.CannotConnectNowError,
        ConnectionError,
    )
    return isinstance(exc, infra)


def _encode_result(value: object) -> dict[str, object] | None:
    """The result envelope: the body's return rides the row's jsonb
    ``result`` (wrapped so a bare dict body return never collides with
    the engine's envelope keys). The value walks the typed boundary (a
    body may return a list of models — the map's shape)."""
    if value is None:
        return None
    return {"value": _encode_data_arg(value)}


def _decode_result(raw: object) -> object:
    """jsonb decoded ONCE (cut #14): asyncpg returns ``str`` on un-coded
    connections — parse through the estate's seam, never per call site.
    The runner's own envelope (a bare ``{"value": …}``) unwraps — a body's
    dict return never collides with the envelope keys."""
    if raw is None:
        return None
    # The Any-contract walk (the _json seam's parse contract — the same
    # house style _types.py's decoders use): every branch asserts the
    # runtime shape it consumes, and each USE of an Unknown member carries
    # the targeted ignore with the Why.
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    if "value" in decoded and len(decoded) == 1:  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk.
        return cast(object, decoded["value"])  # pyright: ignore[reportUnknownArgumentType]  # Why: the walk's boundary — the cast IS the declared laundering (the envelope's single key is the runner's own).
    return cast(object, decoded)  # pyright: ignore[reportUnknownArgumentType]  # Why: the same boundary.

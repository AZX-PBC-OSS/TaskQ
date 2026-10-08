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
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sql_finalize import NODE_INSERT_SQL
from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec, NodeSpec, _jsonb
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
        a support ticket")."""
        models: tuple[type[BaseModel], ...] = cast(
            tuple[type[BaseModel], ...],
            signals if isinstance(signals, tuple) else (signals,),
        )
        names = tuple(m.__name__ for m in models)
        from taskq.workflows.api._hitl import mark_awaited, register_hold

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
                return self._coerce_signal(models, payload)
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
                signal_name=names[0] if len(names) == 1 else "|".join(names),
                hold_epoch=int(epoch),  # pyright: ignore[reportAny, reportArgumentType]
                call_id=f"call:{new_uuid()}",
                payload_schema=schema_ref,
                timeout_s=timeout_s,
                is_loop_node=self._is_loop,
            )
            if reason or tool or args:
                await conn.execute(
                    _stmt(
                        "UPDATE {schema}.wf_signals SET payload = $2::jsonb WHERE id = $1",
                        self._wsql.schema,
                    ),
                    hold_id,
                    dumps_jsonb_str({"reason": reason, "tool": tool, "args": args}),
                )
        raise _NodeHeld(hold_id=str(hold_id), signal_names=names)

    @staticmethod
    def _coerce_signal(models: Any, payload: Any) -> Any:
        """The delivered payload re-validates into the declared model
        (the typed wait's return — narrows with ``isinstance``)."""
        if not isinstance(payload, dict):
            return payload
        payload_doc: dict[str, object] = payload  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk — the delivered payload is the row's jsonb; the isinstance guard above is the runtime shape check.
        for m in models:
            if isinstance(m, type) and issubclass(m, BaseModel):
                return cast(Any, m.model_validate(payload_doc))
        return cast(Any, payload)

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

_EDGE_INSERT_SQL_TEMPLATE = """
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
VALUES ($1, $2, $3, $4)
"""

_INCREMENT_DEPS_SQL_TEMPLATE = """
UPDATE {schema}.jobs SET deps_pending = deps_pending + 1 WHERE id = $1
"""

_CLAIMABLE_NODES_SQL_TEMPLATE = """
SELECT id, step_key, map_index, attempt, payload FROM {schema}.jobs
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
  AND scheduled_at > now()
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
            if any(
                self.compiled.nodes[p].kind == "map_join"
                for p in node.parents
                if p in self.compiled.nodes
            ):
                # A downstream of a MAP JOIN spawns from the fork's
                # outbox (the join row exists only after the source's
                # finalize) — inserted upfront it would dispatch BEFORE
                # its parent exists (the stranded-edge lie).
                continue
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
            if any(
                self.compiled.nodes[p].kind == "map_join"
                for p in node.parents
                if p in self.compiled.nodes
            ):
                continue  # the fork's outbox writes these edges at fire
            for parent_key in node.parents:
                parent_row = await conn.fetchval(
                    _stmt(_NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), parent_key, flow_id
                )
                child_row = await conn.fetchval(
                    _stmt(_NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), key, flow_id
                )
                if parent_row is None:
                    continue  # the map join's edge — the fork writes it
                if child_row is not None:
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

        # THE DISPATCH-TIME PREDICATE (cut #4): decided NOW, against the
        # flow's state — never at create time.
        if (
            node is not None
            and node.skip is not None
            and node.skip(await self._flow_state(flow_id))
        ):
            await self._finalize_skipped(flow_id, row, node)
            return

        # THE LOOP NODE (T19): the driver owns the node's lifecycle
        # (the iterations, the walls) — the caller's body path never
        # runs for it.
        if node is not None and node.loop_spec is not None:
            await self._run_loop_node(flow_id, row, attempt, node, ledger_id)
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
                result = _encode_result(outcome_value)
        except _NodeHeld as held:
            # THE HOLD (T10): the node rests in the held representation
            # (pending + the deadline + the signal row as truth) — NO
            # terminal, NO ledger failure; the resume re-executes the
            # body from the top. The runner's tick reports it ran.
            logger.info(
                "node.held",
                run_id=str(flow_id),
                node=node_key,
                hold_id=held.hold_id,
                signals=list(held.signal_names),
            )
            return
        except Exception as exc:  # Why: the ladder's boundary — ANY body failure routes through the retry classification.
            await self._ladder_or_fail(flow_id, row, attempt, node, exc)
            return
        await self._finalize_success(flow_id, row, attempt, node, result)

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
        if node is None or not node.args:
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

    async def _finalize_success(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        result: dict[str, object] | None,
    ) -> None:
        fork: ForkSpec | None = None
        if node is not None and node.map_item is not None:
            fork = self._map_fork(row, node, result)
        await finalize_node(
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
        read of the wall (holds are free)."""
        from taskq._json import loads as _loads
        from taskq.workflows.api._loop import Done, LoopSpec, Refine
        from taskq.workflows.api._sql_loop import (
            LOOP_ADVANCE_SQL,
            LOOP_ERROR_BODY,
            LOOP_INIT_SQL,
            LOOP_NODE_STATE_SQL,
            LOOP_REMAINING_SQL,
            render_loop_sql,
        )
        from taskq.workflows.ledger import claim_step_ledger, memoized_step_result

        spec = cast("LoopSpec", node.loop_spec)  # the driver's own declaration

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
        if "iteration" not in meta:
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
                await self._finalize_success(flow_id, row, attempt, node, _encode_result(carry))
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
                        flow_id, row, attempt, node, _encode_result(payload)
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
                )
                try:
                    outcome = await node.loop_body(loop_ctx, carry)
                except _NodeHeld:
                    # THE HOLD INSIDE THE ITERATION (T10 × T19's
                    # composition): the loop node rests in the held
                    # representation (the budget PAUSED — register_hold's
                    # loop case); the resume re-runs the iteration (the
                    # ledger's 'awaited' row — the ladder unburned). The
                    # driver's walls stay blind while paused (the arm's
                    # heart) — holds are free.
                    return
                except Exception as exc:
                    # THE LADDER-ROUTES-BY-FAILURE-CLASS decision: an INFRA
                    # fault (reclaim-eligible) records 'crashed' and re-pends
                    # WITHOUT burning the ladder (the vanilla lease machinery
                    # re-claims from the ledger); a BODY failure is the
                    # loop's typed failure (the named class, the flow
                    # terminalized in the same tx — STRANDED-FLOW's
                    # body-failure sibling).
                    if _is_infra_fault(exc):
                        async with self.pool.acquire() as conn:
                            await conn.execute(
                                self.wsql.ledger_terminal,
                                flow_id,
                                iter_key,
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
                        return  # the reclaim owns it — never a ladder burn
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
                        flow_id, row, attempt, node, _encode_result(done_payload)
                    )
                    return
                assert isinstance(outcome, Refine), (
                    "the loop body must return Done(...) or Refine(...) — "
                    "anything else is the shape error (the control union's "
                    "residual machinery refuses the unconsumed member)"
                )
                carry = cast(object, outcome.feedback)  # the walk's boundary

            # THE ADVANCE STATEMENT — the carry + THE CAP GUARD, one
            # atomic write (a refused advance IS the exhaustion).
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

    async def _exhaust_loop(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        error_class: str | None,
        message: str | None,
    ) -> None:
        """The NAMED exhaustion (the driver's own arms — the cap guard's
        refusal and the body failure; the SWEEP's arm runs the same
        statement): the named state + the FLOW TERMINALIZED in the SAME
        tx (STRANDED-FLOW)."""
        from taskq.workflows.api._sql_loop import (
            ITERATION_STATE_CAP_EXHAUSTED,
            LOOP_ERROR_BODY,
            LOOP_ERROR_CAP,
            LOOP_EXHAUST_SQL,
            render_loop_sql,
        )

        error_class = error_class or LOOP_ERROR_CAP
        state_name = (
            ITERATION_STATE_CAP_EXHAUSTED if error_class != LOOP_ERROR_BODY else "loop_body_failed"
        )
        async with self.pool.acquire() as conn:
            await conn.execute(
                render_loop_sql(LOOP_EXHAUST_SQL, self.schema).replace(
                    "{terminal}", "('{succeeded}','failed','cancelled','crashed','abandoned')"
                ),
                row["id"],
                error_class,
                f'{{"iteration_state": "{state_name}", "kind": "loop"}}',
                message or f"the loop's wall fired (the named state: {state_name})",
            )
        del flow_id

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
        await finalize_node(
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
            error_message=str(exc)[:500],
            map_index=row["map_index"],
        )

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
    decision, stated once): a RECLAIM-ELIGIBLE fault (connection loss,
    admin shutdown, interface failure — the storm's ConnectionDoesNotExist
    class) routes to RECLAIM and NEVER burns the retry ladder; a body
    failure is the body's own. The vanilla lease machinery re-claims from
    the ledger — the ledger row says 'crashed', the ladder counts
    'failed'."""
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

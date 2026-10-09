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

§7b's CONCERNS-SEPARATE MAP (the split of the former god-module — the
seams are the engine's own sweeps/fences/dispatch shape):

* ``_sql_runner`` — the runner's named statements (never inline SQL);
* ``_runner_codec`` — the row-codec seam (the jsonb envelope's boundary);
* ``_runner_errors`` — :class:`WorkflowRunError` (the loud refusal);
* ``_ctx`` / ``_ctx_wait`` — the body's runtime context + the HITL
  wait/deliver machinery;
* ``_runner_loop`` — the T19 loop driver (the machinery + the reclaim);
* ``_runner_chain`` — the T20 chain step's routed finalize;
* ``_runner_ladder`` — the retry ladder + the failure-class classifier;
* ``_runner_exit`` — §17.1's typed early-exit + §17.2's manual resume;
* ``_runner`` (this module) — the driving loop: create, dispatch,
  finalize-success, the args/codec resolution, the cancel, the reads.

The public surface is UNCHANGED (a pure structural refactor): the same
``FlowRunner``/``StepContext``/``WorkflowRunError`` importable from here,
the same methods, the same rows.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal, cast

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.obs import get_logger
from taskq.workflows._progress import (
    KIND_NODE_STARTED,
    KIND_NODE_TERMINAL,
    ProgressEmitter,
    project_auto_event,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sql_finalize import NODE_INSERT_SQL
from taskq.workflows._types import ChildSpec, ForkSpec, JoinSpec, NodeSpec, _jsonb
from taskq.workflows.api._ctx import StepContext, build_step_context
from taskq.workflows.api._ctx_wait import NodeHeldError
from taskq.workflows.api._graph import Exit
from taskq.workflows.api._runner_chain import ChainOps
from taskq.workflows.api._runner_codec import (
    FlowEntryShim,
    decode_result,
    encode_data_arg,
    encode_result,
    next_data,
)
from taskq.workflows.api._runner_errors import WorkflowRunError
from taskq.workflows.api._runner_exit import ExitOps
from taskq.workflows.api._runner_ladder import LadderOps
from taskq.workflows.api._runner_loop import LoopOps
from taskq.workflows.api._sql_runner import (
    CANCEL_NODES_SQL_TEMPLATE,
    CANCEL_ROOT_SQL_TEMPLATE,
    CLAIMABLE_NODES_SQL_TEMPLATE,
    EDGE_INSERT_SQL_TEMPLATE,
    FLOW_PAYLOAD_SQL_TEMPLATE,
    FLOW_STATUS_SQL_TEMPLATE,
    HELD_COUNT_SQL_TEMPLATE,
    INCREMENT_DEPS_SQL_TEMPLATE,
    INPUT_KEY,
    ITEM_KEY,
    NODE_BY_STEP_KEY_SQL_TEMPLATE,
    NODE_CLAIM_SQL_TEMPLATE,
    PARENT_RESULTS_BY_KEY_SQL_TEMPLATE,
    ROOT_START_SQL_TEMPLATE,
    SKIPPED_RESULT,
    SUCCEEDED_RESULTS_SQL_TEMPLATE,
    TERMINAL_RESULT_SQL_TEMPLATE,
    WF_ARGS_KEY,
    render_sql,
)
from taskq.workflows.engine import fan_in_skip, finalize_node
from taskq.workflows.ledger import (
    RunClaim,
    insert_flow_run,
    step_idempotency_key,
    step_idempotency_scope,
)

__all__ = ["FlowRunner", "StepContext", "WorkflowRunError"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


class FlowRunner(ChainOps, ExitOps, LadderOps, LoopOps):
    """The API's runner: create, drive, read. One engine, many surfaces —
    the runner OWNS no state; every fact is a row. The concern modules
    (the loop driver, the ladder, the exit, the chain finalize) compose
    in as mixins — each seam's machinery lives in its own module (§7b)."""

    def __init__(
        self,
        compiled: Any,
        pool: asyncpg.Pool,
        schema: str,
        *,
        worker_id: JobId | None = None,
    ) -> None:
        from taskq.workflows.api._validate import validate_compiled

        validate_compiled(compiled)  # a graph with errors does not run
        self.compiled = compiled
        self.pool = pool
        self.wsql: WorkflowSql = WorkflowSql.build(schema)
        self.schema = schema
        # THE DRIVER'S IDENTITY: the in-process driver mints its own (no
        # worker behind it); a WORKER-HOSTED runner passes the real
        # worker's id — every fence (the finalize CAS, the emit's cursor
        # checkpoint) then names the worker that actually holds the row
        # (the fleet-claimed door, _worker_execution).
        self._worker_id = worker_id if worker_id is not None else JobId(new_uuid())
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
        async with self.pool.acquire() as conn, conn.transaction():
            # THE CREATE IS ONE TRANSACTION (the deploy matrix's fleet-
            # crash cure): the rows, the edges, the root's start — one
            # commit. The create ran statement-autocommit before, and the
            # fleet's dispatch round swept the not-yet-wired rows UP
            # INSIDE the create: a join-wait child claimed (and EXECUTED
            # — its parents' results did not exist, the arg resolution
            # crashed the worker), the flow's own wiring half-born. A
            # transaction bounds the dispatch's visibility to the WHOLE
            # wiring (the fork's atomicity law, create-time face).
            await self._insert_static_nodes(conn, claim.flow_id, input)
            # The run is LIVE: the root flips pending → running (the
            # maintenance leg's derivation owns the TERMINAL verdict
            # from the rows — the root is a cache, never the decider).
            await conn.execute(render_sql(ROOT_START_SQL_TEMPLATE, self.schema), claim.flow_id)
        return claim.flow_id

    async def _insert_root(self, input: object, run_key: str | None) -> RunClaim:
        key = run_key or f"flow:{new_uuid()}"
        entry_payload: dict[str, object] = {INPUT_KEY: encode_data_arg(input)}
        async with self.pool.acquire() as conn:
            return await insert_flow_run(
                conn,
                self.wsql,
                entry=FlowEntryShim(self.compiled.name, entry_payload),
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
            payload: dict[str, object] = (
                {INPUT_KEY: encode_data_arg(input)} if not node.parents else {}
            )
            if data_args:
                payload[WF_ARGS_KEY] = [encode_data_arg(v) for v in data_args]
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
                render_sql(NODE_INSERT_SQL, self.schema),
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
                    render_sql(NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), parent_key, flow_id
                )
                child_row = await conn.fetchval(
                    render_sql(NODE_BY_STEP_KEY_SQL_TEMPLATE, self.schema), key, flow_id
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
                            render_sql(INCREMENT_DEPS_SQL_TEMPLATE, self.schema), child_row
                        )
                    continue  # the map join's edge — the fork writes it
                await conn.execute(render_sql(INCREMENT_DEPS_SQL_TEMPLATE, self.schema), child_row)
                await conn.execute(
                    render_sql(EDGE_INSERT_SQL_TEMPLATE, self.schema),
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
        execute: bool = True,
    ) -> str:
        """The driver (cut #10): dispatch + sweeps until ``until`` —
        ``"terminal"`` (the flow row is terminal) or ``"held"`` (a node
        waits on a human / a deadline). BOUNDED: ``max_ticks`` caps the
        loop (a hang is a defect with no stack trace); the bound is
        tested by the timeout pins.

        ``execute=False`` is the ORCHESTRATION-ONLY drive (the execution
        verdict's probe-B shape): this process claims and executes
        NOTHING — the tick runs only the sweep arms (the crash-window
        heal + the outbox drain) and the flow's WORK is executed by the
        fleet's workers through the queue-routed dispatch (the
        worker-hosted door). The in-process body execution — the
        dev-loop driver — is the ``execute=True`` default, unchanged."""
        for _ in range(max_ticks):
            if await self._flow_status(flow_id) in ("succeeded", "failed", "cancelled"):
                return "terminal"
            if until == "held" and await self._any_held(flow_id):
                return "held"
            ran = await self.tick(flow_id, execute=execute)
            if not ran:
                await asyncio.sleep(tick)
        return "max_ticks"

    async def tick(self, flow_id: JobId, *, execute: bool = True) -> bool:
        """One dispatch pass: claim + run + finalize every claimable node
        (``execute=True`` — the dev-loop driver); then the sweep arms
        (the crash-window heal + the outbox drain). With
        ``execute=False`` the sweep arms only — the orchestration-only
        pass. Returns whether ANY node ran."""
        ran = False
        if execute:
            for row in await self._claimable(flow_id):
                ran = True
                await self._run_node(flow_id, row)
        from taskq.workflows._sweep import drain_outbox, sweep_join_rederive

        await sweep_join_rederive(self.pool, self.wsql)
        await drain_outbox(self.pool, self.wsql)
        return ran

    async def _claimable(self, flow_id: JobId) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, self.schema), flow_id)
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
                render_sql(NODE_CLAIM_SQL_TEMPLATE, self.schema), row["id"], self._worker_id
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

        # THE TAIL IS SHARED with the worker-hosted execution
        # (``_worker_execution``'s fleet-claimed door): from the ledger
        # claim on, the machinery is ONE machinery — the auto projection,
        # the emitter, the router, the ladder, the finalize — and the two
        # drivers differ only in WHO CLAIMED (this driver's own CAS
        # stamps claim_epoch 0; the fleet's dispatch claim carries the
        # row's epoch).
        await self._execute_claimed(flow_id, row, attempt, ledger_id, node, body)

    async def run_fleet_claimed_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        *,
        attempt: int,
        claim_epoch: int,
    ) -> str:
        """THE WORKER-HOSTED EXECUTION DOOR (the execution verdict's gap-1
        cure): *row* is a node the FLEET's queue-routed dispatch already
        claimed (``dispatch_batch`` — the row is ``running``, the attempt
        incremented, the claim epoch bumped, the lock held by the worker
        that dispatched it). This door borrows the runner's OWN machinery
        — the ledger claim, the emitter, the router, the ladder, the
        two-transaction finalize — and differs from :meth:`_run_node`
        ONLY in WHO CLAIMED: no claim CAS runs here (the row is past
        ``pending``), the ledger claim keys the fleet claim's attempt,
        and every fence carries the fleet claim's epoch.

        *row* is the claimed node's read model: ``id`` / ``step_key`` /
        ``map_index`` / ``payload`` / ``trace_id`` — the fields the tail
        consumes (the worker's intercept reads them off the claimed row
        plus one bounded read of the step key and map index).

        THE LEASE: the worker's heartbeat renews the fleet claim's lease
        for as long as the body runs (the row is ``running`` and locked
        by this worker); a lapsed lease re-claimed by a peer fences THIS
        attempt's terminal out (the CAS) — the same at-least-once
        boundary the ledger states everywhere else.
        """
        from taskq.workflows.ledger import claim_step_ledger

        node_key = row["step_key"]
        node = self.compiled.nodes.get(node_key)
        body = self._resolve_body(node_key, node)
        async with self.pool.acquire() as conn:
            claim = await claim_step_ledger(
                conn,
                self.wsql,
                flow_id=flow_id,
                job_id=JobId(row["id"]),
                step_key=node_key,
                map_index=row["map_index"],
                attempt=attempt,
            )
        return await self._execute_claimed(
            flow_id, row, attempt, claim.ledger_id, node, body, claim_epoch=claim_epoch
        )

    async def _execute_claimed(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        node: Any,
        body: Any,
        *,
        claim_epoch: int = 0,
    ) -> str:
        """The shared execution tail, from the ledger claim to the
        terminal: the in-process driver (:meth:`_run_node`) and the
        worker-hosted fleet-claimed door (:meth:`run_fleet_claimed_step`)
        both land here — ONE execution semantics, the machinery never
        forked, the drivers differing only in WHO CLAIMED. ``claim_epoch``
        is the fence epoch the finalize/emit fences carry: the runner's
        own claim stamps 0; a fleet-claimed row carries its dispatch
        claim's epoch. Returns the attempt's outcome label (the caller's
        log line — the ROW is the truth, this is the record's echo)."""
        # THE PER-ATTEMPT CODE-VERSION RECORD (T03/§22.1 — the deploy
        # matrix's audit cure): the record is "written at claim by the
        # workflow claim path" — every claim's execution stamps the
        # row's ``code_version`` with the body's canonical content hash,
        # so a re-claimed node's SECOND attempt rewrites the record (the
        # §22.1 record rides BOTH claims). A RECORD, never a gate: a
        # hash failure is a logged loss, never a node failure.
        await self._stamp_code_version(JobId(row["id"]), row["step_key"], node, body)

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
            await self._finalize_skipped(flow_id, row, node, claim_epoch=claim_epoch)
            return "skipped"

        # THE EMISSION OP's BUFFER (T21): the attempt's own emitter — the
        # declared progress schema rides the node's decl (a MAP CHILD
        # inherits its source's: the fork-spawned keys have no decl of
        # their own).
        emitter = ProgressEmitter(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            node_id=JobId(row["id"]),
            schema_decl=self._progress_schema(row["step_key"], node),
        )

        # THE LOOP NODE (T19): the driver owns the node's lifecycle
        # (the iterations, the walls) — the caller's body path never
        # runs for it. The emitter rides in; the driver's ctx gets it,
        # and the close is THIS call's finally (bounded, best-effort —
        # the driver's internal finalizes never wait on the buffer).
        if node is not None and node.loop_spec is not None:
            try:
                await self._run_loop_node(
                    flow_id, row, attempt, node, ledger_id, emitter, claim_epoch=claim_epoch
                )
            finally:
                await emitter.aclose()
            return "loop"

        ctx = build_step_context(
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            node_key=row["step_key"],
            attempt=attempt,
            pool=self.pool,
            wsql=self.wsql,
            input=await self._flow_input(flow_id),
            map_index=row["map_index"],
            ledger_id=ledger_id,
            worker_id=self._worker_id,
            workflow_name=self.compiled.name,
            redact=self._redact_hook(),
            progress=emitter,
            claim_epoch=claim_epoch,
            # THE RUNTIME INFO (the context contract — the observability
            # primitive the body asserts on).
            flow_name=self.compiled.name,
            queue=node.queue if node is not None else None,
            claimed_at=datetime.now(UTC),
        )
        try:
            # THE LADDER'S BOUNDARY OPENS AT THE ARGS RESOLUTION (the
            # deploy matrix's fleet-crash cure): the parents' results'
            # read and the arg resolution ran BEFORE the try — a
            # resolution failure (a parent with no result yet, a payload
            # decode) escaped the ladder's classification and killed the
            # WORKER (the exception surfaced on the dispatch loop, the
            # whole process died with every live run of its estate).
            # The boundary is the EXECUTION'S boundary: from the ledger
            # claim on, anything the attempt raises routes through the
            # ladder — never through the worker's skull.
            parents_ordered, parents_by_key = await self._parent_results(row["id"])
            payload_raw = row["payload"]
            payload = _json_loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            assert isinstance(payload, dict)  # the Any-contract walk (the seed wrote the shape)
            payload_doc: dict[str, object] = payload  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk's boundary — the seed wrote the shape; the assert is the runtime check.
            args = self._resolve_args(node, body, parents_by_key, payload_doc)
            if body is None:
                # THE DEFAULT IDENTITY PACKER (the join/gather kinds): the
                # join's result IS the decoded parents' list — the FLAT
                # shape the downstream body's list param declares (the
                # gather's contract): a GATHER whose every parent returned
                # a list flattens one level (the barrier over per-stage
                # lists); the MAP join packs the items as-is (a map over
                # list items is a collect of lists — never flattened).
                # Edge order (map children distinct, the gather's wiring
                # order).
                parents_values = [r for _key, r in parents_ordered]
                if (
                    node is not None
                    and node.kind == "gather"
                    and parents_values
                    and all(isinstance(v, list) for v in parents_values)
                ):
                    parents_values = [
                        item for v in parents_values for item in cast(list[object], v)
                    ]
                result: dict[str, object] | None = {"value": parents_values}
            else:
                outcome_value = await body(ctx, *args)
                chain = self._chain_steps.get(row["step_key"])
                if chain is not None:
                    # THE CHAIN STEP (T20): the body's typed outcome IS
                    # the router's decision — the finalize routes it (at
                    # most one forked child, no join). The engine's
                    # fork-at-finalize machinery owns the child row; the
                    # ROUTE is this runner's decision.
                    await self._finalize_chain_step(
                        flow_id,
                        row,
                        attempt,
                        row["step_key"],
                        chain,
                        outcome_value,
                        payload_doc,
                        claim_epoch=claim_epoch,
                    )
                    return "succeeded"
                if isinstance(outcome_value, Exit):
                    # THE TYPED EARLY-EXIT (§17.1): the sentinel ends the
                    # node NOW — terminal-succeed with the typed payload,
                    # the downstream graph marked skipped-with-the-record.
                    exit_value = cast("Exit[object]", outcome_value)  # pyright: ignore[reportUnknownArgumentType]  # Why: the body's object-typed return — the isinstance guard IS the runtime shape check; the sentinel's payload is the walk's boundary.
                    await self._finalize_exit(
                        flow_id, row, attempt, node, exit_value, claim_epoch=claim_epoch
                    )
                    return "succeeded"
                result = encode_result(outcome_value)
        except NodeHeldError as held:
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
                node=row["step_key"],
                hold_id=held.hold_id,
                signals=list(held.signal_names),
            )
            return "held"
        except Exception as exc:  # Why: the ladder's boundary — ANY body failure routes through the retry classification.
            # THE BUFFER CLOSES BEFORE THE LADDER (the asymmetry's ordering
            # law): the final flush is bounded and best-effort — it can
            # cost freshness, never the ladder's correctness.
            await emitter.aclose()
            await self._ladder_or_fail(flow_id, row, attempt, node, exc, claim_epoch=claim_epoch)
            return "laddered"
        await emitter.aclose()
        await self._finalize_success(flow_id, row, attempt, node, result, claim_epoch=claim_epoch)
        return "succeeded"

    async def _stamp_code_version(self, job_id: JobId, node_key: str, node: Any, body: Any) -> None:
        """The per-attempt ``code_version`` record's write (T03/§22.1):
        the claim's own stamp — the body's canonical content hash onto
        the row. Best-effort (a record, never a gate): a body the hash
        cannot read (a builtin, a partial) records as NULL, the loss
        logged; the row's claim never fails on its own audit."""
        import inspect

        from taskq.workflows._version import compute_code_version

        target = (
            body
            if body is not None
            else (getattr(node, "body", None) if node is not None else None)
        )
        if target is None:
            return  # the join/gather kinds: the identity packer is the engine's own code
        try:
            version = compute_code_version(
                getattr(target, "__module__", "") or "",
                getattr(target, "__qualname__", getattr(target, "__name__", "")) or "",
                inspect.getsource(target),
            )
        except (
            Exception
        ) as exc:  # Why: the record's asymmetry — a hash loss is logged, never a node failure.
            logger.warning(
                "node.code-version-unstamped",
                node=node_key,
                error=str(exc)[:200],
            )
            return
        async with self.pool.acquire() as conn:
            # S608: the schema identifier is the settings boundary the
            # runner validated at build; the values are $-bound.
            await conn.execute(
                f'UPDATE "{self.schema}".jobs SET code_version = $2 WHERE id = $1',  # noqa: S608
                job_id,
                version,
            )

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
                render_sql(PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, self.schema), node_id
            )
        ordered: list[tuple[str, object]] = []
        for r in rows:
            result: object = decode_result(r["result"])  # pyright: ignore[reportUnknownVariableType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
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
        if ITEM_KEY in payload:
            item_raw: object = payload[ITEM_KEY]  # pyright: ignore[reportUnknownVariableType]  # Why: the Any-contract walk — the fork wrote the shape.
            return (self._coerce(item_raw, node, body, 0),)
        if node is None:
            # THE OUTBOX CONSUMER with no compiled NodeDecl (the
            # escalation step — attack-3 H1's cure): its declared
            # payload rides the row as data args (the bindings'
            # ``wf_args``), the body's OWN contract with the enqueue's
            # context. No parents exist for such a consumer — the
            # parent-results walk has nothing to say.
            wf_args_raw: object = payload.get(WF_ARGS_KEY, [])
            if wf_args_raw:
                return tuple(cast(list[object], wf_args_raw))
            return tuple(parent_results.values())
        if not node.args:
            return tuple(parent_results.values())
        wf_args_raw: object = payload.get(WF_ARGS_KEY, [])
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
                parts.append(self._coerce(next_data(data_iter), node, body, position))
        return tuple(parts)

    def _coerce(self, raw: object, node: Any, body: Any, position: int) -> object:
        """The payload codec hook: the body's declared param annotation
        re-validates the jsonb round-trip's value (the decode once, typed
        end to end). The walk lives in the CODEC module
        (``_runner_codec.coerce_arg`` — bare models fast-path, lists/
        unions/generics walk the TypeAdapter)."""
        if body is None:
            return raw
        from taskq.workflows.api._hints import body_hints
        from taskq.workflows.api._runner_codec import coerce_arg

        hints = body_hints(body)
        params = [v for k, v in hints.items() if k not in ("return", "ctx")]
        if position >= len(params):
            return raw
        param = params[position]
        return coerce_arg(raw, param=param, position=position, params=params)

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
                render_sql(CANCEL_ROOT_SQL_TEMPLATE, self.schema),
                flow_id,
                (reason or "cancel_workflow")[:500],
            )
            if flipped is None:
                return 0  # already terminal — idempotent
            await conn.execute(render_sql(CANCEL_NODES_SQL_TEMPLATE, self.schema), flow_id)
            held = await cancel_run_signals(conn, schema=self.schema, workflow_id=flow_id)
            # THE AUDIT ROW (the caller owns the tx — the same-tx
            # guarantee; the lazy import keeps the layering).
            from taskq.audit import record_admin_action

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
        emitter: ProgressEmitter | None = None,
        *,
        claim_epoch: int = 0,
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
            claim_epoch=claim_epoch,
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
                    payload={ITEM_KEY: encode_data_arg(item)},
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

    async def _finalize_skipped(
        self, flow_id: JobId, row: dict[str, Any], node: Any, *, claim_epoch: int = 0
    ) -> None:
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
            claim_epoch=claim_epoch,
            outcome="succeeded",
            result=dict(SKIPPED_RESULT),
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
                render_sql(TERMINAL_RESULT_SQL_TEMPLATE, self.schema), terminal, flow_id
            )
        return decode_result(raw)

    async def _flow_status(self, flow_id: JobId) -> str:
        async with self.pool.acquire() as conn:
            status = await conn.fetchval(render_sql(FLOW_STATUS_SQL_TEMPLATE, self.schema), flow_id)
        return status or "missing"

    async def _flow_input(self, flow_id: JobId) -> object:
        async with self.pool.acquire() as conn:
            raw = await conn.fetchval(render_sql(FLOW_PAYLOAD_SQL_TEMPLATE, self.schema), flow_id)
        decoded = decode_result(raw)
        if isinstance(decoded, dict) and INPUT_KEY in decoded:
            return cast(object, decoded[INPUT_KEY])  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk's boundary — the flow input's key is the runner's own.
        return None

    async def _flow_state(self, flow_id: JobId) -> dict[str, object]:
        """The dispatch-time predicate's view (cut #4): the input + the
        completed results so far — the state a sibling-reading guard
        needs."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                render_sql(SUCCEEDED_RESULTS_SQL_TEMPLATE, self.schema), flow_id
            )
        return {
            "input": await self._flow_input(flow_id),
            "results": {
                r["step_key"]: decode_result(r["result"])  # pyright: ignore[reportUnknownArgumentType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
                for r in rows
            },
        }

    async def _any_held(self, flow_id: JobId) -> bool:
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(render_sql(HELD_COUNT_SQL_TEMPLATE, self.schema), flow_id)
        return bool(count)

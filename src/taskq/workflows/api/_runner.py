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
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Literal, cast

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.exceptions import RetryAfter, Snooze
from taskq.obs import get_logger
from taskq.workflows._cache import CachedStep
from taskq.workflows._progress import (
    KIND_NODE_STARTED,
    KIND_NODE_TERMINAL,
    ProgressEmitter,
    project_auto_event,
)
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sql_finalize import (
    COLLECT_TRUNCATION_RECEIPT_SQL,
    FANIN_FAILURES_BYTE_CAP,
    NODE_INSERT_SQL,
)
from taskq.workflows._types import (
    ChildSpec,
    FinalizeResult,
    ForkSpec,
    JoinSpec,
    NodeSpec,
    _jsonb,
    _row_metadata,
)
from taskq.workflows.api._app import CompiledWorkflow
from taskq.workflows.api._ctx import StepContext, build_step_context
from taskq.workflows.api._ctx_wait import NodeHeldError
from taskq.workflows.api._deps import deps_param_declared
from taskq.workflows.api._graph import (
    Exit,
    rate_limit_names,
    reservation_names,
    route_child_key,
)
from taskq.workflows.api._runner_chain import ChainOps
from taskq.workflows.api._runner_codec import (
    FlowEntryShim,
    decode_result,
    encode_data_arg,
    encode_result,
    next_data,
    result_truncation,
)
from taskq.workflows.api._runner_defer import DeferOps
from taskq.workflows.api._runner_errors import VanishedNodeError, WorkflowRunError
from taskq.workflows.api._runner_exit import ExitOps
from taskq.workflows.api._runner_ladder import LadderOps, is_infra_fault
from taskq.workflows.api._runner_loop import LoopOps
from taskq.workflows.api._sql_runner import (
    CLAIMABLE_NODES_SQL_TEMPLATE,
    EDGE_INSERT_SQL_TEMPLATE,
    FLOW_FAILURE_SQL_TEMPLATE,
    FLOW_NODE_CENSUS_SQL_TEMPLATE,
    FLOW_PAYLOAD_SQL_TEMPLATE,
    FLOW_STATUS_SQL_TEMPLATE,
    HELD_COUNT_SQL_TEMPLATE,
    INCREMENT_DEPS_SQL_TEMPLATE,
    INPUT_KEY,
    ITEM_KEY,
    NODE_BY_STEP_KEY_SQL_TEMPLATE,
    NODE_CLAIM_SQL_TEMPLATE,
    PARENT_RESULTS_BY_KEY_SQL_TEMPLATE,
    ROOT_PRIORITY_STAMP_SQL_TEMPLATE,
    ROOT_START_SQL_TEMPLATE,
    SKIPPED_RESULT,
    SUCCEEDED_RESULTS_SQL_TEMPLATE,
    TERMINAL_RESULT_SQL_TEMPLATE,
    WF_ARGS_KEY,
    render_sql,
)
from taskq.workflows.definitions import FanInBoundExceededError, validate_fork
from taskq.workflows.engine import fan_in_skip, finalize_node
from taskq.workflows.ledger import (
    RunClaim,
    insert_flow_run,
    step_idempotency_key,
    step_idempotency_scope,
)

__all__ = ["FlowRunner", "StepContext", "VanishedNodeError", "WorkflowRunError"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


def _bounded_collect(values: list[object]) -> dict[str, object]:
    """THE COLLECTED RESULT'S CAP (the F4 cure — ONE mechanism with the
    failures'-collect's own discipline, ``_sql_finalize.py``'s
    ``COLLECT_FAN_IN_APPEND_SQL`` arms mirrored at the collect face): a
    join/gather's envelope — the identity packer's
    ``{"value": [every parent's result]}`` — compacts PAST the estate's
    result cap (``FANIN_FAILURES_BYTE_CAP``, the SAME 64 Ki bound the
    failures' array honors):

    * under the cap: byte-identical to the pre-cure shape (the common
      case pays nothing);
    * over the cap: the fitted PREFIX of items stays spelled under
      ``value`` (the collect's arm-2 face — keep what fits), and the
      truncation is HONEST — ``__truncated__`` counts the items no
      longer spelled on this row (the marker is the read's own warning,
      the failures'-collect's arm-3 pattern) and ``detail_home`` names
      where the full collect lives: THE CHILD ROWS — each map child /
      gather parent carries its OWN result row (the record never loses
      the detail; the join row is the bounded summary, exactly the
      failures' split: the ledger/children the detail, the row the
      summary).

    The pre-cure face was the bypass: the wf finalize's TERMINAL_MARK
    checked no ``result_max_bytes`` — a 700-child map landed a
    730,001-byte envelope (11.1x the cap every plain job's terminal
    enforces non-retryably); the ceiling at the fan-in bound was
    ~305 MiB of jsonb in ONE row. The byte measure is the finalize's
    own (``_jsonb`` → utf-8 length — ``engine._jsonb_len``'s shape), so
    the recorded ``result_size_bytes`` IS the bounded number."""
    detail_home = (
        "the full collect lives on the children's own result rows (the "
        "fork-spawned .item rows / the gather's parents); this join row "
        "carries the bounded summary"
    )
    envelope: dict[str, object] = {"value": values}
    if len(_jsonb(envelope).encode("utf-8")) <= FANIN_FAILURES_BYTE_CAP:
        return envelope
    kept: list[object] = []
    for value in values:
        candidate = {
            "value": [*kept, value],
            "__truncated__": len(values) - len(kept) - 1,
            "detail_home": detail_home,
        }
        if len(_jsonb(candidate).encode("utf-8")) > FANIN_FAILURES_BYTE_CAP:
            break
        kept.append(value)
    return {
        "value": kept,
        "__truncated__": len(values) - len(kept),
        "detail_home": detail_home,
    }


def _resolve_priority(*levels: int | None) -> int:
    """THE PRECEDENCE LAW'S ARITHMETIC (the wf-priority cure), stated
    once: the first non-``None`` level wins; ``None`` everywhere = the
    column default's own value (0). The levels, in the law's order: the
    explicit per-call override, the step's declaration, the arm's
    override, the actor handle's declaration."""
    for value in levels:
        if value is not None:
            return value
    return 0


class FlowRunner(ChainOps, ExitOps, LadderOps, LoopOps, DeferOps):
    """The API's runner: create, drive, read. One engine, many surfaces —
    the runner OWNS no state; every fact is a row. The concern modules
    (the loop driver, the ladder, the exit, the chain finalize, the
    deferral) compose in as mixins — each seam's machinery lives in its
    own module (§7b)."""

    def __init__(
        self,
        compiled: Any,
        pool: asyncpg.Pool,
        schema: str,
        *,
        worker_id: JobId | None = None,
        deps: object | None = None,
    ) -> None:
        from taskq.workflows.api._validate import validate_compiled

        # THE DEPS SEAM'S DOOR (the DI capability): the runner binds ONE
        # instance — the explicit *deps* overrides, the compiled's
        # binding (the app's, bound once at WorkflowApp(deps=…)) is the
        # default. The VALIDATION runs against the EFFECTIVE binding (a
        # frozen-compile copy carries it — the E12 rule reads what the
        # bodies will actually receive), so a binding the bodies'
        # declarations do not satisfy is refused here too.
        compiled_deps = compiled.deps if isinstance(compiled, CompiledWorkflow) else None
        if deps is not None and isinstance(compiled, CompiledWorkflow):
            compiled = replace(compiled, deps=deps)
            compiled_deps = deps
        self._deps: object | None = deps if deps is not None else compiled_deps
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
        # THE DESCENDANT RUNNERS' CACHE (T28 — the child flows): the
        # child flow's runner resolved ONCE per drive (the root's
        # stamped workflow name → the compile registry).
        self._child_runners: dict[str, FlowRunner] = {}
        # THE RUN-PRIORITY ECHO (the wf-priority cure): the create-time
        # per-call override (``create_flow(priority=…)``), echoed here so
        # the DEV door's fork builds (``_run_node``'s executions) resolve
        # the run's own law without re-reading the root. The ROW stays
        # the truth — the root's ``metadata.wf_priority`` stamp is the
        # durable leg, and the FLEET door (``execute_flow_job``) reads it
        # from the row; this echo only spares the dev loop a per-node
        # read. ``None`` = no override declared.
        self._run_priority: int | None = None

    # ── create ───────────────────────────────────────────────────────

    async def create_flow(
        self,
        *,
        input: object = None,
        run_key: str | None = None,
        root_metadata_extra: dict[str, object] | None = None,
        priority: int | None = None,
    ) -> RunClaim:
        """Create the run: the flow root (its payload CARRIES the input —
        cut #7) + every non-map node upfront (the join-wait rows among
        them, edges declared in the same call). The map joins are
        runtime-spawned by their source's fork (the engine's FORK
        ATOMICITY) — the wiring's downstream consumers ride the join's
        declared consumers (the outbox).

        THE CREATE ATOMICITY (the fork-atomicity law at creation
        granularity): the ROOT INSERT rides the SAME transaction as the
        static nodes + edges + ROOT_START. The pre-cure shape committed
        the root ALONE (its own acquire) and the wiring rode a second
        transaction — the kill in between stranded the ORPHAN ROOT
        (pending, ZERO nodes): a root the maintenance derivation can
        never develop (its rollup INNER-JOINS the node rows — a nodeless
        root never derives, never terminalizes, never prunes) and one
        that SQUATS THE RUN KEY (the retry's arbiter conflict answered
        ``created=False`` and the nodes never landed — ``drive()`` ran to
        ``max_ticks``). One transaction makes the orphan UNREPRESENTABLE
        (the create-time face of the fleet-crash cure's own law — the
        dispatch's visibility is bounded to the WHOLE wiring, and now to
        the root that owns it).

        THE CLAIM SURFACE HONEST (the run-key failure lie's cure): the
        TYPED :class:`RunClaim` returns — ``created`` /
        ``existing-running`` / ``existing-terminal`` (the
        :attr:`RunClaim.kind` verdict) — never a bare id the caller
        cannot read a status off.

        THE RETRY-COMPLETION CENSUS (the belt's belt): an arbiter
        conflict over a PENDING run with ZERO node rows is the orphan's
        one remaining signature (the pre-cure debris) — the nodes
        re-insert and the root starts IN THE SAME TRANSACTION (the retry
        completes; the killed create's key is finished, not squatted). A
        conflict over anything else is the G2 contract: the EXISTING run,
        never a second silent run, and a terminal run's key is never
        re-fired. THE REAP BELT: the census-grace arm
        (:func:`taskq.workflows._sweep.reap_nodeless_roots`) reaps any
        orphan that could ever exist (a future statement-order
        regression's debris) past the grace.

        THE PROVENANCE STAMPS (T28 — the child flows' identity):
        *root_metadata_extra* rides the SAME create transaction (the
        child root's parent pointers land ATOMICALLY with the root —
        never a post-create UPDATE that would re-open the orphan window
        this seam closed). ``None`` (the default) = the shipped shape
        byte-identical.

        THE PER-CALL PRIORITY OVERRIDE (the wf-priority cure — the
        load-bearing face): *priority* is the run's explicit override —
        EVERY row this create materializes (the static nodes) lands at
        the value in the ``jobs.priority`` column, and the ROOT's
        metadata stamps it (``wf_priority``) so the FLEET door's fork
        builds resolve the same override for rows materialized later
        (the map/route children a worker forks mid-run). The law's
        precedence: this override > the step's declaration > the actor
        handle's > the column default. ``None`` (the default) = no
        override — the declarations (or the column default) speak, the
        pre-cure byte-identical shape. The domain is the smallint column's
        (``check_priority_domain`` — the same refusal at every layer that
        accepts a priority).
        """
        from taskq.constants import check_priority_domain

        if priority is not None:
            check_priority_domain(priority, what="the run's priority override")
        if priority is not None:
            root_metadata_extra = {**(root_metadata_extra or {}), "wf_priority": priority}
        async with self.pool.acquire() as conn, conn.transaction():
            claim = await self._insert_root(conn, input, run_key, root_metadata_extra)
            complete = claim.created
            if not complete:
                # THE RETRY-COMPLETION CENSUS: the orphan's signature is
                # pending + zero nodes — a committed create whose node
                # pass never rode the root's tx. Anything else is the
                # existing run.
                complete = (
                    claim.status == "pending" and await self._node_census(conn, claim.flow_id) == 0
                )
            if complete:
                # THE CREATE IS ONE TRANSACTION (the deploy matrix's
                # fleet-crash cure): the rows, the edges, the root's
                # start — one commit. The create ran statement-autocommit
                # before, and the fleet's dispatch round swept the
                # not-yet-wired rows UP INSIDE the create: a join-wait
                # child claimed (and EXECUTED — its parents' results did
                # not exist, the arg resolution crashed the worker), the
                # flow's own wiring half-born. A transaction bounds the
                # dispatch's visibility to the WHOLE wiring (the fork's
                # atomicity law, create-time face).
                # THE CREATE-TIME ECHO (the dev door's own leg): the
                # override the drive's fork builds resolve — set only
                # when THIS create materialized the rows (an arbiter
                # conflict returns an EXISTING run whose root stamp is
                # its own truth; the row is the law, never the echo).
                self._run_priority = priority
                await self._insert_static_nodes(conn, claim.flow_id, input, run_priority=priority)
                # The run is LIVE: the root flips pending → running (the
                # maintenance leg's derivation owns the TERMINAL verdict
                # from the rows — the root is a cache, never the decider).
                await conn.execute(render_sql(ROOT_START_SQL_TEMPLATE, self.schema), claim.flow_id)
                # THE CLAIM CARRIES THE COMMITTED STATUS: the ROOT_START
                # rode the same tx — the run returns LIVE ('running'),
                # never the insert's pre-start 'pending' (the envelope
                # never lies about the state it handed back).
                claim = replace(claim, status="running")
            else:
                # THE ADOPTION'S ECHO (review F2's cure — the dev door's
                # arbiter-adoption path): the claim returned an EXISTING
                # run — the create-time echo above never ran, so the
                # per-call level resolves from the ROOT'S STAMP (ONE
                # bounded PK read, the fleet door's own SELECT shape):
                # the row is the law at BOTH doors. A re-drive of a run
                # created with ``priority=6`` forks its rows at 6 even
                # when the re-drive passes no kwarg — the adopted run's
                # stamp is its own truth, and the fork's cohort never
                # splits (the children riding the declarations while the
                # root says 6 was the convicted shape).
                self._run_priority = await self._adopted_run_priority(conn, claim.flow_id)
        return claim

    async def _adopted_run_priority(self, conn: ConnLike, flow_id: JobId) -> int | None:
        """THE ADOPTED RUN'S PER-CALL OVERRIDE (review F2's cure): the
        root's stamped ``wf_priority``, resolved with the FLEET DOOR'S OWN
        ARITHMETIC — the same parse → post-parse DOMAIN CLAMP → degrade
        (a stamp a hand-crafted row rendered unparseable OR out of the
        smallint domain degrades to NO override, never a door death;
        ``check_priority_domain`` is the predicate, the exception the
        degrade). ``None`` = no override stamped (the declarations — or
        the column default — speak)."""
        from taskq.constants import check_priority_domain

        read = await conn.fetchrow(
            render_sql(ROOT_PRIORITY_STAMP_SQL_TEMPLATE, self.schema), flow_id
        )
        raw_priority: object | None = read["wf_priority"] if read is not None else None
        try:
            parsed_priority = (
                int(raw_priority) if raw_priority is not None else None  # type: ignore[call-overload]  # Why: the jsonb decode is text-or-None at runtime; the int() is the parse.
            )
            if parsed_priority is not None:
                check_priority_domain(parsed_priority, what="the root's wf_priority stamp")
        except (TypeError, ValueError):
            parsed_priority = None
        return parsed_priority

    async def _node_census(self, conn: ConnLike, flow_id: JobId) -> int:
        """The run's node-row count — the orphan's census (a nodeless
        root counts ZERO). The named statement, never inline SQL."""
        raw = await conn.fetchval(render_sql(FLOW_NODE_CENSUS_SQL_TEMPLATE, self.schema), flow_id)
        return int(raw or 0)

    async def _insert_root(
        self,
        conn: ConnLike,
        input: object,
        run_key: str | None,
        root_metadata_extra: dict[str, object] | None = None,
    ) -> RunClaim:
        key = run_key or f"flow:{new_uuid()}"
        entry_payload: dict[str, object] = {INPUT_KEY: encode_data_arg(input)}
        return await insert_flow_run(
            conn,
            self.wsql,
            entry=FlowEntryShim(self.compiled.name, entry_payload),
            run_key=key,
            extra_metadata=root_metadata_extra,
        )

    async def _insert_static_nodes(
        self, conn: ConnLike, flow_id: JobId, input: object, *, run_priority: int | None = None
    ) -> None:
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
                # THE ROW'S RESOLVED PRIORITY (the wf-priority cure): the
                # per-call override > the node's declared (the handle's
                # folded in at the wiring) > the column default.
                priority=_resolve_priority(run_priority, node.priority),
            )
            # THE STATIC ROW'S OWN TERMS (CURE 2 + H-A1): a step
            # declaring rate_limits carries the bucket names on its
            # metadata, and a step declaring RESERVATIONS carries their
            # NAMES beside them (the claim path's read — the gate
            # resolves the names through the resolved registry); the
            # no-terms rows keep the base shape byte-identical.
            row_meta = (
                _row_metadata(
                    flow_id,
                    rate_limits=rate_limit_names(
                        node.rate_limits, what=f"step {key!r}'s rate_limits"
                    ),
                    reservations=reservation_names(
                        tuple(node.reservations), what=f"step {key!r}'s reservations"
                    ),
                )
                if node.rate_limits or node.reservations
                else {"flow_id": str(flow_id)}
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
                _jsonb(row_meta),
                step_idempotency_scope(flow_id),
                step_idempotency_key(spec.step_key),
                spec.priority,
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
        (the crash-window heal + the outbox drain + the DURABLE TIMER's
        fire — T28: the dev loop's timer arm mirrors the leader's, so a
        timer flow completes in-process exactly as it completes on the
        fleet). With ``execute=False`` the sweep arms only — the
        orchestration-only pass. Returns whether ANY node ran."""
        ran = False
        if execute:
            for row in await self._claimable(flow_id):
                ran = True
                await self._run_node(flow_id, row)
            # THE DESCENDANT FLOWS (T28 — the child flows): the child is
            # a REAL flow — the dev-loop driver walks the receipts and
            # claims its rows too (the fleet door needs nothing: the
            # child's rows are queue-dispatched like any flow's).
            ran |= await self._drive_descendants(flow_id, execute=execute)
        from taskq.workflows._sweep import drain_outbox, sweep_join_rederive

        await sweep_join_rederive(self.pool, self.wsql)
        # THE CHILD-JOIN WAKE (T28): the child's terminal is the join
        # hold's resolver — the arm runs AFTER the root-maintain leg (a
        # child terminalized THIS tick wakes its waiter THIS tick).
        from taskq.workflows.api._hitl import sweep_child_joins

        await sweep_child_joins(self.pool, self.wsql)
        # THE EXPIRY ARM (rv5-C5a — the dev-drives parity): the tick
        # runs the signal sweep's expiry leg, so a ``timeout_s`` wait in
        # a DEV DRIVE fires the ``Expired`` member exactly as it does
        # under the leader's belt — the pre-cure dev face parked on the
        # hold until ``max_ticks`` (the deadline inert, the dev loop a
        # different engine from the fleet's).
        from taskq.workflows.api._hitl import sweep_expired_signals

        await sweep_expired_signals(self.pool, schema=self.schema)
        await drain_outbox(self.pool, self.wsql)
        from taskq.workflows.api._hitl import sweep_due_timers

        # THE DRAIN (the timer-cures lane's cure 4): the dev loop's
        # timer arm mirrors the leader's — the due set empties per tick.
        await sweep_due_timers(self.pool, schema=self.schema, drain=True)
        return ran

    async def _drive_descendants(self, flow_id: JobId, *, execute: bool) -> bool:
        """The descendant flows' one claim pass (the dev-loop driver's
        face — T28): the receipts' recursive walk (the UNION's dedup
        terminates it), each descendant's claimable nodes run through
        ITS OWN runner (the child's compiled graph resolves its bodies
        and args — never the parent's)."""
        from taskq.workflows._childflow import DESCENDANT_FLOWS_SQL_TEMPLATE

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(render_sql(DESCENDANT_FLOWS_SQL_TEMPLATE, self.schema), flow_id)
        ran = False
        for fid_row in rows:
            fid = JobId(fid_row["fid"])
            child = await self._runner_for_flow(fid)
            if child is None or not execute:
                continue
            for row in await child._claimable(fid):
                ran = True
                await child._run_node(fid, row)
        return ran

    async def _runner_for_flow(self, flow_id: JobId) -> FlowRunner | None:
        """The descendant flow's runner: the root row's stamped workflow
        name resolves through the compile registry (D1 — the same
        registry the spawn and the fleet door read). ``None`` = the
        name is unregistered in THIS process (loud, logged; the rows
        keep their work for a driver that carries the definition)."""
        from taskq.workflows._childflow import FLOW_WORKFLOW_NAME_SQL_TEMPLATE
        from taskq.workflows._worker_execution import get_compiled_workflow

        async with self.pool.acquire() as conn:
            name = await conn.fetchval(
                render_sql(FLOW_WORKFLOW_NAME_SQL_TEMPLATE, self.schema), flow_id
            )
        if not name:
            return None
        cached = self._child_runners.get(str(flow_id))
        if cached is not None:
            return cached
        try:
            compiled = get_compiled_workflow(name)
        except KeyError:
            logger.warning(
                "childflow.definition_unregistered",
                run_id=str(flow_id),
                workflow=name,
            )
            return None
        child = FlowRunner(compiled, self.pool, self.schema)
        self._child_runners[str(flow_id)] = child
        return child

    async def _claimable(self, flow_id: JobId) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, self.schema), flow_id)
        return [dict(r) for r in rows]

    async def _run_node(self, flow_id: JobId, row: dict[str, Any]) -> None:
        node_key = row["step_key"]
        node = self.compiled.nodes.get(node_key)
        try:
            body = self._resolve_body(node_key, node)
        except WorkflowRunError as exc:
            # THE VANISHED-NODE WEDGE's LOUD TERMINAL (rv5-t6's cure):
            # the body resolves NOWHERE (the structural redeploy — the
            # registry-is-truth law, D1). Before this cure the error
            # escaped drive() RAW — the row stayed ``pending``, the root
            # ``running``, NOTHING surfaced on the run (the eternal
            # zombie). Now the node is CLAIMED (the attempt's ledger row
            # exists — the record's truth) and routed through the
            # ladder, whose DETERMINISTIC route terminal-fails it on the
            # first attempt with the typed class on the record; T06's
            # propagation fails the root. The driver never raises; the
            # rows tell the operator the truth.
            await self._fail_vanished_node(flow_id, row, exc)
            return
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
        await self._execute_claimed(
            flow_id, row, attempt, ledger_id, node, body, run_priority=self._run_priority
        )

    async def _fail_vanished_node(
        self, flow_id: JobId, row: dict[str, Any], exc: WorkflowRunError
    ) -> None:
        """THE VANISHED-NODE WEDGE's loud terminal (rv5-t6's cure — the
        structural redeploy's pending row must terminal-fail, never
        zombie). The node is claimed HERE (the CAS — pending → running,
        the attempt incremented, the ledger claim's running row inserted:
        the record's truth), then the error routes through
        :meth:`LadderOps._ladder_or_fail` — :class:`VanishedNodeError` is
        a DETERMINISTIC authoring failure (a re-run can never resolve a
        key the registry does not carry), so the ladder's deterministic
        route terminal-fails the node ON THE FIRST ATTEMPT with the typed
        class + the cancel-then-restart remedy on the record, the ladder
        unburned; T06's propagation fails the root.

        A lost CAS (a peer claimed the row first — or the flow died)
        touches nothing: the winner's own drive owns the verdict."""
        async with self.pool.acquire() as conn:
            claimed = await conn.fetchval(
                render_sql(NODE_CLAIM_SQL_TEMPLATE, self.schema), row["id"], self._worker_id
            )
            if claimed is not None:
                from taskq.workflows.ledger import claim_step_ledger

                await claim_step_ledger(
                    conn,
                    self.wsql,
                    flow_id=flow_id,
                    job_id=JobId(row["id"]),
                    step_key=row["step_key"],
                    map_index=row["map_index"],
                    attempt=int(claimed),
                )
        if claimed is None:
            return  # someone else claimed it (or the flow died) — their drive owns the verdict
        logger.warning(
            "node.vanished-from-registry",
            run_id=str(flow_id),
            node=row["step_key"],
            error_class=type(exc).__name__,
            error=str(exc)[:200],
        )
        await self._ladder_or_fail(flow_id, row, int(claimed), None, exc)

    def _deps_tail(self, body: Callable[..., object], wired_count: int) -> tuple[object, ...]:
        """THE DEPS SEAM'S INJECTION (the DI capability's runtime face):
        the bound instance rides as the LAST positional argument when the
        body declares the deps shape (one positional param beyond ctx +
        the wired sources — the SAME shape E12 convicts at build; the
        runtime read is the signature's positional truth, so an
        unresolvable-annotations body still gets the injection it
        declared). The instance is the app's, bound once — never a copy,
        never a re-mint; the runner never inspects it."""
        if self._deps is None:
            return ()
        if not deps_param_declared(body, wired_count):
            return ()
        return (self._deps,)

    async def run_fleet_claimed_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        *,
        attempt: int,
        claim_epoch: int,
        run_priority: int | None = None,
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

        ``run_priority`` is the RUN's per-call override (the wf-priority
        cure), resolved by the CALLER from the root's metadata stamp
        (``execute_flow_job``'s bounded read — the same round trip the
        workflow-name stamp rides): the fork builds this door performs
        resolve it as the law's first level. ``None`` = no override (the
        declarations — or the column default — speak).

        THE LEASE: the worker's heartbeat renews the fleet claim's lease
        for as long as the body runs (the row is ``running`` and locked
        by this worker); a lapsed lease re-claimed by a peer fences THIS
        attempt's terminal out (the CAS) — the same at-least-once
        boundary the ledger states everywhere else.
        """
        from taskq.workflows.ledger import claim_step_ledger

        node_key = row["step_key"]
        node = self.compiled.nodes.get(node_key)
        # THE FLEET DOOR KEEPS THE PARKING SHAPE (the deploy matrix's own
        # law — the door's pins enforce it): a body unresolvable in THIS
        # process raises; the door translates it to the
        # WorkflowBodyUnresolvableError parking (the loud, budget-free
        # snooze) and the fleet's OTHER workers — the new deploy's
        # rolling wave — re-claim and finish the run. This is the skew
        # face, deliberately NOT T6's terminal: the dev drive
        # (:meth:`_run_node`) is the workflow-OWNING process (no other
        # resolver exists — the vanished key is the run's own wedge, and
        # THAT door terminal-fails loudly); the fleet worker is one
        # claimant among many (the parking is the handoff, never a
        # zombie).
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
            flow_id,
            row,
            attempt,
            claim.ledger_id,
            node,
            body,
            claim_epoch=claim_epoch,
            run_priority=run_priority,
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
        run_priority: int | None = None,
    ) -> str:
        """The shared execution tail, from the ledger claim to the
        terminal: the in-process driver (:meth:`_run_node`) and the
        worker-hosted fleet-claimed door (:meth:`run_fleet_claimed_step`)
        both land here — ONE execution semantics, the machinery never
        forked, the drivers differing only in WHO CLAIMED. ``claim_epoch``
        is the fence epoch the finalize/emit fences carry: the runner's
        own claim stamps 0; a fleet-claimed row carries its dispatch
        claim's epoch. Returns the attempt's outcome label (the caller's
        log line — the ROW is the truth, this is the record's echo).

        ``run_priority`` is the RUN's per-call override (the wf-priority
        cure): the fork builds (the map's children, the route's arms)
        resolve it as the law's FIRST level — the dev door passes the
        create-time echo, the fleet door the root's metadata stamp's
        resolution. ``None`` = no override declared (the declarations —
        or the column default — speak)."""
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

        route_fork: ForkSpec | None = (
            None  # the typed route's prebuilt fork (T27 — the body path builds it from the LIVE elements)
        )

        # THE RESULT'S BINDING (the tail's read): the miss path's
        # envelope, None on the hit path (the hit's finalize IS the
        # tail's — below — and returns before the miss finalize reads
        # this).
        result: dict[str, object] | None = None
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
            # THE MAX-IN-FLIGHT BOUND (T20 / DH9): the RUN's declared
            # admission control, handed to every ctx.emit_batch — the
            # workflow-level declare's runtime wiring.
            max_in_flight=getattr(self.compiled, "max_in_flight", None),
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

            # THE CROSS-RUN CACHE'S CLAIM-PATH SEAM (T25 — the TEMPORAL
            # dedup; the arbiter owns the CONCURRENT one). Opt-in per
            # step (``cache=``), the PLAIN step face only: the address
            # is the Nix-style recursive hash — the body's §22.1
            # code-version canon + the resolved input's canonical jsonb
            # — computed HERE, on the rows. A HIT (the row's freshness
            # leg, the DB clock) delivers the cached envelope through
            # the ordinary fenced finalize — the body NEVER runs, the
            # node row carries the receipt. A MISS runs the body; the
            # store happens ON TERMINAL-SUCCEEDED ONLY (the
            # failed-run-squats-the-key bug closed by construction).
            cache_address: str | None = None
            cache_hit: CachedStep | None = None
            if node is not None and node.cache and body is not None and node.kind == "step":
                code_version = self._body_code_version(row["step_key"], node, body)
                if code_version is None:
                    logger.warning(
                        "step-cache-unaddressable",
                        run_id=str(flow_id),
                        node=row["step_key"],
                        error="the body's code-version hash is not reproducible — caching NOTHING",
                    )
                else:
                    from taskq.workflows._cache import cache_address as _cache_address
                    from taskq.workflows.api._runner_codec import encode_data_arg

                    cache_address = _cache_address(code_version, [encode_data_arg(a) for a in args])
                    from taskq.workflows._cache import lookup_step_cache

                    hit = await lookup_step_cache(self.pool, self.wsql, address=cache_address)
                    if hit is not None:
                        # THE HIT: the cached envelope IS the result —
                        # the finalize runs OUTSIDE the body boundary
                        # (the claim tail, below — the unify cure): the
                        # body NEVER runs, the same typed decode
                        # downstream.
                        cache_hit = hit

            if cache_hit is not None:
                # THE HIT: the body NEVER runs — the cached envelope's
                # finalize is the CLAIM TAIL'S (below, outside the body
                # boundary: the unify cure — a finalize fault there is
                # machinery, never a body failure).
                pass
            elif body is None:
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
                # THE COLLECTED RESULT'S CAP (the F4 cure): the envelope
                # compacts past the estate's result cap — the same
                # discipline the failures'-collect ships (the cap, the
                # honest __truncated__ marker, the detail on the child
                # rows). Under the cap: byte-identical to the pre-cure
                # shape.
                result = _bounded_collect(parents_values)
            else:
                deps_tail = self._deps_tail(body, len(args))
                outcome_value = await body(ctx, *args, *deps_tail)
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
                if node is not None and node.map_arms is not None:
                    # THE TYPED ROUTE'S ARM DISPATCH (T27) — on the LIVE
                    # elements (the runtime TYPE is the dispatch key; the
                    # encoded envelope's dicts arrive too late). The
                    # no-arm element is the LOUD death (the chain face's
                    # shape): terminal-FAILED with
                    # ``error_class='RouterNotTotal'``, NOTHING routes —
                    # the skip-silent shape is dead (T27's R1).
                    from taskq.workflows.chain import RouterNotTotal

                    try:
                        route_fork = self._route_fork(
                            row, node, outcome_value, run_priority=run_priority
                        )
                    except RouterNotTotal as exc:
                        await emitter.aclose()
                        logger.warning(
                            "node.router_not_total",
                            run_id=str(flow_id),
                            node=row["step_key"],
                            outcome=repr(outcome_value)[:200],
                        )
                        await finalize_node(
                            self.pool,
                            self.wsql,
                            flow_id=flow_id,
                            job_id=JobId(row["id"]),
                            step_key=row["step_key"],
                            worker_id=self._worker_id,
                            attempt=attempt,
                            claim_epoch=claim_epoch,
                            outcome="failed",
                            error_class="RouterNotTotal",
                            error_message=str(exc)[:500],
                            map_index=row["map_index"],
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
        except (Snooze, RetryAfter) as exc:
            # THE DEFERRAL DISPOSITION (the vanilla consumer's own catch,
            # mirrored onto the wf finalize — see api/_runner_defer for
            # the law and the extension seam): the body's typed
            # control-flow signals re-schedule the node at the delay —
            # the attempt refunded (never burned: a deferral did not
            # execute), the budget-gated consuming face terminalised
            # through the wf terminal door, the run alive, no terminal,
            # no decrement (the join's accounting unchanged). The catch's
            # placement is the vanilla catch's own: AROUND the body
            # invocation, BEFORE the ladder's body-failure catch — the
            # signals are the body's control flow, never a failure the
            # ladder may classify. The buffer closes first (the
            # asymmetry's ordering law — the progress published before
            # the deferral survives it). THE SEAT: this is the runner's
            # STEP-body seam (the step, the chain, the map's item
            # children, the route's arms — every body this tail
            # invokes); the LOOP driver's iteration bodies have their
            # own boundary (the loop's body-failure face, unchanged).
            await emitter.aclose()
            return await self._defer_step(
                flow_id, row, attempt, ledger_id, exc, claim_epoch=claim_epoch
            )
        except Exception as exc:  # Why: the ladder's boundary — ANY body failure routes through the retry classification.
            # THE BUFFER CLOSES BEFORE THE LADDER (the asymmetry's ordering
            # law): the final flush is bounded and best-effort — it can
            # cost freshness, never the ladder's correctness.
            await emitter.aclose()
            await self._ladder_or_fail(flow_id, row, attempt, node, exc, claim_epoch=claim_epoch)
            return "laddered"
        try:
            # THE CLAIM TAIL'S MACHINERY BOUNDARY (the K7 cure — ONE
            # fault, ONE semantics): everything from here to the return
            # is the MACHINERY's own statements (the emitter close, the
            # finalize on EITHER face, the receipt write, the cache
            # store) — the body boundary above already routed the body's
            # exceptions, so ANY exception reaching THIS boundary
            # escaped the machinery. The classifier's law applies
            # symmetrically to both finalize faces: an INFRA fault (the
            # storm's ConnectionError class — :func:`is_infra_fault`)
            # is the RECLAIM class — the ledger records 'crashed' (never
            # 'failed': the ladder unburned, no failure record), the row
            # re-pends (reclaim-eligible — the cleared-fault re-drive
            # completes the step, via the HIT when cached: the body
            # still never runs), and the ESCAPE IS PRESERVED (the
            # re-raise — the driver's loop counts it, the F2-2 arm).
            # Anything else re-raises unchanged (the loud escape).
            await emitter.aclose()
            if cache_hit is not None:
                # THE HIT'S FINALIZE — THE CLAIM TAIL, OUTSIDE THE BODY
                # BOUNDARY (the unify cure, CURE 6a): a fault escaping THIS
                # finalize is the MACHINERY's seat, never the body's — the
                # body never ran, and the identical fault on the MISS path
                # (below) escapes through this exact tail (the
                # escape-point contract: the machinery boundary's
                # classifier reads WHERE the error escaped). The pre-cure
                # seat routed the hit's finalize fault through the body
                # ladder: a transient blip terminal-failed a
                # max_attempts=1 step whose body never ran (the live
                # repro: .measurements/cache-cures/hit_finalize_fault_repro.py —
                # node failed, InterfaceError on the record, the run killed
                # UnabsorbedNodeFailure). One fault, one seat: the infra
                # class — the reclaim, the ladder unburned.
                final = await self._finalize_success(
                    flow_id,
                    row,
                    attempt,
                    node,
                    cache_hit.envelope,
                    claim_epoch=claim_epoch,
                    run_priority=run_priority,
                )
                if final.applied:
                    await self._write_cache_receipt(
                        JobId(row["id"]), row["step_key"], flow_id, cache_hit
                    )
                return "succeeded"
            # THE FORK-VALIDATION RAISE IS IN THE LADDER'S TRY (rv4 F8's
            # cure): the map fork's build (and the fork's own validation,
            # insert_fork's door) runs INSIDE _finalize_success — before this
            # wrapper, the fan-in bound's exceed escaped the ladder's
            # boundary, the finalize tx rolled back, and the reclaim
            # re-crashed the node FOREVER (the wedge the conviction named).
            # The typed FanInBoundExceededError is a DETERMINISTIC authoring
            # failure: the ladder's deterministic route terminal-fails it on
            # the first attempt, the error class on the record — the reclaim
            # path heals or fails named, never wedges.
            try:
                final = await self._finalize_success(
                    flow_id,
                    row,
                    attempt,
                    node,
                    result,
                    claim_epoch=claim_epoch,
                    prebuilt_route_fork=route_fork,
                    run_priority=run_priority,
                )
            except FanInBoundExceededError as exc:
                await emitter.aclose()
                await self._ladder_or_fail(
                    flow_id, row, attempt, node, exc, claim_epoch=claim_epoch
                )
                return "laddered"
            # THE SUCCESS-ONLY STORE (T25): the miss ran the body — the
            # cache fill happens ONLY on the terminal that actually landed
            # (a fenced write is a zombie's, never a terminal-succeeded).
            # The CAS inside the store keeps ONE winner per address.
            if final.applied and cache_address is not None and result is not None:
                await self._store_step_result(cache_address, result, flow_id, row["step_key"])
            return "succeeded"
        except Exception as exc:  # Why: the MACHINERY boundary — the classifier reads WHERE the error escaped (the body's exceptions never reach this tail). BLE001 is the boundary's shape: ANY machinery exception is classified, then reclaimed or re-raised.
            if is_infra_fault(exc):
                await self._reclaim_step(flow_id, row, attempt, exc)
            raise

    async def _stamp_code_version(self, job_id: JobId, node_key: str, node: Any, body: Any) -> None:
        """The per-attempt ``code_version`` record's write (T03/§22.1):
        the claim's own stamp — the body's canonical content hash onto
        the row. Best-effort (a record, never a gate): a body the hash
        cannot read (a builtin, a partial) records as NULL, the loss
        logged; the row's claim never fails on its own audit.

        THE WRAPPER'S UNWRAP (the teardown round's cure): the
        ``@app.actor`` canonical path wires the ``WorkflowActor`` HANDLE
        into the node — the stamp read the WRAPPER's identity and
        ``getsource`` refused the instance outright (every canonical
        node recorded ``node.code-version-unstamped``, §22.1's
        deploy-drift feature dead where the docs teach the path). The
        stamp reads the INNER function through
        :func:`taskq.workflows.api._hints.inner_fn` — the one-unwrap
        seam the validator's own walk shares."""
        version = self._body_code_version(node_key, node, body)
        if version is None:
            return
        async with self.pool.acquire() as conn:
            # S608: the schema identifier is the settings boundary the
            # runner validated at build; the values are $-bound.
            await conn.execute(
                f'UPDATE "{self.schema}".jobs SET code_version = $2 WHERE id = $1',  # noqa: S608
                job_id,
                version,
            )

    def _body_code_version(self, node_key: str, node: Any, body: Any) -> str | None:
        """The body's §22.1 canonical hash — the pure computation the
        per-attempt RECORD's write (above) and the cross-run cache's
        ADDRESS (T25) both consume; ``None`` when the hash cannot read
        the body (a builtin, a partial, an unreadable source): the
        record's loss is logged, and an UNADDRESSABLE body caches
        NOTHING (the cache seam decides — a key that cannot be
        reproduced is the stale-truth hazard itself)."""
        from taskq.workflows._version import compute_code_version
        from taskq.workflows.api._hints import inner_fn, own_source

        target = (
            body
            if body is not None
            else (getattr(node, "body", None) if node is not None else None)
        )
        if target is None:
            return None  # the join/gather kinds: the identity packer is the engine's own code
        target = inner_fn(target)
        # THE SOURCE READS' ONE HOME (own_source — the unwrap + the
        # slice-identity guard): the fence's exec'd bodies re-slice to
        # PROSE — a prose stamp is a hash nobody can reproduce, the
        # same unreadable-source skip the validator's walk keeps.
        source = own_source(target)
        if source is None:
            logger.warning(
                "node.code-version-unstamped",
                node=node_key,
                error="the body's own source is not verifiable (unreadable, or not its own slice)",
            )
            return None
        try:
            return compute_code_version(
                getattr(target, "__module__", "") or "",
                getattr(target, "__qualname__", getattr(target, "__name__", "")) or "",
                source,
            )
        except (
            Exception
        ) as exc:  # Why: the record's asymmetry — a hash loss is logged, never a node failure.
            logger.warning(
                "node.code-version-unstamped",
                node=node_key,
                error=str(exc)[:200],
            )
            return None

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
        keys — no decl of their own) inherits its SOURCE's declaration;
        a ROUTE child (the fork-spawned ``<src>.item:<type-tag>``, T27)
        inherits the same way (the ``.item:`` segment is the derived
        namespace's marker)."""
        if node is not None and getattr(node, "progress_schema", None) is not None:
            return node.progress_schema
        if node_key.endswith(".item"):
            source = self.compiled.nodes.get(node_key[: -len(".item")])
            if source is not None and source.progress_schema is not None:
                return source.progress_schema
        if ".item:" in node_key:
            source = self.compiled.nodes.get(node_key.split(".item:", 1)[0])
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
        compiled NodeDecl — their bodies live in the definition only.

        THE VANISHED-NODE WEDGE (the structural redeploy — rv5-t6's
        cure): a key in NEITHER the compiled graph nor the registered
        definition raises the TYPED :class:`VanishedNodeError` — the
        redeploy renamed/removed the node while this run was in flight,
        and no re-run can ever resolve it. The claimants of the wedge
        (``_run_node`` / :meth:`run_fleet_claimed_step`) route the typed
        error into the ladder's DETERMINISTIC terminal — the node
        terminal-fails with the class on the record, the root follows,
        never an eternal zombie."""
        from taskq.workflows.definitions import resolve_step_body

        try:
            return resolve_step_body(self.compiled.name, node_key)
        except KeyError:
            pass
        if node is None:
            raise VanishedNodeError(
                f"step {node_key!r} is not in workflow "
                f"{self.compiled.name!r}'s compiled graph nor in its "
                "registered definition — THE VANISHED-NODE REDEPLOY "
                "WEDGE: a redeploy renamed or removed this node while "
                "the run was in flight (the registry is the truth — the "
                "only body source, D1; a re-run can never resolve a key "
                "it does not carry). The remedy is cancel-then-restart: "
                "cancel this run and restart the work on the new deploy "
                "under a fresh run key"
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
                key: str = r["step_key"]  # pyright: ignore[reportUnknownVariableType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
                ordered.append(
                    (key, await self._decode_with_detail(conn, key, r["id"], r["result"]))  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                )
        return ordered, dict(ordered)

    async def _decode_with_detail(
        self, conn: ConnLike, step_key: str, node_id: Any, raw: object
    ) -> object:
        """THE BOUNDED COLLECT'S DETAIL RE-READ (the F4 cure's read-side
        reconciliation): a parent whose result row is the TRUNCATED
        envelope (the bounded summary — the cap kept the row honest)
        re-reads the FULL collect from the detail_home face the marker
        names — the children's own result rows, through the SAME
        ordered parent read the join packed (edge order, map children
        distinct). The consumer's typed decode sees the declared
        ``list[...]`` with EVERY item — the summary row bounded, the
        detail never lost. Non-truncated rows decode directly (the
        common case pays one marker check)."""
        decoded = decode_result(raw)
        if result_truncation(raw) is None:
            return decoded
        rows = await conn.fetch(
            render_sql(PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, self.schema), node_id
        )
        values: list[object] = [decode_result(r["result"]) for r in rows]  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]  # Why: the Record walk's boundary.
        node = self.compiled.nodes.get(step_key)
        if (
            node is not None
            and node.kind == "gather"
            and values
            and all(isinstance(v, list) for v in values)
        ):
            # THE GATHER'S FLATTEN, mirrored (the identity packer's own
            # rule — the join row's pre-truncation value was the FLAT
            # list; the re-read must hand the body the SAME shape).
            values = [item for v in values for item in cast("list[object]", v)]
        return values

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
        return coerce_arg(raw, param=param)

    async def cancel_workflow(
        self,
        flow_id: JobId,
        *,
        reason: str | None = None,
        principal: Any = None,
    ) -> int:
        """P3 rule 4's cancel (T10): ONE transaction per run — the flow
        flip is the linearization point; every non-terminal node + held
        signal resolves in the same snapshot; running children take the
        existing two-phase cooperative cancel; terminal rows untouched;
        IDEMPOTENT (cancel twice = one cancel — a terminal root updates
        nothing). THE AUDIT (G4): "who cancelled this" is a ROW, not a
        log line.

        THE CASCADE IS THE ROWS' OWN (rv5-C2b/C4's cure — the SAME walk
        the id-addressed verb carries): the worklist walk, never Python
        recursion (a 1200-deep cascade cancels clean), and the children
        are read from the ROWS' crash-safe ``parent_flow_id`` direction
        FIRST (the receipts the accelerator, the rows the law — the
        crash between create and receipt-write no longer orphans a
        child from the cascade)."""
        from taskq.workflows.api._runner_exit import cancel_cascade

        return await cancel_cascade(
            self.pool, schema=self.schema, flow_id=flow_id, reason=reason, principal=principal
        )

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
        prebuilt_route_fork: ForkSpec | None = None,
        run_priority: int | None = None,
    ) -> FinalizeResult:
        """The success finalize (the fenced terminal + the ledger row +
        the fork): the FinalizeResult is RETURNED now (T25) — the
        cross-run cache's SUCCESS-ONLY store and the hit path's receipt
        merge both key on ``applied`` (a fenced write is a zombie's,
        never a terminal-succeeded). ``run_priority`` threads the RUN's
        per-call override into the fork build (the wf-priority cure —
        the map's children resolve it as the law's first level)."""
        fork: ForkSpec | None = None
        if node is not None and node.map_item is not None:
            fork = self._map_fork(row, node, result, run_priority=run_priority)
        elif node is not None and node.map_arms is not None and prebuilt_route_fork is not None:
            # THE TYPED ROUTE'S FORK (T27) — built at the body path from
            # the LIVE elements (the arm dispatch reads the element's
            # runtime TYPE; the encoded envelope's dicts cannot).
            fork = prebuilt_route_fork
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
            # THE TRUNCATION'S RECEIPT (the F4 cure's read-side
            # reconciliation): a bounded collect's envelope unwraps at
            # the consumer's typed boundary (decode_result's key-set
            # door), so the ``__truncated__`` count surfaces HERE — the
            # join row's metadata — for the reads that decide, never
            # swallowed by the unwrap. The hit path's cached envelope
            # (byte-equal to the producing run's) carries the same face.
            if result is not None and "__truncated__" in result:
                await self._write_collect_receipt(
                    JobId(row["id"]),
                    row["step_key"],
                    flow_id,
                    int(result["__truncated__"]),  # pyright: ignore[reportAny,reportArgumentType]  # Why: the envelope's own marker (the bounded collect's write) — the int is the marker's contract.
                )
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {"outcome": "succeeded", "step_key": row["step_key"]},
            )
        return final

    async def _store_step_result(
        self, address: str, result: dict[str, object], flow_id: JobId, node_key: str
    ) -> None:
        """THE SUCCESS-ONLY STORE (T25): the terminal-succeeded body's
        result fills the cache — the CAS inside keeps ONE winner per
        address (a fresh winner never loses its row; an expired corpse
        is re-filled). Best-effort (the projection's asymmetry): a lost
        store is a logged freshness loss (the next run re-executes —
        the cache's miss face is ALWAYS safe), never a node failure."""
        from taskq.workflows._cache import STEP_CACHE_TTL_DEFAULT_S, store_step_cache

        node = self.compiled.nodes.get(node_key)
        ttl = (
            float(node.cache_ttl)
            if node is not None and node.cache_ttl is not None
            else STEP_CACHE_TTL_DEFAULT_S
        )
        try:
            winner = await store_step_cache(
                self.pool,
                self.wsql,
                address=address,
                envelope=result,
                run_id=flow_id,
                ttl_s=ttl,
            )
        except Exception as exc:  # Why: the asymmetry — a lost store degrades to the miss face, never a node failure.
            logger.warning(
                "step-cache-store-lost",
                run_id=str(flow_id),
                node=node_key,
                error=str(exc)[:200],
            )
            return
        if not winner:
            logger.debug(
                "step-cache-cas-lost",
                run_id=str(flow_id),
                node=node_key,
                detail="a fresher winner holds the address (the CAS's loser)",
            )

    async def _write_cache_receipt(
        self, job_id: JobId, node_key: str, flow_id: JobId, hit: CachedStep
    ) -> None:
        """THE RECEIPT'S WRITE (T25): the hit run's node row carries the
        provenance (the address + the producing run's id + the store
        instant) in its metadata. Best-effort (the projection's
        asymmetry — a lost receipt is a logged freshness loss, never a
        node failure; the LEDGER owns the state)."""
        from taskq.workflows._cache import cache_receipt_metadata, merge_cache_receipt

        try:
            await merge_cache_receipt(
                self.pool,
                self.wsql,
                job_id=job_id,
                receipt=cache_receipt_metadata(
                    address=hit.address,
                    run_id=hit.run_id,
                    stored_at=hit.stored_at,
                ),
            )
        except (
            Exception
        ) as exc:  # Why: the asymmetry — a lost receipt degrades the audit, never correctness.
            logger.warning(
                "step-cache-receipt-lost",
                run_id=str(flow_id),
                node=node_key,
                error=str(exc)[:200],
            )

    async def _write_collect_receipt(
        self, job_id: JobId, node_key: str, flow_id: JobId, truncated: int
    ) -> None:
        """THE TRUNCATION'S RECEIPT (the F4 cure's read-side
        reconciliation): the bounded collect's ``__truncated__`` count
        rides the join row's METADATA (``wf_collect_truncated`` — the
        wf_cache_hit receipt's own shape), because the decode's
        key-set door hands the consumer the declared ``list[...]`` and
        can never hand it the markers too. The raw row keeps the
        markers (the detail_home pointer included); the receipt is the
        READ face's own copy. Best-effort (the projection's asymmetry —
        a lost receipt is a logged freshness loss, never a node
        failure; the LEDGER owns the state)."""
        try:
            async with self.pool.acquire() as conn:
                # S608: the schema identifier is the settings boundary
                # the runner validated at build; the values are $-bound.
                await conn.execute(
                    render_sql(COLLECT_TRUNCATION_RECEIPT_SQL, self.schema),
                    job_id,
                    dumps_jsonb_str({"wf_collect_truncated": {"count": truncated}}),
                )
        except (
            Exception
        ) as exc:  # Why: the asymmetry — a lost receipt degrades the audit, never correctness.
            logger.warning(
                "collect-truncation-receipt-lost",
                run_id=str(flow_id),
                node=node_key,
                truncated=truncated,
                error=str(exc)[:200],
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
        self,
        row: dict[str, Any],
        node: Any,
        result: dict[str, object] | None,
        *,
        run_priority: int | None = None,
    ) -> ForkSpec | None:
        """The map source's finalize fork: N children (the item body,
        one per list element — fresh jobs, per-item ledger identity) +
        the join whose consumers are the wiring's downstream nodes (the
        outbox carries the consumer bindings — the cascade, cut #1's
        cure). ``None`` when the body returned no list (the map over
        nothing — the join fires empty, the collect states it).

        ``run_priority`` is the RUN's per-call override (the wf-priority
        cure): the children AND the join land at the RESOLVED priority —
        the override > the source node's declaration > the column
        default (the children share the source's cohort, so the source's
        resolution is theirs). A map body wired as an ``@app.actor``
        HANDLE adds ITS declared priority as the CHILDREN'S actor level
        (the review F3 cure — never folded into the source node); the
        join stays the source's resolution."""
        from taskq.workflows._emit import MapIndexExhaustedError
        from taskq.workflows._types import MAP_INDEX_CEILING, ConsumerBinding

        raw_items = result.get("value") if result is not None else None
        items: list[object] = (
            list(raw_items) if isinstance(raw_items, list) else []  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk — the envelope's value is the body's own return, jsonb-round-tripped.
        )
        # THE SMALLINT CEILING AT THE FORK'S DOOR (the typed death's
        # other seat — the emit batch's own guard is in _emit.py): the
        # map children's ledger identity rides map_index (a smallint) —
        # a map over more than CEILING+1 items dies as the raw DataError
        # MID-FINALIZE-TX (the whole fork rolled back, the raw error
        # laddered). The named refusal, BEFORE the tx: the record's
        # identity cannot grow past the column's domain.
        if len(items) > MAP_INDEX_CEILING + 1:
            raise MapIndexExhaustedError(
                f"the map source {row['step_key']!r} returned "
                f"{len(items)} items — record #{MAP_INDEX_CEILING + 1} is "
                f"past the map_index smallint ceiling "
                f"({MAP_INDEX_CEILING}); the fork refuses BEFORE the "
                "finalize tx (the ceiling is documented on "
                "MAP_INDEX_CEILING — split the source's page)"
            )
        join_key = f"{row['step_key']}.join"
        # THE CHILDREN'S RESOLVED PRIORITY (the wf-priority cure): the
        # map's children share the source's cohort — the source's
        # resolution is theirs (per-call override > the source node's
        # declaration > the column default).
        # THE MAP BODY HANDLE'S OWN LEVEL (review F3's cure — the
        # never-a-silent-drop law, the capacity terms' own H-A4 shape): a
        # map body wired as an ``@app.actor(priority=…)`` HANDLE carries
        # its declaration as the CHILDREN'S actor level — read here, at
        # the fork, NEVER folded into the source node (the source's row
        # and the join are the source's own cohort; the handle's value
        # would cross-contaminate them).
        from taskq.workflows.api._app import (
            WorkflowActor,  # the cycle-break (inner_fn's own pattern)
        )

        map_body_priority = (
            node.map_item.priority if isinstance(node.map_item, WorkflowActor) else None
        )
        child_priority = _resolve_priority(run_priority, map_body_priority, node.priority)
        # THE JOIN'S LEVEL = THE SOURCE'S RESOLUTION (the map body
        # handle's declaration governs the CHILDREN only — the join stays
        # the source's own).
        source_priority = _resolve_priority(run_priority, node.priority)
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
                    # THE ADMISSION TERMS STAMPED (CURE 2): the map's
                    # declared buckets' NAMES on every child row — the
                    # claim path's read.
                    rate_limits=rate_limit_names(
                        node.map_rate_limits, what="map_source's rate_limits"
                    ),
                    # THE STRICT CAPACITY TERMS STAMPED (H-A1): the map's
                    # children share the source node's cohort, so the
                    # source's declared reservations ride their rows — the
                    # same stamp the static row carries.
                    reservations=reservation_names(
                        tuple(node.reservations), what=f"map {row['step_key']!r}'s reservations"
                    ),
                    # THE CHILD'S RESOLVED PRIORITY (the wf-priority
                    # cure): the source's resolution (above) rides every
                    # child row.
                    priority=child_priority,
                )
                for i, item in enumerate(items)
            ),
            join=JoinSpec(
                step_key=join_key,
                actor=node.actor,
                queue=node.map_queue,
                consumers=bindings,
                failure_policy=node.map_on_failure,  # type: ignore[arg-type]  # Why: the policy vocabulary is the compile's own — validated by the engine's validators.
                # THE FAN-IN BOUND'S ESCAPE, EXPOSED AT THE WIRING (the
                # docshelf team's F6): the map_source's child_driven= —
                # past the bound the fire counts its terminal children
                # FROM THE EDGE LEDGER (the choice recorded on the joined
                # node), never the counter cache.
                child_driven=node.map_child_driven,
            ),
            max_attempts=node.map_max_attempts,
            # THE JOIN'S PRIORITY = THE SOURCE'S RESOLUTION (the
            # wf-priority cure): the join is the source's own cohort — the
            # map body handle's declaration governs the CHILDREN only.
            priority=source_priority,
        )

    def _route_fork(
        self,
        row: dict[str, Any],
        node: Any,
        outcome_value: object,
        *,
        run_priority: int | None = None,
    ) -> ForkSpec | None:
        """THE TYPED ROUTE'S FORK (T27 — the map fork's arm-dispatch
        sibling): the source's body returned the elements' LIST; each
        element's RUNTIME type tag picks its arm and the fork stamps the
        child's own derived step key (``<source>.item:<type-tag>`` — the
        row NAMES its arm), the arm's placement (R4: the per-arm
        actor/queue), and the element's jsonb under the item key (the
        arm body's declared param type is the decode's target — R3).

        THE LOUD DOOR (R1 — the skip-silent shape is DEAD): an element
        whose type has NO arm raises :class:`RouterNotTotal` BEFORE any
        row is written — the caller converts it to the source's
        terminal-FAILED finalize with ``error_class='RouterNotTotal'``
        (the chain face's exact shape; nothing routes, the run fails).

        ``None`` when the body returned no list (the route over nothing —
        no fork is carried at all). An EMPTY LIST is NOT that case (the
        rv4 F1 cure — the empty-join precedent): the empty list IS the
        typed sum's honest value, so the fork fires its join with zero
        children — the join is born ``deps_pending=0``, fires
        immediately, and ``result() == []`` (the run terminal, SUCCESS
        truthful — a night with no documents chunked zero documents, and
        that is correct). The pre-cure ``None``-on-empty wedged the run
        (the join row never existed, the consumers' reserved deps never
        released — stuck ``running`` with zero error rows) or
        terminalized succeeded with the lying ``result() is None``.

        ``run_priority`` is the RUN's per-call override (the wf-priority
        cure): each ARM's children resolve the law's levels — the
        override > the ARM's own ``priority=`` (the down-override) > the
        arm body HANDLE's declaration (the actor level — review F3's
        cure, read per arm and never folded into the source node) > the
        source node's declaration > the column default; the JOIN (the
        source's own cohort — the arm overrides never retouch it) lands
        at the SOURCE's resolution."""
        from taskq.workflows._types import ConsumerBinding
        from taskq.workflows.chain import RouterNotTotal, type_tag

        if not isinstance(outcome_value, list):
            return None
        items: list[object] = cast(
            "list[object]", outcome_value
        )  # Why: the isinstance narrowed the object-typed body return to list[Unknown] — the cast IS the declared laundering (the element walk below re-narrows per element; the codec's own seam shape).
        # THE SOURCE'S RESOLUTION (the wf-priority cure): the JOIN's
        # level — the arm overrides apply to the arm CHILDREN, never the
        # join (the join is the source's own cohort, the placement
        # fields' law).
        source_priority = _resolve_priority(run_priority, node.priority)
        # THE PER-ARM HANDLE LEVEL (review F3's cure — the
        # never-a-silent-drop law, the capacity terms' own H-A4 shape):
        # an arm body wired as an ``@app.actor(priority=…)`` HANDLE
        # carries its declaration as THAT ARM'S actor level — read per
        # arm here, at the fork, NEVER folded into the source node (the
        # other arms and the join would cross-contaminate).
        from taskq.workflows.api._app import (
            WorkflowActor,  # the cycle-break (inner_fn's own pattern)
        )

        arms_typed = cast(
            "dict[str, Any]", node.map_arms or {}
        )  # Why: the compiled NodeDecl is Any-typed here — the cast names the arms' dict shape the walk reads.
        arm_handle_priorities: dict[str, int | None] = {
            arm_tag: arm.body.priority if isinstance(arm.body, WorkflowActor) else None
            for arm_tag, arm in arms_typed.items()
        }
        join_key = f"{row['step_key']}.join"
        bindings = tuple(
            ConsumerBinding(
                step_key=downstream,
                actor=self.compiled.nodes[downstream].actor,
                queue=self.compiled.nodes[downstream].queue,
                payload={},
                # THE CONSUMER'S OWN EDGE POLICY (the map-join consumption
                # cure): the fork writes the join→consumer EDGE with it —
                # T06's propagation reads the policy off the ledger when
                # the JOIN terminal-fails.
                failure_policy=self.compiled.nodes[downstream].on_failure,
            )
            for downstream in sorted(self.compiled.nodes)
            if join_key in self.compiled.nodes[downstream].parents
        )
        children: list[ChildSpec] = []
        for i, element in enumerate(items):
            tag = type_tag(type(element))
            arm = node.map_arms.get(tag) if node.map_arms is not None else None
            if arm is None:
                raise RouterNotTotal(
                    f"element #{i} of {row['step_key']!r} is "
                    f"{type(element).__name__!r} — no route arm for it (the "
                    "route is total over the source's declared union "
                    "members): this is a defect, not a dead end — the body "
                    "lied about its union, or the union drifted from the "
                    "route. NOTHING routes: the run fails loudly (the "
                    "skip-silent shape is dead — T27's R1)."
                )
            children.append(
                ChildSpec(
                    step_key=route_child_key(str(row["step_key"]), tag),
                    actor=arm.actor or node.actor,
                    queue=arm.queue or node.map_queue,
                    payload={ITEM_KEY: encode_data_arg(element)},
                    map_index=i,
                    # THE ARM'S ADMISSION TERMS STAMPED (CURE 2): the arm's
                    # own buckets' NAMES on ITS children's rows — and the
                    # BARE arm (no own rate_limits, no actor override)
                    # INHERITS THE SOURCE'S rate_limits (C5's
                    # symmetrization: the reservations' own ride-the-cohort
                    # law, the same stamp the source's rows carry). An arm
                    # declaring its own keeps its OWN — never a silent
                    # merge.
                    rate_limits=(
                        rate_limit_names(arm.rate_limits, what="route arm's rate_limits")
                        or (
                            ()
                            if (arm.actor is not None and arm.actor != node.actor)
                            else rate_limit_names(
                                tuple(node.rate_limits), what="the source node's rate_limits"
                            )
                        )
                    ),
                    # THE ARM'S STRICT CAPACITY TERMS STAMPED (H-A1 +
                    # H-A4's passthrough): the arm's own declaration rides
                    # ITS rows; an arm declaring none KEEPS the source
                    # node's stamp (the arm rides the source's cohort) and
                    # an arm OVERRIDING the actor stamps none (a different
                    # cohort — inheriting the source's cap onto it would
                    # be the silent lie; the wiring refused a capped body
                    # whose terms that override would drop).
                    reservations=(
                        reservation_names(tuple(arm.reservations), what="route arm's reservations")
                        or (
                            ()
                            if (arm.actor is not None and arm.actor != node.actor)
                            else reservation_names(
                                tuple(node.reservations), what="the source node's reservations"
                            )
                        )
                    ),
                    # THE CHILD'S RESOLVED PRIORITY (the wf-priority
                    # cure): the arm's DOWN-OVERRIDE — the override > the
                    # arm's own declaration > the arm body HANDLE's
                    # declaration (the actor level — review F3's cure)
                    # > the source node's declaration > the column
                    # default. ``None`` on a level means it declares
                    # nothing (the arm overrides DOWN from the source,
                    # never up).
                    priority=_resolve_priority(
                        run_priority,
                        arm.priority,
                        arm_handle_priorities.get(tag),
                        node.priority,
                    ),
                )
            )
        spec = ForkSpec(
            children=tuple(children),
            join=JoinSpec(
                step_key=join_key,
                actor=node.actor,
                queue=node.map_queue,
                consumers=bindings,
                failure_policy=node.map_on_failure,  # type: ignore[arg-type]  # Why: the policy vocabulary is the compile's own — validated by the engine's validators (the map fork's own declaration, the same seam).
                # THE FAN-IN BOUND'S ESCAPE (F6 — the same face the map
                # fork carries): the route's child_driven= rides ITS join.
                child_driven=node.map_child_driven,
            ),
            max_attempts=node.map_max_attempts,
            # THE JOIN'S PRIORITY = THE SOURCE'S RESOLUTION (the
            # wf-priority cure — the arm overrides never retouch it).
            priority=source_priority,
        )
        # THE VALIDATED FORK (rv4 F8's cure — the raise INTO the ladder's
        # try): the fork validates HERE, at the body path — this call
        # site sits inside the execution ladder's try, so the fan-in
        # bound's exceed is the DETERMINISTIC named refusal
        # (FanInBoundExceededError → the immediate named terminal), never
        # the mid-finalize raise the reclaim re-crashed forever.
        validate_fork(spec)
        return spec

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
        node finalizes.

        THE FAILURE FACE (the teardown round's cure): a FAILED run's
        read raises the typed :class:`WorkflowRunError` carrying the
        failing node row's error class + message — the failed run's
        first question (WHAT failed, and why) answered AT the read face,
        never the empty read that made failed and running
        indistinguishable."""
        status = await self._flow_status(flow_id)
        if status == "failed":
            raise await self._failure_verdict(flow_id)
        terminal = self.compiled.terminal
        if terminal is None:
            raise WorkflowRunError(
                f"workflow {self.compiled.name!r} names no terminal — "
                "wire one (wf.build(p)) to read a result"
            )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                render_sql(TERMINAL_RESULT_SQL_TEMPLATE, self.schema), terminal, flow_id
            )
            if row is None:
                return None
            # THE BOUNDED COLLECT'S DETAIL RE-READ (the F4 read-side
            # reconciliation): a truncating JOIN as the terminal node
            # reads the full collect off the children's rows — the
            # decoded answer is the declared shape, never the bounded
            # summary's envelope.
            return await self._decode_with_detail(
                conn,
                str(row["step_key"]),  # pyright: ignore[reportUnknownArgumentType,reportAny]  # Why: the Record walk's boundary.
                row["id"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                row["result"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
            )

    async def _failure_verdict(self, flow_id: JobId) -> WorkflowRunError:
        """The failed run's typed verdict (the error the read raises):
        the newest terminal-FAILED node row's class + message — the
        rows are the truth, the propagation verdict on the root
        (``UnabsorbedNodeFailure``) is the CASCADE's face, never the
        body's own failure."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(render_sql(FLOW_FAILURE_SQL_TEMPLATE, self.schema), flow_id)
        if row is None:
            return WorkflowRunError(
                f"workflow run {flow_id} FAILED — no failing node row "
                "found (the root carries the verdict; inspect the run's "
                "rows: taskq flows status "
                f"{flow_id})"
            )
        return WorkflowRunError(
            f"workflow run {flow_id} FAILED — node {row['step_key']!r} "
            f"terminal-failed: [{row['error_class']}] "
            f"{row['error_message'] or '(no message)'} — the result face "
            "is the failure's face: inspect the run's rows (taskq flows "
            f"status {flow_id})"
        )

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

"""THE CHILD FLOWS (T28) — ``ctx.spawn_flow`` + the join hold: a step
spawns a SUB-FLOW (a REAL flow — its own run rows; the run explorer sees
it, the admin operates it, the CLI lists it) and JOINS its terminal (the
HITL machinery's own shape: the wait IS a hold; the child's terminal
event is the hold's resolver; the wake arm delivers + resumes; the sweep
belt reaps the orphaned waiter).

THE SPAWN = the child's ``create_flow`` INSIDE the parent's step — the
create-seam's ONE-transaction law respected BY CONSTRUCTION (the child's
root + static nodes + edges + ROOT_START land in the child create's own
transaction; the parent's step never wraps it in a second one).

THE TWO-CLAIMS LAW AT THE FLOW LEVEL: the spawn's run key is
``childflow:{parent_flow_id}:{node_key}:{slot}`` under the child's OWN
``workflow-run:<child name>`` scope — the G2 arbiter is the rememberer.
The SLOT is the spawn site's identity, STABLE ACROSS RETRIES: the body
re-executes from the top (the hold's resume, the retry's reclaim) and
re-derives the SAME keys — the arbiter conflict ADOPTS the same child,
never a duplicate (the retry-completion census's law, at the flow
boundary). A terminal child adopted as-is is the deterministic replay:
the retried body re-decides off the same terminal child.

THE SLOT'S SOURCE: the per-attempt spawn cursor on the executing node's
row (``metadata.spawn_cursor_{attempt}`` — the hold-cursor's exact
pattern: a fresh attempt restarts at 0, so the retry re-derives the same
slots). The loop's iteration is ITS OWN node row (``<loop>.iter<i>``),
so one rule covers the fan-out in a loop body: distinct iteration rows
carry distinct cursors. An explicit ``key=`` overrides the slot (the
author's identity).

THE RECEIPT (the identity, both directions): the parent's node row
carries the child's flow id (``metadata.child_flows`` — the idempotent
ensure-append, dedup'd: an adoption re-run never duplicates the
receipt); the child's ROOT row carries the parent's
(``parent_flow_id`` / ``parent_node_id`` / ``parent_step_key`` — riding
the create's ONE transaction via ``create_flow(root_metadata_extra=…)``,
never a post-create UPDATE that would re-open the orphan window).

THE JOIN = the HOLD (T10's machinery, the ``Decision | Expired``
pattern's sibling): the join site registers a hold on the PARENT's node
(signal ``__child__:{child_flow_id}``, the declared result model as the
payload contract — the contract is mandatory at hold time), marks the
ledger ``awaited``, and raises :class:`NodeHeldError` — the held
representation (pending + the deadline + the signal row as truth; NO
terminal, NO ledger failure), the worker releases the slot. The child's
terminal is the hold's RESOLVER: :func:`sweep_child_joins` (a sweep arm,
wired after the root-maintain leg) CAS-delivers the held join, resumes
the parent node, and broadcasts :data:`CHILD_TERMINAL_CHANNEL` (the T26
transport — the pointer-only payload). The resumed body re-executes from
the top; the join site reads THE CHILD ROW FIRST (the row is the truth)
and returns the CLOSED UNION member:

* ``ChildOk(result=…)`` — the child's typed result decoded into the
  parent's body (R3's face at the flow boundary);
* ``ChildFailed`` — the child terminal-failed (or its rows are GONE —
  the fail-close face; NEVER a silent swallow, never a raised error:
  the body's own arm decides);
* ``ChildCancelled`` — the child was cancelled (the cascade: the
  parent's cancel walks the receipts and cancels each child —
  ``cancel_workflow_run``'s extension);
* ``Expired`` — the JOIN's own patience expired (the hold's deadline,
  armed by ``wait(timeout_s=…)``; the face fires ONCE per abandoned row
  per attempt — the hold→expire→re-hold→∞ dragon's kill site, T26's
  amendment verbatim).

THE CHECKER FORCES THE ARMS: the wait returns
``ChildOutcome[M] | Expired`` — a body that matches only the ``ChildOk``
arm falls through and reds against its declared return; a body that
bare-unwraps reds the attribute.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows.api._ctx_wait import Expired, NodeHeldError
from taskq.workflows.api._runner_codec import decode_result
from taskq.workflows.api._sql_runner import render_sql

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "DEFAULT_SPAWN_MAX_DEPTH",
    "ChildCancelled",
    "ChildFailed",
    "ChildFlowLostError",
    "ChildHandle",
    "ChildOk",
    "ChildOutcome",
    "ChildWorkflowUnknownError",
]

from taskq.workflows.api._hitl import CHILD_JOIN_SIGNAL_PREFIX

#: The verdict set the join reads as the child's TERMINAL (the state
#: machine's own vocabulary, re-read from the terminal-statuses door at
#: call time — never a re-spelled literal drift).
_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})


class ChildWorkflowUnknownError(RuntimeError):
    """``ctx.spawn_flow`` named a workflow the registry does not carry —
    the typed refusal (the registration door — ``app.get(name)`` — is
    the only body source, D1)."""


class ChildFlowLostError(RuntimeError):
    """THE RETENTION-RESPAWN FAIL-CLOSE: the
    parent's resume found a DANGLING child pointer — the parent's row
    still names a child flow whose rows are GONE (the retention purge's
    own law: the purge must walk the parent links first).

    The pre-cure shape SILENTLY RE-MINTED a fresh child on the resume —
    the child's side effects (emails, payments) ran twice while the
    parent run SUCCEEDED (the worst face: the fabricated health). The
    spawn now REFUSES: the dangling pointer is the failure, the run
    terminal-fails with this class on the record — never a silent
    respawn. A DETERMINISTIC death (re-running cannot restore the
    purged rows — the deterministic-authoring-failure ladder's own
    routing)."""


class SpawnDepthExceededError(ValueError):
    """THE DEPTH BOUND (``FanInBoundExceededError``'s
    sibling): the spawn's ancestor walk (the rows' ``parent_flow_id``
    links, the cycle detection folded into the same walk) reached the
    declared max depth — a self-spawning flow can no longer mint a
    generation per sweep tick forever, and a corrupted (cyclic) parent
    link cannot run the walk forever either.

    A DETERMINISTIC authoring failure (the ladder's fourth class' own
    law): re-running cannot shrink the ancestor chain — the retry ladder
    never burns; the node terminal-fails with this class on the record.
    The remedy: bound the fan-out (raise ``max_depth=`` at the spawn
    site, or flatten the recursion into a loop over a ledgered step)."""


#: THE DECLARED MAX DEPTH (the spawn's default bound): the
#: ancestor chain the spawn walks is bounded HERE; a deeper fan-out is
#: the typed refusal's, never an unbounded generation machine. A spawn
#: site that genuinely needs more depth declares ``max_depth=`` — the
#: author's identity, recorded on the spawn.
DEFAULT_SPAWN_MAX_DEPTH: Final[int] = 32


# ── the typed outcome (the closed union's members) ──────────────────────


@dataclass(frozen=True, slots=True)
class ChildOk[G: BaseModel]:
    """The child terminal-SUCCEEDED: ``result`` is the child's typed
    result — decoded from the child's terminal node row (R3's face at
    the flow boundary). The members are IN-MEMORY verdicts (never rows —
    dataclasses, not pydantic: nothing here round-trips the wire)."""

    flow_id: str
    result: G


@dataclass(frozen=True, slots=True)
class ChildFailed:
    """The child terminal-FAILED (or its rows are gone — the fail-close
    face). The verdict is a MEMBER, never a raised error: the body's own
    arm decides (fail the step, substitute, finish with what it has)."""

    flow_id: str
    error_class: str
    error_message: str


@dataclass(frozen=True, slots=True)
class ChildCancelled:
    """The child was CANCELLED (the cascade: the parent's cancel walks
    the receipts)."""

    flow_id: str


#: The closed union the checker forces (the ``Decision | Expired``
#: pattern's sibling).
type ChildOutcome[G: BaseModel] = ChildOk[G] | ChildFailed | ChildCancelled


# ── the spawn site's typed host (the same seam _ctx_wait spells) ────────


class _SpawnHost(Protocol):
    """The context surface the spawn machinery reads."""

    flow_id: JobId
    job_id: JobId
    node_key: str
    attempt: int
    _pool: asyncpg.Pool
    _wsql: WorkflowSql
    _ledger_id: JobId | None


@dataclass(frozen=True, slots=True)
class JoinSite:
    """The PARENT's wait-site identity: the fields the join hold and the
    child reads key on (public — the handle reads its own record, never
    reaches into the context's privates)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    flow_id: JobId
    job_id: JobId
    node_key: str
    attempt: int
    ledger_id: JobId | None


class ChildHandle[G: BaseModel]:
    """The spawn's receipt: the child flow's id + the JOIN face. The
    handle is inert data + one method — every fact is a ROW."""

    __slots__ = ("_result_model", "_site", "_terminal_key", "flow_id")

    def __init__(
        self,
        *,
        site: JoinSite,
        flow_id: JobId,
        result_model: type[G],
        terminal_key: str | None,
    ) -> None:
        self._site = site
        self.flow_id = flow_id
        self._result_model = result_model
        self._terminal_key = terminal_key

    async def wait(self, *, timeout_s: float | None = None) -> ChildOutcome[G] | Expired:
        """THE JOIN (the wait IS the hold): a terminal child returns the
        typed member immediately (the row is the truth — the resume's
        pure row-replay face); a live child registers the join hold and
        raises :class:`NodeHeldError` (the held representation — the
        slot released, the ledger 'awaited', never a failed attempt).

        ``timeout_s=`` arms the hold's deadline (the DB clock): the
        expiry sweep abandons the hold, the body resumes, and THIS site
        returns the :class:`Expired` member ONCE per abandoned row per
        attempt (the face-marker pattern). ``None`` waits as long as the
        child lives — safe by the cascade (a cancelled parent cancels
        its children, so no join outlives its flow)."""
        site = self._site
        signal_name = f"{CHILD_JOIN_SIGNAL_PREFIX}{self.flow_id}"
        async with site.pool.acquire() as conn:
            # THE ROW IS THE TRUTH: the child's terminal read FIRST — a
            # terminal child needs no hold at all (this is the resume's
            # pure row-replay face, and the crash-mid-wait's wake: the
            # successor's body re-executes, reads, joins).
            root = await conn.fetchrow(
                render_sql(_CHILD_ROOT_STATUS_SQL, site.wsql.schema), self.flow_id
            )
            if root is not None:
                status = str(root["status"])
                if status == "succeeded":
                    return await self._ok_result(conn)
                if status in ("failed", "crashed", "abandoned"):
                    return await self._failed_result(conn, status)
                if status == "cancelled":
                    return ChildCancelled(flow_id=str(self.flow_id))

            # THE JOIN'S OWN TIMEOUT FACE (the abandoned hold — the
            # expiry sweep fired THIS site's hold): the Expired member,
            # ONCE per abandoned row per attempt (the face marker).
            abandoned = await conn.fetchrow(
                render_sql(
                    "SELECT id, hold_epoch FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND status = 'abandoned' ORDER BY hold_epoch DESC LIMIT 1",
                    site.wsql.schema,
                ),
                site.flow_id,
                site.node_key,
                signal_name,
            )
            if abandoned is not None:
                face_key = f"child_join_face_{site.attempt}"
                face_seen = await conn.fetchval(
                    render_sql(
                        "SELECT (metadata ->> $2)::int FROM {schema}.jobs WHERE id = $1",
                        site.wsql.schema,
                    ),
                    site.job_id,
                    face_key,
                )
                if face_seen != int(abandoned["hold_epoch"]):
                    await conn.execute(
                        render_sql(
                            "UPDATE {schema}.jobs SET metadata = metadata || $2::jsonb "
                            "WHERE id = $1",
                            site.wsql.schema,
                        ),
                        site.job_id,
                        dumps_jsonb_str({face_key: int(abandoned["hold_epoch"])}),
                    )
                    return Expired(
                        signal=signal_name,
                        node_key=site.node_key,
                        hold_id=str(abandoned["id"]),
                    )

            # THE STANDING HOLD: this join already registered (the
            # resume's re-execution BEFORE the wake) — idempotent, never
            # a second registration of one wait.
            held_row = await conn.fetchrow(
                render_sql(
                    "SELECT id FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND status = 'held' ORDER BY hold_epoch DESC LIMIT 1",
                    site.wsql.schema,
                ),
                site.flow_id,
                site.node_key,
                signal_name,
            )
            if held_row is not None:
                raise NodeHeldError(hold_id=str(held_row["id"]), signal_names=(signal_name,))

            # REGISTER: the ledger 'awaited' (resume-not-retry), then the
            # hold — the payload contract is the child's declared result
            # model (mandatory at hold time — the audit hole's law).
            from taskq.workflows.api._hitl import mark_awaited, register_hold

            if site.ledger_id is not None:
                await mark_awaited(conn, site.wsql.schema, site.ledger_id)
            epoch = await conn.fetchval(
                render_sql(
                    "SELECT COALESCE(MAX(hold_epoch), 0) + 1 FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2",
                    site.wsql.schema,
                ),
                site.flow_id,
                site.node_key,
            )
            hold_id = await register_hold(
                conn,
                schema=site.wsql.schema,
                workflow_id=site.flow_id,
                node_id=site.job_id,
                node_key=site.node_key,
                signal_name=signal_name,
                hold_epoch=int(epoch),  # pyright: ignore[reportAny, reportArgumentType]
                call_id=f"call:{new_uuid()}",
                payload_schema={
                    self._result_model.__name__: self._result_model.model_json_schema()
                },
                timeout_s=timeout_s,
                context={"kind": "child_join", "child_flow_id": str(self.flow_id)},
            )
        raise NodeHeldError(hold_id=str(hold_id), signal_names=(signal_name,))

    async def _ok_result(self, conn: ConnLike) -> ChildOk[G]:
        """The child's typed result read: the terminal node's decoded
        row result re-validated into the declared model (the decode
        once, typed end to end — the R3 face at the flow boundary)."""
        if self._terminal_key is None:  # pragma: no cover - the spawn refuses a terminal-less child
            raise ChildWorkflowUnknownError(
                f"the child flow {self.flow_id} names no terminal — the spawn "
                "refuses a terminal-less child (the join could never read a result)"
            )
        raw = await conn.fetchval(
            render_sql(_CHILD_TERMINAL_RESULT_SQL, self._site.wsql.schema),
            self._terminal_key,
            self.flow_id,
        )
        decoded = decode_result(raw)  # the envelope's unwrap — the runner's own codec seam
        result = self._result_model.model_validate(decoded)
        return ChildOk(flow_id=str(self.flow_id), result=result)

    async def _failed_result(self, conn: ConnLike, status: str) -> ChildFailed:
        """The fail-close verdict: the newest terminal-failed node row's
        error class + message; a crash/abandonment with no failed node
        row carries the maintenance verdict's own class (the rows are
        the truth — the envelope never lies about WHY)."""
        row = await conn.fetchrow(
            render_sql(_CHILD_FAILURE_READ_SQL, self._site.wsql.schema), self.flow_id
        )
        if row is None:
            return ChildFailed(
                flow_id=str(self.flow_id),
                error_class=f"ChildFlow{status.capitalize()}",
                error_message="",
            )
        return ChildFailed(
            flow_id=str(self.flow_id),
            error_class=str(row["error_class"] or f"ChildFlow{status.capitalize()}"),
            error_message=str(row["error_message"] or ""),
        )


# ── the spawn (the ctx mixin) ───────────────────────────────────────────


class CtxSpawnOps(_SpawnHost):
    """The context's spawn mixin (the machinery's home; the context
    composes it — beside :class:`CtxWaitOps`)."""

    __slots__ = ()

    async def spawn_flow[G: BaseModel](
        self,
        name: str,
        *,
        input: object = None,
        result: type[G],
        key: str | None = None,
        max_depth: int | None = None,
    ) -> ChildHandle[G]:
        """THE SPAWN (T28): ``handle = await ctx.spawn_flow("child_name",
        input=…, result=ChildResult)`` — the child is a REAL flow (its
        own run rows; the create-seam's ONE-transaction law) and the
        spawn's run key is the TWO-CLAIMS LAW at the flow level: the
        retry ADOPTS the same child, never duplicates.

        ``result=`` is the child's typed result contract (type[BaseModel])
        — the join's ``ChildOk.result`` re-validates into it; the spawn
        refuses a terminal-less child (the join could never read a
        result). ``key=`` overrides the slot (the author's identity);
        the default slot is the per-attempt spawn cursor (stable across
        retries). ``max_depth=`` raises the spawn's default depth bound
        (:data:`DEFAULT_SPAWN_MAX_DEPTH`) for a fan-out that genuinely
        needs it — the bound's typed arm is
        :class:`SpawnDepthExceededError`.

        THE SPAWN'S ATOMICITY FACE (the gather dragon's cure):
        the default slot's read+write is ONE atomic claim at the row
        (an ``UPDATE … RETURNING`` increment), so K CONCURRENT spawns
        (``asyncio.gather(*[ctx.spawn_flow(...) …])`` — the natural
        fan-out spelling) take K DISTINCT slots and mint K DISTINCT
        children — the pre-cure shape raced the cursor's read and
        write, all K spawns adopted the same slot (run key), minted ONE
        child, and every caller received tenant-0's verdict wearing K
        labels (the silent fabricated result). A SAME-key concurrent
        spawn ADOPTS the same child (the arbiter's CAS — the run-key
        claim at the row) and every caller receives THAT child's
        handle: the honest shared face, never a fabricated per-caller
        verdict. A CONCURRENT fan-out that must join STABLY across the
        resume passes ``key=`` (the slot's allocation order is the
        scheduler's, the author's identity is the caller's)."""
        from taskq.workflows._worker_execution import get_compiled_workflow

        try:
            compiled_obj = get_compiled_workflow(name)
        except KeyError:
            raise ChildWorkflowUnknownError(
                f"workflow {name!r} is not registered — the spawn resolves the "
                "child from the compile registry (the registration door: "
                "app.get(name); the definitions must be imported in THIS process)"
            ) from None
        from taskq.workflows.api._app import CompiledWorkflow

        assert isinstance(compiled_obj, CompiledWorkflow), type(compiled_obj)
        if compiled_obj.terminal is None:
            raise ChildWorkflowUnknownError(
                f"workflow {name!r} names no terminal — the spawn refuses a "
                "terminal-less child (the join could never read a result)"
            )

        # THE FAIL-CLOSE: a dangling child pointer on the
        # parent's own receipts — a receipt whose child rows are GONE —
        # is the TYPED refusal, never a silent respawn. The pre-cure
        # shape re-minted a fresh child on the resume (the purge took
        # the arbiter's row with it): the child's side effects ran
        # twice, the parent SUCCEEDED.
        async with self._pool.acquire() as conn:
            dangling = await conn.fetchval(
                render_sql(_DANGLING_RECEIPT_SQL, self._wsql.schema), self.flow_id
            )
            if dangling is not None:
                raise ChildFlowLostError(
                    f"the parent flow {self.flow_id} still names child flow "
                    f"{dangling} and that child's rows are GONE (the retention "
                    "purge). The spawn REFUSES to re-execute the child — a "
                    "silent respawn runs the child's side effects twice. The "
                    "purge must walk the parent links (the operator's law); "
                    "to proceed, clear the dangling receipt from the parent's "
                    "row and re-run the flow on the current code."
                )

        # THE DEPTH BOUND + THE CYCLE CHECK: the ancestor chain
        # read from the rows' parent links — ONE walk serves both (a
        # cycle's walk reaches the cap and fires the same bound).
        bound = DEFAULT_SPAWN_MAX_DEPTH if max_depth is None else max_depth
        async with self._pool.acquire() as conn:
            chain_depth = await conn.fetchval(
                render_sql(_ANCESTOR_DEPTH_SQL, self._wsql.schema), self.flow_id, bound
            )
        if int(chain_depth) >= bound:
            raise SpawnDepthExceededError(
                f"spawn under flow {self.flow_id} exceeds the declared max "
                f"spawn depth ({bound}): the ancestor chain walked the rows' "
                "parent links to the cap (or a CYCLE in the parent links — "
                "the same walk detects it). The remedy: bound the fan-out "
                "(flatten the recursion into a loop over a ledgered step), "
                "or declare max_depth= at the spawn site."
            )

        # THE SLOT (the atomic face): the per-attempt spawn cursor
        # is CLAIMED in one atomic statement — the increment and the
        # returned slot are one row-CAS, so concurrent spawns take
        # distinct slots. A fresh attempt re-derives the same slots (the
        # cursor is per-attempt; the arbiter adopts).
        if key is not None:
            # The author's identity IS the key: no slot claimed (the
            # parent row's counter never moves for a keyed site).
            spawn_key = f"childflow:{self.flow_id}:{self.node_key}:{key}"
        else:
            cursor_key = f"spawn_cursor_{self.attempt}"
            async with self._pool.acquire() as conn:
                slot = await conn.fetchval(
                    render_sql(_SPAWN_SLOT_CLAIM_SQL, self._wsql.schema),
                    self.job_id,
                    cursor_key,
                )
            assert slot is not None  # the claim's UPDATE hit the executing node's row
            spawn_key = f"childflow:{self.flow_id}:{self.node_key}:{int(slot)}"

        from taskq.workflows.api._runner import FlowRunner

        # NB: this runner only CREATES the child run's rows (the spawn +
        # the join's receipts) — it executes no bodies. The child's rows
        # execute through the doors that own them (the fleet's
        # ``execute_flow_job``, the dev drive's ``_runner_for_flow``) —
        # each threads the worker's default deadline itself (the wf
        # start_to_close cure's own resolution chain).
        child_runner = FlowRunner(compiled_obj, self._pool, self._wsql.schema)
        claim = await child_runner.create_flow(
            input=input,
            run_key=spawn_key,
            root_metadata_extra={
                "parent_flow_id": str(self.flow_id),
                "parent_node_id": str(self.job_id),
                "parent_step_key": self.node_key,
            },
        )
        handle = ChildHandle[G](
            site=JoinSite(
                pool=self._pool,
                wsql=self._wsql,
                flow_id=self.flow_id,
                job_id=self.job_id,
                node_key=self.node_key,
                attempt=self.attempt,
                ledger_id=self._ledger_id,
            ),
            flow_id=claim.flow_id,
            result_model=result,
            terminal_key=compiled_obj.terminal,
        )

        # THE RECEIPT (the parent's row): the ensure-append is dedup'd
        # (an adoption re-run never duplicates).
        async with self._pool.acquire() as conn:
            await conn.execute(
                render_sql(_CHILD_RECEIPT_ENSURE_SQL, self._wsql.schema),
                self.job_id,
                str(claim.flow_id),
            )
        return handle


# ── the named statements (never an inline f-string — the schema is the
# bundle's validated identifier; every value is $-bound) ─────────────────

_CHILD_RECEIPTS_SQL_TEMPLATE = """
SELECT DISTINCT jsonb_array_elements_text(metadata->'child_flows') AS child_id
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND metadata ? 'child_flows'
"""

_CHILD_RECEIPT_ENSURE_SQL = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(
    metadata,
    '{child_flows}',
    CASE WHEN metadata->'child_flows' ? $2::text
         THEN metadata->'child_flows'
         ELSE COALESCE(metadata->'child_flows', '[]'::jsonb) || to_jsonb($2::text)
    END
)
WHERE id = $1
"""

#: THE SPAWN'S ATOMIC SLOT CLAIM (the gather dragon's cure):
#: the per-attempt cursor's read+write is ONE statement — the row's
#: jsonb counter increments IN the UPDATE and the allocated slot
#: RETURNS from it. Concurrent claims serialize on the row lock and
#: each receives a DISTINCT slot; a same-key spawn never reaches this
#: statement (the explicit key bypasses the slot).
_SPAWN_SLOT_CLAIM_SQL = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(
    metadata,
    ARRAY[$2::text],
    to_jsonb(COALESCE((metadata->>$2)::int, -1) + 1)
)
WHERE id = $1
RETURNING (metadata->>$2)::int
"""

#: THE DANGLING-RECEIPT READ (the retention-respawn's
#: fail-close): a receipt on the parent's rows whose child's ROOT row
#: is gone. ONE id is enough to refuse the spawn (the purge's own law:
#: it must walk the parent links — a purge that leaves a pointer is
#: this read's catch).
_DANGLING_RECEIPT_SQL = """
SELECT r.cid FROM (
    SELECT DISTINCT jsonb_array_elements_text(metadata->'child_flows') AS cid
    FROM {schema}.jobs
    WHERE (metadata->>'flow_id')::uuid = $1
      AND metadata ? 'child_flows'
) r
WHERE NOT EXISTS (
    SELECT 1 FROM {schema}.jobs j
    WHERE j.id = r.cid::uuid AND j.step_key = '__flow__'
)
LIMIT 1
"""

#: THE ANCESTOR WALK (the depth bound + the cycle check, ONE
#: walk): the chain of the parent flow's OWN ancestors read from the
#: rows' ``parent_flow_id`` links, capped at the declared bound (the
#: cap is what makes a cycle's walk terminate — the bound fires on it).
#: The result is the parent's ancestor count $depth$; the child would
#: be depth+1, so a walk that reaches the bound IS the refusal.
_ANCESTOR_DEPTH_SQL = """
WITH RECURSIVE chain AS (
    SELECT 0 AS depth, (metadata->>'parent_flow_id')::uuid AS parent
    FROM {schema}.jobs WHERE id = $1 AND step_key = '__flow__'
    UNION ALL
    SELECT c.depth + 1, (r.metadata->>'parent_flow_id')::uuid
    FROM chain c
    JOIN {schema}.jobs r ON r.id = c.parent AND r.step_key = '__flow__'
    WHERE c.depth < $2
)
SELECT COALESCE(MAX(depth), 0) FROM chain
"""

_CHILD_ROOT_STATUS_SQL = """
SELECT status FROM {schema}.jobs WHERE id = $1 AND step_key = '__flow__'
"""

_CHILD_TERMINAL_RESULT_SQL = """
SELECT result FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

_CHILD_FAILURE_READ_SQL = """
SELECT error_class, error_message FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND status = 'failed'
  AND step_key <> '__flow__'
ORDER BY finished_at DESC NULLS LAST
LIMIT 1
"""

#: The cancel cascade's collection (the receipts over the flow's rows —
#: the reverse-edge walk's extension reads the parent's OWN receipt, the
#: one direction that names the children).
CHILD_RECEIPTS_SQL_TEMPLATE = _CHILD_RECEIPTS_SQL_TEMPLATE

#: THE CANCEL CASCADE'S COLLECTION (the ROWS' LAW
#: + the receipts' ACCELERATOR): the children of one run read BOTH
#: directions — the crash-unsafe receipts (the parent's node rows'
#: ``child_flows``) AND the crash-safe ``parent_flow_id`` direction the
#: child's root rows carry from the CREATE transaction. The receipts
#: accelerate the common case; the rows are the law: a child created
#: but never receipted (the crash between create and receipt-write)
#: is STILL collected — the orphan window is unrepresentable.
CHILD_FLOWS_OF_RUN_SQL_TEMPLATE = """
WITH receipt_children AS (
    SELECT DISTINCT (e.val)::uuid AS cid
    FROM {schema}.jobs j
    CROSS JOIN LATERAL jsonb_array_elements_text(j.metadata->'child_flows') AS e(val)
    WHERE (j.metadata->>'flow_id')::uuid = $1
      AND j.metadata ? 'child_flows'
), row_children AS (
    SELECT (metadata->>'flow_id')::uuid AS cid
    FROM {schema}.jobs
    WHERE step_key = '__flow__'
      AND (metadata->>'parent_flow_id')::uuid = $1
      AND metadata ? 'flow_id'
)
SELECT DISTINCT cid FROM (
    SELECT cid FROM receipt_children
    UNION
    SELECT cid FROM row_children
) u
"""

#: The descendants' drive (the dev-loop driver's face — the child is a
#: REAL flow and its rows need a claimer): the recursive CTE walks the
#: receipts; the UNION's dedup terminates it; the seed itself is
#: EXCLUDED (``fid <> $1`` — the parent's own claim pass is tick's
#: first half, never the descendants' walk; re-including it would
#: re-claim the parent's rows and the fork's fresh children in the
#: same pass — the mid-flight world the progress pins stage).
DESCENDANT_FLOWS_SQL_TEMPLATE = """
WITH RECURSIVE tree AS (
    SELECT $1::uuid AS fid
    UNION
    SELECT (e.val)::uuid AS fid
    FROM tree t
    JOIN {schema}.jobs j ON (j.metadata->>'flow_id')::uuid = t.fid
    CROSS JOIN LATERAL jsonb_array_elements_text(j.metadata->'child_flows') AS e(val)
    WHERE j.metadata ? 'child_flows'
)
SELECT fid FROM tree WHERE fid <> $1
"""

#: The child root's workflow name (the descendant runner's resolution —
#: the D1 registry's key stamped on the root at create).
FLOW_WORKFLOW_NAME_SQL_TEMPLATE = """
SELECT metadata->>'workflow' AS workflow FROM {schema}.jobs
WHERE id = $1 AND step_key = '__flow__'
"""

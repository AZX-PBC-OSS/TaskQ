"""CHILD FLOWS — ``ctx.spawn_flow`` + the join hold: a step
spawns a SUB-FLOW (a REAL flow — its own run rows; the run explorer sees
it, the admin operates it, the CLI lists it) and JOINS its terminal (the
HITL machinery's own shape: the wait IS a hold; the child's terminal
event is the hold's resolver; the wake arm delivers + resumes; the sweep
belt reaps the orphaned waiter).

the SPAWN = the child's ``create_flow`` inside the parent's step — the
create-seam's one-transaction law respected BY CONSTRUCTION (the child's
root + static nodes + edges + ROOT_START land in the child create's own
transaction; the parent's step never wraps it in a second one).

The spawn's run key is
``childflow:{parent_flow_id}:{node_key}:{slot}`` — in a loop body the node
segment is ``{node_key}.iter{i}`` (the iteration's ledger key), and on a
map item it is ``{node_key}.item{m}`` (the sibling's ``map_index``; map
items share one ``step_key``) — under the child's own
``workflow-run:<child name>`` scope — the G2 arbiter is the rememberer.
The SLOT is the spawn site's identity, STABLE ACROSS RETRIES: the body
re-executes from the top (the hold's resume, the retry's reclaim) and
re-derives the same keys — the arbiter conflict ADOPTS the same child,
never a duplicate (the retry-completion census's law, at the flow
boundary). A terminal child adopted as-is is the deterministic replay:
the retried body re-decides off the same terminal child.

The per-claim spawn cursor on the executing node's
row (``metadata.spawn_slot_{attempt}`` holds the claim's ``claim_epoch``
beside the counter): every execution, including a re-claim at a refunded
attempt number (a ``Snooze``, the lost-claim refund), restarts at slot 0,
so the re-run re-derives the same slots and the arbiter adopts. A loop's
iterations run on the loop node's own row under one claim, so each iteration
keeps its own cursor (``spawn_slot_{attempt}.iter{i}``) and its own run keys:
a resume or a deferral that re-runs iteration k restarts at k's slot 0 and
adopts k's children. A map item is its OWN job row, so its cursor is already
per-sibling; the run key carries the item (``{node_key}.item{m}``) because
its siblings share one ``step_key``. An explicit ``key=`` overrides the slot
(the author's identity, never scoped to an iteration) and may not be a bare
decimal string: that spelling is byte-identical to the slot segment, so the
spawn refuses it.

The parent's node row
carries the child's flow id (``metadata.child_flows`` — the idempotent
ensure-append, dedup'd: an adoption re-run never duplicates the
receipt); the child's ROOT row carries the parent's
(``parent_flow_id`` / ``parent_node_id`` / ``parent_step_key`` — riding
the create's one transaction via ``create_flow(root_metadata_extra=…)``,
never a post-create UPDATE that would re-open the orphan window).

the JOIN = the HOLD (the hold machinery, the ``Decision | Expired``
pattern's sibling): the join site registers a hold on the PARENT's node
(signal ``__child__:{child_flow_id}``, the declared result model as the
payload contract — the contract is mandatory at hold time), marks the
ledger ``awaited``, and raises :class:`NodeHeldError` — the held
representation (pending + the deadline + the signal row as truth; no
terminal, no ledger failure), the worker releases the slot. The child's
terminal is the hold's RESOLVER: :func:`sweep_child_joins` (a sweep arm,
wired after the root-maintain leg) CAS-delivers the held join, resumes
the parent node, and broadcasts :data:`CHILD_TERMINAL_CHANNEL` (the
transport — the pointer-only payload). The resumed body re-executes from
the top; the join site reads the CHILD ROW first (the row is the truth)
and returns the CLOSED UNION member:

* ``ChildOk(result=…)`` — the child's typed result decoded into the
  parent's body (R3's face at the flow boundary);
* ``ChildFailed`` — the child terminal-failed (or its rows are GONE —
  the fail-close face; never a silent swallow, never a raised error:
  the body's own arm decides);
* ``ChildCancelled`` — the child was cancelled (the cascade: the
  parent's cancel walks the receipts and cancels each child —
  ``cancel_workflow_run``'s extension);
* ``Expired`` — the JOIN's own patience expired (the hold's deadline,
  armed by ``wait(timeout_s=…)``; the face fires once per abandoned row
  per EXECUTION — the hold→expire→re-hold→∞ dragon's kill site, the
  amendment verbatim). The replay slot is one per node row and attempt,
  not one per child: when one execution observes two expired children, a
  deliberate re-wait on the FIRST replays ``Expired`` again instead of
  minting a new hold.

The wait returns
``ChildOutcome[M] | Expired`` — a body that matches only the ``ChildOk``
arm falls through and reds against its declared return; a body that
bare-unwraps reds the attribute.
"""

from __future__ import annotations

import re
import types
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, get_origin

from pydantic import BaseModel, ValidationError

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows.api._ctx_wait import (
    Expired,
    HoldMintFencedError,
    NodeHeldError,
    read_claim_marker,
    require_claim_epoch,
    write_claim_marker,
)
from taskq.workflows.api._runner_codec import decode_result_with_detail
from taskq.workflows.api._runner_errors import (
    WorkflowResultDetailUnavailableError,
    WorkflowRunError,
)
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
    "ChildResultContractError",
    "ChildWorkflowNoTerminalError",
    "ChildWorkflowUnknownError",
    "SpawnDepthExceededError",
    "SpawnKeyReservedError",
    "SpawnResultModelError",
]

from taskq.workflows.api._hitl import CHILD_JOIN_SIGNAL_PREFIX

#: The verdict set the join reads as the child's terminal (the state
#: machine's own vocabulary, re-read from the terminal-statuses door at
#: call time — never a re-spelled literal drift).
_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})

#: An author key shaped exactly like the unkeyed run key's slot segment. The
#: keyed derivation splices the author's string into the same position the
#: unkeyed one fills with ``int(slot)``, so this shape aliases the node's
#: first unkeyed spawn (a plain node with two spawn sites hands one caller
#: the other's child). The spawn refuses it; every other key keeps its
#: byte-identical run key.
_BARE_DECIMAL_KEY_RE: Final = re.compile(r"[0-9]+")


class ChildWorkflowUnknownError(RuntimeError):
    """``ctx.spawn_flow`` named a workflow the registry does not carry —
    the typed refusal (the registration door — ``app.get(name)`` — is
    the only body source, D1).

    Raised directly for an unregistered NAME, and the base of
    :class:`ChildWorkflowNoTerminalError`, whose child IS registered —
    so ``except ChildWorkflowUnknownError:`` keeps matching both spawn
    target refusals."""


class ChildWorkflowNoTerminalError(ChildWorkflowUnknownError):
    """``ctx.spawn_flow`` named a child workflow that is REGISTERED and
    compiled in this process but declares no terminal node — the join
    could never read a result, so the spawn refuses before any child row
    or spawn cursor is written.

    A DETERMINISTIC authoring failure (the ladder's own routing): within
    a process, re-running cannot give the compiled graph a terminal, so
    the refusal spends no retry budget and writes one failed ledger row
    (``jobs.attempt`` reads the claim ordinal, which a prior hold's resume
    increments, so it is not always 1), with this class on the record and
    no ``on_exhaustion`` effect. During a mixed-version rolling deploy that
    adds a terminal to a previously terminal-less child, an in-flight
    parent landing on an older worker fails deterministically without
    retrying onto a newer worker.

    Distinct from its base :class:`ChildWorkflowUnknownError` because the
    workflow is neither unknown nor missing — reusing that class would
    record a registered workflow as unregistered. The remedy: give the
    child workflow a terminal (return its step promise, or ``build(...)``
    it) rather than sinking it."""


class ChildFlowLostError(RuntimeError):
    """The retention-respawn fail-close: the
    parent's resume found a DANGLING child pointer — the parent's row
    still names a child flow whose rows are GONE (the retention purge's
    own rule: the purge must walk the parent links first).

    A dangling pointer must fail the run, never respawn: re-minting a
    fresh child would run the child's side effects (emails, payments)
    twice while the parent run reads SUCCEEDED — the fabricated-health
    face. The spawn REFUSES: the dangling pointer is the failure, the run
    terminal-fails with this class on the record — never a silent
    respawn. A DETERMINISTIC death (re-running cannot restore the
    purged rows — the deterministic-authoring-failure ladder's own
    routing)."""


class SpawnDepthExceededError(ValueError):
    """The depth bound (``FanInBoundExceededError``'s
    sibling): the spawn's ancestor walk (the rows' ``parent_flow_id``
    links, the cycle detection folded into the same walk) reached the
    declared max depth — a self-spawning flow cannot mint a
    generation per sweep tick forever, and a corrupted (cyclic) parent
    link cannot run the walk forever either.

    A DETERMINISTIC authoring failure (the ladder's own
    deterministic-authoring-failure law): re-running cannot shrink the
    ancestor chain — the retry ladder never burns; the node terminal-fails
    with this class on the record. The remedy: bound the fan-out (raise
    ``max_depth=`` at the spawn site, or flatten the recursion into a loop
    over a ledgered step)."""


class SpawnResultModelError(TypeError):
    """``ctx.spawn_flow(result=…)`` is not a pydantic model subclass (bare
    ``BaseModel`` is refused too), or is a model class pydantic cannot
    render the hold's JSON schema for. That class is the child's typed
    result contract, which the join's hold schema and the ChildOk decode
    read. A DETERMINISTIC authoring failure: refused before the slot claim
    and the child create, the retry ladder never burns."""


class ChildResultContractError(ValueError):
    """The child terminal-SUCCEEDED but its result does not satisfy the
    parent's declared ``result=`` model — the decode door
    (``ChildHandle._ok_result``) re-raised pydantic's ``ValidationError``
    as this class. A DETERMINISTIC authoring failure: the child's terminal
    row is the truth, so every attempt re-reads the same undecodable
    result; the retry ladder never burns. The remedy: pass the child's own
    result model at the spawn site."""


class SpawnKeyReservedError(ValueError):
    """``ctx.spawn_flow(key=...)`` named a BARE DECIMAL string (``"0"``,
    ``"12"``) — a spelling the unkeyed derivation owns as its slot segment.

    The keyed run key ``childflow:{flow}:{node}:{key}`` and the unkeyed
    ``childflow:{flow}:{node}:{int(slot)}`` are one string for such a key,
    so it would alias the node's first unkeyed spawn (or a numeric keyed
    site's) and hand a caller another site's child. A DETERMINISTIC
    authoring failure (the ladder's routing never burns a retry):
    re-executing cannot change the key, so the node terminal-fails with
    this class on the record. ``ValueError``'s subclass, so a caller that
    only guarded the older refusal shape keeps working. The remedy: give
    the key an unmistakable shape (``f"item-{i}"``) or drop ``key=`` to use
    the slot."""


#: the ancestor chain the spawn walks is bounded here; a deeper fan-out is the
#: typed refusal's, never an unbounded generation machine. A spawn site that
#: genuinely needs more depth declares ``max_depth=`` — the author's identity,
#: recorded on the spawn.
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
    claim_epoch: int
    _pool: asyncpg.Pool
    _wsql: WorkflowSql
    _ledger_id: JobId | None
    _loop_iteration: int | None
    _map_index: int | None


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
    claim_epoch: int


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
        """A terminal child returns the
        typed member immediately (the row is the truth — the resume's
        pure row-replay face); a live child registers the join hold and
        raises :class:`NodeHeldError` (the held representation — the
        slot released, the ledger 'awaited', never a failed attempt).

        ``timeout_s=`` arms the hold's deadline (the DB clock): the
        expiry sweep abandons the hold, the body resumes, and THIS site
        returns the :class:`Expired` member once per abandoned row per
        EXECUTION (the face-marker pattern: the marker is scoped to the
        executing claim, so a re-claim at a refunded attempt ordinal
        replays the member instead of minting a second join hold). The
        slot is one per node row and attempt, not one per child: two
        expired children observed in one execution share it, so a
        deliberate re-wait on the FIRST replays ``Expired`` again rather
        than minting a new hold.
        ``None`` waits as long as the
        child lives — safe by the cascade (a cancelled parent cancels
        its children, so no join outlives its flow).

        A succeeded child whose terminal value cannot be read raises
        :class:`~taskq.workflows.api._runner_errors.WorkflowResultDetailUnavailableError`
        — the terminal row is missing, or a truncated join's parent
        detail rows have aged out or been purged. The raise lands inside
        the calling node's body (so the failure is recorded against the
        consumer's row, not the child's) and is deterministic: a retry
        cannot restore the rows, so the ladder does not spend its budget
        on it."""
        site = self._site
        signal_name = f"{CHILD_JOIN_SIGNAL_PREFIX}{self.flow_id}"
        async with site.pool.acquire() as conn:
            # the child's terminal read first — a terminal child needs no hold
            # at all (this is the resume's pure row-replay face, and the
            # crash-mid-wait's wake: the successor's body re-executes, reads,
            # joins).
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

            # the Expired member, once per abandoned row per execution (the
            # face marker, scoped to the executing claim; one slot per node
            # row and attempt, shared by that execution's children). the
            # door marker rides the read: the sweep's rows read 'timeout',
            # the operator abandon's payload marker reads 'operator' (the
            # same member either way — the fail-close).
            abandoned = await conn.fetchrow(
                render_sql(
                    "SELECT id, hold_epoch, "
                    "COALESCE(payload->>'abandoned_by', 'timeout') AS abandoned_by "
                    "FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND status = 'abandoned' ORDER BY hold_epoch DESC LIMIT 1",
                    site.wsql.schema,
                ),
                site.flow_id,
                site.node_key,
                signal_name,
            )
            if abandoned is not None:
                # the face marker is claim-scoped (the same document the wait
                # and timer cursors carry): this join's Expired is surfaced
                # once per abandoned row per EXECUTION, so a deliberate second
                # ``wait()`` under this claim re-holds while a re-claim after
                # a ``Snooze`` / free ``RetryAfter`` (a refunded attempt
                # ordinal, a new epoch) replays Expired instead of minting a
                # second join hold. The slot is one per node row and attempt,
                # NOT one per child: an execution that observes two expired
                # children leaves the second child's ``hold_epoch`` in the
                # slot, so a deliberate re-wait on the first replays Expired
                # again instead of minting a new hold. The key is this site's
                # own (per attempt, beside the legacy bare-integer
                # ``child_join_face_*`` the previous release wrote, which now
                # reads as no marker at all).
                face_key = f"child_join_replay_{site.attempt}"
                marker = await read_claim_marker(
                    conn,
                    schema=site.wsql.schema,
                    job_id=site.job_id,
                    key=face_key,
                    claim_epoch=site.claim_epoch,
                )
                hold_epoch = int(abandoned["hold_epoch"])
                if marker.get("hold_epoch") != hold_epoch:
                    require_claim_epoch(
                        "ChildHandle.wait",
                        flow_id=site.flow_id,
                        node_key=site.node_key,
                        claim_epoch=site.claim_epoch,
                    )
                    await write_claim_marker(
                        conn,
                        schema=site.wsql.schema,
                        job_id=site.job_id,
                        key=face_key,
                        claim_epoch=site.claim_epoch,
                        marker={"hold_epoch": hold_epoch},
                    )
                    return Expired(
                        signal=signal_name,
                        node_key=site.node_key,
                        hold_id=str(abandoned["id"]),
                        abandoned_by=str(abandoned["abandoned_by"]),  # pyright: ignore[reportArgumentType]  # Why: the row's COALESCE vocabulary is the writer's own closed pair ('timeout'/'operator') — the member's Literal re-validates it.
                    )

                    # this join already registered (the resume's re-execution before
            # the wake) — idempotent, never a second registration of one wait.
            held_row = await conn.fetchrow(
                render_sql(
                    "SELECT id, signal_name, expires_at FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND status = 'held' ORDER BY hold_epoch DESC LIMIT 1",
                    site.wsql.schema,
                ),
                site.flow_id,
                site.node_key,
                signal_name,
            )
            from taskq.workflows.api._hitl import mark_awaited, reassert_hold, register_hold

            if held_row is not None:
                # this claim re-entered onto the standing hold: park the row
                # onto it, or it stays 'running' with nothing executing it.
                # A missed park wrote nothing — a newer claim owns the row —
                # so the unwind is the fresh mint's fence label, never "held".
                if site.ledger_id is not None:
                    await mark_awaited(conn, site.wsql.schema, site.ledger_id)
                parked = await reassert_hold(
                    conn,
                    schema=site.wsql.schema,
                    node_id=site.job_id,
                    hold_id=held_row["id"],
                    signal_name=str(held_row["signal_name"]),
                    expires_at=held_row["expires_at"],
                    claim_epoch=site.claim_epoch,
                )
                if not parked:
                    raise HoldMintFencedError(node_id=site.job_id, claim_epoch=site.claim_epoch)
                raise NodeHeldError(hold_id=str(held_row["id"]), signal_names=(signal_name,))

            # register: the ledger 'awaited' (resume-not-retry), then the
            # hold — the payload contract is the child's declared result
            # model (mandatory at hold time — the audit hole's law).
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
                claim_epoch=site.claim_epoch,
            )
        raise NodeHeldError(hold_id=str(hold_id), signal_names=(signal_name,))

    async def _ok_result(self, conn: ConnLike) -> ChildOk[G]:
        """The child's typed result read: the terminal node's decoded
        row result re-validated into the declared model (the decode
        once, typed end to end — the R3 face at the flow boundary).

        A terminal that is a JOIN reads through THE one detail re-read
        (:func:`taskq.workflows.api._runner_codec.decode_result_with_detail`)
        — the same function ``FlowRunner.result()`` uses — so a bounded
        collect's truncated summary row hands back the FULL collect, and a
        truncated ``gather`` flattens exactly as its own packer did.

        The flatten authority differs by row age, and this seat has to be
        honest about it: a row born after this deploy carries the durable
        ``wf_node_kind`` stamp in its ``metadata``, which is this reader's
        authority (it holds no ``CompiledWorkflow`` to ask). A row born
        before the stamp existed carries no kind here, so the re-read
        falls back to a COUNT-VERIFIED reconciliation: when the rebuilt
        parent values do not match the recorded item count and every one
        of them is a list, it tries the one-level gather flatten and
        accepts it only if the flattened length equals the recorded count.
        If neither shape reconciles the read refuses.

        A row with no durable value at all — a swept or re-pended terminal,
        or a truncated join whose parent detail rows were purged — raises
        :class:`~taskq.workflows.api._runner_errors.WorkflowResultDetailUnavailableError`,
        the same fail-closed class the runner seats raise, so the join
        fails LOUDLY instead of returning a 64 KiB prefix or validating a
        ``None`` into a pydantic ``ValidationError``."""
        if self._terminal_key is None:  # pragma: no cover - the spawn refuses a terminal-less child
            raise ChildWorkflowNoTerminalError(
                f"the child flow {self.flow_id} names no terminal — the spawn "
                "refuses a terminal-less child (the join could never read a result)"
            )
        row = await conn.fetchrow(
            render_sql(_CHILD_TERMINAL_RESULT_SQL, self._site.wsql.schema),
            self._terminal_key,
            self.flow_id,
        )
        if row is None:
            # The child's root said ``succeeded`` and its terminal row is
            # gone: the result face has no source. Deterministic corruption
            # (no re-run restores the row) — the same fail-closed class the
            # detail re-read raises, so the ladder does not burn retries on a
            # read that can never succeed.
            raise WorkflowResultDetailUnavailableError(
                node_id=self.flow_id,
                step_key=self._terminal_key,
                reason=(
                    f"the child flow {self.flow_id} reads as succeeded but its "
                    "terminal row is missing (purged or never written)"
                ),
            )
        decoded = await decode_result_with_detail(
            conn,
            self._site.wsql.schema,
            node_id=row["id"],  # pyright: ignore[reportUnknownArgumentType]  # Why: the asyncpg Record's members are Unknown; the statement's SELECT names the columns.
            raw=row["result"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
            metadata=row["metadata"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
            step_key=str(row["step_key"]),  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
            result_expired=bool(row["result_expired"]),  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
        )
        try:
            result = self._result_model.model_validate(decoded)
        except ValidationError as exc:
            # the summary names WHERE and WHAT KIND of mismatch, never the
            # child's values: this text is persisted unredacted.
            fields = ", ".join(
                f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['type']}"
                for error in exc.errors(include_input=False, include_url=False)
            )
            raise ChildResultContractError(
                f"the child flow {self.flow_id} returned a result the parent's "
                f"declared result= model {self._result_model.__name__} cannot "
                f"decode ({fields or 'no field detail'}) — pass the child's own "
                "result model at the spawn site"
            ) from exc
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


#: The persisted refusal message's name budget, measured on the RENDERED
#: ``repr`` (``repr`` is not length-preserving: a non-printable astral code
#: point costs ten characters). The ladder and the loop boundary keep only
#: the first ``MESSAGE_TERMINAL_MAX`` characters of the body's exception
#: (``_progress``), so the rendered name is bounded to keep the diagnosis
#: and the remedy inside that window for any registry key — nothing else
#: bounds a workflow name.
_SPAWN_NAME_DISPLAY_MAX: Final[int] = 40


def _display_spawn_name(name: str) -> str:
    """The workflow name as the persisted message shows it: the already
    quoted ``repr`` (the repr IS the display) bounded so escaped
    non-printable characters cannot crowd the diagnosis and the remedy out
    of the ``MESSAGE_TERMINAL_MAX`` window."""
    rendered = repr(name)
    if len(rendered) <= _SPAWN_NAME_DISPLAY_MAX:
        return rendered
    return f"{rendered[: _SPAWN_NAME_DISPLAY_MAX - 3]}..."


def _offending_value_name(value: object) -> str:
    """Name a refused ``result=`` value. Only a type or a typing/union alias
    keeps its ``repr`` (the repr IS the diagnosis); every other value is
    named by class, because the message is persisted unredacted into
    ``jobs.error_message`` and ``wf_step_ledger.error_message``."""
    if (
        isinstance(value, (type, types.GenericAlias, types.UnionType))
        or get_origin(value) is not None
    ):
        return repr(value)
    return f"an instance of {type(value).__name__}"


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
        """``handle = await ctx.spawn_flow("child_name",
        input=…, result=ChildResult)`` — the child is a REAL flow (its
        own run rows; the create-seam's one-transaction law) and the
        spawn's run key is the TWO-CLAIMS LAW at the flow level: the
        retry ADOPTS the same child, never duplicates.

        ``result=`` is the child's typed result contract (type[BaseModel])
        — the join's hold schema is rendered from it and ``ChildOk.result``
        re-validates into it. A value that is not a pydantic model subclass
        (bare ``BaseModel`` included), or a model class pydantic cannot
        render a JSON schema for, is refused with
        :class:`SpawnResultModelError` before anything is read or written;
        a child result the declared model cannot decode raises
        :class:`ChildResultContractError` at the join (deterministic: the
        retry ladder never burns).
        The spawn refuses a terminal-less child (the join could never read a
        result) with :class:`ChildWorkflowNoTerminalError` — a deterministic
        authoring failure, so the retry ladder never burns and no
        ``on_exhaustion`` effect fires.
        ``key=`` overrides the slot (the author's identity, the
        same key in every loop iteration unless the author puts the
        iteration in it); the default slot is the per-claim spawn cursor
        (every execution restarts at slot 0, so a retry or a deferral's
        re-run adopts; in a loop body the cursor and the run key are per
        iteration, so a re-run of iteration k adopts k's own children; a
        map item's cursor is its own row's, and its run key carries the
        sibling's index). ``key=`` may not be a bare decimal string
        (``"0"``, ``"12"``): the slot segment of the default run key is
        exactly that spelling, so a numeric key would alias the node's
        first unkeyed spawn and hand a caller another site's child — the
        spawn refuses it instead of minting the collision.
        ``max_depth=`` raises the spawn's default depth bound
        (:data:`DEFAULT_SPAWN_MAX_DEPTH`) for a fan-out that genuinely
        needs it — the bound's typed arm is
        :class:`SpawnDepthExceededError`.

        The spawn's atomicity face (the gather guard):
        the default slot's read+write is one atomic claim at the row
        (an ``UPDATE … RETURNING`` increment), so K CONCURRENT spawns
        (``asyncio.gather(*[ctx.spawn_flow(...) …])`` — the natural
        fan-out spelling) take K DISTINCT slots and mint K DISTINCT
        children — a cursor read/write race would adopt the same slot
        (run key) for all K spawns, mint one
        child, and hand every caller tenant-0's verdict wearing K
        labels (the silent fabricated result). A same-key concurrent
        spawn ADOPTS the same child (the arbiter's CAS — the run-key
        claim at the row) and every caller receives THAT child's
        handle: the shared face, never a fabricated per-caller
        verdict. A CONCURRENT fan-out that must join STABLY across the
        resume passes ``key=`` (the slot's allocation order is the
        scheduler's, the author's identity is the caller's)."""
        if not (isinstance(result, type) and issubclass(result, BaseModel)) or result is BaseModel:  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the type[G] bound is checker-only; this is its runtime half.
            # an offending value is named by class: the message is persisted
            # unredacted.
            got = _offending_value_name(result)
            # remedy before the value: the loop boundary keeps only
            # MESSAGE_TERMINAL_MAX chars (`_progress`), and the name is
            # bounded so the remedy lands there for any key.
            raise SpawnResultModelError(
                f"ctx.spawn_flow({_display_spawn_name(name)}): result= must be a "
                "subclass of pydantic BaseModel (not BaseModel itself); pass that "
                f"class; got {got}"
            )
        # the guard asks the hold door's own question: ``wait`` registers the
        # model's JSON schema as the hold's payload contract, and the schema
        # generator can refuse a class the predicate accepted. Any failure
        # means the class is not a usable result contract.
        try:
            result.model_json_schema()
        except Exception as exc:  # Why: pydantic runs author types' schema hooks here; any failure means the hold cannot use the class.
            raise SpawnResultModelError(
                f"ctx.spawn_flow({_display_spawn_name(name)}): result= must be a "
                "model class pydantic can schema; pass that class; "
                f"{result.__name__} failed ({type(exc).__name__})"
            ) from exc
        from taskq.workflows._worker_execution import get_compiled_workflow

        try:
            compiled_obj = get_compiled_workflow(name)
        except KeyError:
            raise ChildWorkflowUnknownError(
                f"workflow {_display_spawn_name(name)} is not registered — the spawn "
                "resolves the child from the compile registry (the registration door: "
                "app.get(name); the definitions must be imported in THIS process)"
            ) from None
        from taskq.workflows.api._app import CompiledWorkflow

        assert isinstance(compiled_obj, CompiledWorkflow), type(compiled_obj)
        if compiled_obj.terminal is None:
            # the rendered name is bounded so the diagnosis and the remedy stay
            # inside the persisted message window (`_progress`) for any key.
            raise ChildWorkflowNoTerminalError(
                f"ctx.spawn_flow({_display_spawn_name(name)}): the named child "
                "workflow is registered but declares no terminal node — the join "
                "could never read a result; return the child's step promise (or "
                "build(...) it) instead of sink-ing it"
            )

        # The fail-close: a dangling child pointer on the
        # parent's own receipts — a receipt whose child rows are gone —
        # is the typed refusal, never a silent respawn (a respawn would
        # run the child's side effects twice while the parent reads
        # succeeded).
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

                # the ancestor chain read from the rows' parent links — one walk serves
        # both (a cycle's walk reaches the cap and fires the same bound).
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

            # the per-claim spawn cursor is claimed in one atomic statement — the
        # increment and the returned slot are one row-CAS, so concurrent spawns
        # take distinct slots. The attempt's key carries claim_epoch, because a
        # refund (a deferral, the lost-claim reconcile) re-uses the attempt
        # number: every execution restarts at 0 and the arbiter adopts.
        if key is not None:
            # The author's identity is the key: no slot claimed (the parent
            # row's counter never moves for a keyed site), and the derivation
            # stays byte-identical for every key that cannot be confused with
            # the slot segment. A bare decimal key is refused because it WOULD
            # be that segment.
            if _BARE_DECIMAL_KEY_RE.fullmatch(key):
                raise SpawnKeyReservedError(
                    f"spawn key {key!r} on node {self.node_key!r} is a bare decimal "
                    "string — the unkeyed run key's slot segment is exactly that "
                    "spelling, so this key would alias the node's first unkeyed "
                    "spawn (or any other numeric keyed site's) and hand a caller a "
                    "different spawn site's child. Give the key an unmistakable "
                    "shape (e.g. f'item-{key}') or drop key= to use the slot."
                )
            spawn_key = f"childflow:{self.flow_id}:{self.node_key}:{key}"
        else:
            # a loop's iterations share one row and one claim: the cursor and
            # the key carry the iteration (the ledger's <key>.iter<i>), so a
            # re-run of iteration k restarts at k's slot 0 and adopts k's children.
            # A map item is its own row (its cursor is already per-sibling);
            # its run key carries the sibling's map_index, because every map
            # item shares one step_key (the fork's own :{map_index} law).
            iter_suffix = "" if self._loop_iteration is None else f".iter{self._loop_iteration}"
            map_suffix = "" if self._map_index is None else f".item{self._map_index}"
            cursor_key = f"spawn_slot_{self.attempt}{iter_suffix}"
            if self.claim_epoch <= 0:
                # The cursor's reset law compares the stored epoch against the
                # executing claim's: at 0 every comparison is NULL/false and a
                # re-claim never resets, minting a duplicate child per resume.
                # An unsupplied epoch must be loud, never a silent mis-claim.
                raise WorkflowRunError(
                    f"spawn_flow on node {self.node_key!r} of flow {self.flow_id} has "
                    f"claim_epoch={self.claim_epoch!r} — an executing claim's epoch is "
                    "at least 1 (both claim doors increment it). Building a context "
                    "without the real claim epoch would leave the spawn slot's reset "
                    "law inert; pass the claim's granted epoch."
                )
            async with self._pool.acquire() as conn:
                slot = await conn.fetchval(
                    render_sql(_SPAWN_SLOT_CLAIM_SQL, self._wsql.schema),
                    self.job_id,
                    cursor_key,
                    self.claim_epoch,
                )
            assert slot is not None  # the claim's UPDATE hit the executing node's row
            spawn_key = (
                f"childflow:{self.flow_id}:{self.node_key}{map_suffix}{iter_suffix}:{int(slot)}"
            )

        from taskq.workflows.api._runner import FlowRunner

        # NB: this runner only creates the child run's rows (the spawn +
        # the join's receipts) — it executes no bodies. The child's rows
        # execute through the doors that own them (the fleet's
        # ``execute_flow_job``, the dev drive's ``_runner_for_flow``) —
        # each threads the worker's default deadline itself (the wf
        # start_to_close fix's own resolution chain).
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
                claim_epoch=self.claim_epoch,
            ),
            flow_id=claim.flow_id,
            result_model=result,
            terminal_key=compiled_obj.terminal,
        )

        # the ensure-append is dedup'd (an adoption re-run never duplicates).
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

#: the per-claim cursor's read+write is one statement — the attempt's key
#: holds ``{epoch, n}``: the counter increments under the same claim_epoch
#: and restarts at 0 under any other, and the allocated slot returns from
#: the UPDATE. Concurrent claims serialize on the row lock and each receives
#: a DISTINCT slot; a same-key spawn never reaches this statement (the
#: explicit key bypasses the slot). One key per attempt number keeps the
#: metadata bounded under a polling deferral; a loop body keys per attempt
#: and iteration, so a spawning loop adds one key per spawning iteration,
#: growing at the rate of the ``child_flows`` receipts those spawns append.
#: The stored epoch only moves forward: a writer with an older epoch on the
#: same key (a stale body whose lease lapsed, sharing the attempt number as
#: a chain source's logical attempt does) takes the next slot and never
#: resets the live claim's counter, or the live claim's next spawn would
#: re-derive slot 0 and adopt its own first child. The price of that rule is
#: the slot's meaning: it indexes ARRIVAL order across every writer of the
#: key, not one execution's logical spawn ordinal. While one execution owns
#: the key they coincide; after a stale writer interleaves (or a concurrent
#: unkeyed fan-out permutes), a later claim's slot k is the k-th child in
#: that interleaved order — which is why a fan-out that must join to a
#: per-item identity across resumes passes ``key=``.
_SPAWN_SLOT_CLAIM_SQL = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(
    metadata,
    ARRAY[$2::text],
    jsonb_build_object(
        'epoch', GREATEST($3::bigint, (metadata->$2->>'epoch')::bigint),
        'n', CASE WHEN (metadata->$2->>'epoch')::bigint >= $3::bigint
                  THEN (metadata->$2->>'n')::int + 1 ELSE 0 END
    )
)
WHERE id = $1
RETURNING (metadata->$2->>'n')::int
"""

#: a receipt on the parent's rows whose child's root row is gone. one id is
#: enough to refuse the spawn (the purge's own law: it must walk the parent
#: links — a purge that leaves a pointer is this read's catch).
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

#: the chain of the parent flow's own ancestors read from the rows'
#: ``parent_flow_id`` links, capped at the declared bound (the cap is what
#: makes a cycle's walk terminate — the bound fires on it). The result is the
#: parent's ancestor count $depth$; the child would be depth+1, so a walk that
#: reaches the bound is the refusal.
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
SELECT id, step_key, result, metadata, (result IS NULL) AS result_expired
FROM {schema}.jobs WHERE step_key = $1
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
#: the reverse-edge walk's extension reads the parent's own receipt, the
#: one direction that names the children).
CHILD_RECEIPTS_SQL_TEMPLATE = _CHILD_RECEIPTS_SQL_TEMPLATE

#: the children of one run read both directions — the crash-unsafe receipts
#: (the parent's node rows' ``child_flows``) and the crash-safe
#: ``parent_flow_id`` direction the child's root rows carry from the create
#: transaction. The receipts accelerate the common case; the rows are the law:
#: a child created but never receipted (the crash between create and
#: receipt-write) is still collected — the orphan window is unrepresentable.
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
#: real flow and its rows need a claimer): the recursive CTE walks the
#: receipts; the union's dedup terminates it; the seed itself is
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

"""The body's HITL WAIT/DELIVER machinery (the split of ``api/_runner`` —
§7b's concerns-separate law): the wait site's typed hold, the
answer-queue replay, the timeout face, and the delivered-payload read.
The deliver boundary itself (the admin/hitl client's side) lives in
``api/_hitl``; THIS module is the ctx's side of the same contract.

the RESUME CONTRACT (documented ON the wait method —
disposition): **the body re-executes FROM the TOP on resume** — make it
idempotent; pre-wait side effects are ``ctx.step``-ledgered and replay
cheap.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

import asyncpg
from pydantic import BaseModel

from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.workflows.api._runner_errors import ClaimEpochMissingError
from taskq.workflows.api._sql_runner import render_sql

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike, JobId
    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "CtxWaitOps",
    "HoldMintFencedError",
    "LoopWaitShapeError",
    "SignalUnavailableError",
    "WaitHost",
    "read_claim_marker",
    "require_claim_epoch",
    "write_claim_marker",
]

from datetime import timedelta

import structlog

from taskq.obs import get_logger

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


# ── the claim-scoped replay marker (the wait, timer and join sites' shared
# cursor): an ``{epoch: <claim_epoch>, ...}`` document under one per-attempt
# metadata key. The bounded-by-one non-consuming refunds — ``Snooze``,
# ``RetryAfter(consume_budget=False)`` and the lost-claim refund — subtract
# one from ``jobs.attempt`` while ``jobs.claim_epoch`` stays strictly
# monotonic, and the re-claim restores the same ordinal, so the re-claim
# runs at the SAME attempt ordinal and a new epoch. A marker keyed by
# attempt alone therefore survives into an execution that never wrote it and
# reports an answer it never consumed. The ``EmitBackpressure`` park also
# leaves ``jobs.claim_epoch`` strictly monotonic, but it zeroes the ordinal
# outright (``attempt = 0``); only ``chain_source`` nodes restore
# ``wf_source_logical_attempt`` across that park, so only their ctx attempt
# (and the attempt leg of their answer queue) survives it. A plain step
# re-claims at attempt 1 after the park and its answers minted at attempt >=
# 2 fall outside the ``1..attempt`` queue window. The epoch in the value is
# what makes the marker belong to one execution either way.
# The key names carry a leg the bare-integer keys never had, so a row
# stamped by the previous release reads as no marker at all (the new claim
# replays its queue from the start, which is what a from-the-top
# re-execution wants) and an older worker reads NO key under the names it
# knows — no ``::int`` cast ever meets a JSON object.

#: The marker document *key* carries (``NULL`` when the key was never
#: written).
_CLAIM_MARKER_READ_SQL = """
SELECT metadata->$2::text AS marker FROM {schema}.jobs WHERE id = $1
"""

#: The marker's write, fenced forward-only on the epoch it stores: a stale
#: body whose lease lapsed (sharing the attempt number with the claim that
#: now owns the row) never overwrites the live claim's marker with an older
#: epoch's count.
_CLAIM_MARKER_WRITE_SQL = """
UPDATE {schema}.jobs
SET metadata = metadata || $3::jsonb
WHERE id = $1
  AND COALESCE((metadata->$2::text->>'epoch')::bigint, 0) <= $4::bigint
RETURNING id
"""


def require_claim_epoch(site: str, *, flow_id: JobId, node_key: str, claim_epoch: int) -> None:
    """The claim epoch a replay marker is scoped to, refused loudly when it
    cannot scope anything. Both claim doors grant at least 1
    (``_sql_runner.NODE_CLAIM_SQL_TEMPLATE`` and the terminal claim UPDATE
    both write ``claim_epoch + 1``), so 0 means the epoch was never supplied:
    every stored marker would compare unequal to it and the site would replay
    a queue it already consumed — the second hold this marker exists to
    prevent."""
    if claim_epoch > 0:
        return
    raise ClaimEpochMissingError(
        f"{site} on node {node_key!r} of flow {flow_id} was handed "
        f"claim_epoch={claim_epoch!r}: an executing claim's epoch is at least 1 "
        "(both claim doors increment it). The wait replay marker resets per "
        "claim, so at 0 it never resets and the re-executed site treats its "
        "already-delivered answer as spent; pass the claim's granted epoch."
    )


async def read_claim_marker(
    conn: ConnLike,
    *,
    schema: str,
    job_id: JobId,
    key: str,
    claim_epoch: int,
) -> dict[str, Any]:
    """The marker document *key* carries for THIS claim: empty when the key
    was never written, when it belongs to another execution, or when the
    value under it is not a document (a scalar, an array or any other
    non-document value reads as ``{}`` instead of raising)."""
    raw = await conn.fetchval(render_sql(_CLAIM_MARKER_READ_SQL, schema), job_id, key)
    document = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(document, dict):
        return {}
    marker = cast("dict[str, Any]", document)
    if marker.get("epoch") != claim_epoch:
        return {}
    return marker


async def write_claim_marker(
    conn: ConnLike,
    *,
    schema: str,
    job_id: JobId,
    key: str,
    claim_epoch: int,
    marker: dict[str, int],
) -> None:
    """Record *marker* under *key* for THIS claim (the epoch rides the value,
    so a later claim's read sees no marker of its own and starts clean). The
    fence value is written LAST, so a caller payload can never displace or
    zero it."""
    landed = await conn.fetchval(
        render_sql(_CLAIM_MARKER_WRITE_SQL, schema),
        job_id,
        key,
        dumps_jsonb_str({key: {**marker, "epoch": claim_epoch}}),
        claim_epoch,
    )
    if landed is None:
        logger.debug(
            "wf-claim-marker-fenced",
            job_id=str(job_id),
            claim_epoch=claim_epoch,
            key=key,
        )


class NodeHeldError(Exception):
    """The wait site's control-flow unwinding (INTERNAL): the body
    raised it after registering the hold — the runner catches it, leaves
    the node in the held representation (pending + the deadline + the
    signal row as truth; no terminal, no ledger failure — the resume
    consumes no attempt), and the worker releases the slot. The body
    re-executes FROM the TOP on resume (the re-execution doctrine)."""

    def __init__(self, hold_id: str, signal_names: tuple[str, ...]) -> None:
        super().__init__(f"node held on {signal_names} (hold {hold_id})")
        self.hold_id = hold_id
        self.signal_names = signal_names


class HoldMintFencedError(Exception):
    """The hold mint's (or a re-entry park's) control-flow unwinding
    (INTERNAL): the park CAS — ``AND status = 'running' AND claim_epoch = $4
    AND cancel_phase = 0 AND NOT metadata ? 'cancel_phase'`` — matched no
    row, so this execution no longer owns the node: the lease lapsed and a
    successor claimed the row, a concurrent resume moved it, it already
    rests in the held representation, or a cancel marked/terminalised it.
    A cooperative cancel leaves ``status`` at ``'running'`` and stamps
    ``metadata.cancel_phase`` (and, normally, the ``cancel_phase`` column);
    the fence refuses that row by its mark, so a cancelled run never parks a
    live hold through this door. The signal row is never written (the mint's
    transaction rolls back), so nothing announces a hold no node stands on.

    ``NodeHeldError``'s sibling, deliberately not its subclass: being held
    is a representation the row carries, while this is the claim that wrote
    nothing — reporting it as held would record a resume and a
    ``ConsumedOutcome`` for a row this execution does not own. Neither is it
    a ``WorkflowRunError``: the worker translates that family into a
    body-unresolvable parking, which a stale claim must never reach."""

    def __init__(self, *, node_id: JobId, claim_epoch: int) -> None:
        super().__init__(
            f"the hold mint's park matched no running row at claim_epoch="
            f"{claim_epoch} (node {node_id})"
        )
        self.node_id = node_id
        self.claim_epoch = claim_epoch


class SignalUnavailableError(RuntimeError):
    """``ctx.signal(name)`` read a signal that has no delivered payload
    (the read-before-delivery mistake — the typed refusal, never a
    silent None)."""


class LoopWaitShapeError(RuntimeError):
    """E13: a LOOP-kind body's wait site found the iteration cursor past
    the answer queue — the state a conditional wait mints (some
    iterations wait, others don't). The loop's answer-queue cursor is
    the iteration counter (the advance statement's own write), so the
    mis-aligned state means an earlier iteration's answer is stranded
    (it can never be consumed again — the cursor indexes answers by
    iteration), and a retry of a waited iteration would replay the
    wrong slot (the retry-replay law, silently broken). Before the
    typed error, the mis-index was silent (the wait minted a fresh
    hold; the operator was asked again; the stranded answer rotted on
    the rows) — this error gives the loop-shape rule ("one wait per
    iteration") its teeth: the named typed failure at the wait site,
    routed by the loop's failure-class rules exactly as they route the
    body's own exceptions. The boundary: a body that waits twice in one
    iteration is indistinguishable at the wait site from a retry (both
    are cursor < len) — the second wait re-reads the iteration's own
    answer; this docstring is that shape's only guard. A plain
    (non-loop) step never sees this rule: its claim-scoped cursor is
    consumption-ordered, so conditional waits are legal there."""


class Expired(BaseModel):
    """the TYPED EXPIRY MEMBER (the amendment — the timeout face is a
    VALUE, not an exception): the hold's deadline passed the DB clock
    (the sweep abandoned it) and the wait site RETURNS this member
    instead of raising — the wait's outcome is the CLOSED union
    ``Decision | Expired``, so the CHECKER forces the fail-close arm: a
    body that matches only the decision arm falls through and reds
    against its own declared return; a body that bare-unwraps reds the
    attribute (``Expired`` carries no decision fields — only the
    pointers).

    ``abandoned_by`` names the door that wrote the abandoned state (the
    state is one; the doors are two): ``"timeout"`` — the expiry sweep
    fired past ``expires_at`` (the default; the sweep's rows carry no
    payload marker) — or ``"operator"`` — the operator abandon door
    (``HitlClient.abandon`` / ``taskq flows abandon`` / the admin's
    Abandon) wrote ``abandoned_by='operator'`` on the row's payload.

    :class:`taskq.exceptions.SignalTimeoutError`
    stays in the vocabulary for the body that wants the timeout
    failure — it raises the glossary exception itself off the
    ``Expired`` member (the machinery never raises it anymore; the
    exception face cannot be forced, the union face can). The same
    value-not-machinery law is the operator door's:
    :class:`taskq.exceptions.SignalAbandonedError` is raised by a body
    that wants the operator-abandon to be a failure — off this same
    member when its ``abandoned_by`` reads ``"operator"``."""

    signal: str
    node_key: str
    hold_id: str
    abandoned_by: Literal["timeout", "operator"] = "timeout"


class WaitHost(Protocol):
    """The context surface the wait machinery reads (the split's typed
    seam — the fields live on the context, the machinery here; the
    timer's mixin, ``_ctx_timer.CtxTimerOps``, composes on the same
    protocol — the two waits share one host shape)."""

    flow_id: JobId
    job_id: JobId
    node_key: str
    attempt: int
    #: The claim's epoch: a re-entered wait re-parks its row fenced on it,
    #: and the answer-queue cursor / expiry-face marker is scoped to it.
    claim_epoch: int
    #: The wait-call counter (the wait site's deterministic call
    #: identity — see ``CtxWaitOps.wait_signal``): the body replays the
    #: same call order within one execution, so the Nth ``wait_signal``
    #: call has the same site id on every execution.
    _wait_call_seq: int
    _pool: asyncpg.Pool
    _wsql: WorkflowSql
    _is_loop: bool
    _ledger_id: JobId | None
    #: The map item's index (the F2 fix's identity leg: a map
    #: sibling's holds read and mint per sibling).
    _map_index: int | None
    #: The registered workflow's name (D1 — the signal-model catalog's
    #: key; the hold-context redact hook's resolution).
    _workflow_name: str
    #: The workflow's redact hook (chain → hook — the hold context's
    #: persist-time redact; the crash-window fix).
    _redact: Callable[[str], str] | None


class CtxWaitOps(WaitHost):
    """The context's wait/deliver mixin (the machinery's home; the
    context composes it)."""

    __slots__ = ()

    # The runtime-info dict's declaration (the mixin's typed face — the
    # field lives on StepContext; the mixin's wait path records into it).
    _runtime: dict[str, int]

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
        """
        the TUPLE FORM is the typed wait — ``await ctx.wait_signal((Approval,
        Escalate))``; the single-payload form is the one-member overload.
        The return is the CLOSED UNION of the declared models joined with
        :class:`Expired` — ``Decision | Expired`` — and the CHECKER
        forces the fail-close arm:

        >>> outcome = await ctx.wait_signal((ContinueApproval,), timeout_s=120.0)
        >>> match outcome:
        ...     case ContinueApproval() as approval: ...
        ...     case Expired(): ...  # the fail-close: finish with what you have

        A body that matches only the decision arm falls through (implicit
        ``None`` against the body's declared ``Done | Refine`` return — the
        checker reds it); a body that bare-unwraps reds the attribute
        (``Expired`` carries no decision fields). The type-probe corpus
        holds both markers (``tests/typeprobe/wf_wait_expired_negative_types.py``).

        A body that WANTS the timeout as
        a FAILURE raises :class:`taskq.exceptions.SignalTimeoutError`
        ITSELF off the ``Expired`` member — the machinery never raises it
        (the exception face cannot be forced; the union face can).

        No unresolved signal row → the node HOLDS (the held
        representation: pending + the deadline + the signal row as
        truth; the worker releases — no slot held; a LOOP node's budget
        PAUSES). A DELIVERED row (the resume) returns the payload.
        ``timeout=None`` must be EXPLICIT — the bare form is the W1
        validate warning ("a workflow that waits forever on a human is
        a support ticket").

        the TIMEOUT'S ARM (the union face's runtime half — the
        declaration's docstring carries the split): THIS ``timeout_s=``
        arms the RUNTIME deadline (the hold's expiry — the DB-clock
        wall the sweep fires); a matching ``GateDecl.timeout_s`` on the
        node feeds the COMPILE surfaces only (the Mermaid face, W1)
        and never arms the runtime. Where both are statically readable
        and they disagree, validate() names the drift (W4).

        An
        ABANDONED hold (the expiry sweep fired on this wait site, no
        held row stands) RETURNS the :class:`Expired` member — the
        fail-close arm the checker forces. The re-execution never
        automatically mints a new epoch — hold → expire → re-hold → ∞
        is the rejected dragon: the read filters the site's own rows by
        the call's identity, so every replay of the site lands the
        same deterministic face (the site's own abandoned row, never a
        mint); a deliberate re-wait (the body matched the expiry member
        and waits again) is a new body decision at a new call ordinal —
        the next call's own site state is empty, so it registers a new
        hold with a new epoch.

        ``discriminator=`` (the shape-keyed resolution's small honest API): when the
        payload fits MORE than one declared model, the gate's explicit
        picker resolves the union — its pick must be one of the fitting
        candidates. Without one, an ambiguous payload is the typed
        refusal at the deliver boundary.

        the DOUBLE-TIMEOUT PRECEDENCE (the law — both faces declare a
        timeout): THIS parameter arms the expiry sweep — the hold row's
        ``expires_at`` is written from THIS ``timeout_s`` (the DB clock
        plus it). The gate's ``GateDecl(timeout_s=…)`` is the
        COMPILE-VISIBLE declaration (the Mermaid face, the W1 warning)
        — never a second runtime clock. Keep them equal on purpose; when
        they disagree, the WAIT'S VALUE WINS, on the rows."""
        models: tuple[type[BaseModel], ...] = cast(
            tuple[type[BaseModel], ...],
            signals if isinstance(signals, tuple) else (signals,),
        )
        if not models:
            # a hold minted without a declared payload schema is the audit hole
            # — any payload delivers to it from a fresh process (the row's
            # payload_schema is the cold process's only witness). The
            # declaration is one tuple at the wait site; the hold never ships
            # contract-less. (Static face: the new signature refuses an empty
            # tuple at the checker — the guard keeps the runtime seam's teeth:
            # a cast-seam caller passes an empty tuple.)
            raise TypeError(
                "ctx.wait_signal requires at least one declared payload "
                "model — a hold without a declared contract is the audit "
                "hole (the typed door refuses to mint one)"
            )
        names = tuple(m.__name__ for m in models)
        signal_name = names[0] if len(names) == 1 else "|".join(names)
        from taskq.workflows.api._hitl import (
            mark_awaited,
            reassert_hold,
            refuse_reserved_timer_name,
            register_hold,
            register_signal_models,
        )

        # E17 — the reserved namespace at the mint: a payload model named
        # inside the durable
        # timer's reserved ``__timer__`` prefix would mint a hold every
        # reader mis-identifies as the clock's (invisible to
        # HitlClient.list(), unanswerable, never fail-closing, fired at
        # the deadline into a SignalPayloadError death). The refusal is
        # here — loud, typed, the fix named, before anything registers
        # or writes.
        for model_name in names:
            refuse_reserved_timer_name(model_name, mint="ctx.wait_signal")

        # the call ordinal is this wait site's identity: the body replays
        # the same call order within one execution, so the id is stable
        # across the resume's re-execution. the hold row carries it as
        # its call_id (the mint below) and the site's three reads — the
        # delivered-answer queue, the standing-hold arm, the abandoned
        # face — filter by it, so two sequential waits never surface
        # each other's rows.
        seq = self._wait_call_seq
        object.__setattr__(self, "_wait_call_seq", seq + 1)
        call_site = f"call:{self.attempt}:{seq}"
        # the row's call_id carries the minting attempt; the reader
        # replays the same ordinal under its own, so the set covers every
        # attempt up to this one. rows minted before the identity (a
        # call_id without the attempt:ordinal shape) are the site's own
        # too: the legacy leg keeps their answers, holds and faces
        # readable.
        site_call_ids = [f"call:{a}:{seq}" for a in range(1, self.attempt + 1)]

        # The catalog (the typed door's runtime registry): this process
        # ran the wait site — the deliver boundary validates against
        # these models (D1's discipline, the deliver path's face).
        register_signal_models(
            self._workflow_name, self.node_key, signal_name, models, discriminator
        )

        # resume-not-retry's ledger face: this attempt's ledger row
        # records 'awaited' (never 'failed' — the ladder counts failed
        # only).
        if self._ledger_id is not None:
            async with self._pool.acquire() as conn:
                await mark_awaited(conn, self._wsql.schema, self._ledger_id)

        # two facts, both structural. (1) the answer queue: the
        # delivered holds are the node's durable answers, consumed in
        # epoch order by each wait call's own sequence (the per-call
        # cursor, scoped to the executing claim, over the call-disjoint
        # queue below): a resume or a retry replays the answers (the
        # operator never re-answers), a wait past the queue's end
        # registers a new hold. (2) the consumed hold can never answer
        # twice. for a loop-kind node the cursor is the row's iteration
        # counter (the advance statement's own write): iteration k's wait
        # consumes the k-th answer — a hold-resume continues, a failed
        # iteration's retry replays its own answer. a plain step's cursor
        # is per call: each of the body's wait calls counts its own
        # consumptions under its own ordinal key, so one site's
        # consumption can never advance another site's position past its
        # own answer.
        async with self._pool.acquire() as conn:
            row_meta = await conn.fetchrow(
                render_sql(
                    "SELECT metadata->>'kind' AS kind, "
                    "(metadata->>'iteration')::int AS iteration "
                    "FROM {schema}.jobs WHERE id = $1",
                    self._wsql.schema,
                ),
                self.job_id,
            )
            is_loop_kind = row_meta is not None and row_meta["kind"] == "loop"
            row_iteration = (
                int(row_meta["iteration"])
                if row_meta is not None and row_meta["iteration"] is not None
                else 0
            )  # pyright: ignore[reportIndexType,reportArgumentType]
            # the cursor is keyed to the wait call (its own ordinal,
            # beneath the attempt), never bare to the attempt nor
            # shared per signal name: one body's two wait calls must
            # each count their own consumptions. a shared key lets
            # one site's consumption advance another site's position
            # past its own answer. ``seq`` gives every call its own
            # key and a plain step's body runs once per claim, so the
            # epoch fence is the whole mechanism here: a re-claim at a
            # refunded attempt ordinal reads no marker of its own and
            # replays from the queue's start instead of inheriting a
            # spent cursor. the ``"n"`` leg is still written for shape
            # parity with the timer and join markers, which do read
            # their count inside one claim.
            cursor = row_iteration
            if not is_loop_kind:
                marker = await read_claim_marker(
                    conn,
                    schema=self._wsql.schema,
                    job_id=self.job_id,
                    key=f"hold_replay_signal_{self.attempt}_{seq}",
                    claim_epoch=self.claim_epoch,
                )
                cursor = int(marker.get("n", 0))
            # every wait-site read carries the map index: a map sibling's
            # answers, holds and timeout face are the sibling's own rows —
            # a node_key-only read would let one sibling's hold or answer
            # contaminate another's. `IS NOT DISTINCT FROM` keeps the
            # plain step's NULL map_index exact. every read also carries
            # the call identity (the same `site_call_ids`): the queue is
            # the site's own delivered answers, never another call's.
            queue = await conn.fetch(
                render_sql(
                    "SELECT id, payload, hold_epoch FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND map_index IS NOT DISTINCT FROM $4::bigint "
                    "AND (call_id = ANY($5) OR call_id NOT LIKE 'call:%:%') "
                    "AND status = 'delivered' ORDER BY hold_epoch ",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
                self._map_index,
                site_call_ids,
            )
            if is_loop_kind and cursor > len(queue):
                # the iteration cursor may never run past the answer queue. The
                # aligned states are: cursor == len (the continuation's wait —
                # a fresh hold next) and cursor < len (a retry's replay).
                # cursor > len means earlier iterations did not wait — the
                # conditional-wait shape: answers stranded (unconsumable — the
                # cursor indexes by iteration) and retries replaying the wrong
                # slot. Named, loud, at the wait site: never a silent
                # mis-index.
                raise LoopWaitShapeError(
                    f"E13-uniform-loop-wait: this loop body's wait is CONDITIONAL "
                    f"(the iteration cursor {cursor} runs past the {len(queue)}-answer "
                    f"queue on {signal_name}) — a loop body must declare ONE wait per "
                    "iteration: the answer-queue cursor IS the iteration counter, so a "
                    "wait on only some iterations strands earlier answers (never "
                    "consumable again) and replays the wrong slot on retry. Wait "
                    "unconditionally each iteration (branch on the RESULT), or move "
                    "the wait out of the loop"
                )
            if cursor < len(queue):
                # this wait takes the queue's next answer (the claim's own
                # cursor advances — the same hold can never answer the same
                # claim twice; the loop's counter is the advance statement's
                # own write, never this one).
                answer = queue[cursor]
                payload = (
                    _json_loads(answer["payload"])
                    if isinstance(answer["payload"], str)
                    else answer["payload"]
                )
                if not is_loop_kind:
                    require_claim_epoch(
                        "ctx.wait_signal",
                        flow_id=self.flow_id,
                        node_key=self.node_key,
                        claim_epoch=self.claim_epoch,
                    )
                    await write_claim_marker(
                        conn,
                        schema=self._wsql.schema,
                        job_id=self.job_id,
                        key=f"hold_replay_signal_{self.attempt}_{seq}",
                        claim_epoch=self.claim_epoch,
                        marker={"n": cursor + 1},
                    )
                # the resumed body's answer identity — the epoch of the hold
                # this wait consumed (the body asserting on ctx sees which
                # answer it got).
                self._runtime["hold_epoch"] = int(answer["hold_epoch"])
                return cast("M", self._coerce_signal(models, payload, discriminator=discriminator))
            # past the queue: the node's pending hold (if any) is this
            # wait's wait — the held row stands (idempotent re-hold,
            # never a second registration of one wait). the read is the
            # site's own (the same call_id filter): an earlier call must
            # never raise NodeHeldError for a later call's standing hold.
            held_row = await conn.fetchrow(
                render_sql(
                    "SELECT id, signal_name, expires_at FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND map_index IS NOT DISTINCT FROM $4::bigint "
                    "AND (call_id = ANY($5) OR call_id NOT LIKE 'call:%:%') "
                    "AND status = 'held' ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
                self._map_index,
                site_call_ids,
            )
            if held_row is not None:
                # this claim re-entered onto the standing hold: park the row
                # onto it (the stamp names the row that stands, which may be
                # one of the names rather than the joined one). A missed park
                # wrote nothing — the row belongs to a newer claim — so the
                # unwind is the fresh mint's own fence label, never "held".
                parked = await reassert_hold(
                    conn,
                    schema=self._wsql.schema,
                    node_id=self.job_id,
                    hold_id=held_row["id"],
                    signal_name=str(held_row["signal_name"]),
                    expires_at=held_row["expires_at"],
                    claim_epoch=self.claim_epoch,
                )
                if not parked:
                    raise HoldMintFencedError(node_id=self.job_id, claim_epoch=self.claim_epoch)
                raise NodeHeldError(hold_id=str(held_row["id"]), signal_names=names)
                # the sweep marked this wait site's hold 'abandoned' and no held
            # row stands — the wait site returns the union's ``Expired`` member
            # (the fail-close arm the checker forces; the body that wants the
            # failure raises the glossary exception itself off the member). no
            # automatic new epoch: a re-execution after abandonment never
            # re-holds. the return is unconditional on the site's own row, so
            # every replay of the site lands the same deterministic face. a
            # deliberate re-wait is a new body decision at a new call ordinal:
            # the next call's own site state is empty, so that registration
            # mints the new hold with the new epoch. (the one-shot
            # `timeout_face_{attempt}_{seq}` marker is gone: with the reads
            # site-disjoint by call_id its only remaining effect was a
            # same-attempt re-execution minting past the face, and its read's
            # fall-through was unreachable. the rows are the truth.)
            abandoned_row = await conn.fetchrow(
                render_sql(
                    "SELECT id, hold_epoch, "
                    "COALESCE(payload->>'abandoned_by', 'timeout') AS abandoned_by "
                    "FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND map_index IS NOT DISTINCT FROM $4::bigint "
                    "AND (call_id = ANY($5) OR call_id NOT LIKE 'call:%:%') "
                    "AND status = 'abandoned' ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
                self._map_index,
                site_call_ids,
            )
            if abandoned_row is not None:
                # the expiry member returns — the closed union's second
                # arm, the fail-close the checker forces. The member
                # carries the DOOR that wrote the abandoned row (the
                # sweep's rows read 'timeout'; the operator abandon's
                # marker reads 'operator'). The body that wants the
                # timeout failure raises SignalTimeoutError itself off
                # this member (the escalation ladder's own use); the
                # body that wants the operator failure raises
                # SignalAbandonedError off the same arm when the door
                # reads 'operator'; the body that matches it is the
                # fail-close.
                return Expired(
                    signal=signal_name,
                    node_key=self.node_key,
                    hold_id=str(abandoned_row["id"]),
                    abandoned_by=str(abandoned_row["abandoned_by"]),  # pyright: ignore[reportArgumentType]  # Why: the row's COALESCE vocabulary is the writer's own closed pair ('timeout'/'operator') — the member's Literal re-validates it.
                )
            # A new hold: a new epoch (the count of this name's holds —
            # the identity's mint). the mint is per map sibling (the F2
            # fix): siblings of one source mint independent epoch
            # sequences (the pre-fix shared mint collided the
            # siblings' epochs onto one row).
            epoch = await conn.fetchval(
                render_sql(
                    "SELECT COALESCE(MAX(hold_epoch), 0) + 1 FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 "
                    "AND map_index IS NOT DISTINCT FROM $3::bigint",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                self._map_index,
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
                # the call site's OWN id (deterministic — the replay
                # reproduces it), never a random uuid: the abandoned-row
                # read above filters its site's rows by it.
                call_id=call_site,
                payload_schema=schema_ref,
                timeout_s=timeout_s,
                context={"reason": reason, "tool": tool, "args": args},
                redact=self._redact,
                map_index=self._map_index,
                claim_epoch=self.claim_epoch,
            )
        raise NodeHeldError(hold_id=str(hold_id), signal_names=names)

    @staticmethod
    def _coerce_signal(
        models: Any,
        payload: Any,
        *,
        discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
    ) -> Any:
        """The delivered payload re-validates into the declared model
        (the typed wait's return — narrows with ``isinstance``). BY
        SHAPE, never by declaration order (the shape-keyed law): every
        declared model is tried STRICTLY; exactly one must fit (it is
        returned); zero or more than one is the typed refusal —
        :class:`taskq.exceptions.SignalPayloadError` /
        :class:`taskq.exceptions.SignalPayloadAmbiguousError` (on the
        deliver path this refusal happens before the hold is consumed;
        here at the replay it is the body's typed failure)."""
        from taskq.workflows.api._hitl import resolve_payload_fit

        if not isinstance(payload, dict):
            return payload
        payload_doc = cast("dict[str, object]", payload)  # pyright: ignore[reportUnknownVariableType] — the answer-queue's jsonb decode, the Any-contract walk's boundary (the isinstance guard above is the runtime shape check).
        fitted = resolve_payload_fit(models, payload_doc, discriminator)
        return cast(Any, fitted.model_validate(payload_doc))

    async def signal(self, name: str) -> Any:
        """Read a DELIVERED signal's payload on resume (the
        the payload rides the ROW; the author never hand-rolls a side
        table). The read is the sibling's own: the
        map_index filter keeps a map sibling off a sibling's answers."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                render_sql(
                    "SELECT payload, status FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND map_index IS NOT DISTINCT FROM $4::bigint "
                    "ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                name,
                self._map_index,
            )
        if row is None or row["status"] != "delivered":
            raise SignalUnavailableError(
                f"signal {name!r} has no delivered payload on "
                f"{self.node_key!r} (status: {row['status'] if row else 'none'})"
            )
        return _json_loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]

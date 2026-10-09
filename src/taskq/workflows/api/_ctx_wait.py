"""The body's HITL WAIT/DELIVER machinery (the split of ``api/_runner`` —
§7b's concerns-separate law): the wait site's typed hold, the
answer-queue replay, the timeout face, and the delivered-payload read.
The deliver boundary itself (the admin/hitl client's side) lives in
``api/_hitl``; THIS module is the ctx's side of the same contract.

THE RESUME CONTRACT (documented ON the wait method — cut #18's
disposition): **the body re-executes FROM THE TOP on resume** — make it
idempotent; pre-wait side effects are ``ctx.step``-ledgered and replay
cheap.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, cast

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.workflows.api._sql_runner import render_sql

if TYPE_CHECKING:
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql

__all__ = ["CtxWaitOps", "SignalUnavailableError"]

import structlog

from taskq.obs import get_logger

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


class NodeHeldError(Exception):
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


class _WaitHost(Protocol):
    """The context surface the wait machinery reads (the split's typed
    seam — the fields live on the context, the machinery here)."""

    flow_id: JobId
    job_id: JobId
    node_key: str
    attempt: int
    _pool: asyncpg.Pool
    _wsql: WorkflowSql
    _is_loop: bool
    _ledger_id: JobId | None
    #: The REGISTERED workflow's name (D1 — the signal-model catalog's
    #: key; the hold-context redact hook's resolution).
    _workflow_name: str
    #: The workflow's redact hook (chain → hook — the hold context's
    #: persist-time redact; attack-3 H3's cure).
    _redact: Callable[[str], str] | None


class CtxWaitOps(_WaitHost):
    """The context's wait/deliver mixin (the machinery's home; the
    context composes it)."""

    __slots__ = ()

    # The runtime-info dict's DECLARATION (the mixin's typed face — the
    # field lives on StepContext; the mixin's wait path records into it).
    _runtime: dict[str, int]

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
        if not models:
            # THE CONTRACT IS MANDATORY AT HOLD TIME (attack-4
            # F-P4-UNTYPED-COLD-DOOR's cure, the authoring half): a hold
            # minted without a declared payload schema is the audit hole —
            # ANY payload delivers to it from a fresh process (the row's
            # payload_schema is the cold process's only witness). The
            # declaration is one tuple at the wait site; the hold never
            # ships contract-less.
            raise TypeError(
                "ctx.wait_signal requires at least one declared payload "
                "model — a hold without a declared contract is the audit "
                "hole (the typed door refuses to mint one)"
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
        #
        # THE LOOP'S ITERATION INDEX IS THE CURSOR (the deploy matrix's
        # multi-HITL cure): a LOOP resume is a CONTINUATION of the
        # iteration sequence, never a replay of it — the per-attempt
        # cursor restarts at 0 on every resume (a new attempt), so the
        # resumed loop's later iterations re-consumed the EARLIER
        # answers (the deep-research march's third iteration received
        # the second refine, the carry ran past the cap, the loop
        # exhausted without the operator's third decision ever landing).
        # For a loop-kind node the queue position IS the row's iteration
        # counter (the ADVANCE statement's own write — the derived value
        # has exactly one writer): iteration k's wait consumes the k-th
        # answer — a hold-resume continues, a retry of a failed
        # iteration replays ITS OWN answer (the same k-th), both laws
        # one mechanism. A loop body declares ONE wait per iteration
        # (the loop's shape); the plain steps keep the per-attempt
        # cursor.
        async with self._pool.acquire() as conn:
            row_meta = await conn.fetchrow(
                render_sql(
                    "SELECT (metadata ->> $2)::int AS cursor, "
                    "metadata->>'kind' AS kind, "
                    "(metadata->>'iteration')::int AS iteration "
                    "FROM {schema}.jobs WHERE id = $1",
                    self._wsql.schema,
                ),
                self.job_id,
                f"hold_cursor_{self.attempt}",
            )
            is_loop_kind = row_meta is not None and row_meta["kind"] == "loop"
            row_cursor = (
                int(row_meta["cursor"])
                if row_meta is not None and row_meta["cursor"] is not None
                else 0
            )  # pyright: ignore[reportIndexType,reportArgumentType]
            row_iteration = (
                int(row_meta["iteration"])
                if row_meta is not None and row_meta["iteration"] is not None
                else 0
            )  # pyright: ignore[reportIndexType,reportArgumentType]
            cursor = row_iteration if is_loop_kind else row_cursor
            queue = await conn.fetch(
                render_sql(
                    "SELECT id, payload, hold_epoch FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = ANY($3) "
                    "AND status = 'delivered' ORDER BY hold_epoch ",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                [*names, "|".join(names)],
            )
            if cursor < len(queue):
                # THE REPLAY/CONSUME: this wait takes the queue's next
                # answer (the per-attempt cursor advances — the same
                # hold can never answer the same attempt twice; the
                # loop's counter is the ADVANCE statement's own write,
                # never this one).
                answer = queue[cursor]
                payload = (
                    _json_loads(answer["payload"])
                    if isinstance(answer["payload"], str)
                    else answer["payload"]
                )
                if not is_loop_kind:
                    await conn.execute(
                        render_sql(
                            "UPDATE {schema}.jobs SET metadata = metadata || $2::jsonb WHERE id = $1",
                            self._wsql.schema,
                        ),
                        self.job_id,
                        dumps_jsonb_str({f"hold_cursor_{self.attempt}": cursor + 1}),
                    )
                # THE CONTEXT CONTRACT: the resumed body's answer
                # identity — the epoch of the hold THIS wait consumed
                # (the body asserting on ctx sees which answer it got).
                self._runtime["hold_epoch"] = int(answer["hold_epoch"])
                return self._coerce_signal(models, payload, discriminator=discriminator)
            # PAST THE QUEUE: the node's PENDING hold (if any) is THIS
            # wait's wait — the held row stands (idempotent re-hold,
            # never a second registration of one wait).
            held_row = await conn.fetchrow(
                render_sql(
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
                raise NodeHeldError(hold_id=str(held_row["id"]), signal_names=names)
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
                render_sql(
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
                    render_sql(
                        "SELECT (metadata ->> $2)::int FROM {schema}.jobs WHERE id = $1",
                        self._wsql.schema,
                    ),
                    self.job_id,
                    face_key,
                )
                if face_seen != int(abandoned_row["hold_epoch"]):
                    await conn.execute(
                        render_sql(
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
                render_sql(
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
                render_sql(
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

"""The body's DURABLE TIMER (T28) — the hold whose RESOLVER IS THE
CLOCK: ``ctx.wait_until(when)`` mints a hold row whose deadline-pass is
THE WAKE, never the fail-close.

THE TIMER'S FACE INVERTS THE EXPIRY'S (the design law this module
implements; the guide's §4b carries the same text): the HITL wait's
deadline-pass (``wait_signal``'s ``timeout_s``) = the :class:`~taskq.
workflows.api._ctx_wait.Expired` MEMBER — the fail-close arm the
checker forces. THE TIMER'S deadline-pass = THE WAKE: the sweep's timer
arm CAS's the hold ``'held' → 'delivered'`` with the CLOCK'S OWN
RECEIPT (the ``TimerWake`` payload — ``fired_by='timer'``, the
scheduled vs the fired-at stamps), clears the node's held
representation, and the body RESUMES — the timer's point is the
CONTINUATION ("remind them tomorrow"), never the fail-close.

THE ROW IS THE TRUTH (the hold's law, unmodified): the timer IS a
``wf_signals`` hold row (the reserved ``__timer__`` signal prefix
names it); the held representation releases the worker's slot (the
slot is NEVER parked — a wait costs no lease, no claim, no attempt);
a LOOP node's budget pauses while the timer stands (the same derived
pause the human hold takes — the wall is blind while the body waits on
the clock, exactly as while it waits on a human). A worker DEATH
mid-wait changes nothing: the row's expiry arms the reclaim, the
successor's sweep fires it, the body resumes — the durability IS the
design, not a repair.

THE RESUME CONTRACT is the wait machinery's own (documented on
``wait_signal``): the body re-executes FROM THE TOP on resume — make
it idempotent; pre-timer side effects are ``ctx.substep``-ledgered and
replay cheap. On the resume pass the wait site finds the DELIVERED
timer row (the answer queue's replay/consume, the per-attempt cursor —
the same stale-payload dragon's kill sites) and RETURNS the typed
:class:`TimerWake`.

THE AT-LEAST FACE (the documented precision — the listener landmine 4's
law, restated for the timer): the fire lands at the FIRST sweep pass
at-or-after the deadline (the DB clock's comparison), never at a
precise instant — the wake's ``fired_at`` is at-or-after ``scheduled``
by construction, and the delta is the receipt's own arithmetic
(:attr:`TimerWake.lateness_s`).
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel

from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.workflows.api._ctx_wait import LoopWaitShapeError, NodeHeldError, WaitHost
from taskq.workflows.api._sql_runner import render_sql

__all__ = ["CtxTimerOps", "TimerWake"]


class TimerWake(BaseModel):
    """THE TYPED WAKE (the clock's own receipt — the timer's return):
    the wake carries the timer's identity (``key`` — the caller's name
    or ``""`` for the anonymous timer), the SCHEDULED instant (the
    deadline the body asked for — the hold row's ``expires_at``, read
    back from the row at fire time), the FIRED-AT instant (the sweep
    pass's DB clock — at-or-after ``scheduled`` by construction: the
    at-least face's receipt), and the resolver's mark
    (``fired_by='timer'`` — the clock resolved this hold; a human never
    did, the payload schema's strict fit refuses any other delivery)."""

    key: str
    scheduled: datetime
    fired_at: datetime
    fired_by: Literal["timer"] = "timer"

    @property
    def lateness_s(self) -> float:
        """THE AT-LEAST FACE'S DELTA: ``fired_at - scheduled`` in
        seconds — zero when the sweep's pass landed exactly on the
        deadline, positive by the sweep-interval + failover gap it
        actually cost. The docs' law ("never promise a precise
        instant") is this number, on every wake."""
        return (self.fired_at - self.scheduled).total_seconds()


def normalize_timer_when(when: datetime | float) -> tuple[datetime | None, float | None]:
    """The deadline's normalization (the typed door's arithmetic): a
    *datetime* → the UTC-aware absolute instant (a naive datetime is
    the UTC reading — the wait method's stated law; the DB never
    re-bases it), a *float* → the relative seconds (the DB clock owns
    the base). A non-finite float is refused LOUDLY (an infinite timer
    is the W1 warning's subject wearing a different hat — say what you
    mean with a far datetime); a cast-seam caller's non-temporal junk
    dies on the float() conversion's own TypeError, re-raised as the
    typed refusal."""
    if isinstance(when, datetime):
        aware = when if when.tzinfo is not None else when.replace(tzinfo=UTC)
        return aware.astimezone(UTC), None
    try:
        seconds = float(when)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "ctx.wait_until takes a datetime or a float (seconds from now) — "
            f"got {type(when).__name__!r}"
        ) from exc
    if not math.isfinite(seconds):
        raise ValueError(
            f"ctx.wait_until takes a finite deadline — got {seconds!r} "
            "(an infinite timer is the W1 warning's subject wearing a "
            "different hat: say what you mean with a far datetime)"
        )
    return None, seconds


class CtxTimerOps(WaitHost):
    """The context's timer mixin (the machinery's home; the context
    composes it beside :class:`~taskq.workflows.api._ctx_wait.CtxWaitOps`
    — the two waits share the hold machinery, the answer queue and the
    cursor, and nothing else)."""

    __slots__ = ()

    # The runtime-info dict's DECLARATION (the mixin's typed face — the
    # field lives on StepContext; the mixin's consume path records into it).
    _runtime: dict[str, int]

    async def wait_until(
        self,
        when: datetime | float,
        *,
        key: str | None = None,
    ) -> TimerWake:
        """THE DURABLE TIMER (T28): hold until the clock says *when* —
        ``await ctx.wait_until(when, key="remind")``. The body's node
        takes the HELD REPRESENTATION (the slot is released — no lease,
        no claim, no attempt burned; a LOOP node's budget pauses), the
        hold row stands (``wf_signals`` under the reserved
        ``__timer__`` signal prefix), and the SWEEP's timer arm fires
        the wake at the first pass at-or-after the deadline: the clock
        itself resolves the hold (``fired_by='timer'``) with its own
        receipt, the node re-claims, and THE BODY RESUMES.

        THE INVERTED FACE (vs ``wait_signal``'s expiry): a deadline
        pass here is THE WAKE, never the fail-close — the continuation
        is the timer's point. There is NO ``Expired`` arm and no
        automatic re-arm: the hold fires exactly once, the wake is the
        consumed answer (a replay/retry re-consumes it from the answer
        queue — the operator's re-run never re-sleeps).

        *when* is a :class:`~datetime.datetime` (the absolute instant)
        or a *float* of seconds (the DB clock owns the base — the
        estate's clock doctrine; the row's ``expires_at`` is that
        clock's reading).

        THE NAIVE-DATETIME LAW (read this before you pass a bare
        datetime): a NAIVE datetime — no ``tzinfo`` — is interpreted as
        UTC. The coercion is silent (``when.replace(tzinfo=UTC)``): no
        error, no local-zone reading — ``17:00`` naive means ``17:00
        UTC``, NOT your server's 17:00. THE DB never re-bases the
        instant: the row's ``expires_at`` is exactly the coerced value,
        the wake fires at that UTC instant, and the wake's ``scheduled``
        stamp reads it back unchanged. Pass a tz-AWARE datetime
        (``datetime.now(timezone.utc) + timedelta(...)``) to mean what
        you said; a naive one is accepted for continuity and READS AS
        UTC.

        *key* names the timer (the wake's ``key``; timers and signals
        of one node share the answer queue's cursor — the loop's
        ONE-wait-per-iteration shape law reads both the same way). A
        key beginning with the reserved ``__timer__`` prefix is the
        typed E17 refusal (the reserved namespace is the clock's own —
        no caller names a timer inside it).

        THE DURABILITY (the row's the truth): a worker death mid-wait
        is invisible to the timer — the hold row's expiry arms the
        successor's sweep, the wake fires, the body resumes (the
        re-execution doctrine: from the top; the pre-timer substeps
        replay). THE CANCEL INTERPLAY: the flow's cancel cascades the
        held timer row to ``cancelled`` — the hold dies with the flow,
        the wake never fires (the sweep arm only resolves 'held' rows).

        THE TIMER IS NOT A HUMAN HOLD: ``HitlClient.list()`` never
        enumerates it (the reserved prefix), the expiry arm never
        abandons it, and the resolve boundary's strict schema fit
        refuses any hand-delivered payload — the clock is the only
        resolver this hold takes."""
        from taskq.workflows.api._hitl import (
            TIMER_SIGNAL_PREFIX,
            TIMER_WAKE_SCHEMA_NAME,
            mark_awaited,
            refuse_reserved_timer_name,
            register_timer_hold,
        )

        absolute, relative = normalize_timer_when(when)
        # E17 — THE CALLER'S KEY (the timer-cures lane's cure 1): a key
        # beginning with the reserved prefix would mint a DOUBLE-prefixed
        # row (this method composes the prefix itself) whose wake's
        # key-strip mis-names it — the caller's key stays outside the
        # reserved namespace, the same typed refusal the human mint
        # carries.
        if key is not None:
            refuse_reserved_timer_name(key, mint="ctx.wait_until")
        signal_name = TIMER_SIGNAL_PREFIX + (key or "")

        # RESUME-NOT-RETRY's ledger face: THIS attempt's ledger row
        # records 'awaited' (never 'failed') — the timer's resume burns
        # no attempt, exactly as the human hold's does.
        if self._ledger_id is not None:
            async with self._pool.acquire() as conn:
                await mark_awaited(conn, self._wsql.schema, self._ledger_id)

        async with self._pool.acquire() as conn:
            # The cursor/kind read — the wait machinery's own shape (the
            # answer queue + the per-attempt cursor are SHARED with
            # wait_signal: one node, one queue, one cursor; the loop's
            # counter is the ADVANCE statement's own write).
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
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND map_index IS NOT DISTINCT FROM $4 "
                    "AND status = 'delivered' ORDER BY hold_epoch ",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                signal_name,
                self._map_index,
            )
            if is_loop_kind and cursor > len(queue):
                # THE E13 RULE, the timer's leg: the same uniform-wait
                # law the human hold takes (the cursor is shared; a
                # conditional TIMER in a loop strands answers exactly
                # as a conditional signal-wait does).
                raise LoopWaitShapeError(
                    f"E13-uniform-loop-wait: this loop body's timer is CONDITIONAL "
                    f"(the iteration cursor {cursor} runs past the {len(queue)}-wake "
                    f"queue on {signal_name}) — a loop body must declare ONE wait per "
                    "iteration: the answer-queue cursor IS the iteration counter. "
                    "Wait unconditionally each iteration (branch on the RESULT), or "
                    "move the timer out of the loop"
                )
            if cursor < len(queue):
                # THE WAKE'S REPLAY/CONSUME: the delivered receipt IS
                # the answer (the per-attempt cursor advances — the
                # loop's counter is the ADVANCE statement's own write,
                # never this one).
                wake_row = queue[cursor]
                payload = (
                    _json_loads(wake_row["payload"])
                    if isinstance(wake_row["payload"], str)
                    else wake_row["payload"]
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
                self._runtime["hold_epoch"] = int(wake_row["hold_epoch"])
                return TimerWake.model_validate(payload)
            # PAST THE QUEUE: the standing timer hold is THIS wait's
            # wait — the held row stands (idempotent re-hold, never a
            # second registration of one wait). THE MAP INDEX FILTER
            # (the holdmap lane's identity cure, the timer's leg): a
            # sibling's held row is NOT this sibling's — a plain
            # step's NULL keeps exact via IS NOT DISTINCT FROM.
            held_row = await conn.fetchrow(
                render_sql(
                    "SELECT id FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 AND signal_name = $3 "
                    "AND map_index IS NOT DISTINCT FROM $4 "
                    "AND status = 'held' ORDER BY hold_epoch DESC LIMIT 1",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                signal_name,
                self._map_index,
            )
            if held_row is not None:
                raise NodeHeldError(hold_id=str(held_row["id"]), signal_names=(signal_name,))
            # THE TIMER HOLDS ARE NEVER ABANDONED (the inverted face's
            # other half): no 'abandoned' arm here — the expiry sweep
            # excludes the reserved prefix by law, so a past-due timer
            # is a due timer (the arm fires it), never a fail-close.
            # A NEW HOLD: a NEW epoch (the count of this name's holds —
            # THIS identity's: the map index filters, a sibling's epoch
            # never gates this sibling's next hold).
            epoch = await conn.fetchval(
                render_sql(
                    "SELECT COALESCE(MAX(hold_epoch), 0) + 1 FROM {schema}.wf_signals "
                    "WHERE workflow_id = $1 AND node_key = $2 "
                    "AND map_index IS NOT DISTINCT FROM $3",
                    self._wsql.schema,
                ),
                self.flow_id,
                self.node_key,
                self._map_index,
            )
            hold_id = await register_timer_hold(
                conn,
                schema=self._wsql.schema,
                workflow_id=self.flow_id,
                node_id=self.job_id,
                node_key=self.node_key,
                signal_name=signal_name,
                hold_epoch=int(epoch),  # pyright: ignore[reportAny, reportArgumentType]
                absolute=absolute,
                relative=relative,
                payload_schema={TIMER_WAKE_SCHEMA_NAME: TimerWake.model_json_schema()},
                map_index=self._map_index,
            )
        raise NodeHeldError(hold_id=str(hold_id), signal_names=(signal_name,))

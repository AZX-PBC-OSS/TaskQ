"""the DEFERRAL DISPOSITION (the body's control-flow signals' wf face —
the split of ``api/_runner`` §7b's concerns-separate law): the machinery
that translates the vanilla surface's typed control-flow signals —
:class:`~taskq.exceptions.Snooze` (reschedule, does not consume retry
budget) and :class:`~taskq.exceptions.RetryAfter(delay, consume_budget=)`
(reschedule, consumes by default) — into one bounded finalize
disposition on the claimed node row.

The signals are
``TaskQError`` subclasses, so a body raising one fell into the runner's
body-failure catch and LADDERED — the attempt budget burned on a
deferral that never executed, the node parked at the backoff curve and
exhausted. The vanilla consumer translated the very same signals at its
own body seam (``worker/_consumer.py``'s ``_preserved_exc`` catch) into
the row dispositions ``mark_snoozed`` / ``mark_retry_after_consume_{
true,false}`` (``backend/_sql_templates.py``); the workflow runner had
no translation. one machinery now: the row law lives in
``_sql_finalize.NODE_DEFER_SQL`` (the vanilla arms mirrored onto the wf
finalize's own fence vocabulary — the claim CAS, the cancel mark, the
budget gate), and this module owns the disposition's RESOLUTION and the
verdicts' routes.

the NAMED DISPOSITIONS (the extension seam, stated once — the law this
module exists to keep):

* ``"snooze"`` — the NON-consuming deferral: the claim's attempt
  increment is REFUNDED (the vanilla ``ATTEMPT_REFUND_SQL`` law), the
  delay carries the vanilla's ``MIN_DEFERRAL_INTERVAL`` floor, the
  counter moves. ``Snooze`` resolves here, and so does
  ``RetryAfter(consume_budget=False)`` — the vanilla's own vocabulary
  ("consume_budget=False: snooze semantics", ``docs/guides/retries.md``).
* ``"retry_after_consuming"`` — the budget-bounded retry: the claim's
  increment STANDS (the burn), the raw delay is kept, and the budget
  gates it — past ``max_attempts`` the EXHAUSTED verdict routes the node
  through the wf terminal door as ``MaxAttemptsExceeded`` (the vanilla's
  own class + message).

#718's ``EmitBackpressure`` is deliberately not another timer arm here:
its ordinary-capacity control uses the separate EMIT_BACKPRESSURE_PARK_SQL
and fresh post-commit wake helpers, with no delay, snooze_count, or provider
retry counter. Generic timer/provider dispositions remain unchanged.

The next timed deferral DISPOSITION (a new signal, a new
non-terminal row law) adds exactly three things and touches nothing
else —

1. a member on :data:`DeferralDisposition` (the named set, never a bare
   string at a call site);
2. one arm in ``NODE_DEFER_SQL`` keyed on the disposition bind (the
   row law's own CASE arms — the vanilla dispositions' shape);
3. one arm in :func:`deferral_disposition` (the signal → disposition
   resolution — the catch arm's whole mapping).

A new TERMINAL verdict is not that seam: a verdict the ROW LAW refuses
to apply (a death the doors own) needs the three things above for its
disposition PLUS a per-door route in both mixin methods
(:meth:`DeferOps._defer_step` and
:meth:`~taskq.workflows.api._runner_loop.LoopOps._defer_loop_step`) —
the doors' semantics differ by law (the step's exhaustion finalizes
through the wf terminal door; the loop's through ``_exhaust_loop``'s
named state), so the ROUTE is per-door and the docs do not overclaim
"never a third case" for it. What stays shared either way: the row law
(:func:`defer_row_law`), the ledger's truth, and the terminal doors'
own mechanics (:func:`route_defer_terminal`).

The catch arms (the STEP's in ``api/_runner``, the LOOP's in
``api/_runner_loop``) consume signals and dispatch by the named
disposition; neither grows a per-signal branch. The ROW LAW + the
LEDGER'S TRUTH are one shared phase (:func:`defer_row_law` — the two
catch arms' common body); the TERMINAL verdicts route per SEAM — the
step's through the wf terminal door (:meth:`DeferOps._defer_step`), the
loop's through the loop's own doors
(:meth:`~taskq.workflows.api._runner_loop.LoopOps._defer_loop_step`:
the named state + STRANDED-FLOW ride ``_exhaust_loop``). The ladder's
own arms (the count, the backoff, the deterministic route) are
untouched — the deferral is the ladder's SIBLING (the budget's other
face), never its special case.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

from taskq.backend._protocol import JobId
from taskq.constants import (
    # The exhaustion message's single home; re-exported from this module
    # because the loop's deferral route — the exhausted verdict's own
    # body-failure face — reads it beside the loop's own named-state
    # machinery (module-level public under the same name).
    BUDGET_EXHAUSTED_MESSAGE,
    ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED,
)
from taskq.exceptions import RetryAfter, Snooze
from taskq.obs import get_logger
from taskq.workflows._emit import EmitBackpressure
from taskq.workflows._policy import TerminalEffect, TerminalEffectReason
from taskq.workflows._progress import KIND_NODE_TERMINAL
from taskq.workflows._sql_finalize import EMIT_BACKPRESSURE_PARK_SQL

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "BUDGET_EXHAUSTED_MESSAGE",
    "DeferOps",
    "DeferralDisposition",
    "defer_row_law",
    "deferral_disposition",
    "route_defer_terminal",
]

logger: Any = get_logger(__name__)

#: The wf cancel's own error class (``CANCEL_NODES_SQL_TEMPLATE``'s event
#: stamp): the cancel-fenced deferral terminalises the node under the
#: cancel's vocabulary, never a body-failure class.
WORKFLOW_CANCELLED_CLASS: Final[str] = "WorkflowCancelled"

#: the named deferral dispositions (the extension seam's member set —
#: see the module docstring for the law).
DeferralDisposition = Literal[
    "snooze", "retry_after_consuming", "retry_after_non_consuming", "emit_backpressure"
]


def deferral_disposition(exc: EmitBackpressure | Snooze | RetryAfter) -> DeferralDisposition:
    """the SIGNAL → DISPOSITION RESOLUTION (the catch arm's whole
    mapping — one function, never a per-signal branch at the call site).

    * ``Snooze`` → ``"snooze"`` (the non-consuming deferral: the refund,
      the floor, the counter);
    * ``RetryAfter`` → the ``consume_budget`` flag's own face —
      ``True`` the consuming retry (the burn, the budget gate),
      ``False`` the vanilla's own snooze semantics.

    The next
    deferral verdict adds ITS arm here, beside the two above.
    """
    if isinstance(exc, EmitBackpressure):
        return "emit_backpressure"
    if isinstance(exc, RetryAfter) and exc.consume_budget:
        return "retry_after_consuming"
    return "snooze"


class _DeferHost(Protocol):
    """The runner surface the deferral reads (the split's typed seam —
    the same shape the ladder's ``_LadderHost`` keeps)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    schema: str
    # The compiled workflow (the declared capture policy's source — the
    # ladder's own field, read the same way).
    compiled: Any
    _worker_id: JobId
    # The bounded-run gate (the §4.1 event rule's host-side fact) and the
    # terminal route's own post-COMMIT drain trigger.
    _emit_capacity: bool

    async def _drain_created_intent(self, flow_id: JobId, intent_created: bool) -> None: ...

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None: ...

    def _terminal_effect_for(self, node: Any, row: dict[str, Any]) -> TerminalEffect | None: ...

    async def _exhaust_loop(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        error_class: str | None,
        message: str | None,
        *,
        worker_id: JobId,
        attempt: int,
        claim_epoch: int,
    ) -> None:
        """the LOOP'S own EXHAUSTION DOOR (the exhausted verdict's route
        for the loop's deferral — the loop's named state + STRANDED-FLOW
        + the escalation ride the loop's own machinery, never a
        re-spelling)."""


@dataclass(frozen=True, slots=True)
class _DeferOutcome:
    """The shared deferral row-law's verdict plus its one-bit §4.1 result
    (True only when the park's own event crossed the threshold and created
    the durable promotion intent — the caller's acknowledged post-COMMIT
    drain trigger)."""

    verdict: str
    intent_created: bool = False


async def defer_row_law(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    job_id: JobId,
    worker_id: JobId,
    delay: timedelta,
    attempt: int,
    claim_epoch: int,
    disposition: DeferralDisposition,
    signal_name: str,
    ledger_id: JobId | None,
    ledger_claimed_at: Any,
    backpressure: EmitBackpressure | None = None,
    source_logical_attempt: int | None = None,
    iteration_rest: tuple[JobId, Any] | None = None,
) -> _DeferOutcome:
    """the ROW LAW + the LEDGER'S TRUTH, one transaction (the terminal-atomicity law's own
    shape — a split write leaves the row 'scheduled' with the ledger
    'running': the phantom the fence prevents) — the SHARED phase of both
    verdict routes (the STEP's and the LOOP's; the two mixins' own
    methods apply the verdict, never the SQL):

    * ``"deferred"`` — the row re-scheduled at the delay (the attempt
      arm's refund/keep law applied) and the claim's ledger row rested
      ``awaited`` (the hold's own resume-not-retry status — never a
      ``failed`` row the ladder's budget read would count);
    * ``"fenced"`` — the ZOMBIE's deferral: the claim fence matched
      nothing (a peer re-claimed the row at a newer attempt) and the
      attempt records 'fenced' on the ledger (never a running row
      left forever), keyed on the claim's own RETURNING instant (the
      re-mint guard — a re-minted row is the LIVE winner's, never
      ours);
    * ``"exhausted"`` / ``"cancel_fenced"`` — the row law wrote NOTHING
      (the verdict names the caller's own terminal door).

    ``disposition``/``signal_name`` arrive pre-resolved
    (:func:`deferral_disposition` + the signal's class name) — this
    function is the ROW LAW only, never the signal's interpretation.

    ``iteration_rest`` is the LOOP driver's deferred ITERATION claim's own
    identity (id, instant): the epoch arbiter gives every claim its own
    ledger row, so the deferred iteration's claim must rest 'awaited'
    (or 'fenced' when the deferral was) in the SAME transaction — never a
    running row left beside the resumed claim's own row.
    """
    if (backpressure is None) != (disposition != "emit_backpressure"):
        raise ValueError("EmitBackpressure and its disposition must be supplied together")
    if disposition != "emit_backpressure":
        from taskq.workflows._policy import validate_retry_after_delay

        validate_retry_after_delay(delay)
    park_intent_created = False
    async with pool.acquire() as conn, conn.transaction():
        if backpressure is not None:
            from taskq.workflows.api._sql_runner import render_sql

            if (
                ledger_id is None
                or ledger_claimed_at is None
                or source_logical_attempt is None
                or type(source_logical_attempt) is not int
                or source_logical_attempt < 1
            ):
                raise ValueError(
                    "emit backpressure requires the validated current source logical "
                    "attempt and ledger id/claim instant"
                )
            # Every numeric fact came from actual admission/the P0 carrier.
            # Validate exact integers again at this persistence seam; no JSON
            # coercion or malformed descriptor can become durable park state.
            numbers = (
                backpressure.page_width,
                backpressure.bound,
                backpressure.outstanding,
            )
            if any(type(value) is not int or value < 0 for value in numbers):
                raise ValueError("emit-backpressure admission facts must be non-negative ints")
            if backpressure.bound < 1:
                raise ValueError("emit-backpressure bound must be positive")
            verdict = await conn.fetchrow(
                render_sql(EMIT_BACKPRESSURE_PARK_SQL, wsql.schema),
                job_id,
                worker_id,
                attempt,
                claim_epoch,
                backpressure.page_width,
                backpressure.bound,
                source_logical_attempt,
                backpressure.outstanding,
                ledger_id,
                ledger_claimed_at,
            )
            if verdict is not None and verdict["verdict"] == "parked":
                # THE PARK'S OWN EVENT (§4.1 rule 4) — in this same
                # transaction: no preceding existence probe (the committed
                # exact park already proves the marker-positive case), and
                # exactly one shared observation INCLUDING the park's own
                # row, because removing the source's live row can make an
                # older waiter fit. The bound record's one-time adoption
                # rides the same transaction.
                from taskq.workflows._emit import emit_capacity_event, ensure_emit_capacity

                await ensure_emit_capacity(
                    conn, wsql, flow_id=flow_id, bound=backpressure.bound
                )
                park_intent_created = await emit_capacity_event(
                    conn, wsql, flow_id=flow_id, probe=False
                )
        else:
            try:
                verdict = await conn.fetchrow(
                    wsql.node_defer,
                    job_id,
                    worker_id,
                    delay,
                    attempt,
                    claim_epoch,
                    disposition,
                    signal_name,
                )
            except Exception as exc:
                import asyncpg

                if disposition == "retry_after_non_consuming" and isinstance(
                    exc, asyncpg.DataError
                ):
                    raise ValueError(
                        "metadata.wf_retry.non_consuming_count and non_consuming_limit "
                        "must be valid signed-int32 integers; the deferral, counter, "
                        "and ledger transition rolled back"
                    ) from exc
                raise
        if verdict is None:
            if ledger_id is not None:
                await conn.execute(
                    wsql.ledger_fence_by_id,
                    ledger_id,
                    signal_name,
                    ledger_claimed_at,
                )
            if iteration_rest is not None:
                await conn.execute(
                    wsql.ledger_fence_by_id,
                    iteration_rest[0],
                    signal_name,
                    iteration_rest[1],
                )
            return _DeferOutcome("fenced")
        if verdict["verdict"] in ("deferred", "parked"):
            if backpressure is not None:
                if int(verdict["ledger_count"]) != 1:
                    raise RuntimeError(
                        "the source park lost its exact current ledger claim; "
                        "the job park and ledger transition rolled back"
                    )
                return _DeferOutcome("deferred", intent_created=park_intent_created)
            if ledger_id is not None:
                # 'awaited' — the hold's own resume-not-retry status. Never
                # 'failed': the ladder's budget read counts failures only, and
                # the deferral never executed. The write keys the EXACT claim
                # identity — the claim's own row id AND its own RETURNING
                # instant (the re-mint guard): a re-minted row is the LIVE
                # winner's, never ours to rest (the caller proved its claim at
                # the node row's fence; the instant's match is the
                # fence-by-id's own). No nullable-instant bypass exists in
                # current production claims.
                await conn.execute(
                    wsql.ledger_terminal_by_id,
                    ledger_id,
                    "awaited",
                    None,
                    None,
                    None,
                    None,
                    ledger_claimed_at,
                )
            if iteration_rest is not None:
                # The deferred ITERATION claim rests 'awaited' too — its own
                # id+instant identity, the same transaction (the epoch
                # arbiter's rows are per-claim; the resumed claim mints its
                # own row, so this one must never read 'running' beside it).
                await conn.execute(
                    wsql.ledger_terminal_by_id,
                    iteration_rest[0],
                    "awaited",
                    None,
                    None,
                    None,
                    None,
                    iteration_rest[1],
                )
            return _DeferOutcome("deferred")
        verdict_text = str(verdict["verdict"])
        if verdict_text == "malformed_counter":
            raise ValueError(
                "metadata.wf_retry.non_consuming_count and non_consuming_limit "
                "must be valid signed-int32 integers; no counter or ledger reset occurred"
            )
        return _DeferOutcome(verdict_text)


async def route_defer_terminal(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    row: dict[str, Any],
    worker_id: JobId,
    attempt: int,
    claim_epoch: int,
    ledger_id: JobId | None,
    ledger_claimed_at: Any = None,
    capture_policy: Any,
    project_auto: Any,
    outcome: str,
    error_class: str,
    error_message: str,
    terminal_effect: TerminalEffect | None = None,
    terminal_reason: TerminalEffectReason = "retry_exhausted",
) -> None:
    """the TERMINAL VERDICTS' SHARED ROUTE (the wf terminal door's own
    mechanics — the ledger's terminal + the door + the projection ride):
    the one identical shape both doors' terminal verdicts run, factored
    once (the DRY of the step's and the loop's routes — the DOORS differ,
    never the mechanics):

    * the engine's one tx1 owns the fenced terminal mark and current-ledger
      terminal together with job_attempts/job_events; no separate pre-terminal
      ledger write leaves atomicity to crash luck;
    * :func:`~taskq.workflows.engine.finalize_node` also owns failed-parent
      propagation and the ordinary outbox work;
    * the auto projection (the node's terminal event, best-effort) follows
      that acknowledged commit.

    The DOORS' SEMANTICS live at the call sites (which verdict routes
    which door with which vocabulary — the step's exhaustion finalizes
    through this door, the loop's through ``_exhaust_loop``'s named
    state); this function is the shape they share, never a decision."""
    from taskq.workflows.engine import finalize_node

    # The engine's tx1 is the sole job + current-ledger + attempt + event
    # commit. A separate pre-terminal ledger write would reopen exactly the
    # crash window the cancellation door is required to close. The ledger's
    # terminal rides the EXACT claim identity (id + instant) — no
    # four-expression fallback, never a cross-write over the parked
    # attempt's own row.
    final = await finalize_node(
        pool,
        wsql,
        flow_id=flow_id,
        job_id=JobId(row["id"]),
        step_key=row["step_key"],
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=claim_epoch,
        outcome=outcome,  # pyright: ignore[reportArgumentType]  # Why: the two verdicts' outcomes are the door's own vocabulary members ('failed' | 'cancelled').
        error_class=error_class,
        error_message=error_message,
        capture_policy=capture_policy,
        map_index=row["map_index"],
        ledger_id=ledger_id,
        ledger_claimed_at=ledger_claimed_at,
        terminal_effect=terminal_effect,
        terminal_reason=terminal_reason,
    )
    if final.applied:
        await project_auto(
            flow_id,
            JobId(row["id"]),
            KIND_NODE_TERMINAL,
            {
                "outcome": outcome,
                "step_key": row["step_key"],
                "error_class": error_class,
            },
        )


async def policy_retry_defer(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    row: dict[str, Any],
    worker_id: JobId,
    attempt: int,
    claim_epoch: int,
    ledger_id: JobId | None,
    ledger_claimed_at: Any,
    exc: Exception,
    delay: timedelta,
    capture_policy: Any,
    project_auto: Any,
    terminal_effect: TerminalEffect | None = None,
) -> str:
    """Apply one bounded workflow-classifier non-consuming advertised retry."""
    from taskq.workflows._policy import RetryDeferralBudgetExceeded

    signal_name = type(exc).__name__
    verdict_outcome = await defer_row_law(
        pool,
        wsql,
        job_id=JobId(row["id"]),
        worker_id=worker_id,
        delay=delay,
        attempt=attempt,
        claim_epoch=claim_epoch,
        disposition="retry_after_non_consuming",
        signal_name=signal_name,
        ledger_id=ledger_id,
        ledger_claimed_at=ledger_claimed_at,
    )
    verdict = verdict_outcome.verdict
    if verdict in ("deferred", "fenced"):
        return verdict
    cancelled = verdict == "cancel_fenced"
    error_class = WORKFLOW_CANCELLED_CLASS if cancelled else RetryDeferralBudgetExceeded.__name__
    error_message = (
        "cancelled while applying an advertised retry policy"
        if cancelled
        else f"{signal_name} reached the workflow non_consuming_limit without consuming max_attempts"
    )
    await route_defer_terminal(
        pool,
        wsql,
        flow_id=flow_id,
        row=row,
        worker_id=worker_id,
        attempt=attempt,
        claim_epoch=claim_epoch,
        ledger_id=ledger_id,
        ledger_claimed_at=ledger_claimed_at,
        capture_policy=capture_policy,
        project_auto=project_auto,
        outcome="cancelled" if cancelled else "failed",
        error_class=error_class,
        error_message=error_message,
        terminal_effect=None if cancelled else terminal_effect,
        terminal_reason="non_consuming_exhausted",
    )
    return "cancelled" if cancelled else "laddered"


class DeferOps(_DeferHost):
    """The runner's deferral mixin (the machinery's home; the runner
    composes it)."""

    __slots__ = ()

    async def _defer_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        ledger_id: JobId | None,
        exc: EmitBackpressure | Snooze | RetryAfter,
        *,
        claim_epoch: int = 0,
        ledger_claimed_at: Any = None,
    ) -> str:
        """The
        shared row law (``defer_row_law`` — the NODE_DEFER_SQL write + the
        ledger's truth, one transaction) applied by the STEP's own routes —
        the two TERMINAL verdicts route through the wf terminal door
        (:meth:`~taskq.workflows.engine.finalize_node` — the ledger's
        terminal, the failed-parent cascade and the propagation ride it), exactly
        like the ladder's own exhaustion arm. (The LOOP body's signals
        route through ``LoopOps._defer_loop_step`` — the same row law, the
        loop's own doors.)

        ``ledger_claimed_at`` is the claim's own RETURNING instant (the
        re-mint guard's discriminator): the zombie's fence keys on it,
        so a claim the arbiter re-minted onto a LIVE winner (the refund's
        own row re-use) can never be fenced by the stale claimant.

        Returns the attempt's outcome label (the caller's log line —
        the ROW is the truth, this is the record's echo):
        ``"deferred"`` / ``"fenced"`` / ``"laddered"`` (the exhausted
        budget) / ``"cancelled"`` (the cancel won).
        """
        source_logical_attempt: int | None = None
        if isinstance(exc, EmitBackpressure):
            from taskq.workflows.api._attempt import resolve_source_logical_attempt

            # Existing birth carriers win over a refunded physical ordinal;
            # genuinely legacy rows materialize the current validated physical
            # claim in the same atomic park rather than inventing attempt zero.
            source_logical_attempt = resolve_source_logical_attempt(row["metadata"], attempt)
        disposition = deferral_disposition(exc)
        outcome = await defer_row_law(
            self.pool,
            self.wsql,
            job_id=JobId(row["id"]),
            worker_id=self._worker_id,
            # The park arm has no timer; the legacy positional signature is
            # retained for ordinary Snooze/RetryAfter callers only.
            delay=timedelta(0) if isinstance(exc, EmitBackpressure) else exc.delay,
            attempt=attempt,
            claim_epoch=claim_epoch,
            disposition=disposition,
            signal_name=type(exc).__name__,
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
            backpressure=exc if isinstance(exc, EmitBackpressure) else None,
            source_logical_attempt=source_logical_attempt,
        )
        verdict_text = outcome.verdict

        if verdict_text == "deferred":
            if isinstance(exc, EmitBackpressure):
                # The park transaction committed (its own §4.1 event ran
                # inside it); the park's terminal route drains the intent it
                # may have created — exactly one acknowledged post-COMMIT
                # promotion pass. A drain error propagates after the
                # committed park remains intact.
                await self._drain_created_intent(flow_id, outcome.intent_created)
                logger.info(
                    "wf.emit_backpressured",
                    run_id=str(flow_id),
                    node=row["step_key"],
                    logical_attempt=source_logical_attempt,
                    physical_attempt=0,
                )
                return "deferred"
            logger.info(
                "node.deferred",
                run_id=str(flow_id),
                node=row["step_key"],
                signal=type(exc).__name__,
                disposition=disposition,
                delay_seconds=exc.delay.total_seconds(),
            )
            return "deferred"
        if verdict_text == "fenced":
            logger.info(
                "node.defer-fenced",
                run_id=str(flow_id),
                node=row["step_key"],
                signal=type(exc).__name__,
            )
            return "fenced"

        # the terminal verdicts — the wf terminal door's own faces (the
        # row law wrote nothing; the door owns every death). The
        # exhaustion's ledger row is the attempt's failed record (the
        # vanilla max_attempts arm's own audit shape); the
        # cancel-fenced node terminalises under the cancel's vocabulary.
        # The door's mechanics are the shared route's (the ledger's
        # terminal + finalize_node + the projection — the one shape both
        # doors' terminal verdicts run); the verdict's vocabulary is the
        # step's own.
        exhausted = verdict_text == "exhausted"
        outcome = "failed" if exhausted else "cancelled"
        error_class = ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED if exhausted else WORKFLOW_CANCELLED_CLASS
        error_message = (
            BUDGET_EXHAUSTED_MESSAGE
            if exhausted
            else "cancelled while deferring (the operator's cancel outranks the deferral)"
        )
        await route_defer_terminal(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            row=row,
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
            capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the compile (the workflow decorator's declaration); the host protocol's compiled is the runner's Any-typed field — the ladder's own call shape.
            project_auto=self._project_auto,
            outcome=outcome,
            error_class=error_class,
            error_message=error_message,
            terminal_effect=self._terminal_effect_for(None, row) if exhausted else None,
        )
        logger.info(
            "node.defer-terminal",
            run_id=str(flow_id),
            node=row["step_key"],
            signal=type(exc).__name__,
            outcome=outcome,
            error_class=error_class,
        )
        # The exhausted budget is the ladder's own face (the budget's
        # terminal); the cancel-fenced node is the cancel's. Both labels
        # name the verdict's owner, never a body failure class.
        return "laddered" if exhausted else "cancelled"

"""THE DEFERRAL DISPOSITION (the body's control-flow signals' wf face —
the split of ``api/_runner`` §7b's concerns-separate law): the machinery
that translates the vanilla surface's typed control-flow signals —
:class:`~taskq.exceptions.Snooze` (reschedule, does NOT consume retry
budget) and :class:`~taskq.exceptions.RetryAfter(delay, consume_budget=)`
(reschedule, consumes by default) — into ONE bounded finalize
disposition on the claimed node row.

THE DEFECT THIS CURES (the OCR-429 exhaustion): the signals are
``TaskQError`` subclasses, so a body raising one fell into the runner's
body-failure catch and LADDERED — the attempt budget burned on a
deferral that never executed, the node parked at the backoff curve and
exhausted. The vanilla consumer translated the very same signals at its
own body seam (``worker/_consumer.py``'s ``_preserved_exc`` catch) into
the row dispositions ``mark_snoozed`` / ``mark_retry_after_consume_{
true,false}`` (``backend/_sql_templates.py``); the workflow runner had
no translation. ONE machinery now: the row law lives in
``_sql_finalize.NODE_DEFER_SQL`` (the vanilla arms mirrored onto the wf
finalize's own fence vocabulary — the claim CAS, the cancel mark, the
budget gate), and this module owns the disposition's RESOLUTION and the
verdicts' routes.

THE NAMED DISPOSITIONS (the extension seam, stated once — the law this
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

THE EXTENSION SEAM: the NEXT deferral DISPOSITION (a new signal, a new
non-terminal row law) adds exactly three things and touches nothing
else —

1. a member on :data:`DeferralDisposition` (the named set, never a bare
   string at a call site);
2. ONE arm in ``NODE_DEFER_SQL`` keyed on the disposition bind (the
   row law's own CASE arms — the vanilla dispositions' shape);
3. ONE arm in :func:`deferral_disposition` (the signal → disposition
   resolution — the catch arm's whole mapping).

A new TERMINAL verdict is NOT that seam: a verdict the ROW LAW refuses
to apply (a death the doors own) needs the three things above for its
disposition PLUS a per-door route in BOTH mixin methods
(:meth:`DeferOps._defer_step` AND
:meth:`~taskq.workflows.api._runner_loop.LoopOps._defer_loop_step`) —
the doors' semantics differ by law (the step's exhaustion finalizes
through the wf terminal door; the loop's through ``_exhaust_loop``'s
named state), so the ROUTE is per-door and the docs do not overclaim
"never a third case" for it. What stays shared either way: the row law
(:func:`defer_row_law`), the ledger's truth, and the terminal doors'
own mechanics (:func:`route_defer_terminal`).

The catch arms (the STEP's in ``api/_runner``, the LOOP's in
``api/_runner_loop``) consume signals and dispatch by the named
disposition; neither grows a per-signal branch. The ROW LAW + THE
LEDGER'S TRUTH are ONE shared phase (:func:`defer_row_law` — the two
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

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

from taskq.backend._protocol import JobId
from taskq.constants import (
    # The exhaustion message's single home; re-exported from this module
    # because the LOOP's deferral route — the exhausted verdict's own
    # body-failure face — reads it beside the loop's own named-state
    # machinery (module-level public under the same name).
    BUDGET_EXHAUSTED_MESSAGE,
    ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED,
)
from taskq.exceptions import RetryAfter, Snooze
from taskq.obs import get_logger
from taskq.workflows._progress import KIND_NODE_TERMINAL

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._sql import WorkflowSql

__all__ = [
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

#: THE NAMED DEFERRAL DISPOSITIONS (the extension seam's member set —
#: see the module docstring for the law).
DeferralDisposition = Literal["snooze", "retry_after_consuming"]


def deferral_disposition(exc: Snooze | RetryAfter) -> DeferralDisposition:
    """THE SIGNAL → DISPOSITION RESOLUTION (the catch arm's whole
    mapping — one function, never a per-signal branch at the call site).

    * ``Snooze`` → ``"snooze"`` (the non-consuming deferral: the refund,
      the floor, the counter);
    * ``RetryAfter`` → the ``consume_budget`` flag's own face —
      ``True`` the consuming retry (the burn, the budget gate),
      ``False`` the vanilla's own snooze semantics.

    THE EXTENSION SEAM'S THIRD LEG (the module docstring): the next
    deferral verdict adds ITS arm here, beside the two above.
    """
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

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None: ...

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
        """THE LOOP'S OWN EXHAUSTION DOOR (the exhausted verdict's route
        for the loop's deferral — the loop's named state + STRANDED-FLOW
        + the escalation ride the loop's own machinery, never a
        re-spelling)."""


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
) -> str:
    """THE ROW LAW + THE LEDGER'S TRUTH, one transaction (the H9 law's own
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
      attempt records 'fenced' on the ledger (H1 — never a running row
      left forever), keyed on the claim's OWN RETURNING instant (the
      re-mint guard, C5 — a re-minted row is the LIVE winner's, never
      ours);
    * ``"exhausted"`` / ``"cancel_fenced"`` — the row law wrote NOTHING
      (the verdict names the caller's own terminal door).

    ``disposition``/``signal_name`` arrive pre-resolved
    (:func:`deferral_disposition` + the signal's class name) — this
    function is the ROW LAW only, never the signal's interpretation.
    """
    async with pool.acquire() as conn, conn.transaction():
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
        if verdict is None:
            if ledger_id is not None:
                await conn.execute(
                    wsql.ledger_fence_by_id,
                    ledger_id,
                    signal_name,
                    ledger_claimed_at,
                )
            return "fenced"
        if verdict["verdict"] == "deferred":
            if ledger_id is not None:
                # THE DEFERRED REST: 'awaited' — the hold's own
                # resume-not-retry status. Never 'failed': the ladder's
                # budget read counts failures only, and the deferral never
                # executed.
                await conn.execute(
                    wsql.ledger_terminal_by_id,
                    ledger_id,
                    "awaited",
                    None,
                    None,
                    None,
                    None,
                )
            return "deferred"
        return str(verdict["verdict"])


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
    capture_policy: Any,
    project_auto: Any,
    outcome: str,
    error_class: str,
    error_message: str,
) -> None:
    """THE TERMINAL VERDICTS' SHARED ROUTE (the wf terminal door's own
    mechanics — the ledger's terminal + the door + the projection ride):
    the ONE identical shape both doors' terminal verdicts run, factored
    once (the DRY of the step's and the loop's routes — the DOORS differ,
    never the mechanics):

    * THE LEDGER'S TERMINAL: the claim's OWN RETURNING id when held (the
      strongest keying — ``_run_tx1``'s own law), else the arbiter tuple
      (the ladder's exhaustion shape);
    * :func:`~taskq.workflows.engine.finalize_node` — the fenced
      terminal mark, the T06 cascade and the propagation ride it;
    * the auto projection (the node's terminal event, best-effort).

    The DOORS' SEMANTICS live at the call sites (which verdict routes
    which door with which vocabulary — the step's exhaustion finalizes
    through this door, the loop's through ``_exhaust_loop``'s named
    state); this function is the shape they share, never a decision."""
    from taskq.workflows.engine import finalize_node

    async with pool.acquire() as conn:
        if ledger_id is not None:
            await conn.execute(
                wsql.ledger_terminal_by_id,
                ledger_id,
                outcome,
                None,
                error_class,
                error_message,
                None,
            )
        else:
            await conn.execute(
                wsql.ledger_terminal,
                flow_id,
                row["step_key"],
                attempt,
                outcome,
                None,
                error_class,
                error_message,
                None,
                row["map_index"],
            )
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
        exc: Snooze | RetryAfter,
        *,
        claim_epoch: int = 0,
        ledger_claimed_at: Any = None,
    ) -> str:
        """THE STEP'S DEFERRAL VERDICT ROUTES (the catch arm's body): the
        shared row law (``defer_row_law`` — the NODE_DEFER_SQL write + the
        ledger's truth, one transaction) applied by the STEP's own routes —
        the two TERMINAL verdicts route through the wf terminal door
        (:meth:`~taskq.workflows.engine.finalize_node` — the ledger's
        terminal, the T06 cascade and the propagation ride it), exactly
        like the ladder's own exhaustion arm. (The LOOP body's signals
        route through ``LoopOps._defer_loop_step`` — the same row law, the
        loop's own doors.)

        ``ledger_claimed_at`` is the claim's OWN RETURNING instant (the
        re-mint guard's discriminator, C5): the zombie's fence keys on it,
        so a claim the arbiter re-minted onto a LIVE winner (the refund's
        own row re-use) can never be fenced by the stale claimant.

        Returns the attempt's outcome label (the caller's log line —
        the ROW is the truth, this is the record's echo):
        ``"deferred"`` / ``"fenced"`` / ``"laddered"`` (the exhausted
        budget) / ``"cancelled"`` (the cancel won).
        """
        disposition = deferral_disposition(exc)
        verdict_text = await defer_row_law(
            self.pool,
            self.wsql,
            job_id=JobId(row["id"]),
            worker_id=self._worker_id,
            delay=exc.delay,
            attempt=attempt,
            claim_epoch=claim_epoch,
            disposition=disposition,
            signal_name=type(exc).__name__,
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
        )

        if verdict_text == "deferred":
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

        # THE TERMINAL VERDICTS — the wf terminal door's own faces (the
        # row law wrote nothing; the door owns every death). The
        # exhaustion's ledger row is the ATTEMPT's failed record (the
        # vanilla max_attempts arm's own audit shape); the
        # cancel-fenced node terminalises under the cancel's vocabulary.
        # The DOOR'S MECHANICS are the shared route's (the ledger's
        # terminal + finalize_node + the projection — the ONE shape both
        # doors' terminal verdicts run); the VERDICT's vocabulary is the
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
            capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the COMPILE (the workflow decorator's declaration); the host protocol's compiled is the runner's Any-typed field — the ladder's own call shape.
            project_auto=self._project_auto,
            outcome=outcome,
            error_class=error_class,
            error_message=error_message,
        )
        logger.info(
            "node.defer-terminal",
            run_id=str(flow_id),
            node=row["step_key"],
            signal=type(exc).__name__,
            outcome=outcome,
            error_class=error_class,
        )
        # The exhausted budget is the LADDER's own face (the budget's
        # terminal); the cancel-fenced node is the cancel's. Both labels
        # name the verdict's owner, never a body failure class.
        return "laddered" if exhausted else "cancelled"

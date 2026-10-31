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

THE EXTENSION SEAM: the NEXT deferral verdict (a new signal, a new
row law) adds exactly three things and touches nothing else —

1. a member on :data:`DeferralDisposition` (the named set, never a bare
   string at a call site);
2. ONE arm in ``NODE_DEFER_SQL`` keyed on the disposition bind (the
   row law's own CASE arms — the vanilla dispositions' shape);
3. ONE arm in :func:`deferral_disposition` (the signal → disposition
   resolution — the catch arm's whole mapping).

The catch arm in ``api/_runner`` consumes signals and dispatches by the
named disposition; it never grows a per-signal branch. The ladder's own
arms (the count, the backoff, the deterministic route) are untouched —
the deferral is the ladder's SIBLING (the budget's other face), never
its special case.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

from taskq.backend._protocol import JobId
from taskq.constants import ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED
from taskq.exceptions import RetryAfter, Snooze
from taskq.obs import get_logger
from taskq.workflows._progress import KIND_NODE_TERMINAL

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "DeferOps",
    "DeferralDisposition",
    "deferral_disposition",
]

logger: Any = get_logger(__name__)

#: The vanilla ``mark_retry_after_consume_true``'s own exhaustion message
#: (``backend/_sql_templates.py``'s max_attempts_failed arm) — one
#: vocabulary with the vanilla surface's record, never a re-spelling.
_BUDGET_EXHAUSTED_MESSAGE: Final[str] = "retry budget exhausted"

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


async def defer_node_row(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    job_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int,
    disposition: DeferralDisposition,
    delay: timedelta,
    signal_name: str,
) -> str | None:
    """THE DISPOSITION'S ROW WRITE (one bounded, keyed statement — the
    statement-level face, split from the verdict routes so the pins can
    drive it directly).

    Applies ``NODE_DEFER_SQL`` — the vanilla deferral arms mirrored onto
    the wf fences (the claim CAS, the cancel mark, the budget gate) —
    and returns the verdict:

    * ``"deferred"`` — the row re-scheduled at the delay (the attempt
      arm's own refund/keep law applied);
    * ``"exhausted"`` — a consuming retry the budget refused (the row
      untouched; the caller routes the terminal);
    * ``"cancel_fenced"`` — an operator cancel in flight won (the row
      untouched; the caller routes the cancel's terminal);
    * ``None`` — the ZOMBIE's deferral: the claim fence matched nothing
      (the row moved to a newer attempt — the winner owns the row).

    The statement is the disposition's whole row law; the caller owns
    the LEDGER's truth (the claim's own row — the deferred rest, the
    fenced record, the terminal faces) in the same transaction as the
    write where the two must be atomic.
    """
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            wsql.node_defer,
            job_id,
            worker_id,
            delay,
            attempt,
            claim_epoch,
            disposition,
            signal_name,
        )
    if rec is None:
        return None
    verdict: str = rec["verdict"]
    return verdict


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
    ) -> str:
        """THE DEFERRAL DISPOSITION'S VERDICT ROUTES (the catch arm's
        body): the row law runs in ONE transaction with the LEDGER's own
        truth for the claim's row — the deferred rest (``awaited``, the
        hold's own resume-not-retry status: the ladder's budget read
        counts 'failed' rows only, so a deferral never burns), the
        zombie's fenced record (H1: never a running row left forever) —
        and the two TERMINAL verdicts route through the wf terminal door
        (:meth:`~taskq.workflows.engine.finalize_node` — the ledger's
        terminal, the T06 cascade and the propagation ride it), exactly
        like the ladder's own exhaustion arm.

        Returns the attempt's outcome label (the caller's log line —
        the ROW is the truth, this is the record's echo):
        ``"deferred"`` / ``"fenced"`` / ``"laddered"`` (the exhausted
        budget) / ``"cancelled"`` (the cancel won).
        """
        from taskq.workflows.engine import finalize_node

        disposition = deferral_disposition(exc)
        # THE ROW LAW + THE LEDGER'S TRUTH, one transaction (the H9 law's
        # own shape — a split write leaves the row 'scheduled' with the
        # ledger 'running': the phantom the fence prevents).
        async with self.pool.acquire() as conn, conn.transaction():
            verdict = await conn.fetchrow(
                self.wsql.node_defer,
                JobId(row["id"]),
                self._worker_id,
                exc.delay,
                attempt,
                claim_epoch,
                disposition,
                type(exc).__name__,
            )
            if verdict is None:
                # THE ZOMBIE'S DEFERRAL (the claim fence matched nothing —
                # a peer re-claimed the row at a newer attempt): the
                # attempt records 'fenced' on the ledger (H1) and this
                # attempt touches nothing else — the winner owns the row.
                if ledger_id is not None:
                    await conn.execute(self.wsql.ledger_fence_by_id, ledger_id, type(exc).__name__)
                logger.info(
                    "node.defer-fenced",
                    run_id=str(flow_id),
                    node=row["step_key"],
                    signal=type(exc).__name__,
                )
                verdict_text = "fenced"
            elif verdict["verdict"] == "deferred":
                if ledger_id is not None:
                    # THE DEFERRED REST: 'awaited' — the hold's own
                    # resume-not-retry status. Never 'failed': the
                    # ladder's budget read counts failures only, and the
                    # deferral never executed.
                    await conn.execute(
                        self.wsql.ledger_terminal_by_id,
                        ledger_id,
                        "awaited",
                        None,
                        None,
                        None,
                        None,
                    )
                verdict_text = "deferred"
            else:
                verdict_text = str(verdict["verdict"])

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
            return "fenced"

        # THE TERMINAL VERDICTS — the wf terminal door's own faces (the
        # row law wrote nothing; the door owns every death). The
        # exhaustion's ledger row is the ATTEMPT's failed record (the
        # vanilla max_attempts arm's own audit shape); the
        # cancel-fenced node terminalises under the cancel's vocabulary.
        exhausted = verdict_text == "exhausted"
        outcome = "failed" if exhausted else "cancelled"
        error_class = ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED if exhausted else WORKFLOW_CANCELLED_CLASS
        error_message = (
            _BUDGET_EXHAUSTED_MESSAGE
            if exhausted
            else "cancelled while deferring (the operator's cancel outranks the deferral)"
        )
        # THE LEDGER'S TERMINAL: the claim's OWN RETURNING id when held
        # (the strongest keying — _run_tx1's own law), else the arbiter
        # tuple (the ladder's exhaustion shape).
        async with self.pool.acquire() as conn:
            if ledger_id is not None:
                await conn.execute(
                    self.wsql.ledger_terminal_by_id,
                    ledger_id,
                    outcome,
                    None,
                    error_class,
                    error_message,
                    None,
                )
            else:
                await conn.execute(
                    self.wsql.ledger_terminal,
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
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=row["step_key"],
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            outcome=outcome,  # pyright: ignore[reportArgumentType]  # Why: the two verdicts' outcomes are the door's own vocabulary members ('failed' | 'cancelled').
            error_class=error_class,
            error_message=error_message,
            capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the COMPILE (the workflow decorator's declaration); the host protocol's compiled is the runner's Any-typed field — the ladder's own call shape.
            map_index=row["map_index"],
        )
        if final.applied:
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {
                    "outcome": outcome,
                    "step_key": row["step_key"],
                    "error_class": error_class,
                },
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

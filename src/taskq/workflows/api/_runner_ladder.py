"""The LADDER + the failure-class routing (the split of ``api/_runner`` —
§7b's concerns-separate law): the retry ladder's count-and-repend shape
and the LADDER-ROUTES-BY-FAILURE-CLASS classifier (the classifier's semantics
decision, stated once).

Module homes: the loop machinery's reclaim arm (the classifier's other
consumer) lives in ``_runner_loop``; the engine's propagation (the
terminal-fail cascade) in ``taskq.workflows.engine``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

import asyncpg
import structlog

from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.retry import compute_backoff
from taskq.workflows._policy import (
    RetrySpec,
    TerminalEffect,
    WorkflowRetryOverride,
    canonical_retry_kind,
)
from taskq.workflows._progress import (
    KIND_NODE_TERMINAL,
    MESSAGE_TERMINAL_MAX,
)
from taskq.workflows.api._attempt import AttemptClaim, repend_attempt
from taskq.workflows.api._sql_runner import (
    NODE_DEADLINE_REPEND_SQL,
    render_sql,
    wf_conn_fetchval,
)
from taskq.workflows.engine import finalize_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

__all__ = ["LadderOps", "is_deterministic_authoring_failure", "is_infra_fault"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: the deadline's record stamp -- the pair below. The wf face records the same
#: pair: the deadline is the machinery's expiry, and the record reads like the
#: vanilla rows'.
DEADLINE_ERROR_CLASS: Final[str] = "TimeoutError"
DEADLINE_ERROR_MESSAGE: Final[str] = "start_to_close"

#: one statement, two consumers — never a drifted pair of inline strings.
_FAILED_COUNT_SQL = (
    "SELECT count(*) FROM {schema}.wf_step_ledger WHERE flow_id = $1 "
    "AND step_key = $2 AND COALESCE(map_index, -1) = COALESCE($3::smallint, -1) "
    "AND status = 'failed'"
)


def is_infra_fault(exc: BaseException) -> bool:
    """the LADDER-ROUTES-BY-FAILURE-CLASS classifier (the classifier's semantics
    decision, stated once) — the escape-point contract sharpened it to the
    ESCAPE-POINT contract: the classifier reads WHERE the error escaped,
    never merely its type. It is consulted only at the MACHINERY
    boundary (a driver/sweep/client statement failed — the loop's own
    state reads, the ledger claims, the advance, the exhaust): a
    RECLAIM-ELIGIBLE transport fault (connection loss, admin shutdown,
    interface failure — the storm's ConnectionDoesNotExist class) routes
    to RECLAIM and never burns the retry ladder; the vanilla lease
    machinery re-claims from the ledger — the ledger row says 'crashed',
    the ladder counts 'failed'.

    The BODY boundary never consults this function: an exception the
    BODY raised is a body failure by WHERE it escaped — a body-raised
    ``ConnectionError`` exhausts the loop as the body failure it is (the
    body cannot forge an infra fault; the STRANDED-FLOW wedge — 20
    crashed rows, a ``running`` flow forever — is the rejected variant,
    kept red by the attack probe)."""
    import asyncpg as _asyncpg

    infra: tuple[type[BaseException], ...] = (
        _asyncpg.exceptions.ConnectionDoesNotExistError,
        _asyncpg.exceptions.InterfaceError,
        _asyncpg.exceptions.AdminShutdownError,
        _asyncpg.exceptions.CannotConnectNowError,
        ConnectionError,
    )
    return isinstance(exc, infra)


def is_deterministic_authoring_failure(exc: BaseException) -> bool:
    """the FAILURE-CLASS ROUTING RULE's deterministic leg (the rule
    stated once, pinned per class):
    a DETERMINISTIC authoring failure never rides the retry ladder. The
    three classes the engine itself defines are deterministic BY
    CONSTRUCTION — each is raised by the engine's own gates for a defect
    that re-running cannot fix:

    * :class:`ProgressRefusedError` — a wrong-shaped emission (an
      authoring bug; the body will send the same shape on every retry);
    * :class:`PageDivergedError` — the emit's page already exists (the
      resume's cursor diverged from the row history; the re-claim
      re-reads the same cursor and collides again);
    * :class:`MapIndexExhaustedError` — the smallint ceiling (the
      record's identity cannot grow past it on any retry);
    * :class:`FanInBoundExceededError` — the fork's fan-in past the
      declared bound (re-running cannot shrink the
      corpus; the refusal NAMES the child_driven escape — the remedy is
      a re-wiring, never a retry);
    * :class:`SpawnDepthExceededError` — the spawn's ancestor walk past
      the declared max depth (re-running cannot shrink
      the ancestor chain — the child line's own bound, the fan-in
      bound's sibling);
    * :class:`ChildFlowLostError` — the parent's resume found a
      DANGLING child pointer (re-running cannot restore
      the purged rows — the respawn refusal is the honest face, the
      silent re-execution it replaced was the lie).

    ROUTING RULE, per class
    ``deterministic = the NAMED terminal`` (finalize immediately, the
    error class on the record, the ladder unburned — a retry budget
    spent on a deterministic death is the record's lie: three identical
    failures pretending to be a flake); ``infra = reclaim`` (the
    machinery boundary's classifier, :func:`is_infra_fault` — the
    ladder untouched); ``transient = the ladder`` (the body's ordinary
    failures — the backoff curve, then the terminal).

    The body boundary consults this before the ladder
    (:meth:`LadderOps._ladder_or_fail`): a body-raised wrong-shape
    emission is deterministic here, never a raw
    laddered failure — the deterministic death must not be
    mislabeled transient and burn the ladder.
    """
    from taskq.exceptions import ReservedTimerNamespaceError
    from taskq.workflows._childflow import ChildFlowLostError, SpawnDepthExceededError
    from taskq.workflows._emit import MapIndexExhaustedError, PageDivergedError
    from taskq.workflows._progress import ProgressRefusedError
    from taskq.workflows.api._runner_errors import VanishedNodeError
    from taskq.workflows.definitions import FanInBoundExceededError

    deterministic: tuple[type[BaseException], ...] = (
        ProgressRefusedError,
        PageDivergedError,
        MapIndexExhaustedError,
        FanInBoundExceededError,
        # E17 — the reserved-timer-prefix mint refusal: the payload
        # model's name is the code's own —
        # re-running the body re-raises the same refusal on every
        # attempt (a retry budget spent on it is the record's lie).
        ReservedTimerNamespaceError,
        SpawnDepthExceededError,
        ChildFlowLostError,
        # a step key the registry does not carry has no body on any attempt —
        # the registry is the truth (D1); the remedy is cancel-then-restart,
        # never a retry budget spent on an unresolvable key.
        VanishedNodeError,
    )
    return isinstance(exc, deterministic)


class _LadderHost(Protocol):
    """The runner surface the ladder reads (the split's typed seam —
    the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    schema: str
    compiled: Any  # the compiled workflow (the declared capture's source)
    _worker_id: JobId

    async def _project_auto(
        self, flow_id: JobId, node_id: JobId, kind: str, payload: dict[str, Any]
    ) -> None: ...


class LadderOps(_LadderHost):
    """The runner's ladder mixin (the machinery's home; the runner
    composes it)."""

    __slots__ = ()

    def _map_child_ladder_terms(self, node_key: str) -> tuple[int, str]:
        """The fork-spawned map child's ladder terms: the
        SOURCE's ``map_max_attempts`` + ``retry_kind`` — the same
        inheritance the progress schema's read carries
        (:meth:`_progress_schema`: the ``.item`` / ``.item:`` derived
        namespaces have no decl of their own). A step key that is no
        map child (an outbox consumer, the escalation step) keeps the
        engine's defaults (3 / transient) — correct for every key the
        fork never spawned."""
        source: Any = None
        if node_key.endswith(".item"):
            source = self.compiled.nodes.get(node_key[: -len(".item")])
        elif ".item:" in node_key:
            source = self.compiled.nodes.get(node_key.split(".item:", 1)[0])
        if source is not None:
            return source.map_max_attempts, source.retry_kind if source.retry_kind else "transient"
        return 3, "transient"

    def _target_retry_spec(self, node: Any, row: dict[str, Any]) -> RetrySpec | None:
        """Read the declared source/effect resolver that planned this exact row."""
        registered = getattr(self, "_terminal_effects", {}).get(row["step_key"])
        if registered is not None:
            retry = registered.effect.policy.retry
            return retry if isinstance(retry, RetrySpec) else None
        if node is not None:
            resolved = getattr(node, "execution_policy", None)
            return resolved.retry if resolved is not None else None
        chain = getattr(self, "_chain_steps", {}).get(row["step_key"])
        if chain is None:
            return None
        return chain.resolved_policy(row["step_key"], resolve_names=False).retry

    def _terminal_effect_for(self, node: Any, row: dict[str, Any]) -> TerminalEffect | None:
        from taskq.workflows._effects import effect_for_source

        return effect_for_source(getattr(self, "_terminal_effects", {}), str(row["step_key"]))

    @staticmethod
    def _row_ladder_terms(
        row: dict[str, Any], node: Any, fallback: tuple[int, str]
    ) -> tuple[int, str]:
        max_attempts = row.get("max_attempts")
        retry_kind = row.get("retry_kind")
        if max_attempts is None:
            max_attempts = fallback[0]
        if retry_kind is None:
            retry_kind = fallback[1]
        return (
            int(max_attempts),
            canonical_retry_kind(str(retry_kind)),
        )

    async def _ladder_or_fail(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        exc: Exception,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: Any = None,
    ) -> Literal["retried", "failed", "cancelled", "fenced", "deferred"]:
        """The ladder: an attempt failure emits no terminal (P3 rule 7) —
        the node re-pends with backoff until ``max_attempts``, then
        terminal-fails (the failed-parent propagation takes over). ``permanent`` retry
        kinds fail immediately (the classifier knob). Returns the
        route the ladder took — ``"retried"`` (the re-pend; the attempt's
        span echoes it on the ``wf.laddered`` event) or ``"failed"`` (the
        terminal landed).

        A
        fork-spawned map child (``<src>.item`` / ``<src>.item:<tag>``)
        has no compiled decl of its own — the ladder reads the SOURCE's
        ``map_max_attempts`` (the knob the operator wired onto the
        map_source, stamped on the child rows by the fork). The
        pre-fix fallback (the bare 3) made the ROW LIE: the record
        carried the operator's budget, the death came at the hardcoded
        ceiling (the team's probe: a plain step wired 7 laddered 7; a
        map item wired 7 died at 3).

        the DETERMINISTIC ROUTE (the failure-class routing rule —
        :func:`is_deterministic_authoring_failure`): a DETERMINISTIC
        authoring failure finalizes immediately with the named class, the
        ladder unburned — whatever the declared ``retry_kind`` (the rule
        outranks the knob: a retry budget spent on a deterministic death
        is the record's lie)."""
        fallback = (
            (node.max_attempts, node.retry_kind or "transient")
            if node is not None
            else self._map_child_ladder_terms(row["step_key"])
        )
        max_attempts, retry_kind = self._row_ladder_terms(row, node, fallback)
        retry_spec = self._target_retry_spec(node, row)
        # the ladder (the count read + the re-pend) is skipped entirely for a
        # deterministic authoring failure — the finalizer below runs on the
        # first attempt.
        deterministic = is_deterministic_authoring_failure(exc)
        # resume-not-retry: the ladder counts the ledger's
        # 'failed' rows — the claim's attempt ordinal increments on every
        # claim (holds' resumes included), so a hold-heavy node keeps its
        # full retry curve (the shared-counter variant — 2 holds +
        # max_attempts=3 = terminal failure with zero retries — is pin
        # 7's red forever). the deterministic route skips the count (the
        # ladder is not consulted).
        failed_count = 0
        if not deterministic:
            failed_count = int(
                await wf_conn_fetchval(
                    self.pool,
                    self.schema,
                    _FAILED_COUNT_SQL,
                    flow_id,
                    row["step_key"],
                    row["map_index"],
                )
            )
        override = (
            retry_spec.classify(exc, failed_count + 1)
            if retry_spec is not None and not deterministic
            else None
        )
        if (
            retry_spec is not None
            and isinstance(override, WorkflowRetryOverride)
            and not override.consume_budget
        ):
            from taskq.workflows.api._runner_defer import policy_retry_defer

            # The classifier only sees this body exception. Deadlines, control
            # flow, deterministic engine authoring faults, cancellation, and
            # infrastructure never enter this arm.
            assert override.delay is not None
            outcome = await policy_retry_defer(
                self.pool,
                self.wsql,
                flow_id=flow_id,
                row=row,
                worker_id=self._worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                ledger_id=ledger_id,
                ledger_claimed_at=ledger_claimed_at,
                exc=exc,
                delay=override.delay,
                capture_policy=self.compiled.capture,
                project_auto=self._project_auto,
                terminal_effect=self._terminal_effect_for(node, row),
            )
            return (
                "deferred"
                if outcome == "deferred"
                else (
                    "cancelled"
                    if outcome == "cancelled"
                    else "fenced"
                    if outcome == "fenced"
                    else "failed"
                )
            )
        if override is not None and override.kind is not None:
            retry_kind = override.kind
        backoff_s = (
            override.delay.total_seconds()
            if override is not None and override.delay is not None
            else (
                compute_backoff(retry_spec.policy, failed_count + 1).total_seconds()
                if retry_spec is not None
                else 0.05 * (2**failed_count)
            )
        )
        # One full-fenced transaction owns both the row transition and the
        # claim-owned ledger result. A cancelled row is not another failed
        # attempt; a stale row cannot report a retry it did not write.
        if not deterministic:
            async with self.pool.acquire() as conn, conn.transaction():
                disposition = await repend_attempt(
                    conn,
                    self.wsql,
                    claim=AttemptClaim(
                        job_id=JobId(row["id"]),
                        flow_id=flow_id,
                        step_key=row["step_key"],
                        worker_id=self._worker_id,
                        attempt=attempt,
                        claim_epoch=claim_epoch,
                        ledger_id=ledger_id,
                        ledger_claimed_at=ledger_claimed_at,
                        map_index=row["map_index"],
                    ),
                    error_class=type(exc).__name__,
                    error_message=str(exc)[:500],
                    failures_before=failed_count,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    backoff_s=backoff_s,
                    # Only the compiled chain_source declaration enters the
                    # logical-source carrier domain. Static/map/chain-step
                    # rows keep their physical claim ordinal unchanged.
                    chain_source=getattr(node, "kind", None) == "chain_source",
                )
            if disposition == "repended":
                return "retried"
            if disposition == "cancel_fenced":
                from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS

                await self._project_auto(
                    flow_id,
                    JobId(row["id"]),
                    KIND_NODE_TERMINAL,
                    {
                        "outcome": "cancelled",
                        "step_key": row["step_key"],
                        "error_class": WORKFLOW_CANCELLED_CLASS,
                    },
                )
                return "cancelled"
            if disposition == "stale":
                return "fenced"
        final = await finalize_node(
            self.pool,
            self.wsql,
            flow_id=flow_id,
            job_id=JobId(row["id"]),
            step_key=row["step_key"],
            worker_id=self._worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            outcome="failed",
            error_class=type(exc).__name__,
            error_message=str(exc)[:MESSAGE_TERMINAL_MAX],
            capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the compile (the workflow decorator's declaration); the host protocol's compiled is the runner's Any-typed field.
            map_index=row["map_index"],
            ledger_id=ledger_id,
            ledger_claimed_at=ledger_claimed_at,
            terminal_effect=(None if deterministic else self._terminal_effect_for(node, row)),
            terminal_reason=(
                "non_retryable" if retry_kind == "non_retryable" else "retry_exhausted"
            ),
        )
        if final.applied:
            await self._project_auto(
                flow_id,
                JobId(row["id"]),
                KIND_NODE_TERMINAL,
                {
                    "outcome": "failed",
                    "step_key": row["step_key"],
                    "error_class": type(exc).__name__,
                },
            )
        return "failed"

    async def _deadline_disposition(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: Any = None,
    ) -> str:
        """the DEADLINE'S DISPOSITION (the wf face of the vanilla
        ``_handle_timeout`` — the start_to_close expiry's own ladder arm,
        the deferral's SIBLING: a START_TO_CLOSE TimeoutError is a REAL
        attempt burn, never a Snooze):

        * the attempt BURNS — the claim's increment stands, the ledger's
          failed row records the deadline (the truthful
          :data:`DEADLINE_ERROR_CLASS`/message pair — the vanilla rows'
          own stamp, never the private marker's name), and the row
          re-pends at the ladder's own backoff curve until
          ``max_attempts`` then terminal-fails (the retry per the
          contract; ``permanent`` fails immediately — the ladder's own
          arms);
        * the CANCEL WINS (the vanilla ``mark_retry``'s own law, mirrored
          in-statement — a phase-carrying row on EITHER carrier matches
          no arm of ``NODE_DEADLINE_REPEND_SQL``): the node terminalises
          'cancelled' through the wf terminal door (the deferral's
          cancel_fenced shape — the cancel's vocabulary, the ledger row
          the cancel's record), never re-queued under the deadline's
          backoff;
        * the ZOMBIE (the claim fence matched nothing — a newer attempt
          owns the row): the ledger records 'fenced' keyed on the claim's
          own instant (the re-mint guard's own shape), — never a
          running row left forever.

        Returns the attempt's outcome label (the caller's log line — the
        ROW is the truth, this is the record's echo): ``"laddered"`` /
        ``"cancelled"`` / ``"fenced"``."""
        from taskq.workflows.api._runner_defer import (
            WORKFLOW_CANCELLED_CLASS,
            route_defer_terminal,
        )

        fallback = (
            (node.max_attempts, node.retry_kind or "transient")
            if node is not None
            else self._map_child_ladder_terms(row["step_key"])
        )
        max_attempts, retry_kind = self._row_ladder_terms(row, node, fallback)
        retry_spec = self._target_retry_spec(node, row)
        # the ladder's own count + curve (the burn prices like any body
        # failure: the ledger's failed rows count, the backoff doubles).
        failed_count = int(
            await wf_conn_fetchval(
                self.pool,
                self.schema,
                _FAILED_COUNT_SQL,
                flow_id,
                row["step_key"],
                row["map_index"],
            )
        )
        # Deadline enforcement stays outside the author classifier. Its policy
        # is still RETRY, with the same failed ordinal and full declared curve.
        backoff_s = (
            compute_backoff(retry_spec.policy, failed_count + 1).total_seconds()
            if retry_spec is not None
            else 0.05 * (2**failed_count)
        )
        exhausted = retry_kind == "non_retryable" or failed_count + 1 >= max_attempts
        async with self.pool.acquire() as conn, conn.transaction():
            rec = await conn.fetchrow(
                render_sql(NODE_DEADLINE_REPEND_SQL, self.schema),
                row["id"],
                backoff_s,
                exhausted,
                # the deadline's disposition flips only the row the caller's
                # own claim holds; a zombie's no-match routes to the zombie
                # face below.
                self._worker_id,
                attempt,
                claim_epoch,
            )
            if rec is None:
                # the claim fence matched nothing — the row moved to a newer
                # attempt, the winner owns it. The ledger's 'fenced' keyed on
                # the claim's own instant (the re-mint guard — a re-minted row
                # is the live winner's, never ours).
                if ledger_id is not None:
                    await conn.execute(
                        self.wsql.ledger_fence_by_id,
                        ledger_id,
                        DEADLINE_ERROR_CLASS,
                        ledger_claimed_at,
                    )
                verdict = "fenced"
            else:
                verdict = str(rec["verdict"])
                if ledger_id is not None:
                    if verdict == "cancel_fenced":
                        # the ledger row the cancel's own vocabulary (the
                        # cancel_fenced face), keyed on the claim's own instant
                        # (the re-mint guard — a re-minted row is the live
                        # winner's).
                        await conn.execute(
                            self.wsql.ledger_terminal_by_id,
                            ledger_id,
                            "cancelled",
                            None,
                            WORKFLOW_CANCELLED_CLASS,
                            "cancelled while past its start_to_close "
                            "(the operator's cancel outranks the deadline)",
                            None,
                            ledger_claimed_at,
                        )
                    else:
                        # the attempt's failed row, the deadline's truthful
                        # stamp — on both live faces (the repended retry and
                        # the exhausted terminal: the attempt executed and the
                        # deadline ended it) — keyed on the claim's own instant
                        # (the re-mint guard: the failed stamp cannot land on a
                        # re-minted live winner's ledger row).
                        await conn.execute(
                            self.wsql.ledger_terminal_by_id,
                            ledger_id,
                            "failed",
                            None,
                            DEADLINE_ERROR_CLASS,
                            DEADLINE_ERROR_MESSAGE,
                            None,
                            ledger_claimed_at,
                        )
        if verdict == "fenced":
            logger.info(
                "node.deadline-fenced",
                run_id=str(flow_id),
                node=row["step_key"],
                error_class=DEADLINE_ERROR_CLASS,
            )
            return "fenced"
        if verdict == "cancel_fenced":
            # the node terminalises through the wf terminal door (the own route
            # — the ledger's terminal + finalize_node + the projection ride
            # it).
            await route_defer_terminal(
                self.pool,
                self.wsql,
                flow_id=flow_id,
                row=row,
                worker_id=self._worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                ledger_id=ledger_id,
                capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the compile (the ladder's own call shape — the host's compiled is Any-typed).
                project_auto=self._project_auto,
                outcome="cancelled",
                error_class=WORKFLOW_CANCELLED_CLASS,
                error_message=(
                    "cancelled while past its start_to_close "
                    "(the operator's cancel outranks the deadline)"
                ),
            )
            logger.info(
                "node.deadline-cancelled",
                run_id=str(flow_id),
                node=row["step_key"],
                error_class=WORKFLOW_CANCELLED_CLASS,
            )
            return "cancelled"
        # the row re-pended at the backoff (the burn's retry). The budget's
        # bound: past max_attempts — or a ``permanent`` kind — the deadline
        # terminal-fails the node (the ladder's own exhaustion face, the
        # deadline's stamp on the record).
        if exhausted:
            final = await finalize_node(
                self.pool,
                self.wsql,
                flow_id=flow_id,
                job_id=JobId(row["id"]),
                step_key=row["step_key"],
                worker_id=self._worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                outcome="failed",
                error_class=DEADLINE_ERROR_CLASS,
                error_message=DEADLINE_ERROR_MESSAGE,
                capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]
                map_index=row["map_index"],
                ledger_id=ledger_id,
                ledger_claimed_at=ledger_claimed_at,
                terminal_effect=self._terminal_effect_for(node, row),
                terminal_reason="deadline",
            )
            if final.applied:
                await self._project_auto(
                    flow_id,
                    JobId(row["id"]),
                    KIND_NODE_TERMINAL,
                    {
                        "outcome": "failed",
                        "step_key": row["step_key"],
                        "error_class": DEADLINE_ERROR_CLASS,
                    },
                )
        logger.info(
            "node.deadline-exceeded",
            run_id=str(flow_id),
            node=row["step_key"],
            attempt=attempt,
            error_class=DEADLINE_ERROR_CLASS,
            exhausted=exhausted,
        )
        return "laddered"

    async def _reclaim_step(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        exc: Exception,
        *,
        claim_epoch: int = 0,
        ledger_id: JobId | None = None,
        ledger_claimed_at: Any = None,
    ) -> None:
        """the STEP NODE'S MACHINERY RECLAIM (the claim tail's
        escape-point contract — the machinery boundary, the classifier's law applied
        SYMMETRICALLY): an INFRA fault escaping the claim tail's own
        statements (:meth:`_finalize_success` on EITHER face — the hit's
        and the miss's finalize share this one seat — plus the receipt
        write and the cache store) is the RECLAIM class:

        * the ledger records the MACHINERY's truth — ``crashed``, never
          ``failed`` (the ladder is UNTOUCHED, no failure record lands,
          the retry budget is never spent on a transport blip);
        * the row re-pends (the vanilla re-claim's own statement —
          ``NODE_REPEND_SQL_TEMPLATE``, the loop driver's
          ``_reclaim_loop`` shape) — the node stands RECLAIM-ELIGIBLE;
        * the ESCAPE ITSELF IS PRESERVED — the caller re-raises: the
          fault surfaces to the driver (the worker loop counts it, the
          F2-2 arm), and the cleared-fault RE-DRIVE completes the step —
          via the HIT when the step is cached (the body still never
          runs; the cache's whole point).

        One fault, one semantics: the identical fault at either
        finalize face takes the identical seat — never a second patched
        boundary."""
        async with self.pool.acquire() as conn, conn.transaction():
            await repend_attempt(
                conn,
                self.wsql,
                claim=AttemptClaim(
                    job_id=JobId(row["id"]),
                    flow_id=flow_id,
                    step_key=row["step_key"],
                    worker_id=self._worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                    ledger_id=ledger_id,
                    ledger_claimed_at=ledger_claimed_at,
                    map_index=row["map_index"],
                ),
                error_class=type(exc).__name__,
                error_message=str(exc)[:500],
                failures_before=0,
                max_attempts=1,
                retry_kind="transient",
                backoff_s=0.05,
                machinery_crashed=True,
                # Reclaim can persist a legacy source's current ordinal but
                # never turns infrastructure into another authored failure.
                chain_source=(
                    getattr(self.compiled.nodes.get(row["step_key"]), "kind", None)
                    == "chain_source"
                ),
            )

"""The LADDER + the failure-class routing (the split of ``api/_runner`` —
§7b's concerns-separate law): the retry ladder's count-and-repend shape
and THE LADDER-ROUTES-BY-FAILURE-CLASS classifier (T19's semantics
decision, stated once).

Module homes: the loop machinery's reclaim arm (the classifier's other
consumer) lives in ``_runner_loop``; the engine's propagation (the
terminal-fail cascade) in ``taskq.workflows.engine``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import asyncpg
import structlog

from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._progress import (
    KIND_NODE_TERMINAL,
    MESSAGE_TERMINAL_MAX,
)
from taskq.workflows.api._sql_runner import (
    NODE_REPEND_SQL_TEMPLATE,
    render_sql,
    wf_conn_fetchval,
)
from taskq.workflows.engine import finalize_node

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

__all__ = ["LadderOps", "is_deterministic_authoring_failure"]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


def is_infra_fault(exc: BaseException) -> bool:
    """THE LADDER-ROUTES-BY-FAILURE-CLASS classifier (T19's semantics
    decision, stated once) — attack-3 H5's cure sharpened it to the
    ESCAPE-POINT contract: the classifier reads WHERE the error escaped,
    never merely its type. It is consulted ONLY at the MACHINERY
    boundary (a driver/sweep/client statement failed — the loop's own
    state reads, the ledger claims, the advance, the exhaust): a
    RECLAIM-ELIGIBLE transport fault (connection loss, admin shutdown,
    interface failure — the storm's ConnectionDoesNotExist class) routes
    to RECLAIM and NEVER burns the retry ladder; the vanilla lease
    machinery re-claims from the ledger — the ledger row says 'crashed',
    the ladder counts 'failed'.

    The BODY boundary NEVER consults this function: an exception the
    BODY raised is a body failure by WHERE it escaped — a body-raised
    ``ConnectionError`` exhausts the loop as the body failure it is (the
    body cannot forge an infra fault; the STRANDED-FLOW wedge — 20
    crashed rows, a ``running`` flow forever — is the convicted variant,
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
    """THE FAILURE-CLASS ROUTING RULE's deterministic leg (the T20/T21
    fixer's cure — the rule stated once, pinned per class):
    a DETERMINISTIC authoring failure never rides the retry ladder. The
    three classes the engine itself defines are deterministic BY
    CONSTRUCTION — each is raised by the engine's own gates for a defect
    that re-running cannot cure:

    * :class:`ProgressRefusedError` — a wrong-shaped emission (an
      authoring bug; the body will send the same shape on every retry);
    * :class:`PageDivergedError` — the emit's page already exists (the
      resume's cursor diverged from the row history; the re-claim
      re-reads the SAME cursor and collides again);
    * :class:`MapIndexExhaustedError` — the smallint ceiling (the
      record's identity cannot grow past it on any retry);
    * :class:`FanInBoundExceededError` — the fork's fan-in past the
      declared bound (the rv4 F8 cure: re-running cannot shrink the
      corpus; the refusal NAMES the child_driven escape — the remedy is
      a re-wiring, never a retry).

    THE ROUTING RULE, per class (the pin drills all three legs):
    ``deterministic = the NAMED terminal`` (finalize immediately, the
    error class on the record, the ladder unburned — a retry budget
    spent on a deterministic death is the record's lie: three identical
    failures pretending to be a flake); ``infra = reclaim`` (the
    machinery boundary's classifier, :func:`is_infra_fault` — the
    ladder untouched); ``transient = the ladder`` (the body's ordinary
    failures — the backoff curve, then the terminal).

    THE BODY BOUNDARY consults this BEFORE the ladder
    (:meth:`LadderOps._ladder_or_fail`): a body-raised wrong-shape
    emission used to burn the whole ladder and terminalize as a raw
    laddered failure — the deterministic death, mislabeled transient.
    """
    from taskq.workflows._emit import MapIndexExhaustedError, PageDivergedError
    from taskq.workflows._progress import ProgressRefusedError
    from taskq.workflows.definitions import FanInBoundExceededError

    deterministic: tuple[type[BaseException], ...] = (
        ProgressRefusedError,
        PageDivergedError,
        MapIndexExhaustedError,
        FanInBoundExceededError,
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

    async def _ladder_or_fail(
        self,
        flow_id: JobId,
        row: dict[str, Any],
        attempt: int,
        node: Any,
        exc: Exception,
        *,
        claim_epoch: int = 0,
    ) -> None:
        """The ladder: an attempt failure emits NO terminal (P3 rule 7) —
        the node re-pends with backoff until ``max_attempts``, THEN
        terminal-fails (T06's propagation takes over). ``permanent`` retry
        kinds fail immediately (cut #12's classifier knob).

        THE DETERMINISTIC ROUTE (the failure-class routing rule —
        :func:`is_deterministic_authoring_failure`): a DETERMINISTIC
        authoring failure finalizes immediately with the named class, the
        ladder unburned — whatever the declared ``retry_kind`` (the rule
        outranks the knob: a retry budget spent on a deterministic death
        is the record's lie)."""
        max_attempts = node.max_attempts if node is not None else 3
        retry_kind = node.retry_kind if node is not None else "transient"
        # THE DETERMINISTIC ROUTE: the ladder (the count read + the
        # re-pend) is skipped entirely for a deterministic authoring
        # failure — the finalizer below runs on the first attempt.
        deterministic = is_deterministic_authoring_failure(exc)
        # RESUME-NOT-RETRY (cut #5): the LADDER counts the ledger's
        # 'failed' rows — the claim's attempt ordinal increments on EVERY
        # claim (holds' resumes included), so a hold-heavy node keeps its
        # full retry curve (the shared-counter variant — 2 holds +
        # max_attempts=3 = terminal failure with ZERO retries — is pin
        # 7's RED forever). THE DETERMINISTIC ROUTE skips the count (the
        # ladder is not consulted).
        failed_count = 0
        if not deterministic:
            failed_count = int(
                await wf_conn_fetchval(
                    self.pool,
                    self.schema,
                    "SELECT count(*) FROM {schema}.wf_step_ledger WHERE flow_id = $1 "
                    "AND step_key = $2 AND COALESCE(map_index, -1) = COALESCE($3::smallint, -1) "
                    "AND status = 'failed'",
                    flow_id,
                    row["step_key"],
                    row["map_index"],
                )
            )
        # The attempt's OWN ledger row terminalizes 'failed' (the claim
        # inserted it — the arbiter's key is the attempt's identity): an
        # INSERT here would collide with the claim (the UniqueViolation
        # the runner pins convict). Ladder retries emit NO terminal on
        # the JOB row (P3 rule 7) — the node re-pends with backoff.
        async with self.pool.acquire() as conn:
            await conn.execute(
                self.wsql.ledger_terminal,
                flow_id,
                row["step_key"],
                attempt,
                "failed",
                None,
                type(exc).__name__,
                str(exc)[:500],
                None,
                row["map_index"],
            )
            if not deterministic and retry_kind != "permanent" and failed_count + 1 < max_attempts:
                await conn.execute(
                    render_sql(NODE_REPEND_SQL_TEMPLATE, self.schema),
                    row["id"],
                    0.05 * (2 ** (failed_count)),
                )
                return
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
            capture_policy=self.compiled.capture,  # pyright: ignore[reportAttributeAccessIssue]  # Why: the declared capture policy rides the COMPILE (the workflow decorator's declaration); the host protocol's compiled is the runner's Any-typed field.
            map_index=row["map_index"],
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

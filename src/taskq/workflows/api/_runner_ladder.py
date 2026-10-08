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

__all__ = ["LadderOps"]

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


class _LadderHost(Protocol):
    """The runner surface the ladder reads (the split's typed seam —
    the fields live on the runner, the machinery here)."""

    pool: asyncpg.Pool
    wsql: WorkflowSql
    schema: str
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
    ) -> None:
        """The ladder: an attempt failure emits NO terminal (P3 rule 7) —
        the node re-pends with backoff until ``max_attempts``, THEN
        terminal-fails (T06's propagation takes over). ``permanent`` retry
        kinds fail immediately (cut #12's classifier knob)."""
        max_attempts = node.max_attempts if node is not None else 3
        retry_kind = node.retry_kind if node is not None else "transient"
        # RESUME-NOT-RETRY (cut #5): the LADDER counts the ledger's
        # 'failed' rows — the claim's attempt ordinal increments on EVERY
        # claim (holds' resumes included), so a hold-heavy node keeps its
        # full retry curve (the shared-counter variant — 2 holds +
        # max_attempts=3 = terminal failure with ZERO retries — is pin
        # 7's RED forever).
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
            if retry_kind != "permanent" and failed_count + 1 < max_attempts:
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
            claim_epoch=0,
            outcome="failed",
            error_class=type(exc).__name__,
            error_message=str(exc)[:MESSAGE_TERMINAL_MAX],
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

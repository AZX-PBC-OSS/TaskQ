"""Claim-owned workflow attempt disposition.

The ordinary body ladder, machinery reclaim, and loop reclaim must not
independently invent re-pend fences.  This module owns the shared primitive:
one Postgres transaction writes the row disposition and the current ledger
claim's outcome.  Cancellation outranks exhaustion; a stale or terminal
claim has no authority to change a newer owner's row or memoized result.

The caller still owns the terminal transaction after ``exhausted``.  The
primitive deliberately leaves that row running until the engine's existing
terminal/fork/outbox transaction can commit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal, cast

import structlog

from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS
from taskq.workflows.api._sql_runner import render_sql

__all__ = [
    "WF_REPEND_HISTORY_LIMIT",
    "AttemptClaim",
    "AttemptDisposition",
    "repend_attempt",
    "resolve_source_logical_attempt",
]

logger: structlog.stdlib.BoundLogger = structlog.getLogger(__name__)


AttemptDisposition = Literal["repended", "exhausted", "cancel_fenced", "stale"]
WF_SOURCE_LOGICAL_ATTEMPT: Final[str] = "wf_source_logical_attempt"
# Eight retained physical disposals cover the ordinary default retry curve plus
# a recent infrastructure window. Older detail remains in wf_step_ledger; only
# the duplicate row-local diagnostic summary is folded.
WF_REPEND_HISTORY_LIMIT: Final[int] = 8
_SOURCE_ATTEMPT_MAX: Final[int] = 2_147_483_647


def _metadata_object(raw: object) -> dict[str, object]:
    decoded: object = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise ValueError("workflow source metadata must be a JSON object")
    return cast("dict[str, object]", decoded)


def resolve_source_logical_attempt(metadata: object, physical_attempt: int) -> int:
    """Resolve a source's public ordinal from its durable row carrier.

    A missing key is the one legacy-row fallback: its physical claim is the
    current logical ordinal, and the next disposition persists that carrier.
    Existing values are never inferred, coerced, or replaced from counters.
    """
    if type(physical_attempt) is not int or not 1 <= physical_attempt <= _SOURCE_ATTEMPT_MAX:
        raise ValueError("workflow source physical claim attempt must be a positive integer")
    document = _metadata_object(metadata)
    if WF_SOURCE_LOGICAL_ATTEMPT not in document:
        return physical_attempt
    value = document[WF_SOURCE_LOGICAL_ATTEMPT]
    if type(value) is not int or not 1 <= value <= _SOURCE_ATTEMPT_MAX:
        raise ValueError(
            "metadata.wf_source_logical_attempt must be a positive JSON integer; "
            f"got {type(value).__name__} {value!r}"
        )
    return value


@dataclass(frozen=True, slots=True)
class AttemptClaim:
    """The exact job and ledger ownership acquired by one execution attempt."""

    job_id: JobId
    flow_id: JobId
    step_key: str
    worker_id: JobId
    attempt: int
    claim_epoch: int
    ledger_id: JobId | None
    ledger_claimed_at: datetime | None
    map_index: int | None


# One statement owns ALL row verdicts. A caller does not infer cancellation
# or exhaustion from a failed UPDATE. source_next_attempt is NULL for ordinary
# rows; for a source it is the validated current carrier plus exactly one body
# failure (or the unchanged carrier for machinery reclaim).
NODE_REPEND_SQL_TEMPLATE = """\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::double precision AS backoff_s,
           $3::int AS attempt,
           $4::uuid AS worker_id,
           $5::bigint AS claim_epoch,
           $6::boolean AS exhausted,
           $7::uuid AS ledger_id,
           $8::timestamptz AS ledger_claimed_at,
           $9::int AS source_next_attempt
),
claim_current AS (
    SELECT 1
    FROM {schema}.wf_step_ledger AS l
    WHERE l.id = (SELECT ledger_id FROM params)
      AND l.updated_at = (SELECT ledger_claimed_at FROM params)
      AND l.status IN ('running', 'awaited', 'crashed')
),
repend_merged AS (
    SELECT j.id AS job_id, entry.key, entry.value,
           COALESCE(NULLIF(j.metadata->>'wf_repend_folded', '')::bigint, 0) AS prior_folded
    FROM {schema}.jobs AS j CROSS JOIN params
    CROSS JOIN LATERAL jsonb_each(
        COALESCE(j.metadata->'wf_repend', '{}'::jsonb) || jsonb_build_object(
            (SELECT attempt::text FROM params),
            jsonb_build_object(
                'backoff', (SELECT backoff_s FROM params),
                'at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                'due', to_char((now() + ((SELECT backoff_s FROM params) * interval '1 second'))
                               AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
            )
        )
    ) AS entry
    WHERE j.id = (SELECT job_id FROM params)
),
repend_ranked AS (
    SELECT m.*,
           row_number() OVER (
               PARTITION BY job_id
               ORDER BY value->>'at' DESC NULLS LAST, key::int DESC
           ) AS recent_rank
    FROM repend_merged AS m
),
repend_history AS (
    SELECT job_id AS id,
           jsonb_object_agg(key, value) FILTER (
               WHERE recent_rank <= __WF_REPEND_HISTORY_LIMIT__
           ) AS history,
           max(prior_folded) + count(*)
               - count(*) FILTER (WHERE recent_rank <= __WF_REPEND_HISTORY_LIMIT__)
               AS folded_count
    FROM repend_ranked
    GROUP BY job_id
),
repended AS (
    UPDATE {schema}.jobs AS j
    SET status = 'scheduled',
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        scheduled_at = now() + ((SELECT backoff_s FROM params) * interval '1 second'),
        metadata = j.metadata
            || jsonb_build_object('wf_repend', h.history)
            || CASE WHEN h.folded_count > 0 OR j.metadata ? 'wf_repend_folded'
                    THEN jsonb_build_object('wf_repend_folded', h.folded_count)
                    ELSE '{}'::jsonb END
            || CASE WHEN params.source_next_attempt IS NULL THEN '{}'::jsonb
                    ELSE jsonb_build_object(
                        'wf_source_logical_attempt', params.source_next_attempt
                    ) END
    FROM params CROSS JOIN repend_history AS h
    WHERE j.id = params.job_id
      AND h.id = j.id
      AND j.status = 'running'
      AND j.locked_by_worker = params.worker_id
      AND j.attempt = params.attempt
      AND j.claim_epoch = params.claim_epoch
      AND EXISTS (SELECT 1 FROM claim_current)
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
      AND NOT params.exhausted
    RETURNING j.id
),
cancelled AS (
    -- This arm only selects the verdict. The shared engine terminal tx below
    -- owns the node terminal, current attempt and state-change event. No
    -- caller-only SQL status change can lose those records.
    SELECT j.id
    FROM {schema}.jobs AS j CROSS JOIN params
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = params.worker_id
      AND j.attempt = params.attempt
      AND j.claim_epoch = params.claim_epoch
      AND EXISTS (SELECT 1 FROM claim_current)
      AND (j.metadata ? 'cancel_phase' OR j.cancel_phase <> 0)
      AND NOT EXISTS (SELECT 1 FROM repended)
),
exhausted_source AS (
    -- Exhaustion is a real authored failure too. Persist its one ordinal
    -- advance now; the existing terminal transaction remains authoritative
    -- for the job/attempt/event/ledger terminal records. A subsequent
    -- finalize-infra reclaim reads this carrier and cannot advance it again.
    UPDATE {schema}.jobs AS j
    SET metadata = j.metadata || jsonb_build_object(
        'wf_source_logical_attempt', params.source_next_attempt
    )
    FROM params
    WHERE j.id = params.job_id
      AND j.status = 'running'
      AND j.locked_by_worker = params.worker_id
      AND j.attempt = params.attempt
      AND j.claim_epoch = params.claim_epoch
      AND EXISTS (SELECT 1 FROM claim_current)
      AND params.exhausted
      AND params.source_next_attempt IS NOT NULL
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
    RETURNING j.id
)
SELECT 'repended'::text AS verdict, id FROM repended
UNION ALL
SELECT 'cancel_fenced'::text AS verdict, id FROM cancelled
UNION ALL
SELECT 'exhausted'::text AS verdict, (SELECT id FROM exhausted_source) AS id
WHERE (SELECT exhausted FROM params)
  AND EXISTS (
      SELECT 1 FROM {schema}.jobs AS j
      WHERE j.id = (SELECT job_id FROM params)
        AND j.status = 'running'
        AND j.locked_by_worker = (SELECT worker_id FROM params)
        AND j.attempt = (SELECT attempt FROM params)
        AND j.claim_epoch = (SELECT claim_epoch FROM params)
        AND EXISTS (SELECT 1 FROM claim_current)
        AND j.cancel_phase = 0
        AND NOT j.metadata ? 'cancel_phase'
  )
  AND (
      (SELECT source_next_attempt FROM params) IS NULL
      OR EXISTS (SELECT 1 FROM exhausted_source)
  )
  AND NOT EXISTS (SELECT 1 FROM repended)
  AND NOT EXISTS (SELECT 1 FROM cancelled)
"""


_SOURCE_ROW_SQL = """\
SELECT metadata, cancel_phase
FROM {schema}.jobs
WHERE id = $1
  AND status = 'running'
  AND locked_by_worker = $2
  AND attempt = $3
  AND claim_epoch = $4
"""


def _canonical_kind(kind: str) -> str:
    """The persistent vocabulary; authoring's permanent alias is one helper,
    never a set of divergent comparisons throughout the runner."""
    return "non_retryable" if kind == "permanent" else kind


async def _read_source_next_attempt(
    conn: ConnLike,
    wsql: WorkflowSql,
    claim: AttemptClaim,
    *,
    machinery_crashed: bool,
) -> int | None:
    """Read and validate the carrier under this exact physical claim.

    Cancellation remains the higher-priority disposition and does not depend
    on retry metadata being intelligible. Every active retry/exhaustion write
    validates the current carrier before any row or ledger mutation.
    """
    owned = await conn.fetchrow(
        render_sql(_SOURCE_ROW_SQL, wsql.schema),
        claim.job_id,
        claim.worker_id,
        claim.attempt,
        claim.claim_epoch,
    )
    if owned is None:
        return None
    metadata = _metadata_object(owned["metadata"])
    if owned["cancel_phase"] != 0 or "cancel_phase" in metadata:
        return None
    current = resolve_source_logical_attempt(metadata, claim.attempt)
    if machinery_crashed:
        return current
    if current >= _SOURCE_ATTEMPT_MAX:
        raise ValueError("metadata.wf_source_logical_attempt cannot advance past the integer bound")
    return current + 1


async def repend_attempt(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    claim: AttemptClaim,
    error_class: str,
    error_message: str,
    failures_before: int,
    max_attempts: int,
    retry_kind: str,
    backoff_s: float,
    machinery_crashed: bool = False,
    chain_source: bool = False,
) -> AttemptDisposition:
    """Write the current claim's retry/cancel/exhaustion disposition.

    ``failures_before`` and ``max_attempts`` are evaluated in the caller from
    the durable ledger/policy.  ``machinery_crashed`` explicitly preserves
    the non-consuming reclaim route.  A machinery error never becomes a
    body-failure attempt merely because this shared primitive is used.

    The supplied connection owns the transaction.  This function does not
    acquire another pool connection.  Its row and ledger writes commit or
    roll back together, including the terminal cancellation route.
    """
    if claim.ledger_id is None or claim.ledger_claimed_at is None:
        raise RuntimeError(
            "workflow disposition requires the claim-owned ledger id and claim instant"
        )
    if failures_before < 0 or max_attempts < 1 or backoff_s < 0:
        raise ValueError("invalid workflow retry budget or backoff")
    exhausted = (not machinery_crashed) and (
        _canonical_kind(retry_kind) == "non_retryable" or failures_before + 1 >= max_attempts
    )
    source_next_attempt = (
        await _read_source_next_attempt(conn, wsql, claim, machinery_crashed=machinery_crashed)
        if chain_source
        else None
    )
    statement = render_sql(NODE_REPEND_SQL_TEMPLATE, wsql.schema).replace(
        "__WF_REPEND_HISTORY_LIMIT__", str(WF_REPEND_HISTORY_LIMIT)
    )
    record = await conn.fetchrow(
        statement,
        claim.job_id,
        backoff_s,
        claim.attempt,
        claim.worker_id,
        claim.claim_epoch,
        exhausted,
        claim.ledger_id,
        claim.ledger_claimed_at,
        source_next_attempt,
    )
    verdict = "stale" if record is None else cast("AttemptDisposition", record["verdict"])
    if verdict == "exhausted":
        # Do not separate the exhaustion ledger from the terminal tx1.  The
        # caller's existing engine finalizer owns the claim-owned terminal.
        return verdict
    if verdict == "cancel_fenced":
        # Reuse the same engine terminal tx as every other cancellation:
        # terminal + attempt + state-change event + ledger, with no fork.
        from taskq.workflows.engine import (
            _run_tx1,  # pyright: ignore[reportPrivateUsage]  # Why: this is the terminal kernel's same-connection, claim-owned use, not a second terminal implementation.
        )

        applied = await _run_tx1(
            conn,
            wsql,
            flow_id=claim.flow_id,
            job_id=claim.job_id,
            worker_id=claim.worker_id,
            attempt=claim.attempt,
            claim_epoch=claim.claim_epoch,
            outcome="cancelled",
            result=None,
            error_class=WORKFLOW_CANCELLED_CLASS,
            error_message="workflow run was cancelled",
            error_traceback=None,
            fork=None,
            capture=None,
            step_key=claim.step_key,
            map_index=claim.map_index,
            ledger_id=claim.ledger_id,
            ledger_claimed_at=claim.ledger_claimed_at,
        )
        if applied.applied:
            return verdict
        verdict = "stale"
    if verdict == "stale":
        fenced = await conn.fetchval(
            wsql.ledger_fence_by_id,
            claim.ledger_id,
            "StaleWorkflowClaim",
            claim.ledger_claimed_at,
        )
        if fenced is None:
            observed = await conn.fetchrow(
                render_sql(
                    "SELECT status::text AS status, updated_at "
                    "FROM {schema}.wf_step_ledger WHERE id = $1",
                    wsql.schema,
                ),
                claim.ledger_id,
            )
            if observed is not None and (
                observed["status"] in ("succeeded", "failed", "cancelled", "fenced", "crashed")
                or observed["updated_at"] != claim.ledger_claimed_at
            ):
                logger.info(
                    "workflow-attempt-stale-noop",
                    flow_id=str(claim.flow_id),
                    job_id=str(claim.job_id),
                    step_key=claim.step_key,
                    attempt=claim.attempt,
                    claim_epoch=claim.claim_epoch,
                    outcome="stale",
                )
                return verdict
            if observed is None:
                # Retention physically deletes aged terminal-flow/rootless
                # ledger rows (STEP_LEDGER_*_RETENTION_SQL). The row-claim
                # fence already refused this stale caller; record the loss
                # observably instead of resurrecting a drive-killing raise.
                logger.info(
                    "workflow-attempt-stale-missing-ledger",
                    flow_id=str(claim.flow_id),
                    job_id=str(claim.job_id),
                    step_key=claim.step_key,
                    attempt=claim.attempt,
                    claim_epoch=claim.claim_epoch,
                    outcome="stale",
                )
                return verdict
            raise RuntimeError("a live current ledger claim could not complete its fence")
        return verdict
    terminal = "crashed" if machinery_crashed else "failed"

    written = await conn.fetchval(
        wsql.ledger_terminal_by_id,
        claim.ledger_id,
        terminal,
        None,
        error_class,
        error_message[:500],
        None,
        claim.ledger_claimed_at,
    )
    if written is None:
        raise RuntimeError("workflow disposition lost its own ledger claim")
    return verdict

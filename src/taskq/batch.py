"""Batch enqueue primitives for TaskQ.

Provides:
- :class:`EnqueueItem` — one item in a :meth:`~taskq.client.JobsClient.enqueue_batch` call.
- :class:`BatchCompletionStatus` — aggregated counts across all jobs in a batch.
- :class:`BatchHandle` — returned by :meth:`~taskq.client.JobsClient.enqueue_batch`;
  holds all :class:`~taskq.client.JobHandle` instances and exposes a
  :meth:`BatchHandle.status` query.
- :func:`wait_for_batch` — convenience helper for the fan-out-then-finalize
  pattern.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, assert_never
from uuid import UUID

import structlog
from pydantic import BaseModel, Field, computed_field

from taskq._json import dumps_str
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    AttemptOutcome,
    Backend,
    BatchRow,
    IdempotencyKey,
    IdentityKey,
    JobRow,
)
from taskq.backend._records import jsonb_to_dict
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.exceptions import BatchAbortedError, EmptyBatchError, Snooze

if TYPE_CHECKING:
    import asyncpg

    from taskq.client._handle import JobHandle

__all__ = [
    "BatchCompletionStatus",
    "BatchHandle",
    "BatchSummary",
    "EnqueueItem",
    "apply_batch_terminal_outcome",
    "wait_for_batch",
]


class EnqueueItem(BaseModel):
    """One item in a :meth:`~taskq.client.JobsClient.enqueue_batch` call.

    ``actor_ref`` is an :class:`~taskq.actor.ActorRef` for any payload and
    result type.  ``payload`` is the Pydantic model that will be
    serialized into the job row — it is validated by the actor's
    ``payload_type`` inside :meth:`~taskq.client.JobsClient.enqueue_batch`
    before any INSERT.

    ``metadata`` is merged with the library-injected ``batch_id`` key
    before the row is written; callers MUST NOT set ``metadata.batch_id``
    manually.
    """

    model_config = {"arbitrary_types_allowed": True, "frozen": True}

    actor_ref: ActorRef[Any, Any]
    payload: BaseModel
    scheduled_at: datetime | None = None
    priority: int | None = None
    fairness_key: str | None = None
    idempotency_key: IdempotencyKey | str | None = None
    idempotency_scope: str | None = None
    identity_key: IdentityKey | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
    tags: list[str] | None = None
    start_to_close: timedelta | None = None


class BatchCompletionStatus(BaseModel):
    """Aggregated completion counts for a batch of jobs.

    ``pending`` counts jobs still in flight (``pending``, ``scheduled``,
    or ``running`` status).  ``is_complete`` is ``True`` when all jobs
    have reached a terminal status.
    """

    total: int
    pending: int
    succeeded: int
    failed: int
    cancelled: int
    crashed: int
    abandoned: int

    @computed_field  # type: ignore[prop-decorator]  # Why: pydantic v2 computed_field decorator; pyright stubs lag the runtime API
    @property
    def is_complete(self) -> bool:
        """``True`` when no jobs remain in a non-terminal state."""
        return self.pending == 0


class BatchHandle(BaseModel):
    """Handle to a group of jobs inserted by a single
    :meth:`~taskq.client.JobsClient.enqueue_batch` call.

    ``job_handles`` contains one :class:`~taskq.client.JobHandle` per
    item in the original list (including idempotency-key collisions that
    returned existing rows).  ``size`` equals ``len(job_handles)``.

    :meth:`status` queries the database for the current completion
    counts of the batch.
    """

    model_config = {"arbitrary_types_allowed": True}

    batch_id: UUID
    job_handles: list["JobHandle[BaseModel | None]"]
    """List of :class:`~taskq.client.JobHandle` instances, one per enqueued item."""
    size: int
    finalizer_handle: "JobHandle[BaseModel | None] | None" = None
    """The :class:`~taskq.client.JobHandle` for the finalizer job, or ``None``
    when no finalizer was enqueued.  When set, the finalizer handle is also
    appended as the last entry of :attr:`job_handles` for backward compat."""

    async def status(
        self,
        db: "asyncpg.Connection",
        *,
        schema: str = "taskq",
    ) -> BatchCompletionStatus:
        """Query live completion counts for all jobs in this batch.

        Uses a JSONB containment query against the ``metadata`` column so
        the ``jobs_metadata_gin_idx`` GIN index is used (``@>`` is
        supported by ``jsonb_path_ops``).  The query groups by status in a
        single round-trip.

        ``schema`` must match the schema used when the :class:`PostgresBackend`
        was constructed (default ``"taskq"``).
        """
        if not _IDENT_RE.match(schema):
            raise ValueError(f"invalid schema identifier: {schema!r}")

        containment = dumps_str({"batch_id": str(self.batch_id)})
        records = await db.fetch(
            f"SELECT status, count(*)::int AS cnt "  # noqa: S608  # Why: schema validated against _IDENT_RE immediately above.
            f'FROM "{schema}".jobs '
            "WHERE metadata @> $1::jsonb "
            "GROUP BY status",
            containment,
        )

        counts: dict[str, int] = {}
        for rec in records:
            counts[str(rec["status"])] = int(rec["cnt"])

        pending = counts.get("pending", 0) + counts.get("scheduled", 0) + counts.get("running", 0)
        return BatchCompletionStatus(
            total=sum(counts.values()),
            pending=pending,
            succeeded=counts.get("succeeded", 0),
            failed=counts.get("failed", 0),
            cancelled=counts.get("cancelled", 0),
            crashed=counts.get("crashed", 0),
            abandoned=counts.get("abandoned", 0),
        )


@dataclass(frozen=True, slots=True)
class BatchSummary:
    """One row from the batches table, augmented with live job counts."""

    batch_id: UUID
    queue: str
    status: Literal["active", "complete", "aborted"]
    expected_size: int
    consecutive_failures: int
    failure_threshold: int | None
    finalizer_job_id: UUID | None
    originating_actor: str | None
    created_at: datetime
    completed_at: datetime | None
    completion: BatchCompletionStatus


def _decide_batch_status(
    *,
    batch_id: UUID,
    batch_row: BatchRow | None,
    status: BatchCompletionStatus,
    snooze_interval: timedelta,
    expect_at_least: int | None,
    on_empty: Literal["error", "ok"],
) -> BatchCompletionStatus:
    """Apply the wait_for_batch decision table.

    Raises :class:`~taskq.exceptions.Snooze` when the batch is aborted
    but jobs are still in flight (so the caller retries until all are
    terminal, at which point :class:`~taskq.exceptions.BatchAbortedError`
    is raised).  Raises :class:`~taskq.exceptions.EmptyBatchError` when
    the batch has fewer jobs than expected or no jobs at all and no
    batches row exists (unless ``on_empty="ok"``).

    When ``status.pending > 0`` and the batch is NOT aborted, returns
    the status unchanged — the caller decides whether to raise
    :class:`~taskq.exceptions.Snooze` (exception mode) or block
    (polling mode).
    """
    # Case 1-2: batch is aborted
    if batch_row is not None and batch_row.status == "aborted":
        if status.pending == 0:
            raise BatchAbortedError(
                batch_id,
                batch_row.consecutive_failures,
                batch_row.failure_threshold or 0,
            )
        raise Snooze(snooze_interval)

    # Case 3: expected minimum not met
    if expect_at_least is not None and status.pending == 0 and status.total < expect_at_least:
        raise EmptyBatchError(batch_id, expected=expect_at_least, actual=status.total)

    # Case 4-6: no jobs found
    if status.total == 0:
        if batch_row is not None:
            return status
        if on_empty == "ok":
            return status
        raise EmptyBatchError(batch_id, expected=1, actual=0)

    # Case 7-8: jobs exist — if pending > 0, caller decides Snooze vs block
    return status


_WAIT_FOR_BATCH_SQL = (
    "SELECT"
    " count(*) AS total,"
    " count(*) FILTER (WHERE status = 'succeeded') AS succeeded,"
    " count(*) FILTER (WHERE status = 'failed') AS failed,"
    " count(*) FILTER (WHERE status = 'cancelled') AS cancelled,"
    " count(*) FILTER (WHERE status = 'crashed') AS crashed,"
    " count(*) FILTER (WHERE status = 'abandoned') AS abandoned,"
    " count(*) FILTER (WHERE status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')) AS in_flight"
    ' FROM "{schema}".jobs'
    " WHERE metadata @> $1::jsonb"
)

_logger = structlog.get_logger("taskq.batch")


async def apply_batch_terminal_outcome(
    backend: Backend,
    job: JobRow,
    outcome: AttemptOutcome,
    *,
    loop_conn: "asyncpg.Connection | None" = None,
) -> None:
    """Apply batch policy after a job reaches a terminal write.

    Called after every terminal write by the consumer and the in-memory
    runner.  For non-batched jobs (no ``metadata.batch_id``) this returns
    immediately — zero overhead.

    - ``"succeeded"``: resets the consecutive-failure counter.  If no
      jobs remain non-terminal, marks the batch complete.
    - ``"failed"``: increments the consecutive-failure counter.  If the
      threshold is reached, aborts the batch and logs ``batch-aborted``.
      If not aborted and no jobs remain non-terminal, marks the batch
      complete.
    - ``"cancelled"`` / ``"crashed"``: counts non-terminal jobs.  If none
      remain, marks the batch complete.
    - ``"snoozed"`` / ``"reservation_denied"`` / ``"rate_limit_denied"`` /
      ``"scheduled"``: returns immediately — the job is rescheduled, not
      terminal.
    """
    raw_bid = job.metadata.get("batch_id")
    if raw_bid is None:
        return
    batch_id = UUID(str(raw_bid))

    if outcome in ("snoozed", "reservation_denied", "rate_limit_denied", "scheduled"):
        return

    if outcome == "succeeded":
        remaining = await backend.reset_batch_failures(batch_id, connection=loop_conn)
        if remaining == 0:
            await backend.complete_batch(batch_id, connection=loop_conn)
        return

    if outcome == "failed":
        count, threshold, remaining = await backend.increment_batch_failures(
            batch_id, connection=loop_conn
        )
        if threshold is not None and count >= threshold:
            await backend.abort_batch(batch_id, connection=loop_conn)
            _logger.info(
                "batch-aborted",
                batch_id=str(batch_id),
                consecutive_failures=count,
                threshold=threshold,
                job_id=str(job.id),
            )
        elif remaining == 0:
            await backend.complete_batch(batch_id, connection=loop_conn)
        return

    # outcome is "cancelled" or "crashed" — the only remaining
    # terminal outcomes in AttemptOutcome that are not handled above.
    if outcome == "cancelled" or outcome == "crashed":
        remaining = await backend.count_batch_non_terminal(batch_id)
        if remaining == 0:
            await backend.complete_batch(batch_id, connection=loop_conn)
        return

    assert_never(outcome)


async def wait_for_batch(
    db: "asyncpg.Connection | asyncpg.Pool",
    batch_id: UUID,
    *,
    schema: str = "taskq",
    snooze_interval: timedelta = timedelta(seconds=10),
    snooze_via_exception: bool = True,
    expect_at_least: int | None = None,
    on_empty: Literal["error", "ok"] = "error",
    exclude_job_id: UUID | None = None,
) -> BatchCompletionStatus:
    """Convenience helper for the fan-out-then-finalize pattern.

    Queries batch children by batch_id using the GIN-indexed
    ``WHERE metadata @> $1::jsonb`` predicate.

    Inside an actor (snooze_via_exception=True, the default):
      - If any children are in-flight, raises Snooze(snooze_interval).
        The consumer transitions the job to scheduled; the actor is
        retried after snooze_interval without consuming retry budget.
      - If all children are terminal, returns BatchCompletionStatus.

    Outside an actor (snooze_via_exception=False):
      - Blocks via asyncio.sleep(snooze_interval) in a loop until all
        children are terminal, then returns BatchCompletionStatus.
      - Use this form from scripts and integration tests where no consumer
        is present to translate a Snooze into a rescheduled job.

    ``expect_at_least`` raises :class:`~taskq.exceptions.EmptyBatchError`
    when fewer than the expected number of jobs are present and none are
    in flight.  ``on_empty`` controls the behaviour when the batch has
    zero jobs and no ``batches`` row exists: ``"error"`` (default) raises
    :class:`~taskq.exceptions.EmptyBatchError`; ``"ok"`` returns the
    empty status.  ``exclude_job_id`` omits a specific job from the
    count — when not set, the batch row's ``finalizer_job_id`` is used
    automatically.

    If the batch row has ``status = 'aborted'`` and all jobs are
    terminal, raises :class:`~taskq.exceptions.BatchAbortedError`.

    snooze_interval is clamped to a minimum of 1 second.
    ``schema`` must match the schema used when the PostgresBackend was
    constructed (default ``"taskq"``).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    _min_snooze = timedelta(seconds=1)

    import asyncpg as _asyncpg

    if snooze_interval < _min_snooze:
        original = snooze_interval
        snooze_interval = _min_snooze
        _logger.warning(
            "snooze-interval-clamped",
            original=str(original),
            clamped=str(snooze_interval),
        )

    containment = dumps_str({"batch_id": str(batch_id)})

    _batch_row_sql = (
        f"SELECT id, queue, status, expected_size, consecutive_failures, "  # noqa: S608  # Why: schema validated against _IDENT_RE immediately above.
        f"failure_threshold, finalizer_job_id, originating_actor, "
        f"created_at, completed_at, metadata "
        f'FROM "{schema}".batches WHERE id = $1'
    )

    async def _fetch_batch_row(
        conn: "asyncpg.Connection",
    ) -> BatchRow | None:
        try:
            rec = await conn.fetchrow(_batch_row_sql, batch_id)
        except _asyncpg.exceptions.UndefinedTableError:
            return None
        if rec is None:
            return None
        return BatchRow(
            id=rec["id"],
            queue=rec["queue"],
            status=rec["status"],
            expected_size=rec["expected_size"],
            consecutive_failures=rec["consecutive_failures"],
            failure_threshold=rec["failure_threshold"],
            finalizer_job_id=rec["finalizer_job_id"],
            originating_actor=rec["originating_actor"],
            created_at=rec["created_at"],
            completed_at=rec["completed_at"],
            metadata=jsonb_to_dict(rec["metadata"]) or {},
        )

    async def _fetch_and_decide(
        conn: "asyncpg.Connection",
    ) -> BatchCompletionStatus:
        batch_row = await _fetch_batch_row(conn)

        exclusion_id = exclude_job_id
        if exclusion_id is None and batch_row is not None:
            exclusion_id = batch_row.finalizer_job_id

        if exclusion_id is not None:
            sql = _WAIT_FOR_BATCH_SQL.format(schema=schema) + " AND id <> $2"
            row = await conn.fetchrow(sql, containment, exclusion_id)
        else:
            sql = _WAIT_FOR_BATCH_SQL.format(schema=schema)
            row = await conn.fetchrow(sql, containment)

        if row is None:
            status = BatchCompletionStatus(
                total=0,
                pending=0,
                succeeded=0,
                failed=0,
                cancelled=0,
                crashed=0,
                abandoned=0,
            )
        else:
            status = BatchCompletionStatus(
                total=int(row["total"]),
                pending=int(row["in_flight"]),
                succeeded=int(row["succeeded"]),
                failed=int(row["failed"]),
                cancelled=int(row["cancelled"]),
                crashed=int(row["crashed"]),
                abandoned=int(row["abandoned"]),
            )

        status = _decide_batch_status(
            batch_id=batch_id,
            batch_row=batch_row,
            status=status,
            snooze_interval=snooze_interval,
            expect_at_least=expect_at_least,
            on_empty=on_empty,
        )

        # Snooze for pending > 0 (batch not aborted) — the decision
        # function already raises Snooze for the aborted-but-in-flight
        # case.  Here we handle the normal pending case.
        if status.pending > 0 and snooze_via_exception:
            raise Snooze(snooze_interval)

        return status

    if isinstance(db, _asyncpg.Pool):
        async with db.acquire() as conn:  # type: ignore[reportArgumentType]  # Why: Pool.acquire() returns PoolConnectionProxy; pyright stubs model it as incompatible with Connection but it is runtime-compatible
            status = await _fetch_and_decide(conn)  # type: ignore[reportArgumentType]  # Why: PoolConnectionProxy is a runtime-compatible Connection proxy; pyright stubs model it as incompatible
    else:
        status = await _fetch_and_decide(db)

    if status.pending > 0 and not snooze_via_exception:
        while status.pending > 0:
            await asyncio.sleep(snooze_interval.total_seconds())
            if isinstance(db, _asyncpg.Pool):
                async with db.acquire() as conn:  # type: ignore[reportArgumentType]  # Why: Pool.acquire() returns PoolConnectionProxy; pyright stubs model it as incompatible with Connection but it is runtime-compatible
                    status = await _fetch_and_decide(conn)  # type: ignore[reportArgumentType]  # Why: PoolConnectionProxy is a runtime-compatible Connection proxy; pyright stubs model it as incompatible
            else:
                status = await _fetch_and_decide(db)

    return status


# ActorRef is a generic class that may not be fully defined when
# EnqueueItem is first parsed.  model_rebuild() ensures Pydantic can
# resolve the forward reference and validate instances at runtime.
EnqueueItem.model_rebuild()

# BatchHandle references JobHandle in its field types. JobHandle lives in
# taskq.client._handle, which (via taskq.client.__init__) imports back from
# taskq.batch — a circular dependency.  Deferring the import to the end of
# the module ensures all batch.py classes are already defined when the
# client subpackage tries to import them.  model_rebuild() then resolves
# the forward references in BatchHandle's field annotations.
from taskq.client._handle import (  # noqa: E402  # Why: deferred to end of module to break circular dependency with taskq.client; needed for Pydantic forward-reference resolution via model_rebuild()
    JobHandle,
)

BatchHandle.model_rebuild()

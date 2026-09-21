"""Batch enqueue primitives for TaskQ.

Provides:
- :class:`EnqueueItem`, one item in a :meth:`~taskq.client.JobsClient.enqueue_batch` call.
- :class:`BatchCompletionStatus`, aggregated counts across all jobs in a batch.
- :class:`BatchHandle`, returned by :meth:`~taskq.client.JobsClient.enqueue_batch`;
  holds all :class:`~taskq.client.JobHandle` instances and exposes a
  :meth:`BatchHandle.status` query.
- :class:`BatchSummary`, one row from the batches table augmented with
  live job counts; returned by :meth:`~taskq.client.JobsClient.list_batches`.
- :func:`wait_for_batch`, convenience helper for the fan-out-then-finalize
  pattern.
- :func:`apply_batch_terminal_outcome`, batch policy hook called after
  every terminal write; drives abort/completion semantics.
- :data:`MAX_BATCH_SIZE`, maximum number of items per ``enqueue_batch``
  call and upper bound on ``chunk_size`` for ``enqueue_batch_streaming``.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, assert_never
from uuid import UUID

import structlog
from pydantic import BaseModel, Field, computed_field

from taskq._json import dumps_str
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    AttemptOutcome,
    Backend,
    BatchRow,
    BatchStatus,
    ConnLike,
    IdempotencyKey,
    IdentityKey,
    JobRow,
)
from taskq.backend._records import _batch_row_from_record
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.exceptions import BatchAbortedError, EmptyBatchError, Snooze

if TYPE_CHECKING:
    import asyncpg

    from taskq.client._handle import JobHandle

__all__ = [
    "MAX_BATCH_SIZE",
    "MIN_SNOOZE_INTERVAL",
    "BatchCompletionStatus",
    "BatchHandle",
    "BatchSummary",
    "EnqueueItem",
    "apply_batch_terminal_outcome",
    "decide_batch_status",
    "wait_for_batch",
]

MAX_BATCH_SIZE: int = 1000
"""Maximum number of items accepted by :meth:`enqueue_batch` and the
chunk size upper bound for :meth:`enqueue_batch_streaming`.

Exceeding this in :meth:`enqueue_batch` raises :class:`ValueError`;
:meth:`enqueue_batch_streaming` rejects ``chunk_size`` outside
``[1, MAX_BATCH_SIZE]``.
"""

MIN_SNOOZE_INTERVAL: timedelta = timedelta(seconds=1)
"""Minimum snooze interval enforced by :func:`wait_for_batch`.

Caller-supplied ``snooze_interval`` values below this are clamped and a
warning is logged.
"""

_POOL_ACQUIRE_TIMEOUT_S: Final[float] = 2.0
"""Bound for the pool acquire on every :func:`wait_for_batch` poll.

asyncpg's ``Pool.acquire`` has no default timeout, so an unbounded
acquire parks the poll, the first one and every snooze-loop iteration
after it, for as long as the pool stays exhausted. This is the same
bound and rationale as the project's other caller-facing pool waits
(``pg_pool.acquire(timeout=2.0)`` in the workgroup health check,
``DEFAULT_CAPACITY_READ_TIMEOUT`` around JobsClient's schedule-seed
read): a wait on something outside the process is bounded, and
exceeding the bound is reported as a :class:`TimeoutError` instead of
wedging the caller. Only the acquire is bounded, the poll's statement
runs on the caller's own pool, whose statement/command timeouts remain
that pool's contract.
"""


class EnqueueItem(BaseModel):
    """One item in a :meth:`~taskq.client.JobsClient.enqueue_batch` call.

    ``actor_ref`` is an :class:`~taskq.actor.ActorRef` for any payload and
    result type.  ``payload`` is the Pydantic model that will be
    serialized into the job row, it is validated by the actor's
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

    **Invariant:** ``job_handles`` contains one
    :class:`~taskq.client.JobHandle` per item in the original list
    (including idempotency-key collisions that returned existing rows).
    When a finalizer was enqueued, the finalizer handle is appended as
    the **last** entry of ``job_handles`` AND set separately as
    ``finalizer_handle``.  ``size`` is the number of non-finalizer items
    (i.e. ``len(job_handles) - (1 if finalizer_handle is not None else 0)``).

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
    status: BatchStatus
    expected_size: int
    consecutive_failures: int
    failure_threshold: int | None
    finalizer_job_id: UUID | None
    originating_actor: str | None
    created_at: datetime
    completed_at: datetime | None
    completion: BatchCompletionStatus


def decide_batch_status(
    *,
    batch_id: UUID,
    batch_row: BatchRow | None,
    status: BatchCompletionStatus,
    snooze_interval: timedelta,
    expect_at_least: int | None,
    on_empty: Literal["error", "ok"],
    snooze_via_exception: bool = True,
) -> BatchCompletionStatus:
    """Apply the wait_for_batch decision table.

    Raises :class:`~taskq.exceptions.Snooze` when the batch is aborted
    but jobs are still in flight and ``snooze_via_exception`` is True
    (so the caller retries until all are terminal, at which point
    :class:`~taskq.exceptions.BatchAbortedError` is raised).  When
    ``snooze_via_exception`` is False, returns the status instead so the
    poll loop continues.

    Raises :class:`~taskq.exceptions.EmptyBatchError` when the batch has
    fewer jobs than expected or no jobs at all and no batches row exists
    (unless ``on_empty="ok"``).  Also raises when a batch row exists with
    ``expected_size > 0`` but zero jobs are found and none are pending
    (jobs pruned or never created).

    When ``status.pending > 0`` and the batch is NOT aborted, returns
    the status unchanged, the caller decides whether to raise
    :class:`~taskq.exceptions.Snooze` (exception mode) or block
    (polling mode).
    """
    # Case 1-2: batch is aborted
    if batch_row is not None and batch_row.status == "aborted":
        if status.pending == 0:
            raise BatchAbortedError(
                batch_id,
                batch_row.consecutive_failures,
                batch_row.failure_threshold,
            )
        if snooze_via_exception:
            raise Snooze(snooze_interval)
        return status

    # Case 3: expected minimum not met
    if expect_at_least is not None and status.pending == 0 and status.total < expect_at_least:
        raise EmptyBatchError(batch_id, expected=expect_at_least, actual=status.total)

    # Case 4-6: no jobs found
    if status.total == 0:
        if batch_row is not None:
            # M5: batch row exists with expected_size > 0 but zero jobs
            # (pruned or never created), surface as an error, not silent OK.
            if batch_row.expected_size > 0 and status.pending == 0:
                raise EmptyBatchError(batch_id, expected=batch_row.expected_size, actual=0)
            return status
        if on_empty == "ok":
            return status
        raise EmptyBatchError(batch_id, expected=1, actual=0)

    # Case 7-8: jobs exist, if pending > 0, caller decides Snooze vs block
    return status


# Build the terminal-status NOT IN clause from the canonical
# TERMINAL_STATUSES set so the SQL never drifts when a new status is
# added to the state machine.
# Duplicates _TERMINAL_NOT_IN in taskq.backend._batch_sql, kept separate
# because _batch_sql wraps the clause in "NOT IN (...)" while here it is
# interpolated into a FILTER expression that supplies its own "NOT IN (".
_TERMINAL_NOT_IN_SQL = ",".join(f"'{s}'" for s in TERMINAL_STATUSES)

# One statement per poll for wait_for_batch: the member counts and the
# batches row travel together in a single statement instead of a separate
# row fetch and counts fetch, two sequential round trips of pure latency
# for the finalizer loop. The counts subquery has no GROUP BY and therefore
# always yields exactly one row; the batches row joins by primary key on
# that row, so a batch with no row (enqueue_batch_fast members carry
# batch_id metadata only) still reports its counts, and a row with no
# members still reports its fields, the expected_size the empty-batch
# decision reads. A joined-away row would surface as all-NULL batch fields,
# which the reader turns into batch_row=None.
_WFB_SELECT = (
    "SELECT c.total, c.succeeded, c.failed, c.cancelled, c.crashed,"
    " c.abandoned, c.in_flight,"
    " b.id, b.queue, b.status, b.expected_size, b.consecutive_failures,"
    " b.failure_threshold, b.finalizer_job_id, b.originating_actor,"
    " b.created_at, b.completed_at, b.metadata"
)
_WFB_COUNTS = (
    " SELECT count(*) AS total,"
    " count(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,"
    " count(*) FILTER (WHERE j.status = 'failed') AS failed,"
    " count(*) FILTER (WHERE j.status = 'cancelled') AS cancelled,"
    " count(*) FILTER (WHERE j.status = 'crashed') AS crashed,"
    " count(*) FILTER (WHERE j.status = 'abandoned') AS abandoned,"
    " count(*) FILTER (WHERE j.status NOT IN ({terminal_status_list})) AS in_flight"
    ' FROM "{schema}".jobs j'
)
_WFB_BATCH_ROW_JOIN = ' LEFT JOIN "{schema}".batches b ON b.id = $2'

# Finalizer auto-exclusion: the batches row joins inside the counts CTE
# so the exclusion resolves from the row itself, a NULL
# finalizer_job_id (or no row at all) excludes nothing.
_WAIT_FOR_BATCH_SQL = (
    _WFB_SELECT
    + " FROM ("
    + _WFB_COUNTS
    + ' LEFT JOIN "{schema}".batches fb ON fb.id = $2'
    + " WHERE j.metadata @> $1::jsonb"
    + " AND (fb.finalizer_job_id IS NULL OR j.id <> fb.finalizer_job_id)"
    + " ) c"
    + _WFB_BATCH_ROW_JOIN
)

# Caller-supplied exclude_job_id replaces the finalizer exclusion, one
# excluded id either way, the same contract the loop has always held.
_WAIT_FOR_BATCH_EXCLUDED_SQL = (
    _WFB_SELECT
    + " FROM ("
    + _WFB_COUNTS
    + " WHERE j.metadata @> $1::jsonb"
    + " AND j.id <> $3"
    + " ) c"
    + _WFB_BATCH_ROW_JOIN
)

_logger = structlog.get_logger("taskq.batch")


async def apply_batch_terminal_outcome(
    backend: Backend,
    job: JobRow,
    outcome: AttemptOutcome | Literal["noop"],
    *,
    transaction_conn: "ConnLike | None" = None,
) -> None:
    """Apply batch policy after a job reaches a terminal write.

    Called after every terminal write by the consumer and the in-memory
    runner.  For non-batched jobs (no ``metadata.batch_id``) this returns
    immediately, zero overhead.  *outcome* is the dispatch outcome the
    caller reports: an attempt-row outcome, or the consumer's ``"noop"``
    (a terminal write that matched nothing, the job was never this
    dispatch's to move, so no batch counter may budge).

    - ``"succeeded"``: resets the consecutive-failure counter and
      attempts completion.
    - ``"failed"``: increments the consecutive-failure counter.  If the
      threshold is reached, aborts the batch and logs ``batch-aborted``
     , abort wins, so no completion attempt runs on that path.  The
      event reports the threshold DECISION (the abort was issued for
      this count); the flip itself can still be delayed by the bounded
      batches-row wait, a skip is separately disclosed by the
      ``batch-abort-row-lock-timeout`` debug event, and the next
      threshold-triggered abort or the stale-batch sweep re-arbitrates.
      If the threshold is not reached, attempts completion.
    - ``"cancelled"`` / ``"crashed"``: attempts completion.
    - ``"snoozed"`` / ``"reservation_denied"`` / ``"rate_limit_denied"`` /
      ``"scheduled"`` / ``"noop"``: returns immediately, the job is
      rescheduled (or was never this dispatch's to move), not terminal.

    Completion is self-arbitrating: every terminal outcome issues the
    ``complete_batch`` attempt, and that statement's ``NOT EXISTS``
    guard decides against the live member set in its own snapshot.  The
    increment/reset count is advisory only, under READ COMMITTED two
    members terminating concurrently can each read the other as
    non-terminal, so a hook that gated the attempt on that count could
    leave a fully-terminal batch for the leader sweep; the optimistic
    attempt after the last terminal write is the one that lands, and a
    premature one is a no-op.

    **Best-effort semantics (M7):** the increment/reset/abort/complete
    writes are best-effort.  A crash between the terminal job write and
    the counter increment loses that increment, the failure count is
    under-counted by one.  The next terminal failure re-triggers the
    check and increments again, so a consistently failing batch still
    aborts (just one failure later than it would have).  The same loss
    class covers a counter write that skipped instead of parking: the
    batches-row wait is bounded, and a streaming append holding the row
    past the budget skips that one increment or reset (the streak is
    frozen, never reset) while the failure itself stays recorded on the
    job row.  The stale-batch
    sweep is the safety net for batch **STATUS** (it transitions stuck
    active/aborted rows to terminal) but it **cannot** recover lost
    failure **counts**, a crash gap means the consecutive-failure streak
    is permanently broken, potentially preventing an abort that should
    have fired.
    """
    raw_bid = job.metadata.get("batch_id")
    if raw_bid is None:
        return
    batch_id = UUID(str(raw_bid))

    if outcome in ("snoozed", "reservation_denied", "rate_limit_denied", "scheduled", "noop"):
        return

    if outcome == "succeeded":
        await backend.reset_batch_failures(batch_id, connection=transaction_conn)
        # The reset's remaining count is that statement's snapshot, not
        # the completion decision, see the docstring's self-arbitrating
        # paragraph. complete_batch re-checks membership in its own
        # statement, so the optimistic attempt can delay but never
        # complete prematurely.
        await backend.complete_batch(batch_id, connection=transaction_conn)
        return

    if outcome == "failed":
        count, threshold, _remaining = await backend.increment_batch_failures(
            batch_id, connection=transaction_conn
        )
        if threshold is not None and count >= threshold:
            await backend.abort_batch(batch_id, connection=transaction_conn)
            _logger.info(
                "batch-aborted",
                batch_id=str(batch_id),
                consecutive_failures=count,
                threshold=threshold,
                job_id=str(job.id),
            )
            # Abort wins over complete: the completion attempt is the
            # fall-through below this return, so an aborted batch is
            # never also completed from this hook call.
            return
        await backend.complete_batch(batch_id, connection=transaction_conn)
        return

    # outcome is "cancelled" or "crashed", the only remaining
    # terminal outcomes in AttemptOutcome that are not handled above.
    if outcome in ("cancelled", "crashed"):
        await backend.complete_batch(batch_id, connection=transaction_conn)
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
    ``WHERE metadata @> $1::jsonb`` predicate, the containment form,
    not the open-members partial index the completion probes use, because
    the poll counts every member including the terminal ones. Each poll is
    one round trip: the member counts and the ``batches`` row travel in a
    single statement.

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
      - Each poll's pool acquire is bounded by
        :data:`_POOL_ACQUIRE_TIMEOUT_S`: a pool that cannot yield a
        connection within the bound surfaces as a :class:`TimeoutError`
        (the project's report idiom for an exhausted pool) rather than
        parking the wait forever.

    ``expect_at_least`` raises :class:`~taskq.exceptions.EmptyBatchError`
    when fewer than the expected number of jobs are present and none are
    in flight.  ``on_empty`` controls the behaviour when the batch has
    zero jobs and no ``batches`` row exists: ``"error"`` (default) raises
    :class:`~taskq.exceptions.EmptyBatchError`; ``"ok"`` returns the
    empty status.  ``exclude_job_id`` omits a specific job from the
    count, when not set, the batch row's ``finalizer_job_id`` is used
    automatically.

    If the batch row has ``status = 'aborted'`` and all jobs are
    terminal, raises :class:`~taskq.exceptions.BatchAbortedError`.

    snooze_interval is clamped to a minimum of 1 second.
    ``schema`` must match the schema used when the PostgresBackend was
    constructed (default ``"taskq"``).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    import asyncpg as _asyncpg

    if snooze_interval < MIN_SNOOZE_INTERVAL:
        original = snooze_interval
        snooze_interval = MIN_SNOOZE_INTERVAL
        _logger.warning(
            "snooze-interval-clamped",
            original=str(original),
            clamped=str(snooze_interval),
        )

    containment = dumps_str({"batch_id": str(batch_id)})

    async def _fetch_and_decide(
        conn: "asyncpg.Connection",
    ) -> BatchCompletionStatus:
        # One round trip per poll: counts and the batches row arrive in
        # one statement (see the _WAIT_FOR_BATCH_* templates). The
        # aggregate always yields exactly one row; None is the degraded
        # or faked-connection path.
        if exclude_job_id is not None:
            row = await conn.fetchrow(
                _WAIT_FOR_BATCH_EXCLUDED_SQL.format(
                    schema=schema, terminal_status_list=_TERMINAL_NOT_IN_SQL
                ),
                containment,
                batch_id,
                exclude_job_id,
            )
        else:
            row = await conn.fetchrow(
                _WAIT_FOR_BATCH_SQL.format(
                    schema=schema, terminal_status_list=_TERMINAL_NOT_IN_SQL
                ),
                containment,
                batch_id,
            )

        if row is None:
            batch_row = None
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
            # The batches row joined by primary key: all-NULL fields mean
            # no row exists (batch_id-only metadata, or a pruned row).
            batch_row = None if row["id"] is None else _batch_row_from_record(row)
            status = BatchCompletionStatus(
                total=int(row["total"]),
                pending=int(row["in_flight"]),
                succeeded=int(row["succeeded"]),
                failed=int(row["failed"]),
                cancelled=int(row["cancelled"]),
                crashed=int(row["crashed"]),
                abandoned=int(row["abandoned"]),
            )

        status = decide_batch_status(
            batch_id=batch_id,
            batch_row=batch_row,
            status=status,
            snooze_interval=snooze_interval,
            expect_at_least=expect_at_least,
            on_empty=on_empty,
            snooze_via_exception=snooze_via_exception,
        )

        # Snooze for pending > 0 (batch not aborted), the decision
        # function already handles the aborted-but-in-flight case
        # (raises Snooze or returns status depending on snooze_via_exception).
        # Here we handle the normal pending case.
        if status.pending > 0 and snooze_via_exception:
            raise Snooze(snooze_interval)

        return status

    async def _fetch() -> BatchCompletionStatus:
        if isinstance(db, _asyncpg.Pool):
            # Bounded acquire (see _POOL_ACQUIRE_TIMEOUT_S): on an
            # exhausted pool the wait surfaces as a TimeoutError to the
            # caller instead of parking this poll, and every
            # snooze-loop iteration after it, forever. The workgroup
            # health check bounds the identical shape the same way.
            async with db.acquire(timeout=_POOL_ACQUIRE_TIMEOUT_S) as conn:  # type: ignore[reportArgumentType]  # Why: Pool.acquire() returns PoolConnectionProxy; pyright stubs model it as incompatible with Connection but it is runtime-compatible
                return await _fetch_and_decide(conn)  # type: ignore[reportArgumentType]  # Why: PoolConnectionProxy is a runtime-compatible Connection proxy; pyright stubs model it as incompatible
        return await _fetch_and_decide(db)

    status = await _fetch()

    if status.pending > 0 and not snooze_via_exception:
        while status.pending > 0:
            await asyncio.sleep(snooze_interval.total_seconds())
            status = await _fetch()

    return status


# ActorRef is a generic class that may not be fully defined when
# EnqueueItem is first parsed.  model_rebuild() ensures Pydantic can
# resolve the forward reference and validate instances at runtime.
EnqueueItem.model_rebuild()

# BatchHandle references JobHandle in its field types. JobHandle lives in
# taskq.client._handle, which (via taskq.client.__init__) imports back from
# taskq.batch, a circular dependency.  Deferring the import to the end of
# the module ensures all batch.py classes are already defined when the
# client subpackage tries to import them.  model_rebuild() then resolves
# the forward references in BatchHandle's field annotations.
from taskq.client._handle import (  # noqa: E402  # Why: deferred to end of module to break circular dependency with taskq.client; needed for Pydantic forward-reference resolution via model_rebuild()
    JobHandle,
)

BatchHandle.model_rebuild()

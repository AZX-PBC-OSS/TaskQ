"""Exception hierarchy for TaskQ.

Mirrors: control-flow exceptions like Snooze and
RetryAfter are not errors — they are signals the consumer translates into
state transitions.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from taskq._scope import Scope
from taskq.backend._protocol import JobId, JobStatus

if TYPE_CHECKING:
    from taskq.backend._protocol import EnqueueArgs, JobRow


class TaskQError(Exception):
    """Base for all library-raised exceptions."""


class JobFailed(TaskQError):
    """:meth:`JobHandle.wait` saw a non-success terminal state.

    Carries the row so callers can inspect ``status``, ``error_class``,
    ``error_message``, and ``error_traceback``. Distinct from
    :class:`ResultUnavailable` (which means terminal but no result
    stored) and from the original actor exception (which is recorded on
    the row, not raised).
    """

    def __init__(self, row: "JobRow") -> None:
        self.row = row
        super().__init__(
            f"job {row.id} ended in {row.status!r}"
            + (f": {row.error_class}: {row.error_message}" if row.error_class else ""),
        )


class ResultUnavailable(TaskQError):
    """:meth:`JobHandle.wait` saw a terminal state but no usable result.

    Causes:
    - result TTL expired before the call;
    - actor returned ``None`` while ``R`` is non-``None``
      (treated as schema mismatch, not a value);
    - row stored ``result=NULL`` for a non-success status.

    Carries the row for inspection.
    """

    def __init__(self, row: "JobRow") -> None:
        self.row = row
        super().__init__(f"job {row.id} has no stored result")


class BackpressureError(TaskQError):
    """Base class for synchronous enqueue-time backpressure signals.

    Subclassed by SingletonCollisionError (singleton collision) and used
    directly by max_pending enforcement. The caller decides whether to
    retry, fail, or wait; the library does not block on capacity.
    """

    def __init__(self, actor: str, pending: int = 0, max_pending: int | None = None) -> None:
        self.actor = actor
        self.pending = pending
        self.max_pending = max_pending
        super().__init__(
            f"BackpressureError: actor={actor}, pending={pending}, max_pending={max_pending}"
        )


class SingletonCollisionError(BackpressureError):
    """Raised when a singleton actor already has a job in pending/scheduled/running.

    ``blocking_job_id`` is the UUID of the existing job from the Layer 1
    pre-flight query; it is ``None`` when raised from the Layer 2
    UniqueViolationError catch (the race path) because no pre-flight row
    was fetched.

    ``retry_after`` is computed from the blocking job's ``schedule_to_close``
    when available. It is ``None`` when the blocking job has no
    ``schedule_to_close`` set, or when raised from the Layer 2 catch path.

    The ``heartbeat_interval * 4`` fallback is intentionally NOT
    implemented — ``retry_after`` is computed from ``schedule_to_close`` only.
    Callers who need a poll cadence when ``retry_after is None`` should poll on
    their own schedule (research.md Gap 1, resolution path (a)). Reason:
    ``heartbeat_interval`` is not available at the backend enqueue boundary;
    propagating it would require enlarging the backend constructor surface and
    is out of scope.
    """

    def __init__(
        self,
        actor: str,
        blocking_job_id: UUID | None = None,
        retry_after: timedelta | None = None,
    ) -> None:
        self.blocking_job_id = blocking_job_id
        self.retry_after = retry_after
        super().__init__(actor)


class MaxPendingExceededError(BackpressureError):
    """Raised when an actor's max_pending queue-depth limit is reached.

    ``current_count`` is the count of pending+scheduled jobs at the time
    of the pre-flight check. ``max_pending`` is the configured limit.
    The caller decides whether to retry, fail, or wait; the library does
    not block on capacity.
    """

    def __init__(self, actor: str, current_count: int, max_pending: int) -> None:
        self.current_count = current_count
        super().__init__(actor, pending=current_count, max_pending=max_pending)


class PayloadValidationError(TaskQError):
    """Pydantic validation failed at enqueue or dispatch.

    At enqueue: raised before the row is inserted ('fail at the door').
    At dispatch: causes the job to transition to 'failed' with
    error_class='PayloadValidationError'. Non-retryable in both cases
    regardless of the actor's retry policy.
    """

    def __init__(
        self,
        detail: str,
        *,
        actor: str | None = None,
        payload_schema_ver: str | None = None,
        validation_errors: list[dict[str, object]] | None = None,
    ) -> None:
        self.actor = actor
        self.payload_schema_ver = payload_schema_ver
        self.validation_errors: list[dict[str, object]] = validation_errors or []
        super().__init__(detail)


class ResultTooLarge(TaskQError):
    """Terminal result exceeded the 64KB cap."""


class ProgressTooLarge(TaskQError):
    """Raised when progress data payload exceeds the configured size limit.

    ``limit`` is the configured cap in bytes (``WorkerSettings.progress_data_max_bytes``).
    ``actual`` is the serialised byte length of the ``data`` dict that was rejected.
    Non-retryable: the caller must reduce the payload before retrying.
    """

    def __init__(self, limit: int, actual: int) -> None:
        self.limit = limit
        self.actual = actual
        super().__init__(f"Progress data payload {actual}B exceeds limit {limit}B")


class ScopeViolation(TaskQError):
    """A provider depends on a shorter-lived scope than its own."""

    def __init__(
        self,
        *,
        from_scope: Scope,
        to_scope: Scope,
        type_name: str,
        dependent: str,
    ) -> None:
        self.from_scope = from_scope
        self.to_scope = to_scope
        self.type_name = type_name
        self.dependent = dependent
        super().__init__(
            f"{from_scope.name}-scoped {dependent} depends on {to_scope.name}-scoped {type_name}"
        )


class DependencyCycle(TaskQError):
    """A cycle was detected in the provider graph."""

    def __init__(self, cycle_path: list[str]) -> None:
        if len(cycle_path) < 2:
            raise ValueError(
                f"cycle_path must contain at least 2 entries (got {len(cycle_path)!r})"
            )
        self.cycle_path = list(cycle_path)
        super().__init__(f"dependency cycle: {' -> '.join(cycle_path)}")


class MissingProvider(TaskQError):
    """A type was injected but no provider is registered."""

    def __init__(self, *, type_name: str, required_by: str) -> None:
        self.type_name = type_name
        self.required_by = required_by
        super().__init__(f"no provider registered for {type_name} (required by {required_by})")


class DIError(TaskQError):
    """Base for DI engine errors not covered by startup-validation.

    Raised by the solver at resolution time for malformed annotations (e.g.
    multiple Scope markers in one Annotated parameter) or unresolvable
    forward references in actor signatures. Distinct from MissingProvider /
    ScopeViolation / DependencyCycle, which are raised at startup
    validation.
    """


class Snooze(TaskQError):
    """Job returns control with new scheduled_at; does not consume retry budget."""

    def __init__(self, delay: timedelta) -> None:
        if delay < timedelta(0):
            raise ValueError(f"delay must be non-negative, got {delay!r}")
        super().__init__(f"snooze for {delay}")
        self.delay = delay


class RetryAfter(TaskQError):
    """Schedule retry at specific delay. Consumes retry budget by default."""

    def __init__(self, delay: timedelta, *, consume_budget: bool = True) -> None:
        if delay < timedelta(0):
            raise ValueError(f"delay must be non-negative, got {delay!r}")
        super().__init__(f"retry after {delay}")
        self.delay = delay
        self.consume_budget = consume_budget


class ReservationUnavailable(TaskQError):
    """A ConcurrencyReservation slot could not be acquired.

    When the upstream ``RateLimitDecision.retry_after`` is ``None``, callers
    MUST substitute ``DEFAULT_RESERVATION_BACKOFF``. When it is
    ``timedelta(0)`` (allowed decisions) callers MUST pass it through
    unchanged — do NOT use a truthiness coalesce
    (``x or DEFAULT_RESERVATION_BACKOFF``) because ``timedelta(0)`` is falsy
    and would be wrongly replaced.
    """

    def __init__(
        self,
        bucket_name: str,
        retry_after: timedelta,
        *,
        source: Literal["reservation", "rate_limit"] = "reservation",
    ) -> None:
        if retry_after < timedelta(0):
            raise ValueError(f"retry_after must be non-negative, got {retry_after!r}")
        super().__init__(f"no reservation slot in {bucket_name!r}")
        self.bucket_name = bucket_name
        self.retry_after = retry_after
        self.source = source


class IllegalStateTransition(TaskQError):
    """Attempted to transition a job to a status not reachable from its current status.

    Best-effort fast-path check only; the SQL WHERE clause is the
    authoritative serialization gate for concurrent writes.
    """

    def __init__(
        self,
        job_id: JobId,
        from_status: JobStatus,
        to_status: JobStatus,
    ) -> None:
        self.job_id = job_id
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"job {self.job_id} cannot transition from {self.from_status} to {self.to_status}"
        )


class WorkerOwnershipMismatch(TaskQError):
    """Terminal write predicate failed: job exists but is owned by a different worker."""

    def __init__(
        self,
        job_id: UUID,
        expected: UUID,
        actual: UUID | None,
    ) -> None:
        self.job_id = job_id
        self.expected = expected
        self.actual = actual
        super().__init__(f"job {self.job_id} owned by {actual}, expected {expected}")


_ACTOR_CONFIG_DRIFT_HINT = (
    "Re-run with --force-update-actor-config or set "
    "TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true to overwrite the stored config."
)


class ActorConfigDriftError(TaskQError):
    """One actor whose registered config differs from the stored row."""

    hint = _ACTOR_CONFIG_DRIFT_HINT

    def __init__(
        self,
        actor: str,
        field: Literal["max_concurrent", "max_pending", "queue", "result_ttl", "metadata"],
        registered: int | float | str | dict[str, object] | None,
        stored: int | float | str | dict[str, object] | None,
    ) -> None:
        self.actor = actor
        self.field = field
        self.registered = registered
        self.stored = stored
        super().__init__(
            f"ActorConfigDrift: actor={actor}, field={field}, "
            f"registered={registered!r}, stored={stored!r}"
        )


class ActorConfigDriftList(TaskQError):
    """Collected wrapper raised at worker startup when one or more actors have drift."""

    hint = _ACTOR_CONFIG_DRIFT_HINT

    def __init__(self, drifts: tuple[ActorConfigDriftError, ...]) -> None:
        self.drifts = drifts
        lines = [f"{len(drifts)} actor(s) have config drift:"]
        for d in drifts:
            lines.append(f"  {d}")
        lines.append(self.hint)
        super().__init__("\n".join(lines))


class PartialBatchError(TaskQError):
    """Raised when an autonomous enqueue_batch partially fails.

    Items enqueued before the first failure are committed; remaining
    items are not inserted.  ``succeeded_count`` is the number of items
    that were successfully enqueued.  ``failed_items`` maps the index
    of each failed item to its exception.  ``total`` is the original
    batch size.
    """

    def __init__(
        self,
        *,
        succeeded_count: int,
        failed_items: list[tuple[int, Exception]],
        total: int,
    ) -> None:
        self.succeeded_count = succeeded_count
        self.failed_items = failed_items
        self.total = total
        super().__init__(
            f"PartialBatchError: {succeeded_count}/{total} succeeded, "
            f"{len(failed_items)} failed at indices: {[i for i, _ in failed_items]}"
        )


class SchemaNotMigratedError(TaskQError):
    """Backend raised ``UndefinedTableError`` — the TaskQ schema is missing.

    Translated by the client layer (:mod:`taskq.client._jobs`) from an
    ``asyncpg.exceptions.UndefinedTableError`` on the enqueue/get/list/cancel
    paths, so operators see an actionable message instead of a raw asyncpg
    traceback. The original exception is chained via ``__cause__``.
    """

    def __init__(self, schema: str) -> None:
        self.schema = schema
        super().__init__(
            f"TaskQ schema {schema!r} is missing or not migrated. "  # noqa: S608  # Why: human-readable error message, not a SQL query; ruff's SQL-injection heuristic false-positives on the word "schema" near f-string interpolation.
            f"Run `taskq migrate up` to create/update it, or set "
            f"TASKQ_MIGRATE_ON_START=true to migrate automatically at worker startup."
        )


class SubEnqueueError(TaskQError):
    """Raised by flush_buffer() when one or more buffered sub-job enqueues fail after parent commit.

    ``failed_items`` carries each failed ``EnqueueArgs`` and the exception
    that caused the enqueue to fail.  The parent job has already been
    marked succeeded — this exception signals that child jobs were lost.
    """

    def __init__(
        self,
        failed_items: "list[tuple[EnqueueArgs, Exception]]",
    ) -> None:
        self.failed_items = failed_items
        super().__init__(
            f"SubEnqueueError: {len(failed_items)} sub-job(s) failed to enqueue after parent commit"
        )

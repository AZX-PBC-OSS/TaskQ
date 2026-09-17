"""Exception hierarchy for TaskQ.

Mirrors: control-flow exceptions like Snooze and
RetryAfter are not errors — they are signals the consumer translates into
state transitions.
"""

from datetime import timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from taskq._scope import Scope

if TYPE_CHECKING:
    from taskq._validation import validate_actor_payload as validate_actor_payload
    from taskq.backend._protocol import EnqueueArgs, JobId, JobRow, JobStatus


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

    hint = (
        "Inside an actor body, prefer `raise Snooze(delay)` over letting this "
        "reach the retry classifier: a full queue is backpressure, not a "
        "failure, and a Snooze defers without spending the job's retry budget."
    )

    def __init__(self, actor: str, current_count: int, max_pending: int) -> None:
        self.current_count = current_count
        super().__init__(actor, pending=current_count, max_pending=max_pending)


class MaxPendingLockTimeoutError(BackpressureError):
    """Raised when the advisory-lock wait bounding a capped actor's exact
    count-then-insert exceeded its budget.

    Distinct from :class:`MaxPendingExceededError`: the cap was never
    observed -- this enqueue lost the race to even run the check, waiting
    behind other producers on the same ``(schema, actor)`` lock until the
    budget expired. The caller's correct response is the same as for a cap
    rejection (retry later, or shed load), so this is raised from the same
    :class:`BackpressureError` family and recorded against the same
    ``taskq.backpressure.errors`` counter (``kind="max_pending_lock_timeout"``).

    ``timeout_ms`` is the budget that expired. ``pending`` is 0 and
    ``max_pending`` is ``None`` -- no count was taken.

    Deliberately NOT a subclass of :class:`MaxPendingExceededError`: that
    class means "the cap is full" and carries the observed count; a lock
    timeout means "too contended to check" and conflating the two would
    mislead handlers that react to a full queue (e.g. by logging the
    count). Catch :class:`BackpressureError` to treat both the same way.
    """

    def __init__(self, actor: str, timeout_ms: float) -> None:
        self.timeout_ms = timeout_ms
        # BackpressureError.__init__ stamps actor/pending/max_pending and a
        # generic message; args is re-set afterwards so the message names
        # the actual condition (a bounded-wait loss, not a cap rejection).
        super().__init__(actor, pending=0, max_pending=None)
        self.args = (
            f"backpressure: enqueue for actor {actor!r} could not acquire the "
            f"max_pending advisory lock within {timeout_ms:g} ms of contention "
            "(the exact cap check was not reached). Retry later or shed load, "
            "exactly as for MaxPendingExceededError.",
        )


class UniqueForLockTimeoutError(TaskQError):
    """Raised when the advisory-lock wait bounding a ``unique_for``
    enqueue's preflight-then-insert exceeded its budget.

    Distinct from :class:`MaxPendingLockTimeoutError` on purpose. That
    error is a :class:`BackpressureError`: the caller's own load filled
    the contention scope (every producer of a capped actor), the cap
    check never ran, and the correct response is the same as for a cap
    rejection — retry later or shed load. This error means the DEDUP
    ANSWER for one ``(schema, actor, identity_key)`` could not be
    determined in time: the contention scope is a single logical
    entity's identity (a same-key stampede, or a black-holed holder the
    server has not yet reaped), nothing about capacity is wrong, and the
    correct response is to RETRY THE SAME ENQUEUE — by then the winner's
    row is typically committed and the preflight returns it as a dedup
    hit, which is the very outcome the wait existed to produce.
    Deliberately NOT a :class:`BackpressureError` so handlers that react
    to backpressure by shedding load or logging queue counts cannot
    misreact, and deliberately NOT recorded against the
    ``taskq.backpressure.errors`` counter (identity-key contention is
    not a capacity signal; the ``unique-for-lock-timeout`` log event
    carries the observability instead).

    ``identity_key`` names the contended entity. ``timeout_ms`` is the
    budget that expired. This enqueue wrote nothing: on a pool-owned
    transaction the loser's transaction rolled back before any write; on
    a caller-owned transaction (``enqueue_with_conn`` /
    ``TaskQ.with_conn``) the savepoint the bounded acquire used rolled
    back, the transaction remains usable, and durability of anything the
    CALLER wrote alongside is the caller's decision, not this error's
    claim to make.
    """

    def __init__(self, actor: str, identity_key: str, timeout_ms: float) -> None:
        self.actor = actor
        self.identity_key = identity_key
        self.timeout_ms = timeout_ms
        super().__init__(
            f"unique_for enqueue for actor {actor!r} identity_key {identity_key!r} "
            f"could not acquire the single-flight advisory lock within {timeout_ms:g} ms "
            "of contention, so the dedup check did not run and nothing was inserted. "
            "Retry the same enqueue: once the holder's row is visible, the retry "
            "typically dedupes against it."
        )


class IdempotencyKeyLockTimeoutError(TaskQError):
    """Raised when the bounded wait for an idempotency token insert
    exceeded its budget.

    The third member of the enqueue serialization family, after
    :class:`MaxPendingLockTimeoutError` (capacity) and
    :class:`UniqueForLockTimeoutError` (single-flight identity): the
    speculative ``ON CONFLICT (idempotency_scope, idempotency_key) DO
    NOTHING`` token INSERT blocks on another transaction's UNCOMMITTED
    same-pair row — Postgres must wait for that transaction's uniqueness
    verdict — and on a transactional consumer the holder IS the actor's
    own open transaction, whose runtime is unbounded by default
    (``default_start_to_close`` = None). The wait is bounded by a
    ``lock_timeout`` scoped to the INSERT's savepoint; on expiry the
    DEDUP ANSWER for that one ``(idempotency_scope, idempotency_key)``
    pair could not be determined in time, which is
    :class:`UniqueForLockTimeoutError`'s exact situation and therefore
    takes its treatment: retry the same enqueue — once the holder's
    transaction resolves, the retry either dedupes against the committed
    token or inserts fresh. Deliberately NOT a
    :class:`BackpressureError` (nothing about capacity is wrong) and
    deliberately NOT recorded against ``taskq.backpressure.errors``;
    the ``idempotency-lock-timeout`` log event carries the
    observability instead.

    This enqueue wrote nothing: the savepoint that carried the GUC and
    the INSERT rolled back first, so the caller's transaction remains
    usable — the same caller-owned-transaction discipline the singleton
    collision's savepoint established.
    """

    def __init__(
        self,
        actor: str | None,
        idempotency_key: str,
        timeout_ms: float,
        *,
        idempotency_scope: str | None = None,
    ) -> None:
        self.actor = actor
        self.idempotency_key = idempotency_key
        self.idempotency_scope = idempotency_scope
        self.timeout_ms = timeout_ms
        super().__init__(
            f"enqueue for actor {actor!r} idempotency_key {idempotency_key!r} "
            f"(scope {idempotency_scope!r}) could not resolve the speculative token "
            f"insert within {timeout_ms:g} ms — another transaction holds an "
            "uncommitted same-pair token (on a transactional consumer, the actor's "
            "own open transaction). Nothing was inserted; retry the same enqueue — "
            "once the holder's transaction resolves, the retry dedupes or inserts."
        )


class IdempotencyKeyActorMismatchError(TaskQError):
    """An idempotency hit resolved to a job of a DIFFERENT actor.

    Uniqueness is ``(idempotency_scope, idempotency_key)`` — schema-wide, so
    two actors sharing a key collide. A same-actor hit is a dedup and returns
    the existing row; a cross-actor hit cannot be one — the caller asked for
    THIS actor's job and would receive a handle whose result is another
    actor's, indistinguishable from a successful dedup — so it is refused.
    The composite index here cannot include the actor without a migration,
    so the hit is checked after the fact and refused instead of silently
    resolved.

    Nothing was inserted (single enqueue: the arbiter skipped the row; batch:
    the whole batch is rolled back, all-or-nothing like a singleton
    collision; batch fast: the COPY aborts the whole batch the same way).
    ``existing_job_id`` is ``None`` only on the batch-fast tier, when the
    holder was an earlier item of the same COPY batch — its row never
    persisted, so there is no id to name. Namespace keys per actor
    (``"send_receipt:order_123"``) or give the two actors different
    ``idempotency_scope`` values.
    """

    def __init__(
        self,
        *,
        actor: str,
        existing_actor: str,
        existing_job_id: UUID | None,
        idempotency_key: str,
        idempotency_scope: str | None,
    ) -> None:
        self.actor = actor
        self.existing_actor = existing_actor
        self.existing_job_id = existing_job_id
        self.idempotency_key = idempotency_key
        self.idempotency_scope = idempotency_scope
        matched = (
            f"job {existing_job_id} of actor {existing_actor!r}"
            if existing_job_id is not None
            else f"an item of the same batch for actor {existing_actor!r}"
        )
        super().__init__(
            f"enqueue for actor {actor!r} with idempotency_key {idempotency_key!r} "
            f"(scope {idempotency_scope!r}) matched {matched}: keys are unique per "
            "scope across actors, and a hit on another actor's job is not a dedup of "
            "this one. Nothing was enqueued. Namespace the key per actor or use a "
            "different idempotency_scope."
        )


class BatchMaxPendingExceededError(BackpressureError):
    """A bulk enqueue partitioned its admission per actor and refused some.

    Raised by :meth:`~taskq.backend._protocol.Backend.enqueue_batch` /
    :meth:`~taskq.backend._protocol.Backend.enqueue_batch_fast` (and every
    client path riding them) when one or more actors' items exceed their
    effective ``max_pending``: the within-cap actors' items are inserted
    FIRST, then this error raises naming the refusals — the bulk-tier
    sibling of :class:`PartialBatchError`, which is the house shape for
    partial batch admission (succeeded count + failed indices + typed
    per-failure exceptions).

    An over-cap actor's items are refused as a whole group, never
    partially filled up to the cap: the single-enqueue path refuses a
    capped enqueue outright, and a partial fill would admit an arbitrary
    prefix of the caller's items that the caller never chose.

    Fields:

    - ``refusals`` — one :class:`MaxPendingExceededError` per over-cap
      actor (``actor``, ``current_count`` at the admission check, the
      effective ``max_pending``).
    - ``refused_indices`` — actor name -> indices into the caller's items
      list of that actor's refused items. For
      :meth:`~taskq.client.JobsClient.enqueue_batch_streaming`'s chunked
      path the indices are stream-global and the stream stops at the
      refusing chunk: items after it were never attempted.
    - ``admitted_count`` — how many items were admitted and inserted by
      the raising call.

    Durability of the admitted items depends on the path: committed when
    the call owned its transaction (no caller-supplied connection — one
    pool transaction per call/chunk); inserted-but-uncommitted on a
    caller-supplied connection with an open transaction, where that
    transaction's commit/rollback decides. ``enqueue_batch`` /
    ``enqueue_batch_streaming`` with ``failure_policy`` or ``finalizer``
    and no connection (the atomic path) never raises this error — its
    single transaction keeps the legacy all-or-nothing contract and
    raises plain :class:`MaxPendingExceededError` with nothing committed.

    Deliberately NOT a :class:`MaxPendingExceededError` subclass: handlers
    written for the pre-partition contract assume that a raised
    ``MaxPendingExceededError`` left nothing enqueued. Under this error
    part of the batch IS stored — a blind whole-batch retry would
    duplicate the admitted items. Catch this type explicitly and retry
    only the refused indices, or give items ``idempotency_key``s so a
    whole-batch retry deduplicates against the admitted rows.

    The same hazard reaches handlers written against the shared base:
    ``except BackpressureError`` catches this error too, and the
    base's contract ("the caller decides whether to retry, fail, or
    wait") predates partial admission. A generic backpressure handler
    that retries the whole batch MUST first consult
    ``admitted_count`` / ``refused_indices`` — retry only the refused
    items, or rely on ``idempotency_key``s — otherwise it duplicates
    the admitted items on every retry.
    """

    def __init__(
        self,
        *,
        refusals: list[MaxPendingExceededError],
        refused_indices: dict[str, list[int]],
        admitted_count: int,
    ) -> None:
        self.refusals = refusals
        self.refused_indices = refused_indices
        self.admitted_count = admitted_count
        # Why attribute mirrors of the first refusal: BackpressureError
        # consumers (metrics, generic handlers) read .actor/.pending/
        # .max_pending; multi-actor callers should read .refusals.
        self.actor = refusals[0].actor if refusals else ""
        self.pending = refusals[0].current_count if refusals else 0
        self.max_pending = refusals[0].max_pending if refusals else None
        detail = "; ".join(
            f"{r.actor} (pending={r.current_count}, max_pending={r.max_pending}) "
            f"refused at item indices {refused_indices.get(r.actor, [])}"
            for r in refusals
        )
        # Why the skipped super().__init__: BackpressureError builds a
        # single-actor message; this error's facts are multi-actor, so the
        # message is composed here and set through the grandparent.
        super(BackpressureError, self).__init__(
            f"BatchMaxPendingExceededError: {admitted_count} items admitted; {detail}"
        )


class PayloadValidationError(TaskQError):
    """Pydantic validation failed at enqueue or dispatch.

    At enqueue: raised before the row is inserted ('fail at the door').
    At dispatch: causes the job to transition to 'failed' with
    error_class='PayloadValidationError'. Non-retryable in both cases
    regardless of the actor's retry policy.

    ``item_index`` is the failing item's position in the CALLER's
    coordinate space (the batch list / the streaming caller's stream)
    whenever the raise site knows one — batch per-item guards and their
    remapping boundaries populate it, so a handler or retry tool reads
    the position as a field instead of parsing the message. ``None``
    is the no-coordinate case: single-item enqueue, dispatch-time
    validation, non-itemized configuration errors. The message embeds
    the same index for humans; the field is the machine-readable copy.
    """

    def __init__(
        self,
        detail: str,
        *,
        actor: str | None = None,
        payload_schema_ver: str | None = None,
        validation_errors: list[dict[str, object]] | None = None,
        item_index: int | None = None,
    ) -> None:
        self.actor = actor
        self.payload_schema_ver = payload_schema_ver
        self.validation_errors: list[dict[str, object]] = validation_errors or []
        self.item_index = item_index
        super().__init__(detail)


def __getattr__(name: str) -> object:
    """Lazy runtime re-export of ``validate_actor_payload``.

    The implementation lives in :mod:`taskq._validation` (the sanitized
    variant: ``include_url=False, include_input=False`` and no raw-payload
    embedding). It cannot be re-exported at module level here because
    ``taskq._validation`` imports ``PayloadValidationError`` from this
    module — a module-level ``from taskq._validation import ...`` would be
    circular and blow up whenever ``taskq._validation`` is imported first.
    PEP 562 module ``__getattr__`` resolves the name only when requested,
    by which time both modules are fully initialized.
    """
    if name == "validate_actor_payload":
        from taskq._validation import validate_actor_payload

        return validate_actor_payload
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class ResultTooLarge(TaskQError):
    """Terminal result exceeded ``WorkerSettings.result_max_bytes``.

    Non-retryable: the actor already ran to completion and a re-run returns
    the same oversized value, so retrying only burns the remaining attempts
    (re-running the actor's side effects each time) before the job fails
    anyway. Classified alongside ``PayloadValidationError`` in
    :meth:`taskq.retry.RetryClassifier.classify`.
    """


class UnencodableValue(TypeError):
    """A value no UTF-8 JSON encoding accepts, so no PG ``text``/``jsonb``
    form of it exists.

    The canonical case is a lone surrogate (``"\\udcff"`` — exactly what
    ``os.fsdecode`` of a non-UTF-8 filename byte yields): a legal Python
    ``str`` that orjson refuses to encode. Non-``str`` dict keys and
    objects the fallback cannot convert raise the same class. Raised by
    :func:`taskq._json.dumps` — the single serialization boundary — so
    every producer sees one class; subclassing :class:`TypeError` keeps
    the historical orjson contract every ``except TypeError`` caller and
    wording pin already relies on.

    Non-retryable wherever the producer already ran (an actor result): a
    re-run reproduces the same unencodable value, so retrying only burns
    the remaining attempts — the exact burn :class:`ResultTooLarge`
    exists to prevent. Classified alongside it in
    :meth:`taskq.retry.RetryClassifier.classify`. The durable-write
    boundary (:func:`taskq._json.dumps_jsonb_str`) instead escapes the
    unencodable codepoints, mirroring :func:`taskq._json.sanitize_nul_str`:
    rejecting there would strand the very work the value describes.
    """


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


class RateLimitDependencyUnavailable(RuntimeError):
    """A rate limiter's PG store was never wired — no pool was injected.

    Raised by every ratelimit PG delegate's no-pool branch (the token
    bucket's acquire/peek/reset/refund and both sliding-window styles)
    when the delegate is reached with ``pg_pool=None``: most often the
    Redis→PG fallback funnelling into a fallback pool the caller never
    injected, but a directly PG-backed limiter with no pool is the same
    condition. The store dependency cannot answer, so the acquire
    boundary's correct response is the limiter's fail-closed denial —
    :data:`taskq.worker._consumer._RATE_LIMIT_DEPENDENCY_EXCEPTIONS`
    includes this class for exactly that; an escapee would instead be
    misattributed to the job as a failure (a retry attempt burnt and a
    wiring gap persisted as the job's ``error_class``).

    Deliberately NOT a :class:`TaskQError`: subclassing
    :class:`RuntimeError` keeps the historical contract every existing
    caller and wording pin relies on — the ``except RuntimeError`` /
    ``pytest.raises(RuntimeError, match="pg_pool not injected...")`` pins
    and the chaos tier's fail-loud ``pytest.raises(RuntimeError)`` all
    hold unchanged (the same builtin-base precedent as
    :class:`UnencodableValue`).
    """


class IllegalStateTransition(TaskQError):
    """Attempted to transition a job to a status not reachable from its current status.

    Best-effort fast-path check only; the SQL WHERE clause is the
    authoritative serialization gate for concurrent writes.
    """

    def __init__(
        self,
        job_id: "JobId",
        from_status: "JobStatus",
        to_status: "JobStatus",
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
    """One actor whose registered *structural* config differs from the stored row.

    Only ``metadata`` is structural — no operator surface can move it, so a
    mismatch there is always a correctness bug and refuses boot. The queue
    assignment is operator-owned once a row exists (moved by
    ``taskq actor-config move-queue``): a differing literal never raises,
    it logs ``actor-config-queue-override`` at boot and the stored
    assignment wins. Capacity fields (``max_concurrent``,
    ``max_pending``, ``result_ttl``) are likewise operator-owned and never
    raise this error; see :func:`taskq.worker.startup.sync_actor_config`.
    """

    hint = _ACTOR_CONFIG_DRIFT_HINT

    def __init__(
        self,
        actor: str,
        field: Literal["metadata"],
        registered: dict[str, object] | None,
        stored: dict[str, object] | None,
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


class ActorDeregistrationError(TaskQError):
    """Base for actor deregistration refusals."""

    def __init__(self, actor: str, detail: str) -> None:
        self.actor = actor
        super().__init__(f"Cannot deregister actor {actor!r}: {detail}")


class ActorHasActiveJobsError(ActorDeregistrationError):
    """Non-terminal jobs reference the actor.

    Carries the count and per-status breakdown of the blocking jobs so the
    caller can decide whether to cancel them first or use ``force=True``.

    When *force* is ``True``, the message reflects that running jobs
    cannot be cancelled by ``force=True`` — the caller must wait for
    them to finish or cancel them individually first.
    """

    def __init__(
        self,
        actor: str,
        active_count: int,
        status_counts: dict[str, int],
        *,
        force: bool = False,
    ) -> None:
        self.active_count = active_count
        self.status_counts = status_counts
        if force:
            detail = (
                f"{active_count} running job(s) still reference this actor"
                f" (breakdown: {status_counts}). Running jobs cannot be"
                f" cancelled by force=True \u2014 wait for them to finish or"
                f" cancel them individually first."
            )
        else:
            detail = (
                f"{active_count} non-terminal job(s) still reference this actor"
                f" (breakdown: {status_counts}). Cancel them first or pass"
                f" force=True to cancel pending/scheduled jobs automatically."
            )
        super().__init__(actor, detail)


class ActorHasEnabledSchedulesError(ActorDeregistrationError):
    """Enabled cron schedules reference the actor.

    Carries the schedule IDs so the caller can disable or delete them first.
    """

    def __init__(
        self,
        actor: str,
        schedule_ids: list[str],
    ) -> None:
        self.schedule_ids = schedule_ids
        detail = (
            f"{len(schedule_ids)} enabled cron schedule(s) reference this actor"
            f" (ids: {schedule_ids}). Disable or delete them first or pass"
            f" force=True to disable them automatically."
        )
        super().__init__(actor, detail)


class ActorNotFoundError(ActorDeregistrationError):
    """The actor_config row does not exist — nothing to deregister.

    Currently raised only by :func:`deregister_actor`. Other ops
    (``get``, ``set_capacity``) return ``None`` for missing rows.
    """

    def __init__(self, actor: str) -> None:
        super().__init__(actor, "no stored actor_config row for this actor")


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
            f"TaskQ schema {schema!r} is missing or not migrated. "
            "Run `taskq migrate up` from a pre-deploy job or init container to "
            "create/update it. Workers never self-migrate: "
            "TASKQ_MIGRATE_ON_START is read only by `taskq ui serve`."
        )


class EmptyFilterError(TaskQError):
    """Raised when cancel_where is called with a filter that has no predicates.

    A filter with no queue, status, actor, identity_key, batch_id, tags, or
    active predicate would match every job in the table — almost certainly
    a bug. The guardrail is intentionally loud: the caller must add at least
    one predicate or explicitly bypass with ``allow_empty_filter=True``.
    """

    def __init__(self) -> None:
        super().__init__(
            "cancel_where requires at least one filter predicate "
            "(queue, status, actor, identity_key, batch_id, tags, or active); "
            "an empty filter would cancel the entire table. "
            "Pass allow_empty_filter=True to override this guardrail."
        )


class ScopedIdempotencyMigrationPendingError(TaskQError):
    """``idempotency_scope`` was used, but the schema has not yet had
    ``01.00.03_01_post_idempotency_scope_drop_old_index.sql`` applied.

    Between applying ``01.00.03_01_pre_idempotency_scope.sql`` and its
    ``post`` counterpart (the rolling-deploy window every worker's schema
    passes through), BOTH the old global ``jobs_idempotency_key_uniq``
    index (on ``idempotency_key`` alone) and the new composite
    ``jobs_idempotency_scope_key_uniq`` index (on ``(idempotency_scope,
    idempotency_key)``) exist simultaneously — this is deliberate, see the
    "PHASE OBLIGATIONS" comment in the pre migration file, and is what
    keeps pre-this-release code's unscoped ``ON CONFLICT (idempotency_key)``
    working unmodified during the window.

    The cost of that safety: enqueuing the same ``idempotency_key`` under
    two *different* ``idempotency_scope`` values satisfies the new
    composite index's ``ON CONFLICT`` target (no conflict there — the
    ``(scope, key)`` pair is new) but still violates the still-present old
    global index, which is not covered by that ``ON CONFLICT`` target.
    PostgreSQL raises ``UniqueViolationError`` for a conflict against a
    non-arbiter unique index unconditionally — the library deliberately
    does NOT catch that and silently fall back to a different scope's row,
    because doing so would return the *wrong* job for the scope the caller
    actually asked for, silently, which is a worse failure mode than a
    loud, explicit error for a purely transitional migration-window
    condition. Raised instead of letting the raw
    ``asyncpg.UniqueViolationError`` propagate.

    Any call — scoped or unscoped — is affected whenever its
    ``idempotency_key`` already exists under a *different* scope: an
    unscoped call that reuses a key first written under a non-default
    scope raises this error just as a scoped call reusing an unscoped
    key does (verified against live PostgreSQL). Only brand-new keys and
    same-scope repeats are unaffected — a repeated key under the *same*
    scope (including two unscoped calls, which share the default ``''``
    scope) conflicts identically against both indexes for the exact same
    row, which ``ON CONFLICT DO NOTHING`` on the composite index resolves
    cleanly.

    Resolution: confirm every worker is running the release that shipped
    ``idempotency_scope``, then apply
    ``taskq migrate up --phase post`` (or a plain ``taskq migrate up``) to
    drop the old index and activate scoped dedupe — or avoid passing
    ``idempotency_scope`` until that migration has run.
    """

    def __init__(
        self,
        *,
        actor: str | None = None,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        detail: str | None = None,
    ) -> None:
        self.actor = actor
        self.idempotency_key = idempotency_key
        self.idempotency_scope = idempotency_scope
        self.detail = detail
        if idempotency_key is not None:
            what = (
                f"idempotency_key={idempotency_key!r} is already enqueued under a "
                "different idempotency_scope (this call: "
            )
            if actor is not None:
                what += f"actor={actor!r}, "
            what += f"idempotency_scope={idempotency_scope!r})"
        else:
            what = (
                "one or more items in the batch reuse an idempotency_key that "
                "already exists under a different idempotency_scope"
            )
        message = (
            f"enqueue rejected: {what}. This schema has not yet had "
            "01.00.03_01_post_idempotency_scope_drop_old_index.sql applied, so the "
            "legacy global jobs_idempotency_key_uniq index still enforces "
            "idempotency_key uniqueness across ALL scopes, and cross-scope key reuse "
            "is rejected rather than silently deduped against the wrong scope's job. "
            "No row was inserted or modified. To resolve: confirm every worker is on "
            "this release, then run `taskq migrate up --phase post` to activate "
            "scoped dedupe. Until then, do not reuse an idempotency_key under more "
            "than one scope (including the default '' scope) -- this fires in either "
            "direction, scoped-then-unscoped included."
        )
        if detail is not None:
            message = f"{message} (postgres detail: {detail})"
        super().__init__(message)


class DuplicateIdempotencyKeyError(TaskQError):
    """``enqueue_batch_fast`` aborted: an item's
    ``(idempotency_scope, idempotency_key)`` pair is already enqueued by the
    SAME actor. A pair spanning two actors raises
    :class:`IdempotencyKeyActorMismatchError` instead, the same refusal the
    single and batch tiers apply.

    COPY has no ``ON CONFLICT`` arbiter, so a same-pair duplicate —
    repeated within the batch or raced against a row the composite
    ``jobs_idempotency_scope_key_uniq`` index already covers — aborts the
    ENTIRE batch before a single row is written (all-or-nothing; the
    abort is deliberate bulk-import semantics, unchanged by the
    classification this error introduced). The non-fast paths never
    raise for this condition: their ``ON CONFLICT`` arbiter dedupes and
    RETURNS the existing row, so a typed domain error for a
    deduplication-constraint violation on the enqueue path does not exist
    there — hence this class, expressing the same idempotency constraint
    at the bulk-import boundary in both the SQL and in-memory backends.
    Distinct from :class:`ScopedIdempotencyMigrationPendingError`, which is
    the rolling-deploy window's cross-scope reuse signal.

    ``idempotency_key`` / ``idempotency_scope`` carry the offending pair,
    resolved exactly on both backends by one shared rule
    (``first_duplicate_idempotency_pair``): the first item in batch order
    whose pair repeats an earlier item or is already stored. The PG path
    resolves the stored half with a targeted post-abort lookup inside the
    caller's transaction scope — never by parsing the violation's detail
    text, which renders values raw and unquoted (ambiguous under
    positional reading for comma-bearing scopes, unusable when localized
    or truncated). Both fields are ``None`` only when the conflicting
    row could not be resolved at all — a committed-and-instantly-deleted
    racer — where the detail-text match is the last word and still never
    guesses. ``detail`` carries the postgres detail verbatim when
    present.

    Resolution: pre-deduplicate the items, or use
    :meth:`~taskq.client.JobsClient.enqueue_batch`, which dedupes and
    returns the existing rows.
    """

    def __init__(
        self,
        *,
        idempotency_key: str | None = None,
        idempotency_scope: str | None = None,
        detail: str | None = None,
    ) -> None:
        self.idempotency_key = idempotency_key
        self.idempotency_scope = idempotency_scope
        self.detail = detail
        message = (
            "enqueue_batch_fast rejected: an item's (idempotency_scope, "
            "idempotency_key) pair is already enqueued (duplicate within "
            "the batch or already stored). COPY has no ON CONFLICT arbiter, "
            "so the entire batch aborted with nothing written. "
            "Pre-deduplicate the items or use enqueue_batch, which dedupes "
            "and returns the existing rows."
        )
        if idempotency_key is not None:
            message += (
                f" Offending pair: idempotency_scope={idempotency_scope!r}, "
                f"idempotency_key={idempotency_key!r}."
            )
        if detail is not None:
            message += f" (postgres detail: {detail})"
        super().__init__(message)


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


class BatchAbortedError(TaskQError):
    """A batch was aborted because consecutive failures exceeded the threshold.

    Running jobs are NOT cancelled by the abort — only pending and
    scheduled jobs are cancelled.  Running jobs continue to completion.
    This matches the post-terminal-write hook design: the hook runs after
    the terminal write, so a job that was dispatched before the abort
    triggered will run to completion.
    """

    def __init__(self, batch_id: UUID, consecutive_failures: int, threshold: int | None) -> None:
        self.batch_id = batch_id
        self.consecutive_failures = consecutive_failures
        self.threshold = threshold
        displayed_threshold = threshold if threshold is not None else 0
        super().__init__(
            f"batch {batch_id} aborted after {consecutive_failures} consecutive failures "
            f"(threshold={displayed_threshold})"
        )


class EmptyBatchError(TaskQError):
    """A batch has fewer jobs than the expected minimum.

    This can happen when jobs were pruned before ``wait_for_batch`` ran,
    or when ``expected_size`` was set but jobs were never created.  Pass
    ``on_empty="ok"`` to ``wait_for_batch`` to suppress the no-batch-row
    variant of this error.
    """

    def __init__(self, batch_id: UUID, expected: int, actual: int) -> None:
        self.batch_id = batch_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"batch {batch_id} has {actual} jobs, expected at least {expected}"
            + (
                ' — jobs may have been pruned; pass on_empty="ok" to suppress'
                " the no-batch-row variant"
                if actual == 0
                else ""
            )
        )


class BatchIdExistsError(TaskQError):
    """A caller-supplied ``batch_id`` already exists in the ``batches`` table.

    Raised when :meth:`~taskq.client.JobsClient.enqueue_batch` or
    :meth:`~taskq.client.JobsClient.enqueue_batch_streaming` is called with
    an explicit ``batch_id`` that collides with an existing batch row.
    The original ``asyncpg.UniqueViolationError`` (PG) is chained via
    ``__cause__`` when available.
    """

    def __init__(self, batch_id: UUID) -> None:
        self.batch_id = batch_id
        super().__init__(
            f"batch_id {batch_id} already exists; use a different batch_id "
            f"or omit it to auto-generate one"
        )

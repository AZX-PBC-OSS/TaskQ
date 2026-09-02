"""Backend protocol, data carriers, and protocol version constant.

Defines the :class:`Backend` protocol that both :class:`PostgresBackend`
(production) and :class:`InMemoryBackend` (tests) must satisfy, along with
the frozen dataclass carriers that cross the protocol boundary.

This submodule exists so that concrete backend implementations (e.g.
``taskq.backend.postgres``) can import the protocol and carriers without
creating a circular dependency through the re-export boundary in
``taskq.backend.__init__``.
"""

import asyncio
import re
from collections.abc import Iterable, Sequence
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, IntEnum
from typing import (
    TYPE_CHECKING,
    Annotated,
    ClassVar,
    Final,
    Literal,
    NewType,
    Protocol,
    cast,
    get_args,
    runtime_checkable,
)
from uuid import UUID

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type ConnLike = asyncpg.Connection | PoolConnectionProxy  # pyright: ignore[reportUnusedImport]  # Why: PoolConnectionProxy is only used in the type alias; pyright may not see it

else:
    type ConnLike = object  # pyright: ignore[reportInvalidTypeForm]  # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

from pydantic import AfterValidator, BaseModel, ConfigDict

__all__ = [
    "BACKEND_PROTOCOL_VERSION",
    "JOB_STATUS_VALUES",
    "AttemptOutcome",
    "AttemptRow",
    "Backend",
    "BackendDeps",
    "BatchCounts",
    "BatchFilter",
    "BatchRow",
    "BatchStatus",
    "BulkCancelResult",
    "CancelFlag",
    "CancelPhase",
    "DstStrategy",
    "EnqueueArgs",
    "ErrorInfo",
    "EventRow",
    "IdempotencyKey",
    "IdentityKey",
    "JobFilter",
    "JobId",
    "JobPage",
    "JobRow",
    "JobSortField",
    "JobStatus",
    "LongRunningJobEventsWriter",
    "QueueMode",
    "QueueName",
    "RateLimitBackend",
    "RetryKind",
    "ScheduleCreateArgs",
    "ScheduleRecord",
    "ScheduleUpdateArgs",
    "parse_batch_status",
    "parse_cancel_phase",
    "parse_retry_kind",
]

# ── Protocol version ───────────────────────────────────────────────────
# Bump rule: increment when a change alters an existing protocol member's
# observable contract such that an implementation written against the
# previous version would *silently* misbehave (wrong results, ignored
# inputs) instead of failing loudly.  Purely additive changes an old
# implementation can ignore without producing incorrect behaviour do not
# require a bump.  See docs/architecture.md §Backend protocol.
# v3 (unreleased; folds in every protocol change since the last shipped
#     release): list_jobs — JobFilter.status widened to accept a sequence
#     and the `active` meta-filter was added; a v2 implementation returns
#     wrong rows for both shapes without erroring. get_actor_max_pending
#     added (required) — a v2 implementation lacks the method, and the
#     client capacity cache's fail-open would otherwise swallow the
#     AttributeError and silently enforce code literals forever.
#     mark_succeeded / mark_succeeded_with_conn gained the
#     `fallback_result_ttl` keyword — without it a v2 implementation
#     keeps the enqueue-pinned result_expires_at when the stored
#     result_ttl is cleared, silently expiring results at completion.
#     EnqueueArgs.scheduled_at is now optional — None means immediate
#     and the backend's server stamps/decides it. A v2-era implementation
#     fails LOUDLY on None ('>' not supported between NoneType and
#     datetime at its scheduled_at > now checks) rather than silently
#     misbehaving, so per the bump rule above this is a documented
#     no-bump incompatibility.
#     mark_failed_or_retry's next_scheduled_at (datetime | None) is
#     replaced by retry_delay (timedelta | None) — the backend derives
#     scheduled_at, the scheduled/pending status, AND the
#     schedule_to_close deadline outcome from its own clock (single
#     arbiter); a v2-era implementation binding a datetime into the
#     interval slot fails loudly at the driver instead of silently
#     misbehaving.
#     The vestigial `now` parameters are REMOVED from
#     scheduled_to_pending / deadline_sweep / reclaim_expired_locks and
#     the PostgresBackend.sweep_* statics — PG ignored them (the server
#     clock is the arbiter); an implementation still declaring them fails
#     loudly with TypeError on the call.
BACKEND_PROTOCOL_VERSION: Final[int] = 3

# ── Type aliases (PEP 695) ─────────────────────────────────────────────

type JobStatus = Literal[
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]

# PEP-695 ``type`` aliases are ``TypeAliasType`` objects; ``get_args``
# returns ``()`` on the alias itself — unwrap via ``__value__`` to reach
# the ``Literal[...]`` and enumerate its members at runtime.
JOB_STATUS_VALUES: Final[frozenset[str]] = frozenset(get_args(JobStatus.__value__))
"""Runtime membership set of every :data:`JobStatus` literal value.

Derived from the ``JobStatus`` Literal itself (the canonical declaration)
so validation can never drift from the type.  Used by
:meth:`JobFilter.__post_init__` to reject unknown statuses before they
reach a backend.
"""

type AttemptOutcome = Literal[
    "succeeded",
    "failed",
    "snoozed",
    "cancelled",
    "crashed",
    "scheduled",
    "reservation_denied",
    "rate_limit_denied",
]

type RetryKind = Literal["transient", "indefinite", "non_retryable"]
"""Closed set of retry tiers.

Why ``Literal`` and not an ``Enum``: serialization round-trips through
``model_dump(mode="json")`` produce plain strings without
``use_enum_values`` configuration; pyright exhaustive matching works
identically for either; no ``.value`` access required at call sites.
"""

type QueueMode = Literal["strict_fifo", "round_robin"]

type RateLimitBackend = Literal["redis", "postgres", "memory"]

type DstStrategy = Literal["skip", "firstof", "allof"]

type BatchStatus = Literal["active", "complete", "aborted"]
"""Lifecycle status of a batch row in the ``batches`` table."""


class JobSortField(Enum):
    """Sort ordering for :meth:`Backend.list_jobs` via :attr:`JobFilter.order_by`.

    ``SCHEDULED_AT_ASC`` (and the default ``None``) preserve the canonical
    dispatch-friendly ordering — ``priority DESC, scheduled_at ASC, id ASC`` —
    so existing ``list_jobs`` callers see no behaviour change.

    ``CREATED_AT_DESC`` and ``FINISHED_AT_DESC`` serve "latest run by business
    key" queries: newest-created first and most-recently-finished first
    (``NULLS LAST``) respectively.  Cursor pagination is only valid with the
    default ordering; :meth:`JobFilter.__post_init__` rejects a cursor
    combined with a non-default ``order_by``.
    """

    SCHEDULED_AT_ASC = "scheduled_at_asc"
    CREATED_AT_DESC = "created_at_desc"
    FINISHED_AT_DESC = "finished_at_desc"


class CancelPhase(IntEnum):
    """Phases of cooperative-then-forced cancellation.

    Why ``IntEnum`` and not ``Literal[0, 1, 2]``: the cancel-poll loop
    performs arithmetic comparisons (``db_phase >= 1``,
    ``active.cancel_phase < 2``) that ``Literal[int]`` does not narrow
    correctly under pyright strict. ``IntEnum`` subclasses ``int``, so
    every existing comparison continues to work, while the typed enum
    carries the OTel attribute semantics (``cancel_phase`` attribute on
    transition counters) and prevents bare-int values like ``99`` from
    slipping past the type checker.

    Values ``NONE``, ``COOPERATIVE``, and ``FORCED`` are persistable —
    they map directly to the PG ``cancel_phase`` column whose check
    constraint is ``BETWEEN 0 AND 2``. ``ABANDON_PENDING`` is an
    in-process sentinel only: the cancel-poll loop sets it on
    ``_ActiveJob`` to mark a job as queued for post-transaction
    abandonment. It is never written to PG. Keeping it on the same
    enum lets ``cancel_phase`` stay strongly typed end-to-end.
    """

    NONE = 0
    COOPERATIVE = 1
    FORCED = 2
    ABANDON_PENDING = 3  # in-process sentinel; never persisted to PG


# ── Opaque identifier types ────────────────────────────────────────────

JobId = NewType("JobId", UUID)
"""Opaque job identifier — prevents ``UUID`` mixups across the API."""

IdempotencyKey = NewType("IdempotencyKey", str)
"""Distinguishes idempotency keys from identity keys at call sites."""

IdentityKey = NewType("IdentityKey", str)
"""Distinguishes identity keys from idempotency keys at call sites."""


_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")


def _validate_queue_name(v: str) -> str:
    if not _QUEUE_NAME_RE.match(v):
        raise ValueError(f"invalid queue name: {v!r}")
    return v


_RETRY_KINDS: Final[frozenset[str]] = frozenset({"transient", "indefinite", "non_retryable"})


def parse_retry_kind(value: str) -> RetryKind:
    """Convert an untrusted ``str`` (from a PG row) into :data:`RetryKind`.

    Pyright cannot narrow ``str`` to a ``Literal`` union by membership
    test alone; this helper performs the runtime check and returns a
    statically-typed ``RetryKind``. Raises :class:`ValueError` if the
    value is not one of the three allowed kinds — that signals schema
    drift between PG and Python.
    """
    if value not in _RETRY_KINDS:
        raise ValueError(f"unknown retry_kind from backend row: {value!r}")
    # The membership check above is the runtime guarantee; cast expresses
    # the narrowing to pyright without a bare ignore.
    return cast(RetryKind, value)


def parse_cancel_phase(value: int) -> CancelPhase:
    """Convert an untrusted ``int`` (from a PG row) into :class:`CancelPhase`.

    The PG check constraint ``cancel_phase BETWEEN 0 AND 2`` ensures
    only persistable values reach Python; we reject
    :attr:`CancelPhase.ABANDON_PENDING` (3) explicitly because that
    value is an in-process sentinel and must never appear in a row.
    """
    phase = CancelPhase(value)
    if phase is CancelPhase.ABANDON_PENDING:
        raise ValueError(
            f"cancel_phase {value} is an in-process sentinel; PG must never store it",
        )
    return phase


_BATCH_STATUSES: Final[frozenset[str]] = frozenset({"active", "complete", "aborted"})


def parse_batch_status(value: str) -> BatchStatus:
    """Convert an untrusted ``str`` (from a PG row) into :data:`BatchStatus`.

    Pyright cannot narrow ``str`` to a ``Literal`` union by membership
    test alone; this helper performs the runtime check and returns a
    statically-typed ``BatchStatus``. Raises :class:`ValueError` if the
    value is not one of the three allowed statuses — that signals schema
    drift between PG and Python.
    """
    if value not in _BATCH_STATUSES:
        raise ValueError(f"unknown batch status from backend row: {value!r}")
    return cast(BatchStatus, value)


QueueName = Annotated[str, AfterValidator(_validate_queue_name)]
"""Validator alias for queue names — accepts plain ``str`` literals.

Why ``Annotated`` and not ``NewType``: every studied vendor (river,
dramatiq, arq, procrastinate) uses raw ``str`` + a separate validator
for queue names; no nominal type because no other ``str`` field at any
call site could be confused with ``queue``. ``Annotated`` gives runtime
validation in Pydantic models without forcing every caller to wrap
literals in ``QueueName("default")``.
"""

# ── Data carriers ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class EnqueueArgs:
    """Input struct for :meth:`Backend.enqueue`.  Carries every column the
    caller specifies at enqueue time.  ``scheduled_at=None`` means
    immediate — the backend's server stamps ``now()`` and decides
    ``status``; a non-None value is the caller's explicit absolute intent
    (deprecated cross-domain residue, kept only for explicit scheduling —
    prefer delay/interval forms where available).
    """

    id: JobId
    actor: str
    queue: str
    payload: dict[str, object]
    max_attempts: int
    retry_kind: RetryKind
    scheduled_at: datetime | None
    payload_schema_ver: int = 1
    priority: int = 0
    max_pending: int | None = None
    schedule_to_close: datetime | None = None
    schedule_to_close_interval: timedelta | None = None
    start_to_close: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    identity_key: IdentityKey | None = None
    fairness_key: str | None = None
    idempotency_key: IdempotencyKey | None = None
    idempotency_scope: str = ""
    trace_id: str | None = None
    span_id: str | None = None
    result_ttl: timedelta | None = None
    unique_for: timedelta | None = None
    unique_states: tuple[JobStatus, ...] = ("pending", "scheduled", "running")
    metadata: dict[str, object] = field(default_factory=dict[str, object])
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.schedule_to_close is not None and self.schedule_to_close_interval is not None:
            raise ValueError(
                "schedule_to_close and schedule_to_close_interval are mutually exclusive; "
                "if both are desired, pass only schedule_to_close (datetime) — "
                "the interval form is the actor-declaration default."
            )


@dataclass(frozen=True, slots=True)
class JobRow:
    """Read-model of a ``taskq.jobs`` row.  Every column the dispatch loop,
    heartbeat, and terminal writes need appears as a typed field.
    ``status`` uses a ``Literal`` union (8 values) matching the
    ``job_status`` enum in ``01.00.00_01_pre_initial.sql``.
    """

    id: JobId
    actor: str
    queue: str
    identity_key: IdentityKey | None
    fairness_key: str | None
    payload: dict[str, object]
    payload_schema_ver: int
    status: JobStatus
    priority: int
    attempt: int
    max_attempts: int
    retry_kind: RetryKind
    schedule_to_close: datetime | None
    start_to_close: timedelta | None
    heartbeat_timeout: timedelta | None
    created_at: datetime
    scheduled_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    last_heartbeat_at: datetime | None
    locked_by_worker: UUID | None
    lock_expires_at: datetime | None
    cancel_requested_at: datetime | None
    cancel_phase: CancelPhase
    error_class: str | None
    error_message: str | None
    error_traceback: str | None
    progress_state: dict[str, object]
    progress_seq: int
    result: dict[str, object] | None
    result_size_bytes: int | None
    result_expires_at: datetime | None
    idempotency_key: IdempotencyKey | None
    idempotency_scope: str
    trace_id: str | None
    span_id: str | None
    metadata: dict[str, object]
    tags: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AttemptRow:
    """Read-model of a ``taskq.job_attempts`` row.  ``outcome`` uses
    a ``Literal`` union so pyright catches invalid strings at the protocol
    boundary.
    """

    job_id: JobId
    attempt: int
    started_at: datetime
    finished_at: datetime | None
    outcome: AttemptOutcome
    error_class: str | None
    error_message: str | None
    error_traceback: str | None
    duration_ms: int | None
    worker_id: UUID | None
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class EventRow:
    """Read-model of a ``taskq.job_events`` row.

    Mirrors the ``job_events`` table shape: monotonic ``event_id``,
    the owning job, timestamp, event kind, and a detail payload.
    """

    event_id: int
    job_id: JobId
    occurred_at: datetime
    kind: Literal["state_change", "cancel_request"]
    detail: dict[str, object]


@dataclass(frozen=True, slots=True)
class LongRunningJobEventsWriter:
    """A transaction holding a lock on ``job_events`` for longer than a
    ``poll_reclaim_events`` visibility-delay margin — a candidate cause of
    a silently missed reclaim event (see
    ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``). Diagnostic only:
    reported by ``PostgresBackend.check_reclaim_visibility_delay_risk``,
    not a guarantee that this specific transaction will write to
    ``job_events`` again or actually cause a miss — a proxy signal for an
    operator to investigate, not proof of an incident.

    Not directly ``json.dumps``-safe: ``xact_start`` is a
    :class:`~datetime.datetime`. Serialise with a datetime-aware encoder
    (or ``str()``/``.isoformat()``) in the monitoring loop consuming this.
    """

    pid: int
    xact_start: datetime
    xact_age_seconds: float


@dataclass(frozen=True, slots=True)
class CancelFlag:
    """Carries exactly the two fields returned by the heartbeat cancel-poll
    query: ``job_id`` and ``cancel_phase``.  ``cancel_requested_at``
    is tracked locally by the heartbeat, not read from PG on every poll.
    """

    job_id: JobId
    cancel_phase: CancelPhase


@dataclass(frozen=True, slots=True)
class JobFilter:
    """Filter parameters for :meth:`Backend.list_jobs` and
    :meth:`Backend.cancel_where`.

    For ``cancel_where``, the ``limit``, ``cursor``, and ``order_by``
    fields are ignored — a bulk cancel is not paginated. Use
    :meth:`has_predicates` to check whether the filter has at least one
    predicate before passing it to ``cancel_where``.

    Heads-up: ``active=True`` is **not** Celery's 'active' — Celery's
    means 'currently executing' (``running`` only), TaskQ's means 'not
    yet finished' (``pending`` + ``scheduled`` + ``running``).  Read the
    ``active`` section below before relying on the name.

    ``cursor`` is an opaque keyset-pagination token encoding
    ``(priority, scheduled_at, id)`` from the last row of the previous
    page.  Both backends must agree on cursor encoding and comparison
    semantics.

    ``batch_id`` is a :class:`UUID`. The PG backend converts it to its
    canonical string form at the SQL boundary; the in-memory backend
    compares the UUID directly. Keeping the typed shape here means
    ``JobsClient.list(batch_id=UUID(...))`` flows without an implicit
    ``str(uuid)`` coercion.

    ``status`` accepts either a single :data:`JobStatus` (backwards
    compatible — e.g. ``JobFilter(status="pending")``) or a sequence of
    statuses (e.g. ``JobFilter(status=["pending", "running"])``).
    An empty sequence (``status=[]``) matches no jobs — it is not
    treated as 'no filter'.  Unknown status values raise
    :class:`ValueError` in :meth:`__post_init__`, so untrusted input
    fails identically on both backends instead of surfacing as a PG
    enum-cast error or a silent empty result.
    The PG backend renders a single status as ``status = $n`` and a
    sequence as ``status = ANY($n)``; the in-memory backend performs a
    membership check in both cases.

    ``active`` is a meta-filter that selects statuses by terminality.
    **This is not Celery's 'active'.**  Celery/Flower use 'active' for
    tasks currently executing on a worker (``running`` only); here it
    means 'not yet finished' — a superset that also includes work that
    has not started yet:

    - ``active=True`` → non-terminal statuses (pending, scheduled, running)
    - ``active=False`` → terminal statuses (succeeded, failed, cancelled,
      crashed, abandoned)
    - ``active=None`` (default) → no status-terminality filter

    The non-terminal set is derived from
    :data:`~taskq.backend.statemachine.ACTIVE_STATUSES`, which is itself
    derived from the state machine — adding a new non-terminal state
    updates this filter automatically.

    ``status`` and ``active`` are mutually exclusive; specifying both
    raises :class:`ValueError` in :meth:`__post_init__`.

    Usage examples::

        JobsClient.list(JobFilter(status="pending"))
        JobsClient.list(JobFilter(status=["pending", "running"]))
        JobsClient.list(JobFilter(active=True))
    """

    queue: str | None = None
    status: JobStatus | Sequence[JobStatus] | None = None
    actor: str | None = None
    identity_key: IdentityKey | None = None
    batch_id: UUID | None = None
    limit: int = 100
    cursor: str | None = None
    tags: tuple[str, ...] | None = None
    order_by: JobSortField | None = None
    # Not Celery's 'active' ('currently executing') — True selects every
    # non-terminal status, i.e. 'not yet finished'. See the class docstring.
    active: bool | None = None

    def __post_init__(self) -> None:
        # A negative limit diverges across backends: PG raises
        # "LIMIT must not be negative" while the in-memory slice would
        # silently drop rows.  Reject it here so both fail identically.
        if self.limit < 0:
            raise ValueError(f"limit must be >= 0, got {self.limit}")
        if (
            self.cursor is not None
            and self.order_by is not None
            and self.order_by is not JobSortField.SCHEDULED_AT_ASC
        ):
            raise ValueError(
                "cursor pagination is only supported with the default ordering "
                "(order_by=None or JobSortField.SCHEDULED_AT_ASC); "
                "non-default order_by changes the keyset the cursor encodes"
            )
        if self.status is not None:
            values = (self.status,) if isinstance(self.status, str) else tuple(self.status)
            unknown = [v for v in values if v not in JOB_STATUS_VALUES]
            if unknown:
                raise ValueError(
                    f"unknown job status value(s): {list(dict.fromkeys(unknown))!r}; "
                    f"valid statuses are {sorted(JOB_STATUS_VALUES)}"
                )
        if self.status is not None and self.active is not None:
            raise ValueError(
                "status and active are mutually exclusive; "
                "use status for specific status(es) or active for the "
                "terminal/non-terminal meta-filter"
            )

    def has_predicates(self) -> bool:
        """Return True if at least one filter predicate is set.

        Used by ``JobsClient.cancel_where`` to reject empty filters that
        would match the entire table. New predicate fields added to
        ``JobFilter`` MUST be added here and in
        ``build_filter_conditions`` — the two are kept in sync manually.
        Non-predicate fields (``limit``, ``cursor``, ``order_by``) are
        excluded by design.
        """
        return (
            self.queue is not None
            or self.status is not None
            or self.actor is not None
            or self.identity_key is not None
            or self.batch_id is not None
            or (self.tags is not None and len(self.tags) > 0)
            or self.active is not None
        )


@dataclass(frozen=True, slots=True)
class ScheduleCreateArgs:
    """Input struct for :meth:`Backend.create_schedule`.

    Carries every column the caller specifies at schedule creation time.
    ``next_fire_at`` is computed client-side via
    :func:`~taskq.cron._compute_next_fire_after` — the initial value
    is an approximation; the leader's tick corrects on first fire.
    """

    actor: str
    cron_expr: str
    timezone: str
    next_fire_at: datetime
    dst_strategy: DstStrategy = "skip"
    payload_factory: str | None = None
    enabled: bool = True
    name: str = ""
    identity_key: IdentityKey | None = None
    metadata: dict[str, object] = field(default_factory=dict[str, object])

    def __post_init__(self) -> None:
        from croniter import croniter

        if not croniter.is_valid(self.cron_expr):
            raise ValueError(f"Invalid cron expression: {self.cron_expr!r}")


@dataclass(frozen=True, slots=True)
class ScheduleUpdateArgs:
    """Input struct for :meth:`Backend.update_schedule`.

    Only non-None fields are applied in the UPDATE SET clause.
    When ``enabled`` is True, the UPDATE also resets
    ``consecutive_failures = 0`` and ``last_fire_error = NULL``.
    When ``cron_expr`` is provided, ``next_fire_at`` must also be
    provided (recomputed by the caller via
    :func:`~taskq.cron._compute_next_fire_after`).

    To explicitly clear ``payload_factory`` (set the column to NULL),
    set ``clear_payload_factory=True`` — ``None`` for payload_factory
    means "don't change this field."
    """

    cron_expr: str | None = None
    next_fire_at: datetime | None = None
    enabled: bool | None = None
    payload_factory: str | None = None
    clear_payload_factory: bool = False
    metadata: dict[str, object] | None = None
    consecutive_failures: int | None = None
    last_fire_error: str | None = None

    def __post_init__(self) -> None:
        if self.cron_expr is not None and self.next_fire_at is None:
            raise ValueError(
                "next_fire_at must be provided when cron_expr is changed; "
                "recompute via _compute_next_fire_after"
            )
        if self.clear_payload_factory and self.payload_factory is not None:
            raise ValueError(
                "clear_payload_factory and payload_factory are mutually exclusive; "
                "use clear_payload_factory=True to set the column to NULL, "
                "or payload_factory to assign a new value"
            )


class ScheduleRecord(BaseModel):
    """Read-only snapshot of a cron schedule row from the database.

    ``model_config = ConfigDict(frozen=True)`` enforces immutability per
    public API discipline.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID
    actor: str
    name: str = ""
    cron_expr: str
    timezone: str
    dst_strategy: DstStrategy = "skip"
    payload_factory: str | None
    identity_key: IdentityKey | None = None
    enabled: bool
    last_fired_at: datetime | None
    last_fire_error: str | None
    consecutive_failures: int
    next_fire_at: datetime
    metadata: dict[str, object]


class BulkCancelResult(BaseModel):
    """Structured outcome of a bulk cancellation request.

    Returned by ``JobsClient.cancel_where()`` so callers can inspect
    how many jobs were cancelled directly (pending/scheduled → terminal
    'cancelled') vs how many had cooperative cancel requested (running →
    cancel_phase=1).
    """

    model_config = ConfigDict(frozen=True)

    cancelled_directly: int
    """Count of pending/scheduled jobs moved straight to terminal 'cancelled'."""

    cancel_requested: int
    """Count of running jobs with cancel_phase=1 set (cooperative cancel)."""

    cancelled_ids: tuple[UUID, ...]
    """IDs of jobs cancelled directly (pending/scheduled → cancelled)."""

    cancel_requested_ids: tuple[UUID, ...]
    """IDs of running jobs with cancel requested."""

    @property
    def total_affected(self) -> int:
        """Total jobs affected by the bulk cancel."""
        return self.cancelled_directly + self.cancel_requested


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """Structured error information for terminal writes."""

    error_class: str
    error_message: str
    error_traceback: str | None


@dataclass(frozen=True, slots=True)
class JobPage:
    """Paged result from :meth:`JobsClient.list`.  Defined at the
    protocol layer because cursor encoding is a cross-backend contract.
    ``next_cursor`` is ``None`` when no more rows exist.
    """

    jobs: list[JobRow]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class BatchRow:
    """Read-model of a ``taskq.batches`` row."""

    id: UUID
    queue: str
    status: BatchStatus
    expected_size: int
    consecutive_failures: int
    failure_threshold: int | None
    finalizer_job_id: UUID | None
    originating_actor: str | None
    created_at: datetime
    completed_at: datetime | None
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class BatchCounts:
    """Live job-count aggregate for one batch.

    Mirrors BatchCompletionStatus fields; defined at the protocol layer
    so backends do not import the client-side batch module.
    """

    total: int
    pending: int
    succeeded: int
    failed: int
    cancelled: int
    crashed: int
    abandoned: int


@dataclass(frozen=True, slots=True)
class BatchFilter:
    """Filter parameters for Backend.list_batches.

    Unlike JobFilter, this only carries fields relevant to batch queries:
    queue, active (status terminality), batch_id, and limit. Job-oriented
    fields (status, actor, tags, cursor, order_by, identity_key) are
    intentionally absent — using JobFilter for batch queries would
    silently ignore those fields, which is a type trap.
    """

    queue: str | None = None
    active: bool | None = None
    batch_id: UUID | None = None
    limit: int = 100

    def __post_init__(self) -> None:
        if self.limit < 0:
            raise ValueError(f"limit must be >= 0, got {self.limit}")


# ── Backend deps protocol ───────────────────────────────────────────────
# Worker-layer dependencies consumed by PostgresBackend at construction time.
# Typed as a Protocol (not object) so pyright can verify attribute access
# without union-attr suppresssions — WorkerDeps satisfies this at runtime.


@runtime_checkable
class BackendSettings(Protocol):
    """Narrow settings protocol for PostgresBackend constructor consumption.

    WorkerSettings and _ClientSettings both satisfy this interface.
    """

    schema_name: str
    dispatch_oversample: int


@runtime_checkable
class BackendDeps(Protocol):
    """Protocol satisfied by WorkerDeps — consumed by PostgresBackend.__init__."""

    @property
    def settings(self) -> BackendSettings:
        """Settings object with schema_name and dispatch_oversample."""
        ...

    @property
    def worker_pool(self) -> "asyncpg.Pool":
        """Pool for terminal writes (pg_dsn_pooled)."""
        ...

    @property
    def heartbeat_pool(self) -> "asyncpg.Pool":
        """Pool for heartbeat writes (pg_dsn_direct, command_timeout=2s)."""
        ...

    @property
    def dispatcher_pool(self) -> "asyncpg.Pool | None":
        """Dispatcher pool for session-sensitive operations.

        WorkerDeps provides a non-optional Pool.  Client-side usage
        (``_ClientDeps``) may provide ``None`` when no dispatcher pool
        is needed — the constructor handles this via ``getattr``.
        """
        ...


# ── Backend protocol ───────────────────────────────────────────────────


@runtime_checkable
class Backend(Protocol):
    """Contract that both PostgresBackend and InMemoryBackend satisfy.

    46 async methods plus two sync methods (``subscribe_wake`` and
    ``subscribe_cancel_wake``) (48 methods total) covering enqueue,
    dispatch, heartbeat, terminal writes, attempt history, cancel
    signals, scheduling / sweeps, read, NOTIFY hook, schedule CRUD,
    and batch operations. Method order grouped for review-grep
    ergonomics.

    Why monomorphic (no ``Generic[P, R]``): the backend is the DB
    adapter boundary. Payloads are stored as ``dict[str, object]`` (the
    JSONB ``payload`` column) regardless of the actor's typed payload
    model. Generic parameters here would propagate ``P`` and ``R`` into
    every method (``dispatch_batch``, ``mark_succeeded``, etc.) with no
    safety benefit at the storage layer. The worker consumer
    reconstructs the typed ``JobContext[P]`` at dispatch time using
    ``ActorRef.payload_type.model_validate(row.payload)``.
    """

    BACKEND_PROTOCOL_VERSION: ClassVar[int]

    supports_transactional_simulation: ClassVar[bool] = False
    """Whether this backend simulates transactional sub-enqueue via a
    buffer (True) or relies on real database transactions (False).

    ``PostgresBackend`` returns False — its real PG transaction provides
    the atomicity guarantee directly: sub-job INSERTs run on the open
    LOOP-scope connection and are rolled back along with the parent's
    writes if the actor raises.

    ``InMemoryBackend`` returns True — it has no real transaction
    concept, so ``SubJobEnqueuer`` buffers ``EnqueueArgs`` and flushes on
    actor success / discards on failure. A third-party ``Backend``
    implementation that wants transactional simulation in tests can opt
    in by overriding this to True.
    """

    # ── Enqueue ────────────────────────────────────────────────────────
    async def enqueue(self, args: EnqueueArgs) -> JobRow: ...

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> list[JobRow]:
        """Insert multiple jobs in a single batched operation.

        All items in *args_list* must be validated before calling this
        method — the backend does not re-validate payloads.  The list
        must be non-empty and contain at most 1000 items (enforced by the
        client layer).

        Returns one :class:`JobRow` per item in *args_list*, in the same
        order.  For idempotency-key collisions the existing row is
        returned; its ``id`` will differ from the requested ``args.id``.
        """
        ...

    async def enqueue_batch_fast(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> int:
        """Insert multiple jobs via the COPY FROM protocol for maximum throughput.

        COPY cannot evaluate expressions or handle conflicts, so the write
        is two statements inside one transaction: a bare COPY of the
        domain-insensitive columns, then a corrective UPDATE
        (``enqueue_batch_fast_fixup``) that stamps/decides the
        clock-sensitive ones — ``status``, ``scheduled_at``,
        ``schedule_to_close``, ``result_expires_at`` — from the database
        clock (``clock_timestamp()``); ``created_at`` takes its DDL
        default (``now()``).  Nothing is observable half-fixed: both
        statements commit or abort together.

        Consequences of the COPY-no-conflicts shape:

        - ``scheduled_at=None`` means immediate — the fixup's server-side
          CASE stamps it and decides ``pending``/``scheduled`` (the same
          single-arbiter contract as :meth:`enqueue`/:meth:`enqueue_batch`).
        - ``schedule_to_close_interval``/``result_ttl`` are anchored to the
          server clock at ENQUEUE time by the fixup — a future-scheduled
          item with a short interval can therefore fail DeadlineExceeded
          before it is ever dispatched.
        - A duplicate ``idempotency_key`` — within the batch or already
          stored — violates the unique index and aborts the ENTIRE batch
          (all-or-nothing atomicity; nothing is written).

        Returns the count of rows written.  On success this is exactly
        ``len(args_list)`` — this path never deduplicates, so the count
        never includes pre-existing rows.  The in-memory mirror implements
        the same contract: duplicates raise
        ``asyncpg.UniqueViolationError`` before any row is written, and
        the count is the number of items.

        This is a performance-focused variant of :meth:`enqueue_batch`
        (which DOES deduplicate idempotency-key collisions via ``ON
        CONFLICT``).  Use for bulk import / backfill with 10K+ rows where
        collision handling is not needed.  Max batch size is 50 000
        (client-enforced).
        """
        ...

    async def enqueue_with_conn(
        self,
        conn: "asyncpg.Connection",
        args: EnqueueArgs,
    ) -> JobRow:
        """Enqueue a job using the supplied connection.

        The connection MUST already be in an open transaction managed by
        the caller — this method does NOT issue BEGIN/COMMIT. The
        autonomous variant ``enqueue(args)`` acquires its own connection
        and opens a transaction internally.
        """
        ...

    # ── Dispatch ────────────────────────────────────────────────────────
    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]: ...

    # ── Heartbeat ───────────────────────────────────────────────────────
    async def heartbeat_jobs(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
    ) -> int: ...

    async def extend_reservation_leases(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
    ) -> int: ...

    # ── Terminal writes ─────────────────────────────────────────────────
    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
    ) -> bool:
        """Mark a job succeeded, computing ``result_expires_at`` at completion.

        Expiry resolution, first match wins: a non-NULL stored
        ``actor_config.result_ttl`` (operator-owned) applies; otherwise
        *fallback_result_ttl* — the worker-side ``@actor(result_ttl=...)``
        literal, which the terminal-write SQL cannot see — applies;
        otherwise the row's existing ``result_expires_at`` is kept. The
        computed arms use ``clock_timestamp()`` — the wall-clock time the
        write executes, not the transaction start — so neither a long
        queue wait nor a long actor runtime can make a job complete
        already expired and have its result reaped immediately.
        """
        ...

    async def mark_succeeded_with_conn(
        self,
        conn: "asyncpg.Connection",
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
    ) -> bool:
        """Mark a job succeeded using the supplied connection.

        Used by the consumer when a LOOP-scope ``asyncpg.Connection`` is
        available so the success status update commits atomically with
        the actor's writes and sub-job INSERTs in the same transaction.
        The connection MUST already be in an open transaction; this
        method does NOT open or close one. The autonomous variant
        ``mark_succeeded(...)`` acquires its own connection.

        ``fallback_result_ttl`` follows the same resolution rule as
        :meth:`mark_succeeded`.
        """
        ...

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow:
        """Mark a running job failed, or schedule a retry *retry_delay* later.

        ``retry_delay=None`` is the terminal-fail arm (``status='failed'``,
        the original ``error_info`` persisted).  A non-None delay is applied
        by the backend's own clock, never the caller's: ``scheduled_at =
        now() + delay`` and the ``scheduled``/``pending`` status derive from
        the delay alone (zero → immediate).  The same statement arbitrates
        the ``schedule_to_close`` deadline server-side — when
        ``clock_timestamp() + delay`` would land past the deadline, the row
        is failed with ``error_class='DeadlineExceeded'`` instead of
        retried — so app↔DB clock skew can neither void the retry backoff
        nor kill a job whose deadline has not actually passed.
        """
        ...

    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool: ...

    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool: ...

    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool: ...

    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: AttemptOutcome = "snoozed",
    ) -> Literal["scheduled", "failed", "noop"]: ...

    async def mark_retry_after(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]: ...

    # ── Attempt history ─────────────────────────────────────────────────
    async def write_attempt(self, attempt: AttemptRow) -> None: ...

    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]: ...

    async def get_events(self, job_id: JobId) -> list[EventRow]: ...

    async def poll_reclaim_events(
        self,
        after_id: int,
        limit: int = 100,
        *,
        visibility_delay: timedelta | None = None,
    ) -> list[EventRow]:
        """Return up to *limit* crash-reclaim events with ``event_id >
        after_id``, ascending — the durable cursor behind
        ``TaskQ.watch_reclaims``.

        **An event can be silently missed if a ``job_events`` writer
        transaction stays open longer than the visibility-delay margin
        between its INSERT and its COMMIT** — ids are allocated at INSERT
        time but transactions commit out of order, so a late-committing
        lower-id row can land behind an already-advanced cursor.  Rows
        are therefore held back by a trailing-watermark filter
        (*visibility_delay*; backend-configured default when ``None`` —
        see :data:`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` for
        the exact assumption and its violation modes, and
        ``PostgresBackend.check_reclaim_visibility_delay_risk`` for the
        diagnostic that makes a violation operator-visible).
        """
        ...

    # ── Cancel signals ──────────────────────────────────────────────────
    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool: ...

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter* in a set-based operation.

        Pending/scheduled jobs → terminal 'cancelled'.
        Running jobs → cancel_phase=1 (cooperative cancel + NOTIFY).

        The filter's ``limit``, ``cursor``, and ``order_by`` fields are
        ignored — this is a bulk write, not a paginated read.

        **Guardrail:** the client layer (:meth:`JobsClient.cancel_where`)
        rejects empty filters (no predicates) with
        :class:`EmptyFilterError`. Backend implementations receive a
        filter that has already been validated. A direct backend call
        with ``JobFilter()`` renders ``WHERE TRUE`` and cancels the
        entire table — callers using the backend directly are
        responsible for validating the filter.

        Returns a :class:`BulkCancelResult` with counts and affected IDs.
        """
        ...

    async def poll_cancel_flags(
        self,
        worker_id: UUID,
    ) -> list[CancelFlag]: ...

    # ── Admin operations ──────────────────────────────────────────────
    async def retry_job(self, job_id: JobId) -> bool:
        """Reset a terminal job (failed/crashed/cancelled) to pending.

        Returns ``True`` if the job was retried, ``False`` if it was not
        in a retryable state.
        """
        ...

    # ── Scheduling / sweeps ─────────────────────────────────────────────
    # The sweep methods take no ``now`` parameter: the arbiter is the
    # backend's own clock (PG: ``clock_timestamp()`` in the statement;
    # InMemory: the injected Clock) — a caller-supplied timestamp would be
    # a second, skewable domain mixed into the predicate.
    async def scheduled_to_pending(self) -> int:
        """Promote ``scheduled`` jobs whose ``scheduled_at`` has passed.

        The backend's own clock is the arbiter (PG evaluates
        ``scheduled_at <= clock_timestamp()`` server-side; InMemory
        compares against its injected Clock).  Returns the count of
        promoted rows.
        """
        ...

    async def deadline_sweep(self) -> int:
        """Fail pending/scheduled jobs whose ``schedule_to_close`` has passed.

        Transitions to ``failed`` with ``error_class='DeadlineExceeded'``,
        arbitrated by the backend's own clock.  Returns the count of swept
        rows.
        """
        ...

    async def reclaim_expired_locks(
        self,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
    ) -> int:
        """Reclaim ``running`` jobs whose lock has expired.

        The expiry check is arbitrated by the backend's own clock; the
        grace parameters only widen the carve-out for jobs with an
        in-flight cancel request.  Returns the count of reclaimed rows.
        """
        ...

    # ── Read ────────────────────────────────────────────────────────────
    async def get(self, job_id: JobId) -> JobRow | None: ...

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        """List jobs matching *filters*, returning at most ``filters.limit``
        rows in keyset-pagination order.

        ``filters.status`` accepts a single :data:`JobStatus` or a
        sequence of statuses; ``filters.active`` is a meta-filter for
        non-terminal (``True``) or terminal (``False``) statuses —
        'active' here means 'not yet finished', not Celery's 'currently
        executing'.  See :class:`JobFilter` for details.
        """
        ...

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        """Return pending+scheduled job counts per actor.

        Returns a dict mapping actor name to count.  Only actors with
        at least one pending or scheduled job appear in the result.
        Actors not in the result have a count of zero.  The ``actors``
        list is used as an ``IN``/``ANY`` filter — pass all distinct actor
        names from a batch to fetch all counts in one round-trip.
        """
        ...

    async def count_active_jobs(self, queues: list[str]) -> int:
        """Count non-terminal jobs (pending, scheduled, running) in the given queues.

        Returns the total count across all specified queues. Used by the
        drain monitor to detect when queues are empty. An empty queues
        list returns 0.
        """
        ...

    async def get_actor_max_pending(self) -> dict[str, int | None]:
        """Return the stored ``actor_config.max_pending`` for every actor
        with a row.

        Key present with an ``int`` value: the stored (operator-owned)
        limit. Key present with ``None``: a row exists but the column is
        NULL (a cleared override). Key absent: no stored row. Client-side
        capacity resolution
        (:class:`taskq.client._capacity.ActorCapacityCache`) treats
        "absent" and "NULL" identically — both fall back to the
        ``@actor(...)`` literal; the distinction is preserved here only
        so observability callers can tell them apart.

        This is the enqueue-path analog of the dispatch CTE's per-cycle
        ``actor_config`` join: one small whole-table read, consumed
        through a TTL-bounded cache so the hot path pays no per-enqueue
        query.
        """
        ...

    # ── NOTIFY hook ─────────────────────────────────────────────────────
    def subscribe_wake(self) -> AsyncContextManager[asyncio.Event]: ...

    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]:
        """Return an async context manager yielding a fresh ``asyncio.Event``
        that is set whenever a cancel NOTIFY arrives for any job.

        The heartbeat loop uses this to interrupt its sleep immediately on
        cancel, rather than waiting for the next scheduled tick.
        """
        ...

    # ── Schedule CRUD ────────────────────────────────────────────────────
    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord: ...

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]: ...

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord: ...

    async def delete_schedule(self, schedule_id: UUID) -> None: ...

    # ── Batch operations ────────────────────────────────────────────
    async def enqueue_batch_atomic(
        self,
        items: Iterable[EnqueueArgs],
        *,
        batch_id: UUID,
        queue: str,
        batch_row: BatchRow | None,
        finalizer_args: EnqueueArgs | None,
        chunk_size: int = 1000,
    ) -> list[JobRow]: ...

    async def create_batch(
        self,
        batch_id: UUID,
        queue: str,
        expected_size: int,
        failure_threshold: int | None,
        finalizer_job_id: UUID | None,
        originating_actor: str | None,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> None: ...

    async def increment_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> tuple[int, int | None, int]: ...

    async def reset_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> int: ...

    async def abort_batch(
        self,
        batch_id: UUID,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> int: ...

    async def complete_batch(
        self,
        batch_id: UUID,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> None: ...

    async def get_batch(
        self,
        batch_id: UUID,
    ) -> BatchRow | None: ...

    async def list_batches(
        self,
        filter: BatchFilter,
    ) -> list[tuple[BatchRow, BatchCounts]]: ...

    async def count_batch_non_terminal(
        self,
        batch_id: UUID,
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> int: ...

    async def prune_old_batches(
        self,
        cutoff: datetime,
    ) -> int: ...

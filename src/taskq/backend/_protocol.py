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
    "AttemptOutcome",
    "AttemptRow",
    "Backend",
    "BackendDeps",
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
    "QueueMode",
    "QueueName",
    "RateLimitBackend",
    "RetryKind",
    "ScheduleCreateArgs",
    "ScheduleRecord",
    "ScheduleUpdateArgs",
    "parse_cancel_phase",
    "parse_retry_kind",
]

# ── Protocol version ───────────────────────────────────────────────────
BACKEND_PROTOCOL_VERSION: Final[int] = 2

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

type AttemptOutcome = Literal[
    "succeeded",
    "failed",
    "snoozed",
    "cancelled",
    "crashed",
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
    and

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
    caller specifies at enqueue time.  ``scheduled_at`` has no default —
    callers set it explicitly (``clock.now()`` for immediate dispatch, or a
    future datetime for deferred execution).
    """

    id: JobId
    actor: str
    queue: str
    payload: dict[str, object]
    max_attempts: int
    retry_kind: RetryKind
    scheduled_at: datetime
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
class CancelFlag:
    """Carries exactly the two fields returned by the heartbeat cancel-poll
    query: ``job_id`` and ``cancel_phase``.  ``cancel_requested_at``
    is tracked locally by the heartbeat, not read from PG on every poll.
    """

    job_id: JobId
    cancel_phase: CancelPhase


@dataclass(frozen=True, slots=True)
class JobFilter:
    """Filter parameters for :meth:`Backend.list_jobs`.  ``cursor`` is an
    opaque keyset-pagination token encoding ``(priority, scheduled_at, id)``
    from the last row of the previous page.  Both backends must
    agree on cursor encoding and comparison semantics.

    ``batch_id`` is a :class:`UUID`. The PG backend converts it to its
    canonical string form at the SQL boundary; the in-memory backend
    compares the UUID directly. Keeping the typed shape here means
    ``JobsClient.list(batch_id=UUID(...))`` flows without an implicit
    ``str(uuid)`` coercion. See  / audit M102-3.
    """

    queue: str | None = None
    status: JobStatus | None = None
    actor: str | None = None
    identity_key: IdentityKey | None = None
    batch_id: UUID | None = None
    limit: int = 100
    cursor: str | None = None
    tags: tuple[str, ...] | None = None
    order_by: JobSortField | None = None

    def __post_init__(self) -> None:
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

    30 async methods plus two sync methods (``subscribe_wake`` and
    ``subscribe_cancel_wake``) (32 methods total) covering enqueue,
    dispatch, heartbeat, terminal writes, attempt history, cancel
    signals, scheduling / sweeps, read, NOTIFY hook, and schedule CRUD.
    Method order grouped for review-grep ergonomics.

    Why monomorphic (no ``Generic[P, R]``): the backend is the DB
    adapter boundary. Payloads are stored as ``dict[str, object]`` (the
    JSONB ``payload`` column) regardless of the actor's typed payload
    model. Generic parameters here would propagate ``P`` and ``R`` into
    every method (``dispatch_batch``, ``mark_succeeded``, etc.) with no
    safety benefit at the storage layer. The worker consumer
    reconstructs the typed ``JobContext[P]`` at dispatch time using
    ``ActorRef.payload_type.model_validate(row.payload)``. See
     /
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
        """Insert multiple jobs via COPY FROM protocol for maximum throughput.

        Returns the count of inserted rows. All values are pre-computed
        in Python — no server-side expressions, no ON CONFLICT, no
        RETURNING.  Duplicate idempotency_key causes the entire batch to
        fail (all-or-nothing atomicity).

        This is a performance-focused variant of :meth:`enqueue_batch`.
        Use for bulk import / backfill scenarios with 10K+ rows where
        idempotency-key collision handling is not needed.  Max batch size
        is 50 000 (client-enforced).  See ``docs/spec/copy-from-batch-insert.md``
        for tradeoffs.
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
    ) -> bool: ...

    async def mark_succeeded_with_conn(
        self,
        conn: "asyncpg.Connection",
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        """Mark a job succeeded using the supplied connection.

        Used by the consumer when a LOOP-scope ``asyncpg.Connection`` is
        available so the success status update commits atomically with
        the actor's writes and sub-job INSERTs in the same transaction.
        The connection MUST already be in an open transaction; this
        method does NOT open or close one. The autonomous variant
        ``mark_succeeded(...)`` acquires its own connection.
        """
        ...

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        next_scheduled_at: datetime | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow: ...

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

    # ── Cancel signals ──────────────────────────────────────────────────
    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool: ...

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
    async def scheduled_to_pending(self, now: datetime) -> int: ...

    async def deadline_sweep(self, now: datetime) -> int: ...

    async def reclaim_expired_locks(
        self,
        now: datetime,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
    ) -> int: ...

    # ── Read ────────────────────────────────────────────────────────────
    async def get(self, job_id: JobId) -> JobRow | None: ...

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]: ...

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        """Return pending+scheduled job counts per actor.

        Returns a dict mapping actor name to count.  Only actors with
        at least one pending or scheduled job appear in the result.
        Actors not in the result have a count of zero.  The ``actors``
        list is used as an ``IN``/``ANY`` filter — pass all distinct actor
        names from a batch to fetch all counts in one round-trip.
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

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    CancelPhase,
    EnqueueArgs,
    ErrorInfo,
    IdempotencyKey,
    IdentityKey,
    JobId,
    JobRow,
    JobStatus,
    RetryKind,
)
from taskq.testing.in_memory import InMemoryBackend

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


def make_job_row(
    *,
    attempt: int = 1,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    schedule_to_close: datetime | None = None,
    identity_key: IdentityKey | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    start_to_close: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    cancel_phase: int | CancelPhase | None = None,
    status: JobStatus = "running",
    priority: int = 0,
    error_class: str | None = None,
    error_message: str | None = None,
    payload: dict[str, object] | None = None,
    queue: str = "default",
    actor: str = "test_actor",
    progress_seq: int = 0,
) -> JobRow:
    """Build a JobRow with sensible defaults."""
    phase: CancelPhase
    if cancel_phase is None:
        phase = CancelPhase.NONE
    elif isinstance(cancel_phase, CancelPhase):
        phase = cancel_phase
    else:
        phase = CancelPhase(cancel_phase)

    locked_by = _WORKER_ID if status == "running" else None

    return JobRow(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        identity_key=identity_key,
        fairness_key=None,
        payload=payload or {},
        payload_schema_ver=1,
        status=status,
        priority=priority,
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        schedule_to_close=schedule_to_close,
        start_to_close=start_to_close,
        heartbeat_timeout=heartbeat_timeout,
        created_at=_NOW,
        scheduled_at=_NOW,
        started_at=_NOW if status == "running" else None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=locked_by,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=phase,
        error_class=error_class,
        error_message=error_message,
        error_traceback=None,
        progress_state={},
        progress_seq=progress_seq,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        trace_id=trace_id,
        span_id=span_id,
        metadata={},
        tags=(),
    )


def make_enqueue_args(
    *,
    actor: str = "test_actor",
    queue: str = "default",
    payload: dict[str, object] | None = None,
    idempotency_key: str | None = None,
    identity_key: str | None = None,
    scheduled_at: datetime | None = None,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    priority: int = 0,
    schedule_to_close: datetime | None = None,
    metadata: dict[str, object] | None = None,
    tags: tuple[str, ...] | None = None,
) -> EnqueueArgs:
    """Build EnqueueArgs with sensible defaults.

    ``scheduled_at`` defaults to 1 second in the past (relative to the
    Python wall clock) so freshly-enqueued test jobs can never classify
    as a future-"scheduled" job: the enqueue SQL compares against PG's
    ``clock_timestamp()``, and Python's monotonic/wall clock can diverge
    from PG's realtime clock enough under parallel load that a "now"
    computed here reads as still-future by the time the row lands.
    """
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload or {"value": 1},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=scheduled_at or (datetime.now(UTC) - timedelta(seconds=1)),
        priority=priority,
        schedule_to_close=schedule_to_close,
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
        identity_key=IdentityKey(identity_key) if identity_key is not None else None,
        metadata=metadata or {},
        tags=tags if tags is not None else (),
    )


def error_info(
    error_class: str = "ValueError",
    error_message: str = "boom",
) -> ErrorInfo:
    """Shorthand for ErrorInfo with error_traceback=None."""
    return ErrorInfo(
        error_class=error_class,
        error_message=error_message,
        error_traceback=None,
    )


async def enqueue_and_dispatch_memory(
    backend: InMemoryBackend,
    *,
    actor: str = "test_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> tuple[JobId, UUID]:
    """Enqueue and dispatch a job on the in-memory backend.

    Returns ``(job_id, worker_id)`` for the dispatched job.
    """
    now = backend._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test-only
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,  # type: ignore[arg-type]  # Why: retry_kind param is str but EnqueueArgs.retry_kind expects RetryKind; known-valid values are passed here
        scheduled_at=now,
        tags=(),
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only
    dispatched = await backend.dispatch_batch(
        worker_id,
        [queue],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


__all__ = [
    "enqueue_and_dispatch_memory",
    "error_info",
    "make_enqueue_args",
    "make_job_row",
]

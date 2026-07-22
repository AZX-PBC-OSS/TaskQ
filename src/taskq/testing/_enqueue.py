"""Enqueue operations for InMemoryBackend.

``enqueue``, ``enqueue_with_conn``, ``enqueue_batch``, and
``enqueue_batch_fast`` live here as module-level functions taking
``self: InMemoryBackend`` as the first parameter.
"""

from typing import TYPE_CHECKING

import structlog

from taskq.backend._protocol import (
    CancelPhase,
    EnqueueArgs,
    JobRow,
)
from taskq.exceptions import (
    MaxPendingExceededError,
    SingletonCollisionError,
)

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_enqueue",
    "_enqueue_batch",
    "_enqueue_batch_fast",
    "_enqueue_with_conn",
]

logger = structlog.get_logger("taskq.testing.in_memory")


async def _enqueue(self: "InMemoryBackend", args: EnqueueArgs) -> JobRow:
    if args.unique_for is not None and args.identity_key is not None:
        now = self._clock.now()
        cutoff = now - args.unique_for
        candidates = [
            row
            for row in self._jobs.values()
            if row.actor == args.actor
            and row.identity_key == args.identity_key
            and row.status in args.unique_states
            and row.created_at > cutoff
        ]
        if candidates:
            existing_row = max(candidates, key=lambda r: r.created_at)
            logger.info(
                "job_enqueue_deduplicated",
                kind="job_enqueue_deduplicated",
                job_id=str(existing_row.id),
                actor=existing_row.actor,
                queue=existing_row.queue,
                identity_key=existing_row.identity_key,
                idempotency_key=None,
                existing_job_id=str(existing_row.id),
                dedup_reason="unique_for",
            )
            return existing_row

    if args.metadata.get("singleton") is True:
        from datetime import timedelta

        for row in self._jobs.values():
            if (
                row.actor == args.actor
                and row.status in ("pending", "scheduled", "running")
                and row.metadata.get("singleton") is True
            ):
                now = self._clock.now()
                retry_after: timedelta | None = None
                if row.schedule_to_close is not None and row.schedule_to_close > now:
                    retry_after = row.schedule_to_close - now
                logger.info(
                    "singleton-collision",
                    actor=args.actor,
                    blocking_job_id=str(row.id),
                    detection_path="preflight_check",
                )
                raise SingletonCollisionError(
                    actor=args.actor,
                    blocking_job_id=row.id,
                    retry_after=retry_after,
                )

    if args.max_pending is not None:
        current_count = sum(
            1
            for row in self._jobs.values()
            if row.actor == args.actor and row.status in ("pending", "scheduled")
        )
        if current_count >= args.max_pending:
            logger.warning(
                "max-pending-exceeded",
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )
            raise MaxPendingExceededError(
                actor=args.actor,
                current_count=current_count,
                max_pending=args.max_pending,
            )

    if args.idempotency_key is not None:
        existing_id = self._idempotency_index.get(args.idempotency_key)
        if existing_id is not None:
            existing_row = self._jobs.get(existing_id)
            if existing_row is not None:
                logger.info(
                    "job_enqueue_deduplicated",
                    kind="job_enqueue_deduplicated",
                    job_id=str(existing_row.id),
                    actor=existing_row.actor,
                    queue=existing_row.queue,
                    identity_key=existing_row.identity_key,
                    idempotency_key=existing_row.idempotency_key,
                    existing_job_id=str(existing_row.id),
                    dedup_reason="idempotency_key",
                )
                return existing_row

    now = self._clock.now()
    status: object = "pending" if args.scheduled_at <= now else "scheduled"

    resolved_schedule_to_close = (
        now + args.schedule_to_close_interval
        if args.schedule_to_close_interval is not None
        else args.schedule_to_close
    )

    result_expires_at = now + args.result_ttl if args.result_ttl is not None else None

    row = JobRow(
        id=args.id,
        actor=args.actor,
        queue=args.queue,
        identity_key=args.identity_key,
        fairness_key=args.fairness_key,
        payload=args.payload,
        payload_schema_ver=args.payload_schema_ver,
        status=status,  # type: ignore[arg-type]  # Why: ternary "pending" if ... else "scheduled" is not narrowed to JobStatus by pyright
        priority=args.priority,
        attempt=0,
        max_attempts=args.max_attempts,
        retry_kind=args.retry_kind,
        schedule_to_close=resolved_schedule_to_close,
        start_to_close=args.start_to_close,
        heartbeat_timeout=args.heartbeat_timeout,
        created_at=now,
        scheduled_at=args.scheduled_at,
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=result_expires_at,
        idempotency_key=args.idempotency_key,
        trace_id=args.trace_id,
        span_id=args.span_id,
        metadata=args.metadata,
        tags=args.tags,
    )

    self._jobs[args.id] = row

    if args.idempotency_key is not None:
        self._idempotency_index[args.idempotency_key] = args.id

    for event in self._wake_subscribers:
        event.set()

    logger.debug(
        "state_change",
        kind="state_change",
        from_state=None,
        to_state=status,
        job_id=str(args.id),
        actor=args.actor,
    )

    return row


async def _enqueue_with_conn(
    self: "InMemoryBackend",
    conn: object,
    args: EnqueueArgs,
) -> JobRow:
    return await _enqueue(self, args)


async def _enqueue_batch(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
    *,
    connection: object = None,
) -> list[JobRow]:
    if not args_list:
        raise ValueError("args_list must not be empty")
    rows: list[JobRow] = []
    for args in args_list:
        row = await _enqueue(self, args)
        rows.append(row)
    return rows


async def _enqueue_batch_fast(
    self: "InMemoryBackend",
    args_list: list[EnqueueArgs],
    *,
    connection: object = None,
) -> int:
    rows = await _enqueue_batch(self, args_list)
    return len(rows)

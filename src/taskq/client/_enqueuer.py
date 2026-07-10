"""Sub-job enqueuer — enqueues child jobs from within an actor body.

Uses the LOOP-scope DB connection by default (transactional enqueue).
Falls back to the worker pool if no LOOP-scope connection is registered.
One instance per loop — survives across dispatches so the per-100-enqueue
re-warning fires on the loop-level counter, not per-job.

"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.backend._protocol import (
    Backend,
    CancelPhase,
    EnqueueArgs,
    IdempotencyKey,
    IdentityKey,
    JobRow,
    JobStatus,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.batch import EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args, enqueue_span
from taskq.client._handle import JobHandle
from taskq.exceptions import PartialBatchError, SubEnqueueError

if TYPE_CHECKING:
    import asyncpg

    from taskq.actor import ActorRef

__all__ = ["SubJobEnqueuer"]

_log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class SubJobEnqueuer:
    """Enqueue sub-jobs from within an actor body.

    Uses the LOOP-scope DB connection by default (transactional
    enqueue). Falls back to the worker pool if no LOOP-scope
    connection is registered. One instance per loop — survives
    across dispatches so the per-100-enqueue re-warning fires on
    the loop-level counter, not per-job.
    """

    def __init__(
        self,
        loop_scope_resolved: Mapping[type, object] | None,
        worker_pool: asyncpg.Pool | None,
        backend: Backend,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._loop_scope_resolved = loop_scope_resolved
        self._worker_pool = worker_pool
        self._backend = backend
        self._clock = clock if clock is not None else SystemClock()
        self._pending_buffer: list[EnqueueArgs] = []
        self._loop_enqueue_args: list[EnqueueArgs] = []
        self._autonomous_enqueue_count: int = 0

    async def enqueue[P: BaseModel, R: BaseModel | None](
        self,
        actor_ref: ActorRef[P, R],
        payload: P,
        *,
        connection: asyncpg.Connection | None = None,
        scheduled_at: datetime | None = None,
        priority: int | None = None,
        fairness_key: str | None = None,
        metadata: dict[str, object] | None = None,
        identity_key: IdentityKey | None = None,
        idempotency_key: IdempotencyKey | str | None = None,
        unique_for: timedelta | None = None,
        unique_states: tuple[JobStatus, ...] | None = None,
        max_pending: int | None = None,
    ) -> JobHandle[R]:
        resolved_queue = actor_ref.queue
        identity_key_str = str(identity_key) if identity_key is not None else ""

        with enqueue_span(actor_ref.name, resolved_queue, identity_key=identity_key_str) as (
            span,
            extracted_trace_id,
            extracted_span_id,
        ):
            args = build_enqueue_args(
                actor_ref,
                payload,
                scheduled_at=scheduled_at,
                priority=priority,
                fairness_key=fairness_key,
                metadata=metadata,
                identity_key=identity_key,
                idempotency_key=idempotency_key,
                trace_id=extracted_trace_id,
                span_id=extracted_span_id,
                unique_for=unique_for,
                unique_states=unique_states,
                max_pending=max_pending,
                clock=self._clock,
            )
            span.set_attribute("messaging.message.id", str(args.id))
            row = await self._do_enqueue(args, connection)
        return JobHandle(
            row=row,
            result_adapter=actor_ref.result_adapter,
            was_existing=(row.id != args.id),
            backend=self._backend,
            client=None,
        )

    def _resolve_connection(
        self,
        connection: asyncpg.Connection | None,
    ) -> tuple[asyncpg.Connection | None, bool]:
        import asyncpg as _asyncpg

        conn = connection
        from_loop_scope = False

        if conn is not None:
            pass
        elif (
            self._loop_scope_resolved is not None
            and (loop_conn := self._loop_scope_resolved.get(_asyncpg.Connection)) is not None
        ):
            conn = cast(_asyncpg.Connection, loop_conn)
            from_loop_scope = True

        # Why: cast — loop_conn comes from Mapping[type, object]; the DI resolver guarantees it is asyncpg.Connection at runtime
        return conn, from_loop_scope

    async def _do_enqueue(
        self,
        args: EnqueueArgs,
        connection: asyncpg.Connection | None,
    ) -> JobRow:
        conn, from_loop_scope = self._resolve_connection(connection)

        if conn is not None:
            if from_loop_scope and self._backend.supports_transactional_simulation:
                self._pending_buffer.append(args)
                return self._synthesize_row(args)
            row = await self._backend.enqueue_with_conn(conn, args)
            if from_loop_scope:
                self._loop_enqueue_args.append(args)
            return row

        if self._worker_pool is None:
            raise RuntimeError("ctx.jobs is only available inside an actor body")

        row = await self._backend.enqueue(args)
        self._autonomous_enqueue_count += 1
        if self._autonomous_enqueue_count % 100 == 0:
            _log.warning(
                "sub_enqueue_autonomous_fallback",
                autonomous_enqueue_count=self._autonomous_enqueue_count,
            )

        return row

    async def enqueue_batch(
        self,
        items: Sequence[EnqueueItem[Any, Any]],
        *,
        batch_id: UUID | None = None,
        connection: asyncpg.Connection | None = None,
    ) -> list[JobHandle[Any]]:
        """Enqueue a batch of sub-jobs sharing a single ``batch_id``.

        All ``items`` share a single ``batch_id`` UUID written into each
        job's ``metadata.batch_id`` field (as a string). When ``batch_id``
        is not supplied it is auto-generated as a UUIDv7 via
        :func:`~taskq._ids.new_job_id` — mirrors
        :meth:`~taskq.client.JobsClient.enqueue_batch`. Pass an explicit
        ``batch_id`` to correlate this batch with a caller-constructed
        identifier (e.g. a finalizer job enqueued separately that needs to
        reference the same batch).
        """
        resolved_batch_id = batch_id if batch_id is not None else UUID(bytes=new_job_id().bytes)

        conn, from_loop_scope = self._resolve_connection(connection)

        if conn is not None:
            args_list = build_batch_args(items, resolved_batch_id, self._clock)

            if from_loop_scope and self._backend.supports_transactional_simulation:
                for args in args_list:
                    self._pending_buffer.append(args)
                return [
                    JobHandle(
                        row=self._synthesize_row(args),
                        result_adapter=item.actor_ref.result_adapter,
                        was_existing=False,
                        backend=self._backend,
                        client=None,
                    )
                    for args, item in zip(args_list, items, strict=True)
                ]

            rows = await self._backend.enqueue_batch(args_list, connection=conn)  # type: ignore[call-arg]  # Why: asyncpg.Connection is compatible with the protocol's connection parameter at runtime
            if from_loop_scope:
                self._loop_enqueue_args.extend(args_list)
            handles: list[JobHandle[Any]] = []
            for i, row in enumerate(rows):
                args = args_list[i]
                handles.append(
                    JobHandle(
                        row=row,
                        result_adapter=items[i].actor_ref.result_adapter,
                        was_existing=(row.id != args.id),
                        backend=self._backend,
                        client=None,
                    )
                )
            return handles

        if self._worker_pool is None:
            raise RuntimeError("ctx.jobs is only available inside an actor body")

        handles = []
        failed_items: list[tuple[int, Exception]] = []
        for i, item in enumerate(items):
            item_metadata: dict[str, object] = dict(item.metadata)
            item_metadata["batch_id"] = str(resolved_batch_id)
            try:
                handle = await self.enqueue(
                    item.actor_ref,
                    item.payload,
                    scheduled_at=item.scheduled_at,
                    priority=item.priority,
                    fairness_key=item.fairness_key,
                    metadata=item_metadata,
                    idempotency_key=item.idempotency_key,
                    identity_key=item.identity_key,
                )
                handles.append(handle)
            except Exception as exc:
                failed_items.append((i, exc))

        if failed_items:
            raise PartialBatchError(
                succeeded_count=len(handles),
                failed_items=failed_items,
                total=len(items),
            )

        return handles

    async def flush_buffer(self) -> None:
        """Flush buffered EnqueueArgs to the backend (in-memory simulation).

        Called by the consumer on actor success, AFTER the LOOP-scope
        transaction has committed. Per-item flush failures are collected
        and re-raised as :class:`~taskq.exceptions.SubEnqueueError` after
        the loop completes so callers can detect lost sub-jobs.
        """
        snapshot = self._pending_buffer
        self._pending_buffer = []
        self._loop_enqueue_args.clear()
        failed_items: list[tuple[EnqueueArgs, Exception]] = []
        for args in snapshot:
            try:
                await self._backend.enqueue(args)
            except Exception as exc:
                failed_items.append((args, exc))
                _log.warning(
                    "sub_enqueue_flush_error",
                    kind="sub_enqueue_flush_error",
                    job_id=args.id,
                    message=str(exc),
                )
        if failed_items:
            raise SubEnqueueError(failed_items=failed_items)

    def discard_buffer(self) -> None:
        """Clear the pending buffer without flushing."""
        self._pending_buffer.clear()
        self._loop_enqueue_args.clear()

    def drain_for_re_enqueue(self) -> list[EnqueueArgs]:
        """Return and clear both loop-scope and pending buffers for re-enqueue."""
        items = self._loop_enqueue_args + list(self._pending_buffer)
        self._loop_enqueue_args = []
        self._pending_buffer = []
        return items

    @property
    def pending_count(self) -> int:
        return len(self._pending_buffer)

    @property
    def pending_items(self) -> Sequence[EnqueueArgs]:
        return tuple(self._pending_buffer)

    def _synthesize_row(self, args: EnqueueArgs) -> JobRow:
        """Build a synthetic JobRow from EnqueueArgs for the in-memory buffer path."""
        now = self._clock.now()
        return JobRow(
            id=args.id,
            actor=args.actor,
            queue=args.queue,
            identity_key=args.identity_key,
            fairness_key=args.fairness_key,
            payload=args.payload,
            payload_schema_ver=args.payload_schema_ver,
            status="scheduled" if args.scheduled_at > now else "pending",
            priority=args.priority,
            attempt=0,
            max_attempts=args.max_attempts,
            retry_kind=args.retry_kind,
            schedule_to_close=args.schedule_to_close,
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
            result_expires_at=None,
            idempotency_key=args.idempotency_key,
            trace_id=args.trace_id,
            span_id=args.span_id,
            metadata=args.metadata,
            tags=args.tags,
        )

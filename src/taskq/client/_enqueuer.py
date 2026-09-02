"""Sub-job enqueuer — enqueues child jobs from within an actor body.

Uses the LOOP-scope DB connection by default (transactional enqueue).
Falls back to the worker pool if no LOOP-scope connection is registered.
One instance per loop — survives across dispatches so the per-100-enqueue
re-warning fires on the loop-level counter, not per-job.

Parent-tag propagation: the consumer sets the parent job's tags via
``set_parent_tags()`` before actor invocation and resets them after
(via ``parent_tags()`` context manager or manual token reset). The
``contextvars.ContextVar`` ensures concurrent consumers in the same
event loop each see their own parent tags — asyncio Tasks copy the
context at creation time.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Generator, Mapping, Sequence
from dataclasses import replace
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
from taskq.batch import MAX_BATCH_SIZE, EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args, enqueue_span
from taskq.client._capacity import ActorCapacityCache
from taskq.client._handle import JobHandle
from taskq.exceptions import PartialBatchError, SubEnqueueError

if TYPE_CHECKING:
    import asyncpg

    from taskq.actor import ActorRef

__all__ = ["SubJobEnqueuer", "parent_tags", "set_parent_tags"]

_log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_parent_tags_var: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "taskq_parent_tags",
    default=(),
)


def set_parent_tags(tags: tuple[str, ...]) -> contextvars.Token[tuple[str, ...]]:
    """Set the parent job's tags for sub-job tag inheritance.

    Called by the consumer before actor invocation. The returned token
    must be used to reset the context after the actor completes — use
    ``_parent_tags_var.reset(token)`` or the ``parent_tags()`` context
    manager.
    """
    return _parent_tags_var.set(tags)


@contextlib.contextmanager
def parent_tags(tags: tuple[str, ...]) -> Generator[None, None, None]:
    """Context manager that sets parent tags for the duration of the block.

    Ensures the ContextVar is reset on all exit paths (success, exception,
    cancellation). Use this at worker entry points instead of manual
    set/reset::

        with parent_tags(tuple(job.tags)):
            # actor invocation, sub-job enqueues, etc.
            ...
    """
    token = _parent_tags_var.set(tags)
    try:
        yield
    finally:
        _parent_tags_var.reset(token)


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
        capacity_cache: ActorCapacityCache | None = None,
    ) -> None:
        self._loop_scope_resolved = loop_scope_resolved
        self._worker_pool = worker_pool
        self._backend = backend
        self._clock = clock if clock is not None else SystemClock()
        self._capacity_cache = (
            capacity_cache if capacity_cache is not None else ActorCapacityCache(backend)
        )
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
        idempotency_scope: str | None = None,
        unique_for: timedelta | None = None,
        unique_states: tuple[JobStatus, ...] | None = None,
        max_pending: int | None = None,
        _batch_id: str | None = None,
        tags: list[str] | None = None,
        inherit_tags: bool = True,
        schedule_to_close: datetime | None = None,
        start_to_close: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
    ) -> JobHandle[R]:
        """Enqueue a sub-job. ``max_pending`` is a per-call limit resolved
        against the operator-owned stored cap and the ``@actor(...)``
        literal: against a non-NULL *stored* ``actor_config.max_pending``
        the tighter of the two wins (``min(stored, per_call)`` — an
        explicit caller shedding load is never widened by an operator
        override, and no code path can raise an operator's fleet cap);
        with no stored value this parameter wins outright over the
        literal (historical behavior — actor code may loosen its own
        declaration).

        ``_batch_id`` is a library-internal parameter used by
        :meth:`enqueue_batch` to stamp ``batch_id`` into metadata after
        :func:`build_enqueue_args` has stripped any caller-supplied
        ``batch_id`` (H5 security boundary). Callers MUST NOT pass it.
        """
        resolved_queue = actor_ref.queue
        identity_key_str = str(identity_key) if identity_key is not None else ""

        with enqueue_span(actor_ref.name, resolved_queue, identity_key=identity_key_str) as (
            span,
            extracted_trace_id,
            extracted_span_id,
        ):
            effective_max_pending = await self._capacity_cache.effective_max_pending(
                actor_ref.name,
                actor_ref.max_pending,
                per_call=max_pending,
            )
            resolved_tags = self._resolve_tags(tags, inherit_tags)
            args = build_enqueue_args(
                actor_ref,
                payload,
                scheduled_at=scheduled_at,
                priority=priority,
                fairness_key=fairness_key,
                metadata=metadata,
                identity_key=identity_key,
                idempotency_key=idempotency_key,
                idempotency_scope=idempotency_scope,
                trace_id=extracted_trace_id,
                span_id=extracted_span_id,
                tags=resolved_tags,
                schedule_to_close=schedule_to_close,
                start_to_close=start_to_close,
                heartbeat_timeout=heartbeat_timeout,
                unique_for=unique_for,
                unique_states=unique_states,
                max_pending=effective_max_pending,
            )
            if _batch_id is not None:
                # H5: stamp batch_id AFTER build_enqueue_args, which strips
                # any caller-supplied batch_id as a security boundary.
                args = replace(
                    args,
                    metadata={**args.metadata, "batch_id": _batch_id},
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

    def _resolve_tags(
        self,
        tags: list[str] | None,
        inherit_tags: bool,
    ) -> list[str] | None:
        """Resolve tags with parent inheritance.

        Caller tags are UNIONED with the parent's, so ``[]`` — the
        identity element — resolves to the parent's tags exactly as
        ``None`` does. Why: every non-empty list unions, and making the
        empty list mean "suppress" would put a discontinuity in the
        middle of that, so a computed list that happens to come out
        empty would silently drop the parent's tags. Suppression has its
        own explicit control, ``inherit_tags=False``; there is one way
        to do it, not two. Returns a list suitable for
        build_enqueue_args, or None for empty. Deduplication is order-preserving (parent first); the
        downstream ``_validate_and_dedup_tags`` in ``build_enqueue_args``
        also deduplicates, but we do it here so the merge result is
        clean.
        """
        parent_tags = _parent_tags_var.get() if inherit_tags else ()

        if tags is None:
            if parent_tags:
                return list(parent_tags)
            return None

        if not inherit_tags or not parent_tags:
            return tags

        return list(dict.fromkeys((*parent_tags, *tags)))

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

        Raises ``ValueError`` when ``items`` is empty or exceeds
        ``MAX_BATCH_SIZE`` — the same guardrails
        :meth:`~taskq.client.JobsClient.enqueue_batch` applies to the
        identical operation one layer up. Without the empty check the
        no-connection fallback loop would iterate zero items and return
        ``[]`` silently. The backend binds every item
        as 21 parallel array parameters to a single ``unnest`` INSERT in one
        transaction, so an uncapped batch enqueued from inside a job body is
        unbounded fan-out that bypasses the client-side guardrail.
        """
        if len(items) == 0:
            raise ValueError("items must not be empty")
        if len(items) > MAX_BATCH_SIZE:
            raise ValueError(
                f"items must contain at most {MAX_BATCH_SIZE} entries, got {len(items)}"
            )

        resolved_batch_id = batch_id if batch_id is not None else UUID(bytes=new_job_id().bytes)

        conn, from_loop_scope = self._resolve_connection(connection)

        if conn is not None:
            effective_mp: dict[str, int | None] = {}
            for item in items:
                ref = item.actor_ref
                if ref.name not in effective_mp:
                    effective_mp[ref.name] = await self._capacity_cache.effective_max_pending(
                        ref.name, ref.max_pending
                    )
            args_list = build_batch_args(
                items, resolved_batch_id, max_pending_by_actor=effective_mp
            )

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
        batch_id_str = str(resolved_batch_id)
        for i, item in enumerate(items):
            try:
                handle = await self.enqueue(
                    item.actor_ref,
                    item.payload,
                    scheduled_at=item.scheduled_at,
                    priority=item.priority,
                    fairness_key=item.fairness_key,
                    metadata=dict(item.metadata),
                    idempotency_key=item.idempotency_key,
                    idempotency_scope=item.idempotency_scope,
                    identity_key=item.identity_key,
                    _batch_id=batch_id_str,
                    tags=list(item.tags) if item.tags else None,
                    inherit_tags=False,
                    start_to_close=item.start_to_close,
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
                    job_id=str(args.id),
                    error_class=type(exc).__name__,
                    error_message=str(exc),
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
        """Build a synthetic JobRow from EnqueueArgs for the in-memory buffer path.

        Display-only guess in the Python domain: ``status``/``scheduled_at``
        are predicted from this process's clock so callers get a plausible
        row before the transaction commits — the stored row is decided
        server-side and may differ (this row is never written back).
        """
        now = self._clock.now()
        return JobRow(
            id=args.id,
            actor=args.actor,
            queue=args.queue,
            identity_key=args.identity_key,
            fairness_key=args.fairness_key,
            payload=args.payload,
            payload_schema_ver=args.payload_schema_ver,
            status=(
                "pending" if args.scheduled_at is None or args.scheduled_at <= now else "scheduled"
            ),
            priority=args.priority,
            attempt=0,
            max_attempts=args.max_attempts,
            retry_kind=args.retry_kind,
            schedule_to_close=args.schedule_to_close,
            start_to_close=args.start_to_close,
            heartbeat_timeout=args.heartbeat_timeout,
            created_at=now,
            scheduled_at=args.scheduled_at or now,
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
            idempotency_scope=args.idempotency_scope,
            trace_id=args.trace_id,
            span_id=args.span_id,
            metadata=args.metadata,
            tags=args.tags,
        )

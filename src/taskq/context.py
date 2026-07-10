"""Per-job execution context for TaskQ actors.

This module defines :class:`JobContext`, the per-job execution state
handed to worker actors. :class:`SubJobEnqueuer` is defined in
:mod:`taskq.client._enqueuer` and imported here for the ``jobs`` field.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from opentelemetry.trace import Span
from pydantic import BaseModel

from taskq._json import dumps
from taskq.exceptions import ProgressTooLarge
from taskq.progress._publish import _publish_progress_event

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.progress._buffer import _ProgressBuffer
    from taskq.settings import WorkerSettings

__all__ = ["JobContext"]

_log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class JobContext[P: BaseModel]:
    """Per-job execution context handed to worker actors.

    The ``cancel_event`` field is a plain :class:`asyncio.Event` — never
    wrapped in a cancel scope or :class:`asyncio.TaskGroup` (
    PEP 789 mitigation). The consumer constructs a fresh event per
    attempt; the cancel-poll hook sets it on phase 1; user actor code
    polls :attr:`cancellation_requested` or awaits ``cancel_event.wait()``.

    ``payload`` is typed as the actor's payload model ``P``. The worker
    consumer validates the raw ``dict[str, object]`` payload from the
    JobRow against ``actor_ref.payload_type`` before constructing the
    context, so handlers see a fully-validated Pydantic instance.

    ``jobs`` provides :class:`SubJobEnqueuer` for enqueuing sub-jobs
    from within the actor body. The enqueuer resolves the database
    connection via LOOP-scope DI → worker-pool fallback.
    """

    job_id: UUID
    actor: str
    queue: str
    attempt: int
    worker_id: UUID
    payload: P
    jobs: SubJobEnqueuer
    log: structlog.stdlib.BoundLogger
    span: Span | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    _abort_requested: threading.Event = field(default_factory=threading.Event)
    _progress_buffers: dict[UUID, _ProgressBuffer] | None = None
    _redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    _worker_settings: WorkerSettings | None = None
    _pending_publish_tasks: set[asyncio.Task[None]] | None = None

    @property
    def cancellation_requested(self) -> bool:
        return self.cancel_event.is_set()

    def check_cancelled(self) -> None:
        if self.cancel_event.is_set():
            raise asyncio.CancelledError

    def should_abort(self) -> bool:
        """Synchronous cancellation check for sync actors (thread-safe).

        Sync actors cannot ``await`` the async :attr:`cancel_event`, so
        they poll this method cooperatively. The cancel controller sets
        the underlying :class:`threading.Event` during phase 1.

        Returns:
            ``True`` when cancellation has been requested — the sync
            actor should return or raise immediately.
        """
        return self._abort_requested.is_set()

    async def progress(
        self,
        *,
        step: int | None = None,
        percent: float | None = None,
        detail: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        """Report incremental progress for this job.

        Updates the in-memory coalesce buffer synchronously, then schedules a
        best-effort ``kind="progress"`` Redis publish as a background task
        when a client is connected — this call never blocks on the network.
        Raises :class:`~taskq.exceptions.ProgressTooLarge` if the serialised
        ``data`` payload exceeds ``WorkerSettings.progress_data_max_bytes``.

        All arguments are optional and merged last-writer-wins into the
        accumulated ``pending_state``. Intermediate calls between periodic
        flush ticks are coalesced: only the latest value for each field
        reaches Postgres. ``seq`` is strictly monotone across calls.

        The Redis publish is genuinely fire-and-forget: it may complete out
        of order relative to other in-flight publishes for the same job.
        Consumers reading the SSE/pub-sub stream already discard any event
        whose ``seq`` is not greater than the last one seen (see
        :mod:`taskq.web.progress`), so out-of-order or dropped publishes
        never corrupt displayed state — the buffer mutation above (and the
        eventual Postgres flush) is the durable source of truth. Failures
        publishing to Redis are logged and recorded as a metric, never
        raised here.
        """
        if data is not None and self._worker_settings is not None:
            serialised_len = len(dumps(data))
            limit = self._worker_settings.progress_data_max_bytes
            if serialised_len > limit:
                raise ProgressTooLarge(limit=limit, actual=serialised_len)

        if self._progress_buffers is None:
            return

        buffer = self._progress_buffers.get(self.job_id)
        if buffer is None:
            return

        buffer.pending_seq_delta += 1
        if step is not None:
            buffer.pending_state["step"] = step
        if percent is not None:
            buffer.pending_state["percent"] = percent
        if detail is not None:
            buffer.pending_state["detail"] = detail
        if data is not None:
            buffer.pending_state["data"] = data
        buffer.dirty = True

        seq = buffer.base_seq + buffer.pending_seq_delta

        if self._redis_client is not None and self._worker_settings is not None:
            coro = _publish_progress_event(
                self._redis_client,
                self._worker_settings,
                self.actor,
                self.job_id,
                step=step,
                percent=percent,
                detail=detail,
                data=data,
                seq=seq,
            )
            if self._pending_publish_tasks is not None:
                task = asyncio.create_task(coro, name=f"taskq-progress-publish-{self.job_id}")
                self._pending_publish_tasks.add(task)
                task.add_done_callback(self._pending_publish_tasks.discard)
            else:
                # No shared task set to hold a reference (e.g. a caller
                # constructing JobContext directly without a WorkerDeps) —
                # fall back to awaiting inline rather than risking the
                # scheduled task being garbage-collected mid-publish.
                await coro

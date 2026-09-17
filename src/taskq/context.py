"""Per-job execution context for TaskQ actors.

This module defines :class:`JobContext`, the per-job execution state
handed to worker actors. :class:`SubJobEnqueuer` is defined in
:mod:`taskq.client._enqueuer` and imported here for the ``jobs`` field.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from opentelemetry.trace import Span
from pydantic import BaseModel

from taskq._json import dumps
from taskq.exceptions import ProgressTooLarge
from taskq.progress._buffer import _EncodedProgressData
from taskq.progress._publish import _publish_progress_event

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.progress._buffer import _ProgressBuffer
    from taskq.settings import WorkerSettings

__all__ = ["CancelOrigin", "JobContext"]


class CancelOrigin(IntEnum):
    """Who asked for the running attempt's cancellation.

    The terminal routing for a cancelled attempt keys on the ORIGIN, not on
    the exception type (a deploy and an operator cancel both surface as
    ``CancelledError``), so the distinction must come from the row's
    cancel bookkeeping rather than from the raised error's type.

    NONE     — no cancel has been signalled.
    OPERATOR — the row's ``cancel_requested_at`` was observed (the heartbeat
               cancel poll), or the orchestrator's escalation probe found
               the row already under an operator cancel.
    SHUTDOWN — the shutdown orchestration (SIGTERM / drain monitor) asked.
    """

    NONE = 0
    OPERATOR = 1
    SHUTDOWN = 2


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

    ``snooze_count`` is the job row's count of completed non-consuming
    deferrals (:class:`~taskq.exceptions.Snooze` /
    :meth:`RetryAfter<taskq.exceptions.RetryAfter>` with
    ``consume_budget=False``) at dispatch time. Such a deferral refunds
    the claim's attempt increment, so ``attempt`` alone cannot count
    snooze cycles — an actor that wants to snooze N times and then
    succeed keys off ``snooze_count``, not off ``attempt``.
    """

    job_id: UUID
    actor: str
    queue: str
    attempt: int
    worker_id: UUID
    payload: P
    jobs: SubJobEnqueuer
    log: structlog.stdlib.BoundLogger
    snooze_count: int = 0
    span: Span | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    _abort_requested: threading.Event = field(default_factory=threading.Event)
    _cancel_origin: CancelOrigin = CancelOrigin.NONE
    _progress_buffers: dict[UUID, _ProgressBuffer] | None = None
    _redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    _worker_settings: WorkerSettings | None = None
    _pending_publish_tasks: set[asyncio.Task[None]] | None = None
    _progress_dropped_notice: threading.Event = field(default_factory=threading.Event)

    @property
    def cancellation_requested(self) -> bool:
        return self.cancel_event.is_set()

    @property
    def cancel_origin(self) -> CancelOrigin:
        """Who asked for this attempt's cancellation.

        Read it alongside :attr:`cancellation_requested` when the two
        answers differ in what the actor should do: on a shutdown
        (:attr:`CancelOrigin.SHUTDOWN`) the attempt is released back to the
        fleet with its budget refunded, so an actor that can checkpoint
        should prefer to stash progress and raise rather than return a
        partial result; on an operator cancel
        (:attr:`CancelOrigin.OPERATOR`) the job terminalises, so returning
        the partial result is the only way to keep it.
        """
        return self._cancel_origin

    def _set_cancel_origin(self, origin: CancelOrigin) -> None:
        """Stamp the cancel origin. Called by the cancel controller and the
        shutdown orchestrator alongside ``cancel_event.set()`` — never by
        actor code.

        ``object.__setattr__`` because the dataclass is frozen: the origin
        is process state that arrives AFTER construction (the registry
        entry's), exactly as the ``cancel_event``/``_abort_requested``
        fields mutate through their own methods rather than assignment.
        """
        object.__setattr__(self, "_cancel_origin", origin)

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

        ``data`` must have ``str`` dict keys (nested too): the size check
        and the PG flush serialize it directly, and a non-``str`` key
        raises ``TypeError`` (JSON and ``jsonb`` cannot carry non-string
        keys) where it previously would have been silently coerced.

        When no progress buffers are wired into this context (direct
        actor testing, a miswired context), the call — the Redis publish
        included — is a deliberate no-op; the first such dropped call
        emits one debug-level notice so the no-op is discoverable, and
        later calls stay silent.

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
        data_json: bytes | None = None
        if (data is not None or detail is not None) and self._worker_settings is not None:
            # Load-bearing serialization, not redundant with the publish
            # path's ``model_dump_json``: this is the only
            # ``progress_data_max_bytes`` enforcement in the codebase, and it
            # must raise ``ProgressTooLarge`` synchronously to the actor
            # BEFORE the oversized dict enters the coalesce buffer (and from
            # there the durable PG jsonb). ``_publish_progress_event`` is
            # fire-and-forget — a failure there is logged, never raised — so
            # the cap cannot move below this call; and pydantic cannot reuse
            # pre-serialized bytes for the event's ``data`` field (a
            # ``Json[dict]``-typed field rejects dict construction outright
            # and would change the wire format; verified against pydantic
            # 2.13). The double serialization of ``data`` (here + the event
            # dump) is therefore the price of the synchronous-raise contract.
            # The flush is not a third: the bytes measured here travel with
            # the dict on the buffer and are bound to the jsonb parameter
            # as-is.
            if data is not None:
                data_json = dumps(data)
                limit = self._worker_settings.progress_data_max_bytes
                if len(data_json) > limit:
                    raise ProgressTooLarge(limit=limit, actual=len(data_json))
            if detail is not None:
                # The detail string passes through the same publish-time
                # serialization ``data`` does: an unencodable detail (a lone
                # surrogate) raises to the actor HERE, before it enters the
                # coalesce buffer and detonates at the durable write as a
                # failure no terminal-write classification answers for. The
                # settings-wiring condition is the data guard's own: an
                # unwired context (direct actor testing) falls through to
                # the write boundary, which escapes the unencodable form
                # instead of stranding the job.
                dumps(detail)

        if self._progress_buffers is None:
            # No coalesce buffer wired, so this call — publish included —
            # is a deliberate no-op. The first dropped call reports itself
            # at debug level so a silent no-op is discoverable; the
            # once-latch keeps a tight progress loop from emitting one
            # log line per dropped call.
            if not self._progress_dropped_notice.is_set():
                self._progress_dropped_notice.set()
                self.log.debug("progress-dropped-no-buffer", kind="progress_dropped")
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
            buffer.encoded_data = (
                _EncodedProgressData(source=data, json=data_json) if data_json is not None else None
            )
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

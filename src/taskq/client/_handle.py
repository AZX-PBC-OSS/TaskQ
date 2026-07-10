"""Generic :class:`JobHandle` — typed handle to an enqueued job.

Carries a :class:`pydantic.TypeAdapter` for the actor's return type
``R``, which is the mechanism that prevents ``R`` from being a phantom
type parameter. The single blocking accessor :meth:`wait` returns ``R``
(never ``R | None``); it raises on missing / failed / timeout.

The handle reads the backend through ``self._backend`` for
:meth:`wait`; read-back operations (:meth:`status`, :meth:`attempts`)
require a :class:`JobsClient` and raise :class:`RuntimeError` when the
handle was constructed without one.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, TypeAdapter, ValidationError

from taskq.backend._protocol import AttemptRow, Backend, JobId, JobRow, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client._transport import pg_poll_event_stream, redis_event_stream
from taskq.constants import progress_channel
from taskq.exceptions import JobFailed, ResultUnavailable
from taskq.progress._events import ProgressEvent
from taskq.types import CancelResult

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.client._jobs import JobsClient
    from taskq.settings import TaskQSettings

__all__ = ["JobHandle"]

_WAIT_POLL_INTERVAL: float = 0.5


class JobHandle[R: BaseModel | None]:
    """Typed handle to a single enqueued job.

    Created by :class:`JobsClient` methods (:meth:`~JobsClient.enqueue`,
    :meth:`~JobsClient.get`) or by :class:`SubJobEnqueuer` (with
    ``backend=`` only). The type parameter ``R`` flows from the
    actor's declared return type through :class:`ActorRef` into this
    handle: ``JobHandle[OrderResult]`` for an actor returning
    ``OrderResult``, ``JobHandle[None]`` for fire-and-forget actors.

    At least one of ``client`` or ``backend`` must be supplied. When
    ``client`` is provided, ``_backend`` is filled from
    ``client.backend``. When only ``backend`` is provided, the four
    read-back methods (:meth:`status`, :meth:`refresh`,
    :meth:`attempts`, :meth:`cancel`) raise :class:`RuntimeError`
    because they require the client's higher-level coordination.
    :meth:`wait` always works (it reads through ``_backend`` directly).

    Why ``result_adapter: TypeAdapter[R]`` is a constructor arg: pyright
    only infers ``R`` for a generic class when the type parameter
    appears in at least one field or method signature. The adapter is
    that field — without it ``R`` would be phantom and inference would
    silently fall back to ``Unknown``.
    """

    def __init__(
        self,
        *,
        row: JobRow,
        result_adapter: TypeAdapter[R],
        was_existing: bool,
        client: "JobsClient | None" = None,
        backend: Backend | None = None,
        _redis_client: "redis_async.Redis | None" = None,
        _settings: "TaskQSettings | None" = None,
    ) -> None:
        if client is None and backend is None:
            raise ValueError(
                "JobHandle requires at least one of client= or backend= (received neither)"
            )
        self._row = row
        self._result_adapter = result_adapter
        self.was_existing: bool = was_existing
        self._client = client
        self._backend: Backend = backend if backend is not None else client.backend  # pyright: ignore[reportOptionalMemberAccess]  # Why: client is guaranteed non-None when backend is None; the ValueError above ensures at least one is provided
        self._redis_client: "redis_async.Redis | None" = _redis_client  # noqa: UP037  # Why: redis_async is under TYPE_CHECKING; string annotation prevents a runtime import cycle.
        self._handle_settings: "TaskQSettings | None" = _settings  # noqa: UP037  # Why: TaskQSettings is under TYPE_CHECKING; string annotation prevents a runtime import cycle.

    # ── Properties ─────────────────────────────────────────────────────

    @property
    def job_id(self) -> JobId:
        """The job's unique id."""
        return self._row.id

    @property
    def actor_name(self) -> str:
        """The actor this job targets."""
        return self._row.actor

    @property
    def queue(self) -> str:
        """The queue this job was enqueued on."""
        return self._row.queue

    # ── Read-back operations ────────────────────────────────────────────

    async def status(self) -> JobStatus:
        """Return the current status of this job (live read).

        Cheap, non-blocking: a single ``backend.get`` and a status
        projection. No polling. Use this when you want to know the
        state without waiting for a terminal transition.

        Raises:
            RuntimeError: this handle was constructed without a
                :class:`JobsClient`.
        """
        if self._client is None:
            raise RuntimeError(
                "JobHandle.status() requires a JobsClient. "
                "This handle was constructed via ctx.jobs.enqueue(); use "
                "the worker's JobsClient to read job state externally."
            )
        row = await self._client.backend.get(self.job_id)
        if row is None:
            raise KeyError(self.job_id)
        return row.status

    async def refresh(self) -> JobRow:
        """Re-read the row from the backend and return the raw
        :class:`JobRow`.

        Useful for callers that want full row state (timestamps,
        attempt counts, error metadata) without going through
        :meth:`wait`. Does not block on terminal state — returns the
        current row whatever its status.

        Raises:
            RuntimeError: this handle was constructed without a
                :class:`JobsClient`.
        """
        if self._client is None:
            raise RuntimeError(
                "JobHandle.refresh() requires a JobsClient. "
                "This handle was constructed via ctx.jobs.enqueue(); use "
                "the worker's JobsClient to read job state externally."
            )
        row = await self._client.backend.get(self.job_id)
        if row is None:
            raise KeyError(self.job_id)
        return row

    async def attempts(self) -> list[AttemptRow]:
        """Return the attempt rows for this job, ordered by attempt number.

        Raises:
            RuntimeError: this handle was constructed without a
                :class:`JobsClient`.
        """
        if self._client is None:
            raise RuntimeError(
                "JobHandle.attempts() requires a JobsClient. "
                "This handle was constructed via ctx.jobs.enqueue(); use "
                "the worker's JobsClient to read job state externally."
            )
        return await self._client.backend.get_attempts(self.job_id)

    # ── Cancel ─────────────────────────────────────────────────────────

    async def cancel(self, reason: str | None = None) -> CancelResult:
        """Delegate to :meth:`JobsClient.cancel`.

        Raises:
            RuntimeError: this handle was constructed without a
                :class:`JobsClient`.
        """
        if self._client is None:
            raise RuntimeError(
                "JobHandle.cancel() requires a JobsClient. "
                "This handle was constructed via ctx.jobs.enqueue(); use "
                "the worker's JobsClient to read job state externally."
            )
        return await self._client.cancel(self.job_id, reason)

    # ── Wait ───────────────────────────────────────────────────────────

    async def wait(self, *, timeout: float | None = None) -> R:  # noqa: ASYNC109  # Why: timeout is part of the public API contract; asyncio.timeout() context-manager doesn't fit a polling loop
        """Block until the job reaches a terminal status, then return ``R``.

        Returns the actor's return value, validated through
        :attr:`result_adapter`. The result type is ``R`` exactly —
        never ``R | None``. Missing or failed results raise.

        Raises:
            ResultUnavailable: terminal state reached but no result was
                stored (result TTL expired, actor returned ``None``
                while ``R`` is non-``None``, etc.).
            JobFailed: the job ended in a non-success terminal state
                (``failed`` / ``cancelled`` / ``crashed`` / ``abandoned``);
                the row is attached to the exception for inspection.
            TimeoutError: ``timeout`` elapsed before any terminal
                transition was observed.
        """
        deadline: float | None = None
        if timeout is not None:
            deadline = asyncio.get_running_loop().time() + timeout

        while True:
            row = await self._backend.get(self.job_id)
            if row is None:
                raise KeyError(self.job_id)
            if row.status in TERMINAL_STATUSES:
                return self._extract_result(row)

            remaining: float
            if deadline is not None:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError()
                sleep = min(_WAIT_POLL_INTERVAL, remaining)
            else:
                sleep = _WAIT_POLL_INTERVAL

            await asyncio.sleep(sleep)

    def _extract_result(self, row: JobRow) -> R:
        """Convert a terminal :class:`JobRow` into ``R`` or raise."""
        if row.status != "succeeded":
            raise JobFailed(row)
        if row.result is None:
            # For ``R = NoneType`` the adapter accepts ``None`` and returns
            # ``None``; for any other ``R`` the adapter raises a
            # :class:`ValidationError` which we surface as
            # :class:`ResultUnavailable`.
            try:
                return self._result_adapter.validate_python(None)
            except ValidationError as exc:
                raise ResultUnavailable(row) from exc
        return self._result_adapter.validate_python(row.result)

    # ── progress_stream ────────────────────────────────────────────────

    async def progress_stream(self) -> AsyncIterator[ProgressEvent]:
        """Stream live progress events for this job.

        When Redis is configured, subscribes to the per-job Redis pub/sub
        channel and yields :class:`~taskq.progress.ProgressEvent` objects in
        real time. When Redis is not available, falls back to polling Postgres
        at 500 ms intervals and synthesising events from row diffs.

        Raises :class:`NotImplementedError` when the in-memory backend is
        detected — the in-memory backend does not support pub/sub.

        Yields events until a ``terminal=True`` event is produced.
        """
        from taskq.testing.in_memory import InMemoryBackend  # lazy — test-only dep

        if isinstance(self._backend, InMemoryBackend):
            raise NotImplementedError(
                "progress_stream requires Redis; in-memory backend does not support SSE."
            )

        if self._redis_client is not None and self._handle_settings is not None:
            async for event in self._progress_stream_redis():
                yield event
        else:
            async for event in self._progress_stream_pg():
                yield event

    async def _progress_stream_redis(self) -> AsyncIterator[ProgressEvent]:
        """Redis pub/sub path for progress_stream."""
        assert self._redis_client is not None
        assert self._handle_settings is not None
        channel = progress_channel(self._handle_settings.schema_name, self.job_id)
        last_seq = -1

        async def decode(raw_str: str) -> ProgressEvent | None:
            nonlocal last_seq
            try:
                event = ProgressEvent.model_validate_json(raw_str)
            except Exception:
                return None
            if event.kind == "progress" and event.seq <= last_seq:
                return None
            last_seq = event.seq
            return event

        async for event in redis_event_stream(
            self._redis_client,
            channel,
            poll_timeout=30.0,
            decode_message=decode,
        ):
            yield event

    async def _progress_stream_pg(self) -> AsyncIterator[ProgressEvent]:
        """PG polling fallback path for progress_stream."""

        def row_to_event(row: JobRow, status_changed: bool) -> ProgressEvent:
            kind = "state_change" if status_changed else "progress"
            terminal = row.status in TERMINAL_STATUSES
            return ProgressEvent(
                kind=kind,  # type: ignore[arg-type]  # Why: kind is narrowed to "progress"|"state_change" by the ternary but pyright cannot track str→Literal narrowing.
                job_id=self.job_id,
                actor=row.actor,
                ts=datetime.now(UTC),
                seq=row.progress_seq,
                status=row.status,
                step=row.progress_state.get("step"),  # type: ignore[arg-type]  # Why: progress_state is dict[str, object]; field types are runtime-correct but pyright cannot verify narrowing through a generic dict.
                percent=row.progress_state.get("percent"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
                detail=row.progress_state.get("detail"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
                data=row.progress_state.get("data"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
                terminal=terminal,
            )

        async for event in pg_poll_event_stream(
            lambda: self._backend.get(self.job_id),
            row_to_event,
            poll_interval=_WAIT_POLL_INTERVAL,
        ):
            yield event

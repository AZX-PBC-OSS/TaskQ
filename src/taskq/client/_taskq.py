"""Top-level entry point for non-worker applications.

Provides :class:`TaskQ` — a Postgres-backed client that manages its own
connection pool and exposes job operations (enqueue, get, list, cancel)
directly.

Two lifecycle patterns are supported:

**Async context manager** (scripts, tests)::

    async with TaskQ(dsn="postgresql://user:pw@host/db") as tq:
        handle = await tq.enqueue(my_actor, MyPayload(...))
        result = await handle.wait()

**Explicit open/close** (FastAPI lifespan, long-lived processes)::

    tq = TaskQ(dsn=settings.pg_dsn)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await tq.open()
        yield
        await tq.close()

    @app.post("/tasks")
    async def create_task(payload: MyPayload):
        handle = await tq.enqueue(my_actor, payload)
        return {"job_id": str(handle.job_id)}

Passing an existing pool (e.g. shared with the rest of the application)::

    async with TaskQ(pool=app.state.pool) as tq:
        await tq.cancel(job_id)
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import structlog
from pydantic import BaseModel, ConfigDict, TypeAdapter

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.connections import ConnFactory

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded, close_pool_bounded
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    BatchFilter,
    BatchRow,
    DstStrategy,
    EventRow,
    IdempotencyKey,
    IdentityKey,
    JobFilter,
    JobId,
    JobPage,
    JobRow,
    JobStatus,
    QueueName,
    ScheduleRecord,
)
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import BatchHandle, BatchSummary, EnqueueItem
from taskq.batch_policy import BatchFailurePolicy
from taskq.client._actors import ActorsClient
from taskq.client._handle import JobHandle
from taskq.client._jobs import JobsClient
from taskq.constants import RECLAIM_EVENT_VISIBILITY_DELAY, progress_channel, wake_channel
from taskq.cron import ScheduleHandle
from taskq.progress._events import ProgressEvent
from taskq.types import BulkCancelResult, CancelResult

__all__ = ["ActorsClient", "EventRow", "JobEvent", "TaskQ"]

logger = structlog.get_logger("taskq.client._taskq")


class JobEvent(BaseModel):
    """A single event yielded by :meth:`TaskQ.stream`.

    Represents a point-in-time snapshot of a job's observable state.
    Yielded on every status transition or progress update; the final
    event always has ``terminal=True``.

    The ``progress_state`` and ``progress_seq`` fields reflect the last
    values written by the worker. They are ``None`` / ``0`` until the
    worker emits a progress update.

    Serialises cleanly to JSON via ``model_dump()`` for SSE or WebSocket
    fanout — fields are deliberately flat so the caller can forward the
    event without transformation::

        async for event in tq.stream(job_id):
            await websocket.send_json(event.model_dump())
    """

    model_config = ConfigDict(frozen=True)

    job_id: JobId
    status: JobStatus
    progress_state: dict[str, object]
    progress_seq: int
    terminal: bool


def _row_to_event(row: JobRow) -> JobEvent:
    """Map a :class:`JobRow` snapshot to a :class:`JobEvent` for streaming."""
    return JobEvent(
        job_id=row.id,
        status=row.status,
        progress_state=row.progress_state,
        progress_seq=row.progress_seq,
        terminal=row.status in TERMINAL_STATUSES,
    )


@dataclass(slots=True)
class _ClientSettings:
    schema_name: str
    dispatch_oversample: int = 2  # Satisfies BackendSettings protocol; unused in client paths


@dataclass(slots=True)
class _ClientDeps:
    settings: _ClientSettings
    worker_pool: "asyncpg.Pool"
    heartbeat_pool: "asyncpg.Pool"
    dispatcher_pool: "asyncpg.Pool | None" = None


class TaskQ:
    """Postgres-backed TaskQ client.

    Manages a connection pool and exposes job operations directly. Supports
    both the async context manager pattern and explicit ``open()`` / ``close()``
    for frameworks like FastAPI that manage their own lifecycle.

    Parameters
    ----------
    dsn:
        Postgres DSN string. Mutually exclusive with ``pool``.
    pool:
        An already-open ``asyncpg.Pool``. The caller retains ownership;
        ``close()`` will not close it.
    schema:
        TaskQ schema name. Defaults to ``"taskq"``.
    min_pool_size:
        Minimum pool connections. Only used when ``dsn`` is provided.
    max_pool_size:
        Maximum pool connections. Only used when ``dsn`` is provided.
    redis_url:
        Redis URL string. Mutually exclusive with ``redis_client``.
        The library creates and owns the Redis client; ``close()`` will
        close it.
    redis_client:
        An already-open ``redis.asyncio.Redis`` client. The caller retains
        ownership; ``close()`` will not close it. Mutually exclusive with
        ``redis_url``.
    pg_conn_factory:
        A zero-arg async factory returning an ``asyncpg.Connection`` for the
        LISTEN/NOTIFY transport used by :meth:`stream`. Mutually exclusive
        with ``listen_conn``. Takes precedence over ``dsn`` when set. Use
        this when you have no DSN (e.g. AAD-managed-identity auth) but still
        want streaming. TaskQ owns and closes the connection produced by
        the factory per ``stream()`` call.
    listen_conn:
        A pre-constructed ``asyncpg.Connection`` for the LISTEN transport.
        Caller-owned; TaskQ does not close it. Mutually exclusive with
        ``pg_conn_factory``. Takes precedence over ``dsn`` when set. Use
        this to share a dedicated LISTEN conn across callers.
    poll_timeout:
        Maximum seconds to wait between transport wakeups before re-fetching
        job state. Defaults to ``30.0``.
    """

    def __init__(
        self,
        *,
        dsn: str | None = None,
        pool: "asyncpg.Pool | None" = None,
        schema: str = "taskq",
        min_pool_size: int = 1,
        max_pool_size: int = 5,
        redis_url: str | None = None,
        redis_client: Any | None = None,
        pg_conn_factory: "ConnFactory | None" = None,
        listen_conn: "asyncpg.Connection | None" = None,
        poll_timeout: float = 30.0,
        reclaim_event_visibility_delay: timedelta | None = None,
    ) -> None:
        if dsn is None and pool is None:
            raise ValueError("TaskQ requires either 'dsn' or 'pool'")
        if dsn is not None and pool is not None:
            raise ValueError("TaskQ accepts 'dsn' or 'pool', not both")
        if redis_url is not None and redis_client is not None:
            raise ValueError("TaskQ accepts 'redis_url' or 'redis_client', not both")
        if pg_conn_factory is not None and listen_conn is not None:
            raise ValueError("TaskQ accepts 'pg_conn_factory' or 'listen_conn', not both")

        self._dsn = dsn
        self._pool: "asyncpg.Pool | None" = pool  # noqa: UP037  # Why: asyncpg imported under TYPE_CHECKING; quotes required for runtime resolution.
        self._schema = schema
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._redis_url = redis_url
        self._redis_client: "redis_async.Redis | None" = redis_client  # type: ignore[type-arg]  # noqa: UP037  # Why: erasure boundary — redis_async is under TYPE_CHECKING; string annotation avoids runtime import. type-arg: redis-py stubs expose Redis as an unparameterised generic. The caller-supplied client is stored here and forwarded to JobsClient without entering it on the exit stack.
        self._pg_conn_factory = pg_conn_factory
        self._listen_conn = listen_conn
        self._poll_timeout = poll_timeout
        # Passed through to the constructed PostgresBackend's poll_reclaim_events
        # default — see RECLAIM_EVENT_VISIBILITY_DELAY. Must match whatever
        # margin the worker fleet's sweep-adjacent backend uses, since the
        # margin's correctness depends on writer transaction duration, not
        # reader preference.
        self._reclaim_event_visibility_delay = reclaim_event_visibility_delay
        self._owns_pool = pool is None
        self._client: JobsClient | None = None
        self._actors_client: ActorsClient | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def open(self) -> None:
        """Open the connection pool and prepare the client.

        Called automatically by ``__aenter__``. Safe to call explicitly
        for frameworks that manage lifecycle outside an ``async with`` block.
        Raises :class:`RuntimeError` if already open.
        """
        if self._client is not None:
            raise RuntimeError("TaskQ is already open")

        # Lazy imports keep asyncpg out of the module-level import graph so
        # taskq.testing can be imported without pulling in asyncpg.
        import asyncpg

        from taskq.backend.clock import SystemClock
        from taskq.backend.postgres import PostgresBackend
        from taskq.settings import TaskQSettings

        if self._pool is None:
            created = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=self._min_pool_size,
                max_size=self._max_pool_size,
            )
            assert created is not None  # asyncpg returns None only for record_class paths
            self._pool = created

        pool = self._pool
        assert pool is not None
        deps = _ClientDeps(
            settings=_ClientSettings(schema_name=self._schema),
            worker_pool=pool,
            heartbeat_pool=pool,
        )
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=30),
            cleanup_grace_period=timedelta(seconds=10),
            reclaim_event_visibility_delay=(
                self._reclaim_event_visibility_delay
                if self._reclaim_event_visibility_delay is not None
                else RECLAIM_EVENT_VISIBILITY_DELAY
            ),
        )
        settings = TaskQSettings.load_from_dict(
            {"TASKQ_SCHEMA_NAME": self._schema},
        )
        if self._redis_url is not None:
            settings.redis_url = self._redis_url  # type: ignore[assignment]  # Why: dotenvmodel PostgresDsn/RedisDsn fields accept str values at runtime but pyright cannot verify the coercion through the model's __setattr__.
        self._client = JobsClient(backend, settings=settings)
        self._actors_client = ActorsClient(pool, schema=self._schema)
        if self._redis_client is not None:
            self._client._redis_client = self._redis_client  # pyright: ignore[reportPrivateUsage]  # Why: TaskQ owns the JobsClient lifecycle; assigning the caller-owned redis_client directly bypasses _open_redis so the client is NOT entered on the exit stack — TaskQ.close() must not close a caller-owned client.
        elif self._redis_url is not None:
            await self._client._open_redis(settings)  # pyright: ignore[reportPrivateUsage]  # Why: TaskQ owns the JobsClient lifecycle; _open_redis is the canonical hook for the owner to call after construction.

    async def close(self) -> None:
        """Close the client and release the pool if owned.

        Called automatically by ``__aexit__``. Safe to call explicitly.
        No-op if already closed.
        """
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._actors_client = None
        if self._owns_pool and self._pool is not None:
            # Why bounded: an enqueue in flight at close time can stall
            # Pool.close() indefinitely against a dead PG.
            await close_pool_bounded(self._pool, "client", CLOSE_TIMEOUT_SECS)
            self._pool = None

    async def __aenter__(self) -> "TaskQ":
        await self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ── Internal ───────────────────────────────────────────────────────────

    def _require_open(self) -> JobsClient:
        if self._client is None:
            raise RuntimeError(
                "TaskQ is not open. Call 'await tq.open()' or use 'async with TaskQ(...) as tq:'"
            )
        return self._client

    # ── Actor configuration ────────────────────────────────────────────────

    @property
    def actors(self) -> ActorsClient:
        """Actor configuration client — list, get, set capacity, deregister.

        Raises RuntimeError if called before ``open()`` or outside an
        ``async with`` block.
        """
        if self._actors_client is None:
            raise RuntimeError(
                "TaskQ is not open. Call 'await tq.open()' or use 'async with TaskQ(...) as tq:'"
            )
        return self._actors_client

    # ── Job operations ─────────────────────────────────────────────────────

    async def enqueue[P: BaseModel, R: BaseModel | None](
        self,
        ref: ActorRef[P, R],
        payload: P,
        *,
        queue: QueueName | None = None,
        scheduled_at: datetime | None = None,
        priority: int | None = None,
        schedule_to_close: datetime | None = None,
        start_to_close: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        identity_key: IdentityKey | None = None,
        fairness_key: str | None = None,
        idempotency_key: IdempotencyKey | None = None,
        idempotency_scope: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        metadata: dict[str, object] | None = None,
        tags: list[str] | None = None,
    ) -> JobHandle[R]:
        """Enqueue a job and return a typed handle."""
        return await self._require_open().enqueue(
            ref,
            payload,
            queue=queue,
            scheduled_at=scheduled_at,
            priority=priority,
            schedule_to_close=schedule_to_close,
            start_to_close=start_to_close,
            heartbeat_timeout=heartbeat_timeout,
            identity_key=identity_key,
            fairness_key=fairness_key,
            idempotency_key=idempotency_key,
            idempotency_scope=idempotency_scope,
            trace_id=trace_id,
            span_id=span_id,
            metadata=metadata,
            tags=tags,
        )

    async def enqueue_batch(
        self,
        items: list[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
        failure_policy: BatchFailurePolicy | None = None,
        finalizer: EnqueueItem | None = None,
    ) -> BatchHandle:
        """Enqueue multiple jobs in a single batched INSERT.

        Delegates to :meth:`JobsClient.enqueue_batch`; see its docstring
        for validation rules and idempotency-key collision semantics.
        """
        return await self._require_open().enqueue_batch(
            items,
            batch_id=batch_id,
            connection=connection,
            failure_policy=failure_policy,
            finalizer=finalizer,
        )

    async def enqueue_batch_streaming(
        self,
        items: Iterable[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
        failure_policy: BatchFailurePolicy | None = None,
        finalizer: EnqueueItem | None = None,
        chunk_size: int = 1000,
    ) -> BatchHandle:
        """Enqueue jobs from a lazy iterable in chunks.

        Delegates to :meth:`JobsClient.enqueue_batch_streaming`; see its
        docstring for chunk_size validation and streaming semantics.
        """
        return await self._require_open().enqueue_batch_streaming(
            items,
            batch_id=batch_id,
            connection=connection,
            failure_policy=failure_policy,
            finalizer=finalizer,
            chunk_size=chunk_size,
        )

    async def get_batch(self, batch_id: UUID) -> BatchRow | None:
        """Fetch a single batch row by ID.

        Delegates to :meth:`JobsClient.get_batch`. Returns ``None`` when
        the batch does not exist.
        """
        return await self._require_open().get_batch(batch_id)

    async def list_batches(
        self,
        filter: BatchFilter,
    ) -> list[BatchSummary]:
        """List batches matching *filter*, returning :class:`BatchSummary` objects.

        Delegates to :meth:`JobsClient.list_batches`.
        """
        return await self._require_open().list_batches(filter)

    async def enqueue_batch_fast(
        self,
        items: list[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
    ) -> int:
        """Enqueue jobs via COPY FROM protocol for maximum throughput.

        Delegates to :meth:`JobsClient.enqueue_batch_fast`; see its
        docstring for tradeoffs vs the regular :meth:`enqueue_batch`.
        """
        return await self._require_open().enqueue_batch_fast(
            items,
            batch_id=batch_id,
            connection=connection,
        )

    async def get[R: BaseModel | None](
        self,
        job_id: JobId,
        *,
        result_adapter: TypeAdapter[R] | None = None,
    ) -> JobHandle[R] | None:
        """Look up a job by id. Returns ``None`` when the job does not exist."""
        return await self._require_open().get(job_id, result_adapter=result_adapter)

    async def list(self, filter: JobFilter) -> JobPage:
        """List jobs matching *filter*, returning a :class:`JobPage`.

        Delegates to :meth:`JobsClient.list` — note ``filter.active``
        is not Celery's 'active' ('currently executing'); it selects by
        terminality ('not yet finished').  See :class:`JobFilter`.
        """
        return await self._require_open().list(filter)

    async def cancel(
        self,
        job_id: JobId,
        reason: str | None = None,
    ) -> CancelResult:
        """Request cancellation of a job. Raises :class:`KeyError` if not found."""
        return await self._require_open().cancel(job_id, reason)

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None = None,
        *,
        allow_empty_filter: bool = False,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter*. See :meth:`JobsClient.cancel_where`."""
        return await self._require_open().cancel_where(
            filter, reason, allow_empty_filter=allow_empty_filter
        )

    # ── Schedule operations ─────────────────────────────────────────────────

    async def create_schedule[P: BaseModel, R: BaseModel | None](
        self,
        actor: str | ActorRef[P, R],
        cron_expr: str,
        *,
        timezone: str = "UTC",
        dst_strategy: DstStrategy = "skip",
        payload_factory: str | None = None,
        static_payload: dict[str, object] | None = None,
        name: str = "",
        identity_key: IdentityKey | None = None,
        enabled: bool = True,
    ) -> ScheduleHandle:
        """Create a cron schedule.  Delegates to :meth:`JobsClient.create_schedule`.

        ``dst_strategy`` controls how DST gaps/overlaps are handled; see
        :meth:`JobsClient.create_schedule` for the full semantics.
        """
        return await self._require_open().create_schedule(
            actor,
            cron_expr,
            timezone=timezone,
            dst_strategy=dst_strategy,
            payload_factory=payload_factory,
            static_payload=static_payload,
            name=name,
            identity_key=identity_key,
            enabled=enabled,
        )

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> "list[ScheduleRecord]":
        """List cron schedules.  Delegates to :meth:`JobsClient.list_schedules`."""
        return await self._require_open().list_schedules(actor=actor, enabled=enabled)

    async def update_schedule(
        self,
        schedule_id: UUID,
        *,
        cron_expr: str | None = None,
        enabled: bool | None = None,
        payload_factory: str | None = None,
        static_payload: dict[str, object] | None = None,
        clear_payload_factory: bool = False,
    ) -> ScheduleRecord:
        """Update a cron schedule.  Delegates to :meth:`JobsClient.update_schedule`."""
        return await self._require_open().update_schedule(
            schedule_id,
            cron_expr=cron_expr,
            enabled=enabled,
            payload_factory=payload_factory,
            static_payload=static_payload,
            clear_payload_factory=clear_payload_factory,
        )

    async def delete_schedule(self, schedule_id: UUID) -> None:
        """Delete a cron schedule.  Delegates to :meth:`JobsClient.delete_schedule`."""
        await self._require_open().delete_schedule(schedule_id)

    # ── Streaming ──────────────────────────────────────────────────────────

    async def stream(self, job_id: JobId) -> AsyncIterator[JobEvent]:
        """Stream live state changes for a job as :class:`JobEvent` objects.

        Yields one event per observable state transition (status change or
        progress update), terminating automatically when the job reaches a
        terminal state. The final event always has ``terminal=True``.

        Usage::

            async for event in tq.stream(job_id):
                print(event.status, event.progress_state)
                # loop exits automatically when event.terminal is True

            # Or wire directly into a FastAPI SSE response:
            async def event_generator():
                async for event in tq.stream(job_id):
                    yield f"data: {event.model_dump_json()}\n\n"

        Raises
        ------
        RuntimeError
            Called before ``tq.open()`` or outside an ``async with`` block.
        KeyError
            The job does not exist.
        RuntimeError
            PG LISTEN transport requested but ``dsn`` was not provided at
            construction (pool-only mode).
        """
        client = self._require_open()
        row = await client.backend.get(job_id)
        if row is None:
            raise KeyError(job_id)

        event = _row_to_event(row)
        yield event
        if event.terminal:
            return

        gen: AsyncGenerator[JobEvent, None] = (
            _stream_redis(
                self._redis_client,
                self._schema,
                job_id,
                client,
                self._poll_timeout,
                last_seq=row.progress_seq,
                last_status=row.status,
            )
            if self._redis_client is not None
            else _stream_pg(
                self._dsn,
                self._schema,
                job_id,
                client,
                self._poll_timeout,
                last_seq=row.progress_seq,
                last_status=row.status,
                pg_conn_factory=self._pg_conn_factory,
                listen_conn=self._listen_conn,
            )
        )
        async with contextlib.aclosing(gen) as agen:
            async for evt in agen:
                yield evt
                if evt.terminal:
                    return

    async def watch_reclaims(
        self,
        after_id: int = 0,
        *,
        poll_timeout: float | None = None,
    ) -> AsyncIterator[EventRow]:
        """Stream fleet-wide crash-reclaim events as :class:`EventRow` objects.

        **Delivery guarantee.** At-least-once, and gap-free *only under a
        bounded writer-transaction assumption*: an event is **silently and
        permanently missed** if a ``job_events`` writer transaction stays
        open longer than ``reclaim_event_visibility_delay`` (default 2s —
        see :data:`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`)
        between its INSERT and its COMMIT.  Ids are allocated at INSERT
        time but transactions commit out of order, so a late-committing
        lower-id row can land *behind* the cursor after the cursor has
        already advanced past its position; no error is raised anywhere.
        Sweep and terminal-write transactions are a handful of
        single-round-trip statements, so 2s is a generous bound under
        normal operation — but it is an assumption enforced by nothing in
        the SQL, not a property the query guarantees.  So this watcher
        does not leave detection to chance: on a slow cadence
        (``_VISIBILITY_RISK_CHECK_INTERVAL``, 60s) it runs the backend's
        ``check_reclaim_visibility_delay_risk`` diagnostic (when the
        backend implements it — PostgresBackend does) and logs a loud
        ``watch_reclaims-visibility-delay-at-risk`` warning for every
        long-open ``job_events`` writer it finds.  That is a proxy
        warning, not proof of an actual miss.

        Yields ``job_events`` rows with ``kind='state_change'`` and
        ``detail['reason']='lock_expired'``, ordered by the monotonic
        ``event_id`` cursor ascending.  ``to_state`` is ``'pending'`` for
        a retried reclaim, ``'crashed'`` or ``'cancelled'`` (cancel was
        in-flight when the worker died) for a terminal one.

        Cursor and duplicate semantics
        ------------------------------
        The caller persists the last-seen ``event_id`` and passes it back
        as *after_id* on resumption.  Persist the cursor **after**
        processing each event: a crash between processing and persisting
        re-delivers that event on the next run — delivery is
        at-least-once, so consumers must dedupe on ``event_id``.  The
        cursor is a watermark, not a reference: pruning ``job_events``
        rows at or below it is always safe, but rows pruned *before* the
        consumer reads them are gone for good — only prune rows older
        than your slowest consumer's cursor.  A cursor far behind after
        a long outage drains at query speed (full batches are re-polled
        immediately, not one batch per *poll_timeout*).

        Shutdown and backpressure
        -------------------------
        This is a pull-based async generator: events are fetched only as
        fast as the consumer iterates, so a slow consumer simply polls
        slower — no internal buffer grows.  To stop, break out of the
        ``async for`` (or cancel the consuming task); generator cleanup
        removes the LISTEN registration and closes any owned connection.

        Transport
        ---------
        On Postgres with a LISTEN transport source (``dsn``,
        ``pg_conn_factory``, or ``listen_conn``), the method LISTENs on
        ``wake_channel(schema)`` purely as a low-latency wakeup, but
        always polls ``backend.poll_reclaim_events(after_id)`` as the
        durable source of truth — NOTIFY is an optimisation, never the
        only path, and a dropped LISTEN connection degrades to (and
        recovers from) polling automatically.  Without a LISTEN
        transport or on Redis-configured backends, a plain poll loop
        against ``poll_reclaim_events`` runs on ``poll_timeout``.

        Usage::

            cursor = await load_cursor()  # your own durable store
            async for evt in tq.watch_reclaims(after_id=cursor):
                outstanding -= 1          # fan-out completion tracking
                cursor = evt.event_id
                await save_cursor(cursor)  # persist AFTER processing

        Raises
        ------
        RuntimeError
            Called before ``tq.open()`` or outside an ``async with`` block.
        """
        client = self._require_open()
        timeout = poll_timeout if poll_timeout is not None else self._poll_timeout

        has_listen_source = (
            self._dsn is not None
            or self._pg_conn_factory is not None
            or self._listen_conn is not None
        )
        if self._redis_client is None and has_listen_source:
            gen: AsyncGenerator[EventRow, None] = _watch_reclaims_pg(
                self._dsn,
                self._schema,
                client,
                timeout,
                after_id=after_id,
                pg_conn_factory=self._pg_conn_factory,
                listen_conn=self._listen_conn,
                visibility_delay=(
                    self._reclaim_event_visibility_delay
                    if self._reclaim_event_visibility_delay is not None
                    else RECLAIM_EVENT_VISIBILITY_DELAY
                ),
            )
        else:
            gen = _watch_reclaims_poll(client, timeout, after_id=after_id)

        async with contextlib.aclosing(gen) as agen:
            async for evt in agen:
                yield evt


async def _stream_pg(
    dsn: str | None,
    schema: str,
    job_id: JobId,
    client: JobsClient,
    poll_timeout: float,
    *,
    last_seq: int = -1,
    last_status: JobStatus | None = None,
    pg_conn_factory: "ConnFactory | None" = None,
    listen_conn: "asyncpg.Connection | None" = None,
) -> AsyncGenerator[JobEvent, None]:
    """PG LISTEN/NOTIFY transport for :meth:`TaskQ.stream`.

    Opens a dedicated asyncpg connection, registers a LISTEN callback on
    ``wake_channel(schema)``, and yields :class:`JobEvent` on each detected
    state change. Terminates on terminal state.

    Connection sources, in priority order:
    * ``listen_conn`` — pre-constructed, caller-owned; NOT closed here.
    * ``pg_conn_factory`` — zero-arg async factory; closed in ``finally``.
    * ``dsn`` — ``asyncpg.connect(dsn=...)``; closed in ``finally``.

    Raises :class:`RuntimeError` if none of the three is provided.

    If the LISTEN connection is killed mid-stream (e.g. by
    ``pg_terminate_backend``), the ``InterfaceError`` / ``OSError`` is
    caught and the stream falls back to poll-based re-fetch using
    ``asyncio.sleep(poll_timeout)``.  This provides single-recovery
    resilience without a full reconnect loop (out of scope for M5).
    """
    if listen_conn is None and pg_conn_factory is None and dsn is None:
        raise RuntimeError(
            "TaskQ.stream() requires a LISTEN transport source: pass 'dsn=', "
            "'pg_conn_factory=', or 'listen_conn=' to TaskQ. See "
            "docs/guides/managed-identities.md for AAD / pool-only setups."
        )

    import asyncpg

    wake = asyncio.Event()
    channel = wake_channel(schema)
    listen_alive = True
    owns_conn = listen_conn is None  # factory/DSN → we close; caller-owned → we don't

    def _on_notify(
        conn: asyncpg.Connection,
        pid: int,
        ch: str,
        payload: str,
    ) -> None:
        wake.set()

    if listen_conn is not None:
        conn = listen_conn
    elif pg_conn_factory is not None:
        conn = await pg_conn_factory()
    else:
        conn = await asyncpg.connect(dsn=str(dsn))
    try:
        await conn.add_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type — same pattern as worker/notify.py
        while True:
            wake.clear()
            row = await client.backend.get(job_id)
            if row is None:
                raise KeyError(job_id)
            if row.progress_seq != last_seq or row.status != last_status:
                last_seq = row.progress_seq
                last_status = row.status
                event = _row_to_event(row)
                yield event
                if event.terminal:
                    return
            try:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wake.wait(), timeout=poll_timeout)
            except (asyncpg.InterfaceError, OSError):
                if not listen_alive:
                    raise
                listen_alive = False
                logger.warning(
                    "stream-listen-connection-lost",
                    job_id=str(job_id),
                    error_type="InterfaceError/OSError",
                )
                while True:
                    await asyncio.sleep(poll_timeout)
                    row = await client.backend.get(job_id)
                    if row is None:
                        raise KeyError(job_id) from None
                    if row.progress_seq != last_seq or row.status != last_status:
                        last_seq = row.progress_seq
                        last_status = row.status
                        event = _row_to_event(row)
                        yield event
                        if event.terminal:
                            return
    finally:
        with contextlib.suppress(Exception):
            await conn.remove_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type — same pattern as worker/notify.py
        if owns_conn:
            # Why bounded: suppress(Exception) catches errors but not hangs —
            # asyncpg's close() passes no timeout underneath, so a dead PG
            # would wedge stream teardown (#37). The helper bounds the wait,
            # terminates on timeout, and never raises — subsuming the old
            # suppress.
            await close_conn_bounded(conn, "stream-pg", CLOSE_TIMEOUT_SECS)


async def _stream_redis(
    redis_client: Any,
    schema: str,
    job_id: JobId,
    client: JobsClient,
    poll_timeout: float,
    *,
    last_seq: int = -1,
    last_status: JobStatus | None = None,
) -> AsyncGenerator[JobEvent, None]:
    """Redis pub/sub transport for :meth:`TaskQ.stream`.

    Subscribes to ``progress_channel(schema, job_id)`` and yields
    :class:`JobEvent` on each received message. The Redis channel publishes
    :class:`~taskq.progress._events.ProgressEvent` JSON (not ``JobEvent``);
    on each message the authoritative row is re-fetched via
    ``backend.get(job_id)`` to produce a ``JobEvent``.
    """
    from taskq.client._transport import redis_event_stream

    channel = progress_channel(schema, job_id)
    state = {"last_seq": last_seq, "last_status": last_status}

    async def _refetch() -> JobEvent | None:
        row = await client.backend.get(job_id)
        if row is None:
            raise KeyError(job_id)
        if row.progress_seq != state["last_seq"] or row.status != state["last_status"]:
            state["last_seq"] = row.progress_seq
            state["last_status"] = row.status
            return _row_to_event(row)
        return None

    async def decode(raw_str: str) -> JobEvent | None:
        try:
            ProgressEvent.model_validate_json(raw_str)
        except Exception as exc:
            logger.warning(
                "stream-event-deserialise-error",
                job_id=str(job_id),
                error=repr(exc),
            )
            return None
        return await _refetch()

    async for event in redis_event_stream(
        redis_client,
        channel,
        poll_timeout=poll_timeout,
        decode_message=decode,
        on_timeout=_refetch,
    ):
        yield event


async def _watch_reclaims_poll(
    client: JobsClient,
    poll_timeout: float,
    *,
    after_id: int = 0,
) -> AsyncGenerator[EventRow, None]:
    """Poll-only transport for :meth:`TaskQ.watch_reclaims`.

    Repeatedly calls ``backend.poll_reclaim_events(cursor)`` and yields
    events in ascending ``event_id`` order.  Sleeps for *poll_timeout*
    when no new events are available; a non-empty batch is drained
    immediately (no sleep between pages), so a consumer resuming far
    behind catches up at query speed.
    """
    cursor = after_id
    last_risk_probe = asyncio.get_running_loop().time()
    while True:
        events = await client.backend.poll_reclaim_events(cursor, limit=_WATCH_RECLAIMS_BATCH_LIMIT)
        if events:
            for evt in events:
                cursor = evt.event_id
                yield evt
        else:
            await asyncio.sleep(poll_timeout)
        last_risk_probe = await _maybe_probe_visibility_risk(client, last_risk_probe)


_NOTIFY_CATCHUP_INTERVAL = 0.25
"""Retry cadence for :func:`_catch_up_after_notify`, in seconds."""

_RECONNECT_POLL_INTERVAL = 10
"""Poll iterations between LISTEN-reconnect attempts in
:func:`_watch_reclaims_pg`'s degraded (connection-lost) fallback loop."""

_WATCH_RECLAIMS_BATCH_LIMIT = 100
"""Batch size for ``poll_reclaim_events`` calls in the watch_reclaims
transports.  Passed explicitly (rather than relying on the protocol
default) so a *full* batch can be detected and drained by re-polling
immediately — see :func:`_watch_reclaims_pg`."""

_VISIBILITY_RISK_CHECK_INTERVAL = 60.0
"""Cadence (seconds) between built-in ``check_reclaim_visibility_delay_risk``
probes in the watch_reclaims transports — one extra ``pg_locks`` /
``pg_stat_activity`` query per watcher per interval.  Module-level so
tests can shrink it.  The probe is a no-op on backends that do not
implement the diagnostic (e.g. InMemoryBackend), so the poll-only
transport and the PG LISTEN transport share it unconditionally."""


_VISIBILITY_PROBE_TIMEOUT = 5.0
"""Timeout for a single visibility-risk probe query, in seconds.  The
probe runs inline on the delivery loop, so it must be timeboxed: pool
exhaustion or a slow catalog scan must stall the watcher for at most
this long — a monitoring path must never take down the delivery path it
monitors, by hanging any more than by raising.  Generous for a
``pg_locks``/``pg_stat_activity`` join.  Module-level so tests can
shrink it."""


async def _probe_visibility_risk(client: JobsClient) -> None:
    """Run the backend's visibility-delay risk diagnostic, if it has one,
    and log a loud structured warning for every long-open ``job_events``
    writer it finds.

    This is the detection half of the ``RECLAIM_EVENT_VISIBILITY_DELAY``
    contract, wired in *by default*: the trailing-watermark filter
    assumes every ``job_events`` writer commits within the margin of its
    INSERT, nothing in SQL enforces that, and a violation is otherwise a
    silently missed event.  Running the diagnostic here turns "the
    operator must know ``check_reclaim_visibility_delay_risk`` exists and
    opt in" into "the watcher notices and says so loudly".  Probe
    failures — including a hang past ``_VISIBILITY_PROBE_TIMEOUT`` — are
    logged and swallowed: a monitoring path must never take down the
    delivery path it monitors.
    """
    check = getattr(client.backend, "check_reclaim_visibility_delay_risk", None)
    if check is None:
        return
    try:
        writers = await asyncio.wait_for(check(), timeout=_VISIBILITY_PROBE_TIMEOUT)
    except Exception:
        logger.warning("watch-reclaims-visibility-risk-probe-failed", exc_info=True)
        return
    for w in writers:
        logger.warning(
            "watch_reclaims-visibility-delay-at-risk",
            pid=w.pid,
            xact_age_seconds=round(w.xact_age_seconds, 3),
            xact_start=w.xact_start.isoformat(),
            action=(
                "a job_events writer transaction has been open longer than "
                "the visibility-delay margin; if it commits a lower-id row "
                "after the cursor passes it, that event is silently missed — "
                "investigate the stalled writer or raise "
                "reclaim_event_visibility_delay"
            ),
        )


async def _maybe_probe_visibility_risk(client: JobsClient, last_probe: float) -> float:
    """Time-gated wrapper for :func:`_probe_visibility_risk`: runs at most
    once per ``_VISIBILITY_RISK_CHECK_INTERVAL`` seconds.  Returns the
    timestamp of the probe actually run, or *last_probe* unchanged."""
    now = asyncio.get_running_loop().time()
    if now - last_probe < _VISIBILITY_RISK_CHECK_INTERVAL:
        return last_probe
    await _probe_visibility_risk(client)
    return now


async def _catch_up_after_notify(
    client: JobsClient,
    cursor: int,
    visibility_delay: timedelta = RECLAIM_EVENT_VISIBILITY_DELAY,
) -> AsyncGenerator[EventRow, None]:
    """Bounded short-interval re-poll after a NOTIFY-driven wake finds
    nothing yet, bridging ``visibility_delay`` without waiting for a full
    (possibly much longer) ``poll_timeout`` cycle.

    Gives up once *visibility_delay* has elapsed since the wake — at that
    point either the event has appeared (yielded already) or the writing
    transaction is taking longer than the margin assumes, in which case
    the caller's normal poll_timeout cadence takes back over.

    *visibility_delay* must match the value used by
    ``backend.poll_reclaim_events`` so the catch-up retries for exactly
    as long as the trailing watermark holds rows back. The default falls
    back to ``RECLAIM_EVENT_VISIBILITY_DELAY`` for standalone use.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + visibility_delay.total_seconds()
    while loop.time() < deadline:
        events = await client.backend.poll_reclaim_events(cursor)
        if events:
            for evt in events:
                cursor = evt.event_id
                yield evt
            return
        await asyncio.sleep(_NOTIFY_CATCHUP_INTERVAL)


async def _watch_reclaims_pg(
    dsn: str | None,
    schema: str,
    client: JobsClient,
    poll_timeout: float,
    *,
    after_id: int = 0,
    pg_conn_factory: "ConnFactory | None" = None,
    listen_conn: "asyncpg.Connection | None" = None,
    visibility_delay: timedelta = RECLAIM_EVENT_VISIBILITY_DELAY,
) -> AsyncGenerator[EventRow, None]:
    """PG LISTEN/NOTIFY transport for :meth:`TaskQ.watch_reclaims`.

    Opens a dedicated asyncpg connection, registers a LISTEN callback on
    ``wake_channel(schema)``, and polls ``poll_reclaim_events`` on each
    wakeup or timeout.  NOTIFY is a low-latency optimisation; the
    durable source of truth is the ``poll_reclaim_events`` cursor.  A
    NOTIFY-triggered poll can still come up empty if the event hasn't
    cleared ``poll_reclaim_events``' visibility delay yet — see
    :func:`_catch_up_after_notify`, which bridges that margin on a short
    cadence instead of falling back to a full (possibly much longer)
    ``poll_timeout`` wait.

    Connection sources, in priority order:
    * ``listen_conn`` — pre-constructed, caller-owned; NOT closed here.
    * ``pg_conn_factory`` — zero-arg async factory; closed in ``finally``.
    * ``dsn`` — ``asyncpg.connect(dsn=...)``; closed in ``finally``.

    If none of the three is provided, falls back to pure polling via
    :func:`_watch_reclaims_poll` (does NOT raise, matching
    ``watch_reclaims``'s contract).

    LISTEN-connection-loss detection
    --------------------------------
    asyncpg does NOT raise into a coroutine that isn't awaiting on the
    connection: when the LISTEN connection dies (``pg_terminate_backend``,
    network drop), notifications simply stop and nothing in the consume
    loop ever sees an exception.  Connection death is therefore
    *detected*, not caught: a termination listener wakes the loop
    promptly, and ``conn.is_closed()`` is checked every iteration, so
    detection is bounded by *poll_timeout* even if the wakeup is somehow
    missed.  (An earlier revision wrapped ``wake.wait()`` in
    ``except (asyncpg.InterfaceError, OSError)`` — unreachable from a
    real LISTEN drop for the reason above, and reachable only from
    ``_catch_up_after_notify``'s *pool* polls, which it misdiagnosed.)

    Once detected:

    * caller-supplied ``listen_conn`` — the caller owns the connection's
      lifecycle, so reconnection is impossible; the generator logs one
      ``watch-reclaims-listen-connection-lost`` warning and settles into
      a permanent poll fallback (documented limitation).
    * owned connection (``pg_conn_factory`` / ``dsn``) — the generator
      polls as a fallback and, every ``_RECONNECT_POLL_INTERVAL`` poll
      iterations, attempts to re-establish the LISTEN connection via the
      same factory/DSN path (timeboxed at *poll_timeout* so a hanging
      factory/DSN cannot stall delivery).  On success it logs
      ``watch_reclaims-listen-reconnected`` and resumes the LISTEN-driven
      loop.  On failure it keeps polling and retries after the same
      interval; ``watch_reclaims-reconnect-still-failing`` is logged on
      the first failure and every 10th thereafter — a long outage leaves
      evidence without spamming every attempt.

    Backend (pool) errors from ``poll_reclaim_events`` are NOT swallowed
    or mistaken for LISTEN failure — they propagate to the caller, who
    can resume from the last-seen cursor.

    Backlog draining
    ----------------
    After yielding a *full* batch (``len ==
    _WATCH_RECLAIMS_BATCH_LIMIT``) the loop re-polls immediately instead
    of waiting for NOTIFY/timeout — a crash storm larger than one batch
    would otherwise drain at only ``_WATCH_RECLAIMS_BATCH_LIMIT`` events
    per *poll_timeout* (a minute for 250 events at the 30s default).
    """
    if listen_conn is None and pg_conn_factory is None and dsn is None:
        async for evt in _watch_reclaims_poll(client, poll_timeout, after_id=after_id):
            yield evt
        return

    import asyncpg

    wake = asyncio.Event()
    channel = wake_channel(schema)
    cursor = after_id
    owns_conn = listen_conn is None
    last_risk_probe = asyncio.get_running_loop().time()

    def _on_notify(
        conn: asyncpg.Connection,
        pid: int,
        ch: str,
        payload: str,
    ) -> None:
        wake.set()

    def _on_terminate(conn: asyncpg.Connection) -> None:
        # Fires on any termination — server-side kill, network drop, or a
        # deliberate close().  Wake the consume loop so the dead
        # connection is noticed at the next is_closed() check instead of
        # waiting out the full poll_timeout.
        wake.set()

    async def _open_listen_conn() -> asyncpg.Connection:
        if pg_conn_factory is not None:
            return await pg_conn_factory()
        return await asyncpg.connect(dsn=str(dsn))

    if listen_conn is not None:
        conn = listen_conn
    elif pg_conn_factory is not None:
        conn = await pg_conn_factory()
    else:
        conn = await asyncpg.connect(dsn=str(dsn))
    try:
        await conn.add_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type — same pattern as _stream_pg
        conn.add_termination_listener(_on_terminate)  # pyright: ignore[reportArgumentType]  # Why: asyncpg stubs over-narrow the callback type — same pattern as add_listener above
        while True:
            last_risk_probe = await _maybe_probe_visibility_risk(client, last_risk_probe)
            if conn.is_closed():
                logger.warning("watch-reclaims-listen-connection-lost")
                if not owns_conn:
                    # Caller-supplied connection — cannot reconnect;
                    # permanent poll-fallback is the documented limitation.
                    full_batch = False
                    while True:
                        if not full_batch:
                            await asyncio.sleep(poll_timeout)
                        last_risk_probe = await _maybe_probe_visibility_risk(
                            client, last_risk_probe
                        )
                        events = await client.backend.poll_reclaim_events(
                            cursor, _WATCH_RECLAIMS_BATCH_LIMIT
                        )
                        if events:
                            for evt in events:
                                cursor = evt.event_id
                                yield evt
                        full_batch = len(events) == _WATCH_RECLAIMS_BATCH_LIMIT
                # Owned connection — poll as fallback while periodically
                # attempting to re-establish the LISTEN connection.
                poll_iters = 0
                failed_attempts = 0
                full_batch = False
                while True:
                    if not full_batch:
                        await asyncio.sleep(poll_timeout)
                    last_risk_probe = await _maybe_probe_visibility_risk(client, last_risk_probe)
                    events = await client.backend.poll_reclaim_events(
                        cursor, _WATCH_RECLAIMS_BATCH_LIMIT
                    )
                    if events:
                        for evt in events:
                            cursor = evt.event_id
                            yield evt
                    full_batch = len(events) == _WATCH_RECLAIMS_BATCH_LIMIT
                    poll_iters += 1
                    if poll_iters % _RECONNECT_POLL_INTERVAL != 0:
                        continue
                    new_conn = None
                    try:
                        # Timeboxed at poll_timeout: asyncpg.connect()
                        # defaults to 60s and a user pg_conn_factory is
                        # unbounded — a hanging attempt must not stall the
                        # degraded poll loop, the only live delivery path.
                        new_conn = await asyncio.wait_for(_open_listen_conn(), timeout=poll_timeout)
                        await new_conn.add_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: same asyncpg stub narrowing as above
                    except Exception:
                        if new_conn is not None:
                            # Why bounded: a conn that failed LISTEN setup may
                            # be half-dead, and an unbounded close against a
                            # dead PG would wedge the degraded poll loop — the
                            # only live delivery path at this point. Matches
                            # the notify.py reconnect close this PR converted;
                            # the helper never raises.
                            await close_conn_bounded(
                                new_conn,
                                "watch-reclaims-reconnect",
                                CLOSE_TIMEOUT_SECS,
                                mid_run=True,
                            )
                        failed_attempts += 1
                        if failed_attempts == 1 or failed_attempts % 10 == 0:
                            logger.warning(
                                "watch_reclaims-reconnect-still-failing",
                                poll_attempts=poll_iters,
                                failed_attempts=failed_attempts,
                            )
                        continue
                    # Success — swap the dead connection for the new one
                    # and resume the LISTEN-driven outer loop.
                    new_conn.add_termination_listener(_on_terminate)  # pyright: ignore[reportArgumentType]  # Why: same asyncpg stub narrowing as the initial registration
                    # Why bounded: the conn being swapped out was already
                    # diagnosed dead — the sharpest close()-hang case, and
                    # suppress(Exception) cannot stop a call that never
                    # returns (asyncpg passes no close timeout underneath).
                    # Same mid-run class as the failed-reconnect close above
                    # and the notify.py old-conn close this PR converted.
                    await close_conn_bounded(
                        conn, "watch-reclaims-reconnect", CLOSE_TIMEOUT_SECS, mid_run=True
                    )
                    conn = new_conn
                    wake = asyncio.Event()
                    logger.info(
                        "watch_reclaims-listen-reconnected",
                        poll_attempts=poll_iters,
                    )
                    break
                continue
            events = await client.backend.poll_reclaim_events(cursor, _WATCH_RECLAIMS_BATCH_LIMIT)
            if events:
                for evt in events:
                    cursor = evt.event_id
                    yield evt
            if len(events) == _WATCH_RECLAIMS_BATCH_LIMIT:
                # Full batch — the backlog is likely not drained; re-poll
                # immediately instead of waiting out poll_timeout.
                continue
            woke_via_notify = False
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=poll_timeout)
                woke_via_notify = True
            # Clear AFTER the wait, not before the poll: a NOTIFY arriving
            # mid-poll or mid-drain (full-batch loop) is then consumed
            # exactly once here instead of being silently discarded — which
            # would stall a notified event for up to poll_timeout.
            wake.clear()
            if conn.is_closed():
                # Died during the wait (the termination listener woke us,
                # or poll_timeout elapsed) — handled at the top of the loop.
                continue
            if woke_via_notify:
                # A NOTIFY fired, but the event that triggered it may
                # not have cleared poll_reclaim_events' configured
                # visibility delay yet (see visibility_delay,
                # threaded through from watch_reclaims) — an
                # immediate re-poll (top of the next loop iteration)
                # can still come up empty.  Retry on a short, bounded
                # cadence until it appears, rather than falling back
                # to the full (possibly much longer) poll_timeout —
                # otherwise the low-latency wakeup this LISTEN
                # transport exists for would be silently defeated by
                # that margin.
                async for evt in _catch_up_after_notify(
                    client, cursor, visibility_delay=visibility_delay
                ):
                    cursor = evt.event_id
                    yield evt
    finally:
        # Separate suppress blocks, termination listener first: it is
        # synchronous (cannot fail on the wire), while remove_listener
        # issues an UNLISTEN query that CAN raise on a live connection
        # dropped mid-teardown — grouping them would skip the removal and
        # leak _on_terminate (closing over this frame) on a caller-owned
        # connection reused across watch_reclaims() calls.
        with contextlib.suppress(Exception):
            conn.remove_termination_listener(_on_terminate)  # pyright: ignore[reportArgumentType]  # Why: same asyncpg stub narrowing as add_termination_listener
        with contextlib.suppress(Exception):
            await conn.remove_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: same pattern as _stream_pg
        if owns_conn:
            # Why bounded: same dead-PG close()-hang class as _stream_pg
            # above (#37) — suppress cannot stop a close that never returns.
            await close_conn_bounded(conn, "watch-reclaims", CLOSE_TIMEOUT_SECS)

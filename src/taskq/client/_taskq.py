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
from collections.abc import AsyncGenerator, AsyncIterator
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

from taskq.actor import ActorRef
from taskq.backend._protocol import (
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
from taskq.batch import BatchHandle, EnqueueItem
from taskq.client._handle import JobHandle
from taskq.client._jobs import JobsClient
from taskq.constants import RECLAIM_EVENT_VISIBILITY_DELAY, progress_channel, wake_channel
from taskq.cron import ScheduleHandle
from taskq.progress._events import ProgressEvent
from taskq.types import CancelResult

__all__ = ["EventRow", "JobEvent", "TaskQ"]

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
        if self._owns_pool and self._pool is not None:
            await self._pool.close()
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
    ) -> BatchHandle:
        """Enqueue multiple jobs in a single batched INSERT.

        Delegates to :meth:`JobsClient.enqueue_batch`; see its docstring
        for validation rules and idempotency-key collision semantics.
        """
        return await self._require_open().enqueue_batch(
            items,
            batch_id=batch_id,
            connection=connection,
        )

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
        """List jobs matching *filter*, returning a :class:`JobPage`."""
        return await self._require_open().list(filter)

    async def cancel(
        self,
        job_id: JobId,
        reason: str | None = None,
    ) -> CancelResult:
        """Request cancellation of a job. Raises :class:`KeyError` if not found."""
        return await self._require_open().cancel(job_id, reason)

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

        Yields ``job_events`` rows with ``kind='state_change'`` and
        ``detail['reason']='lock_expired'``, ordered by the monotonic
        ``event_id`` cursor ascending.  The caller persists the last-seen
        ``event_id`` and passes it back as *after_id* on resumption to
        achieve idempotent at-least-once consumption.

        On Postgres with a LISTEN transport source (``dsn``,
        ``pg_conn_factory``, or ``listen_conn``), the method LISTENs on
        ``wake_channel(schema)`` purely as a low-latency wakeup, but
        always polls ``backend.poll_reclaim_events(after_id)`` as the
        durable source of truth — NOTIFY is an optimisation, never the
        only path.

        Without a LISTEN transport or on Redis-configured backends, a
        plain poll loop against ``poll_reclaim_events`` runs on
        ``poll_timeout``.

        Usage::

            cursor = 0
            async for evt in tq.watch_reclaims(after_id=cursor):
                cursor = evt.event_id
                # decrement outstanding-work counter, etc.

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
            with contextlib.suppress(Exception):
                await conn.close()


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
    when no new events are available.
    """
    cursor = after_id
    while True:
        events = await client.backend.poll_reclaim_events(cursor)
        if events:
            for evt in events:
                cursor = evt.event_id
                yield evt
        else:
            await asyncio.sleep(poll_timeout)


_NOTIFY_CATCHUP_INTERVAL = 0.25
"""Retry cadence for :func:`_catch_up_after_notify`, in seconds."""

_RECONNECT_POLL_INTERVAL = 10
"""Poll iterations between LISTEN-reconnect attempts in
:func:`_watch_reclaims_pg`'s degraded (connection-lost) fallback loop."""


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

    Reconnect behaviour on LISTEN-connection loss
    ---------------------------------------------
    When the connection is owned by this function (``pg_conn_factory`` or
    ``dsn`` path — not caller-supplied ``listen_conn``), a transient
    ``InterfaceError`` / ``OSError`` logs one warning and falls back to
    the poll loop, but every ``_RECONNECT_POLL_INTERVAL`` iterations the
    function attempts to re-establish the LISTEN connection via the same
    factory/DSN path.  On success it logs a recovery message and resumes
    the LISTEN-driven outer loop.  On failure it keeps polling and
    retries after the same interval — warnings on repeated failures are
    logged at a cadence (not every attempt) to avoid log spam.

    When the connection is caller-supplied (``listen_conn``), this
    function cannot reconnect it — the caller owns that connection's
    lifecycle — so the permanent poll-fallback is the best available
    behaviour and reconnection is not attempted.
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

    def _on_notify(
        conn: asyncpg.Connection,
        pid: int,
        ch: str,
        payload: str,
    ) -> None:
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
        while True:
            wake.clear()
            events = await client.backend.poll_reclaim_events(cursor)
            if events:
                for evt in events:
                    cursor = evt.event_id
                    yield evt
            try:
                woke_via_notify = False
                try:
                    await asyncio.wait_for(wake.wait(), timeout=poll_timeout)
                    woke_via_notify = True
                except TimeoutError:
                    pass
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
            except (asyncpg.InterfaceError, OSError):
                logger.warning("watch_reclaims-listen-connection-lost")
                if not owns_conn:
                    # Caller-supplied connection — we cannot reconnect it;
                    # permanent poll-fallback is the documented limitation.
                    while True:
                        await asyncio.sleep(poll_timeout)
                        events = await client.backend.poll_reclaim_events(cursor)
                        if events:
                            for evt in events:
                                cursor = evt.event_id
                                yield evt
                # Owned connection — poll as fallback while periodically
                # attempting to re-establish the LISTEN connection.
                poll_iters = 0
                reconnect_failed_logged = False
                while True:
                    await asyncio.sleep(poll_timeout)
                    events = await client.backend.poll_reclaim_events(cursor)
                    if events:
                        for evt in events:
                            cursor = evt.event_id
                            yield evt
                    poll_iters += 1
                    if poll_iters % _RECONNECT_POLL_INTERVAL != 0:
                        continue
                    try:
                        new_conn = await _open_listen_conn()
                    except Exception:
                        if not reconnect_failed_logged:
                            logger.warning(
                                "watch_reclaims-reconnect-still-failing",
                                poll_attempts=poll_iters,
                            )
                            reconnect_failed_logged = True
                        continue
                    reconnect_failed_logged = False
                    try:
                        await new_conn.add_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: same asyncpg stub narrowing as above
                    except Exception:
                        with contextlib.suppress(Exception):
                            await new_conn.close()
                        if not reconnect_failed_logged:
                            logger.warning(
                                "watch_reclaims-reconnect-listen-failed",
                                poll_attempts=poll_iters,
                            )
                            reconnect_failed_logged = True
                        continue
                    # Success — swap the dead connection for the new one
                    # and resume the LISTEN-driven outer loop.
                    with contextlib.suppress(Exception):
                        await conn.remove_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: same asyncpg stub narrowing as above
                    if owns_conn:
                        with contextlib.suppress(Exception):
                            await conn.close()
                    conn = new_conn
                    wake = asyncio.Event()
                    logger.info(
                        "watch_reclaims-listen-reconnected",
                        poll_attempts=poll_iters,
                    )
                    break
    finally:
        with contextlib.suppress(Exception):
            await conn.remove_listener(channel, _on_notify)  # pyright: ignore[reportArgumentType]  # Why: same pattern as _stream_pg
        if owns_conn:
            with contextlib.suppress(Exception):
                await conn.close()

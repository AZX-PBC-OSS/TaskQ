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
from taskq.constants import progress_channel, wake_channel
from taskq.cron import ScheduleHandle
from taskq.progress._events import ProgressEvent
from taskq.types import CancelResult

__all__ = ["JobEvent", "TaskQ"]

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

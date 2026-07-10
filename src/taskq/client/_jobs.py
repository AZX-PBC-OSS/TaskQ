"""JobsClient — the primary entry point for enqueuing, querying, and
cancelling jobs, and managing cron schedules.

Wraps a :class:`~taskq.backend._protocol.Backend` instance and adds the
client-layer behaviours the protocol intentionally omits: payload
serialization through the actor's ``payload_type``, ``CancelResult``
construction in :meth:`cancel`, typed :class:`JobHandle[R]`
wrapping in :meth:`enqueue` / :meth:`get`, and cron schedule management
via :meth:`create_schedule`, :meth:`list_schedules`,
:meth:`update_schedule`, :meth:`delete_schedule`.

The backend is injected at construction so the same client can target
either an :class:`~taskq.testing.in_memory.InMemoryBackend` (tests) or a
:class:`taskq.backend.postgres.PostgresBackend` (production).
"""

from collections.abc import Generator
from contextlib import AsyncExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from croniter import croniter
from pydantic import BaseModel, TypeAdapter, ValidationError

from taskq.actor import ActorRef
from taskq.backend._cursor import encode_cursor
from taskq.backend._protocol import (
    Backend,
    DstStrategy,
    IdempotencyKey,
    IdentityKey,
    JobFilter,
    JobId,
    JobPage,
    QueueName,
    ScheduleCreateArgs,
    ScheduleUpdateArgs,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.batch import BatchHandle, EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args, enqueue_span
from taskq.client._handle import JobHandle
from taskq.exceptions import PayloadValidationError, SchemaNotMigratedError
from taskq.types import CancelResult

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.backend._protocol import ScheduleRecord
    from taskq.cron import ScheduleHandle
    from taskq.settings import TaskQSettings

__all__ = ["JobsClient"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class JobsClient:
    """Public API for job operations.

    Delegates to the injected :class:`Backend` and wraps results in
    typed :class:`JobHandle[R]` instances. The client owns the
    ``payload``-serialization step that turns a typed ``P`` into the
    ``dict[str, object]`` carried by :class:`EnqueueArgs`; the backend
    sees only erased payloads.
    """

    def __init__(
        self,
        backend: Backend,
        *,
        clock: Clock | None = None,
        settings: "TaskQSettings | None" = None,
    ) -> None:
        self._backend = backend
        self._clock = clock if clock is not None else SystemClock()
        self._settings: "TaskQSettings | None" = settings  # noqa: UP037  # Why: TaskQSettings is under TYPE_CHECKING; string annotation avoids runtime import.
        self._redis_client: "redis_async.Redis | None" = None  # type: ignore[type-arg]  # noqa: UP037  # Why: redis_async is under TYPE_CHECKING; string annotation avoids runtime import. type-arg: redis-py stubs expose Redis as an unparameterised generic.
        self._exit_stack: AsyncExitStack = AsyncExitStack()
        self._warned_unique_for: set[str] = set()

    @property
    def backend(self) -> Backend:
        """The underlying :class:`Backend` this client delegates to.

        Exposed so :class:`JobHandle` can read the backend through the
        client without accessing the private ``_backend`` attribute.
        """
        return self._backend

    @contextmanager
    def _translate_schema_errors(self) -> Generator[None, None, None]:
        """Translate a missing-schema asyncpg error into an actionable one.

        ``asyncpg.exceptions.UndefinedTableError`` surfaces as a raw
        Postgres error (``relation "taskq.jobs" does not exist``) when the
        TaskQ schema hasn't been migrated yet. Wrap it in
        :class:`~taskq.exceptions.SchemaNotMigratedError` — chained via
        ``from exc`` so the original traceback is preserved — with a
        message pointing at ``taskq migrate up`` / ``TASKQ_MIGRATE_ON_START``.
        """
        import asyncpg

        try:
            yield
        except asyncpg.exceptions.UndefinedTableError as exc:
            schema = self._settings.schema_name if self._settings is not None else "taskq"
            raise SchemaNotMigratedError(schema) from exc

    async def _open_redis(self, settings: "TaskQSettings") -> None:
        """Open a Redis client when ``settings.redis_url`` is not ``None``.

        Called by :class:`TaskQ.open()` after constructing the client.
        The Redis client is entered on :attr:`_exit_stack` for LIFO teardown.
        Uses ``decode_responses=False`` (bytes mode) consistent with the
        LOOP-scoped client pattern.

        Raises :class:`ImportError` when ``redis_url`` is set but the
        ``[redis]`` extra is not installed.
        """
        if settings.redis_url is not None:
            try:
                import redis.asyncio as redis_async
            except ImportError as exc:
                raise ImportError(
                    "redis_url is configured but the [redis] extra is not installed. "
                    "Install it with: pip install 'taskq[redis]'"
                ) from exc
            self._redis_client = await self._exit_stack.enter_async_context(
                redis_async.from_url(str(settings.redis_url), decode_responses=False)
            )
        self._settings = settings

    async def close(self) -> None:
        """Close the Redis client and release resources via the exit stack."""
        await self._exit_stack.aclose()
        self._redis_client = None

    # ── Enqueue ────────────────────────────────────────────────────────

    def _maybe_warn_unique_for_no_identity[P: BaseModel, R: BaseModel | None](
        self, ref: ActorRef[P, R]
    ) -> None:
        if ref.name in self._warned_unique_for:
            return
        self._warned_unique_for.add(ref.name)
        unique_for = ref.unique_for
        assert unique_for is not None  # guarded by the call site
        # When @actor gains an identity callable parameter, this warning can
        # be moved to _build_ref alongside the actor-config-* family at
        # actor.py:551-578 — the log event name stays the same.
        logger.warning(
            "actor_config_unique_for_ignored",
            kind="actor_config_unique_for_ignored",
            actor=ref.name,
            queue=ref.queue,
            unique_for_seconds=unique_for.total_seconds(),
            reason="unique_for is set but identity_key was not provided at enqueue; "
            "unique_for is a no-op without an identity_key",
        )

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
        """Enqueue a job for the given actor and return a typed handle.

        The payload is serialized through ``ref.payload_type`` so the
        ``EnqueueArgs.payload`` carried over the backend boundary is a
        plain ``dict[str, object]`` ready for the JSONB column. The
        returned :class:`JobHandle[R]` carries ``ref.result_adapter`` so
        :meth:`JobHandle.wait` can validate the stored result back to
        ``R``.

        The ``metadata.singleton`` key is reserved by the library for
        singleton enforcement. When ``ref.singleton`` is ``True`` the
        library unconditionally writes ``metadata.singleton = True``,
        overriding any caller-supplied value. Callers MUST NOT set
        ``metadata.singleton`` manually.

        **max_pending:**

        - When the actor's ``max_pending`` is set, a pre-flight count of
          ``pending`` + ``scheduled`` jobs for the actor is compared to the
          limit. If ``count >= max_pending``, :class:`MaxPendingExceededError`
          is raised synchronously — the caller decides whether to retry,
          fail, or wait; the library does not block on capacity.

        - Evaluation order at enqueue: ``unique_for`` dedup →
          singleton pre-flight → ``max_pending`` count check →
          ``idempotency_key`` INSERT → job INSERT. A ``unique_for`` hit
          bypasses all remaining checks; a singleton collision fires before
          ``max_pending`` to give the caller the more specific
          ``SingletonCollisionError``.

        - ``idempotency_key`` does **not** bypass ``max_pending`` — the
          idempotency ON CONFLICT fires at step 5, after the max_pending
          check at step 3. Re-enqueuing with a duplicate
          ``idempotency_key`` when the queue is full raises
          ``MaxPendingExceededError``, not the deduplicated handle. Only
          ``unique_for`` (step 1) bypasses max_pending.

        **idempotency_key:**

        - ``idempotency_key`` is **globally unique** (not per-actor scoped).
          Callers must namespace keys to avoid collisions between actors
          (e.g. ``"actor_name:delivery_id"``).

        - Key length is bounded at **256 characters**. Empty keys and
          whitespace-only keys raise :class:`ValueError` at the client
          boundary before any backend call.

        **unique_for:**

        - ``unique_for`` deduplication is **best-effort**. Concurrent
          enqueues for the same ``(actor, identity_key)`` may both insert;
          the dispatch CTE's ``running_identities`` filter ensures only one
          runs.

        - When either dedup mechanism matches an existing job,
          ``JobHandle.was_existing`` is ``True``. This field replaces the
          need for callers to inspect the row's ``created_at`` to detect a
          dedup return.
        """
        resolved_queue = queue if queue is not None else ref.queue
        identity_key_str = str(identity_key) if identity_key is not None else ""

        with enqueue_span(ref.name, resolved_queue, identity_key=identity_key_str) as (
            span,
            extracted_trace_id,
            extracted_span_id,
        ):
            args = build_enqueue_args(
                ref,
                payload,
                queue=queue,
                scheduled_at=scheduled_at,
                priority=priority,
                fairness_key=fairness_key,
                metadata=metadata,
                identity_key=identity_key,
                idempotency_key=idempotency_key,
                trace_id=extracted_trace_id,
                span_id=extracted_span_id,
                schedule_to_close=schedule_to_close,
                start_to_close=start_to_close,
                heartbeat_timeout=heartbeat_timeout,
                tags=tags,
                clock=self._clock,
            )
            span.set_attribute("messaging.message.id", str(args.id))
            if ref.unique_for is not None and args.identity_key is None:
                self._maybe_warn_unique_for_no_identity(ref)
            with self._translate_schema_errors():
                row = await self._backend.enqueue(args)

        if row.id == args.id:
            logger.debug(
                "job_enqueued",
                kind="job_enqueued",
                job_id=row.id,
                actor=row.actor,
                queue=row.queue,
                idempotency_key=row.idempotency_key,
            )
        return JobHandle(
            client=self,
            row=row,
            result_adapter=ref.result_adapter,
            was_existing=(row.id != args.id),
            _redis_client=self._redis_client,
            _settings=self._settings,
        )

    async def enqueue_batch(
        self,
        items: list[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
    ) -> BatchHandle:
        """Enqueue multiple jobs in a single batched INSERT and return a
        :class:`~taskq.batch.BatchHandle`.

        All ``items`` share a single ``batch_id`` UUID written into each
        job's ``metadata.batch_id`` field (as a string).  When
        ``batch_id`` is not supplied it is auto-generated as a UUIDv7 via
        :func:`~taskq._ids.new_job_id`.

        **Validation rules:**

        - ``len(items) == 0`` raises :class:`ValueError`.
        - ``len(items) > 1000`` raises :class:`ValueError`.
        - ALL payloads are validated before any INSERT.  A single failure
          raises :class:`~taskq.exceptions.PayloadValidationError` and
          leaves no rows inserted.

        **max_pending:**

        One aggregated ``SELECT actor, count(*) … WHERE actor = ANY($1)
        GROUP BY actor`` is issued for the entire batch.  Per-actor limits
        are checked before the INSERT; any violation raises
        :class:`~taskq.exceptions.MaxPendingExceededError`.

        **idempotency_key collisions:**

        Items whose ``idempotency_key`` collides with an existing row
        return the existing :class:`~taskq.client.JobHandle` (same
        semantics as single-item :meth:`enqueue`).
        """
        from taskq._ids import new_job_id

        if len(items) == 0:
            raise ValueError("items must not be empty")
        if len(items) > 1000:
            raise ValueError(f"items must contain at most 1000 entries, got {len(items)}")

        # Auto-generate batch_id if not provided (UUIDv7)
        resolved_batch_id = UUID(bytes=new_job_id().bytes) if batch_id is None else batch_id

        # Phase 1: Validate ALL payloads (and idempotency keys) before any I/O
        for i, item in enumerate(items):
            ref = item.actor_ref
            try:
                ref.payload_type.model_validate(item.payload)
            except ValidationError as exc:
                # exc.errors() returns list[ErrorDetails] (TypedDict); cast to
                # the erased dict[str, object] expected by PayloadValidationError.
                errs: list[dict[str, object]] = exc.errors()  # type: ignore[assignment]  # Why: pydantic v2 ErrorDetails is a TypedDict (subtype of dict[str, Any]); assignment to list[dict[str,object]] is safe at runtime but pyright cannot prove covariance
                raise PayloadValidationError(
                    f"Payload validation failed for item {i} (actor={ref.name!r}): {exc}",
                    actor=ref.name,
                    validation_errors=errs,
                ) from exc
            if item.idempotency_key is not None:
                if item.idempotency_key == "":
                    raise ValueError(f"idempotency_key for item {i} must not be empty")
                if item.idempotency_key.strip() == "":
                    raise ValueError(f"idempotency_key for item {i} must not be whitespace-only")
                if len(item.idempotency_key) > 256:
                    raise ValueError(
                        f"idempotency_key for item {i} must be at most 256 characters, "
                        f"got {len(item.idempotency_key)}"
                    )

        # Phase 2: Aggregated max_pending check (one query for the whole batch)
        # Collect actors that declare max_pending
        actors_with_limit: dict[str, int] = {}
        for item in items:
            ref = item.actor_ref
            mp = ref.max_pending
            if mp is not None and ref.name not in actors_with_limit:
                actors_with_limit[ref.name] = mp

        if actors_with_limit:
            # One aggregated query for all actors that declare max_pending.
            existing_counts = await self._backend.count_pending_jobs(list(actors_with_limit.keys()))
            for actor_name, limit in actors_with_limit.items():
                batch_count = sum(1 for it in items if it.actor_ref.name == actor_name)
                existing_pending_count = existing_counts.get(actor_name, 0)
                if existing_pending_count + batch_count >= limit:
                    from taskq.exceptions import MaxPendingExceededError

                    raise MaxPendingExceededError(
                        actor=actor_name,
                        current_count=existing_pending_count,
                        max_pending=limit,
                    )

        # Phase 3: Build per-item EnqueueArgs
        args_list = build_batch_args(items, resolved_batch_id, self._clock)

        # Phase 4: Batch INSERT via backend
        rows = await self._backend.enqueue_batch(args_list, connection=connection)  # type: ignore[call-arg]  # Why: asyncpg.Connection is compatible with the protocol's connection parameter at runtime

        # Phase 5: Wrap rows in JobHandles
        handles: list[JobHandle[BaseModel | None]] = []
        for i, row in enumerate(rows):
            args = args_list[i]
            handle: JobHandle[BaseModel | None] = JobHandle(
                client=self,
                row=row,
                result_adapter=items[i].actor_ref.result_adapter,
                was_existing=(row.id != args.id),
                _redis_client=self._redis_client,
                _settings=self._settings,
            )
            handles.append(handle)

        logger.debug(
            "batch_enqueued",
            kind="batch_enqueued",
            batch_id=str(resolved_batch_id),
            size=len(items),
        )

        return BatchHandle(
            batch_id=resolved_batch_id,
            job_handles=handles,
            size=len(items),
        )

    async def enqueue_batch_fast(
        self,
        items: list[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
    ) -> int:
        """Enqueue jobs via COPY FROM protocol for maximum throughput.

        **WARNING — bulk-import semantics, not general-purpose enqueue:**
        this method does NOT enforce ``max_pending``, does NOT detect or
        reject idempotency-key collisions (a duplicate key aborts the
        whole batch instead of being treated as "already enqueued"), and
        returns a bare row **count**, not per-job handles — there is no
        way to await, cancel, or otherwise reference an individual job
        from the return value. Use :meth:`enqueue_batch` unless you
        specifically need COPY-level throughput for a one-shot bulk
        import/backfill and have already accounted for these gaps.

        Returns the count of inserted rows — no :class:`~taskq.batch.BatchHandle`,
        no per-job :class:`~taskq.client.JobHandle` instances.

        **Validation rules:**

        - ``len(items) == 0`` raises :class:`ValueError`.
        - ``len(items) > 50_000`` raises :class:`ValueError`.
        - ALL payloads are validated before any INSERT — a single failure
          raises :class:`~taskq.exceptions.PayloadValidationError`.

        **Tradeoffs vs enqueue_batch:**

        - **No idempotency-key collision handling.** A duplicate key
          aborts the entire batch with ``asyncpg.UniqueViolationError``.
          Callers must pre-deduplicate.
        - **No max_pending check.** The caller is responsible for
          ensuring the batch won't exceed actor limits.
        - **No JobHandle instances.** Only the inserted row count is
          returned.  Use ``batch_id`` to query rows post-insert.
        - **All-or-nothing atomicity.** No partial success — the entire
          COPY fails on any constraint violation.

        Use for bulk import / backfill with 1K-50K rows where throughput
        matters more than idempotency guarantees.
        """
        from taskq._ids import new_job_id

        if len(items) == 0:
            raise ValueError("items must not be empty")
        if len(items) > 50_000:
            raise ValueError(f"items must contain at most 50 000 entries, got {len(items)}")

        # Auto-generate batch_id if not provided (UUIDv7)
        resolved_batch_id = UUID(bytes=new_job_id().bytes) if batch_id is None else batch_id

        # Phase 1: Validate ALL payloads before any I/O
        for i, item in enumerate(items):
            ref = item.actor_ref
            try:
                ref.payload_type.model_validate(item.payload)
            except ValidationError as exc:
                errs: list[dict[str, object]] = exc.errors()  # type: ignore[assignment]  # Why: pydantic v2 ErrorDetails is a TypedDict (subtype of dict[str, Any]); assignment to list[dict[str,object]] is safe at runtime but pyright cannot prove covariance
                raise PayloadValidationError(
                    f"Payload validation failed for item {i} (actor={ref.name!r}): {exc}",
                    actor=ref.name,
                    validation_errors=errs,
                ) from exc

        # Phase 2: Build per-item EnqueueArgs
        args_list = build_batch_args(items, resolved_batch_id, self._clock)

        # Phase 3: COPY FROM via backend
        count = await self._backend.enqueue_batch_fast(args_list, connection=connection)

        logger.debug(
            "batch_fast_enqueued",
            kind="batch_fast_enqueued",
            batch_id=str(resolved_batch_id),
            count=count,
        )

        return count

    # ── Read ────────────────────────────────────────────────────────────

    async def get[R: BaseModel | None](
        self,
        job_id: JobId,
        *,
        result_adapter: TypeAdapter[R] | None = None,
    ) -> JobHandle[R] | None:
        """Look up a job by id.

        Returns ``None`` when the job does not exist; otherwise wraps
        the row in a :class:`JobHandle[R]`. The caller may supply
        ``result_adapter`` because lookups by id do not carry actor
        identity — typical sources are
        ``my_actor.result_adapter`` (when reuniting with an actor) or
        ``TypeAdapter(type(None))`` (when only row metadata is needed).
        When *result_adapter* is ``None`` it defaults to
        ``TypeAdapter(type(None))``, which is suitable for status-only
        lookups.
        """
        adapter: TypeAdapter[R] = (
            result_adapter if result_adapter is not None else TypeAdapter(type(None))
        )  # type: ignore[assignment]  # Why: TypeAdapter(type(None)) returns TypeAdapter[None], which does not narrow to TypeAdapter[R] under pyright; runtime behaviour is correct because None is assignable to the R bound
        with self._translate_schema_errors():
            row = await self._backend.get(job_id)
        if row is None:
            return None
        return JobHandle(
            client=self,
            row=row,
            result_adapter=adapter,
            was_existing=False,
            _redis_client=self._redis_client,
            _settings=self._settings,
        )

    async def list(self, filter: JobFilter) -> JobPage:
        """List jobs matching *filter*, returning a :class:`JobPage`."""
        with self._translate_schema_errors():
            rows = await self._backend.list_jobs(filter)
        next_cursor: str | None = None
        if rows and len(rows) == filter.limit:
            last = rows[-1]
            next_cursor = encode_cursor(last.priority, last.scheduled_at, last.id)
        return JobPage(jobs=rows, next_cursor=next_cursor)

    # ── Cancel ─────────────────────────────────────────────────────────

    async def cancel(
        self,
        job_id: JobId,
        reason: str | None = None,
    ) -> CancelResult:
        """Request cancellation of a job and return a :class:`CancelResult`.

        Reads the row first via :meth:`Backend.get`. If the job does not
        exist, raises :class:`KeyError` — matching Python's stdlib
        idiom for "asked for an entry by id; it isn't there".

        Then calls :meth:`Backend.write_cancel_request` and reads the
        row again to capture the new status. The ``previous_status``
        reflects the row at the first read, not atomically at
        write-time (TOCTOU per  ).

        : increments ``taskq.cancellation.requested`` exactly once
        per call, regardless of ``cancellation_initiated`` outcome.
        """
        from taskq.obs import record_cancel_requested

        record_cancel_requested()

        with self._translate_schema_errors():
            row = await self._backend.get(job_id)
        if row is None:
            raise KeyError(job_id)

        previous_status = row.status
        with self._translate_schema_errors():
            initiated = await self._backend.write_cancel_request(job_id, reason)
            new_row = await self._backend.get(job_id)
        if new_row is None:
            msg = (
                f"job {job_id} disappeared after write_cancel_request; "
                "the row existed a moment ago and a write was issued against it"
            )
            raise RuntimeError(msg)
        new_status = new_row.status

        result = CancelResult(
            job_id=job_id,
            previous_status=previous_status,
            new_status=new_status,
            cancellation_initiated=initiated,
        )
        logger.debug(
            "cancel_requested",
            kind="cancel_requested",
            job_id=job_id,
            previous_status=previous_status,
            cancellation_initiated=initiated,
        )
        return result

    # ── Schedule CRUD ────────────────────────────────────────────────────

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
    ) -> "ScheduleHandle":
        """Create a cron schedule.  Raises :class:`ValueError` if both
        *payload_factory* and *static_payload* are provided, or if
        *cron_expr* is invalid.

        The ``(actor, name)`` UNIQUE constraint means each ``(actor, name)``
        pair may have at most one schedule; a second ``create_schedule`` for
        the same pair raises ``asyncpg.UniqueViolationError`` (PG) or
        :class:`ValueError` (in-memory).  Pass distinct *name* values to run
        several cron schedules per actor (e.g. a per-property sync).

        When *identity_key* is set, the cron loop propagates it to cron-fired
        jobs so they dedup against on-demand jobs for the same business key.

        Does NOT validate actor existence at creation time — any string
        actor name is accepted (validation is deferred to fire time).

        Args:
            dst_strategy: How to handle DST gaps and overlaps.
                ``skip`` (default) advances past gaps, uses the first
                occurrence in overlaps. ``firstof`` explicitly selects the
                earlier wall-clock time in overlaps. ``allof`` fires at
                both occurrences in overlaps.
        """
        from taskq.cron import (
            ScheduleHandle,
            compute_next_fire_after,
        )

        if not croniter.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr!r}")
        if payload_factory is not None and static_payload is not None:
            raise ValueError(
                "payload_factory and static_payload are mutually exclusive; "
                "provide one or the other, not both"
            )
        actor_name = actor.name if isinstance(actor, ActorRef) else actor
        # Why: actor is stored as a name string in the DB; payload type is not preserved at the cron-schedule level
        del actor

        metadata: dict[str, object] = {}
        if static_payload is not None:
            metadata["static_payload"] = static_payload

        now = datetime.now(UTC)
        next_fire = compute_next_fire_after(cron_expr, timezone, now, dst_strategy=dst_strategy)[0]

        args = ScheduleCreateArgs(
            actor=actor_name,
            cron_expr=cron_expr,
            timezone=timezone,
            next_fire_at=next_fire,
            dst_strategy=dst_strategy,
            payload_factory=payload_factory,
            enabled=enabled,
            name=name,
            identity_key=identity_key,
            metadata=metadata,
        )
        record = await self._backend.create_schedule(args)
        return ScheduleHandle(
            schedule_id=record.id,
            actor=record.actor,
            cron_expr=record.cron_expr,
            timezone=record.timezone,
            dst_strategy=record.dst_strategy,
            enabled=record.enabled,
            next_fire_at=record.next_fire_at,
            name=record.name,
            identity_key=record.identity_key,
            _backend=self._backend,
        )

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> "list[ScheduleRecord]":
        """List cron schedules, optionally filtered by actor or enabled status."""
        return await self._backend.list_schedules(actor=actor, enabled=enabled)

    async def update_schedule(
        self,
        schedule_id: UUID,
        *,
        cron_expr: str | None = None,
        enabled: bool | None = None,
        payload_factory: str | None = None,
        static_payload: dict[str, object] | None = None,
        clear_payload_factory: bool = False,
    ) -> "ScheduleRecord":
        """Update a cron schedule.  Setting ``enabled=True`` clears
        ``last_fire_error`` and resets ``consecutive_failures`` to 0.

        Raises :class:`ValueError` if both *payload_factory* and
        *static_payload* are provided, or if *cron_expr* is invalid.

        To explicitly clear ``payload_factory`` (set the column to NULL),
        pass ``clear_payload_factory=True`` — ``None`` for payload_factory
        means "don't change this field."
        """
        from taskq.cron import compute_next_fire_after

        if cron_expr is not None and not croniter.is_valid(cron_expr):
            raise ValueError(f"Invalid cron expression: {cron_expr!r}")
        if payload_factory is not None and static_payload is not None:
            raise ValueError(
                "payload_factory and static_payload are mutually exclusive; "
                "provide one or the other, not both"
            )

        next_fire_at: datetime | None = None
        if cron_expr is not None:
            now = datetime.now(UTC)
            records = await self._backend.list_schedules(actor=None, enabled=None)
            existing = next((r for r in records if r.id == schedule_id), None)
            tz = existing.timezone if existing is not None else "UTC"
            next_fire_at = compute_next_fire_after(cron_expr, tz, now)[0]

        metadata: dict[str, object] | None = None
        if static_payload is not None:
            metadata = {"static_payload": static_payload}

        args = ScheduleUpdateArgs(
            cron_expr=cron_expr,
            next_fire_at=next_fire_at,
            enabled=enabled,
            payload_factory=payload_factory,
            clear_payload_factory=clear_payload_factory,
            metadata=metadata,
        )
        return await self._backend.update_schedule(schedule_id, args)

    async def delete_schedule(self, schedule_id: UUID) -> None:
        """Delete a cron schedule by ID.  Idempotent — no error if missing."""
        await self._backend.delete_schedule(schedule_id)

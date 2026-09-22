"""JobsClient, the primary entry point for enqueuing, querying, and
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

import asyncio
import dataclasses
from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import AsyncExitStack, contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import islice
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast
from uuid import UUID

import structlog
from pydantic import BaseModel, TypeAdapter, ValidationError

from taskq._close import CLOSE_TIMEOUT_SECS, close_redis_bounded
from taskq._validation import CURRENT_PAYLOAD_SCHEMA_VER, validate_actor_payload
from taskq.actor import ActorRef
from taskq.backend._cursor import encode_job_cursor
from taskq.backend._protocol import (
    MAX_JOB_LIST_LIMIT,
    Backend,
    BatchFilter,
    BatchRow,
    DstStrategy,
    EnqueueArgs,
    IdempotencyKey,
    IdentityKey,
    JobFilter,
    JobId,
    JobPage,
    JobRow,
    QueueName,
    ScheduleCreateArgs,
    ScheduleUpdateArgs,
)
from taskq.backend._records import (
    _nul_item_payload_error,  # pyright: ignore[reportPrivateUsage]  # Why: the shared per-item jsonb NUL annotation helper, the same contract the streaming boundary re-locates; redefining it here would let client and backend drift
    item_jsonb_param,
    item_tags_jsonb_param,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.batch import MAX_BATCH_SIZE, BatchHandle, BatchSummary, EnqueueItem
from taskq.batch_policy import BatchFailurePolicy
from taskq.client._args import (
    UniqueForNoIdentityWarner,
    build_batch_args,
    build_enqueue_args,
    enqueue_span,
    validate_idempotency,
)
from taskq.client._capacity import (
    DEFAULT_CAPACITY_CACHE_TTL,
    DEFAULT_CAPACITY_READ_TIMEOUT,
    ActorCapacityCache,
)
from taskq.client._handle import JobHandle
from taskq.constants import DEFAULT_CHUNK_SIZE, MAX_IDEMPOTENCY_KEY_BYTES
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    EmptyFilterError,
    PayloadValidationError,
    SchemaNotMigratedError,
)
from taskq.types import BulkCancelResult, CancelResult

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.backend._protocol import ScheduleRecord
    from taskq.cron import ScheduleHandle
    from taskq.settings import TaskQSettings

__all__ = ["JobsClient"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_NONE_RESULT_ADAPTER: TypeAdapter[None] = TypeAdapter(type(None))
"""Status-only adapter for :meth:`JobsClient.get` when no ``result_adapter`` is supplied.

Why a module singleton: pydantic builds a validator schema on every
``TypeAdapter(...)`` construction, and ``get(result_adapter=None)`` is the
shape of status polling (``wait_for_batch`` and health-check loops), so the
per-call construction was a poll-frequency tax with no per-call variance.
``TypeAdapter`` is immutable and reusable by design (pydantic documents
module-level reuse as the intended pattern).
"""


def _item_payload_error(idx: int, actor_name: str, exc: ValidationError) -> PayloadValidationError:
    """Annotate a batch item's payload :class:`~pydantic.ValidationError` with
     its index and actor, as :class:`~taskq.exceptions.PayloadValidationError`.

     Shared by both streaming paths (the lazy generator and the chunked
     loop): ``build_enqueue_args`` performs the single pydantic-core
     validation pass per item, and the client layer translates its
     ``ValidationError`` here instead of running a second, discarded
     validation per item just to attach context.

     Error details are sanitized via ``include_url=False, include_input=False``
    , the same contract :func:`taskq._validation.validate_actor_payload`
     follows, because ``validation_errors`` propagates into the persisted
     ``error_message`` / web admin via generic exception handling and payload
     values are attacker-controlled. (The previous inline ``exc.errors()``
     calls leaked the raw payload input on this path.)
    """
    errs: list[dict[str, object]] = exc.errors(include_url=False, include_input=False)  # type: ignore[assignment]  # Why: pydantic v2 ErrorDetails is a TypedDict (subtype of dict[str, Any]); assignment to list[dict[str,object]] is safe at runtime but pyright cannot prove covariance
    return PayloadValidationError(
        f"Payload validation failed for item {idx} (actor={actor_name!r}): {exc}",
        actor=actor_name,
        # Enqueue-time validation runs against the actor's currently
        # declared payload_type, so the version in scope is the one being
        # validated against (and about to be stamped on the row).
        payload_schema_ver=str(CURRENT_PAYLOAD_SCHEMA_VER),
        validation_errors=errs,
        # The machine-readable copy of the message's index. ``idx`` is
        # already caller-global at both callers: the atomic arm's lazy
        # generator enumerates the WHOLE stream, and the chunked arm's
        # remap passes chunk_offset + the located position, so the field
        # needs no second shift anywhere (the chunked arm's backend errors
        # are still chunk-local and shifted once by the registry; the
        # atomic arm's backend errors arrive stream-global from the
        # backend's index_base and cross this layer un-shifted).
        item_index=idx,
    )


# ── Streaming boundary: chunk-local → stream-global item indices ────────
#
# enqueue_batch_streaming's chunked arm funnels one caller stream through
# per-chunk calls, and every per-item typed error those calls raise names
# an index into THE CHUNK, the backend's per-call contract, while the
# caller needs an index into THE STREAM: with no caller connection each
# chunk is its own committed transaction, so a later chunk's chunk-local
# index confidently names a stream position that was never attempted
# while the committed prefix stays durable, and a retry guided by the
# wrong index duplicates that prefix.


def _remap_validation_error(
    exc: Exception,
    chunk_items: Sequence[EnqueueItem],
    chunk_args: Sequence[EnqueueArgs] | None,
    chunk_offset: int,
) -> NoReturn:
    """Locate the pydantic failure's chunk item by re-validating it, and
    re-annotate at ``chunk_offset + its chunk-local position``, the
    committed prefix plus the in-chunk position is the item's position in
    the CALLER's stream.

    The raised :class:`~pydantic.ValidationError` carries no item index,
    so the failing item is located by re-running the same validation the
    args build already performed once per item (error path only, the
    happy path validates exactly once, inside
    :func:`~taskq.client._args.build_enqueue_args`).
    """
    for i, item in enumerate(chunk_items):
        ref = item.actor_ref
        try:
            ref.payload_type.model_validate(item.payload)
        except ValidationError:
            # Why cast: the registry dispatch reached this remap via
            # isinstance(ValidationError), so the caller guaranteed the
            # type pydantic already re-established here.
            raise _item_payload_error(
                chunk_offset + i, ref.name, cast(ValidationError, exc)
            ) from exc
    raise exc


def _nul_rejected_field(args: EnqueueArgs, idx: int) -> str | None:
    """Which jsonb-bound field of batch item *idx* the shared NUL guard
    rejects (``"payload"`` / ``"metadata"`` / ``"tags"``), or ``None``
    when the item is clean.

    Re-runs the same serialization guard the backend's batch preflight
    ran, the shared helpers in :mod:`taskq.backend._records`, purely to
    LOCATE the rejected item, so the streaming boundary can re-annotate
    it at its stream-global index without parsing the raised message
    apart.
    """
    try:
        item_jsonb_param(args.payload, idx=idx, field="payload", actor=args.actor)
    except PayloadValidationError:
        return "payload"
    try:
        item_jsonb_param(args.metadata, idx=idx, field="metadata", actor=args.actor)
    except PayloadValidationError:
        return "metadata"
    try:
        item_tags_jsonb_param(args.tags, idx=idx, actor=args.actor)
    except PayloadValidationError:
        return "tags"
    return None


def _remap_payload_validation_error(
    exc: Exception,
    chunk_items: Sequence[EnqueueItem],
    chunk_args: Sequence[EnqueueArgs] | None,
    chunk_offset: int,
) -> NoReturn:
    """Re-run the shared jsonb NUL guard over the chunk's built args to
    locate the rejected item, then re-raise its annotation at the
    stream-global index.

    The backend's batch preflight, PG's build loop and the in-memory
    mirror alike, one shared guard, annotates its rejection with the
    index into the CALL's args list, which on the streaming path is one
    chunk. The annotation is reconstructed through the same helper the
    guard itself uses, so field, actor and wording cannot drift from the
    backend's.
    """
    if chunk_args is None:
        # The args build raised before any args existed, it speaks
        # ValueError/pydantic only, so this arm is unreachable from the
        # boundary today; without built args there is no located item to
        # shift, and the original crosses unchanged rather than
        # guessed-at.
        raise exc
    for i, args in enumerate(chunk_args):
        field = _nul_rejected_field(args, i)
        if field is not None:
            raise _nul_item_payload_error(
                idx=chunk_offset + i, field=field, actor=args.actor
            ) from exc
    raise exc


def _remap_batch_max_pending_error(
    exc: Exception,
    chunk_items: Sequence[EnqueueItem],
    chunk_args: Sequence[EnqueueArgs] | None,
    chunk_offset: int,
) -> NoReturn:
    """Shift the partition refusal's chunk-local indices to stream-global
    and fold the committed prefix into ``admitted_count``.

    The backend's per-actor partition refusal carries indices into the
    CALL's args list, one chunk on the streaming path; the caller's
    safe-retry contract (retry only the refused items, never the
    committed prefix) needs indices into the CALLER's stream and an
    ``admitted_count`` covering every committed item. Items after the
    refusing chunk were never attempted.
    """
    refused = cast(BatchMaxPendingExceededError, exc)
    raise BatchMaxPendingExceededError(
        refusals=refused.refusals,
        refused_indices={
            actor: [i + chunk_offset for i in indices]
            for actor, indices in refused.refused_indices.items()
        },
        admitted_count=chunk_offset + refused.admitted_count,
    ) from exc


_ItemErrorRemap = Callable[
    [Exception, Sequence[EnqueueItem], Sequence[EnqueueArgs] | None, int],
    NoReturn,
]
"""One registry member: given the raised per-item error, the chunk's
items and built args, and the chunk's stream-global base (the committed
prefix), re-raise the error annotated at stream-global item indices."""

_ITEM_ERROR_REMAPS: dict[type[Exception], _ItemErrorRemap] = {
    ValidationError: _remap_validation_error,
    PayloadValidationError: _remap_payload_validation_error,
    BatchMaxPendingExceededError: _remap_batch_max_pending_error,
}
"""The exhaustive registry of per-item error types the streaming boundary
remaps from chunk-local to stream-global indices.

Why a registry instead of except clauses at the call site: the chunked
streaming path once remapped exactly the two types it knew about
(pydantic ``ValidationError`` and the cap partition's
``BatchMaxPendingExceededError``), so a third per-item typed error, the
jsonb NUL guard's ``PayloadValidationError``, silently crossed with its
chunk-local index and confidently named a stream position that was never
attempted, on a path where each chunk is separately committed and a
retry guided by the wrong index duplicates the committed prefix. A new
typed per-item backend error joins THIS mapping (one entry); the
boundary's except clause is derived from it and never grows a bolt-on
arm. A registered type's subclasses inherit its remap, the dispatch
walks by ``isinstance``, not exact type.
"""

_REMAPPABLE_ITEM_ERROR_TYPES: tuple[type[Exception], ...] = tuple(_ITEM_ERROR_REMAPS)
"""The boundary's except-clause tuple, derived from the registry, never
hand-maintained alongside it, so the boundary catches exactly what the
registry knows and nothing else."""


def _remap_chunk_item_error(
    exc: Exception,
    chunk_items: Sequence[EnqueueItem],
    chunk_args: Sequence[EnqueueArgs] | None,
    chunk_offset: int,
) -> NoReturn:
    """The streaming boundary's single dispatch: find the raised error's
    registry entry and hand it to that entry's remap.

    First ``isinstance`` match wins, so a registered type's subclasses
    inherit its remap. A non-member crosses unchanged, the boundary's
    except clause admits only registry members, so that arm is reachable
    only from direct (test) calls.
    """
    for error_type, remap in _ITEM_ERROR_REMAPS.items():
        if isinstance(exc, error_type):
            remap(exc, chunk_items, chunk_args, chunk_offset)
    raise exc


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
        capacity_cache_ttl: float = DEFAULT_CAPACITY_CACHE_TTL,
    ) -> None:
        self._backend = backend
        self._clock = clock if clock is not None else SystemClock()
        self._settings: "TaskQSettings | None" = settings  # noqa: UP037  # Why: TaskQSettings is under TYPE_CHECKING; string annotation avoids runtime import.
        self._redis_client: "redis_async.Redis | None" = None  # type: ignore[type-arg]  # noqa: UP037  # Why: redis_async is under TYPE_CHECKING; string annotation avoids runtime import. type-arg: redis-py stubs expose Redis as an unparameterised generic.
        self._exit_stack: AsyncExitStack = AsyncExitStack()
        self._unique_for_warner = UniqueForNoIdentityWarner()
        self._capacity_cache = ActorCapacityCache(backend, ttl=capacity_cache_ttl)
        # Why resolved here: every enqueue path in this client validates
        # against one number, and a client built without settings still gets
        # the shipped default rather than a second literal.
        self._idempotency_max_bytes: int = (
            settings.idempotency_key_max_bytes
            if settings is not None
            else MAX_IDEMPOTENCY_KEY_BYTES
        )

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
        :class:`~taskq.exceptions.SchemaNotMigratedError`, chained via
        ``from exc`` so the original traceback is preserved, with a
        message pointing at ``taskq migrate up`` / ``TASKQ_MIGRATE_ON_START``.
        """
        import asyncpg

        try:
            yield
        except asyncpg.exceptions.UndefinedTableError as exc:
            schema = self._settings.schema_name if self._settings is not None else "taskq"
            raise SchemaNotMigratedError(schema) from exc

    _OPEN_REDIS_TIMEOUT_SECS: float = 30.0
    """Bounds the eager ``initialize()``, the first broker round trip ,
    in :meth:`_open_redis`. Mirrors ``WorkerSettings.reload_factory_timeout``'s
    default (30.0), the same budget the worker gives every first-use
    redis/factory call (worker/deps.py bounds its redis factory
    identically); the codebase bounds redis operations with
    ``asyncio.wait_for`` (redis-py socket kwargs are configured nowhere in
    src/taskq). A class attribute (not a module constant) so the edit
    stays inside this file's Redis-creation region; tests shrink it as a
    seam through the class, the ``CLOSE_TIMEOUT_SECS`` monkeypatch
    convention."""

    async def _open_redis(self, settings: "TaskQSettings") -> None:
        """Open a Redis client when ``settings.redis_url`` is not ``None``.

        Called by :class:`TaskQ.open()` after constructing the client.
        The Redis client is registered on :attr:`_exit_stack` for LIFO
        teardown via a bounded-close callback. Uses
        ``decode_responses=False`` (bytes mode) consistent with the
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
            client = redis_async.from_url(str(settings.redis_url), decode_responses=False)

            # Why not stack.enter_async_context(client): Redis.__aexit__
            # calls aclose() UNBOUNDED, a hung broker would wedge
            # JobsClient.close(). initialize() preserves
            # __aenter__'s eager-setup semantics; the pushed callback
            # bounds the close instead (b072692 pattern).
            async def _close_client() -> None:
                # Why module-global reads at call time: tests monkeypatch
                # close_redis_bounded / CLOSE_TIMEOUT_SECS as
                # observation and timeout-shrink seams (same convention as
                # taskq.worker.deps).
                await close_redis_bounded(client, "jobs-client", CLOSE_TIMEOUT_SECS)

            # Why push BEFORE initialize(): from_url() has already allocated
            # the connection pool, so if initialize() raises (broker down)
            # the failed eager setup must still release it, the unwind runs
            # the pushed callback through the bounded close (never raises;
            # aclose() on a never-initialized client is a no-op).
            self._exit_stack.push_async_callback(_close_client)
            # Why bounded: initialize() is the EAGER first broker round
            # trip, a black-holed Redis would wedge TaskQ.open() forever,
            # and client processes arm no watchdogs. The pushed callback
            # above already bounds the unwind's close.
            try:
                await asyncio.wait_for(client.initialize(), timeout=self._OPEN_REDIS_TIMEOUT_SECS)
            except TimeoutError as exc:
                raise TimeoutError(
                    f"Redis client initialize() did not complete within "
                    f"{self._OPEN_REDIS_TIMEOUT_SECS}s, the broker at "
                    f"{settings.redis_url} is unreachable or black-holed. "
                    "The open fails loudly instead of parking forever."
                ) from exc
            self._redis_client = client
        self._settings = settings

    async def close(self) -> None:
        """Close the Redis client and release resources via the exit stack."""
        await self._exit_stack.aclose()
        self._redis_client = None

    def invalidate_actor_capacity_cache(self) -> None:
        """Drop the cached ``actor_config.max_pending`` snapshot.

        The next enqueue refreshes from the backend instead of waiting
        out the TTL. Not needed in normal operation (staleness is
        bounded by ``capacity_cache_ttl``, default 5s); intended for
        tests and for tooling that knows it just changed the table and
        cannot wait out the TTL.
        """
        self._capacity_cache.invalidate()

    # ── Enqueue ────────────────────────────────────────────────────────

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

        - When the actor's effective ``max_pending`` is set, a pre-flight
          count of ``pending`` + ``scheduled`` jobs for the actor is
          compared to the limit. If ``count >= max_pending``,
          :class:`MaxPendingExceededError` is raised synchronously, the
          caller decides whether to retry, fail, or wait; the library
          does not block on capacity.

        - The effective limit is **operator-owned**: a non-NULL stored
          ``actor_config.max_pending`` (set via
          ``taskq actor-config set --max-pending``) wins over the
          ``@actor(max_pending=...)`` literal; a cleared or absent
          stored value falls back to the literal. The client reads the
          stored value through a TTL-bounded cache (default 5s
          staleness; see :class:`taskq.client._capacity.ActorCapacityCache`),
          so an operator change takes effect fleet-wide within seconds
          without any redeploy or restart.

        - Evaluation order at enqueue: ``unique_for`` dedup →
          singleton pre-flight → ``max_pending`` count check →
          ``idempotency_key`` INSERT → job INSERT. A ``unique_for`` hit
          bypasses all remaining checks; a singleton collision fires before
          ``max_pending`` to give the caller the more specific
          ``SingletonCollisionError``.

        - ``idempotency_key`` does **not** bypass ``max_pending``, the
          idempotency ON CONFLICT fires at step 5, after the max_pending
          check at step 3. Re-enqueuing with a duplicate
          ``idempotency_key`` when the queue is full raises
          ``MaxPendingExceededError``, not the deduplicated handle. Only
          ``unique_for`` (step 1) bypasses max_pending.

        **idempotency_key:**

        - ``idempotency_key`` is unique within its ``idempotency_scope``
          (composite ``(idempotency_scope, idempotency_key)`` uniqueness).
          The default scope (``idempotency_scope=None`` or ``""``) preserves
          the prior global-until-prune behavior exactly, so existing callers
          see zero behavior change. Passing an explicit scope (e.g. a
          run/batch/epoch id) lets two enqueues with the same business key
          in different scopes both succeed, decoupling the dedupe horizon
          from ``prune_retention_*``.

        - Key length is bounded at ``idempotency_key_max_bytes``
          (``TASKQ_IDEMPOTENCY_KEY_MAX_BYTES``, default 1024 UTF-8 bytes) ,
          the bound is the composite unique index's btree entry size, not a
          round number. Empty and whitespace-only keys raise
          :class:`ValueError` at the client boundary before any backend
          call. The same bound applies to ``idempotency_scope``; an empty
          scope (``""``) is valid and equivalent to ``None`` (the
          default/global scope).

        - **No time-based (TTL) dedupe window.** ``idempotency_scope``
          decouples the dedupe horizon from ``prune_retention_*`` by
          namespace, not by time, there is no ``idempotency_ttl`` or
          equivalent "dedupe for the next N seconds" parameter. A key
          within a given scope still dedupes **until pruned**, exactly
          like the pre-scope global behavior, just scoped to that
          namespace. This is a deliberate scope decision, not an
          oversight: a real sliding-window TTL cannot be expressed as a
          single static unique index the way scope can, a sliding
          window would require either abandoning the atomic ``INSERT
          ... ON CONFLICT`` for a check-then-insert lock (weaker
          concurrency guarantee) or encoding time-bucketing into the key
          itself (coarser, edge-artifact-prone semantics). If your use
          case genuinely needs "dedupe for the next hour, not forever,"
          encode the window into the scope yourself (e.g. a
          time-bucketed scope string) until/unless a TTL parameter ships
          as a separate feature.

        - Rolling-deploy note: if this schema is mid-upgrade (the
          ``01.00.03_01_pre_idempotency_scope.sql`` migration applied but
          ``01.00.03_01_post_idempotency_scope_drop_old_index.sql`` not
          yet applied), reusing the same ``idempotency_key`` under two
          *different* ``idempotency_scope`` values raises
          :class:`~taskq.exceptions.ScopedIdempotencyMigrationPendingError`
          rather than silently dedupe against the wrong scope's job. The
          trigger is a key existing under a different scope, in *either*
          direction, an unscoped call reusing a key first written under
          a non-default scope raises it too. Only brand-new keys and
          same-scope repeats are unaffected. See that exception's
          docstring and the migration file's header comment for the full
          rationale.

        **unique_for:**

        - ``unique_for`` deduplication is **serialized**, not best-effort:
          the preflight and the INSERT run under one transaction-scoped
          advisory lock on ``(actor, identity_key)``, so concurrent
          enqueues of the same identity produce exactly one job and every
          caller is handed that job. This holds on pool connections and on
          a caller-supplied connection with no open transaction. On a
          caller-owned OPEN transaction the lock spans that transaction
          instead, so single-flight holds until its commit/rollback, a
          long-lived caller transaction can exhaust another same-identity
          enqueue's bounded wait
          (:class:`~taskq.exceptions.UniqueForLockTimeoutError`) rather
          than dedup against it. The dispatch CTE's ``running_identities``
          filter remains the execution-level guard behind the
          enqueue-level one.

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
            effective_max_pending = await self._capacity_cache.effective_max_pending(
                ref.name, ref.max_pending
            )
            # An explicit trace_id/span_id overrides the ambient span, per
            # docs/guides/jobs-clients.md: "pass explicitly to override or
            # to propagate an external trace context". Both were previously
            # accepted and then dropped in favour of the extracted values,
            # so cross-service propagation silently produced an unlinked
            # consumer span. SubJobEnqueuer.enqueue has no such override and
            # correctly exposes no parameter for one.
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
                idempotency_scope=idempotency_scope,
                trace_id=trace_id if trace_id is not None else extracted_trace_id,
                span_id=span_id if span_id is not None else extracted_span_id,
                schedule_to_close=schedule_to_close,
                start_to_close=start_to_close,
                heartbeat_timeout=heartbeat_timeout,
                max_pending=effective_max_pending,
                tags=tags,
                idempotency_max_bytes=self._idempotency_max_bytes,
            )
            if span.is_recording():
                # Why the guard: on a non-recording span (no SDK, sampling)
                # set_attribute discards the value, so the str() of the job
                # id is paid per enqueue for nothing. Skipped, the exported
                # spans are unchanged: a recording span still gets exactly
                # this attribute.
                span.set_attribute("messaging.message.id", str(args.id))
            if args.unique_for is not None and args.identity_key is None:
                self._unique_for_warner.maybe_warn(
                    actor=ref.name, queue=ref.queue, unique_for=args.unique_for
                )
            with self._translate_schema_errors():
                row = await self._backend.enqueue(args)

        if row.id == args.id:
            logger.debug(
                "job_enqueued",
                kind="job_enqueued",
                job_id=str(row.id),
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
        failure_policy: BatchFailurePolicy | None = None,
        finalizer: EnqueueItem | None = None,
    ) -> BatchHandle:
        """Enqueue multiple jobs in a single batched INSERT and return a
        :class:`~taskq.batch.BatchHandle`.

        All ``items`` share a single ``batch_id`` UUID written into each
        job's ``metadata.batch_id`` field (as a string).  When
        ``batch_id`` is not supplied it is auto-generated as a UUIDv7 via
        :func:`~taskq._ids.new_job_id`.

        **failure_policy:**

        When ``failure_policy`` is set (e.g.
        :class:`~taskq.batch_policy.AbortBatchAfter`), a ``batches`` row
        is created with the policy's failure threshold. After each child
        job reaches a terminal state, the
        :func:`~taskq.batch.apply_batch_terminal_outcome` hook inspects
        the outcome: ``succeeded`` resets the consecutive-failure
        counter; ``failed`` increments it and aborts the batch if the
        threshold is reached. Aborting cancels all pending and scheduled
        child jobs and sets the batch row to ``aborted``.

        **finalizer:**

        When ``finalizer`` is set, a finalizer job is enqueued alongside
        the batch. The finalizer is NOT stamped with ``batch_id``
        metadata (deadlock prevention, if it were, ``wait_for_batch``
        would count it as a child and the finalizer would wait for
        itself). The batch row's ``finalizer_job_id`` column records the
        link, and ``wait_for_batch`` automatically excludes that job
        from counts. The finalizer is dispatched immediately; the
        in-actor ``wait_for_batch`` snooze pattern gates on child-job
        completion.

        **Transactional enqueue:**

        When ``failure_policy`` or ``finalizer`` is set and
        ``connection`` is ``None``, the entire operation (batch row +
        all child jobs + finalizer) is inserted in a single transaction
        via :meth:`Backend.enqueue_batch_atomic`. If any insert fails,
        no rows are committed. When a ``connection`` is provided, the
        caller controls the transaction boundary; the batch row and
        finalizer are created as the last statements on that connection.

        **Validation rules:**

        - ``len(items) == 0`` raises :class:`ValueError`.
        - ``len(items) > MAX_BATCH_SIZE`` raises :class:`ValueError`.
        - ALL payloads are validated before any INSERT.  A single failure
          raises :class:`~taskq.exceptions.PayloadValidationError` and
          leaves no rows inserted.

        **max_pending:**

        Admission is partitioned per actor.  The backend issues one
        aggregated ``SELECT actor, count(*) … WHERE actor = ANY($1)
        GROUP BY actor`` for the entire batch, resolves per-actor
        effective limits (operator-owned stored value when set, else the
        ``@actor(...)`` literal, same resolution as :meth:`enqueue`),
        and admits every within-cap actor's items; an over-cap actor's
        items are refused as a whole group (never partially filled up to
        the cap).  When any actor is refused, the within-cap actors'
        items are still enqueued and
        :class:`~taskq.exceptions.BatchMaxPendingExceededError` raises
        afterwards, it names each refused actor, the refused item
        indices into ``items``, and the admitted count, so a retry can
        target only the refused items (or rely on ``idempotency_key``s
        to deduplicate a whole-batch retry).  The exception is
        deliberately not a :class:`~taskq.exceptions.MaxPendingExceededError`
        subclass: handlers for that type assume nothing was enqueued,
        and under this error part of the batch is already stored.

        When ``failure_policy`` or ``finalizer`` is set and
        ``connection`` is ``None`` (the atomic path), a cap violation
        keeps the all-or-nothing contract instead: the whole single
        transaction rolls back and plain
        :class:`~taskq.exceptions.MaxPendingExceededError` raises with
        nothing committed.

        On a caller-supplied connection with an open transaction, the
        admitted items are inserted but their durability follows that
        transaction's commit/rollback.

        **unique_for (not applied on batch paths):**

        Actor-declared ``unique_for`` is a single-enqueue contract (see
        :meth:`enqueue`). The batch INSERT writes every item without a
        per-identity preflight, the single path's advisory-lock +
        preflight round trips are exactly what bulk throughput exists to
        avoid, so two batch items with the same
        ``(actor, identity_key)`` both insert, and the dispatch CTE's
        identity serialization ensures only one runs at a time.
        ``unique_for`` items are conservatively fully counted toward
        ``max_pending`` (a batch mixing ``unique_for`` retries near the
        cap may refuse loudly rather than admit silently). Deduplicate
        batch items with per-item ``idempotency_key``s, that arbiter IS
        applied, below. Whether the batch tier should honor
        ``unique_for`` is an open design decision; this documents
        current behavior.

        **idempotency_key collisions:**

        Items whose ``idempotency_key`` collides with an existing row
        return the existing :class:`~taskq.client.JobHandle` (same
        semantics as single-item :meth:`enqueue`).
        """
        from taskq._ids import new_job_id

        if len(items) == 0:
            raise ValueError("items must not be empty")
        if len(items) > MAX_BATCH_SIZE:
            raise ValueError(
                f"items must contain at most {MAX_BATCH_SIZE} entries, got {len(items)}"
            )

        # Auto-generate batch_id if not provided (UUIDv7)
        resolved_batch_id = UUID(bytes=new_job_id().bytes) if batch_id is None else batch_id

        # Phase 1: Validate ALL payloads (and idempotency keys) before any I/O
        for i, item in enumerate(items):
            ref = item.actor_ref
            validate_actor_payload(ref.payload_type, item.payload, actor=ref.name)
            validate_idempotency(
                item.idempotency_key,
                item.idempotency_scope,
                self._idempotency_max_bytes,
                where=f" for item {i}",
            )

        # Resolve the effective cap per actor (stored operator value when
        # set, else the ``@actor(...)`` literal, same resolution as
        # :meth:`enqueue`) so the per-item args carry it into the backend,
        # whose admission check is the single enforcement point: it counts
        # live (not through this client's TTL cache), discounts idempotency
        # pairs that will dedupe instead of writing, and partitions
        # admission per actor. The old client-side aggregated pre-check
        # raised here for the WHOLE call, one capped actor aborted
        # everyone's items, and its count was strictly less
        # informed than the backend's, so it was removed rather than
        # duplicated.
        effective_mp: dict[str, int | None] = {}
        for item in items:
            ref = item.actor_ref
            if ref.name not in effective_mp:
                effective_mp[ref.name] = await self._capacity_cache.effective_max_pending(
                    ref.name, ref.max_pending
                )

        # Build per-item EnqueueArgs carrying the resolved limits for the
        # backend's per-actor admission check.
        args_list = build_batch_args(items, resolved_batch_id, max_pending_by_actor=effective_mp)

        queue = items[0].actor_ref.queue
        has_batch_extras = failure_policy is not None or finalizer is not None

        # Build finalizer EnqueueArgs (without batch_id stamping, deadlock prevention).
        finalizer_args: EnqueueArgs | None = None
        if finalizer is not None:
            finalizer_args = build_enqueue_args(
                finalizer.actor_ref,
                finalizer.payload,
                scheduled_at=finalizer.scheduled_at,
                priority=finalizer.priority,
                fairness_key=finalizer.fairness_key,
                identity_key=finalizer.identity_key,
                idempotency_key=finalizer.idempotency_key,
                idempotency_scope=finalizer.idempotency_scope,
                metadata=dict(finalizer.metadata),
                start_to_close=finalizer.start_to_close,
                tags=finalizer.tags,
                idempotency_max_bytes=self._idempotency_max_bytes,
            )

        # Build BatchRow when failure_policy OR finalizer is set (C3:
        # finalizer-only batches also need a row for list_batches
        # discoverability and finalizer_job_id auto-exclusion in
        # wait_for_batch). When only finalizer is set, failure_threshold=None.
        batch_row: BatchRow | None = None
        if failure_policy is not None or finalizer is not None:
            threshold = failure_policy.failure_threshold if failure_policy is not None else None
            batch_row = BatchRow(
                id=resolved_batch_id,
                queue=queue,
                status="active",
                expected_size=len(items),
                consecutive_failures=0,
                failure_threshold=threshold,
                finalizer_job_id=finalizer_args.id if finalizer_args is not None else None,
                originating_actor=None,
                created_at=self._clock.now(),
                completed_at=None,
                metadata={},
            )

        if has_batch_extras and connection is None:
            # Autonomous atomic path: delegate to backend.enqueue_batch_atomic.
            all_rows = await self._backend.enqueue_batch_atomic(
                args_list,
                batch_id=resolved_batch_id,
                queue=queue,
                batch_row=batch_row,
                finalizer_args=finalizer_args,
            )
            rows = all_rows[: len(items)]
            finalizer_row = all_rows[len(items) :][0] if finalizer is not None else None
        else:
            # Caller-owned transaction or no extras: use regular enqueue_batch.
            # M3: create the batch row BEFORE job inserts so the row exists
            # when the first terminal write triggers the batch hook (on an
            # autocommit conn, jobs can dispatch and fail before the row
            # exists otherwise). Insert the finalizer first so its returned
            # row id is known for finalizer_job_id (M4: idempotency collision
            # may return a different id than finalizer_args.id).
            finalizer_row = None
            if finalizer is not None:
                assert finalizer_args is not None
                finalizer_row = await self._backend.enqueue_with_conn(connection, finalizer_args)  # type: ignore[arg-type]  # Why: guarded by has_batch_extras; when connection is provided it is runtime-compatible
            if batch_row is not None:
                if finalizer_row is not None:
                    batch_row = replace(batch_row, finalizer_job_id=finalizer_row.id)
                await self._backend.create_batch(
                    batch_row.id,
                    batch_row.queue,
                    batch_row.expected_size,
                    batch_row.failure_threshold,
                    batch_row.finalizer_job_id,
                    batch_row.originating_actor,
                    connection=connection,  # type: ignore[arg-type]  # Why: connection may be None but create_batch handles that
                )
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

        # Build finalizer handle (if any) and append to job_handles for backward compat.
        finalizer_handle: JobHandle[BaseModel | None] | None = None
        if finalizer is not None and finalizer_row is not None:
            finalizer_handle = JobHandle(
                client=self,
                row=finalizer_row,
                result_adapter=finalizer.actor_ref.result_adapter,
                was_existing=(finalizer_row.id != finalizer_args.id)
                if finalizer_args is not None
                else False,
                _redis_client=self._redis_client,
                _settings=self._settings,
            )
            handles.append(finalizer_handle)

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
            finalizer_handle=finalizer_handle,
        )

    async def enqueue_batch_streaming(
        self,
        items: Iterable[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
        failure_policy: BatchFailurePolicy | None = None,
        finalizer: EnqueueItem | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> BatchHandle:
        """Enqueue jobs from a lazy iterable in chunks, returning a single
        :class:`~taskq.batch.BatchHandle`.

        Unlike :meth:`enqueue_batch`, this method accepts an
        :class:`~collections.abc.Iterable` (including generators) and
        inserts in chunks of ``chunk_size`` (1-MAX_BATCH_SIZE).  All items share
        the same ``batch_id``.  Payloads are validated on the fly as
        each chunk is built.

        When ``failure_policy`` or ``finalizer`` is set and
        ``connection`` is ``None``, the entire operation is delegated to
        :meth:`Backend.enqueue_batch_atomic` for single-transaction
        atomicity.  Otherwise, chunks are inserted via
        :meth:`Backend.enqueue_batch`: with a caller-supplied connection
        all chunks share that connection's open transaction (one
        transaction aggregate, the caller owns the boundary); with NO
        connection each chunk is its own pool transaction, so progress
        is committed incrementally, chunk by chunk.

        **No-connection failure surface:** when chunk *N* fails (cap
        refusal, payload validation, a driver error), chunks 1..*N*-1
        are already durably committed and nothing is returned, the
        ``BatchHandle`` is only constructed after the whole stream
        drains.  A blind retry of the full iterable would duplicate the
        committed prefix.  Retry safely by giving items
        ``idempotency_key``s (a retry deduplicates against the committed
        rows) or by resuming from the point of failure.

        **Failure-policy counting limitation (caller-connection path):**
        the batch row is created AFTER all chunk inserts, it must carry
        the final ``expected_size`` and ``create_batch`` is INSERT, not
        upsert, so a child job enqueued on that connection that
        reaches a terminal state BEFORE the row exists is not counted
        toward ``failure_policy``: ``increment_batch_failures`` finds
        no row and returns ``(0, None, 0)``. The atomic
        (no-connection) path is unaffected, its single transaction
        makes the batch row and the child jobs visible together.

        **max_pending:** enforced per chunk with the same per-actor
        partition as :meth:`enqueue_batch`, each chunk's items carry
        their actors' *effective* caps (operator-stored override when
        set, else the ``@actor(...)`` literal, same resolution as
        :meth:`enqueue`), the backend admits every within-cap actor's
        items, and an over-cap actor's items are refused.  On the
        caller-connection path sequential chunks share one transaction,
        so per-chunk admission accumulates to the true aggregate.  On
        the no-connection path each chunk counts against the rows the
        previous committed chunks wrote, which also accumulates to the
        true aggregate, but the refusal surfaces after the committed
        prefix: :class:`~taskq.exceptions.BatchMaxPendingExceededError`
        raises with STREAM-GLOBAL refused item indices and an
        ``admitted_count`` covering every committed item; items after
        the refusing chunk were never attempted.  Retry only the
        refused items (or rely on ``idempotency_key``s).  The atomic
        path (``failure_policy``/``finalizer``, no connection) resolves
        the same effective caps and keeps all-or-nothing: a cap
        violation rolls back the entire single transaction and raises
        plain :class:`~taskq.exceptions.MaxPendingExceededError` with
        nothing committed.

        **unique_for (not applied on batch paths):** actor-declared
        ``unique_for`` is a single-enqueue contract (see
        :meth:`enqueue_batch`'s disclosure), every item writes, items
        sharing an ``(actor, identity_key)`` are not deduplicated, and
        ``unique_for`` items are fully counted toward ``max_pending``.
        This holds on both arms (the per-chunk INSERT and the atomic
        path's chunked INSERT). The ``idempotency_key`` arbiter IS
        applied on both arms: a colliding item returns the existing
        row, exactly as :meth:`enqueue_batch` does.
        """
        if chunk_size < 1 or chunk_size > MAX_BATCH_SIZE:
            raise ValueError(f"chunk_size must be in [1, {MAX_BATCH_SIZE}], got {chunk_size}")

        from taskq._ids import new_job_id

        resolved_batch_id = UUID(bytes=new_job_id().bytes) if batch_id is None else batch_id

        # Peek the iterable, empty raises ValueError.
        it = iter(items)
        try:
            first_item = next(it)
        except StopIteration:
            raise ValueError("items must not be empty") from None

        # Re-chain the first item back into the stream.
        def _chain() -> Iterable[EnqueueItem]:
            yield first_item
            yield from it

        has_batch_extras = failure_policy is not None or finalizer is not None

        # Build finalizer args (without batch_id stamping).
        finalizer_args: EnqueueArgs | None = None
        if finalizer is not None:
            finalizer_args = build_enqueue_args(
                finalizer.actor_ref,
                finalizer.payload,
                scheduled_at=finalizer.scheduled_at,
                priority=finalizer.priority,
                fairness_key=finalizer.fairness_key,
                identity_key=finalizer.identity_key,
                idempotency_key=finalizer.idempotency_key,
                idempotency_scope=finalizer.idempotency_scope,
                metadata=dict(finalizer.metadata),
                start_to_close=finalizer.start_to_close,
                tags=finalizer.tags,
                idempotency_max_bytes=self._idempotency_max_bytes,
            )

        # Build a lazy generator of EnqueueArgs. The payload is validated
        # exactly ONCE per item, inside build_enqueue_args, and a
        # ValidationError from there is re-annotated with the item's index
        # and actor here. Why not validate upfront and discard the result
        # (the previous shape): that ran pydantic-core twice per batch item
        # on a hot path, and the discarded pass could not be reused because
        # build_enqueue_args re-validates internally. Note the idempotency/
        # scheduling checks inside build_enqueue_args now precede payload
        # validation for a doubly-invalid item, the same precedence a
        # single enqueue already has.
        # H4: collect per-item (actor_ref, args_id) as a side effect so handles
        # can be paired by index after the backend returns rows. This avoids
        # using a single actor's result_adapter for all handles (mixed-actor
        # batches would get wrong deserialization).
        item_meta: list[tuple[ActorRef[Any, Any], JobId]] = []

        # Effective per-actor caps, memoized as actors appear (same
        # resolution as :meth:`enqueue` / :meth:`enqueue_batch`): without
        # it, an operator-stored ``actor_config.max_pending`` on a
        # literal-less actor never reaches the args, the backend's cap
        # groups skip the actor entirely, and the override is silently
        # unenforced. The chunked arm awaits the cache between chunks; the
        # atomic arm pre-warms the snapshot (see below) and peeks it here,
        # synchronously, because the backend consumes this generator
        # mid-transaction where no await is possible.
        effective_mp: dict[str, int | None] = {}

        def _lazy_args(stream: Iterable[EnqueueItem]) -> Iterable[EnqueueArgs]:
            for idx, item in enumerate(stream):
                ref = item.actor_ref
                if ref.name not in effective_mp:
                    effective_mp[ref.name] = self._capacity_cache.peek_max_pending(
                        ref.name, ref.max_pending
                    )
                try:
                    args = build_enqueue_args(
                        ref,
                        item.payload,
                        scheduled_at=item.scheduled_at,
                        priority=item.priority,
                        fairness_key=item.fairness_key,
                        identity_key=item.identity_key,
                        idempotency_key=item.idempotency_key,
                        idempotency_scope=item.idempotency_scope,
                        metadata=dict(item.metadata),
                        start_to_close=item.start_to_close,
                        tags=item.tags,
                        idempotency_max_bytes=self._idempotency_max_bytes,
                        # The H5 strip-then-stamp boundary runs inside
                        # build_enqueue_args: any batch_id on item.metadata
                        # is stripped before the library's own is stamped.
                        stamp_batch_id=str(resolved_batch_id),
                        # Stored operator overrides win over the @actor
                        # literal (same resolution as enqueue /
                        # enqueue_batch); peek is synchronous because the
                        # backend consumes this generator mid-transaction
                        # where no await is possible.
                        max_pending=effective_mp[ref.name],
                    )
                except ValidationError as exc:
                    raise _item_payload_error(idx, ref.name, exc) from exc
                item_meta.append((ref, args.id))
                yield args

        # Determine queue from the first item.
        queue = first_item.actor_ref.queue

        # Build BatchRow when failure_policy OR finalizer is set (C3:
        # finalizer-only batches also need a row for list_batches
        # discoverability and finalizer_job_id auto-exclusion in
        # wait_for_batch). When only finalizer is set, failure_threshold=None.
        # expected_size=0 is a sentinel, the backend computes the real count
        # from the iterable (H6: no materialization).
        batch_row: BatchRow | None = None
        if failure_policy is not None or finalizer is not None:
            threshold = failure_policy.failure_threshold if failure_policy is not None else None
            batch_row = BatchRow(
                id=resolved_batch_id,
                queue=queue,
                status="active",
                expected_size=0,
                consecutive_failures=0,
                failure_threshold=threshold,
                finalizer_job_id=finalizer_args.id if finalizer_args is not None else None,
                originating_actor=None,
                created_at=self._clock.now(),
                completed_at=None,
                metadata={},
            )

        all_handles: list[JobHandle[BaseModel | None]] = []
        total_count = 0

        if has_batch_extras and connection is None:
            # Autonomous atomic path. H6: do NOT materialize the iterable ,
            # pass the lazy generator directly to the backend, which consumes
            # it in chunks inside its transaction. expected_size=0 is a
            # sentinel; the backend computes the real count from the items
            # consumed. H4: item_meta is populated as a side effect of the
            # generator being consumed, providing per-item actor_refs and
            # args_ids for handle pairing.
            #
            # Warm the capacity snapshot BEFORE the backend's transaction
            # starts: _lazy_args is a SYNC generator consumed mid-transaction,
            # so it cannot await a (possibly stale-triggering) refresh at
            # yield time. One awaited resolution here, for the first item's
            # actor, whose result the generator also reuses, spends the same
            # single refresh-per-call budget enqueue_batch spends; every
            # actor the stream later introduces resolves from that snapshot
            # via peek_max_pending. A failed refresh fails open to literals,
            # exactly like every other arm.
            first_ref = first_item.actor_ref
            effective_mp[first_ref.name] = await self._capacity_cache.effective_max_pending(
                first_ref.name, first_ref.max_pending
            )
            all_rows = await self._backend.enqueue_batch_atomic(
                _lazy_args(_chain()),
                batch_id=resolved_batch_id,
                queue=queue,
                batch_row=batch_row,
                finalizer_args=finalizer_args,
                chunk_size=chunk_size,
            )
            non_finalizer_count = len(all_rows) - (1 if finalizer is not None else 0)
            for i in range(non_finalizer_count):
                row = all_rows[i]
                ref, args_id = item_meta[i]
                all_handles.append(
                    JobHandle(
                        client=self,
                        row=row,
                        result_adapter=ref.result_adapter,
                        was_existing=(row.id != args_id),
                        _redis_client=self._redis_client,
                        _settings=self._settings,
                    )
                )
            total_count = non_finalizer_count
            finalizer_row = all_rows[-1] if finalizer is not None else None
        else:
            # Chunked path (caller-owned connection or no extras). The
            # batch row is created AFTER all chunk inserts: create_batch is
            # INSERT (not upsert), so the row must carry the real
            # expected_size, which is only known once the stream is drained.
            # KNOWN LIMITATION: until the row exists, a child job reaching a
            # terminal state finds no batch row, increment_batch_failures
            # returns (0, None, 0) and the failure is NOT counted toward
            # failure_policy (see the docstring disclosure above). Insert
            # the finalizer first so its returned row id is known for
            # finalizer_job_id (M4).
            stream = _chain()
            finalizer_row = None
            if finalizer is not None:
                assert finalizer_args is not None
                finalizer_row = await self._backend.enqueue_with_conn(connection, finalizer_args)  # type: ignore[arg-type]  # Why: guarded by has_batch_extras; when connection is provided it is runtime-compatible

            # Consume chunks. The payload is validated exactly ONCE per item
            # (inside build_enqueue_args, via build_batch_args), the previous
            # per-item pre-validation pass ran pydantic-core twice per item and
            # its result was discarded. Every per-item typed error this chunk
            # can raise, pydantic validation from the args build, the jsonb
            # NUL guard and the per-actor cap partition from the backend call
            # , crosses ONE remapping boundary below (_ITEM_ERROR_REMAPS) and
            # leaves annotated at stream-global indices, keeping the
            # index-annotated PayloadValidationError contract (M6) without a
            # second validation on the happy path.
            while True:
                chunk_items = list(islice(stream, chunk_size))
                if not chunk_items:
                    break
                # Resolve effective caps for actors NEW to this chunk
                # (memoized in effective_mp across chunks, the TTL'd
                # cache makes this a lookup after the first) before the
                # args are built, so a stored override on a literal-less
                # actor is enforced exactly as enqueue_batch enforces it.
                for ci in chunk_items:
                    if ci.actor_ref.name not in effective_mp:
                        effective_mp[
                            ci.actor_ref.name
                        ] = await self._capacity_cache.effective_max_pending(
                            ci.actor_ref.name, ci.actor_ref.max_pending
                        )
                # The stream-global base for every per-item error this
                # chunk can raise, captured BEFORE anything is built or
                # inserted: total_count is exactly the committed prefix
                # (chunks 1..N-1), so a chunk-local item index plus this
                # base names the item's position in the CALLER's stream.
                chunk_offset = total_count
                chunk_args: list[EnqueueArgs] | None = None
                # ONE boundary for every remappable per-item error: the
                # registry's members (and their subclasses, via the
                # isinstance dispatch) re-raise with stream-global item
                # indices; anything else, driver errors, programming
                # errors, crosses untouched.
                try:
                    chunk_args = build_batch_args(
                        chunk_items, resolved_batch_id, max_pending_by_actor=effective_mp
                    )
                    chunk_rows = await self._backend.enqueue_batch(
                        chunk_args, connection=connection
                    )  # type: ignore[call-arg]  # Why: asyncpg.Connection is compatible with the protocol's connection parameter at runtime
                except _REMAPPABLE_ITEM_ERROR_TYPES as exc:
                    _remap_chunk_item_error(exc, chunk_items, chunk_args, chunk_offset)
                assert (
                    chunk_args is not None
                )  # Why: bound inside the try before the backend call; every failure path re-raises above
                for i, row in enumerate(chunk_rows):
                    all_handles.append(
                        JobHandle(
                            client=self,
                            row=row,
                            result_adapter=chunk_items[i].actor_ref.result_adapter,
                            was_existing=(row.id != chunk_args[i].id),
                            _redis_client=self._redis_client,
                            _settings=self._settings,
                        )
                    )
                total_count += len(chunk_items)

            if batch_row is not None:
                if finalizer_row is not None:
                    batch_row = replace(
                        batch_row, finalizer_job_id=finalizer_row.id, expected_size=total_count
                    )
                else:
                    batch_row = replace(batch_row, expected_size=total_count)
                await self._backend.create_batch(
                    batch_row.id,
                    batch_row.queue,
                    batch_row.expected_size,
                    batch_row.failure_threshold,
                    batch_row.finalizer_job_id,
                    batch_row.originating_actor,
                    connection=connection,  # type: ignore[arg-type]  # Why: connection may be None but create_batch handles that
                )

        # Build finalizer handle.
        finalizer_handle: JobHandle[BaseModel | None] | None = None
        if finalizer is not None and finalizer_row is not None:
            finalizer_handle = JobHandle(
                client=self,
                row=finalizer_row,
                result_adapter=finalizer.actor_ref.result_adapter,
                was_existing=(finalizer_row.id != finalizer_args.id)
                if finalizer_args is not None
                else False,
                _redis_client=self._redis_client,
                _settings=self._settings,
            )
            all_handles.append(finalizer_handle)

        logger.debug(
            "batch-streaming-enqueued",
            kind="batch-streaming-enqueued",
            batch_id=str(resolved_batch_id),
            size=total_count,
        )

        return BatchHandle(
            batch_id=resolved_batch_id,
            job_handles=all_handles,
            size=total_count,
            finalizer_handle=finalizer_handle,
        )

    async def get_batch(self, batch_id: UUID) -> BatchRow | None:
        """Fetch a single batch row by ID.

        Delegates to :meth:`Backend.get_batch`. Returns ``None`` when the
        batch does not exist.
        """
        return await self._backend.get_batch(batch_id)

    async def list_batches(
        self,
        filter: BatchFilter,
    ) -> list[BatchSummary]:
        """List batches matching *filter*, returning :class:`BatchSummary` objects.

        Delegates to :meth:`Backend.list_batches` and maps each
        ``(BatchRow, BatchCounts)`` pair to a :class:`BatchSummary`
        with a :class:`BatchCompletionStatus` derived from the live counts.
        """
        from taskq.batch import BatchCompletionStatus

        pairs = await self._backend.list_batches(filter)
        summaries: list[BatchSummary] = []
        for row, counts in pairs:
            completion = BatchCompletionStatus(
                total=counts.total,
                pending=counts.pending,
                succeeded=counts.succeeded,
                failed=counts.failed,
                cancelled=counts.cancelled,
                crashed=counts.crashed,
                abandoned=counts.abandoned,
            )
            summaries.append(
                BatchSummary(
                    batch_id=row.id,
                    queue=row.queue,
                    status=row.status,
                    expected_size=row.expected_size,
                    consecutive_failures=row.consecutive_failures,
                    failure_threshold=row.failure_threshold,
                    finalizer_job_id=row.finalizer_job_id,
                    originating_actor=row.originating_actor,
                    created_at=row.created_at,
                    completed_at=row.completed_at,
                    completion=completion,
                )
            )
        return summaries

    async def enqueue_batch_fast(
        self,
        items: list[EnqueueItem],
        *,
        batch_id: UUID | None = None,
        connection: "asyncpg.Connection | None" = None,
    ) -> int:
        """Enqueue jobs via COPY FROM protocol for maximum throughput.

        **WARNING, bulk-import semantics, not general-purpose enqueue:**
        this method does NOT detect or
        reject idempotency-key collisions (a duplicate key aborts the
        whole batch instead of being treated as "already enqueued"), and
        returns a bare row **count**, not per-job handles, there is no
        way to await, cancel, or otherwise reference an individual job
        from the return value. ``max_pending`` IS enforced, with the same
        per-actor partition admission and the same effective-cap
        resolution (operator-stored override when set, else the
        ``@actor(...)`` literal) as :meth:`enqueue_batch` (see its
        docstring). Use :meth:`enqueue_batch`
        unless you specifically need COPY-level throughput for a one-shot
        bulk import/backfill and have already accounted for these gaps.

        Returns the count of inserted rows, no :class:`~taskq.batch.BatchHandle`,
        no :class:`~taskq.client.JobHandle` instances.

        **Validation rules:**

        - ``len(items) == 0`` raises :class:`ValueError`.
        - ``len(items) > 50_000`` raises :class:`ValueError`.
        - ALL payloads are validated before any INSERT, a single failure
          raises :class:`~taskq.exceptions.PayloadValidationError`.

        **Tradeoffs vs enqueue_batch:**

        - **No idempotency-key collision handling.** A duplicate key
          aborts the entire batch with
          :class:`~taskq.exceptions.DuplicateIdempotencyKeyError`, a
          same-``(idempotency_scope, idempotency_key)`` pair, whether
          repeated within the batch or already stored; nothing is
          written. Callers must pre-deduplicate. One carve-out: during
          the ``01.00.03`` pre→post migration window, a key reused
          across *different* scopes raises
          :class:`~taskq.exceptions.ScopedIdempotencyMigrationPendingError`
          instead, matching the other enqueue paths.
        - **No unique_for preflight.** Actor-declared ``unique_for`` is
          a single-enqueue contract (see :meth:`enqueue_batch`'s
          disclosure): the COPY writes every item, items sharing an
          ``(actor, identity_key)`` are not deduplicated, and
          ``unique_for`` items are fully counted toward ``max_pending``
         , same semantics as the unnest batch tier.
        - **max_pending partition admission.** One aggregated count runs
          before the COPY: within-cap actors' rows are written, and an
          over-cap actor's items are refused, the COPY of the admitted
          rows commits first, then
          :class:`~taskq.exceptions.BatchMaxPendingExceededError` raises
          naming the refused actors and item indices (retry only those,
          or rely on idempotency keys).
        - **No JobHandle instances.** Only the inserted row count is
          returned.  Use ``batch_id`` to query rows post-insert.
        - **All-or-nothing on constraint violations.** The entire COPY
          fails on any constraint violation (duplicate keys, singleton,
          CHECK), only cap admission partitions.

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
        for item in items:
            ref = item.actor_ref
            validate_actor_payload(ref.payload_type, item.payload, actor=ref.name)

        # Phase 1.5: Resolve the effective cap per actor, the same
        # resolution and rationale as enqueue_batch (see the comment
        # there): a stored operator override on a literal-less actor must
        # be visible to the backend's cap groups, which skip actors
        # carrying no cap.
        effective_mp: dict[str, int | None] = {}
        for item in items:
            ref = item.actor_ref
            if ref.name not in effective_mp:
                effective_mp[ref.name] = await self._capacity_cache.effective_max_pending(
                    ref.name, ref.max_pending
                )

        # Phase 2: Build per-item EnqueueArgs
        args_list = build_batch_args(items, resolved_batch_id, max_pending_by_actor=effective_mp)

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
        the row in a :class:`JobHandle[R]`. The lookup reads the hot
        ``jobs`` table first and falls back to ``jobs_archive`` (the
        same jobs-then-archive probe ``taskq job show`` applies), so an
        id whose row a prune moved to the archive returns that row's
        handle with :attr:`JobHandle.archived` ``True`` instead of
        reading as missing; ``None`` still means "in neither tier".
        The caller may supply
        ``result_adapter`` because lookups by id do not carry actor
        identity, typical sources are
        ``my_actor.result_adapter`` (when reuniting with an actor) or
        ``TypeAdapter(type(None))`` (when only row metadata is needed).
        When *result_adapter* is ``None`` it defaults to
        ``TypeAdapter(type(None))``, which is suitable for status-only
        lookups.
        """
        adapter: TypeAdapter[R] = (
            result_adapter if result_adapter is not None else _NONE_RESULT_ADAPTER
        )  # type: ignore[assignment]  # Why: TypeAdapter[None] does not narrow to TypeAdapter[R] under pyright; runtime behaviour is correct because None is assignable to the R bound
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

    async def get_row(self, job_id: JobId) -> JobRow | None:
        """Look up a job by id and return the raw :class:`JobRow`.

        Mirrors :meth:`get`'s contract, one ``backend.get`` (hot table
        first, ``jobs_archive`` fallback, an archive hit marked
        ``archived=True``), ``None`` when the job exists in neither
        tier, without the handle machinery or
        result adapter. For callers that never need a
        :class:`JobHandle`, this is the direct form; for the fresh-read
        case that does want a handle, prefer ``get`` plus the handle's
        ``row`` property (still a single round trip).
        """
        with self._translate_schema_errors():
            return await self._backend.get(job_id)

    async def list(self, filter: JobFilter) -> JobPage:
        """List jobs matching *filter*, returning a :class:`JobPage`.

        ``filter.status`` accepts a single :data:`JobStatus` or a
        sequence of statuses (e.g. ``JobFilter(status=["pending",
        "running"])``).

        ``filter.unfinished`` is a meta-filter: ``unfinished=True`` selects
        *non-terminal* statuses (pending, scheduled, running, 'not yet
        finished', not 'currently executing') and ``unfinished=False`` selects
        terminal ones.  See :class:`JobFilter` for full semantics.

        ``next_cursor`` is returned for every ordering, encoded from the
        columns that ordering actually sorts by, and is ``None`` exactly on
        the last page: the backend is asked for one row past the limit, so
        a last page that happens to fill the limit is still known to be the
        last, and a caller paging until the cursor runs out never fetches an
        empty trailing page. ``filter.limit`` is capped at
        :data:`~taskq.backend._protocol.MAX_JOB_LIST_LIMIT` (``ValueError``
        above it); at the ceiling there is no room to look past the limit,
        so a full page there carries a cursor that may lead to one empty
        page.
        """
        if filter.limit > MAX_JOB_LIST_LIMIT:
            raise ValueError(
                f"limit must be <= {MAX_JOB_LIST_LIMIT}, got {filter.limit}; page with cursor "
                "for larger result sets"
            )
        if filter.limit < MAX_JOB_LIST_LIMIT:
            probe = dataclasses.replace(filter, limit=filter.limit + 1)
        else:
            probe = filter
        with self._translate_schema_errors():
            fetched = await self._backend.list_jobs(probe)
        rows = fetched[: filter.limit]
        next_cursor: str | None = None
        if rows and len(fetched) >= probe.limit:
            next_cursor = encode_job_cursor(rows[-1], filter.order_by)
        return JobPage(jobs=rows, next_cursor=next_cursor)

    # ── Cancel ─────────────────────────────────────────────────────────

    async def cancel(
        self,
        job_id: JobId,
        reason: str | None = None,
    ) -> CancelResult:
        """Request cancellation of a job and return a :class:`CancelResult`.

        Reads the row first via :meth:`Backend.get`. If the job does not
        exist, raises :class:`KeyError`, matching Python's stdlib
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
            job_id=str(job_id),
            previous_status=previous_status,
            cancellation_initiated=initiated,
        )
        return result

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None = None,
        *,
        allow_empty_filter: bool = False,
    ) -> BulkCancelResult:
        """Cancel all jobs matching *filter* in a single set-based operation.

        Pending/scheduled jobs are moved straight to terminal 'cancelled'
        (no running actor to cooperate with). Running jobs get
        ``cancel_phase=1`` set (cooperative cancel), the worker's
        heartbeat-driven cancel controller observes the phase change and
        sets the in-process ``cancel_event``.

        **Guardrail:** a filter with no predicates (no queue, status,
        actor, identity_key, batch_id, tags, active, or created_before)
        is rejected with :class:`EmptyFilterError` unless
        ``allow_empty_filter=True`` is passed.

        **Filter fields used:** ``queue``, ``status``, ``actor``,
        ``identity_key``, ``batch_id``, ``tags``, ``active``,
        ``created_before``. The ``limit``, ``cursor``, and ``order_by``
        fields are ignored.

        Returns a :class:`BulkCancelResult` with counts and affected IDs.
        """
        from taskq.obs import record_cancel_requested

        if not allow_empty_filter and not filter.has_predicates():
            raise EmptyFilterError()

        record_cancel_requested()

        with self._translate_schema_errors():
            return await self._backend.cancel_where(filter, reason)

    # ── Schedule CRUD ────────────────────────────────────────────────────

    def _server_clock_pool(self) -> "asyncpg.Pool | None":
        """The pool a server-clock read runs on, or ``None`` when this
        backend is not pool-backed (in-memory tests).

        The :class:`Backend` protocol deliberately does not expose pools
        (they live on ``BackendDeps``); ``PostgresBackend`` carries them
        as private accessors delegating to its deps, so probe the public
        ``BackendDeps`` shape first and the Postgres accessor second.
        """
        for attr in ("worker_pool", "_worker_pool"):
            pool = getattr(self._backend, attr, None)
            if pool is not None:
                # Why: cast, duck-typed probe; only an asyncpg-shaped pool reaches this line in practice.
                return cast("asyncpg.Pool", pool)
        return None

    async def _schedule_seed_now(self) -> datetime:
        """Read the clock that arbitrates a schedule's due-check, for
        seeding ``next_fire_at``.

        The cron loop's due-check is server-side
        (``next_fire_at <= clock_timestamp()``) and its normal-path
        recompute re-anchors on the STORED fire time, only a miss
        beyond ``cron_catch_up_window`` re-anchors on the server clock ,
        so the seed fixes the fire chain's phase for the schedule's
        life. Pool-backed (Postgres) clients therefore read the server
        clock first: one row, ``SELECT clock_timestamp()``, mirroring
        the worker bootstrap's schedule seeding. Pool-less clients
        (in-memory tests) have no second clock domain and use the
        client's injected Clock, tests wire it to the backend's clock.

        The pool acquire + read is bounded by
        ``DEFAULT_CAPACITY_READ_TIMEOUT``, the same discipline as
        ``ActorCapacityCache._refresh``: asyncpg acquire has no default
        timeout, so an unbounded acquire wedges every
        ``create_schedule``/``update_schedule`` on an exhausted pool. On
        timeout a clear :class:`TimeoutError` is raised; falling back to
        the client clock here is deliberately NOT an option, because a
        skewed seed would phase-shift the fire chain for the schedule's
        life, a wedged pool must surface.
        """
        pool = self._server_clock_pool()
        if pool is None:
            return self._clock.now()

        async def _read_server_now() -> datetime:
            async with pool.acquire() as conn:
                # Why: annotated assignment, clock_timestamp() is non-null in Postgres; mirrors the worker bootstrap's read.
                seed_now: datetime = await conn.fetchval("SELECT clock_timestamp()")
            return seed_now

        try:
            return await asyncio.wait_for(_read_server_now(), timeout=DEFAULT_CAPACITY_READ_TIMEOUT)
        except TimeoutError as exc:
            raise TimeoutError(
                f"schedule seed clock read timed out after "
                f"{DEFAULT_CAPACITY_READ_TIMEOUT}s: the pool is exhausted or "
                "wedged. Refusing to seed next_fire_at from the client clock, "
                "that would mix clock domains against the server-side due-check."
            ) from exc

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
        owner: Literal["code", "operator"] = "operator",
    ) -> "ScheduleHandle":
        """Create a cron schedule.  Raises :class:`ValueError` if both
        *payload_factory* and *static_payload* are provided, or if
        *cron_expr* is invalid.

        *owner* declares who owns the schedule's enable/disable lifecycle.
        ``"operator"`` (the default here: a client-created schedule is an
        operator act) records operator ownership from birth, so a schedule
        created disabled carries ``disabled_by='operator'`` and no worker
        boot ever reverts it; ``"code"`` matches a ``@cron`` declaration,
        whose stale auto-disable a boot reverts (see
        :class:`CronScheduleSpec`).

        The ``(actor, name)`` UNIQUE constraint means each ``(actor, name)``
        pair may have at most one schedule; a second ``create_schedule`` for
        the same pair raises ``asyncpg.UniqueViolationError`` (PG) or
        :class:`ValueError` (in-memory).  Pass distinct *name* values to run
        several cron schedules per actor (e.g. a per-property sync).

        When *identity_key* is set, the cron loop propagates it to cron-fired
        jobs so they dedup against on-demand jobs for the same business key.

        Does NOT validate actor existence at creation time, any string
        actor name is accepted (validation is deferred to fire time).

        The first ``next_fire_at`` is seeded from the clock that
        arbitrates its due-check: on a Postgres-backed client the PG
        server clock is read first (one-row ``SELECT clock_timestamp()``
        via the backend's pool, mirroring the worker bootstrap), so
        app↔DB clock skew cannot shift the fire chain; on a pool-less
        (in-memory) client the seed comes from the client's injected
        Clock. This matters permanently: the cron loop's normal path
        recomputes every subsequent fire from the STORED fire time
        (only a miss beyond ``cron_catch_up_window`` re-anchors on the
        server clock), so the seed, not any per-tick correction ,
        fixes the chain's phase for the schedule's life.

        Args:
            dst_strategy: How to handle DST gaps and overlaps.
                ``skip`` (default) advances past gaps, uses the first
                occurrence in overlaps. ``firstof`` explicitly selects
                the earlier wall-clock time in overlaps. ``allof`` fires at
                both occurrences in overlaps.
        """
        # Lazy import: croniter (+ dateutil) costs ~16ms at import time and
        # is only needed on the cron-schedule path, not for ``import taskq``.
        from croniter import croniter

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

        now = await self._schedule_seed_now()
        next_fire = compute_next_fire_after(cron_expr, timezone, now, dst_strategy=dst_strategy)[0]

        args = ScheduleCreateArgs(
            actor=actor_name,
            cron_expr=cron_expr,
            timezone=timezone,
            next_fire_at=next_fire,
            dst_strategy=dst_strategy,
            payload_factory=payload_factory,
            enabled=enabled,
            owner=owner,
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
        pass ``clear_payload_factory=True``, ``None`` for payload_factory
        means "don't change this field."

        When *cron_expr* changes, the recomputed ``next_fire_at`` is
        seeded from the same clock as ``create_schedule`` (the PG
        server clock on Postgres-backed clients; the client's injected
        Clock in-memory), the stored chain keeps its server-anchored
        phase.
        """
        # Lazy import: croniter (+ dateutil) costs ~16ms at import time and
        # is only needed on the cron-schedule path, not for ``import taskq``.
        from croniter import croniter

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
            now = await self._schedule_seed_now()
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
        """Delete a cron schedule by ID.  Idempotent, no error if missing."""
        await self._backend.delete_schedule(schedule_id)

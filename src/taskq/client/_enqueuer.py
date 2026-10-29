"""Sub-job enqueuer, enqueues child jobs from within an actor body.

Connection resolution, in priority order:

1. An explicit ``connection=`` argument to ``enqueue()`` /
   ``enqueue_batch()``, an immediate write on that connection, outside
   any transaction lifecycle.
2. A constructor-bound ``transaction_conn``, the per-job enqueuer the
   dispatch path constructs around the job's slot transaction
   connection. Writes join that transaction, and the transactional
   buffering lifecycle (``flush_buffer`` / ``discard_buffer`` /
   ``drain_for_re_enqueue``) is active **by construction**, the
   binding is what activates it, not where the connection came from.
3. A LOOP-scope ``asyncpg.Connection`` from ``loop_scope_resolved`` ,
   the provenance-based inference the loop-level shared enqueuer uses
   for the single-slot / autonomous path.
4. The worker pool, autonomous commit, no transactional semantics.

Parent-tag propagation: the consumer sets the parent job's tags via
``set_parent_tags()`` before actor invocation and resets them after
(via ``parent_tags()`` context manager or manual token reset). The
``contextvars.ContextVar`` ensures concurrent consumers in the same
event loop each see their own parent tags, asyncio Tasks copy the
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
    ConnLike,
    EnqueueArgs,
    IdempotencyKey,
    IdentityKey,
    JobId,
    JobRow,
    JobStatus,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.batch import MAX_BATCH_SIZE, EnqueueItem
from taskq.client._args import (
    UniqueForNoIdentityWarner,
    build_batch_args,
    build_enqueue_args,
    enqueue_span,
)
from taskq.client._backpressure import read_backpressure
from taskq.client._capacity import (
    DEFAULT_CAPACITY_READ_TIMEOUT,
    ActorCapacityCache,
)
from taskq.client._handle import JobHandle
from taskq.exceptions import (
    BatchIdExistsError,
    BatchMaxPendingExceededError,
    PartialBatchError,
    SchemaNotMigratedError,
    SubEnqueueError,
)
from taskq.types import BackpressureSnapshot

if TYPE_CHECKING:
    import asyncpg

    from taskq.actor import ActorRef

__all__ = [
    "SubJobEnqueuer",
    "current_parent_id",
    "parent_tags",
    "set_parent_job_id",
    "set_parent_tags",
]

_log: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_parent_tags_var: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "taskq_parent_tags",
    default=(),
)

# The fan-out ledger's contextvar (LIB-2): the enqueuing parent's job id,
# the sibling of the tag-inheritance var above. Set together at worker
# entry (worker/run.py and worker/_consumer.py set BOTH — the ledger
# stamp and the tag inheritance are one parent context), read by every
# child-creating enqueue arm. The ledger is EXACT accounting: children
# carry parent_id regardless of whether they inherit tags
# (inherit_tags=False suppresses decoration, not linkage), and the
# backpressure read counts pending children by it exactly.
_parent_job_id_var: contextvars.ContextVar[JobId | None] = contextvars.ContextVar(
    "taskq_parent_job_id",
    default=None,
)


def _missing_schema_errors() -> tuple[type[BaseException], ...]:
    """The lazy-import discipline (asyncpg may be absent): the exception
    types this module's schema translations catch (the F5 review fix: the
    actor-body read reports a missing schema as
    :class:`SchemaNotMigratedError`, not a raw asyncpg traceback). A tuple
    so the caller's single ``except`` stays one line; empty when the
    postgres extra is absent, and an empty except never matches."""
    try:
        import asyncpg
    except ImportError:  # pragma: no cover - the postgres extra absent
        return ()
    return (asyncpg.exceptions.UndefinedTableError,)


def set_parent_tags(tags: tuple[str, ...]) -> contextvars.Token[tuple[str, ...]]:
    """Set the parent job's tags for sub-job tag inheritance.

    Called by the consumer before actor invocation. The returned token
    must be used to reset the context after the actor completes, use
    ``_parent_tags_var.reset(token)`` or the ``parent_tags()`` context
    manager.
    """
    return _parent_tags_var.set(tags)


def set_parent_job_id(job_id: JobId) -> contextvars.Token[JobId | None]:
    """Set the enqueuing parent's job id for child fan-out accounting.

    Called by the worker entry points beside :func:`set_parent_tags` (or
    through :func:`parent_tags`' ``job_id`` parameter). The returned
    token must be used to reset the context after the actor completes,
    use ``_parent_job_id_var.reset(token)``.
    """
    return _parent_job_id_var.set(job_id)


def current_parent_id() -> JobId | None:
    """The ambient parent's job id, ``None`` outside any parent context.

    What every child-creating enqueue arm stamps into
    ``EnqueueArgs.parent_id``. Scoped to the async task the worker entry
    set it in; the entry's reset is what unwinds it (a stale id must
    never leak into a later enqueue on the same loop).
    """
    return _parent_job_id_var.get()


@contextlib.contextmanager
def parent_tags(tags: tuple[str, ...], job_id: JobId | None = None) -> Generator[None, None, None]:
    """Context manager that sets the parent context for the duration of the block.

    The ONE context the worker entry installs: the parent's tags for
    sub-job tag inheritance, and — when *job_id* is passed — the
    parent's job id for the fan-out ledger (``parent_id`` stamped on
    every child enqueue, the exact pending-children accounting the
    backpressure read serves).

    Ensures both ContextVars are reset on all exit paths (success,
    exception, cancellation). Use this at worker entry points instead of
    manual set/reset::

        with parent_tags(tuple(job.tags), job_id=job.id):
            # actor invocation, sub-job enqueues, etc.
            ...
    """
    token = _parent_tags_var.set(tags)
    id_token = _parent_job_id_var.set(job_id) if job_id is not None else None
    try:
        yield
    finally:
        _parent_tags_var.reset(token)
        if id_token is not None:
            _parent_job_id_var.reset(id_token)


class SubJobEnqueuer:
    """Enqueue sub-jobs from within an actor body.

    Two construction shapes, deliberately distinct:

    * **Loop-level shared**, ``loop_scope_resolved`` set, no
      ``transaction_conn``. Serves the single-slot path and the
      autonomous fallback; transactional buffering is inferred from
      connection provenance (a LOOP-scope connection means "inside the
      consumer's transaction"). One instance per loop, so the
      per-100-enqueue re-warning fires on the loop-level counter, not
      per-job. Never shared across concurrent jobs that each own a
      transaction, its buffer state is unkeyed.
    * **Per-job bound**, ``transaction_conn`` set to the job's slot
      transaction connection (the shape ``dispatch_one_job``
      constructs whenever the worker runs a dedicated slot pool).
      Every write joins that one transaction and the buffers are this
      job's alone, so a sibling slot's flush/discard/drain can never
      reach them.
    """

    def __init__(
        self,
        loop_scope_resolved: Mapping[type, object] | None,
        worker_pool: asyncpg.Pool | None,
        backend: Backend,
        *,
        clock: Clock | None = None,
        capacity_cache: ActorCapacityCache | None = None,
        queues_strict: bool = False,
        env_queues: Sequence[str] | None = None,
        transaction_conn: ConnLike | None = None,
    ) -> None:
        self._loop_scope_resolved = loop_scope_resolved
        self._worker_pool = worker_pool
        self._backend = backend
        self._clock = clock if clock is not None else SystemClock()
        self._capacity_cache = (
            capacity_cache
            if capacity_cache is not None
            else ActorCapacityCache(backend, queues_strict=queues_strict, env_queues=env_queues)
        )
        self._transaction_conn = transaction_conn
        self._pending_buffer: list[EnqueueArgs] = []
        self._loop_enqueue_args: list[EnqueueArgs] = []
        self._autonomous_enqueue_count: int = 0
        self._unique_for_warner = UniqueForNoIdentityWarner()

    @property
    def capacity_cache(self) -> ActorCapacityCache:
        """The TTL-bounded capacity snapshot this enqueuer reads.

        Exposed read-only so a derived enqueuer (the per-job binding the
        dispatch path constructs) can share the loop-level cache instead
        of paying its own per-job snapshot refresh.
        """
        return self._capacity_cache

    async def backpressure(
        self,
        queues: Sequence[str],
        *,
        parent_id: JobId | None = None,
    ) -> BackpressureSnapshot:
        """Read the submit path's backpressure state for *queues*.

        The actor body's slice of the LIB-2 read (the fan-out decision
        happens HERE, in the parent's execution): same contract as
        :meth:`JobsClient.backpressure` — the actor-scoped verdict, the
        fail-open ``unknown`` state, advisory semantics — reading this
        enqueuer's own backend and capacity cache. ``parent_id`` defaults
        to the ambient parent context: inside an actor body that is THIS
        job, so the snapshot's fan-out ledger includes this parent's
        pending children with no arguments. See
        :class:`~taskq.types.BackpressureSnapshot`.
        """
        await self._capacity_cache.refresh()
        try:
            return await read_backpressure(
                self._backend,
                self._capacity_cache,
                list(dict.fromkeys(queues)),
                parent_id=parent_id if parent_id is not None else current_parent_id(),
                read_timeout=DEFAULT_CAPACITY_READ_TIMEOUT,
            )
        except _missing_schema_errors() as exc:  # type: ignore[misc]  # Why: the helper returns () when asyncpg is absent, and an empty tuple's except never matches.
            # The JobsClient arm's SchemaNotMigratedError translation: a
            # setup defect is not degraded data — the actor body sees the
            # actionable error, not a raw asyncpg traceback. The schema
            # name rides the asyncpg error when it carries one; the
            # shipped default is the honest fallback (a custom-schema
            # deployment reaching an unmigrated database through a worker
            # is already a boot failure).
            raise SchemaNotMigratedError(getattr(exc, "schema", None) or "taskq") from exc

    async def enqueue[P: BaseModel, R: BaseModel | None](
        self,
        actor_ref: ActorRef[P, R],
        payload: P,
        *,
        connection: asyncpg.Connection | None = None,
        allow_unregistered: bool = False,
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
        literal: against a non-NULL *stored* ``max_pending`` the tighter of
        the two wins (``min(stored, per_call)``, an
        explicit caller shedding load is never widened by an operator
        override, and no code path can raise an operator's fleet cap);
        with no stored value this parameter wins outright over the
        literal (historical behavior, actor code may loosen its own
        declaration).

        ``allow_unregistered`` is the per-submit escape from
        ``TASKQ_QUEUES_STRICT`` (whose hard branch this arm shares
        through the capacity cache): a child queue no registered
        assignment routes to normally raises
        :class:`~taskq.exceptions.UnknownQueueError` before any write;
        the flag skips that verdict for THIS call, for genuinely dynamic
        child queue names. The unserved-queue NOTE still fires on an
        escaped call. With strict off the flag changes nothing.

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
            # The sub-job arm's slice of the enqueue-time unserved-queue
            # check, the same snapshot verdict (zero I/O, warn-once per
            # queue per TTL) JobsClient.enqueue applies — and under
            # TASKQ_QUEUES_STRICT the same hard branch (the fan-out arm
            # carries no parallel validation path). allow_unregistered is
            # the per-submit escape for a genuinely dynamic child queue;
            # the note still fires on an escaped call.
            self._capacity_cache.maybe_warn_unserved_queue(
                resolved_queue, actor=actor_ref.name, allow_unregistered=allow_unregistered
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
                # LIB-2: the fan-out ledger stamp — the ambient parent's
                # job id when this runs under a parent context, None
                # otherwise (the builder stays pure, the arm reads the
                # contextvar once).
                parent_id=current_parent_id(),
            )
            if _batch_id is not None:
                # H5: stamp batch_id AFTER build_enqueue_args, which strips
                # any caller-supplied batch_id as a security boundary.
                args = replace(
                    args,
                    metadata={**args.metadata, "batch_id": _batch_id},
                )
            if span.is_recording():
                # Why the guard: on a non-recording span (no SDK, sampling)
                # set_attribute discards the value, so the str() of the job
                # id is paid per enqueue for nothing. Skipped, the exported
                # spans are unchanged: a recording span still gets exactly
                # this attribute.
                span.set_attribute("messaging.message.id", str(args.id))
            # The per-call seam's coherence check, the same warn-once
            # contract JobsClient.enqueue applies to the actor-declared
            # form; this is the only caller-facing surface that accepts a
            # per-call unique_for, and it was fully silent.
            if args.unique_for is not None and args.identity_key is None:
                self._unique_for_warner.maybe_warn(
                    actor=actor_ref.name, queue=actor_ref.queue, unique_for=args.unique_for
                )
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

        Caller tags are UNIONED with the parent's, so ``[]``, the
        identity element, resolves to the parent's tags exactly as
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
    ) -> tuple[ConnLike | None, bool]:
        """Resolve the connection for one write.

        Returns ``(conn, in_transaction)``. ``in_transaction`` is the
        gate on the transactional buffering lifecycle: True means the
        write joins a transaction the consumer owns, so the in-memory
        backend buffers it for flush/discard and the enqueuer tracks it
        for re-enqueue on snooze/retry. Resolution order: an explicit
        per-call connection (never transactional, the caller owns its
        lifecycle), then the constructor-bound ``transaction_conn``
        (transactional by construction), then LOOP-scope provenance
        (transactional by inference), then no connection at all.
        """
        import asyncpg as _asyncpg

        if connection is not None:
            return connection, False
        if self._transaction_conn is not None:
            return self._transaction_conn, True
        if (
            self._loop_scope_resolved is not None
            and (loop_conn := self._loop_scope_resolved.get(_asyncpg.Connection)) is not None
        ):
            # Why: cast, loop_conn comes from Mapping[type, object]; the DI resolver guarantees it is asyncpg.Connection at runtime
            return cast(_asyncpg.Connection, loop_conn), True
        return None, False

    async def _do_enqueue(
        self,
        args: EnqueueArgs,
        connection: asyncpg.Connection | None,
    ) -> JobRow:
        conn, in_transaction = self._resolve_connection(connection)

        if conn is not None:
            if in_transaction and self._backend.supports_transactional_simulation:
                self._pending_buffer.append(args)
                return self._synthesize_row(args)
            row = await self._backend.enqueue_with_conn(conn, args)
            if in_transaction:
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
        :func:`~taskq._ids.new_job_id`, mirrors
        :meth:`~taskq.client.JobsClient.enqueue_batch`. Pass an explicit
        ``batch_id`` to correlate this batch with a caller-constructed
        identifier (e.g. a finalizer job enqueued separately that needs to
        reference the same batch). An explicit ``batch_id`` naming an
        existing TERMINAL batch row raises
        :class:`~taskq.exceptions.BatchIdExistsError`, the same refusal
        the create_batch arms give a collision, a terminal batch must not
        gain a member. An ACTIVE row keeps accepting appends (resumption).

        Raises ``ValueError`` when ``items`` is empty or exceeds
        ``MAX_BATCH_SIZE``, the same guardrails
        :meth:`~taskq.client.JobsClient.enqueue_batch` applies to the
        identical operation one layer up. Without the empty check the
        no-connection fallback loop would iterate zero items and return
        ``[]`` silently. The backend binds every item
        as 26 parallel array parameters to a single ``unnest`` INSERT in one
        transaction, so an uncapped batch enqueued from inside a job body is
        unbounded fan-out that bypasses the client-side guardrail.

        ``max_pending``: the connection arm gets the backend's per-actor
        partition admission, over-cap actors' items are refused after
        the within-cap actors' items are inserted, and converts the
        refusal to :class:`~taskq.exceptions.PartialBatchError` so this
        method speaks one error contract across all connection modes
        (the no-connection fallback already raises it per item). On a
        LOOP-scope connection the inserted items ride the parent's
        transaction: catching the error inside the actor body and
        returning normally commits them; letting it propagate rolls
        everything back.
        """
        if len(items) == 0:
            raise ValueError("items must not be empty")
        if len(items) > MAX_BATCH_SIZE:
            raise ValueError(
                f"items must contain at most {MAX_BATCH_SIZE} entries, got {len(items)}"
            )

        resolved_batch_id = batch_id if batch_id is not None else UUID(bytes=new_job_id().bytes)

        conn, in_transaction = self._resolve_connection(connection)

        if conn is not None:
            effective_mp: dict[str, int | None] = {}
            # Memoized by actor name (verdict per new name only): the
            # name -> queue function assumption — enforced at the registry
            # boundary (worker/_bootstrap refuses keys that disagree with
            # ref.name; sync_actor_config CardinalityViolations on
            # duplicates) — so per-name memoization cannot hide a second
            # queue behind one actor.
            for item in items:
                ref = item.actor_ref
                if ref.name not in effective_mp:
                    effective_mp[ref.name] = await self._capacity_cache.effective_max_pending(
                        ref.name, ref.max_pending
                    )
                    self._capacity_cache.maybe_warn_unserved_queue(ref.queue, actor=ref.name)
            args_list = build_batch_args(
                items,
                resolved_batch_id,
                max_pending_by_actor=effective_mp,
                parent_id=current_parent_id(),
            )

            if in_transaction and self._backend.supports_transactional_simulation:
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

            try:
                rows = await self._backend.enqueue_batch(args_list, connection=conn)
            except BatchMaxPendingExceededError as exc:
                # Why convert here: the backend's per-actor partition
                # admission refuses the over-cap actors' items AFTER the
                # within-cap actors' items are inserted, and this method's
                # no-connection fallback already surfaces per-item
                # failures as PartialBatchError (succeeded count + failed
                # indices + per-failure exceptions). The connection arm
                # must speak the same error contract or actor code would
                # need connection-mode-dependent handling for the same
                # operation.
                refusal_by_actor = {r.actor: r for r in exc.refusals}
                failed_items: list[tuple[int, Exception]] = [
                    (i, refusal_by_actor[item.actor_ref.name])
                    for i, item in enumerate(items)
                    if item.actor_ref.name in refusal_by_actor
                ]
                if in_transaction:
                    # Track only the admitted items: the refused ones were
                    # never inserted on this connection, so a rollback
                    # re-enqueue (drain_for_re_enqueue) must not carry them
                    # as if they had been.
                    self._loop_enqueue_args.extend(
                        a for a in args_list if a.actor not in refusal_by_actor
                    )
                raise PartialBatchError(
                    succeeded_count=exc.admitted_count,
                    failed_items=failed_items,
                    total=len(items),
                ) from exc
            if in_transaction:
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

        # The fallback's member writes are single enqueues: no membership
        # lock wraps them, so the bulk arms' terminal-batch refusal never
        # runs on this path. Check the row once here: a terminal batch
        # must not gain a member through this arm either, the same typed
        # refusal the create_batch and bulk arms give a batch_id reuse.
        existing_batch = await self._backend.get_batch(resolved_batch_id)
        if existing_batch is not None and existing_batch.status != "active":
            raise BatchIdExistsError(resolved_batch_id, reason="terminal")

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

        A batch row that went terminal while the parent's transaction was
        in flight refuses its members at the backend's single-enqueue write
        site (the terminal-batch guard, the twin of the PG single path's
        membership lock), so a terminal batch's buffered members surface
        HERE, per item, through that same SubEnqueueError collection - not
        as a wholesale preflight raise, which would speak no documented
        error contract, discard the buffered args with no handle to them,
        and block the flush of unrelated batches' buffered members. The
        refusal itself is not negotiable: nothing from a terminal batch
        lands, the bulk arms refuse it the same way.
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
        row before the transaction commits, the stored row is decided
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
            # The fan-out ledger rides the display guess too (the F2 fix):
            # the synthetic row is what the caller sees before the
            # transaction commits, a parent_id it omits would make the
            # ledger lie on the transactional sub-enqueue path.
            parent_id=args.parent_id,
        )

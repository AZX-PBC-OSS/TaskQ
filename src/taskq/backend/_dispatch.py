"""Dispatch operations for PostgresBackend.

``dispatch_batch`` and its queue-mode resolver live here as module-level
functions.  :class:`~taskq.backend.postgres.PostgresBackend` methods
are thin wrappers that delegate.

The worker-side :class:`QueueModeCache` also lives here: the resolver it
caches is defined one screen above it, and the queue-ops seam
(:mod:`taskq.worker.queue_ops`) reaches it through
:func:`invalidate_queue_mode_caches` rather than holding a backend
reference — worker → backend is the correct layer direction.
"""

import time
import weakref
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import UUID

from taskq.backend._dispatch_sql import (
    dispatch_batch as dispatch_batch_helper,
)
from taskq.backend._protocol import ConnLike, JobRow
from taskq.backend._records import (
    _job_row_from_record,
    jsonb_param,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

logger = get_logger(__name__)

__all__ = [
    "QUEUE_MODE_CACHE_TTL_SECONDS",
    "QueueModeCache",
    "_dispatch_batch",
    "_resolve_queue_modes",
    "invalidate_queue_mode_caches",
]


QUEUE_MODE_CACHE_TTL_SECONDS: Final[float] = 5.0
"""How long a worker-side queue-mode resolution stays fresh.

Bounds the trade the cache makes: a mode flip (strict_fifo ↔ round_robin)
reaches an out-of-band worker within this many seconds, and in exchange
the per-dispatch-round resolve statement disappears — a notify-driven
fleet dispatches many times per second, so the round trip it removes is
paid far more often than the staleness it can cause. A stale mode is a
bounded fairness degradation, never a correctness one: the strict-FIFO
variant is the round-robin CTE with no-op fairness ranks, so a batch
dispatched under a stale strict mode still admits and orders by
priority/scheduled_at — only cohort interleaving waits out the TTL.
5 s matches the notify fallback poll cadence (notify_poll_interval), the
system's existing bound for how stale a worker's view of the database
may be before it re-reads on its own.
"""

_live_queue_mode_caches: "weakref.WeakSet[QueueModeCache]" = weakref.WeakSet()
"""Every cache a live backend instance owns, so the queue-ops seam can
clear them all without holding backend references. Weak: a backend's
cache must not outlive (or keep alive) the backend that owns it."""


class QueueModeCache:
    """Per-backend-instance TTL cache of resolved queue modes.

    Owned by one :class:`~taskq.backend.postgres.PostgresBackend` — the
    backend the worker's single dispatch loop dispatches through — so
    one worker's mode view never couples to another worker sharing the
    process (two workers, two schemas, two backends).

    Concurrency: the dispatch loop is single-per-worker, and no method
    here awaits — every mutation is one event-loop step, so entries are
    never observed torn and no lock is needed. A backend shared by
    concurrent dispatch callers on one loop can only race two misses
    into duplicate resolves; both stores write the same freshly read
    data, so last-write-wins is benign.

    Negative caching: a queue with no ``queues`` row is cached as
    ``strict_fifo`` (the resolver's fallback) with the same TTL — an
    unconfigured queue must not re-pay the round trip every dispatch,
    or the commonest deployment (no rows at all, nothing in TaskQ seeds
    them) would get no benefit. A queue configured mid-flight is picked
    up within the TTL, or immediately in-process via
    :func:`invalidate_queue_mode_caches`.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = QUEUE_MODE_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._entries: dict[str, tuple[str, float]] = {}
        _live_queue_mode_caches.add(self)

    def resolved_modes(self, queues: list[str]) -> set[str] | None:
        """The distinct-mode set for *queues* when every queue has a fresh
        entry, else ``None`` — the caller resolves the whole list through
        the query (the fallback for unknown and expired queues alike) and
        stores the result.

        An empty *queues* list answers ``{"strict_fifo"}`` from the
        resolver's own empty-list contract, without a query.
        """
        if not queues:
            return {"strict_fifo"}
        now = self._clock()
        modes: set[str] = set()
        for queue in queues:
            entry = self._entries.get(queue)
            if entry is None or now - entry[1] >= self._ttl_seconds:
                return None
            modes.add(entry[0])
        return modes

    def store(self, modes_by_queue: dict[str, str]) -> None:
        """Record freshly resolved per-queue modes, stamped now.

        Merges rather than replaces: a backend dispatching different
        queue lists keeps each list's entries without the others wiping
        them.
        """
        now = self._clock()
        for queue, mode in modes_by_queue.items():
            self._entries[queue] = (mode, now)

    def clear(self) -> None:
        """Drop every entry so the next dispatch re-resolves."""
        self._entries.clear()


def invalidate_queue_mode_caches() -> int:
    """Clear every live queue-mode cache in this process.

    Called wherever this process writes the ``queues`` table (the
    queue-ops seam): the process that changed a mode must not keep
    serving it stale for a TTL. Returns the number of caches cleared —
    zero in a CLI-only process with no live backends, which is the
    common case (operators usually run queue ops out of process).
    """
    cleared = 0
    for cache in list(_live_queue_mode_caches):
        cache.clear()
        cleared += 1
    return cleared


async def _dispatch_batch(
    dispatcher_pool: "asyncpg.Pool",
    sql: SqlTemplates,
    dispatch_oversample: int,
    acquire_timeout: float,
    schema: str,
    worker_id: UUID,
    queues: list[str],
    limit: int,
    lock_lease: timedelta,
    *,
    queue_mode_cache: QueueModeCache | None = None,
) -> list[JobRow]:
    """Dispatch up to *limit* pending jobs from *queues*.

    When *queues* mixes ``strict_fifo`` and ``round_robin`` queues in a
    single call, the round-robin CTE variant is used for the whole batch
    (round-robin is a superset behaviour — strict_fifo queues still dispatch
    in priority/scheduled_at order, just with an extra no-op fairness_rank
    partition). This is silent by design elsewhere, so a debug log is
    emitted here to make the mode selection observable when queues are
    mixed unintentionally.

    *queue_mode_cache* is the backend's worker-side mode cache: a hit
    removes the resolve statement from this transaction entirely. The
    miss path re-resolves through the query and refills the cache.
    ``None`` keeps the pre-cache contract — resolve on every call — for
    standalone callers that want fresh resolution.
    """
    event_sql = sql.insert_events_batch
    async with dispatcher_pool.acquire(timeout=acquire_timeout) as conn:
        queue_modes = (
            queue_mode_cache.resolved_modes(queues) if queue_mode_cache is not None else None
        )
        async with conn.transaction():
            if queue_modes is None:
                modes_by_queue = await _resolve_queue_modes_by_queue(conn, queues, schema)
                if queue_mode_cache is not None:
                    queue_mode_cache.store(modes_by_queue)
                # An empty queue list resolves to the strict variant (the
                # resolver's own empty-list contract); every non-empty
                # list yields at least one entry.
                queue_modes = set(modes_by_queue.values()) or {"strict_fifo"}
            if len(queue_modes) > 1:
                logger.debug(
                    "dispatch-mixed-queue-modes",
                    queues=queues,
                    modes=sorted(queue_modes),
                    selected_sql="round_robin",
                )
            sql_stmt = (
                sql.dispatch_round_robin
                if "round_robin" in queue_modes
                else sql.dispatch_strict_fifo
            )
            records = await dispatch_batch_helper(
                conn,
                sql=sql_stmt,
                queues=queues,
                limit_n=limit,
                worker_id=worker_id,
                lock_lease=lock_lease,
                oversample=dispatch_oversample,
            )
            if records:
                # One statement, not one per job. This runs inside the
                # transaction still holding the dispatch row locks, so each
                # extra round trip is lock hold time; every row here shares
                # `kind` and `detail`, so only the ids vary.
                await conn.execute(
                    event_sql,
                    [rec["id"] for rec in records],
                    "state_change",
                    jsonb_param(
                        {
                            "from_state": "pending",
                            "to_state": "running",
                            "worker_id": str(worker_id),
                        }
                    ),
                )
    return [_job_row_from_record(rec) for rec in records]


async def _resolve_queue_modes_by_queue(
    conn: ConnLike,
    queues: list[str],
    schema: str,
) -> dict[str, str]:
    """Per-queue modes for exactly *queues*, unknown queues defaulting to
    ``strict_fifo`` — the shared query core for the set-returning public
    resolver and the cache fill.

    *schema* is re-validated here rather than trusted from the caller:
    it is interpolated into SQL (asyncpg cannot bind identifiers), and
    the public ``PostgresBackend.resolve_queue_modes`` static method
    accepts an arbitrary schema and a caller-supplied connection, so
    construction-time validation on the backend instance does not cover
    every reach of this module (architecture.md §Key Invariants 4).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    if not queues:
        return {}
    rows = await conn.fetch(
        f'SELECT name, mode FROM "{schema}".queues WHERE name = ANY($1)',  # Why: schema re-validated against _IDENT_RE immediately above; asyncpg cannot bind identifiers as parameters.
        queues,
    )
    modes_by_queue: dict[str, str] = {r["name"]: r["mode"] for r in rows}
    return {q: modes_by_queue.get(q, "strict_fifo") for q in queues}


async def _resolve_queue_modes(
    conn: ConnLike,
    queues: list[str],
    schema: str,
) -> set[str]:
    """Return the set of distinct modes for *queues* from the queues table.

    Queues not present in the table default to ``strict_fifo``. Returns
    ``{"strict_fifo"}`` when all queues are strict FIFO, ``{"round_robin"}``
    when all are round-robin, or a mixed set. The caller selects the
    round-robin SQL variant when ``"round_robin"`` appears in the set.

    *schema* is re-validated here rather than trusted from the caller:
    this is also reachable as the public ``PostgresBackend.resolve_queue_modes``
    static method, which takes an arbitrary schema and a caller-supplied
    connection, so construction-time validation on the backend instance
    does not cover it (architecture.md §Key Invariants 4).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    if not queues:
        return {"strict_fifo"}
    modes_by_queue = await _resolve_queue_modes_by_queue(conn, queues, schema)
    return set(modes_by_queue.values())

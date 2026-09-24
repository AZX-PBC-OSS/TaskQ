"""Dispatch operations for PostgresBackend.

``dispatch_batch`` and its queue-mode resolver live here as module-level
functions.  :class:`~taskq.backend.postgres.PostgresBackend` methods
are thin wrappers that delegate.

The worker-side :class:`QueueModeCache` also lives here: the resolver it
caches is defined one screen above it, and the queue-ops seam
(:mod:`taskq.worker.queue_ops`) reaches it through
:func:`invalidate_queue_mode_caches` rather than holding a backend
reference, worker → backend is the correct layer direction.
"""

import asyncio
import sys
import time
import weakref
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

import asyncpg

from taskq._json import sanitize_nul_str
from taskq.backend._dispatch_sql import (
    dispatch_batch as dispatch_batch_helper,
)
from taskq.backend._protocol import ConnLike, JobId, JobRow
from taskq.backend._records import (
    _job_row_from_record,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)
from taskq.exceptions import CorruptJobDataError
from taskq.obs import (
    get_logger,
    record_corrupt_dispatch_row,
    record_dispatch_duration,
    record_dispatch_failure,
    record_pool_acquire_duration,
)
from taskq.obs._redact_exc import safe_exception_message

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
the per-dispatch-round resolve statement disappears, a notify-driven
fleet dispatches many times per second, so the round trip it removes is
paid far more often than the staleness it can cause. A stale mode is a
bounded fairness degradation, never a correctness one: the strict-FIFO
variant is the round-robin CTE with no-op fairness ranks, so a batch
dispatched under a stale strict mode still admits and orders by
priority/scheduled_at, only cohort interleaving waits out the TTL.
5 s matches the notify fallback poll cadence (notify_poll_interval), the
system's existing bound for how stale a worker's view of the database
may be before it re-reads on its own.
"""

_live_queue_mode_caches: "weakref.WeakSet[QueueModeCache]" = weakref.WeakSet()
"""Every cache a live backend instance owns, so the queue-ops seam can
clear them all without holding backend references. Weak: a backend's
cache must not outlive (or keep alive) the backend that owns it."""

_MAX_DISPATCH_WINDOW_EXPANSIONS: Final[int] = 3
"""How many times one dispatch round may widen its candidate window after
coming back empty.

Each expansion doubles the per-cohort candidate window
(``residual * oversample * 2**expansions``), so three expansions absorb a
transient lock-out by up to ``oversample * 8`` concurrent dispatchers on
one (actor, queue), 16 at the default oversample of 2, before the
round reports empty and defers to the next tick. The steady-state sizing
rule lives on ``WorkerSettings.dispatch_oversample``: an oversample at
or above the number of dispatchers polling the same (actor, queue) keeps
the common case expansion-free. The claim's own per-round bounds are
untouched, every re-run still admits at most ``limit_n`` rows, and each
candidate probe stays an ORDER BY + LIMIT index read whose cost is
independent of backlog depth, so expansion multiplies the round's
constant factor, never its depth coupling.
"""


class QueueModeCache:
    """Per-backend-instance TTL cache of resolved queue modes.

    Owned by one :class:`~taskq.backend.postgres.PostgresBackend`, the
    backend the worker's single dispatch loop dispatches through, so
    one worker's mode view never couples to another worker sharing the
    process (two workers, two schemas, two backends).

    Concurrency: the dispatch loop is single-per-worker, and no method
    here awaits, every mutation is one event-loop step, so entries are
    never observed torn and no lock is needed. A backend shared by
    concurrent dispatch callers on one loop can only race two misses
    into duplicate resolves; both stores write the same freshly read
    data, so last-write-wins is benign.

    Negative caching: a queue with no ``queues`` row is cached as
    ``strict_fifo`` (the resolver's fallback) with the same TTL, an
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
        entry, else ``None``, the caller resolves the whole list through
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
    serving it stale for a TTL. Returns the number of caches cleared ,
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
    (round-robin is a superset behaviour, strict_fifo queues still dispatch
    in priority/scheduled_at order, just with an extra no-op fairness_rank
    partition). This is silent by design elsewhere, so a debug log is
    emitted here to make the mode selection observable when queues are
    mixed unintentionally.

    *queue_mode_cache* is the backend's worker-side mode cache: a hit
    removes the resolve statement from this round entirely. The
    miss path re-resolves through the query and refills the cache.
    ``None`` keeps the pre-cache contract, resolve on every call, for
    standalone callers that want fresh resolution.

    A claim deliberately writes NO ``job_events`` row. pending→running is
    the dispatcher's bookkeeping, not an outcome transition, and a claim
    is the one act every admission-denial cycle repeats, under the 429
    denial contract a denied job is claimed and rescheduled until capacity
    frees or its deadline expires, so a row per claim is precisely the
    unbounded-growth vector the aggregated denial counters on the job row
    (``snooze_count`` / ``rate_limit_blocked_count``) replaced. The
    transitions of record are the terminal writes and the sweep/cancel
    audit entries; the claim itself writes no row, so the jobs table cannot
    grow per claim. The claim's observability rides the ``kind='dispatch'``
    log line and OTEL span in ``_dispatch_sql.dispatch_batch``.

    One claimed row can still fail to DECODE (``_decode_claimed_rows``):
    a row whose jsonb columns hold garbage is terminally failed in the
    round with ``error_class='CorruptJobDataError'`` and the round's
    healthy rows still dispatch. That write is the exception to the
    no-job_events-rows rule on this path, and it is the row's terminal
    failure, not a claim record.
    """
    queue_attr = queues[0] if queues else ""
    # Autocommit, deliberately: the claim is one atomic UPDATE … RETURNING
    # whose row locks end with the statement, and nothing else in the round
    # needs a shared snapshot, the mode resolve and the claimable probe are
    # read-only, and an empty round holds no locks between iterations.
    # asyncpg sends BEGIN and COMMIT as their own round trips, so a
    # transaction here only tripled the cost of every claim; the claim is
    # one statement, so autocommit already gives it all the atomicity it
    # needs.
    # The pool acquire is the round's FIRST stage, and its wait (the part
    # that can raise) happens on the context manager's __aenter__, so it is
    # acquired explicitly and released in the finally below. The wait is a
    # different quantity from the SQL latency the sibling stages feed
    # taskq.dispatch.duration with, so it gets its own histogram; the
    # failure counter is round-scoped and records here as it does for the
    # resolve, the probe, and the claim.
    acquire_started = time.monotonic()
    pool_ctx = dispatcher_pool.acquire(timeout=acquire_timeout)
    try:
        conn = await pool_ctx.__aenter__()
    except asyncio.CancelledError:
        record_pool_acquire_duration(queue_attr, time.monotonic() - acquire_started)
        raise
    except Exception:
        record_pool_acquire_duration(queue_attr, time.monotonic() - acquire_started)
        record_dispatch_failure(queue_attr)
        raise
    # The wait ended in a connection: record it here too. A completed
    # wait is still a wait: the pool-exhausted pod whose multi-second
    # waits eventually SUCCEED (the case the histogram exists to make
    # visible) runs its SQL only after the wait, and only this record
    # separates the two quantities in the metric stream.
    record_pool_acquire_duration(queue_attr, time.monotonic() - acquire_started)
    try:
        try:
            queue_modes = (
                queue_mode_cache.resolved_modes(queues) if queue_mode_cache is not None else None
            )
            if queue_modes is None:
                # Mode resolution runs before the dispatch CTE is issued, so
                # a failure here never reaches the dispatch helper's own
                # telemetry. Recording the round here keeps the whole
                # failure class visible: a producer whose every round dies
                # resolving modes is otherwise silent on every metric, and
                # reads exactly like a pod polling an idle queue.
                resolve_started = time.monotonic()
                try:
                    modes_by_queue = await _resolve_queue_modes_by_queue(conn, queues, schema)
                except Exception:
                    record_dispatch_duration(queue_attr, time.monotonic() - resolve_started)
                    record_dispatch_failure(queue_attr)
                    raise
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
            oversample = dispatch_oversample
            expansions = 0
            while True:
                records = await dispatch_batch_helper(
                    conn,
                    sql=sql_stmt,
                    queues=queues,
                    limit_n=limit,
                    worker_id=worker_id,
                    lock_lease=lock_lease,
                    oversample=oversample,
                )
                if records or expansions >= _MAX_DISPATCH_WINDOW_EXPANSIONS:
                    break
                # An empty round means one of two things: nothing
                # claimable remains, or every row of the candidate
                # window is row-locked by peers, the window is
                # deliberately bounded (residual x oversample per cohort
                # probe) and SKIP LOCKED slides only within it, so more
                # than oversample dispatchers on one (actor, queue) can
                # lock the whole window and starve the rest while deeper
                # rows sit unlocked. The probe arbitrates before a wider
                # re-run is paid for: no pending routable rows (the idle
                # case, by far the commonest empty round) costs one
                # LIMIT-1 probe and ends the round. A round that returns
                # empty never holds row locks, zero admissions means
                # nothing passed the lock stage, so re-executing with a
                # doubled window starts lock-clean, and the expansion
                # bound keeps a permanently saturated round from
                # re-running without limit.
                probe_started = time.monotonic()
                try:
                    probe_rows = await conn.fetch(sql.dispatch_claimable_probe, queues)
                except Exception:
                    record_dispatch_duration(queue_attr, time.monotonic() - probe_started)
                    record_dispatch_failure(queue_attr)
                    raise
                if not probe_rows:
                    break
                expansions += 1
                oversample = dispatch_oversample * (2**expansions)
                logger.debug(
                    "dispatch-window-expansion",
                    queues=queues,
                    expansion=expansions,
                    oversample=oversample,
                )
            # The claimed records are decoded to JobRows while the round's
            # connection is still checked out: a poisoned row's terminal
            # fail-write (see _decode_claimed_rows) runs on THIS
            # connection. Decoding after the release made every fail-write
            # raise InterfaceError ("connection has been released back to
            # the pool") and degrade to the retry-next-round warning, the
            # poison row then loops through claim and reclaim forever -
            # the real-PG pin caught exactly that.
            rows = await _decode_claimed_rows(conn, sql, records, worker_id, queue_attr)
            # Claims deliberately write NO job_events rows (see this
            # function's docstring above): the dispatch log line and OTEL
            # span carry the observability.
        except asyncpg.exceptions.InternalClientError:
            # A server-side abort (a 1ms statement_timeout is the sharpest
            # case) can land in the driver's own protocol handling instead
            # of the statement's: the connection's state machine is then
            # mid-operation and every later acquire on it wedges the pool
            # (observed as "cannot switch to state 12; another operation
            # is in progress" repeating every round until the worker
            # stalls). This round is autocommit, so asyncpg's release
            # taint logic never discards the connection on our exit:
            # terminate it explicitly; the pool's release drops a closed
            # connection and grows a replacement on the next acquire. The
            # error itself propagates: the producer's transient/loud
            # classification is unchanged.
            conn.terminate()
            raise
    finally:
        # Release the checked-out connection on every body exit, normal or
        # exceptional. The in-flight exception info is forwarded so
        # __aexit__ sees exactly what the async-with form would have
        # passed it.
        await pool_ctx.__aexit__(*sys.exc_info())
    return rows


async def _decode_claimed_rows(
    conn: ConnLike,
    sql: SqlTemplates,
    records: list[Any],
    worker_id: UUID,
    queue_attr: str,
) -> list[JobRow]:
    """Convert the claimed records, failing a poisoned row terminally.

    The decode boundary sits AFTER the claim statement has committed (the
    claim is one autocommit UPDATE...RETURNING), so a row whose jsonb
    columns decode to garbage has ALREADY cost its claim. Letting the
    decode exception escape here makes the whole round the producer loop's
    problem: the round raises ``JSONDecodeError`` unclassified, the
    unexpected-failure backstop counts it (and at its consecutive cap
    kills the worker), and the poisoned row itself is stuck claimed until
    the lease sweep reclaims it into the identical failure, a poison loop
    with no terminal state.

    So each row is decoded alone. A row that refuses to decode is failed
    TERMINALLY on the same connection through the standard fused
    ``mark_failed`` statement, ``error_class='CorruptJobDataError'``, the
    same attempt/claim_epoch fence every other terminal write uses, a
    ``job_attempts`` row and a ``state_change`` event included, so the
    failure is an honest job outcome a `job_events` reader sees, not a
    worker crash. The fail-write failing (a dead connection) degrades to
    a logged warning: the row stays claimed, the lease sweep reclaims it,
    and the next round's decode retries the write. Either way the round's
    remaining healthy rows dispatch, and the loop ticks on.
    """
    rows: list[JobRow] = []
    for rec in records:
        try:
            rows.append(_job_row_from_record(rec))
        except CorruptJobDataError as exc:
            await _fail_corrupt_claimed_row(conn, sql, rec, exc, worker_id, queue_attr)
    return rows


async def _fail_corrupt_claimed_row(
    conn: ConnLike,
    sql: SqlTemplates,
    rec: Any,
    exc: CorruptJobDataError,
    worker_id: UUID,
    queue_attr: str,
) -> None:
    """Write one corrupt claimed row's terminal failure, best-effort fenced.

    ``mark_failed``'s fence binds the claimed row's own attempt and
    claim_epoch from the record, so the write can only land on the row
    this round actually claimed. A no-op fence (the row moved underneath
    us between claim and decode, a cancel or sweep won) is fine: the row
    has an owner and its owner owns the outcome.
    """
    record_corrupt_dispatch_row(queue_attr, exc.column or "unknown")
    job_id: JobId | None = None
    try:
        job_id = rec["id"]
        matched = await conn.fetchrow(
            sql.mark_failed,
            job_id,
            worker_id,
            "CorruptJobDataError",
            sanitize_nul_str(safe_exception_message(exc)),
            None,  # error_traceback: a decode defect has none
            rec["progress_seq"],
            None,  # progress_state: keep whatever the row already holds
            rec["attempt"],
            rec["claim_epoch"],
        )
    except Exception as fail_exc:
        # The fail-write could not land (dead connection is the realistic
        # case). The row stays claimed; the lease sweep reclaims it and
        # the next round's decode retries this write. Swallowed, not
        # silenced: the warning names the row and both errors.
        logger.warning(
            "dispatch-corrupt-row-fail-write-failed",
            kind="dispatch_corrupt_row_fail_write_failed",
            job_id=str(job_id),
            column=exc.column,
            error=str(exc),
            fail_error_class=type(fail_exc).__name__,
            fail_error=str(fail_exc),
            queue=queue_attr,
        )
        return
    logger.warning(
        "dispatch-corrupt-row-failed",
        kind="dispatch_corrupt_row_failed",
        job_id=str(job_id),
        column=exc.column,
        error=str(exc),
        error_class="CorruptJobDataError",
        fenced=matched is not None,
        queue=queue_attr,
    )


async def _resolve_queue_modes_by_queue(
    conn: ConnLike,
    queues: list[str],
    schema: str,
) -> dict[str, str]:
    """Per-queue modes for exactly *queues*, unknown queues defaulting to
    ``strict_fifo``, the shared query core for the set-returning public
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

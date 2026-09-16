"""Cron scheduling types, decorator, and helpers.

Public surface: :class:`CronScheduleSpec`, :class:`ScheduleRecord`,
:class:`ScheduleHandle`, :func:`cron` decorator, and helper functions
``compute_next_fire_after`` and ``resolve_payload`` that are shared by
downstream modules (schedule CRUD, cron loop, admin ops).
"""

import asyncio
import importlib
import inspect
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from taskq._json import loads
from taskq.backend._protocol import (
    DST_STRATEGIES,
    Backend,
    DstStrategy,
    IdentityKey,
    ScheduleRecord,
    ScheduleUpdateArgs,
)
from taskq.obs import get_logger

__all__ = [
    "DST_STRATEGIES",
    "CronScheduleSpec",
    "DstStrategy",
    "ScheduleHandle",
    "ScheduleRecord",
    "compute_next_fire_after",
    "cron",
    "resolve_payload",
]

_log = get_logger(__name__)

_factory_cache: dict[str, Callable[[], Any]] = {}
# Why: factory return type is erased here; resolve_payload re-types the result


def _resolve_factory(dotted_path: str) -> Callable[[], Any]:
    # pyright: ignore[reportReturnType]  # Why: factory return is erased; re-typed in resolve_payload
    """Resolve a dotted path string to a callable.

    Uses ``importlib.import_module`` + ``getattr``. Results are cached in
    ``_factory_cache``.  Raises ``ImportError`` or ``AttributeError`` on
    failure — the caller increments ``consecutive_failures`` and may
    auto-disable the schedule.
    """
    if dotted_path in _factory_cache:
        return _factory_cache[dotted_path]
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ImportError(f"Invalid dotted path: {dotted_path!r}")
    module = importlib.import_module(module_path)
    factory = getattr(module, attr)
    _factory_cache[dotted_path] = factory
    return factory


_FACTORY_POOL_SIZE: Final = 4
"""Worker cap on the payload-factory executor.

The per-factory deadline bounds how long a tick WAITS, not how long a
sync factory runs: a cut-off call stays parked on its thread until the
factory itself returns, which a truly hung one never does.  Cron ticks
forever, so a permanently hung schedule strands one thread per fire.
A private, capped pool turns that unbounded leak into a bounded one and
keeps it off the loop's default executor, where sync actor bodies run —
sharing that pool would let one stuck schedule consume the worker's own
execution capacity until unrelated actors have no thread left, and would
let a busy fleet of sync actors starve an instantaneous factory past its
deadline while the tick holds the cron advisory lock.

The cap alone is not the whole defence: a stdlib pool cannot recall a
parked thread, so enough concurrently-hung schedules (or one hung
schedule whose ``cron_auto_disable_threshold`` an operator raised past
the pool size) strand every worker and would starve healthy factories
for the process's life. The submit path therefore retires a pool whose
every thread is stranded on work its waiter already abandoned — see
:func:`_payload_factory_pool` — which is also why no upper-bound
validation on the threshold is needed: saturation self-heals regardless
of how it was reached.
"""


class _FactoryCall:
    """Lifecycle flags for one submitted factory call, shared between the
    submitting loop and the pool thread (always under ``_factory_pool_lock``).

    ``on_thread`` is true while a pool thread is inside the call.
    ``waiter_gone`` flips when the awaiting side stops waiting (deadline
    cut, caller cancelled, factory raised) — a call that is both
    ``on_thread`` and ``waiter_gone`` is pool residue: a parked thread
    nobody will ever collect.
    """

    __slots__ = ("abandoned", "on_thread", "waiter_gone")

    def __init__(self) -> None:
        self.on_thread = False
        self.waiter_gone = False
        self.abandoned = False


class _FactoryPoolState:
    """One generation of the payload-factory executor plus its residue count.

    ``abandoned`` counts calls parked on a thread whose waiter has already
    given up. When it reaches :data:`_FACTORY_POOL_SIZE`, every thread the
    pool will ever have is stranded on work nobody waits for: a new call
    submitted to this pool would queue behind the parked ones forever.
    """

    __slots__ = ("abandoned", "executor", "occupied")

    def __init__(self) -> None:
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=_FACTORY_POOL_SIZE,
            thread_name_prefix="taskq-cron-factory",
        )
        self.occupied = 0
        self.abandoned = 0


_factory_pool: _FactoryPoolState | None = None
_factory_pool_lock = threading.Lock()


def _run_factory_call(
    pool: _FactoryPoolState,
    call: _FactoryCall,
    factory: Callable[[], Any],
) -> Any:
    """Run *factory* on a pool thread, maintaining the residue accounting.

    Runs inside the pool thread, so ``on_thread`` going true is the proof
    the call holds a thread; a call that starts only after its waiter gave
    up (queued behind parked threads, then released into one) is residue
    from its first instruction and is counted then.
    """
    with _factory_pool_lock:
        pool.occupied += 1
        call.on_thread = True
        if call.waiter_gone and not call.abandoned:
            call.abandoned = True
            pool.abandoned += 1
    try:
        return factory()
    finally:
        with _factory_pool_lock:
            call.on_thread = False
            pool.occupied -= 1
            if call.abandoned:
                pool.abandoned -= 1


def _payload_factory_pool() -> _FactoryPoolState:
    """The live payload-factory pool, retiring a fully-stranded one first.

    Lazy so a process that registers no factory-backed schedule — the
    common case — never pays for the threads.

    Retirement fires when every thread of the current pool is stranded on
    a call its waiter abandoned: that pool can never serve new work, so
    new work gets a fresh pool instead of queueing behind residue for the
    rest of the process's life. The retired pool is shut down WITHOUT
    waiting — waiting is exactly what cannot be done, its threads are
    parked in unkillable user code; they finish (and the pool drains its
    queue) whenever the factories return. Queued calls are deliberately
    NOT cancelled: their waiters hold their own per-factory deadlines and
    time out on them, so retirement injects no new exception shape into a
    tick.
    """
    global _factory_pool
    retired: _FactoryPoolState | None = None
    with _factory_pool_lock:
        if _factory_pool is None:
            _factory_pool = _FactoryPoolState()
        elif _factory_pool.abandoned >= _FACTORY_POOL_SIZE:
            retired = _factory_pool
            _factory_pool = _FactoryPoolState()
        state = _factory_pool
    if retired is not None:
        # Outside the lock: the lock guards bookkeeping only, and even a
        # non-blocking shutdown is no work to hold it across.
        retired.executor.shutdown(wait=False)
        _log.warning(
            "cron-factory-pool-saturated-retired",
            kind="cron_factory_pool_saturated_retired",
            pool_size=_FACTORY_POOL_SIZE,
            occupied=retired.occupied,
            abandoned=retired.abandoned,
        )
    return state


_FACTORY_TIMEOUT_S: Final = 5.0
"""The per-factory deadline ``resolve_payload`` enforces on both phases of
a factory: the call itself (run off the event loop) and, for a factory that
returns a coroutine, the await of that coroutine back on the loop."""


async def _await_factory_bounded(
    payload_factory: str,
    work: Awaitable[object],
    timeout_s: float | None = None,
) -> object:
    """Await one phase of a payload factory under the factory deadline.

    *timeout_s* overrides :data:`_FACTORY_TIMEOUT_S` for this call — the
    cron tick passes its ``cron_payload_factory_timeout`` setting already
    clamped to what is left of its whole-tick budget, so a per-factory
    budget can never silently exceed it (the leader's whole-tick
    ``asyncio.timeout`` would then cancel mid-factory, losing the named
    per-schedule failure this path exists to record).

    A timeout raises ``TimeoutError`` naming the factory's dotted path: the
    schedule's error text is the only place an operator sees WHICH factory
    hung.  A factory that fails with its own ``TimeoutError`` reason keeps
    that reason — "pool exhausted" is the diagnosis, "hung for 5s" would be
    a lie.  The type stays ``TimeoutError`` so every classification and
    catch site downstream is unchanged.
    """
    effective_timeout = _FACTORY_TIMEOUT_S if timeout_s is None else timeout_s
    try:
        return await asyncio.wait_for(work, timeout=effective_timeout)
    except TimeoutError as exc:
        reason = str(exc)
        if reason:
            raise TimeoutError(f"cron payload factory {payload_factory!r}: {reason}") from exc
        raise TimeoutError(
            f"cron payload factory {payload_factory!r} timed out after {effective_timeout:g}s"
        ) from exc


async def resolve_payload(
    payload_factory: str | None,
    raw_metadata: object,
    *,
    timeout_s: float | None = None,
) -> dict[str, object]:
    """Resolve payload from a factory dotted path or static metadata.

    If *payload_factory* is set, resolves the dotted path via
    :func:`_resolve_factory` and calls it.  A coroutine FUNCTION is called
    directly on the loop: the call only constructs a coroutine object and
    cannot block, so routing it through a thread buys nothing.  Any other
    callable may block, so the call runs on cron's own bounded executor
    (:func:`_payload_factory_pool`) — never the loop's default pool,
    which sync actor bodies also check out of — bounded by the per-factory
    ``wait_for``, so a sync factory that blocks is cut at the deadline
    instead of freezing every timer on the loop (this bound and the
    caller's whole-tick deadline alike) while the tick holds the cron
    advisory lock.  A factory that RETURNS a coroutine keeps loop
    affinity: the coroutine is awaited back on the loop under the same
    ``wait_for``.  ``BaseModel`` results are converted via
    ``.model_dump()``; ``dict`` results are returned as-is.  Raises
    ``TypeError`` for unexpected return types.

    Thread-safety contract: a sync payload factory runs on a worker
    thread — it must be thread-safe and must not require the event loop;
    a coroutine factory keeps loop affinity.

    If no *payload_factory*, extracts ``static_payload`` from
    *raw_metadata*.  Returns ``{}`` if neither is set.

    Raises:
        TypeError: factory returned an unexpected type (not dict or
            BaseModel).
        TimeoutError: the factory call, or the coroutine it returned,
            outlived the 5 s per-factory deadline; the message names the
            factory.
        ImportError / AttributeError: propagated from :func:`_resolve_factory`.
    """
    if payload_factory is not None:
        factory = _resolve_factory(payload_factory)
        result: object
        if inspect.iscoroutinefunction(factory):
            # Calling a coroutine function builds a coroutine object and
            # runs no user code, so the loop is never at risk and the
            # call needs no thread at all.
            result = factory()
        else:
            # Called inline, a sync factory that blocks freezes every
            # timer on the loop — this deadline and the caller's
            # whole-tick asyncio.timeout alike — so the tick would hold
            # the cron advisory lock for as long as the factory blocks.
            loop = asyncio.get_running_loop()
            pool = _payload_factory_pool()
            call = _FactoryCall()
            try:
                result = await _await_factory_bounded(
                    payload_factory,
                    loop.run_in_executor(pool.executor, _run_factory_call, pool, call, factory),
                    timeout_s=timeout_s,
                )
            except (Exception, asyncio.CancelledError):
                # The waiter is leaving (the per-factory deadline cut the
                # call, the caller's own deadline cancelled it, or the
                # factory raised). If the call is parked on its thread —
                # a factory that never returns — that thread is now pool
                # residue the stdlib cannot recall: count it so the submit
                # path recognizes a fully-stranded pool and retires it.
                # ``waiter_gone`` also arms the start path in
                # ``_run_factory_call`` for the queued-then-started race:
                # a call that only reaches a thread after its waiter left
                # is residue from its first instruction.
                with _factory_pool_lock:
                    call.waiter_gone = True
                    if call.on_thread and not call.abandoned:
                        call.abandoned = True
                        pool.abandoned += 1
                raise
        if inspect.iscoroutine(result):
            # A coroutine-returning factory keeps loop affinity: its body
            # runs here, on the loop, under the same deadline.
            result = await _await_factory_bounded(payload_factory, result, timeout_s=timeout_s)
        if isinstance(result, BaseModel):
            return result.model_dump()
        if isinstance(result, dict):
            return cast(dict[str, object], result)
        raise TypeError(
            f"cron factory {payload_factory!r} returned "
            f"{type(result).__name__}; expected dict or BaseModel"
        )
    if isinstance(raw_metadata, dict):
        metadata = cast(dict[str, object], raw_metadata)
    elif raw_metadata:
        metadata = cast(dict[str, object], loads(str(raw_metadata)))
    else:
        metadata = {}
    static = metadata.get("static_payload")
    if isinstance(static, dict):
        return cast(dict[str, object], static)
    return {}


def compute_next_fire_after(
    cron_expr: str,
    timezone_name: str,
    after: datetime,
    dst_strategy: DstStrategy = "skip",
) -> list[datetime]:
    """Compute the next fire time(s) for *cron_expr* after *after*.

    Uses croniter. *after* should be timezone-aware; the
    result preserves the schedule's timezone.

    DST handling:
      - **Gaps** (spring-forward): croniter may return a local time that
        does not exist (e.g. 02:30 on a day where 02:00→03:00). The gap
        is detected by converting the result to UTC and back; if the
        round-trip shifts the wall-clock time, the local time was in a
        gap. For ``skip`` and ``firstof``, the gap time is advanced to
        the next valid cron match after the gap. For ``allof``, gap times
        are skipped (same as ``skip``).
      - **Overlaps** (fall-back): croniter may return a local time that
        occurs twice (e.g. 01:30 on a day where 02:00→01:00). The
        overlap is detected by computing both the earlier and later UTC
        interpretations and checking that they differ. For ``skip`` and
        ``firstof``, the earlier (first) occurrence is used. For
        ``allof``, both occurrences are returned so the caller can
        enqueue a job for each. A fold-0 seed inside a repeated range is
        special under ``allof``: once the range's fold-0 matches are
        spent, the next owed fire is the fold-1 pass's first match — a
        wall time at or before the seed's that the naive walk cannot
        see — so it is computed directly. Under ``skip`` and
        ``firstof`` the repeated range counts as one slot at the earlier
        occurrence, so the walk's answer stands. A fold-1 seed inside
        the range is the mirror: every in-range wall match's earlier
        occurrence is spent, so under ``allof`` the next owed fire is
        the next in-range match's fold-1 occurrence, and under ``skip``
        and ``firstof`` the answer is the first match beyond the range
        — the naive walk alone would answer an instant at or before the
        seed.

    Returns a list of 1 or 2 datetimes. A single-element list is the
    normal case; a two-element list is returned only when
    ``dst_strategy='allof'`` and the fire time falls in a DST overlap.
    """
    # Lazy import: croniter (+ dateutil) costs ~16ms at import time and is
    # only needed on the cron-tick path, not for ``import taskq``.
    from croniter import croniter

    tz = ZoneInfo(timezone_name)
    after_local = after.astimezone(tz)

    # A repeated range plays twice: its wall matches fire once as fold-0
    # instants, then the whole range re-plays as fold-1 instants.
    # croniter's walk sees only wall time — it finds every match
    # strictly after the seed's wall, but it cannot see the fold-1 pass
    # at all: those matches' walls sit at or before the seed's wall.
    # Only ``allof`` owes that pass — ``skip`` and ``firstof`` fire a
    # repeated range once, at the earlier occurrence, which the seed's
    # range has already given — so for those strategies the walk's
    # answer stands.  This is reachable whenever a leader outage or a
    # manual edit leaves ``next_fire_at`` inside the range: without
    # this branch the fold-1 pass is silently lost for a year.
    if dst_strategy == "allof":
        fold1_next = _next_fold1_fire(cron_expr, after_local, tz)
        if fold1_next is not None:
            return [fold1_next]

    # A fold-1 seed inside the range is the mirror image: every wall
    # match at or before the seed's wall has spent its earlier
    # occurrence — all of the range's fold-0 instants precede all of
    # its fold-1 instants — and the naive walk cannot express that
    # ordering.  It answers the next wall match's fold-0 interpretation
    # (an instant BEFORE the seed) or, under ``allof``, a pair whose
    # first member precedes the seed — a function named
    # ``compute_next_fire_after`` may never answer at or before its
    # seed.  ``allof`` owes the fold-1 occurrence of the next in-range
    # match; ``skip`` and ``firstof`` owe nothing further in the range
    # (each slot fires once, at the earlier occurrence — all spent), so
    # they advance to the first match beyond it.  A candidate outside
    # the seed's range — a later match, possibly in a later repeated
    # range — is ordered correctly by the ordinary walk below.
    if after_local.fold != 0:
        bounds = repeated_range_bounds(after_local, tz)
        if bounds is not None:
            range_start, range_end = bounds
            cr = croniter(cron_expr, after_local)
            candidate = cr.get_next(datetime)
            if candidate.tzinfo is None:
                candidate = candidate.replace(tzinfo=tz)
            if range_start <= candidate.replace(tzinfo=None) < range_end:
                if dst_strategy == "allof":
                    return [_fold_to_utc(candidate, tz, fold=1)]
                cr = croniter(cron_expr, candidate.replace(tzinfo=None))
                beyond = cr.get_next(datetime)
                while range_start <= beyond < range_end:
                    cr = croniter(cron_expr, beyond)
                    beyond = cr.get_next(datetime)
                return [_check_gap(beyond.replace(tzinfo=tz), tz)]

    cr = croniter(cron_expr, after_local)
    candidate = cr.get_next(datetime)

    if dst_strategy == "skip" and timezone_name == "UTC":
        return [candidate]

    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=tz)

    candidate_utc = candidate.astimezone(UTC)
    candidate_roundtrip = candidate_utc.astimezone(tz)

    is_gap = candidate_roundtrip.replace(second=0, microsecond=0) != candidate.replace(
        second=0, microsecond=0
    )
    if is_gap:
        cr2 = croniter(cron_expr, candidate_roundtrip)
        next_valid = cr2.get_next(datetime)
        if next_valid.tzinfo is None:
            next_valid = next_valid.replace(tzinfo=tz)
        return [_check_gap(next_valid, tz)]

    is_overlap = _is_ambiguous_time(candidate, tz)
    if is_overlap:
        if dst_strategy == "allof":
            earlier_utc = _fold_to_utc(candidate, tz, fold=0)
            later_utc = _fold_to_utc(candidate, tz, fold=1)
            return [earlier_utc, later_utc]
        return [_fold_to_utc(candidate, tz, fold=0)]

    return [candidate]


def repeated_range_bounds(after_local: datetime, tz: ZoneInfo) -> tuple[datetime, datetime] | None:
    """The repeated (fall-back) wall range containing *after_local*'s wall
    time, as naive dated walls ``[start, end)`` — or None when the wall is
    not ambiguous in *tz*.

    The bounds are walked from the wall rather than assumed hour-aligned:
    half-hour repeat zones (Lord Howe repeats 02:00→01:30) answer the
    same way full-hour zones do.
    """
    if not _is_ambiguous_time(after_local, tz):
        return None
    start = after_local.replace(tzinfo=None, second=0, microsecond=0)
    while _is_ambiguous_time((start - timedelta(minutes=1)).replace(tzinfo=tz), tz):
        start = start - timedelta(minutes=1)
    end = start + timedelta(minutes=1)
    while _is_ambiguous_time(end.replace(tzinfo=tz), tz):
        end = end + timedelta(minutes=1)
    return start, end


def _next_fold1_fire(
    cron_expr: str,
    after_local: datetime,
    tz: ZoneInfo,
) -> datetime | None:
    """The next fire the fold-1 (later) pass of a repeated range owes
    from a fold-0 seed inside it, or None when the ordinary walk already
    answers the next owed fire.

    A repeated range plays twice: its wall matches fire once as fold-0
    instants, then the whole range re-plays as fold-1 instants.  The
    naive walk sees only wall time, so it finds every match strictly
    after the seed's wall — the fold-0 pass's remaining matches — but
    it cannot see the fold-1 pass at all: those matches' walls sit at
    or before the seed's wall.  Once the fold-0 pass owes nothing more,
    the next owed fire is the fold-1 pass's FIRST match, and only this
    computation can name it.  That match is the seed's own match's twin
    in the founding case (a single match in the range), the MATCH's
    twin — not the seed's — when the seed sits past it, and the
    range's first match when the seed sits late in the range.

    None (the walk answers) when the seed is not a fold-0 instant of a
    repeated range, when a fold-0 match still remains in the range —
    the walk finds it and its own overlap branch returns its pair — or
    when the expression matches nothing in the range: the seed's own
    match, if any, already fired, and the walk's beyond-range answer
    stands.
    """
    from croniter import croniter

    if after_local.fold != 0:
        return None
    bounds = repeated_range_bounds(after_local, tz)
    if bounds is None:
        return None
    start, end = bounds
    naive = after_local.replace(tzinfo=None)
    nxt = croniter(cron_expr, naive).get_next(datetime)
    if nxt < end:
        return None
    first = croniter(cron_expr, start - timedelta(seconds=1)).get_next(datetime)
    if first >= end:
        return None
    return _fold_to_utc(first.replace(tzinfo=tz), tz, fold=1)


def _is_ambiguous_time(dt: datetime, tz: ZoneInfo) -> bool:
    """Return True if *dt*'s wall-clock time is ambiguous in *tz* (DST overlap)."""
    if dt.tzinfo is not tz:
        dt = dt.astimezone(tz)
    naive = dt.replace(tzinfo=None)
    try:
        dt0 = naive.replace(tzinfo=tz, fold=0)
        dt1 = naive.replace(tzinfo=tz, fold=1)
    except Exception:
        return False
    return dt0.astimezone(UTC) != dt1.astimezone(UTC)


def _fold_to_utc(dt: datetime, tz: ZoneInfo, fold: int) -> datetime:
    """Convert an ambiguous local *dt* to UTC using the given *fold* value."""
    naive = dt.replace(tzinfo=None)
    resolved = naive.replace(tzinfo=tz, fold=fold)
    return resolved.astimezone(tz)


def _check_gap(dt: datetime, tz: ZoneInfo) -> datetime:
    """Verify *dt* is not in a DST gap; if it is, advance one minute and retry."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    dt_utc = dt.astimezone(UTC)
    dt_roundtrip = dt_utc.astimezone(tz)
    if dt_roundtrip.replace(second=0, microsecond=0) != dt.replace(second=0, microsecond=0):
        from datetime import timedelta as _td

        advanced = dt + _td(minutes=1)
        return _check_gap(advanced, tz)
    return dt


@dataclass(frozen=True, slots=True)
class CronScheduleSpec:
    """Immutable specification for a cron schedule row.

    Created by the :func:`cron` decorator or constructed directly for
    ``register_cron()``.  ``payload_factory`` and ``static_payload`` are
    mutually exclusive — setting both raises :class:`ValueError` at
    construction time (via :func:`cron`).

    ``dst_strategy`` controls how DST gaps and overlaps are handled.
    See :data:`DstStrategy` for the semantics of each strategy.
    """

    actor: str
    cron_expr: str
    timezone: str = "UTC"
    dst_strategy: DstStrategy = "skip"
    payload_factory: str | None = None
    static_payload: dict[str, object] | None = None
    name: str = ""
    identity_key: IdentityKey | None = None
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class ScheduleHandle:
    """Immutable handle for a cron schedule, returned by ``JobsClient`` methods.

    The handle fields are a point-in-time snapshot of schedule state.
    Async methods delegate to the ``Backend`` injected at construction time
    (not part of the public ``__init__`` signature) via ``ScheduleUpdateArgs``.
    ``enable()`` passes ``ScheduleUpdateArgs(enabled=True)``; the backend
    resets ``consecutive_failures=0`` and ``last_fire_error=NULL`` when
    ``enabled=True`` is set.
    """

    schedule_id: UUID
    actor: str
    cron_expr: str
    timezone: str
    enabled: bool
    next_fire_at: datetime
    _backend: Backend = field()
    dst_strategy: DstStrategy = "skip"
    name: str = ""
    identity_key: IdentityKey | None = None

    async def disable(self) -> None:
        await self._backend.update_schedule(
            self.schedule_id,
            ScheduleUpdateArgs(enabled=False),
        )

    async def enable(self) -> None:
        await self._backend.update_schedule(
            self.schedule_id,
            ScheduleUpdateArgs(enabled=True),
        )

    async def delete(self) -> None:
        await self._backend.delete_schedule(self.schedule_id)


def cron(
    expression: str,
    actor: str,
    *,
    payload_factory: str | None = None,
    static_payload: dict[str, object] | None = None,
    name: str = "",
    identity_key: IdentityKey | None = None,
    timezone: str = "UTC",
    dst_strategy: DstStrategy = "skip",
    enabled: bool = True,
) -> CronScheduleSpec:
    """Declare a cron schedule and auto-register it.

    Validates *expression* via ``croniter.is_valid()``; raises
    :class:`ValueError` on invalid expressions.  Raises
    :class:`ValueError` if both *payload_factory* and *static_payload*
    are provided.

    The returned :class:`CronScheduleSpec` is registered via
    :func:`~taskq.scheduler.register_cron` at decoration time so
    decorated schedules are auto-discovered at worker startup without
    any explicit ``register_cron()`` call.

    Startup auto-discovery is **create-only, skip-on-conflict**.  Existing
    ``cron_schedules`` rows are never modified by the decorator
    registration pass.  If a ``@cron`` decorator's parameters change
    after the schedule was first registered, the operator must manually
    update or delete and recreate the schedule.

    Args:
        dst_strategy: How to handle DST gaps and overlaps.
            ``skip`` (default) advances past gaps, uses the first
            occurrence in overlaps. ``firstof`` explicitly selects the
            earlier wall-clock time in overlaps. ``allof`` fires at
            both occurrences in overlaps (the caller receives two
            datetimes from ``compute_next_fire_after``).
    """
    # Lazy import (see compute_next_fire_after): validated on first
    # decoration, not at module import.
    from croniter import croniter

    if not croniter.is_valid(expression):
        raise ValueError(f"Invalid cron expression: {expression!r}")
    if payload_factory is not None and static_payload is not None:
        raise ValueError(
            "payload_factory and static_payload are mutually exclusive; "
            "provide one or the other, not both"
        )
    spec = CronScheduleSpec(
        actor=actor,
        cron_expr=expression,
        timezone=timezone,
        dst_strategy=dst_strategy,
        payload_factory=payload_factory,
        static_payload=static_payload,
        name=name,
        identity_key=identity_key,
        enabled=enabled,
    )
    from taskq.scheduler import register_cron

    register_cron(spec)
    return spec

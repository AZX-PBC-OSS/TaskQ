"""Cron scheduling types, decorator, and helpers.

Public surface: :class:`CronScheduleSpec`, :class:`ScheduleRecord`,
:class:`ScheduleHandle`, :func:`cron` decorator, and helper functions
``compute_next_fire_after`` and ``resolve_payload`` that are shared by
downstream modules (schedule CRUD, cron loop, admin ops).
"""

import asyncio
import importlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from croniter import croniter
from pydantic import BaseModel

from taskq._json import loads
from taskq.backend._protocol import (
    Backend,
    DstStrategy,
    IdentityKey,
    ScheduleRecord,
    ScheduleUpdateArgs,
)

__all__ = [
    "CronScheduleSpec",
    "DstStrategy",
    "ScheduleHandle",
    "ScheduleRecord",
    "compute_next_fire_after",
    "cron",
    "resolve_payload",
]

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


async def resolve_payload(
    payload_factory: str | None,
    raw_metadata: object,
) -> dict[str, object]:
    """Resolve payload from a factory dotted path or static metadata.

    If *payload_factory* is set, resolves the dotted path via
    :func:`_resolve_factory` and calls it.  Async factories are awaited
    with ``asyncio.wait_for(result, timeout=5.0)``.  ``BaseModel`` results
    are converted via ``.model_dump()``; ``dict`` results are returned
    as-is.  Raises ``TypeError`` for unexpected return types.

    If no *payload_factory*, extracts ``static_payload`` from
    *raw_metadata*.  Returns ``{}`` if neither is set.

    Raises:
        TypeError: factory returned an unexpected type (not dict or
            BaseModel).
        ImportError / AttributeError: propagated from :func:`_resolve_factory`.
    """
    if payload_factory is not None:
        factory = _resolve_factory(payload_factory)
        result: object = factory()
        if inspect.iscoroutine(result):
            result = await asyncio.wait_for(result, timeout=5.0)
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
        enqueue a job for each.

    Returns a list of 1 or 2 datetimes. A single-element list is the
    normal case; a two-element list is returned only when
    ``dst_strategy='allof'`` and the fire time falls in a DST overlap.
    """
    tz = ZoneInfo(timezone_name)
    after_local = after.astimezone(tz)
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

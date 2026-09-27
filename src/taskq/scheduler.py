"""Cron schedule registry, ``register_cron`` and ``get_registered_crons``.

The registry is a module-level list of :class:`~taskq.cron.CronScheduleSpec`
objects populated at import time by the ``@cron`` decorator (or manually
via :func:`register_cron`).  At worker startup, the bootstrap iterates
the registry and calls ``create_schedule()`` for each spec (cron_loop).

The registry is a plain ``list``, deduplication is the caller's
responsibility.  The DB-layer ``(actor, name)`` UNIQUE constraint
prevents duplicate schedules from persisting.
"""

from datetime import UTC, datetime
from typing import Final

from taskq.cron import CronScheduleSpec, compute_next_fire_after

__all__ = [
    "get_registered_crons",
    "register_cron",
]

_CRON_REGISTRY: list[CronScheduleSpec] = []

_LANDABLE_PROBE_SEED: Final[datetime] = datetime(2026, 1, 1, tzinfo=UTC)
"""The seed the landability probe walks from.  Fixed, not ``now``: the gate's
verdict must be a property of the expression, deterministic across boots and
processes.  Every landable cron expression matches somewhere within one leap
cycle of any seed, so a 2026 seed classifies every shape (Feb-29 forms land
2028; the poison family raises immediately)."""


def register_cron(schedule: CronScheduleSpec) -> None:
    """Add *schedule* to the module-level registry.

    Validates the cron expression at call time.  Raises :class:`ValueError`
    on bad expression or mutually exclusive fields.  Duplicate calls append
    again, deduplication is the caller's responsibility (the DB
    ``(actor, name)`` UNIQUE constraint is the authoritative gate at
    startup time).
    """
    _validate_spec(schedule)
    _CRON_REGISTRY.append(schedule)


def get_registered_crons() -> list[CronScheduleSpec]:
    """Return a snapshot copy of the registry.

    Used at worker startup to call ``create_schedule()`` for each
    registered spec.
    """
    return list(_CRON_REGISTRY)


def _validate_spec(schedule: CronScheduleSpec) -> None:
    """Validate a CronScheduleSpec before registration.

    Raises :class:`ValueError` on invalid cron expression, expressions no
    calendar date can ever satisfy, or mutually exclusive payload fields.
    """
    from croniter import croniter

    if not croniter.is_valid(schedule.cron_expr):
        raise ValueError(f"Invalid cron expression: {schedule.cron_expr!r}")
    _require_landable(schedule.cron_expr)
    if schedule.payload_factory is not None and schedule.static_payload is not None:
        raise ValueError(
            "payload_factory and static_payload are mutually exclusive; "
            "provide one or the other, not both"
        )


def _require_landable(cron_expr: str) -> None:
    """Reject expressions ``croniter.is_valid`` certifies that no calendar
    date can ever satisfy (``0 0 30 2 *``: February 30; ``0 0 31 4 *``:
    April 31).

    Why at registration depth, and why it matters: the worker's startup
    registration pass seeds every spec's first ``next_fire_at`` with
    ``compute_next_fire_after`` OUTSIDE its per-spec except-branch, so a
    poisoned spec that reached the registry would kill the whole pass at
    boot - every spec declared after it unregistered, the worker down,
    with croniter's bare "failed to find next date" as the only clue.
    ``is_valid`` alone cannot see this class (grammar is dateless); the
    walk from :data:`_LANDABLE_PROBE_SEED` is the date-aware half of the
    same gate.  The raise comes from ``compute_next_fire_after`` itself,
    so the message names the expression and the diagnosis.
    """
    compute_next_fire_after(cron_expr, "UTC", _LANDABLE_PROBE_SEED)

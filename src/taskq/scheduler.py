"""Cron schedule registry — ``register_cron`` and ``get_registered_crons``.

The registry is a module-level list of :class:`~taskq.cron.CronScheduleSpec`
objects populated at import time by the ``@cron`` decorator (or manually
via :func:`register_cron`).  At worker startup, the bootstrap iterates
the registry and calls ``create_schedule()`` for each spec (cron_loop).

The registry is a plain ``list`` — deduplication is the caller's
responsibility.  The DB-layer ``(actor, name)`` UNIQUE constraint
prevents duplicate schedules from persisting.
"""

from taskq.cron import CronScheduleSpec

__all__ = [
    "get_registered_crons",
    "register_cron",
]

_CRON_REGISTRY: list[CronScheduleSpec] = []


def register_cron(schedule: CronScheduleSpec) -> None:
    """Add *schedule* to the module-level registry.

    Validates the cron expression at call time.  Raises :class:`ValueError`
    on bad expression or mutually exclusive fields.  Duplicate calls append
    again — deduplication is the caller's responsibility (the DB
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

    Raises :class:`ValueError` on invalid cron expression or mutually
    exclusive payload fields.
    """
    from croniter import croniter

    if not croniter.is_valid(schedule.cron_expr):
        raise ValueError(f"Invalid cron expression: {schedule.cron_expr!r}")
    if schedule.payload_factory is not None and schedule.static_payload is not None:
        raise ValueError(
            "payload_factory and static_payload are mutually exclusive; "
            "provide one or the other, not both"
        )

"""Settings factories for TaskQ integration tests.

The default schema name is deliberately PER-CALL unique
(``tq_<random base62>``, lowercased): a fixed per-xdist-worker name
(``tq_<worker>``) is shared by every caller in the process, which silently
clobbers across test modules for any consumer on a shared-database model —
and every ``*_infra`` helper that consumes these settings is DROP-first, so
two modules sharing one name would drop each other's schema mid-run. Pass an
explicit ``schema_name=`` override when a deterministic name is required.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from taskq._ids import new_base62
from taskq.settings import WorkerSettings

_DEFAULTS: dict[str, str] = {
    "TASKQ_HEARTBEAT_INTERVAL": "0.5",
    "TASKQ_LOCK_LEASE": "2.0",
    # Keeps the lag-lease invariant satisfied for these fast defaults
    # (1.2 + 0.5 < 2.0) and the budget above the 1.0s default check
    # interval: a stalled loop dies before its 2s lease expires, without
    # the detector tripping on its own sampling cadence.
    "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
    "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
    "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
    "TASKQ_TERMINATION_GRACE_PERIOD": "7.0",
    "TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL": "1",
}


def _build_dict(pg_dsn: str, **overrides: str) -> dict[str, str]:
    # Lowercased: PG folds unquoted identifiers to lowercase, so a lowercase
    # name behaves identically whether a consumer quotes it or not.
    data: dict[str, str] = {
        "TASKQ_PG_DSN": pg_dsn,
        "TASKQ_SCHEMA_NAME": f"tq_{new_base62()}".lower(),
        **_DEFAULTS,
    }
    for key, value in overrides.items():
        if not key.startswith("TASKQ_"):
            data[f"TASKQ_{key.upper()}"] = value
        else:
            data[key.upper()] = value
    return data


def make_integration_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    """Construct WorkerSettings with fast intervals for integration tests.

    The schema defaults to a per-call unique ``tq_<token>`` name (see module
    docstring); pass ``schema_name="..."`` to pin it.
    """
    return WorkerSettings.load_from_dict(_build_dict(pg_dsn, **overrides))


def make_integration_settings_dict(pg_dsn: str, **overrides: str) -> dict[str, str]:
    """Return the raw dict passed to WorkerSettings.load_from_dict.

    Same per-call unique schema default as :func:`make_integration_settings`.
    """
    return _build_dict(pg_dsn, **overrides)


_CHAOS_DEFAULTS = {
    "heartbeat_interval": 1.0,
    "lock_lease": 4.0,
    # Kept alongside the shortened lease so the pair stays inside the
    # lag-lease invariant (1.2 + 1.0 < 4.0) even when applied to settings
    # loaded from elsewhere.
    "watchdog_loop_lag_budget": 1.2,
    "cancellation_grace_period": 0.0,
    "cleanup_grace_period": 0.0,
}


@contextmanager
def shorten_chaos_settings(*deps_list: Any) -> Generator[None, None, None]:
    """Context manager: temporarily shorten timing on WorkerDeps for chaos tests.

    Sets heartbeat→1s, lock_lease→4s (retains invariant), and zeroes
    cancellation/cleanup grace.  Settings are restored on exit.
    """
    saved: dict[int, dict[str, Any]] = {}
    for i, deps in enumerate(deps_list):
        saved[i] = {name: getattr(deps.settings, name) for name in _CHAOS_DEFAULTS}
    try:
        for deps in deps_list:
            for name, value in _CHAOS_DEFAULTS.items():
                setattr(deps.settings, name, value)
        yield
    finally:
        for i, deps in enumerate(deps_list):
            for name, value in saved[i].items():
                setattr(deps.settings, name, value)

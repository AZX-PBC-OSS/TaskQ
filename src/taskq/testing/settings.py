from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from taskq.settings import WorkerSettings

if TYPE_CHECKING:
    pass

_default_worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
_DEFAULTS: dict[str, str] = {
    "TASKQ_SCHEMA_NAME": f"tq_{_default_worker}",
    "TASKQ_HEARTBEAT_INTERVAL": "0.5",
    "TASKQ_LOCK_LEASE": "2.0",
    "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
    "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
    "TASKQ_TERMINATION_GRACE_PERIOD": "7.0",
    "TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL": "1",
}


def _build_dict(pg_dsn: str, **overrides: str) -> dict[str, str]:
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn, **_DEFAULTS}
    for key, value in overrides.items():
        if not key.startswith("TASKQ_"):
            data[f"TASKQ_{key.upper()}"] = value
        else:
            data[key.upper()] = value
    return data


def make_integration_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    """Construct WorkerSettings with fast intervals for integration tests."""
    return WorkerSettings.load_from_dict(_build_dict(pg_dsn, **overrides))


def make_integration_settings_dict(pg_dsn: str, **overrides: str) -> dict[str, str]:
    """Return the raw dict passed to WorkerSettings.load_from_dict."""
    return _build_dict(pg_dsn, **overrides)


_CHAOS_DEFAULTS = {
    "heartbeat_interval": 1.0,
    "lock_lease": 4.0,
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

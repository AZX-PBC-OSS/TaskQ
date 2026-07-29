"""Unit tests for WorkerSettings idle drain fields."""

import pytest
from dotenvmodel.exceptions import ConstraintViolationError

from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with sensible defaults.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── Defaults ────────────────────────────────────────────────────────────


def test_idle_settle_window_default() -> None:
    s = _load()
    assert s.idle_settle_window == 2.0


def test_idle_poll_interval_default() -> None:
    s = _load()
    assert s.idle_poll_interval == 1.0


def test_idle_max_runtime_default_none() -> None:
    s = _load()
    assert s.idle_max_runtime is None


# ── Env override via load_from_dict ─────────────────────────────────────


def test_idle_settle_window_env_override() -> None:
    s = _load(TASKQ_IDLE_SETTLE_WINDOW="5.0")
    assert s.idle_settle_window == 5.0


def test_idle_poll_interval_env_override() -> None:
    s = _load(TASKQ_IDLE_POLL_INTERVAL="0.5")
    assert s.idle_poll_interval == 0.5


def test_idle_max_runtime_env_override() -> None:
    s = _load(TASKQ_IDLE_MAX_RUNTIME="300")
    assert s.idle_max_runtime == 300.0


# ── Validation tests (review finding F5) ────────────────────────────────


def test_idle_settle_window_rejects_negative() -> None:
    with pytest.raises(ConstraintViolationError):
        _load(TASKQ_IDLE_SETTLE_WINDOW="-1.0")


def test_idle_poll_interval_rejects_below_minimum() -> None:
    with pytest.raises(ConstraintViolationError):
        _load(TASKQ_IDLE_POLL_INTERVAL="0.05")


def test_idle_max_runtime_rejects_zero() -> None:
    with pytest.raises(ConstraintViolationError):
        _load(TASKQ_IDLE_MAX_RUNTIME="0")


# ── Type checks ─────────────────────────────────────────────────────────


def test_idle_settle_window_is_float() -> None:
    s = _load()
    assert isinstance(s.idle_settle_window, float)


def test_idle_poll_interval_is_float() -> None:
    s = _load()
    assert isinstance(s.idle_poll_interval, float)


def test_idle_max_runtime_override_is_float() -> None:
    s = _load(TASKQ_IDLE_MAX_RUNTIME="60")
    assert isinstance(s.idle_max_runtime, float)

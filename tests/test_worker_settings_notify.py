"""Unit tests for WorkerSettings NOTIFY-related fields: poll_interval,
notify_health_check_interval, notify_reconnect_backoff_initial,
notify_enabled, and notify_poll_interval.
"""

import pytest

from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with sensible defaults.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── poll_interval defaults and env override ─────────────


def test_poll_interval_default() -> None:
    """WorkerSettings default poll_interval == 1.0."""
    s = _load()
    assert s.poll_interval == 1.0


def test_poll_interval_from_dict() -> None:
    """load_from_dict with POLL_INTERVAL=0.5 → poll_interval == 0.5."""
    s = _load(TASKQ_POLL_INTERVAL="0.5")
    assert s.poll_interval == 0.5


def test_poll_interval_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_POLL_INTERVAL=2.0 via env → poll_interval == 2.0."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_POLL_INTERVAL", "2.0")
    s = WorkerSettings.load()
    assert s.poll_interval == 2.0


# ── notify_health_check_interval defaults and env override ──────


def test_notify_health_check_interval_default() -> None:
    """-defaults. notify_health_check_interval default == 5.0."""
    s = _load()
    assert s.notify_health_check_interval == 5.0


def test_notify_health_check_interval_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """-env-override. TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL=2.5 → 2.5."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL", "2.5")
    s = WorkerSettings.load()
    assert s.notify_health_check_interval == 2.5


# ── notify_reconnect_backoff_initial defaults and env override ──


def test_notify_reconnect_backoff_initial_default() -> None:
    """-defaults. notify_reconnect_backoff_initial default == 1.0."""
    s = _load()
    assert s.notify_reconnect_backoff_initial == 1.0


def test_notify_reconnect_backoff_initial_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """-env-override. TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL=0.25 → 0.25."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL", "0.25")
    s = WorkerSettings.load()
    assert s.notify_reconnect_backoff_initial == 0.25


# ── NOTIFY dispatch: notify_enabled / notify_poll_interval ──────────────


def test_notify_enabled_default() -> None:
    """notify_enabled defaults to True."""
    s = _load()
    assert s.notify_enabled is True


def test_notify_enabled_from_dict_false() -> None:
    """TASKQ_NOTIFY_ENABLED=false → notify_enabled is False."""
    s = _load(TASKQ_NOTIFY_ENABLED="false")
    assert s.notify_enabled is False


def test_notify_enabled_from_dict_true() -> None:
    """TASKQ_NOTIFY_ENABLED=true → notify_enabled is True."""
    s = _load(TASKQ_NOTIFY_ENABLED="true")
    assert s.notify_enabled is True


def test_notify_poll_interval_default() -> None:
    """notify_poll_interval defaults to 5.0."""
    s = _load()
    assert s.notify_poll_interval == 5.0


def test_notify_poll_interval_from_dict() -> None:
    """TASKQ_NOTIFY_POLL_INTERVAL=10.0 → notify_poll_interval == 10.0."""
    s = _load(TASKQ_NOTIFY_POLL_INTERVAL="10.0")
    assert s.notify_poll_interval == 10.0


def test_notify_poll_interval_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_NOTIFY_POLL_INTERVAL=3.0 via env → notify_poll_interval == 3.0."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_NOTIFY_POLL_INTERVAL", "3.0")
    s = WorkerSettings.load()
    assert s.notify_poll_interval == 3.0


def test_notify_poll_interval_ge_constraint() -> None:
    """notify_poll_interval must be >= 0.5 (Field ge constraint)."""
    # dotenvmodel enforces ge at field level
    s = _load(TASKQ_NOTIFY_POLL_INTERVAL="0.5")
    assert s.notify_poll_interval == 0.5


def test_notify_poll_interval_is_float() -> None:
    """notify_poll_interval is a float."""
    s = _load()
    assert isinstance(s.notify_poll_interval, float)


# ── Co-existence with existing settings fields ──────────────────────────


def test_notify_fields_coexist_with_existing_defaults() -> None:
    """All three new fields coexist with existing WorkerSettings defaults."""
    s = _load()
    assert s.poll_interval == 1.0
    assert s.notify_health_check_interval == 5.0
    assert s.notify_reconnect_backoff_initial == 1.0
    assert s.heartbeat_interval == 10.0
    assert s.lock_lease == 60.0
    assert s.cancellation_grace_period == 30.0
    assert s.cleanup_grace_period == 10.0


def test_notify_fields_are_float() -> None:
    """poll_interval, notify_health_check_interval, and
    notify_reconnect_backoff_initial are all float type."""
    s = _load()
    assert isinstance(s.poll_interval, float)
    assert isinstance(s.notify_health_check_interval, float)
    assert isinstance(s.notify_reconnect_backoff_initial, float)

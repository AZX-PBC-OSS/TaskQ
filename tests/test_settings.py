"""Unit tests for WorkerSettings invariants and DSN fallback (no PG required)."""

import os
from datetime import timedelta
from pathlib import Path

import pytest
from dotenvmodel import (
    ConstraintViolationError,
    DotEnvModelError,
    TypeCoercionError,
    ValidationError,
)
from dotenvmodel.types import RedisDsn
from hypothesis import given
from hypothesis import strategies as st

from taskq.settings import OIDCSettings, SAMLSettings, TaskQSettings, WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


# ── Helpers ──────────────────────────────────────────────────────────────


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with sensible defaults.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── lock_lease invariant validation ────────────────────────────────


def test_lock_lease_too_small_raises() -> None:
    """lock_lease < 4 * heartbeat_interval raises ValidationError."""
    # Pin grace periods small so only the lock_lease invariant fires
    # (cancellation+cleanup < lock_lease holds at 0.1+0.1 < 30), and the
    # lag budget inside the lease so the lag-lease invariant stays quiet
    # (25 < 30 - 10).
    with pytest.raises(ValidationError, match=r"lock_lease.*must be >= 4 \* heartbeat_interval"):
        _load(
            TASKQ_LOCK_LEASE="30.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
            TASKQ_WATCHDOG_LOOP_LAG_BUDGET="15.0",
        )


def test_lock_lease_error_message_contains_fields() -> None:
    """Error message includes both field names and the ratio."""
    with pytest.raises(ValidationError) as exc_info:
        _load(
            TASKQ_LOCK_LEASE="30.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
            TASKQ_WATCHDOG_LOOP_LAG_BUDGET="15.0",
        )
    msg = str(exc_info.value)
    assert "lock_lease" in msg
    assert "heartbeat_interval" in msg
    assert "40" in msg  # 4 * 10


# ── lock_lease boundary acceptance ─────────────────────────────────


def test_lock_lease_at_boundary_accepted() -> None:
    """lock_lease == 4 * heartbeat_interval is accepted."""
    # Lag budget pinned inside the lease (25 + 10 < 40) so the lag-lease
    # invariant stays quiet and only the 4x boundary is under test.
    s = _load(
        TASKQ_LOCK_LEASE="40.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
        TASKQ_CLEANUP_GRACE_PERIOD="5.0",
        TASKQ_WATCHDOG_LOOP_LAG_BUDGET="25.0",
    )
    assert s.lock_lease == 40.0


def test_lock_lease_above_boundary_accepted() -> None:
    """lock_lease > 4 * heartbeat_interval is accepted."""
    s = _load(TASKQ_LOCK_LEASE="60.0", TASKQ_HEARTBEAT_INTERVAL="10.0")
    assert s.lock_lease == 60.0


# ── lag-watchdog lease invariant ────────────────────────────────────


def test_lag_budget_plus_heartbeat_must_fit_lease() -> None:
    """watchdog_loop_lag_budget + heartbeat_interval >= lock_lease raises.

    A stalled event loop must die (the terminal lag watchdog trips at
    watchdog_loop_lag_budget) before its leases can expire (lock_lease),
    otherwise the leader sweep reclaims LIVE jobs' locks mid-stall and the
    worker wakes to find its work reassigned. The heartbeat_interval term
    is the worst-case age the last beat can carry when the stall starts.
    """
    # lease 8 == 4 x heartbeat 2 (the 4x invariant holds at its boundary);
    # default lag budget 30 + 2 >= 8, so only the lag-lease invariant fires.
    with pytest.raises(ValidationError, match=r"watchdog_loop_lag_budget.*must be <"):
        _load(
            TASKQ_LOCK_LEASE="8.0",
            TASKQ_HEARTBEAT_INTERVAL="2.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
        )


def test_lag_lease_error_message_names_both_knobs() -> None:
    """The error names watchdog_loop_lag_budget AND lock_lease so operators
    know both knobs to adjust, and explains the live-reclaim semantics."""
    with pytest.raises(ValidationError) as exc_info:
        _load(
            TASKQ_LOCK_LEASE="8.0",
            TASKQ_HEARTBEAT_INTERVAL="2.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
        )
    msg = str(exc_info.value)
    assert "watchdog_loop_lag_budget" in msg
    assert "lock_lease" in msg
    assert "LIVE" in msg


def test_lag_lease_invariant_exempt_when_watchdog_disabled() -> None:
    """watchdog_enabled=False exempts the config: with no terminal lag
    detector armed, stall-vs-lease ordering is a deployment concern, not a
    load-time guarantee (same gating as the bounded-loop invariant)."""
    s = _load(
        TASKQ_LOCK_LEASE="8.0",
        TASKQ_HEARTBEAT_INTERVAL="2.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
        TASKQ_CLEANUP_GRACE_PERIOD="0.1",
        TASKQ_WATCHDOG_ENABLED="false",
    )
    assert s.lock_lease == 8.0


def test_lag_lease_invariant_satisfied_loads() -> None:
    """lag budget + heartbeat strictly below the lease loads and keeps the
    configured value (the defaults 30 + 10 < 60 are the shipped example)."""
    s = _load(
        TASKQ_LOCK_LEASE="40.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
        TASKQ_CLEANUP_GRACE_PERIOD="5.0",
        TASKQ_WATCHDOG_LOOP_LAG_BUDGET="25.0",
    )
    assert s.watchdog_loop_lag_budget == 25.0
    defaults = _load()
    assert defaults.watchdog_loop_lag_budget + defaults.heartbeat_interval < defaults.lock_lease


# ── lag budget vs check interval coherence ─────────────────────────


def test_lag_budget_at_or_below_check_interval_raises() -> None:
    """watchdog_loop_lag_budget <= watchdog_check_interval raises.

    The lag detector samples the loop once per check interval, so a budget
    at or below its own sampling period trips on a healthy loop's beat
    cadence — the beat is scheduled by the same poll that measures it
    (measured: budget 1.0 against the 1.0s default check interval killed
    an idle worker on its first armed poll).
    """
    with pytest.raises(ValidationError, match=r"watchdog_loop_lag_budget.*must be >"):
        _load(
            TASKQ_LOCK_LEASE="40.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
            TASKQ_CLEANUP_GRACE_PERIOD="5.0",
            TASKQ_WATCHDOG_LOOP_LAG_BUDGET="1.0",
        )


def test_lag_budget_check_interval_coherence_exempt_when_watchdog_disabled() -> None:
    """watchdog_enabled=False exempts the coherence check too: no lag
    detector is spawned, so the budget is inert."""
    s = _load(
        TASKQ_LOCK_LEASE="40.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
        TASKQ_CLEANUP_GRACE_PERIOD="5.0",
        TASKQ_WATCHDOG_LOOP_LAG_BUDGET="1.0",
        TASKQ_WATCHDOG_ENABLED="false",
    )
    assert s.watchdog_loop_lag_budget == 1.0


def test_lag_budget_above_check_interval_accepted() -> None:
    """A budget strictly above the check interval (with headroom) loads."""
    s = _load(
        TASKQ_LOCK_LEASE="40.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
        TASKQ_CLEANUP_GRACE_PERIOD="5.0",
        TASKQ_WATCHDOG_LOOP_LAG_BUDGET="25.0",
        TASKQ_WATCHDOG_CHECK_INTERVAL="3.5",
    )
    assert s.watchdog_loop_lag_budget == 25.0


# ── watchdog_loop_lag_warn_budget field ─────────────────────────────


def test_watchdog_loop_lag_warn_budget_default() -> None:
    """Default is 5.0: tier 1 warns long before the terminal 30s tier."""
    s = _load()
    assert s.watchdog_loop_lag_warn_budget == 5.0


def test_watchdog_loop_lag_warn_budget_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET round-trips through load()."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET", "7.5")
    s = WorkerSettings.load()
    assert s.watchdog_loop_lag_warn_budget == 7.5


# ── DSN fallback ──────────────────────────────────────────────────


def test_dsn_fallback_when_split_dsns_absent() -> None:
    """Only pg_dsn set → pg_dsn_direct and pg_dsn_pooled resolve to pg_dsn."""
    s = _load()
    assert s.pg_dsn_direct is not None
    assert s.pg_dsn_pooled is not None
    assert str(s.pg_dsn_direct) == str(s.pg_dsn)
    assert str(s.pg_dsn_pooled) == str(s.pg_dsn)


def test_dsn_fallback_with_split_dsns() -> None:
    """When pg_dsn_direct and pg_dsn_pooled are set, they are used as-is."""
    direct = "postgresql://user:pass@direct-host/taskq"
    pooled = "postgresql://user:pass@pooled-host/taskq"
    s = _load(TASKQ_PG_DSN_DIRECT=direct, TASKQ_PG_DSN_POOLED=pooled)
    assert str(s.pg_dsn_direct) == direct
    assert str(s.pg_dsn_pooled) == pooled


# ── health_pg_ping_timeout ──────────────────────────────────────────


def test_health_pg_ping_timeout_default() -> None:
    """Default value for health_pg_ping_timeout is 0.2."""
    s = _load()
    assert s.health_pg_ping_timeout == 0.2


def test_health_pg_ping_timeout_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_HEALTH_PG_PING_TIMEOUT=0.5 round-trips through WorkerSettings.load()."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_HEALTH_PG_PING_TIMEOUT", "0.5")
    s = WorkerSettings.load()
    assert s.health_pg_ping_timeout == 0.5


def test_health_pg_ping_timeout_via_dict() -> None:
    """load_from_dict with HEALTH_PG_PING_TIMEOUT=0.05 produces 0.05."""
    s = _load(TASKQ_HEALTH_PG_PING_TIMEOUT="0.05")
    assert s.health_pg_ping_timeout == 0.05


def test_health_pg_ping_timeout_negative_raises() -> None:
    """Negative health_pg_ping_timeout raises via the dotenvmodel ge=0.0 constraint."""
    with pytest.raises(ConstraintViolationError, match="greater than or equal to 0"):
        _load(TASKQ_HEALTH_PG_PING_TIMEOUT="-1.0")


# ── TASKQ_ prefix env-var loading ─────────────────────────────────


def test_env_prefix_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_LOCK_LEASE=120 and TASKQ_HEARTBEAT_INTERVAL=20 are picked up."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_LOCK_LEASE", "120")
    monkeypatch.setenv("TASKQ_HEARTBEAT_INTERVAL", "20")
    s = WorkerSettings.load()
    assert s.lock_lease == 120.0
    assert s.heartbeat_interval == 20.0


# ── Defaults ──────────────────────────────────────────────────────────────


def test_default_values() -> None:
    """Default pool sizes, timing, and grace periods are correct."""
    s = _load()
    assert s.dispatcher_pool_size == 4
    assert s.heartbeat_pool_size == 4
    assert s.max_concurrency == 8
    assert s.heartbeat_interval == 10.0
    assert s.lock_lease == 60.0
    assert s.max_heartbeat_failures == 3
    assert s.termination_grace_period == 75.0
    assert s.cancellation_grace_period == 30.0
    assert s.cleanup_grace_period == 10.0
    assert s.pool_max_inactive_lifetime == 300.0


def test_worker_pool_size_derived() -> None:
    """worker_pool_size is derived from max_concurrency."""
    s = _load(TASKQ_MAX_CONCURRENCY="8")
    assert s.worker_pool_size == 12  # int(8 * 1.5)


def test_worker_pool_size_rounds_down() -> None:
    """worker_pool_size rounds down for non-integer multiples."""
    s = _load(TASKQ_MAX_CONCURRENCY="5")
    assert s.worker_pool_size == 7  # int(5 * 1.5) = int(7.5) = 7


# ── force_update_actor_config ──────────────────────────────────────────────


def test_force_update_actor_config_default() -> None:
    """force_update_actor_config defaults to False."""
    s = _load()
    assert s.force_update_actor_config is False


def test_force_update_actor_config_via_env_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true produces force_update_actor_config=True."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_FORCE_UPDATE_ACTOR_CONFIG", "true")
    s = WorkerSettings.load()
    assert s.force_update_actor_config is True


# ── rate_limit_pg_fallback_enabled ──────────────────────────────────


def test_rate_limit_pg_fallback_enabled_default() -> None:
    """rate_limit_pg_fallback_enabled defaults to True."""
    s = _load()
    assert s.rate_limit_pg_fallback_enabled is True


def test_rate_limit_pg_fallback_enabled_false_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=false produces False."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED", "false")
    s = WorkerSettings.load()
    assert s.rate_limit_pg_fallback_enabled is False


# ── Cancellation invariant ─────────────────────────────────────────


def test_cancellation_grace_plus_cleanup_exceeds_lock_lease_raises() -> None:
    """cancellation_grace_period + cleanup_grace_period >= lock_lease raises ValidationError."""
    # Use lock_lease=60.0, heartbeat_interval=10.0 (valid for).
    # Set termination_grace_period high enough that the check does NOT
    # fire first (termination check: 40+20 < termination-5; use 120 => 60 < 115 ✓).
    # Then set cancellation+cleanup >= lock_lease to trigger the check.
    with pytest.raises(ValidationError, match="must be < lock_lease"):
        _load(
            TASKQ_LOCK_LEASE="60.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="40.0",
            TASKQ_CLEANUP_GRACE_PERIOD="20.0",
            TASKQ_TERMINATION_GRACE_PERIOD="120.0",
        )


def test_cancellation_grace_plus_cleanup_below_lock_lease_accepted() -> None:
    """cancellation_grace_period + cleanup_grace_period < lock_lease is accepted."""
    s = _load(
        TASKQ_LOCK_LEASE="60.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="30.0",
        TASKQ_CLEANUP_GRACE_PERIOD="10.0",
    )
    assert s.cancellation_grace_period + s.cleanup_grace_period < s.lock_lease


# ── lock_lease violation raises before any connection ──────────────


def test_lock_lease_violation_before_connections() -> None:
    """ValidationError is raised at construction, before any asyncpg calls."""
    # If the error fires, no pool or connection was opened.
    # This is a white-box check: post_load fires inside load_from_dict
    # which is a sync call. No asyncpg calls happen synchronously.
    # Lag budget pinned inside the lease (5 + 3 < 10) so the lag-lease
    # invariant stays quiet and the raise is the 4x invariant's alone.
    with pytest.raises(ValidationError, match="lock_lease"):
        _load(
            TASKQ_LOCK_LEASE="10.0",
            TASKQ_HEARTBEAT_INTERVAL="3.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
            TASKQ_WATCHDOG_LOOP_LAG_BUDGET="5.0",
        )


# ── lock_lease invariant universality ──────────────────────────────


@given(
    lock_lease=st.floats(min_value=1.0, max_value=3600.0, allow_nan=False, allow_infinity=False),
    heartbeat_interval=st.floats(
        min_value=0.5, max_value=300.0, allow_nan=False, allow_infinity=False
    ),
)
def test_lock_lease_invariant_universality(lock_lease: float, heartbeat_interval: float) -> None:
    """ValidationError raised iff lock_lease < 4 * heartbeat_interval.

    Picks generous cancellation/cleanup grace values that always satisfy the
    cancellation invariant (sum < lock_lease) when the invariant
    holds, so the 4x boundary is the only one under test. The lag budget is
    derived as 0.7 x lease, which keeps every other invariant quiet
    wherever the 4x invariant holds (hb <= lease/4 < 0.3 x lease, so
    0.7 x lease + hb < lease; and lease >= 2 in that branch, so
    0.7 x lease > 1.0 = the default check interval) — except where the
    draw cannot satisfy the lease invariant at all (heartbeat >= lease
    leaves no legal positive budget), in which case the 4x violation and
    the lag-lease violation aggregate into MultipleValidationErrors;
    DotEnvModelError covers both shapes.
    """
    # Pin grace values small enough that is satisfied for the smallest
    # accepted lock_lease (>= 4 * heartbeat_interval >= 4 * 0.5 = 2.0).
    grace_each = 0.1
    overrides = {
        "TASKQ_LOCK_LEASE": str(lock_lease),
        "TASKQ_HEARTBEAT_INTERVAL": str(heartbeat_interval),
        "TASKQ_CANCELLATION_GRACE_PERIOD": str(grace_each),
        "TASKQ_CLEANUP_GRACE_PERIOD": str(grace_each),
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": str(lock_lease * 0.7),
    }
    should_raise = lock_lease < 4 * heartbeat_interval

    if should_raise:
        with pytest.raises(DotEnvModelError, match="lock_lease"):
            _load(**overrides)
    else:
        s = _load(**overrides)
        assert s.lock_lease == lock_lease
        assert s.heartbeat_interval == heartbeat_interval


# ── A-TG-02: post_load skipped → str(pg_dsn_direct) == "None" footgun ──────


def test_post_load_skipped_produces_none_dsn_string() -> None:
    """A-TG-02: bypassing post_load (e.g. via object.__new__ + field assignment)
    leaves pg_dsn_direct as None. str(None) == 'None', documenting the footgun.

    This test is intentionally white-box: it verifies the contract documented
    in deps.py's explicit assertion guards and confirms that the guard is needed.
    """
    instance = object.__new__(WorkerSettings)
    # Simulate deserialisation from pickle or direct construction without
    # calling load() / load_from_dict() (which run post_load).
    instance.pg_dsn_direct = None
    # The footgun: str(None) == "None" (not a crash, not a missing value error)
    assert str(instance.pg_dsn_direct) == "None"


# ── A-TG-03: compute_connection_budget with odd max_concurrency ──────────────


def test_connection_budget_odd_max_concurrency_floor_truncation() -> None:
    """A-TG-03: with max_concurrency=9, worker_pool_size = int(9*1.5) = 13
    (floor truncation, not ceiling). Documenting that odd values differ from
    ceiling: ceil(9*1.5) = 14, but int() truncates to 13.
    """
    from taskq.worker.budget import compute_connection_budget

    s = _load(TASKQ_MAX_CONCURRENCY="9")
    budget = compute_connection_budget(s, num_worker_pods=1)
    assert budget.pooled_per_worker == 13  # int(9 * 1.5) = int(13.5) = 13
    assert s.worker_pool_size == 13


def test_connection_budget_max_concurrency_five_truncates() -> None:
    """A-TG-03 additional: max_concurrency=5 → int(7.5) = 7 (not 8)."""
    from taskq.worker.budget import compute_connection_budget

    s = _load(TASKQ_MAX_CONCURRENCY="5")
    budget = compute_connection_budget(s, num_worker_pods=1)
    assert budget.pooled_per_worker == 7  # int(5 * 1.5) = int(7.5) = 7


# ── otel_enabled (,) ─────────────────────────────────────


def test_otel_enabled_default_is_true() -> None:
    """otel_enabled defaults to True."""
    s = _load()
    assert s.otel_enabled is True


def test_otel_enabled_false_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_OTEL_ENABLED=false produces otel_enabled=False."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_OTEL_ENABLED", "false")
    s = WorkerSettings.load()
    assert s.otel_enabled is False


def test_otel_enabled_false_via_dict() -> None:
    """load_from_dict with OTEL_ENABLED=false produces False."""
    s = _load(TASKQ_OTEL_ENABLED="false")
    assert s.otel_enabled is False


# ── worker_group (,) ────────────────────────────────────────────


def test_worker_group_default_is_default() -> None:
    """worker_group defaults to 'default'."""
    s = _load()
    assert s.worker_group == "default"


def test_worker_group_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_WORKER_GROUP=my-group round-trips through WorkerSettings.load()."""
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_WORKER_GROUP", "my-group")
    s = WorkerSettings.load()
    assert s.worker_group == "my-group"


def test_worker_group_via_dict() -> None:
    """load_from_dict with WORKER_GROUP=production produces 'production'."""
    s = _load(TASKQ_WORKER_GROUP="production")
    assert s.worker_group == "production"


# ── grace budget validation ──────────────────────────────────


def test_grace_budget_validation_fires_when_termination_grace_present() -> None:
    """cancellation_grace + cleanup_grace must satisfy
    cancellation + cleanup < termination_grace - 5.

    Uses large values that pass the (lock_lease) constraint but violate
    """
    # lock_lease=240, heartbeat=10 → valid (240 >= 40)
    # termination=90, cancellation=50, cleanup=40 → 50+40=90 >= 90-5=85 → fires
    with pytest.raises(ValidationError, match=r"termination_grace_period"):
        _load(
            TASKQ_LOCK_LEASE="240.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_TERMINATION_GRACE_PERIOD="90.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="50.0",
            TASKQ_CLEANUP_GRACE_PERIOD="40.0",
        )


def test_grace_budget_valid_when_within_termination_window() -> None:
    """cancellation + cleanup < termination - 5 is accepted."""
    # termination=100, cancellation=40, cleanup=10 → 50 < 95 ✓
    s = _load(
        TASKQ_LOCK_LEASE="240.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_TERMINATION_GRACE_PERIOD="100.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="40.0",
        TASKQ_CLEANUP_GRACE_PERIOD="10.0",
    )
    assert s.termination_grace_period == 100.0
    assert s.cancellation_grace_period + s.cleanup_grace_period < s.termination_grace_period - 5.0


@given(
    termination=st.floats(min_value=20.0, max_value=600.0, allow_nan=False, allow_infinity=False),
    cancellation=st.floats(min_value=0.0, max_value=300.0, allow_nan=False, allow_infinity=False),
    cleanup=st.floats(min_value=0.0, max_value=300.0, allow_nan=False, allow_infinity=False),
)
def test_grace_budget_invariant_property(
    termination: float, cancellation: float, cleanup: float
) -> None:
    """property: the invariant fires iff cancellation+cleanup >= termination-5.

    lock_lease=4*heartbeat=40 is pinned small to avoid interfering;
    cancellation+cleanup are pinned below lock_lease to avoid fires.
    """
    from hypothesis import assume

    # Pin values such that and do not fire:
    # lock_lease=4000 (large) and cancellation+cleanup < 4000 always (values ≤ 300+300=600)
    lock_lease = 4000.0
    heartbeat = 10.0
    assume(cancellation + cleanup < lock_lease)

    overrides = {
        "TASKQ_LOCK_LEASE": str(lock_lease),
        "TASKQ_HEARTBEAT_INTERVAL": str(heartbeat),
        "TASKQ_TERMINATION_GRACE_PERIOD": str(termination),
        "TASKQ_CANCELLATION_GRACE_PERIOD": str(cancellation),
        "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup),
    }
    should_fail = cancellation + cleanup >= termination - 5.0

    if should_fail:
        with pytest.raises(
            ValidationError, match=r"termination_grace_period|must be < termination"
        ):
            _load(**overrides)
    else:
        s = _load(**overrides)
        assert s.termination_grace_period == termination


# ── environment field (TaskQSettings) ────────────────────────────────────────


def test_environment_default_is_none() -> None:
    """environment defaults to None when TASKQ_ENVIRONMENT is not set."""
    s = TaskQSettings.load_from_dict({})
    assert s.environment is None


def test_environment_dev_via_dict() -> None:
    """TASKQ_ENVIRONMENT=dev is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_ENVIRONMENT": "dev"})
    assert s.environment == "dev"


def test_environment_dev_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_ENVIRONMENT=dev round-trips through TaskQSettings.load()."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    s = TaskQSettings.load()
    assert s.environment == "dev"


def test_environment_development_via_dict() -> None:
    """TASKQ_ENVIRONMENT=development is loaded as-is."""
    s = TaskQSettings.load_from_dict({"TASKQ_ENVIRONMENT": "development"})
    assert s.environment == "development"


def test_environment_production_via_dict() -> None:
    """TASKQ_ENVIRONMENT=production is loaded as-is (non-dev value)."""
    s = TaskQSettings.load_from_dict({"TASKQ_ENVIRONMENT": "production"})
    assert s.environment == "production"


def test_environment_inherited_by_worker_settings() -> None:
    """WorkerSettings inherits the environment field from TaskQSettings."""
    s = _load(TASKQ_ENVIRONMENT="dev")
    assert s.environment == "dev"


# ── admin_max_sse_connections field (TaskQSettings) ──────────────────────────


def test_admin_max_sse_connections_default() -> None:
    """admin_max_sse_connections defaults to 50."""
    s = TaskQSettings.load_from_dict({})
    assert s.admin_max_sse_connections == 50


def test_admin_max_sse_connections_via_dict() -> None:
    """TASKQ_ADMIN_MAX_SSE_CONNECTIONS=100 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_ADMIN_MAX_SSE_CONNECTIONS": "100"})
    assert s.admin_max_sse_connections == 100


def test_admin_max_sse_connections_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_ADMIN_MAX_SSE_CONNECTIONS=100 round-trips through TaskQSettings.load()."""
    monkeypatch.setenv("TASKQ_ADMIN_MAX_SSE_CONNECTIONS", "100")
    s = TaskQSettings.load()
    assert s.admin_max_sse_connections == 100


def test_admin_max_sse_connections_zero_raises() -> None:
    """admin_max_sse_connections=0 violates ge=1 constraint."""
    with pytest.raises(ConstraintViolationError, match="greater than or equal to 1"):
        TaskQSettings.load_from_dict({"TASKQ_ADMIN_MAX_SSE_CONNECTIONS": "0"})


def test_admin_max_sse_connections_inherited_by_worker_settings() -> None:
    """WorkerSettings inherits admin_max_sse_connections from TaskQSettings."""
    s = _load(TASKQ_ADMIN_MAX_SSE_CONNECTIONS="200")
    assert s.admin_max_sse_connections == 200


# ── admin_host / admin_port (TaskQSettings) ───────────────────────────────────


def test_admin_host_default() -> None:
    """admin_host defaults to '0.0.0.0'."""
    s = TaskQSettings.load_from_dict({})
    assert s.admin_host == "0.0.0.0"  # noqa: S104


def test_admin_port_default() -> None:
    """admin_port defaults to 8080."""
    s = TaskQSettings.load_from_dict({})
    assert s.admin_port == 8080


def test_admin_host_via_dict() -> None:
    """TASKQ_ADMIN_HOST=127.0.0.1 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_ADMIN_HOST": "127.0.0.1"})
    assert s.admin_host == "127.0.0.1"


def test_admin_port_via_dict() -> None:
    """TASKQ_ADMIN_PORT=9090 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_ADMIN_PORT": "9090"})
    assert s.admin_port == 9090


def test_admin_port_out_of_range_raises() -> None:
    """admin_port=0 violates ge=1 constraint."""
    with pytest.raises(ConstraintViolationError):
        TaskQSettings.load_from_dict({"TASKQ_ADMIN_PORT": "0"})


# ── admin_url (TaskQSettings) ─────────────────────────────────────────────────


def test_admin_url_default() -> None:
    """admin_url defaults to 'http://localhost:8080'."""
    s = TaskQSettings.load_from_dict({})
    assert s.admin_url == "http://localhost:8080"


def test_admin_url_via_dict() -> None:
    """TASKQ_ADMIN_URL=http://admin:8001 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_ADMIN_URL": "http://admin:8001"})
    assert s.admin_url == "http://admin:8001"


def test_admin_url_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """TASKQ_ADMIN_URL round-trips through TaskQSettings.load()."""
    monkeypatch.setenv("TASKQ_ADMIN_URL", "http://localhost:8001")
    s = TaskQSettings.load()
    assert s.admin_url == "http://localhost:8001"


def test_admin_url_inherited_by_worker_settings() -> None:
    """WorkerSettings inherits admin_url from TaskQSettings."""
    s = _load(TASKQ_ADMIN_URL="http://sidecar:8001")
    assert s.admin_url == "http://sidecar:8001"


# ── example_host / example_port (TaskQSettings) ───────────────────────────────


def test_example_host_default() -> None:
    """example_host defaults to '0.0.0.0'."""
    s = TaskQSettings.load_from_dict({})
    assert s.example_host == "0.0.0.0"  # noqa: S104


def test_example_port_default() -> None:
    """example_port defaults to 8000."""
    s = TaskQSettings.load_from_dict({})
    assert s.example_port == 8000


def test_example_host_via_dict() -> None:
    """TASKQ_EXAMPLE_HOST=127.0.0.1 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_EXAMPLE_HOST": "127.0.0.1"})
    assert s.example_host == "127.0.0.1"


def test_example_port_via_dict() -> None:
    """TASKQ_EXAMPLE_PORT=8000 is loaded by load_from_dict."""
    s = TaskQSettings.load_from_dict({"TASKQ_EXAMPLE_PORT": "8000"})
    assert s.example_port == 8000


def test_example_port_out_of_range_raises() -> None:
    """example_port=0 violates ge=1 constraint."""
    with pytest.raises(ConstraintViolationError):
        TaskQSettings.load_from_dict({"TASKQ_EXAMPLE_PORT": "0"})


def test_example_host_inherited_by_worker_settings() -> None:
    """WorkerSettings inherits example_host from TaskQSettings."""
    s = _load(TASKQ_EXAMPLE_HOST="127.0.0.1")
    assert s.example_host == "127.0.0.1"


def test_example_port_inherited_by_worker_settings() -> None:
    """WorkerSettings inherits example_port from TaskQSettings."""
    s = _load(TASKQ_EXAMPLE_PORT="8000")
    assert s.example_port == 8000


# ── Pruning schedule defaults ─────────────────────────────────────────


def test_prune_schedule_utc_default() -> None:
    """prune_schedule_utc defaults to '03:00'."""
    s = _load()
    assert s.prune_schedule_utc == "03:00"


def test_prune_cron_expr_default_is_none() -> None:
    """prune_cron_expr defaults to None."""
    s = _load()
    assert s.prune_cron_expr is None


def test_prune_batch_size_default() -> None:
    """prune_batch_size defaults to 10000."""
    s = _load()
    assert s.prune_batch_size == 10000


def test_prune_batch_size_via_dict() -> None:
    """TASKQ_PRUNE_BATCH_SIZE=5000 round-trips through load_from_dict."""
    s = _load(TASKQ_PRUNE_BATCH_SIZE="5000")
    assert s.prune_batch_size == 5000


# ── Per-status prune retention defaults ────────────────────────────────


def test_prune_retention_period_default() -> None:
    """prune_retention_period defaults to timedelta(days=30)."""
    s = _load()
    assert s.prune_retention_period == timedelta(days=30)


def test_prune_retention_succeeded_default() -> None:
    """prune_retention_succeeded defaults to timedelta(days=30)."""
    s = _load()
    assert s.prune_retention_succeeded == timedelta(days=30)


def test_prune_retention_failed_default() -> None:
    """prune_retention_failed defaults to timedelta(days=90)."""
    s = _load()
    assert s.prune_retention_failed == timedelta(days=90)


def test_prune_retention_cancelled_default() -> None:
    """prune_retention_cancelled defaults to timedelta(days=30)."""
    s = _load()
    assert s.prune_retention_cancelled == timedelta(days=30)


def test_prune_retention_abandoned_default() -> None:
    """prune_retention_abandoned defaults to timedelta(days=90)."""
    s = _load()
    assert s.prune_retention_abandoned == timedelta(days=90)


# ── Archive retention & expiry schedule defaults ──────────────────────


def test_archive_retention_period_default() -> None:
    """archive_retention_period defaults to timedelta(days=365)."""
    s = _load()
    assert s.archive_retention_period == timedelta(days=365)


def test_archive_expiry_schedule_utc_default() -> None:
    """archive_expiry_schedule_utc defaults to '04:00'."""
    s = _load()
    assert s.archive_expiry_schedule_utc == "04:00"


def test_archive_expiry_cron_expr_default_is_none() -> None:
    """archive_expiry_cron_expr defaults to None."""
    s = _load()
    assert s.archive_expiry_cron_expr is None


# ── negative prune_retention_* raises ConstraintViolationError ──────


def test_prune_retention_succeeded_negative_raises() -> None:
    """prune_retention_succeeded=timedelta(days=-1) raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_retention_succeeded.*must not be negative"
    ):
        _load(TASKQ_PRUNE_RETENTION_SUCCEEDED="-86400")


def test_prune_retention_period_negative_raises() -> None:
    """Negative prune_retention_period raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_retention_period.*must not be negative"
    ):
        _load(TASKQ_PRUNE_RETENTION_PERIOD="-86400")


def test_prune_retention_failed_negative_raises() -> None:
    """Negative prune_retention_failed raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_retention_failed.*must not be negative"
    ):
        _load(TASKQ_PRUNE_RETENTION_FAILED="-86400")


def test_prune_retention_cancelled_negative_raises() -> None:
    """Negative prune_retention_cancelled raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_retention_cancelled.*must not be negative"
    ):
        _load(TASKQ_PRUNE_RETENTION_CANCELLED="-86400")


def test_prune_retention_abandoned_negative_raises() -> None:
    """Negative prune_retention_abandoned raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_retention_abandoned.*must not be negative"
    ):
        _load(TASKQ_PRUNE_RETENTION_ABANDONED="-86400")


# ── negative archive_retention_period raises ConstraintViolationError ───


def test_archive_retention_period_negative_raises() -> None:
    """archive_retention_period=timedelta(days=-1) raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"archive_retention_period.*must not be negative"
    ):
        _load(TASKQ_ARCHIVE_RETENTION_PERIOD="-86400")


# ── negative retention raises before any connection ──────────


def test_negative_retention_raises_before_connections() -> None:
    """ConstraintViolationError is raised at construction, before any asyncpg calls."""
    with pytest.raises(ConstraintViolationError, match="prune_retention_succeeded"):
        _load(TASKQ_PRUNE_RETENTION_SUCCEEDED="-86400")


# ── negative archive_retention_period raises before any connection ────


def test_negative_archive_retention_raises_before_connections() -> None:
    """ConstraintViolationError is raised at construction, before any asyncpg calls."""
    with pytest.raises(ConstraintViolationError, match="archive_retention_period"):
        _load(TASKQ_ARCHIVE_RETENTION_PERIOD="-86400")


# ── timedelta(0) accepted for all retention fields ────────────────────


def test_prune_retention_period_zero_accepted() -> None:
    """prune_retention_period=timedelta(0) is accepted."""
    s = _load(TASKQ_PRUNE_RETENTION_PERIOD="0")
    assert s.prune_retention_period == timedelta(0)


def test_prune_retention_succeeded_zero_accepted() -> None:
    """prune_retention_succeeded=timedelta(0) is accepted."""
    s = _load(TASKQ_PRUNE_RETENTION_SUCCEEDED="0")
    assert s.prune_retention_succeeded == timedelta(0)


def test_prune_retention_failed_zero_accepted() -> None:
    """prune_retention_failed=timedelta(0) is accepted."""
    s = _load(TASKQ_PRUNE_RETENTION_FAILED="0")
    assert s.prune_retention_failed == timedelta(0)


def test_prune_retention_cancelled_zero_accepted() -> None:
    """prune_retention_cancelled=timedelta(0) is accepted."""
    s = _load(TASKQ_PRUNE_RETENTION_CANCELLED="0")
    assert s.prune_retention_cancelled == timedelta(0)


def test_prune_retention_abandoned_zero_accepted() -> None:
    """prune_retention_abandoned=timedelta(0) is accepted."""
    s = _load(TASKQ_PRUNE_RETENTION_ABANDONED="0")
    assert s.prune_retention_abandoned == timedelta(0)


def test_archive_retention_period_zero_accepted() -> None:
    """archive_retention_period=timedelta(0) is accepted."""
    s = _load(TASKQ_ARCHIVE_RETENTION_PERIOD="0")
    assert s.archive_retention_period == timedelta(0)


# ── Cron scheduler settings ─────────────────────────────────────────


def test_cron_catch_up_window_default() -> None:
    """cron_catch_up_window defaults to timedelta(hours=1)."""
    s = _load()
    assert s.cron_catch_up_window == timedelta(hours=1)


def test_cron_auto_disable_threshold_default() -> None:
    """cron_auto_disable_threshold defaults to 3."""
    s = _load()
    assert s.cron_auto_disable_threshold == 3


def test_cron_auto_disable_threshold_zero_raises() -> None:
    """cron_auto_disable_threshold=0 violates ge=1 constraint."""
    with pytest.raises(ConstraintViolationError, match="greater than or equal to 1"):
        _load(TASKQ_CRON_AUTO_DISABLE_THRESHOLD="0")


def test_cron_auto_disable_threshold_via_dict() -> None:
    """TASKQ_CRON_AUTO_DISABLE_THRESHOLD=5 round-trips through load_from_dict."""
    s = _load(TASKQ_CRON_AUTO_DISABLE_THRESHOLD="5")
    assert s.cron_auto_disable_threshold == 5


def test_cron_catch_up_window_negative_raises() -> None:
    """cron_catch_up_window=timedelta(seconds=-1) raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"cron_catch_up_window.*must not be negative"
    ):
        _load(TASKQ_CRON_CATCH_UP_WINDOW="-1")


def test_cron_catch_up_window_zero_accepted() -> None:
    """cron_catch_up_window=timedelta(0) is accepted."""
    s = _load(TASKQ_CRON_CATCH_UP_WINDOW="0")
    assert s.cron_catch_up_window == timedelta(0)


# ── dispatcher_command_timeout vs stale-loop budget ─────────────────────
#
# The period-1 leader loops (cron, scheduled_wake) tick once per iteration,
# await PG (bounded by dispatcher_command_timeout), then sleep 1.0s. Their
# worst-case tick gap is therefore timeout + 1.0s, and their staleness
# budget is max(1.0 * watchdog_tick_grace_factor, watchdog_stale_floor).
# If the gap can exceed the budget, detector 2 force-exits a healthy leader
# mid-degradation, exactly what the field description forbids ("keep this
# below the watchdog staleness budget"). The default must clear the bar,
# and the cross-field invariant must reject configs that do not.


def test_dispatcher_command_timeout_default_clears_period_one_loop_budget() -> None:
    """Default timeout + the 1.0s trailing sleep fits the tightest staleness
    budget with headroom (timeout 10.0 / floor 10.0 gave an 11s gap against a
    10s budget: detector 2 force-exited a healthy, still-leader worker)."""
    s = _load()
    budget = max(s.watchdog_tick_grace_factor, s.watchdog_stale_floor)
    assert s.dispatcher_command_timeout + 1.0 < budget, (
        f"default dispatcher_command_timeout ({s.dispatcher_command_timeout}) + "
        f"1.0s loop period must be < the tightest staleness budget ({budget})"
    )


def test_dispatcher_command_timeout_at_budget_rejected() -> None:
    """timeout == watchdog_stale_floor violates the invariant (the old
    default): the timeout-capped iteration plus the trailing 1s sleep
    overruns the budget."""
    with pytest.raises(ValidationError, match=r"dispatcher_command_timeout"):
        _load(TASKQ_DISPATCHER_COMMAND_TIMEOUT="10.0")


def test_dispatcher_command_timeout_above_budget_rejected() -> None:
    """A timeout beyond the budget is a guaranteed false-trip, not a risk;
    and every bounded loop's constraint reports it (leader + producer here)."""
    with pytest.raises(DotEnvModelError, match=r"dispatcher_command_timeout"):
        _load(TASKQ_DISPATCHER_COMMAND_TIMEOUT="30.0")


def test_dispatcher_command_timeout_allowed_when_floor_raised() -> None:
    """Deliberate configs stay expressible: raise the floor alongside the
    timeout and every loop's budget still covers timeout + period (the
    producer at period 5.0s is the binding one here: 30 + 5 < 40)."""
    s = _load(
        TASKQ_DISPATCHER_COMMAND_TIMEOUT="30.0",
        TASKQ_WATCHDOG_STALE_FLOOR="40.0",
    )
    assert s.dispatcher_command_timeout == 30.0
    assert s.watchdog_stale_floor == 40.0


def test_dispatcher_command_timeout_unchecked_when_watchdog_disabled() -> None:
    """With watchdog_enabled=False detector 2 is never spawned, so the
    timeout-vs-budget invariant cannot fire, so a deployment that pinned the
    old 10.0 default alongside the disabled watchdog must still boot."""
    s = _load(
        TASKQ_WATCHDOG_ENABLED="false",
        TASKQ_DISPATCHER_COMMAND_TIMEOUT="10.0",
    )
    assert s.dispatcher_command_timeout == 10.0
    assert s.watchdog_enabled is False


def test_unsatisfiable_budget_errors_against_budget_fields() -> None:
    """A budget <= period + 1.0s leaves no legal timeout (ge=1.0): the
    error must be attributed to the budget fields the operator can actually
    change, not the timeout field they cannot. (The slow producer poll
    keeps the producer side satisfiable so only the leader-loop error
    fires, since two errors would aggregate instead of raising ValidationError.)"""
    with pytest.raises(ValidationError) as exc_info:
        _load(
            TASKQ_WATCHDOG_STALE_FLOOR="1.5",
            TASKQ_WATCHDOG_TICK_GRACE_FACTOR="2.0",
            TASKQ_NOTIFY_POLL_INTERVAL="6.0",
        )
    assert "Field 'watchdog_stale_floor'" in str(exc_info.value), (
        f"an unsatisfiable budget must be reported against the budget side: {exc_info.value}"
    )


def test_producer_loop_not_checked_by_validator() -> None:
    """The invariant covers only the period-1 leader loops (scheduled_wake,
    cron), NOT the producer: the producer's dispatch_batch is a
    multi-statement transaction not wrapped in asyncio.timeout, so the
    timeout + period model does not hold. A config that would fail the
    producer check if it were checked (but passes the leader check) must
    load successfully."""
    # Leader: budget=max(1*5, 2)=5, 1.0+1=2 < 5 → passes
    # Producer (if checked): budget=max(0.2*5, 2)=2, 1.0+0.2=1.2 < 2 → also passes
    # Use a config where only the leader check matters:
    s = _load(
        TASKQ_NOTIFY_ENABLED="false",
        TASKQ_POLL_INTERVAL="0.2",
        TASKQ_WATCHDOG_STALE_FLOOR="2.0",
        TASKQ_WATCHDOG_TICK_GRACE_FACTOR="5.0",
        TASKQ_DISPATCHER_COMMAND_TIMEOUT="1.0",
    )
    assert s.dispatcher_command_timeout == 1.0


def test_producer_loop_budget_passes_when_timeout_fits() -> None:
    """Same shape, timeout small enough for both loops' budgets."""
    s = _load(
        TASKQ_NOTIFY_ENABLED="false",
        TASKQ_POLL_INTERVAL="0.2",
        TASKQ_WATCHDOG_STALE_FLOOR="2.0",
        TASKQ_WATCHDOG_TICK_GRACE_FACTOR="5.0",
        TASKQ_DISPATCHER_COMMAND_TIMEOUT="1.0",
    )
    assert s.dispatcher_command_timeout == 1.0


# ── OIDC/SAML sub-config singletons (dotenvmodel cached()) ──────────────


def test_oidc_property_returns_cached_singleton() -> None:
    """Repeated settings.oidc accesses return the same cached instance."""
    s = TaskQSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    try:
        assert s.oidc is s.oidc
        assert s.oidc is OIDCSettings.cached()
    finally:
        OIDCSettings.reset_cached()


def test_saml_property_returns_cached_singleton() -> None:
    """Repeated settings.saml accesses return the same cached instance."""
    s = TaskQSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    try:
        assert s.saml is s.saml
        assert s.saml is SAMLSettings.cached()
    finally:
        SAMLSettings.reset_cached()


def test_oidc_reload_picks_up_new_env_in_place(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIGHUP-style reload: reload() mutates the cached instance in place."""
    monkeypatch.setenv("TASKQ_OIDC_ISSUER", "https://idp-a.example")
    s = TaskQSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    try:
        first = s.oidc
        assert first.issuer == "https://idp-a.example"
        monkeypatch.setenv("TASKQ_OIDC_ISSUER", "https://idp-b.example")
        first.reload()
        assert s.oidc is first
        assert s.oidc.issuer == "https://idp-b.example"
    finally:
        OIDCSettings.reset_cached()


def test_oidc_reset_cached_forces_reload(monkeypatch: pytest.MonkeyPatch) -> None:
    """reset_cached() forces the next access to re-read the environment."""
    monkeypatch.setenv("TASKQ_OIDC_ISSUER", "https://idp-a.example")
    s = TaskQSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    try:
        first = s.oidc
        monkeypatch.setenv("TASKQ_OIDC_ISSUER", "https://idp-b.example")
        OIDCSettings.reset_cached()
        assert s.oidc is not first
        assert s.oidc.issuer == "https://idp-b.example"
    finally:
        OIDCSettings.reset_cached()


# ── log_level validation ─────────────────────────────────────────────


def test_log_level_default() -> None:
    """log_level defaults to 'INFO'."""
    s = _load()
    assert s.log_level == "INFO"


def test_log_level_valid_via_dict() -> None:
    """TASKQ_LOG_LEVEL=DEBUG loads successfully."""
    s = _load(TASKQ_LOG_LEVEL="DEBUG")
    assert s.log_level == "DEBUG"


def test_log_level_case_insensitive_lower() -> None:
    """TASKQ_LOG_LEVEL=debug normalizes to 'DEBUG'."""
    s = _load(TASKQ_LOG_LEVEL="debug")
    assert s.log_level == "DEBUG"


def test_log_level_case_insensitive_mixed() -> None:
    """TASKQ_LOG_LEVEL=Warning normalizes to 'WARNING'."""
    s = _load(TASKQ_LOG_LEVEL="Warning")
    assert s.log_level == "WARNING"


def test_log_level_invalid_raises() -> None:
    """TASKQ_LOG_LEVEL=BOGUS raises ConstraintViolationError."""
    with pytest.raises(ConstraintViolationError, match=r"log_level must be one of"):
        _load(TASKQ_LOG_LEVEL="BOGUS")


# ── sso_backend validation ───────────────────────────────────────────


def test_sso_backend_default() -> None:
    """sso_backend defaults to 'none'."""
    s = _load()
    assert s.sso_backend == "none"


def test_sso_backend_valid_via_dict() -> None:
    """TASKQ_SSO_BACKEND=oidc loads successfully."""
    s = _load(TASKQ_SSO_BACKEND="oidc")
    assert s.sso_backend == "oidc"


def test_sso_backend_case_insensitive_upper() -> None:
    """TASKQ_SSO_BACKEND=OIDC normalizes to 'oidc'."""
    s = _load(TASKQ_SSO_BACKEND="OIDC")
    assert s.sso_backend == "oidc"


def test_sso_backend_case_insensitive_mixed() -> None:
    """TASKQ_SSO_BACKEND=Saml normalizes to 'saml'."""
    s = _load(TASKQ_SSO_BACKEND="Saml")
    assert s.sso_backend == "saml"


def test_sso_backend_invalid_raises() -> None:
    """TASKQ_SSO_BACKEND=bogus raises ConstraintViolationError."""
    with pytest.raises(ConstraintViolationError, match=r"sso_backend must be one of"):
        _load(TASKQ_SSO_BACKEND="bogus")


# ── poll_interval gt=0 ───────────────────────────────────────────────


def test_poll_interval_default() -> None:
    """poll_interval defaults to 1.0."""
    s = _load()
    assert s.poll_interval == 1.0


def test_poll_interval_small_accepted() -> None:
    """poll_interval=0.05 is accepted (gt=0 allows any positive value for test speed)."""
    s = _load(TASKQ_POLL_INTERVAL="0.05")
    assert s.poll_interval == 0.05


def test_poll_interval_zero_raises() -> None:
    """poll_interval=0 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_POLL_INTERVAL="0")


def test_poll_interval_negative_raises() -> None:
    """poll_interval=-1 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_POLL_INTERVAL="-1")


# ── notify_health_check_interval gt=0 ────────────────────────────────


def test_notify_health_check_interval_small_accepted() -> None:
    """notify_health_check_interval=0.001 is accepted (gt=0 allows any positive value)."""
    s = _load(TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL="0.001")
    assert s.notify_health_check_interval == 0.001


def test_notify_health_check_interval_zero_raises() -> None:
    """notify_health_check_interval=0 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL="0")


def test_notify_health_check_interval_negative_raises() -> None:
    """notify_health_check_interval=-0.5 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL="-0.5")


# ── notify_reconnect_backoff_initial gt=0 ────────────────────────────


def test_notify_reconnect_backoff_initial_small_accepted() -> None:
    """notify_reconnect_backoff_initial=0.005 is accepted (gt=0 allows any positive value)."""
    s = _load(TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL="0.005")
    assert s.notify_reconnect_backoff_initial == 0.005


def test_notify_reconnect_backoff_initial_zero_raises() -> None:
    """notify_reconnect_backoff_initial=0 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL="0")


def test_notify_reconnect_backoff_initial_negative_raises() -> None:
    """notify_reconnect_backoff_initial=-0.01 violates gt=0 constraint."""
    with pytest.raises(ConstraintViolationError, match=r"greater than 0"):
        _load(TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL="-0.01")


# ── prune_schedule_utc HH:MM validation ──────────────────────────────


def test_prune_schedule_utc_valid_via_dict() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=12:30 loads successfully."""
    s = _load(TASKQ_PRUNE_SCHEDULE_UTC="12:30")
    assert s.prune_schedule_utc == "12:30"


def test_prune_schedule_utc_midnight_accepted() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=00:00 is accepted."""
    s = _load(TASKQ_PRUNE_SCHEDULE_UTC="00:00")
    assert s.prune_schedule_utc == "00:00"


def test_prune_schedule_utc_end_of_day_accepted() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=23:59 is accepted."""
    s = _load(TASKQ_PRUNE_SCHEDULE_UTC="23:59")
    assert s.prune_schedule_utc == "23:59"


def test_prune_schedule_utc_bad_format_raises() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=3:00 (missing leading zero) raises."""
    with pytest.raises(ConstraintViolationError, match=r"prune_schedule_utc must be HH:MM format"):
        _load(TASKQ_PRUNE_SCHEDULE_UTC="3:00")


def test_prune_schedule_utc_out_of_range_hours_raises() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=24:00 raises (hours must be 00-23)."""
    with pytest.raises(ConstraintViolationError, match=r"prune_schedule_utc must be HH:MM format"):
        _load(TASKQ_PRUNE_SCHEDULE_UTC="24:00")


def test_prune_schedule_utc_out_of_range_minutes_raises() -> None:
    """TASKQ_PRUNE_SCHEDULE_UTC=12:60 raises (minutes must be 00-59)."""
    with pytest.raises(ConstraintViolationError, match=r"prune_schedule_utc must be HH:MM format"):
        _load(TASKQ_PRUNE_SCHEDULE_UTC="12:60")


# ── archive_expiry_schedule_utc HH:MM validation ─────────────────────


def test_archive_expiry_schedule_utc_valid_via_dict() -> None:
    """TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC=23:59 loads successfully."""
    s = _load(TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC="23:59")
    assert s.archive_expiry_schedule_utc == "23:59"


def test_archive_expiry_schedule_utc_bad_format_raises() -> None:
    """TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC=foo raises."""
    with pytest.raises(
        ConstraintViolationError, match=r"archive_expiry_schedule_utc must be HH:MM format"
    ):
        _load(TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC="foo")


def test_archive_expiry_schedule_utc_out_of_range_hours_raises() -> None:
    """TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC=25:00 raises."""
    with pytest.raises(
        ConstraintViolationError, match=r"archive_expiry_schedule_utc must be HH:MM format"
    ):
        _load(TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC="25:00")


# ── prune_cron_expr validation ───────────────────────────────────────


def test_prune_cron_expr_valid_via_dict() -> None:
    """TASKQ_PRUNE_CRON_EXPR=0 3 * * * loads successfully."""
    s = _load(TASKQ_PRUNE_CRON_EXPR="0 3 * * *")
    assert s.prune_cron_expr == "0 3 * * *"


def test_prune_cron_expr_invalid_raises() -> None:
    """TASKQ_PRUNE_CRON_EXPR=not-a-cron raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError, match=r"prune_cron_expr must be a valid cron expression"
    ):
        _load(TASKQ_PRUNE_CRON_EXPR="not-a-cron")


# ── archive_expiry_cron_expr validation ──────────────────────────────


def test_archive_expiry_cron_expr_valid_via_dict() -> None:
    """TASKQ_ARCHIVE_EXPIRY_CRON_EXPR=*/5 * * * * loads successfully."""
    s = _load(TASKQ_ARCHIVE_EXPIRY_CRON_EXPR="*/5 * * * *")
    assert s.archive_expiry_cron_expr == "*/5 * * * *"


def test_archive_expiry_cron_expr_invalid_raises() -> None:
    """TASKQ_ARCHIVE_EXPIRY_CRON_EXPR=invalid raises ConstraintViolationError."""
    with pytest.raises(
        ConstraintViolationError,
        match=r"archive_expiry_cron_expr must be a valid cron expression",
    ):
        _load(TASKQ_ARCHIVE_EXPIRY_CRON_EXPR="invalid")


# ── Suite hermeticity vs developer dotfiles ─────────────────────────────


def test_suite_ignores_developer_dotfiles(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A ``.env`` in the working directory is NOT read by ``load()``.

    Behavior proof for the session-scoped ``_no_developer_dotfiles`` fixture
    (``tests/conftest.py``): it points ``DOTENV_DIR`` at an empty directory so
    a developer's gitignored ``.env`` (which ``.env.example`` tells them to
    create, with a real-looking ``TASKQ_PG_DSN``) can never override
    monkeypatched test env vars and redirect destructive SQL at a dev
    database. If this test fails, that fixture is broken and EVERY
    ``.load()``-based test is suspect on a machine with a dotfile.
    """
    dotenv_dir = os.environ.get("DOTENV_DIR")
    assert dotenv_dir is not None, "_no_developer_dotfiles did not set DOTENV_DIR"
    dotenv_path = Path(dotenv_dir)
    assert dotenv_path.is_dir(), "DOTENV_DIR must exist (dotenvmodel raises otherwise)"
    assert not list(dotenv_path.iterdir()), "DOTENV_DIR must be empty"

    # A dotfile in the CWD that WOULD set heartbeat_interval if read (no env
    # var is set for it below, so precedence/override doesn't enter into it —
    # only whether the cascade reads this directory at all).
    (tmp_path / ".env").write_text("TASKQ_HEARTBEAT_INTERVAL=123\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.delenv("TASKQ_HEARTBEAT_INTERVAL", raising=False)

    s = WorkerSettings.load()
    assert s.heartbeat_interval == 10.0  # default — the dotfile value is 123


# ── dotenvmodel 1.0.0: .env precedence & type coercion ──────────────
#
# dotenvmodel 1.0.0 inverted the 0.6.3 default: ``load()`` resolves
# process env vars > merged .env cascade > field defaults (0.6.3 let
# .env files overwrite os.environ). ``load(override=True)`` or
# ``DOTENV_OVERRIDE=true`` opts back into files-beat-env. The tests below
# pin that contract so a silent reversion fails loudly.


_LOAD_KNOB_VARS = (
    "ENV",
    "DOTENV_OVERRIDE",
    "DOTENV_DIR",
    "DOTENV_READ_DOTFILES",
    "DOTENV_READ_ENVIRON",
    "DOTENV_LOAD_LOCAL",
)


def _clear_load_knobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear every process-env knob that can skew the dotenv cascade.

    dotenvmodel resolves these from the process environment before any
    ``.env`` file is read, so a stray host-shell value would reshape the
    cascade under test: ``DOTENV_DIR`` redirects the cascade root away
    from ``tmp_path``, ``DOTENV_READ_DOTFILES=false`` skips files
    entirely, ``ENV`` / ``DOTENV_LOAD_LOCAL`` change which files are
    selected, ``DOTENV_OVERRIDE`` flips the precedence being pinned, and
    ``DOTENV_READ_ENVIRON=false`` drops ``os.environ`` as a value source —
    inverting every env-beats-file pin in this section.
    """
    for var in _LOAD_KNOB_VARS:
        monkeypatch.delenv(var, raising=False)


def _chdir_with_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the dotenv cascade at a ``tmp_path`` holding one ``.env`` file.

    Clearing the load knobs (see :func:`_clear_load_knobs`) keeps the
    fixture's single ``.env`` the only file variable: the cascade always
    probes ``.env``, and ``ENV`` (any value, including the ``dev`` default)
    merely *adds* ``.env.{env}`` layers on top of it (later files win) —
    with no such files here, the cascade reads exactly this one file.
    ``monkeypatch.chdir`` restores the previous cwd afterwards.
    """
    _clear_load_knobs(monkeypatch)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("TASKQ_SCHEMA_NAME=file_value\n", encoding="utf-8")


def _chdir_with_env_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, files: dict[str, str]
) -> None:
    """Point the dotenv cascade at ``tmp_path`` with the given ``.env`` files.

    ``files`` maps file names (``.env``, ``.env.local``, ``.env.staging``,
    ...) to the ``TASKQ_SCHEMA_NAME`` value each carries. Load knobs are
    cleared first (see :func:`_clear_load_knobs`) so only the test's own
    ``ENV`` / ``DOTENV_*`` settings shape the cascade.
    """
    _clear_load_knobs(monkeypatch)
    monkeypatch.chdir(tmp_path)
    for name, value in files.items():
        (tmp_path / name).write_text(f"TASKQ_SCHEMA_NAME={value}\n", encoding="utf-8")


def test_load_env_var_beats_env_file_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dotenvmodel 1.0.0 default: a real env var beats a ``.env`` value."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    s = TaskQSettings.load()
    assert s.schema_name == "env_value"


def test_load_override_true_lets_env_file_beat_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``load(override=True)`` opts back into 0.6.3-style files-beat-env."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    s = TaskQSettings.load(override=True)
    assert s.schema_name == "file_value"


def test_load_dotenv_override_env_var_lets_env_file_beat_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``DOTENV_OVERRIDE=true`` in the environment flips precedence for a
    plain ``load()`` — the deployment-level equivalent of ``override=True``."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    monkeypatch.setenv("DOTENV_OVERRIDE", "true")
    s = TaskQSettings.load()
    assert s.schema_name == "file_value"


def test_env_file_beats_field_default_when_no_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ``.env`` cascade is still read under 1.0.0: a file value beats
    the field default when no env var is set."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "file_value"


def test_load_read_dotfiles_false_ignores_env_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``read_dotfiles=False`` skips the ``.env`` cascade entirely — a
    present ``.env`` has no effect and the field default wins."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load(read_dotfiles=False)
    assert s.schema_name == "taskq"


def test_load_read_environ_false_ignores_process_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``read_environ=False`` skips the process environment — a real env
    var loses to the ``.env`` file value, the mirror image of the default
    precedence."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    s = TaskQSettings.load(read_environ=False)
    assert s.schema_name == "file_value"


def test_stray_host_dotenv_read_environ_cannot_reshape_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stray host ``DOTENV_READ_ENVIRON=false`` is neutralized by the knob
    guard exactly like ``DOTENV_OVERRIDE``: without the var in
    ``_LOAD_KNOB_VARS`` the host value survives ``_clear_load_knobs`` and
    silently inverts every env-beats-file pin in this section (dotenvmodel
    stops reading ``os.environ`` as a value source, so the ``.env`` value
    wins)."""
    monkeypatch.setenv("DOTENV_READ_ENVIRON", "false")  # the stray host value
    _chdir_with_env_file(tmp_path, monkeypatch)  # knob guard must clear it here
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    s = TaskQSettings.load()
    assert s.schema_name == "env_value"


# ── .local skip under ENV=test ──────────────────────────────────────────


_LOCAL_SKIP_FILES = {
    ".env": "base_value",
    ".env.local": "local_value",
    ".env.test.local": "test_local_value",
}


def test_load_env_test_skips_both_local_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under ``ENV=test`` both ``.local`` files are skipped — gitignored
    local overrides cannot decide test outcomes, so ``.env`` wins."""
    _chdir_with_env_files(tmp_path, monkeypatch, _LOCAL_SKIP_FILES)
    monkeypatch.setenv("ENV", "test")
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "base_value"


def test_load_env_test_dotenv_load_local_restores_local_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``DOTENV_LOAD_LOCAL=true`` restores the skipped ``.local`` files
    under ``ENV=test`` — and the later ``.env.test.local`` still beats
    ``.env`` (later files in the chain win)."""
    _chdir_with_env_files(tmp_path, monkeypatch, _LOCAL_SKIP_FILES)
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("DOTENV_LOAD_LOCAL", "true")
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "test_local_value"


def test_load_env_unset_dev_default_reads_env_local(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ``ENV`` unset (dev default) ``.env.local`` IS read and beats
    ``.env`` — the ``.local`` skip is test-env-only."""
    _chdir_with_env_files(tmp_path, monkeypatch, _LOCAL_SKIP_FILES)
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "local_value"


# ── ENV file selection: label vs selector ───────────────────────────────


_STAGING_FILES = {
    ".env": "base_value",
    ".env.staging": "staging_value",
}


def test_load_env_var_selects_env_specific_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ENV=staging`` adds the ``.env.staging`` layer, which beats ``.env``
    (later files in the chain win)."""
    _chdir_with_env_files(tmp_path, monkeypatch, _STAGING_FILES)
    monkeypatch.setenv("ENV", "staging")
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "staging_value"


def test_load_explicit_env_arg_beats_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Explicit ``load(env=...)`` beats the ``ENV`` process var — the
    documented argument > env var > default tier."""
    _chdir_with_env_files(tmp_path, monkeypatch, _STAGING_FILES)
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load(env="staging")
    assert s.schema_name == "staging_value"


def test_taskq_environment_is_label_not_file_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``TASKQ_ENVIRONMENT`` is a TaskQ deployment label, not a file
    selector: setting it never loads ``.env.{environment}`` files."""
    _chdir_with_env_files(tmp_path, monkeypatch, _STAGING_FILES)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "staging")
    monkeypatch.delenv("TASKQ_SCHEMA_NAME", raising=False)
    s = TaskQSettings.load()
    assert s.schema_name == "base_value"
    assert s.environment == "staging"


# ── Explicit-argument-beats-knob tier ───────────────────────────────────


def test_load_explicit_override_arg_beats_dotenv_override_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``load(override=False)`` beats ``DOTENV_OVERRIDE=true`` in the
    process env — the documented argument > ``DOTENV_*`` env var > default
    tier, so callers can pin precedence regardless of deployment knobs."""
    _chdir_with_env_file(tmp_path, monkeypatch)
    monkeypatch.setenv("TASKQ_SCHEMA_NAME", "env_value")
    monkeypatch.setenv("DOTENV_OVERRIDE", "true")
    s = TaskQSettings.load(override=False)
    assert s.schema_name == "env_value"


def test_load_from_dict_coerces_redis_url_to_redis_dsn() -> None:
    """``load_from_dict`` coerces values through field types: a ``redis://``
    string yields a ``dotenvmodel.types.RedisDsn`` instance, not ``str``."""
    s = TaskQSettings.load_from_dict({"TASKQ_REDIS_URL": "redis://localhost:6379/0"})
    redis_url = s.redis_url
    assert isinstance(redis_url, RedisDsn)
    assert str(redis_url) == "redis://localhost:6379/0"


def test_load_from_dict_invalid_redis_url_scheme_raises_type_coercion_error() -> None:
    """A ``TASKQ_REDIS_URL`` with a non-Redis scheme fails type coercion."""
    with pytest.raises(TypeCoercionError, match="redis_url"):
        TaskQSettings.load_from_dict({"TASKQ_REDIS_URL": "http://not-redis"})


# ── result_max_bytes ────────────────────────────────────────────────────


def test_result_max_bytes_default_matches_the_shipped_constant() -> None:
    from taskq.constants import MAX_RESULT_BYTES

    assert _load().result_max_bytes == MAX_RESULT_BYTES == 65536


def test_result_max_bytes_via_dict_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _load(TASKQ_RESULT_MAX_BYTES="262144").result_max_bytes == 262144
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_RESULT_MAX_BYTES", "1048576")
    assert WorkerSettings.load().result_max_bytes == 1048576


def test_result_max_bytes_ceiling_matches_progress_data_max_bytes() -> None:
    """Both payload caps top out at 1 MiB: the durable result can be
    configured as large as the transient progress payload."""
    with pytest.raises(ConstraintViolationError):
        _load(TASKQ_RESULT_MAX_BYTES="1048577")
    with pytest.raises(ConstraintViolationError):
        _load(TASKQ_RESULT_MAX_BYTES="1023")

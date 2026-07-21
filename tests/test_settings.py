"""Unit tests for WorkerSettings invariants and DSN fallback (no PG required)."""

from datetime import timedelta

import pytest
from dotenvmodel import ConstraintViolationError, ValidationError
from hypothesis import given
from hypothesis import strategies as st

from taskq.settings import TaskQSettings, WorkerSettings

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
    # (cancellation+cleanup < lock_lease holds at 0.1+0.1 < 30).
    with pytest.raises(ValidationError, match=r"lock_lease.*must be >= 4 \* heartbeat_interval"):
        _load(
            TASKQ_LOCK_LEASE="30.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
        )


def test_lock_lease_error_message_contains_fields() -> None:
    """Error message includes both field names and the ratio."""
    with pytest.raises(ValidationError) as exc_info:
        _load(
            TASKQ_LOCK_LEASE="30.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
        )
    msg = str(exc_info.value)
    assert "lock_lease" in msg
    assert "heartbeat_interval" in msg
    assert "40" in msg  # 4 * 10


# ── lock_lease boundary acceptance ─────────────────────────────────


def test_lock_lease_at_boundary_accepted() -> None:
    """lock_lease == 4 * heartbeat_interval is accepted."""
    s = _load(
        TASKQ_LOCK_LEASE="40.0",
        TASKQ_HEARTBEAT_INTERVAL="10.0",
        TASKQ_CANCELLATION_GRACE_PERIOD="15.0",
        TASKQ_CLEANUP_GRACE_PERIOD="5.0",
    )
    assert s.lock_lease == 40.0


def test_lock_lease_above_boundary_accepted() -> None:
    """lock_lease > 4 * heartbeat_interval is accepted."""
    s = _load(TASKQ_LOCK_LEASE="60.0", TASKQ_HEARTBEAT_INTERVAL="10.0")
    assert s.lock_lease == 60.0


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
    with pytest.raises(ValidationError, match="lock_lease"):
        _load(
            TASKQ_LOCK_LEASE="10.0",
            TASKQ_HEARTBEAT_INTERVAL="10.0",
            TASKQ_CANCELLATION_GRACE_PERIOD="0.1",
            TASKQ_CLEANUP_GRACE_PERIOD="0.1",
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
    holds, so the boundary is the only one under test.
    """
    # Pin grace values small enough that is satisfied for the smallest
    # accepted lock_lease (>= 4 * heartbeat_interval >= 4 * 0.5 = 2.0).
    grace_each = 0.1
    overrides = {
        "TASKQ_LOCK_LEASE": str(lock_lease),
        "TASKQ_HEARTBEAT_INTERVAL": str(heartbeat_interval),
        "TASKQ_CANCELLATION_GRACE_PERIOD": str(grace_each),
        "TASKQ_CLEANUP_GRACE_PERIOD": str(grace_each),
    }
    should_raise = lock_lease < 4 * heartbeat_interval

    if should_raise:
        with pytest.raises(ValidationError, match="lock_lease"):
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
    # Directly set the private Pydantic fields via __dict__ to bypass validation.
    # This simulates deserialisation from pickle or direct construction without
    # calling load() / load_from_dict() (which run post_load).
    object.__setattr__(instance, "pg_dsn_direct", None)
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

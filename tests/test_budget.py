"""Unit tests for connection-budget arithmetic (no PG required)."""

import pytest

from taskq.settings import WorkerSettings
from taskq.worker.budget import compute_connection_budget

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _settings(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings with the TASKQ_ prefix that load_from_dict expects."""
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── compute_connection_budget arithmetic ───────────────────────────


def test_budget_default_sizing_five_pods() -> None:
    """example: 5 worker pods, 1 leader pod, max_concurrency=8."""
    s = _settings(
        TASKQ_DISPATCHER_POOL_SIZE="4",
        TASKQ_HEARTBEAT_POOL_SIZE="4",
        TASKQ_MAX_CONCURRENCY="8",
    )
    budget = compute_connection_budget(
        s,
        num_worker_pods=5,
        num_leader_pods=1,
        num_web_pods=4,
        web_pool_size=10,
        pgbouncer_compression_ratio=1.0,
    )

    assert budget.direct_per_worker_non_leader == 10  # 4+4+1+1
    assert budget.direct_per_worker_leader == 12  # 10+2 (leader-monitor + cron_conn)
    assert budget.total_direct == 52  # 4*10 + 1*12
    assert budget.pooled_per_worker == 12  # int(8*1.5)
    assert budget.total_pooled == 60  # 5*12
    assert budget.total_web == 40  # 4*10
    assert budget.total_pg == 152.0  # 52 + (60+40)/1.0
    assert budget.pgbouncer_recommended is True  # 152 > 80


def test_budget_with_pgbouncer() -> None:
    """With pgbouncer_compression_ratio=10.0, total_pg drops."""
    s = _settings(
        TASKQ_DISPATCHER_POOL_SIZE="4",
        TASKQ_HEARTBEAT_POOL_SIZE="4",
        TASKQ_MAX_CONCURRENCY="8",
    )
    budget = compute_connection_budget(
        s,
        num_worker_pods=5,
        num_leader_pods=1,
        num_web_pods=4,
        web_pool_size=10,
        pgbouncer_compression_ratio=10.0,
    )
    # total_direct=52, total_pooled=60, total_web=40
    # total_pg = 52 + (60+40)/10 = 52 + 10 = 62
    assert budget.total_pg == 62.0
    assert budget.pgbouncer_recommended is False  # 62 <= 80


# ── pgbouncer_recommended threshold ────────────────────────────────


def test_small_deployment_budget_values() -> None:
    """1 worker pod, 1 leader pod, max_concurrency=4."""
    s = _settings(TASKQ_MAX_CONCURRENCY="4")
    budget = compute_connection_budget(
        s,
        num_worker_pods=1,
        num_leader_pods=1,
        pgbouncer_compression_ratio=1.0,
    )
    assert budget.direct_per_worker_non_leader == 10
    assert budget.direct_per_worker_leader == 12
    assert budget.pooled_per_worker == 6  # int(4*1.5)
    # 1 worker, 1 leader: total_direct = 0*10 + 1*12 = 12
    assert budget.total_direct == 12
    assert budget.total_pooled == 6
    assert budget.total_web == 0
    assert budget.total_pg == 18.0  # 12 + 6
    assert budget.pgbouncer_recommended is False  # 17 <= 80


# ── Leader pods capped ────────────────────────────────────────────────────


def test_leader_pods_capped_at_worker_pods() -> None:
    """num_leader_pods is capped at num_worker_pods."""
    s = _settings()
    budget = compute_connection_budget(
        s,
        num_worker_pods=2,
        num_leader_pods=5,  # capped to 2
    )
    assert budget.total_direct == 2 * 12  # all pods are leaders (capped)
    assert budget.total_pooled == 2 * 12


# ── Non-default pool sizes ────────────────────────────────────────────────


def test_custom_pool_sizes() -> None:
    """Custom dispatcher and heartbeat pool sizes change direct_per_worker."""
    s = _settings(
        TASKQ_DISPATCHER_POOL_SIZE="8",
        TASKQ_HEARTBEAT_POOL_SIZE="2",
    )
    budget = compute_connection_budget(s, num_worker_pods=1)
    assert budget.direct_per_worker_non_leader == 12  # 8+2+1+1
    assert budget.direct_per_worker_leader == 14


# ── ConnectionBudget is frozen ────────────────────────────────────────────


def test_budget_is_frozen() -> None:
    """ConnectionBudget is a frozen dataclass."""
    s = _settings()
    budget = compute_connection_budget(s, num_worker_pods=1)
    with pytest.raises(AttributeError):
        budget.total_pg = 999  # type: ignore[misc]

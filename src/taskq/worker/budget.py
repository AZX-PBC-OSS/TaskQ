"""Connection-budget arithmetic.

Pure-Python helper that computes the number of direct and pooled PostgreSQL
connections required for a given deployment shape.  No I/O, no async.
"""

from dataclasses import dataclass

from taskq.settings import WorkerSettings

__all__ = ["ConnectionBudget", "compute_connection_budget"]


@dataclass(frozen=True, slots=True)
class ConnectionBudget:
    """Counts from the connection-budget formula."""

    direct_per_worker_non_leader: int
    """Per non-leader pod: dispatcher + heartbeat + notify + leader_lock."""

    direct_per_worker_leader: int
    """Per leader pod: above + leader-monitor + cron_conn (opened by the elected leader)."""

    pooled_per_worker: int
    """Per worker pod: int(max_concurrency * 1.5)."""

    total_direct: int
    """All pods, direct connections."""

    total_pooled: int
    """All worker pods, pooled connections (via PgBouncer or direct)."""

    total_web: int
    """Web/API pods, pooled connections."""

    total_pg: float
    """Effective PG connections after PgBouncer compression."""

    pgbouncer_recommended: bool
    """True when total_pg > 80 (conservative below PG default max_connections=100)."""


def compute_connection_budget(
    settings: WorkerSettings,
    num_worker_pods: int,
    num_leader_pods: int = 1,
    num_web_pods: int = 0,
    web_pool_size: int = 10,
    pgbouncer_compression_ratio: float = 1.0,
) -> ConnectionBudget:
    """Compute connection counts.

    Args:
        settings: Worker settings (pool sizes, max_concurrency).
        num_worker_pods: Total worker pods.
        num_leader_pods: Pods that may win the advisory lock (capped at num_worker_pods).
        num_web_pods: Web/API pods.
        web_pool_size: Pool size per web pod.
        pgbouncer_compression_ratio: 1.0 = no PgBouncer; ~10.0 for transaction mode.

    Returns:
        A :class:`ConnectionBudget` with all integer counts.
    """
    # Cap leader pods at worker pods
    effective_leaders = min(num_leader_pods, num_worker_pods)

    direct_per_worker_non_leader = (
        settings.dispatcher_pool_size + settings.heartbeat_pool_size + 2  # notify + leader_lock
    )
    direct_per_worker_leader = (
        direct_per_worker_non_leader + 2
    )  # leader-monitor + cron_conn on elected pod
    pooled_per_worker = settings.worker_pool_size

    non_leaders = num_worker_pods - effective_leaders
    total_direct = (
        non_leaders * direct_per_worker_non_leader + effective_leaders * direct_per_worker_leader
    )
    total_pooled = num_worker_pods * pooled_per_worker
    total_web = num_web_pods * web_pool_size

    total_pg = total_direct + (total_pooled + total_web) / pgbouncer_compression_ratio

    return ConnectionBudget(
        direct_per_worker_non_leader=direct_per_worker_non_leader,
        direct_per_worker_leader=direct_per_worker_leader,
        pooled_per_worker=pooled_per_worker,
        total_direct=total_direct,
        total_pooled=total_pooled,
        total_web=total_web,
        total_pg=total_pg,
        pgbouncer_recommended=total_pg > 80,
    )

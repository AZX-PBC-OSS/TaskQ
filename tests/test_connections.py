"""Unit tests for connection hook points (taskq.connections, worker deps).

These tests do not require a running Postgres/Redis — they use fakes and
mocks to verify the ownership, teardown, and fallback semantics of the
WorkerConnections hook points. Integration tests against real PG live in
test_worker_deps.py (marked ``integration``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import asyncpg
import pytest

from taskq.connections import WorkerConnections

# ── WorkerConnections validation ───────────────────────────────────────


async def _fake_pool_factory() -> asyncpg.Pool:
    return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]


async def _fake_conn_factory() -> asyncpg.Connection:
    return MagicMock(spec=asyncpg.Connection)  # type: ignore[return-value]


async def _fake_redis_factory() -> Any:
    return MagicMock()


def test_worker_connections_empty_has_any_false() -> None:
    """An empty WorkerConnections reports has_any() == False."""
    assert not WorkerConnections().has_any()


def test_worker_connections_has_any_true_with_concrete() -> None:
    """A concrete pool sets has_any() == True."""
    pool = MagicMock(spec=asyncpg.Pool)
    assert WorkerConnections(worker_pool=pool).has_any()


def test_worker_connections_has_any_true_with_factory() -> None:
    """A factory sets has_any() == True."""
    assert WorkerConnections(worker_pool_factory=_fake_pool_factory).has_any()


def test_worker_connections_rejects_concrete_and_factory_same_role() -> None:
    """Providing both concrete and factory for the same role is a config error."""
    pool = MagicMock(spec=asyncpg.Pool)
    with pytest.raises(ValueError, match="worker_pool"):
        WorkerConnections(worker_pool=pool, worker_pool_factory=_fake_pool_factory)


def test_worker_connections_allows_concrete_one_role_factory_another() -> None:
    """Concrete for one role and factory for a different role is fine."""
    pool = MagicMock(spec=asyncpg.Pool)
    conns = WorkerConnections(
        worker_pool=pool,
        heartbeat_pool_factory=_fake_pool_factory,
    )
    assert conns.has_any()


def test_worker_connections_rejects_all_role_conflicts() -> None:
    """Every role pair is validated, not just the first."""
    pool = MagicMock(spec=asyncpg.Pool)
    conn = MagicMock(spec=asyncpg.Connection)
    for concrete, factory in [
        ("dispatcher_pool", "dispatcher_pool_factory"),
        ("heartbeat_pool", "heartbeat_pool_factory"),
        ("worker_pool", "worker_pool_factory"),
        ("notify_conn", "notify_conn_factory"),
        ("leader_conn", "leader_conn_factory"),
        ("redis_client", "redis_client_factory"),
    ]:
        with pytest.raises(ValueError, match=concrete):
            WorkerConnections(
                **{concrete: pool if "pool" in concrete or "redis" in concrete else conn},  # type: ignore[arg-type]
                **{
                    factory: _fake_pool_factory
                    if "pool" in factory
                    else _fake_redis_factory
                    if "redis" in factory
                    else _fake_conn_factory
                },  # type: ignore[arg-type]
            )

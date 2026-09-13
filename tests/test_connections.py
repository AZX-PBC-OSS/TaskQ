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

from taskq.connections import (
    DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    DEFAULT_STATEMENT_CACHE_SIZE,
    WorkerConnections,
    statement_cache_kwargs,
)
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.settings import make_integration_settings
from taskq.worker._bootstrap import (
    _slot_pool_factory,  # pyright: ignore[reportPrivateUsage]  # Why: the DSN-built slot-pool factory is the directly-callable representative of TaskQ's pool-construction sites.
)

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


# ── asyncpg statement-cache defaults ───────────────────────────────────


def test_statement_cache_defaults_hold_their_documented_values() -> None:
    """TaskQ's statement-cache defaults: 512 entries, 1 h lifetime.

    512 covers the rendered list_jobs filter-combination space (384+
    distinct texts) that thrashed asyncpg's 100-entry default (measured
    90-96% steady-state miss rate, benchmarks/ab_stmt_cache.py); 3600 s
    stops needless re-prepares on long-running workers.
    """
    assert DEFAULT_STATEMENT_CACHE_SIZE == 512
    assert DEFAULT_MAX_CACHED_STATEMENT_LIFETIME == 3600


async def test_dsn_built_pool_passes_statement_cache_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TaskQ-built pools forward the statement-cache kwargs to create_pool.

    Drives the per-slot pool factory — a representative TaskQ
    pool-construction site — against a fake create_pool and asserts both
    kwargs arrive, so a call site cannot silently drop them.
    """
    captured: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> asyncpg.Pool:
        captured.update(kwargs)
        return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]

    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)
    settings = make_integration_settings("postgresql://taskq:taskq@localhost:5432/taskq")

    pool = await _slot_pool_factory(settings, None)()

    assert pool is not None
    assert captured["statement_cache_size"] == DEFAULT_STATEMENT_CACHE_SIZE
    assert captured["max_cached_statement_lifetime"] == DEFAULT_MAX_CACHED_STATEMENT_LIFETIME


async def test_dsn_built_pool_resolves_statement_cache_from_settings_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env override → WorkerSettings.load() → the slot pool's create_pool kwargs.

    The settings-aware half of the wiring: ``_slot_pool_factory`` resolves
    the pair through ``statement_cache_kwargs(settings)``, so an operator's
    ``TASKQ_STATEMENT_CACHE_SIZE`` / ``TASKQ_MAX_CACHED_STATEMENT_LIFETIME``
    reach the real ``create_pool`` call without a code change.
    (read_dotfiles=False keeps the test hermetic against a developer's .env;
    the test connections.py double is declared in
    test_double_signature_drift._NARROWER_BY_DESIGN.)
    """
    monkeypatch.setenv("TASKQ_STATEMENT_CACHE_SIZE", "1024")
    monkeypatch.setenv("TASKQ_MAX_CACHED_STATEMENT_LIFETIME", "7200")
    monkeypatch.setenv("TASKQ_PG_DSN_DIRECT", "postgresql://taskq:taskq@localhost:5432/taskq")
    settings = WorkerSettings.load(read_dotfiles=False)

    captured: dict[str, Any] = {}

    async def _fake_create_pool(**kwargs: Any) -> asyncpg.Pool:
        captured.update(kwargs)
        return MagicMock(spec=asyncpg.Pool)  # type: ignore[return-value]

    monkeypatch.setattr(asyncpg, "create_pool", _fake_create_pool)

    pool = await _slot_pool_factory(settings, None)()

    assert pool is not None
    assert captured["statement_cache_size"] == 1024
    assert captured["max_cached_statement_lifetime"] == 7200


# ── statement-cache settings wiring ────────────────────────────────────


def test_statement_cache_kwargs_fallback_without_settings() -> None:
    """No settings in scope → the module constants.

    Call sites without a settings instance (the testing fixtures) keep the
    documented defaults; the resolver's fallback must agree with them.
    """
    assert statement_cache_kwargs() == {
        "statement_cache_size": DEFAULT_STATEMENT_CACHE_SIZE,
        "max_cached_statement_lifetime": DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    }


def test_statement_cache_kwargs_unconfigured_settings_yield_constants() -> None:
    """Settings loaded without the env vars set resolve to the same pair —
    the settings defaults are wired to the constants, not re-hardcoded."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq"})
    assert statement_cache_kwargs(settings) == statement_cache_kwargs()


async def test_statement_cache_kwargs_env_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env var → TaskQSettings field → pool kwarg dict.

    The settings-aware resolution path: an operator setting the env vars
    changes what a settings-consuming pool builder passes to create_pool,
    without any code change. (TaskQSettings.load — not load_from_dict —
    so the process environment is actually read; read_dotfiles=False
    keeps the test hermetic against a developer's .env.)
    """
    monkeypatch.setenv("TASKQ_STATEMENT_CACHE_SIZE", "1024")
    monkeypatch.setenv("TASKQ_MAX_CACHED_STATEMENT_LIFETIME", "7200")
    settings = TaskQSettings.load(read_dotfiles=False)

    # statement_cache_kwargs is the splat a settings-aware builder
    # forwards: asyncpg.create_pool(dsn, **statement_cache_kwargs(settings))
    assert statement_cache_kwargs(settings) == {
        "statement_cache_size": 1024,
        "max_cached_statement_lifetime": 7200,
    }

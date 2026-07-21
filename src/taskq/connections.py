"""Connection hook points — bring-your-own resources or factories.

TaskQ constructs its asyncpg pools, dedicated connections, and Redis
client internally from DSN strings by default. This module lets you
replace any of those with either:

1. a **pre-constructed, caller-owned** resource (TaskQ uses it but never
   closes it — you close it in your own lifespan), or
2. a **zero-arg async factory** that TaskQ invokes at the right point in
   its lifecycle and closes the result of on teardown (TaskQ-owned).

Fields left ``None`` fall back to the existing DSN construction, so the
hook points are purely additive.

See the managed-identities deployment guide
(docs/guides/managed-identities.md); :mod:`taskq.auth` provides
vendor-neutral credential providers and factory builders, with
provider-specific implementations in :mod:`taskq.aad`, :mod:`taskq.aws`,
and :mod:`taskq.vault`.

Ownership rule
--------------

* **Pre-constructed** objects are **caller-owned**. TaskQ never closes
  them — close them in your own ``finally`` / lifespan.
* **Factory-produced** objects are **TaskQ-owned**. TaskQ closes them on
  teardown via its :class:`~contextlib.AsyncExitStack`.

Passing both a concrete resource and a factory for the same role is a
configuration error (caught in :meth:`WorkerConnections.__post_init__`).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

__all__ = [
    "ConnFactory",
    "PoolFactory",
    "RedisFactory",
    "WorkerConnections",
]

# ── Factory type aliases (PEP 695) ─────────────────────────────────────
#
# Zero-arg async factories — closures that capture whatever they need
# (DSN, sizing, credentials). The worker invokes them at the right point
# in its startup sequence and closes the result via AsyncExitStack.
# Returning the concrete ``asyncpg`` / ``redis`` types keeps pyright strict
# happy end-to-end. The aliases are lazily resolved by pyright; at runtime
# they are opaque ``TypeAliasType`` objects (never resolved by application code).

type PoolFactory = Callable[[], Awaitable[asyncpg.Pool]]
type ConnFactory = Callable[[], Awaitable[asyncpg.Connection]]
type RedisFactory = Callable[[], Awaitable[redis_async.Redis]]  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; matches WorkerDeps.redis_client typing.


@dataclass(slots=True)
class WorkerConnections:
    """Per-role connection overrides for the worker.

    Each role has a ``<role>`` (pre-constructed, caller-owned) and a
    ``<role>_factory`` (zero-arg async factory, TaskQ-owned) slot.
    Leave both ``None`` for DSN-based construction (the default).

    Example — AAD-managed-identity worker::

        from azure.identity.aio import DefaultAzureCredential
        from taskq.aad import EntraIdProvider
        from taskq.auth import make_pg_pool_factory
        from taskq.connections import WorkerConnections

        cred = DefaultAzureCredential()
        provider = EntraIdProvider(cred)

        connections = WorkerConnections(
            dispatcher_pool_factory=make_pg_pool_factory(
                settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
            ),
            heartbeat_pool_factory=make_pg_pool_factory(
                settings.pg_dsn_direct, provider,
                max_size=settings.heartbeat_pool_size, command_timeout=2,
            ),
            worker_pool_factory=make_pg_pool_factory(
                settings.pg_dsn_pooled, provider, max_size=settings.worker_pool_size,
            ),
        )

    Example — share an app-wide pool (caller-owned)::

        connections = WorkerConnections(worker_pool=app_state.pg_pool)
    """

    # ── Postgres pools ───────────────────────────────────────────────
    dispatcher_pool: asyncpg.Pool | None = None
    """Dispatcher pool (pg_dsn_direct role). Caller-owned if set."""
    dispatcher_pool_factory: PoolFactory | None = None
    """Factory for the dispatcher pool. TaskQ-owned."""

    heartbeat_pool: asyncpg.Pool | None = None
    """Heartbeat pool (pg_dsn_direct, command_timeout=2s). Caller-owned."""
    heartbeat_pool_factory: PoolFactory | None = None
    """Factory for the heartbeat pool. TaskQ-owned. ``command_timeout`` is
    your responsibility when overriding — set it on ``create_pool``."""

    worker_pool: asyncpg.Pool | None = None
    """Worker pool (pg_dsn_pooled role). Caller-owned."""
    worker_pool_factory: PoolFactory | None = None
    """Factory for the worker pool. TaskQ-owned."""

    # ── Postgres dedicated connections ───────────────────────────────
    notify_conn: asyncpg.Connection | None = None
    """Dedicated LISTEN connection. Caller-owned. TaskQ still issues LISTEN."""
    notify_conn_factory: ConnFactory | None = None
    """Factory for the LISTEN connection. TaskQ-owned."""

    leader_conn: asyncpg.Connection | None = None
    """Dedicated advisory-lock connection. Caller-owned."""
    leader_conn_factory: ConnFactory | None = None
    """Factory for the advisory-lock connection. TaskQ-owned."""

    # ── Redis ────────────────────────────────────────────────────────
    redis_client: redis_async.Redis | None = None  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; matches WorkerDeps.redis_client typing.
    """Redis client for progress fanout / rate limiting. Caller-owned."""
    redis_client_factory: RedisFactory | None = None
    """Factory for the Redis client. TaskQ-owned."""

    def __post_init__(self) -> None:
        """Reject concrete + factory for the same role (configuration error)."""
        for concrete, factory in (
            ("dispatcher_pool", "dispatcher_pool_factory"),
            ("heartbeat_pool", "heartbeat_pool_factory"),
            ("worker_pool", "worker_pool_factory"),
            ("notify_conn", "notify_conn_factory"),
            ("leader_conn", "leader_conn_factory"),
            ("redis_client", "redis_client_factory"),
        ):
            if getattr(self, concrete) is not None and getattr(self, factory) is not None:
                raise ValueError(
                    f"WorkerConnections: provide either {concrete!r} or "
                    f"{factory!r}, not both (role would be ambiguous)."
                )

    def has_any(self) -> bool:
        """True if any override (concrete or factory) is set."""
        return any(
            getattr(self, name) is not None
            for name in (
                "dispatcher_pool",
                "dispatcher_pool_factory",
                "heartbeat_pool",
                "heartbeat_pool_factory",
                "worker_pool",
                "worker_pool_factory",
                "notify_conn",
                "notify_conn_factory",
                "leader_conn",
                "leader_conn_factory",
                "redis_client",
                "redis_client_factory",
            )
        )

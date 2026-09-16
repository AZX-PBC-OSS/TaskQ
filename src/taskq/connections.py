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

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.settings import TaskQSettings

__all__ = [
    "DEFAULT_MAX_CACHED_STATEMENT_LIFETIME",
    "DEFAULT_STATEMENT_CACHE_SIZE",
    "ConnFactory",
    "PoolFactory",
    "RedisFactory",
    "WorkerConnections",
    "bounded_lock_budget_ms",
    "connection_init_hook",
    "lock_budget_command_timeout_secs",
    "statement_cache_kwargs",
    "with_connection_init",
]

# ── asyncpg statement-cache defaults ────────────────────────────────────
#
# asyncpg caches prepared statements per connection in an LRU capped by
# ``statement_cache_size`` (asyncpg default: 100 entries) and evicts
# entries older than ``max_cached_statement_lifetime`` seconds (default:
# 300). TaskQ's read paths render far more distinct SQL texts than that:
# ``list_jobs`` emits 384+ filter-combination variants (backend
# ``_filter_sql.py`` + ``_cursor.py``), and the admin pages add more, so a
# client or UI whose filters vary crosses the 100-entry cap and thrashes
# the cache — a measured 90-96% steady-state miss rate, with the eviction
# churn measuring SLOWER than ``statement_cache_size=0`` (1.27 vs
# 0.77 ms/call; ``benchmarks/ab_stmt_cache.py``). Every miss re-pays the
# Parse/Describe round trips: +10-40 ms/call at managed-PG RTT. The write
# hot loop (dispatch, enqueue, terminal updates, sweeps) is textually
# stable and unaffected.
#
# 512 entries covers the rendered-variant space with headroom; the 1-hour
# lifetime keeps long-lived prepared statements from being needlessly
# re-prepared on long-running workers. Every TaskQ-built pool passes both;
# callers bringing their own pools (``WorkerConnections`` factories) should
# set the same two ``create_pool`` kwargs.
DEFAULT_STATEMENT_CACHE_SIZE = 512
DEFAULT_MAX_CACHED_STATEMENT_LIFETIME = 3600


def statement_cache_kwargs(settings: TaskQSettings | None = None) -> dict[str, int]:
    """The ``create_pool`` kwargs carrying TaskQ's statement-cache tuning.

    Bridges :class:`~taskq.settings.TaskQSettings` to
    ``asyncpg.create_pool``: pass a loaded settings instance to read the
    operator-configured values (``TASKQ_STATEMENT_CACHE_SIZE`` /
    ``TASKQ_MAX_CACHED_STATEMENT_LIFETIME``), or ``None`` for the module
    constants above. Either way the result is the pair every TaskQ-built
    pool passes.

    Call sites with a settings instance in scope resolve through this
    helper and forward the two values as **explicit** ``create_pool``
    kwargs (``statement_cache_size=kwargs["statement_cache_size"]`` …):
    pyright strict rejects a ``dict[str, int]`` splat against
    ``asyncpg.create_pool``'s typed keyword-only parameters, and the
    explicit forwarding keeps the call site type-traced. Sites without a
    settings instance (the testing fixtures) pass the constants directly.
    Returns a fresh dict.
    """
    if settings is None:
        return {
            "statement_cache_size": DEFAULT_STATEMENT_CACHE_SIZE,
            "max_cached_statement_lifetime": DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
        }
    return {
        "statement_cache_size": settings.statement_cache_size,
        "max_cached_statement_lifetime": settings.max_cached_statement_lifetime,
    }


#: Fraction of a connection's per-query bound an enqueue lock budget may
#: occupy. The remainder is what the server's refusal needs to be raised,
#: unwound to the savepoint and written back down the wire before the
#: client-side timer fires; a fifth of the bound is generous for a round
#: trip and still leaves the wait dominated by the budget itself.
_LOCK_BUDGET_COMMAND_TIMEOUT_SHARE = 0.8


def bounded_lock_budget_ms(budget_ms: float, command_timeout_secs: float | None) -> float:
    """Clamp one enqueue lock budget to fit inside its connection's bound.

    Each of the enqueue path's bounded waits is enforced server-side by a
    ``lock_timeout`` GUC whose expiry is the point: it produces the typed
    refusal (``MaxPendingLockTimeoutError``, ``UniqueForLockTimeoutError``,
    ``IdempotencyKeyLockTimeoutError``) naming which contention the caller
    lost and that retrying is the right response.

    A budget at or above the connection's own per-query timeout can never
    deliver that. asyncpg starts its client-side timer before the
    ``SET LOCAL lock_timeout`` has even executed, so it always fires
    first: the caller gets a bare ``TimeoutError`` naming nothing, the
    warning line never logs, the backpressure counter never moves, and
    the typed refusal is unreachable code. Clamping is what makes the
    configured budget mean what it says — an unclamped budget is not a
    longer wait, it is no verdict at all.

    ``command_timeout_secs`` of ``None`` is a connection with no
    client-side bound, so the budget stands as configured; so does a
    budget of ``0`` or less, which is the operator asking for an
    unbounded wait (the ``lock_timeout`` GUC convention).
    """
    if command_timeout_secs is None or command_timeout_secs <= 0 or budget_ms <= 0:
        return budget_ms
    return min(budget_ms, command_timeout_secs * 1000.0 * _LOCK_BUDGET_COMMAND_TIMEOUT_SHARE)


def lock_budget_command_timeout_secs(
    budgets_ms: Iterable[tuple[float, float]],
    *,
    floor_secs: float,
) -> float:
    """The per-query bound a TaskQ-built pool must carry to deliver its
    configured enqueue lock budgets.

    *floor_secs* is the bound sized for the shipped defaults — at the
    defaults each budget is delivered clamped to the floor's
    :data:`_LOCK_BUDGET_COMMAND_TIMEOUT_SHARE` share by
    :func:`bounded_lock_budget_ms`, so the floor is exactly what the
    pre-knob pool carried and a deployment that sets nothing keeps it.

    Each pair in *budgets_ms* is ``(configured_ms, default_ms)`` for one
    knob. A budget configured ABOVE its shipped default cannot be
    delivered inside the floor — the clamp would silently cap it back —
    so the bound is re-derived as ``configured / share``: the widened
    budget occupies the same share of a larger bound, the clamp no
    longer bites, and the server-side ``lock_timeout`` still fires
    before the client-side timer (the race the clamp exists to win).
    Budgets at or below their default never move the bound, and neither
    does a non-positive one: an unbounded lock wait cannot fit inside
    any finite bound, and dropping the pool's per-query bound to honor
    one would remove the black-hole guard every other query relies on.
    """
    bound = floor_secs
    for configured_ms, default_ms in budgets_ms:
        if configured_ms > default_ms:
            bound = max(bound, configured_ms / 1000.0 / _LOCK_BUDGET_COMMAND_TIMEOUT_SHARE)
    return bound


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


# ── Inheritable per-connection init hooks ────────────────────────────
#
# A connection built by a bare factory is opaque: anything applied to it
# after connect — a ``set_type_codec`` registration, a prepared-statement
# warmup — lives in the driver's per-connection state, which exposes no
# read-back, so a SECOND connection the worker builds for the same role
# cannot replay it. The per-slot transaction pool is exactly such a
# second connection family: it shadows the LOOP-registered
# ``asyncpg.Connection`` once ``max_concurrency > 1``
# (``taskq.worker._bootstrap._maybe_open_slot_pool``). A registration
# whose hook is DECLARED on the factory, though, is replayable: the
# worker reads the hook off the registration and installs it as the
# slot pool's ``init``, so the codec the application registered decodes
# identically at every concurrency. ``with_connection_init`` is the
# declaring wrapper for a hand-rolled factory;
# :func:`taskq.auth.make_dedicated_conn_factory` declares its ``setup``
# hook the same way.

#: Attribute a factory sets to declare the per-connection init hook it
#: applies. Private on purpose: the name is an implementation detail
#: shared by the writers (``with_connection_init``,
#: ``make_dedicated_conn_factory``) and the single reader below — apps
#: declare hooks through those, never by setting the attribute directly.
_CONNECTION_INIT_HOOK_ATTR: Final[str] = "taskq_connection_init_hook"


def with_connection_init(
    factory: ConnFactory,
    init: Callable[[asyncpg.Connection], Awaitable[None]],
) -> ConnFactory:
    """Wrap a connection factory so *init* runs on every connection it
    opens — and so the worker's per-slot pool can inherit the same hook.

    *init* is applied to each produced connection exactly once, right
    after the factory returns it — the ``setup=`` hook position in
    ``asyncpg.connect``. This is the channel through which per-connection
    setup (registering type codecs, preparing statements) reaches the
    connections an actor actually runs on: when the wrapped factory
    provides the LOOP-scope ``asyncpg.Connection`` registration and the
    worker activates its per-slot transaction pool, the worker reads the
    declared hook off the registration and passes it as the slot pool's
    ``init``, so every slot connection gets the same setup. Applied
    directly to one live connection instead, the same setup is invisible
    to the slot pool (the driver exposes no read-back) and silently
    absent above ``max_concurrency = 1``.

    Declare the hook here, not inside *factory* as well: the wrapper
    applies it, so a factory that also applies it would run it twice on
    the LOOP-scope connection. If *init* raises, the produced connection
    is closed (bounded) before the error propagates — a failed hook
    means no usable connection, matching asyncpg's own hook contract.
    """

    # Why no return annotation: DI registers factories like this one and
    # resolves type hints at registration time (``_collect_dep_edges``),
    # and this module keeps asyncpg under TYPE_CHECKING — a runtime-
    # evaluated ``-> asyncpg.Connection`` would raise NameError there.
    # The declared ``ConnFactory`` return type of this wrapper keeps the
    # boundary typed; pyright infers the body.
    async def _wrapped():
        conn = await factory()
        try:
            await init(conn)
        except BaseException:
            # Why deferred: this module is import-light by design (no
            # taskq runtime imports above), and the failure path is cold.
            # Why bounded: a hook failure during DI bootstrap must not
            # leak the connection, and a dead PG must not turn that
            # cleanup into a wedge. The helper never raises, so the
            # hook's own error is always the one that propagates.
            from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded

            await close_conn_bounded(conn, "with-connection-init", CLOSE_TIMEOUT_SECS)
            raise
        return conn

    setattr(_wrapped, _CONNECTION_INIT_HOOK_ATTR, init)
    return _wrapped


def connection_init_hook(
    factory: object,
) -> Callable[[asyncpg.Connection], Awaitable[None]] | None:
    """The init hook *factory* declares, if it declares one.

    The read half of the inheritance channel — public so an application
    that hand-rolls its factories can verify its declaration is visible
    the way the worker sees it. Returns the hook exactly as declared (so
    the slot pool installs the very callable the LOOP-scope connection
    got), or ``None`` when the factory carries no declaration — a bare
    closure, a raw ``asyncpg.connect`` partial — in which case nothing
    about its per-connection setup is recoverable.
    """
    hook = getattr(factory, _CONNECTION_INIT_HOOK_ATTR, None)
    if not callable(hook):
        return None
    # Why the string form: asyncpg is TYPE_CHECKING-only in this module,
    # and cast() evaluates its type argument at runtime. The attribute is
    # write-only from the two typed declaring sites above; getattr erases
    # that, so the cast restores the declared contract.
    return cast("Callable[[asyncpg.Connection], Awaitable[None]]", hook)


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
                max_size=settings.heartbeat_pool_size,
                command_timeout=settings.heartbeat_command_timeout,
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
    """Heartbeat pool (pg_dsn_direct, heartbeat_command_timeout). Caller-owned."""
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

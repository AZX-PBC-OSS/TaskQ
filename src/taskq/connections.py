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

from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

if TYPE_CHECKING:
    import asyncpg
    import asyncpg.pool
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

    When the settings declare a transaction-mode pooler
    (``TASKQ_PG_IS_POOLED``, field ``pg_is_pooled``) the pair is
    ``0``/``0`` regardless of the operator's cache tuning: a pooler that
    remaps server connections between statements can split a prepared
    statement's Parse from its Bind, and asyncpg's per-connection cache
    has no way to notice - the only safe cache size is zero. The knob
    deliberately wins over ``TASKQ_STATEMENT_CACHE_SIZE``: a nonzero
    cache under a remapping pooler is broken by definition, not a
    tuning trade-off.

    Call sites with a settings instance in scope resolve through this
    helper and forward the two values as **explicit** ``create_pool``
    kwargs (``statement_cache_size=kwargs["statement_cache_size"]`` …):
    pyright strict rejects a ``dict[str, int]`` splat against
    ``asyncpg.create_pool``'s typed keyword-only parameters, and the
    explicit forwarding keeps the call site type-traced. Sites without a
    settings instance (the testing fixtures) pass the constants directly.
    Returns a fresh dict.
    """
    if settings is not None and settings.pg_is_pooled:
        return {"statement_cache_size": 0, "max_cached_statement_lifetime": 0}
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


#: Bound on the pool-release path the retry guard's checkout owns: the
#: release-time ``reset()`` (``pg_advisory_unlock_all(); CLOSE ALL;
#: UNLISTEN *; RESET ALL;``) plus, on the max-queries/generation paths,
#: a graceful close. asyncpg's acquire-context release passes NO timeout
#: (the holder falls back to the *acquire* timeout, which is ``None``
#: whenever acquire was unbounded), so a server that dies silently,
#: no FATAL, no FIN: a frozen/black-holed endpoint, parks that reset
#: forever, wedging the caller's task AND ``pool.close()`` (issue #236's
#: hang half). Five seconds matches the repo-wide teardown bound
#: (``taskq._close.CLOSE_TIMEOUT_SECS``): the reset is sub-millisecond
#: on a live server, so the bound only ever fires against a dead one,
#: where asyncpg's timeout handler terminates the connection and frees
#: the holder, and the pool reopens a fresh connection on the next acquire.
#: Module-level so tests shrink it as a seam (the ``CLOSE_TIMEOUT_SECS``
#: convention).
_POOL_RELEASE_RESET_TIMEOUT_SECS: Final[float] = 5.0


class _RetryGuard:
    """One attempt's pool discipline, handed to the op by
    :func:`_with_fresh_connection_retry`.

    Two channels, one object:

    * :meth:`checkout`: the bounded acquire/release the op runs its
      statements inside. Replaces a bare ``async with pool.acquire()``:
      the acquire is unchanged (unbounded; pool-exhaustion waits are the
      pool's own backpressure, not this guard's to cut short), but the
      RELEASE carries :data:`_POOL_RELEASE_RESET_TIMEOUT_SECS` and never
      raises: a reset that fails or times out is pool hygiene, not part
      of the op's semantics. asyncpg's release path already terminates
      the connection on any reset failure, so swallowing costs nothing
      but a log line, and it buys two #236 fixes at once: an op whose
      work committed but whose release hit a parked/dead connection
      returns its RESULT instead of an error that invites a
      duplicate-on-retry, and a reset sent into a silently-dead server
      times out instead of parking the caller (and ``pool.close()``)
      forever.
    * :meth:`mark_wrote`: the durability flag the retry decision reads.
      The op calls it immediately after the first point at which a write
      has become DURABLE: an autocommit statement's acknowledgement, or
      the transaction COMMIT's acknowledgement (see the wrapper's
      docstring for why after-the-ack, not before-the-write).

    Not thread-safe and not reusable across attempts: the wrapper builds
    a fresh guard per attempt, so a retry's flag starts clear.
    """

    __slots__ = ("_operation", "_pool", "wrote")

    def __init__(self, pool: asyncpg.Pool, operation: str) -> None:
        self._pool = pool
        self._operation = operation
        self.wrote = False

    def mark_wrote(self) -> None:
        """Record that this attempt has made a write durable.

        Idempotent and one-way: once a write is acknowledged, no later
        event in the same attempt can un-commit it.
        """
        self.wrote = True

    def checkout(self) -> AbstractAsyncContextManager[asyncpg.pool.PoolConnectionProxy]:
        """``async with guard.checkout() as conn:``: acquire, run, bounded
        release.

        The release is bounded and never raises (see the class docstring);
        whatever the body raised or returned is what escapes the context
        manager.
        """
        return _bounded_checkout(self._pool, self._operation)


@asynccontextmanager
async def _bounded_checkout(
    pool: asyncpg.Pool,
    operation: str,
    *,
    acquire_timeout: float | None = None,
) -> AsyncGenerator[asyncpg.pool.PoolConnectionProxy, None]:
    """The bounded acquire/release one attempt's statements run inside:
    :meth:`_RetryGuard.checkout`'s implementation, shared with the read,
    schedule, and batch paths that run no retry wrapper and so have no
    guard of their own (issue #280's sweep).

    Replaces a bare ``async with pool.acquire()``: the acquire is
    unchanged unless *acquire_timeout* is given (a site that already
    bounded its acquire, such as the notify-pool sweeps' dispatcher
    command timeout, keeps that bound verbatim), but the RELEASE carries
    :data:`_POOL_RELEASE_RESET_TIMEOUT_SECS` and never raises: a reset
    that fails or times out is pool hygiene, not part of the op's
    semantics. asyncpg's release path already terminates the connection
    on any reset failure, so swallowing costs nothing but a log line, and
    it buys two #236 fixes at once: an op whose work committed but whose
    release hit a parked/dead connection returns its RESULT instead of an
    error that invites a duplicate-on-retry, and a reset sent into a
    silently-dead server times out instead of parking the caller (and
    ``pool.close()``) forever.

    *operation* names the call site in the ``pool-release-failed``
    WARNING, the operator's one observable trace of the swallowed
    failure.
    """
    acquired = (
        pool.acquire(timeout=acquire_timeout) if acquire_timeout is not None else pool.acquire()
    )
    if isinstance(acquired, Awaitable):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the stubs type asyncpg's acquire() as always-awaitable (PoolAcquireContext), so a real pool only ever takes this arm; the else arm exists because test doubles model only the context-manager half of acquire()'s documented dual surface (see below).
        # asyncpg's acquire() is documented as BOTH awaitable and an
        # async context manager; the await form is the one that lets
        # the release below carry its own timeout: the context
        # manager's __aexit__ releases with no timeout, so the holder
        # falls back to the (unbounded) acquire timeout instead.
        conn = await acquired
        try:
            yield conn
        finally:
            try:
                await pool.release(conn, timeout=_POOL_RELEASE_RESET_TIMEOUT_SECS)
            except Exception as exc:
                # Why swallow: the holder's own release path terminates
                # the connection on any reset failure (asyncpg pool.py),
                # so the pool is already consistent: raising would
                # either mask the op's real outcome with pool hygiene
                # (on the error path) or hand the caller a failure for
                # work that committed (on the success path), which is
                # precisely the duplicate-invitation #236 exists to
                # remove.
                from taskq.obs import get_logger

                get_logger(__name__).warning(
                    "pool-release-failed",
                    kind="pool_release_failed",
                    operation=operation,
                    error=repr(exc),
                )
    else:
        # A stand-in pool that models only the context-manager half of
        # acquire()'s documented surface (an ``@asynccontextmanager``
        # acquire, the common test-double shape): no network exists to
        # hang a release on, so the context's own release is used
        # verbatim and no timeout is imposed. Unreachable for a real
        # asyncpg pool (the stubs' always-awaitable view), which is
        # what makes the cast safe.
        cm = cast(
            "AbstractAsyncContextManager[asyncpg.pool.PoolConnectionProxy]",
            acquired,
        )
        async with cm as conn:
            yield conn


async def _with_fresh_connection_retry[T](  # pyright: ignore[reportUnusedFunction]  # Why: the shared dead-on-acquire guard — its callers are the enqueue and bulk-cancel modules; private usage is declared at each import site.
    pool: asyncpg.Pool,
    op: Callable[[_RetryGuard], Awaitable[T]],
    *,
    operation: str,
) -> T:
    """Run *op* once, retrying once when the pool hands out a just-killed
    connection, and only when no write has become durable yet.

    Shared home for every "acquire from the pool, use immediately" caller,
    the enqueue paths and the bulk-cancel drain, so the recovery
    discipline exists once instead of per call site. *op* receives a
    :class:`_RetryGuard` and must run every pool checkout through
    ``guard.checkout()`` (bounded release, release errors never raised)
    and call ``guard.mark_wrote()`` at its first durability point.

    Why the retry exists: a pooled connection whose backend Postgres has
    terminated (restart, failover, ``pg_terminate_backend``) learns of its
    death in two event-loop steps: the server's FATAL ErrorResponse arrives
    first and, with no in-flight query to attribute it to, parks
    asyncpg's protocol in its error-consume state; only a later
    ``connection_lost`` callback marks the connection closed. In the gap,
    ``Pool.acquire``'s ``is_closed()`` guard still passes, so the pool can
    hand a caller a connection whose first statement fails locally with
    ``asyncpg.InternalClientError`` ("cannot switch to state 15; another
    operation (2) is in progress"): a driver-internal state error that
    matches no except clause written against the database's own error
    types, for a condition that is physically a dropped connection. The
    boundary treats that first-statement failure as what it is, a
    transient connection loss, and retries once. The retry always lands
    on a genuinely fresh connection: releasing the poisoned one cannot
    complete (its reset hits the same parked state), the release path
    terminates it, and the next acquire reconnects.

    When the retry is REFUSED (``guard.wrote``): an ``InternalClientError``
    raised by the op *after* the attempt marked a write durable means the
    connection died between that write's acknowledgement and a LATER
    statement of the same attempt (a post-INSERT read, a savepoint
    RELEASE, a COMMIT-adjacent restore). Re-running the op would re-issue
    the write with the same identity against a fresh connection, so the
    error propagates instead. What that re-run actually produces (the
    enqueue table's ``id`` is the primary key, and the op's args carry
    their id): a ``UniqueViolationError`` for an enqueue that already
    committed and will run: an error handed back for work that
    SUCCEEDED, which is the first half of #236's harm. The second half is
    the invitation that error creates: a caller that retries it generates
    a fresh id (a new enqueue call does), and THAT row lands: the actual
    job-runs-twice route, and why idempotency keys remain the dedup
    channel for the retry the caller chooses. The marker is set AFTER the
    write's acknowledgement, not before the write is issued, on purpose:
    the dead-on-acquire case this wrapper exists for can strike the write
    statement itself (the plain enqueue arm's INSERT is its first
    statement), and a locally-poisoned protocol rejects that statement
    BEFORE anything reaches the server, unmarked, so the retry runs and
    nothing can conflict. Marking before the write would refuse that
    retry and regress the wrapper's whole purpose; the only marking point
    that is correct for both orderings is the acknowledgement.

    What the retry does NOT have to gate: a release-time failure after the
    op's body finished. The guard's checkout bounds the release and never
    raises it (asyncpg terminates the connection either way), so the op's
    result stands: a caller whose enqueue committed gets its row, not an
    error inviting a re-enqueue. Only errors raised by the op's own
    statements reach this wrapper's catch.

    An ``InternalClientError`` from the retry is a real driver state bug,
    not this race, and propagates.
    """
    # Why deferred: this module is import-light by design (no asyncpg or
    # taskq runtime imports at module scope), and both names serve only
    # this cold path — the same discipline ``with_connection_init``
    # follows for its close helper.
    from asyncpg.exceptions import InternalClientError

    guard = _RetryGuard(pool, operation)
    try:
        return await op(guard)
    except InternalClientError as exc:
        if guard.wrote:
            # A write from this attempt is already acknowledged; re-running
            # op would re-issue it with the same identity: against the
            # enqueue table's primary key that is a UniqueViolationError
            # for work that succeeded, and the error invites the caller's
            # fresh-id retry that DOES run the job twice (#236). The
            # driver error for a committed write is ambiguous, but never
            # that invitation; idempotency keys remain the caller's dedup
            # channel for the retry THEY choose to issue.
            raise
        from taskq.obs import get_logger

        get_logger(__name__).warning(
            "pool-conn-dead-on-acquire",
            kind="pool_conn_dead_on_acquire",
            operation=operation,
            error=repr(exc),
        )
        return await op(_RetryGuard(pool, operation))


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

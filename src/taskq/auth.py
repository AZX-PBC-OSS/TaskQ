"""Vendor-neutral credential providers and connection factories.

This module provides the reusable primitives for **rotating-credential**
Postgres and Redis connections - the abstract interfaces that any auth
provider (Azure Entra ID, AWS IAM RDS, HashiCorp Vault, a custom OAuth
flow, a secrets manager, …) plugs into. Provider-specific implementations
live in the ``taskq[aad]``, ``taskq[aws]``, and ``taskq[vault]`` extras;
users with other providers implement :class:`PgCredentialProvider` /
:class:`RedisCredentialProvider` directly and get all the factory
builders for free.

See the managed-identities deployment guide (docs/guides/managed-identities.md).

Design
------

* :class:`PgCredentialProvider` - async Protocol returning a
  :class:`PgCredential` (a password, optionally a fresh username). AAD
  and AWS IAM RDS return a token-as-password; Vault dynamic DB creds
  return a fresh username + password pair.
* :class:`RedisCredentialProvider` - async Protocol returning a
  :class:`RedisCredential` (username + password). AAD returns the
  managed-identity object ID + token.
* :func:`make_pg_pool_factory` / :func:`make_dedicated_conn_factory` /
  :func:`make_redis_client_factory` - accept any provider implementing
  the Protocol and return the zero-arg async factories that
  :class:`~taskq.connections.WorkerConnections` consumes. Credentials
  are passed to asyncpg as ``user=`` / ``password=`` keyword arguments
  (which take precedence over both DSN userinfo and query parameters),
  so the token never appears in the DSN string.
* :func:`enrich_pg_dsn` - shared DSN helper for callers that need a
  self-contained DSN string: the credential is written into the DSN
  userinfo (the only slot asyncpg's resolver never shadows) and
  ``sslmode=require`` is added only when no sslmode is already set.

Credential refresh
------------------

Both transports re-fetch on every physical (re)connect, so no external
rotation schedule is needed:

* Postgres - ``password=`` is handed to asyncpg as an async callable,
  which asyncpg awaits once per physical connection (pool creation, pool
  growth, and replacements after ``max_inactive_connection_lifetime``
  recycles an idle connection).
* Redis - reconnects re-fetch via the redis-py ``CredentialProvider``
  adapter.

The one thing that cannot refresh in place is a **username-bearing
pair**: asyncpg resolves ``user=`` once per pool / connection and accepts
a callable only for ``password=``, and a dynamic username is only valid
with the password issued alongside it. Providers that issue a fresh
username per credential (Vault dynamic database credentials) therefore
pin the pair for the pool's life - the callable hands asyncpg the pair's
password - and rotate on the pool rebuild that ``SIGHUP`` /
``TASKQ_RELOAD_INTERVAL`` / ``taskq.worker.deps.reload_credentials``
performs. The rebuild cadence is a :class:`ReloadSchedule`: the
operator's explicit interval when one is set, otherwise derived from the
lease the provider granted (``PgCredential.lease_duration``) at
:data:`LEASE_RELOAD_FRACTION` of the TTL, so a pair is replaced with a
full half-life to spare. Every pool builder records the leases it is
issued on its schedule, and every consumer that rebuilds pools (the
worker, ``taskq ui serve``, :class:`taskq.TaskQ`) reads the interval
from it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from taskq.connections import (
    _CONNECTION_INIT_HOOK_ATTR,  # pyright: ignore[reportPrivateUsage]  # Why: the attribute name is owned by taskq.connections; the declaring writers share the single constant so the worker-side reader can never drift from them.
    DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    DEFAULT_STATEMENT_CACHE_SIZE,
    ConnFactory,
    PoolFactory,
    RedisFactory,
    WorkerConnections,
    statement_cache_kwargs,
)
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

    from taskq.settings import WorkerSettings

logger = get_logger(__name__)

__all__ = [
    "LEASE_RELOAD_FRACTION",
    "PgCredential",
    "PgCredentialProvider",
    "RedisCredential",
    "RedisCredentialProvider",
    "ReloadSchedule",
    "build_worker_connections",
    "enrich_pg_dsn",
    "ensure_sslmode_require",
    "make_dedicated_conn_factory",
    "make_pg_pool_factory",
    "make_redis_client_factory",
    "reload_schedule_of",
]


# --- Credential data carriers ---


@dataclass(frozen=True, slots=True)
class PgCredential:
    """A Postgres credential issued by a rotating-credential provider.

    ``password`` is always required (a token or dynamic password).
    ``username``, when set, overrides the DSN's userinfo user - needed by
    providers that issue a fresh username alongside the password (e.g.
    Vault dynamic DB creds). When ``None``, the DSN's existing user is
    preserved.

    ``lease_duration`` is how long, in seconds, the issuer will honour this
    credential - the Vault lease TTL. It is the bound a rebuild schedule
    has to beat for a username-bearing pair (see :class:`ReloadSchedule`),
    and ``None`` when the issuer does not say (token providers, whose
    tokens refresh per connection and never need one).
    """

    # Why repr=False: the default dataclass repr embeds the token a provider
    # just fetched - the exact credential this module exists to keep out of
    # DSN strings (see make_pg_pool_factory); repr'd into a log or debugger
    # is the same leak. field() adds no default, so construction is
    # unchanged. username stays repr-able: a principal name, not a secret.
    password: str = field(repr=False)
    username: str | None = None
    lease_duration: float | None = None

    def __post_init__(self) -> None:
        if self.lease_duration is not None and self.lease_duration <= 0:
            raise ValueError(
                f"lease_duration must be a positive number of seconds, got {self.lease_duration!r}"
            )


@dataclass(frozen=True, slots=True)
class RedisCredential:
    """A Redis credential issued by a rotating-credential provider."""

    username: str
    # Why repr=False: same masking rationale as PgCredential.password - the
    # bearer token / password must not survive a repr; username (the
    # managed-identity object ID / principal) is not a credential.
    password: str = field(repr=False)


# --- Provider protocols ---


@runtime_checkable
class PgCredentialProvider(Protocol):
    """Provides rotating Postgres credentials on demand.

    Implementations fetch a fresh token / dynamic username+password each
    call. Called by :func:`make_pg_pool_factory` /
    :func:`make_dedicated_conn_factory` once at pool / connection
    construction (to resolve ``user=`` and fail fast). A credential with
    ``username`` unset is then re-fetched for every **physical**
    connection asyncpg opens thereafter - not on each ``acquire()``, which
    hands back an already-authenticated connection from the pool - so
    token providers are expected to cache and only hit the issuing service
    when the cached token is near expiry. A credential that carries a
    ``username`` is an issued pair used for the pool's life and is fetched
    again only when the pool is rebuilt.
    """

    async def get_pg_credential(self) -> PgCredential:
        """Return a fresh :class:`PgCredential`."""
        ...


@runtime_checkable
class RedisCredentialProvider(Protocol):
    """Provides rotating Redis credentials on demand.

    Implementations fetch a fresh (username, token/password) each call.
    Called by :func:`make_redis_client_factory` on every reconnect via
    the redis-py ``CredentialProvider`` adapter.
    """

    async def get_redis_credential(self) -> RedisCredential:
        """Return a fresh :class:`RedisCredential`."""
        ...


# --- DSN enrichment ---


def ensure_sslmode_require(dsn: str) -> str:
    """Add ``sslmode=require`` to *dsn* unless an sslmode is already set.

    An explicit sslmode is never overridden - in particular stronger
    modes (``verify-ca`` / ``verify-full``) must not be downgraded:
    ``require`` skips certificate verification, which would expose the
    very token this module injects to a MITM.

    Public because anyone assembling a credential-bearing DSN by hand needs
    exactly this rule and must not re-derive it: a token path that silently
    connects without TLS puts the credential on the wire. The factory
    builders in this module apply it for you; reach for it directly only on
    the DSN paths they do not cover (a raw ``asyncpg.connect``, a migration
    connection, a DSN handed to another library).

    ``sslmode=disable`` is an explicit choice and is preserved - that is how
    a test container or a Unix-socket deployment opts out.

    Because an explicit ``verify-ca``/``verify-full`` is passed through
    untouched, it reaches asyncpg needing a CA bundle that this helper does
    not supply: asyncpg and libpq do NOT fall back to the system trust store
    for a verifying sslmode, they look for ``~/.postgresql/root.crt`` and
    raise ``ClientConfigurationError`` while *parsing connection arguments*
    (before any socket is opened) when it is absent - which is the normal
    case in a container. Such a DSN must carry its own ``sslrootcert=``
    (or set ``PGSSLROOTCERT``); see the *sslmode* note in
    ``docs/guides/managed-identities.md``. Deliberately not defaulted here:
    picking a trust root is a security decision and the correct bundle is
    environment-specific.
    """
    parsed = urlparse(str(dsn))
    query = parse_qs(parsed.query, keep_blank_values=True)
    if "sslmode" in query:
        return str(dsn)
    query["sslmode"] = ["require"]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def enrich_pg_dsn(dsn: str, credential: PgCredential) -> str:
    """Apply *credential* to *dsn* and return a self-contained DSN string.

    The credential is written into the DSN **userinfo** (percent-encoded),
    replacing any existing userinfo password - and replacing the userinfo
    user when ``credential.username`` is set (Vault dynamic DB creds).
    This is the only slot that is guaranteed to take effect: asyncpg's
    resolver applies userinfo *before* query parameters (both behind
    ``if user is None`` / ``if password is None`` guards), so a
    query-string ``user=`` / ``password=`` is silently ignored whenever
    the DSN already carries userinfo. A stale ``password=`` query
    parameter is dropped (always shadowed by the userinfo password);
    a ``user=`` query parameter is dropped only when the userinfo
    carries a user to shadow it - a query-carried user with no userinfo
    user is the effective principal and is preserved.

    ``sslmode=require`` is added only when the DSN has no explicit
    sslmode, so stronger modes (``verify-full``) are never downgraded.

    Prefer the factory builders (:func:`make_pg_pool_factory` /
    :func:`make_dedicated_conn_factory`) where possible - they pass the
    credential as keyword arguments instead, keeping the token out of
    the DSN string entirely.
    """
    parsed = urlparse(str(dsn))
    query = parse_qs(parsed.query, keep_blank_values=True)
    query.pop("password", None)

    if "@" in parsed.netloc:
        auth, _, hostspec = parsed.netloc.partition("@")
    else:
        auth, hostspec = "", parsed.netloc
    user, _, _old_password = auth.partition(":")
    if credential.username is not None:
        user = quote(credential.username, safe="")
    if user:
        # The userinfo will carry a user, which shadows any query user= in
        # asyncpg's resolver - drop the stale query copy. When the userinfo
        # has NO user (credential.username unset, none in the DSN), a query
        # user= is the effective principal and must be preserved.
        query.pop("user", None)
    netloc = f"{user}:{quote(credential.password, safe='')}@{hostspec}"

    query.setdefault("sslmode", ["require"])
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(netloc=netloc, query=new_query))


# --- Per-connection credential refresh ---


def _make_pg_password_callable(
    provider: PgCredentialProvider,
    *,
    pinned: PgCredential,
    role: str,
) -> Callable[[], Awaitable[str]]:
    """Build the ``password=`` callable asyncpg invokes per physical connection.

    asyncpg resolves a callable ``password`` inside ``_connect_addr``, which
    runs for **every** physical connection - those opened when the pool is
    created, those opened later by pool growth, and the replacements opened
    after ``max_inactive_connection_lifetime`` recycles an idle connection. It
    awaits the result when the callable returns an awaitable, so an async
    provider is called directly with no thread bridge. asyncpg also retains the
    *original* parameters (callable intact) for its SSL-mode retry path, so the
    callable is never collapsed into a one-shot string.

    This is what makes rotating credentials work without external rotation.
    Postgres authenticates at connect time only, so a token baked in as a fixed
    string keeps working on already-open connections and fails on every new one
    roughly one token-lifetime after deploy - green at rollout, dead hours
    later.

    *pinned* is the credential the pool / connection was built with. Which of
    its two shapes it has decides what the callable does per connection:

    * ``username is None`` (Entra ID, AWS IAM RDS): the principal is the DSN
      user and only the token rotates, so every physical connection re-fetches
      and authenticates with the current token.
    * ``username`` set (Vault dynamic database credentials): the username and
      password were **issued together as one lease** and are only valid as a
      pair. asyncpg's ``user=`` is not callable - it is resolved once in
      ``_parse_connect_arguments`` - so the pool is pinned to that username for
      its life, and the only password that can ever authenticate it is the
      pair's. Re-fetching here would burn a fresh lease per physical
      connection and hand asyncpg a password for a username the pool was never
      built with. The callable therefore returns the pinned pair's password;
      the pair is replaced when the factory is re-invoked (``SIGHUP`` /
      ``reload_credentials`` rebuilding the pool), which is where a lease
      rotates.
    """
    if pinned.username is not None:
        pair_password = pinned.password

        async def _pinned_pair_password() -> str:
            return pair_password

        return _pinned_pair_password

    async def _fetch_password() -> str:
        try:
            credential = await provider.get_pg_credential()
        except Exception as exc:
            # Re-raised unchanged so the provider's own exception type and
            # traceback survive for the operator; asyncpg propagates it out of
            # create_pool()/connect() as a connection failure rather than
            # retrying or falling back to an unauthenticated connection.
            logger.error(
                "pg-credential-refresh-failed",
                role=role,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise
        return credential.password

    return _fetch_password


# --- Reload schedule ---


LEASE_RELOAD_FRACTION: Final[float] = 0.5
"""Fraction of a granted lease TTL at which a pinned pair is rebuilt.

Half the TTL leaves a full half-life for the rebuild to fail and be retried
before the issuer revokes the pair: a rebuild that fails at ``T + TTL/2``
still has until ``T + TTL`` before every reconnect on the old pool starts
failing authentication. Mirrors the renew-at-half-life rule Vault's own
agent applies to its leases."""

_RELOAD_SCHEDULE_ATTR: Final[str] = "taskq_reload_schedule"


@dataclass(slots=True, eq=False)
class ReloadSchedule:
    """How often pools built through a credential provider are rebuilt.

    A username-bearing credential pins its pool to one issued pair (see
    :func:`_make_pg_password_callable`), so the only rotation is a pool
    rebuild, and the rebuild has to happen before the issuer revokes the
    pair. This object is the single source of that cadence:

    * ``configured`` is the operator's explicit interval
      (``TASKQ_RELOAD_INTERVAL`` on the worker and ``taskq ui serve``,
      ``reload_interval=`` on :class:`taskq.TaskQ`). It always wins.
    * Otherwise the interval is **derived from the granted lease**: the
      pool builders record ``PgCredential.lease_duration`` here as each
      credential is issued, and :attr:`interval` is the shortest lease
      seen so far scaled by :data:`LEASE_RELOAD_FRACTION`. Shortest, not
      latest, because Vault caps a lease at the issuing token's remaining
      TTL - a lease that came back shorter than the last is the bound that
      now has to be beaten.
    * ``None`` when nothing is configured and no issued credential carried
      a lease - there is nothing to schedule, and a username-bearing
      credential built against such a schedule warns at build time
      (``pg-lease-pair-pinned-without-reload``).

    ``sources`` composes schedules: a consumer that rebuilds several
    factories (the worker's role pools and dedicated connections) reads
    one schedule whose lease is the shortest across all of them, and whose
    ``configured`` is its own. The composite is live - a lease recorded on
    a source after composition is seen through it.

    One schedule is normally shared by every factory a consumer rebuilds
    (:func:`build_worker_connections` does this); a factory built without
    one declares its own, which :func:`reload_schedule_of` returns so a
    consumer handed an opaque factory can still adopt it.
    """

    configured: float | None = None
    sources: tuple[ReloadSchedule, ...] = ()
    _lease_duration: float | None = field(default=None, init=False)
    _pins_pair: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if self.configured is not None and self.configured <= 0:
            raise ValueError(
                f"configured reload interval must be a positive number of seconds, "
                f"got {self.configured!r}"
            )

    def record(self, credential: PgCredential) -> None:
        """Note a credential a factory on this schedule was just issued."""
        if credential.username is not None:
            self._pins_pair = True
        if credential.lease_duration is not None:
            self._lease_duration = (
                credential.lease_duration
                if self._lease_duration is None
                else min(self._lease_duration, credential.lease_duration)
            )

    @property
    def lease_duration(self) -> float | None:
        """Shortest lease TTL recorded here or on any source, in seconds."""
        leases = [self._lease_duration, *(source.lease_duration for source in self.sources)]
        known = [lease for lease in leases if lease is not None]
        return min(known) if known else None

    @property
    def pins_pair(self) -> bool:
        """Whether a username-bearing credential was issued through this
        schedule or any source - i.e. whether anything actually needs the
        rebuild."""
        return self._pins_pair or any(source.pins_pair for source in self.sources)

    @property
    def interval(self) -> float | None:
        """Seconds between rebuilds: ``configured``, else the derived
        lease-based interval, else ``None``."""
        if self.configured is not None:
            return self.configured
        lease = self.lease_duration
        return None if lease is None else lease * LEASE_RELOAD_FRACTION

    @property
    def derived(self) -> bool:
        """True when :attr:`interval` comes from a granted lease rather
        than an explicit setting."""
        return self.configured is None and self.lease_duration is not None


def reload_schedule_of(factory: object) -> ReloadSchedule | None:
    """The :class:`ReloadSchedule` a pool / connection factory declares.

    Every factory :func:`make_pg_pool_factory` and
    :func:`make_dedicated_conn_factory` return declares the schedule it
    records leases on; a consumer handed an opaque factory (``TaskQ(
    pool_factory=...)``, a hand-assembled ``WorkerConnections``) reads it
    here to derive its rebuild cadence. ``None`` for a factory built some
    other way - such a factory has no lease to report.
    """
    schedule = getattr(factory, _RELOAD_SCHEDULE_ATTR, None)
    return schedule if isinstance(schedule, ReloadSchedule) else None


def _declare_reload_schedule[F: Callable[..., Any]](factory: F, schedule: ReloadSchedule) -> F:
    setattr(factory, _RELOAD_SCHEDULE_ATTR, schedule)
    return factory


def _record_issued_credential(
    schedule: ReloadSchedule,
    credential: PgCredential,
    *,
    role: str,
    warn_without_schedule: bool,
) -> None:
    """Record *credential* on *schedule* and report what that means for rotation.

    A credential that carries a ``username`` is one issued lease pair, and
    asyncpg resolves ``user=`` once per pool, so the pool is pinned to that
    pair for its life (see :func:`_make_pg_password_callable`): unlike a
    token credential it cannot refresh per connection, and every reconnect
    authenticates with the pair's password. If nothing rebuilds the pool
    the pair is never replaced, so reconnects fail authentication once the
    lease expires. Three outcomes, all visible to the operator:

    * an explicit interval is configured - nothing to say, the operator
      owns the cadence;
    * the issuer reported a lease and no interval is configured - the
      rebuild cadence is derived from it and logged
      (``pg-lease-reload-derived``) so the 3am reader can see when the
      next rebuild is due and what TTL it was derived from;
    * neither - the pair is pinned with nothing to rebuild it, and the
      build warns (``pg-lease-pair-pinned-without-reload``) naming
      ``TASKQ_RELOAD_INTERVAL``. A warning, never a refusal: the operator
      may rotate the lease externally.

    *warn_without_schedule* is False for a factory whose caller owns the
    resulting connection's whole life (a one-shot migration connection):
    there is no long-lived pool to rotate, so nothing to warn about.
    """
    schedule.record(credential)
    if credential.username is None or schedule.configured is not None:
        return
    if schedule.derived:
        logger.info(
            "pg-lease-reload-derived",
            role=role,
            lease_duration=schedule.lease_duration,
            reload_interval=schedule.interval,
            reason=(
                "no reload interval is configured, so the pool is rebuilt on a "
                f"fresh pair at {LEASE_RELOAD_FRACTION:g} of the shortest lease "
                "TTL the provider has granted; set TASKQ_RELOAD_INTERVAL to override"
            ),
        )
        return
    if not warn_without_schedule:
        return
    logger.warning(
        "pg-lease-pair-pinned-without-reload",
        kind="lease_pair_without_reload",
        role=role,
        lease_ttl=None,
        reason=(
            "the credential provider issued a username-bearing credential with "
            "no lease duration: the username and password are one lease, so "
            "this pool is pinned to that pair for its life and every reconnect "
            "authenticates with the pair's password, which stops authenticating "
            "once the lease expires - and with no TTL reported, no rebuild can "
            "be scheduled from it"
        ),
        remedy=(
            "set TASKQ_RELOAD_INTERVAL below the lease TTL so the pool is "
            "rebuilt on a fresh pair, have the provider report "
            "PgCredential.lease_duration, or rotate the lease externally; this "
            "is a warning only, the process runs either way"
        ),
    )


# --- Factory builders ---
#
# All factories are zero-arg async callables matching the ``PoolFactory`` /
# ``ConnFactory`` / ``RedisFactory`` aliases in :mod:`taskq.connections`.
# Sizing and DSN are closed over at build time; the worker invokes them at
# the right point in its lifecycle and closes the result via AsyncExitStack.


def make_pg_pool_factory(
    dsn: str,
    provider: PgCredentialProvider,
    *,
    min_size: int = 1,
    max_size: int = 4,
    max_inactive_connection_lifetime: float = 300.0,
    command_timeout: float | None = None,
    statement_cache_size: int = DEFAULT_STATEMENT_CACHE_SIZE,
    max_cached_statement_lifetime: int = DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    init: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    setup: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    server_settings: dict[str, str] | None = None,
    connection_class: type[asyncpg.Connection] | None = None,
    reload_schedule: ReloadSchedule | None = None,
) -> PoolFactory:
    """Build a :data:`~taskq.connections.PoolFactory` backed by *provider*.

    Each invocation fetches a fresh :class:`PgCredential` from *provider*
    and calls ``asyncpg.create_pool`` with the credential as keyword
    arguments - ``password=`` always, ``user=`` when the credential
    carries a username. Keyword arguments take precedence over both DSN
    userinfo and query parameters in asyncpg's resolver, so a stale
    credential baked into *dsn* can never shadow the fresh one, and the
    token never appears in the DSN string. The pool is owned by the
    worker (entered on its ``AsyncExitStack``).

    Token refresh: ``password=`` is passed as an **async callable**, which
    asyncpg invokes and awaits once per *physical* connection - the
    connections opened at pool creation, those opened later by pool
    growth, and the replacements opened after
    ``max_inactive_connection_lifetime`` recycles an idle connection. For
    a token credential (``username`` unset: Entra ID, AWS IAM RDS) every
    new connection therefore authenticates with a freshly fetched token,
    and no external rotation is required. This matters because Postgres
    authenticates at connect time only: a credential resolved once and
    reused as a fixed string keeps working on already-open connections
    while every new connection fails, roughly one token-lifetime after
    deploy.

    A **username-bearing** credential (Vault dynamic database credentials)
    is one issued pair: asyncpg resolves ``user=`` once per pool, so the
    pool is pinned to that username and every physical connection
    authenticates with the pair's password - re-fetching would burn a
    lease per connection for a password the pinned user cannot use. Such
    a pool rotates when the factory is re-invoked: ``SIGHUP`` /
    ``TASKQ_RELOAD_INTERVAL`` (``taskq.worker.deps.reload_credentials``)
    rebuilds it on a fresh pair, on the cadence *reload_schedule* carries
    (below). Reload is also the way to force a full pool rebuild for a
    token credential (dropping sessions opened under a revoked token).

    Per-connection setup: *init* is forwarded verbatim to
    ``asyncpg.create_pool`` and runs **once per new physical connection**
    - on the connections opened at pool creation, on connections opened
    later by pool growth, and again on replacements opened after
    ``max_inactive_connection_lifetime`` recycles an idle connection.
    That lifecycle is exactly why this setup (registering type codecs -
    e.g. ``pgvector.asyncpg.register_vector`` - preparing statements,
    setting session GUCs) cannot be done correctly after pool creation:
    a connection configured by hand is silently replaced under load or
    after an idle period. The only per-connection work this factory does
    of its own is the credential refresh described above (an asyncpg
    ``password=`` callback, not an ``init`` hook), so a caller-supplied
    *init* is the only hook of its kind: it is passed through unwrapped
    and can never silently replace internal setup.

    Per-acquire setup: *setup* is forwarded to ``asyncpg.create_pool``
    and runs **every time a connection is acquired from the pool**
    (via ``pool.acquire()``), not just on new-connection creation. Use
    it for per-checkout work that must run even when a pooled connection
    is reused - e.g. resetting ``search_path`` or verifying session
    state. Unlike *init*, *setup* runs on every acquire, so keep it
    lightweight. Both *init* and *setup* can be provided simultaneously.

    *server_settings* is forwarded to ``asyncpg.create_pool`` and applied
    as session-level GUCs on every new connection (e.g.
    ``{"statement_timeout": "30s", "search_path": "app"}``). Useful for
    per-pool configuration that must be set at connection time.

    *statement_cache_size* / *max_cached_statement_lifetime* are forwarded
    to ``asyncpg.create_pool`` and default to TaskQ's tuning (the
    ``taskq.connections`` module constants — 512 entries, 1 h lifetime —
    which are also the ``TaskQSettings`` field defaults). asyncpg's own
    defaults (100 / 300 s) thrash on TaskQ's read paths, so a
    provider-backed pool must get the same cache treatment as the
    DSN-built ones. Call sites with a
    :class:`~taskq.settings.WorkerSettings` in scope pass the values
    resolved through :func:`taskq.connections.statement_cache_kwargs` so
    the ``TASKQ_STATEMENT_CACHE_SIZE`` / ``TASKQ_MAX_CACHED_STATEMENT_LIFETIME``
    env vars apply to provider-backed pools too.

    *connection_class* is forwarded to ``asyncpg.create_pool`` and sets
    the :class:`asyncpg.Connection` subclass used by the pool. Use it to
    install custom codecs or override connection methods across the
    entire pool.

    *reload_schedule* is the :class:`ReloadSchedule` this pool is rebuilt
    on. Each build records the credential it was issued there - its
    ``lease_duration`` when the issuer reports one - so a consumer with no
    explicit interval rebuilds at :data:`LEASE_RELOAD_FRACTION` of the
    granted TTL. Pass one schedule to every factory a consumer rebuilds
    together (:func:`build_worker_connections` does) so they share the
    shortest lease; omitted, the factory declares a schedule of its own,
    readable with :func:`reload_schedule_of`. A username-bearing
    credential built against a schedule that can derive nothing (no
    configured interval, no reported lease) logs
    ``pg-lease-pair-pinned-without-reload`` naming
    ``TASKQ_RELOAD_INTERVAL``, because reconnects will fail authentication
    once the lease expires and nothing here can prevent it. It is a
    warning, never a refusal: an operator rotating the lease externally
    can ignore it.
    """
    import asyncpg  # Why: deferred so this module is import-safe without asyncpg at module load.

    schedule = reload_schedule if reload_schedule is not None else ReloadSchedule()

    async def factory() -> asyncpg.Pool:
        # Fetched once here to resolve `user=` (not callable in asyncpg) and to
        # fail fast at pool construction on a broken provider, rather than
        # deferring the first failure to the first connection attempt. The
        # password goes in as a callable: re-fetched per physical connection
        # for a token credential, the pair's own for a username-bearing one.
        credential = await provider.get_pg_credential()
        _record_issued_credential(schedule, credential, role="pool", warn_without_schedule=True)
        kwargs: dict[str, Any] = {
            "dsn": ensure_sslmode_require(dsn),
            "password": _make_pg_password_callable(provider, pinned=credential, role="pool"),
            "min_size": min_size,
            "max_size": max_size,
            "max_inactive_connection_lifetime": max_inactive_connection_lifetime,
            "statement_cache_size": statement_cache_size,
            "max_cached_statement_lifetime": max_cached_statement_lifetime,
        }
        if credential.username is not None:
            kwargs["user"] = credential.username
        if command_timeout is not None:
            kwargs["command_timeout"] = command_timeout
        if init is not None:
            kwargs["init"] = init
        if setup is not None:
            kwargs["setup"] = setup
        if server_settings is not None:
            kwargs["server_settings"] = server_settings
        if connection_class is not None:
            kwargs["connection_class"] = connection_class
        pool = await asyncpg.create_pool(**kwargs)
        assert pool is not None  # asyncpg returns None only for record_class paths
        return pool

    return _declare_reload_schedule(factory, schedule)


def make_dedicated_conn_factory(
    dsn: str,
    provider: PgCredentialProvider,
    *,
    command_timeout: float | None = None,
    setup: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    server_settings: dict[str, str] | None = None,
    connection_class: type[asyncpg.Connection] | None = None,
    reload_schedule: ReloadSchedule | None = None,
) -> ConnFactory:
    """Build a :data:`~taskq.connections.ConnFactory` backed by *provider*.

    Used for the worker's ``notify_conn`` / ``leader_conn`` or
    :class:`taskq.TaskQ`'s ``pg_conn_factory``. Like
    :func:`make_pg_pool_factory`, the credential is passed as keyword
    arguments (precedence over userinfo and query params; the token
    never appears in the DSN string), and ``password=`` is an async
    callable that asyncpg awaits per physical connection - a fresh token
    for a token credential, the issued pair's password for a
    username-bearing one.

    A dedicated connection is opened once and then held for the life of
    the worker, so the callable normally fires exactly once - but these
    are precisely the long-lived connections a credential expiry kills,
    and the callable is what makes a *re-open* by asyncpg (a LISTEN
    connection reconnecting after the server drops it) authenticate with
    the current token rather than the one captured when the factory was
    first invoked. ``reload_credentials`` re-invokes the factory itself,
    which is where a username-bearing pair is replaced.

    *command_timeout* is forwarded to ``asyncpg.connect`` as the default
    per-operation timeout. The worker's DSN-built ``notify_conn`` /
    ``leader_conn`` carry ``dispatcher_command_timeout``; pass it here too
    so a credential-provider deployment does not silently drop the bound
    that keeps a wedged query from stalling leader election.

    *setup* is forwarded to ``asyncpg.connect`` and runs once after the
    connection is established (e.g. registering type codecs, setting
    session GUCs). For a dedicated connection this is equivalent to
    *init* on a pool - there is no acquire/reuse cycle. The hook is also
    declared on the returned factory (see
    :func:`taskq.connections.with_connection_init`), so when this factory
    provides the worker's LOOP-scope ``asyncpg.Connection`` registration
    the per-slot transaction pool inherits it - the codec family that a
    bare ``set_type_codec`` on one live connection silently loses above
    ``max_concurrency = 1``.

    *server_settings* is forwarded to ``asyncpg.connect`` and applied as
    session-level GUCs at connect time (e.g.
    ``{"statement_timeout": "30s", "search_path": "app"}``).

    *connection_class* is forwarded to ``asyncpg.connect`` and sets the
    :class:`asyncpg.Connection` subclass for this connection. Use it to
    install custom codecs or override connection methods.

    *reload_schedule* is the :class:`ReloadSchedule` the connection is
    rebuilt on, exactly as for :func:`make_pg_pool_factory`: pass the
    consumer's shared schedule for a connection that lives as long as the
    process (the worker's ``notify_conn`` / ``leader_conn``), so the lease
    it is issued counts toward the derived cadence. Omitted, the factory
    is taken to be one-shot - a migration connection the caller opens,
    uses and closes - and records its lease on a schedule of its own
    without the pinned-pair warning, since there is no long-lived
    connection to rotate.
    """
    import asyncpg

    schedule = reload_schedule if reload_schedule is not None else ReloadSchedule()
    long_lived = reload_schedule is not None

    async def factory() -> asyncpg.Connection:
        # Fetched once to resolve `user=` and fail fast; see make_pg_pool_factory.
        credential = await provider.get_pg_credential()
        _record_issued_credential(
            schedule, credential, role="dedicated_conn", warn_without_schedule=long_lived
        )
        kwargs: dict[str, Any] = {
            "dsn": ensure_sslmode_require(dsn),
            "password": _make_pg_password_callable(
                provider, pinned=credential, role="dedicated_conn"
            ),
        }
        if credential.username is not None:
            kwargs["user"] = credential.username
        if command_timeout is not None:
            kwargs["command_timeout"] = command_timeout
        if setup is not None:
            kwargs["setup"] = setup
        if server_settings is not None:
            kwargs["server_settings"] = server_settings
        if connection_class is not None:
            kwargs["connection_class"] = connection_class
        return await asyncpg.connect(**kwargs)

    if setup is not None:
        # A dedicated connection's setup runs once per (re)open - the same
        # lifecycle position as a pool's init - so it is declared as the
        # inheritable init hook verbatim.
        setattr(factory, _CONNECTION_INIT_HOOK_ATTR, setup)
    return _declare_reload_schedule(factory, schedule)


def make_redis_client_factory(
    url: str | None,
    provider: RedisCredentialProvider,
    **client_kwargs: Any,
) -> RedisFactory:
    """Build a :data:`~taskq.connections.RedisFactory` backed by *provider*.

    ``url`` is the Redis URL **without** credentials. The factory attaches
    a redis-py ``CredentialProvider`` that delegates to *provider*, so
    reconnects re-fetch the credential automatically. Use a ``rediss://``
    (TLS) URL - with a plain ``redis://`` URL the bearer token is sent
    unencrypted, and the factory logs a warning.

    If ``url`` is ``None`` the factory raises :class:`RuntimeError` when
    called (matches the worker's "Redis not configured" contract).
    """
    import redis.asyncio as redis_async  # type: ignore[import-not-found]  # Why: optional [redis] extra; required at call time.
    from redis.credentials import (
        CredentialProvider,  # type: ignore[import-not-found]  # Why: optional [redis] extra; required at call time.
    )

    class _CredentialProviderAdapter(CredentialProvider):
        """redis-py ``CredentialProvider`` → TaskQ ``RedisCredentialProvider``.

        redis-py's async connection calls ``get_credentials_async`` (not
        ``get_credentials``) on every (re)connect - the base class's
        ``get_credentials_async`` only exists for backward compatibility
        and delegates to the *sync* ``get_credentials``, so it must be
        overridden here for the credential to actually rotate.
        """

        def get_credentials(self) -> tuple[str, str]:
            raise NotImplementedError(
                "_CredentialProviderAdapter only supports the async redis client; "
                "get_credentials_async is called instead."
            )

        async def get_credentials_async(self) -> tuple[str, str]:
            cred = await provider.get_redis_credential()
            return cred.username, cred.password

    adapter = _CredentialProviderAdapter()

    async def factory() -> Any:
        if url is None:
            raise RuntimeError(
                "Redis URL is not configured but a Redis credential-provider "
                "factory was provided. Set TASKQ_REDIS_URL or pass url= explicitly."
            )
        if urlparse(url).scheme == "redis":
            logger.warning(
                "redis-credential-over-plaintext",
                scheme="redis",
                note=(
                    "redis:// sends the credential provider's bearer token "
                    "unencrypted; use rediss:// (TLS) instead."
                ),
            )
        client_kwargs.setdefault("decode_responses", False)
        return redis_async.Redis.from_url(
            url,
            credential_provider=adapter,
            **client_kwargs,
        )

    return factory


# --- Whole-worker wiring ---


def build_worker_connections(
    settings: WorkerSettings,
    *,
    pg_provider: PgCredentialProvider | None = None,
    redis_provider: RedisCredentialProvider | None = None,
    pg_dsn: str | None = None,
    pg_dsn_direct: str | None = None,
    pg_dsn_pooled: str | None = None,
    redis_url: str | None = None,
) -> WorkerConnections:
    """Build the full set of provider-backed factories for one worker.

    Every Postgres role the worker opens (dispatcher / heartbeat / worker
    pools, the ``notify_conn`` LISTEN connection and the ``leader_conn``
    advisory-lock connection) plus the Redis client, sized and timed out
    exactly as :func:`taskq.worker.deps.open_worker_deps` sizes its
    DSN-built equivalents - so switching a deployment to a credential
    provider changes *how it authenticates*, never its connection budget
    or its timeouts.

    This is what makes the credential path reachable from the ``taskq
    worker`` console script (``--pg-credential-provider`` /
    ``TASKQ_PG_CREDENTIAL_PROVIDER``): every role is factory-backed, so
    ``SIGHUP`` / ``TASKQ_RELOAD_INTERVAL`` rebuild all of them through the
    provider. A role left on the DSN fallback would be silently
    un-rotatable - ``reload_credentials`` skips roles with no factory - so
    this builder deliberately covers all of them or raises.

    Explicit endpoints
    ------------------

    *pg_dsn* / *pg_dsn_direct* / *pg_dsn_pooled* / *redis_url* override where
    the factories point, while every pool size and timeout still comes from
    *settings*. Pass *pg_dsn* to send all five Postgres roles at one server;
    pass the *_direct* / *_pooled* pair to keep a pgbouncer split. They are
    mutually exclusive - a call that sets both is ambiguous about which wins.

    Why this exists: an application that already knows where TaskQ's tables
    live otherwise had to restate that in ``TASKQ_PG_DSN`` purely to reach
    this builder, duplicating one fact across two config systems (the class
    of bug where the two copies disagree about the schema). The alternative
    it reached for instead - one hand-built ``make_pg_pool_factory`` passed
    to all three pool roles - silently discards the per-role budget this
    function exists to apply: TaskQ resolves each role separately, so every
    role gets a full ``pool_max``-sized pool rather than
    ``dispatcher_pool_size`` / ``heartbeat_pool_size`` / ``worker_pool_size``.
    Overriding the endpoint keeps the budget.

    Raises ``ValueError`` when no provider is given, when *pg_dsn* is
    combined with *pg_dsn_direct* / *pg_dsn_pooled*, or when
    *redis_provider* is set with no Redis URL available from either
    *redis_url* or ``settings``: a Redis provider that quietly did nothing
    is the failure mode this wiring exists to remove.
    """
    if pg_provider is None and redis_provider is None:
        raise ValueError(
            "build_worker_connections requires at least one of pg_provider / redis_provider"
        )
    if pg_dsn is not None and (pg_dsn_direct is not None or pg_dsn_pooled is not None):
        raise ValueError(
            "build_worker_connections accepts 'pg_dsn' or the "
            "'pg_dsn_direct'/'pg_dsn_pooled' pair, not both"
        )

    conns = WorkerConnections()

    if pg_provider is not None:
        direct = pg_dsn_direct or pg_dsn or str(settings.resolved_pg_dsn_direct)
        pooled = pg_dsn_pooled or pg_dsn or str(settings.resolved_pg_dsn_pooled)
        lifetime = settings.pool_max_inactive_lifetime
        # Same statement-cache treatment as open_worker_deps' DSN-built
        # pools — switching authentication must not switch cache behaviour.
        # The kwargs are forwarded explicitly (not splatted) so pyright
        # traces types through make_pg_pool_factory's typed parameters.
        stmt_kwargs = statement_cache_kwargs(settings)
        # One schedule for every role: the worker rebuilds them together, so
        # the cadence is the shortest lease any of them was granted, under
        # the operator's TASKQ_RELOAD_INTERVAL when that is set (see
        # ReloadSchedule). The reload coordinator reads it back off the
        # factories with reload_schedule_of.
        schedule = ReloadSchedule(configured=settings.reload_interval)
        conns.dispatcher_pool_factory = make_pg_pool_factory(
            direct,
            pg_provider,
            max_size=settings.dispatcher_pool_size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=settings.dispatcher_command_timeout,
            statement_cache_size=stmt_kwargs["statement_cache_size"],
            max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
            reload_schedule=schedule,
        )
        conns.heartbeat_pool_factory = make_pg_pool_factory(
            direct,
            pg_provider,
            max_size=settings.heartbeat_pool_size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=settings.heartbeat_command_timeout,
            statement_cache_size=stmt_kwargs["statement_cache_size"],
            max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
            reload_schedule=schedule,
        )
        conns.worker_pool_factory = make_pg_pool_factory(
            pooled,
            pg_provider,
            max_size=settings.worker_pool_size,
            max_inactive_connection_lifetime=lifetime,
            statement_cache_size=stmt_kwargs["statement_cache_size"],
            max_cached_statement_lifetime=stmt_kwargs["max_cached_statement_lifetime"],
            reload_schedule=schedule,
        )
        conns.notify_conn_factory = make_dedicated_conn_factory(
            direct,
            pg_provider,
            command_timeout=settings.dispatcher_command_timeout,
            reload_schedule=schedule,
        )
        conns.leader_conn_factory = make_dedicated_conn_factory(
            direct,
            pg_provider,
            command_timeout=settings.dispatcher_command_timeout,
            reload_schedule=schedule,
        )

    if redis_provider is not None:
        resolved_redis = redis_url or (
            str(settings.redis_url) if settings.redis_url is not None else None
        )
        if resolved_redis is None:
            raise ValueError(
                "a Redis credential provider was configured but no Redis URL is set - "
                "pass redis_url=, set TASKQ_REDIS_URL, or drop the Redis provider."
            )
        conns.redis_client_factory = make_redis_client_factory(resolved_redis, redis_provider)

    return conns

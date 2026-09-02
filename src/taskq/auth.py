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

The one thing that cannot refresh in place is a **changed username**:
asyncpg resolves ``user=`` once per pool / connection and accepts a
callable only for ``password=``. Providers that rotate usernames (e.g.
Vault dynamic database credentials) need a pool rebuild - ``SIGHUP`` /
``taskq.worker.deps.reload_credentials`` - and raise a clear error rather
than pairing a fresh password with a stale username.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from taskq.connections import ConnFactory, PoolFactory, RedisFactory, WorkerConnections
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

    from taskq.settings import WorkerSettings

logger = get_logger(__name__)

__all__ = [
    "PgCredential",
    "PgCredentialProvider",
    "RedisCredential",
    "RedisCredentialProvider",
    "build_worker_connections",
    "enrich_pg_dsn",
    "make_dedicated_conn_factory",
    "make_pg_pool_factory",
    "make_redis_client_factory",
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
    """

    password: str
    username: str | None = None


@dataclass(frozen=True, slots=True)
class RedisCredential:
    """A Redis credential issued by a rotating-credential provider."""

    username: str
    password: str


# --- Provider protocols ---


@runtime_checkable
class PgCredentialProvider(Protocol):
    """Provides rotating Postgres credentials on demand.

    Implementations fetch a fresh token / dynamic username+password each
    call. Called by :func:`make_pg_pool_factory` /
    :func:`make_dedicated_conn_factory` once at pool / connection
    construction (to resolve ``user=`` and fail fast), and then again for
    every **physical** connection asyncpg opens thereafter - not on each
    ``acquire()``, which hands back an already-authenticated connection
    from the pool. Implementations are expected to cache and only hit the
    issuing service when the cached credential is near expiry.
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


def _ensure_sslmode_require(dsn: str) -> str:
    """Add ``sslmode=require`` to *dsn* unless an sslmode is already set.

    An explicit sslmode is never overridden - in particular stronger
    modes (``verify-ca`` / ``verify-full``) must not be downgraded:
    ``require`` skips certificate verification, which would expose the
    very token this module injects to a MITM.
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
    pinned_username: str | None,
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

    ``pinned_username`` is the username asyncpg was configured with at pool /
    connection construction. asyncpg's ``user=`` is **not** callable - it is
    resolved once in ``_parse_connect_arguments`` - so a provider that issues a
    *new username* alongside each password (HashiCorp Vault dynamic database
    credentials being the case that matters) cannot have that username applied
    per connection. Silently pairing a fresh password with the stale username
    would authenticate as the wrong role or fail with an opaque server-side
    error, so a changed username is raised as a configuration error naming the
    mechanism that does handle it (``SIGHUP`` / ``reload_credentials``, which
    rebuilds the pool and therefore re-resolves ``user=``).
    """

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

        if credential.username is not None and credential.username != pinned_username:
            msg = (
                f"PgCredentialProvider changed the username for the {role!r} connection "
                f"mid-rotation (built with user={pinned_username!r}, provider now returns "
                f"user={credential.username!r}). asyncpg resolves `user=` once per pool / "
                "connection and only `password=` per physical connection, so the new "
                "username cannot be applied in place. Send SIGHUP to the worker "
                "(taskq.worker.deps.reload_credentials) to rebuild with the new username, "
                "or use a provider whose username is stable across rotations."
            )
            raise RuntimeError(msg)

        return credential.password

    return _fetch_password


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
    init: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    setup: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    server_settings: dict[str, str] | None = None,
    connection_class: type[asyncpg.Connection] | None = None,
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
    ``max_inactive_connection_lifetime`` recycles an idle connection. Every
    new connection therefore authenticates with a freshly fetched
    credential, and no external rotation is required. This matters because
    Postgres authenticates at connect time only: a credential resolved once
    and reused as a fixed string keeps working on already-open connections
    while every new connection fails, roughly one token-lifetime after
    deploy.

    ``SIGHUP`` (see ``taskq.worker.deps.reload_credentials``) still works
    and is no longer *required* for token refresh. It remains the way to
    force a full pool rebuild - and the only way to pick up a **changed
    username**, since asyncpg resolves ``user=`` once per pool and accepts
    a callable only for ``password=``. A provider that rotates its username
    (e.g. Vault dynamic database credentials) raises a ``RuntimeError``
    naming this constraint rather than pairing a fresh password with a
    stale username.

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

    *connection_class* is forwarded to ``asyncpg.create_pool`` and sets
    the :class:`asyncpg.Connection` subclass used by the pool. Use it to
    install custom codecs or override connection methods across the
    entire pool.
    """
    import asyncpg  # Why: deferred so this module is import-safe without asyncpg at module load.

    async def factory() -> asyncpg.Pool:
        # Fetched once here to resolve `user=` (not callable in asyncpg) and to
        # fail fast at pool construction on a broken provider, rather than
        # deferring the first failure to the first connection attempt. The
        # password itself goes in as a callable so it is re-fetched per
        # physical connection.
        credential = await provider.get_pg_credential()
        kwargs: dict[str, Any] = {
            "dsn": _ensure_sslmode_require(dsn),
            "password": _make_pg_password_callable(
                provider, pinned_username=credential.username, role="pool"
            ),
            "min_size": min_size,
            "max_size": max_size,
            "max_inactive_connection_lifetime": max_inactive_connection_lifetime,
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

    return factory


def make_dedicated_conn_factory(
    dsn: str,
    provider: PgCredentialProvider,
    *,
    command_timeout: float | None = None,
    setup: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
    server_settings: dict[str, str] | None = None,
    connection_class: type[asyncpg.Connection] | None = None,
) -> ConnFactory:
    """Build a :data:`~taskq.connections.ConnFactory` backed by *provider*.

    Used for the worker's ``notify_conn`` / ``leader_conn`` or
    :class:`taskq.TaskQ`'s ``pg_conn_factory``. Like
    :func:`make_pg_pool_factory`, the credential is passed as keyword
    arguments (precedence over userinfo and query params; the token
    never appears in the DSN string), and ``password=`` is an async
    callable that asyncpg awaits per physical connection.

    A dedicated connection is opened once and then held for the life of
    the worker, so the callable normally fires exactly once - but these
    are precisely the long-lived connections a credential expiry kills,
    and the callable is what makes every *re-open* (a LISTEN connection
    reconnecting after the server drops it, or ``reload_credentials``
    rebuilding it) authenticate with a fresh credential rather than the
    one captured when the factory was first invoked.

    *command_timeout* is forwarded to ``asyncpg.connect`` as the default
    per-operation timeout. The worker's DSN-built ``notify_conn`` /
    ``leader_conn`` carry ``dispatcher_command_timeout``; pass it here too
    so a credential-provider deployment does not silently drop the bound
    that keeps a wedged query from stalling leader election.

    *setup* is forwarded to ``asyncpg.connect`` and runs once after the
    connection is established (e.g. registering type codecs, setting
    session GUCs). For a dedicated connection this is equivalent to
    *init* on a pool - there is no acquire/reuse cycle.

    *server_settings* is forwarded to ``asyncpg.connect`` and applied as
    session-level GUCs at connect time (e.g.
    ``{"statement_timeout": "30s", "search_path": "app"}``).

    *connection_class* is forwarded to ``asyncpg.connect`` and sets the
    :class:`asyncpg.Connection` subclass for this connection. Use it to
    install custom codecs or override connection methods.
    """
    import asyncpg

    async def factory() -> asyncpg.Connection:
        # Fetched once to resolve `user=` and fail fast; see make_pg_pool_factory.
        credential = await provider.get_pg_credential()
        kwargs: dict[str, Any] = {
            "dsn": _ensure_sslmode_require(dsn),
            "password": _make_pg_password_callable(
                provider, pinned_username=credential.username, role="dedicated_conn"
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

    return factory


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

    Raises ``ValueError`` when no provider is given, or when
    *redis_provider* is set with no ``redis_url`` configured: a Redis
    provider that quietly did nothing is the failure mode this wiring
    exists to remove.
    """
    if pg_provider is None and redis_provider is None:
        raise ValueError(
            "build_worker_connections requires at least one of pg_provider / redis_provider"
        )

    conns = WorkerConnections()

    if pg_provider is not None:
        direct = str(settings.resolved_pg_dsn_direct)
        pooled = str(settings.resolved_pg_dsn_pooled)
        lifetime = settings.pool_max_inactive_lifetime
        conns.dispatcher_pool_factory = make_pg_pool_factory(
            direct,
            pg_provider,
            max_size=settings.dispatcher_pool_size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=settings.dispatcher_command_timeout,
        )
        conns.heartbeat_pool_factory = make_pg_pool_factory(
            direct,
            pg_provider,
            max_size=settings.heartbeat_pool_size,
            max_inactive_connection_lifetime=lifetime,
            command_timeout=2,
        )
        conns.worker_pool_factory = make_pg_pool_factory(
            pooled,
            pg_provider,
            max_size=settings.worker_pool_size,
            max_inactive_connection_lifetime=lifetime,
        )
        conns.notify_conn_factory = make_dedicated_conn_factory(
            direct, pg_provider, command_timeout=settings.dispatcher_command_timeout
        )
        conns.leader_conn_factory = make_dedicated_conn_factory(
            direct, pg_provider, command_timeout=settings.dispatcher_command_timeout
        )

    if redis_provider is not None:
        if settings.redis_url is None:
            raise ValueError(
                "a Redis credential provider was configured but no Redis URL is set - "
                "set TASKQ_REDIS_URL, or drop the Redis provider."
            )
        conns.redis_client_factory = make_redis_client_factory(
            str(settings.redis_url), redis_provider
        )

    return conns

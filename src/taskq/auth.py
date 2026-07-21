"""Vendor-neutral credential providers and connection factories.

This module provides the reusable primitives for **rotating-credential**
Postgres and Redis connections — the abstract interfaces that any auth
provider (Azure Entra ID, AWS IAM RDS, HashiCorp Vault, a custom OAuth
flow, a secrets manager, …) plugs into. Provider-specific implementations
live in the ``taskq[aad]``, ``taskq[aws]``, and ``taskq[vault]`` extras;
users with other providers implement :class:`PgCredentialProvider` /
:class:`RedisCredentialProvider` directly and get all the factory
builders for free.

See the managed-identities deployment guide (docs/guides/managed-identities.md).

Design
------

* :class:`PgCredentialProvider` — async Protocol returning a
  :class:`PgCredential` (a password, optionally a fresh username). AAD
  and AWS IAM RDS return a token-as-password; Vault dynamic DB creds
  return a fresh username + password pair.
* :class:`RedisCredentialProvider` — async Protocol returning a
  :class:`RedisCredential` (username + password). AAD returns the
  managed-identity object ID + token.
* :func:`make_pg_pool_factory` / :func:`make_dedicated_conn_factory` /
  :func:`make_redis_client_factory` — accept any provider implementing
  the Protocol and return the zero-arg async factories that
  :class:`~taskq.connections.WorkerConnections` consumes. Credentials
  are passed to asyncpg as ``user=`` / ``password=`` keyword arguments
  (which take precedence over both DSN userinfo and query parameters),
  so the token never appears in the DSN string.
* :func:`enrich_pg_dsn` — shared DSN helper for callers that need a
  self-contained DSN string: the credential is written into the DSN
  userinfo (the only slot asyncpg's resolver never shadows) and
  ``sslmode=require`` is added only when no sslmode is already set.

The factories fetch a fresh credential each time they are invoked (at
pool / connection construction time). For long-lived workers, recreate
the pool on a schedule shorter than the credential lifetime. Redis
reconnects re-fetch automatically via the redis-py ``CredentialProvider``
adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from taskq.connections import ConnFactory, PoolFactory, RedisFactory
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

logger = get_logger(__name__)

__all__ = [
    "PgCredential",
    "PgCredentialProvider",
    "RedisCredential",
    "RedisCredentialProvider",
    "enrich_pg_dsn",
    "make_dedicated_conn_factory",
    "make_pg_pool_factory",
    "make_redis_client_factory",
]


# ── Credential data carriers ───────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PgCredential:
    """A Postgres credential issued by a rotating-credential provider.

    ``password`` is always required (a token or dynamic password).
    ``username``, when set, overrides the DSN's userinfo user — needed by
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


# ── Provider protocols ─────────────────────────────────────────────────


@runtime_checkable
class PgCredentialProvider(Protocol):
    """Provides rotating Postgres credentials on demand.

    Implementations fetch a fresh token / dynamic username+password each
    call. Called by :func:`make_pg_pool_factory` /
    :func:`make_dedicated_conn_factory` at pool / connection construction
    time — not on each ``acquire()``.
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


# ── DSN enrichment ─────────────────────────────────────────────────────


def _ensure_sslmode_require(dsn: str) -> str:
    """Add ``sslmode=require`` to *dsn* unless an sslmode is already set.

    An explicit sslmode is never overridden — in particular stronger
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
    replacing any existing userinfo password — and replacing the userinfo
    user when ``credential.username`` is set (Vault dynamic DB creds).
    This is the only slot that is guaranteed to take effect: asyncpg's
    resolver applies userinfo *before* query parameters (both behind
    ``if user is None`` / ``if password is None`` guards), so a
    query-string ``user=`` / ``password=`` is silently ignored whenever
    the DSN already carries userinfo. A stale ``password=`` query
    parameter is dropped (always shadowed by the userinfo password);
    a ``user=`` query parameter is dropped only when the userinfo
    carries a user to shadow it — a query-carried user with no userinfo
    user is the effective principal and is preserved.

    ``sslmode=require`` is added only when the DSN has no explicit
    sslmode, so stronger modes (``verify-full``) are never downgraded.

    Prefer the factory builders (:func:`make_pg_pool_factory` /
    :func:`make_dedicated_conn_factory`) where possible — they pass the
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
        # asyncpg's resolver — drop the stale query copy. When the userinfo
        # has NO user (credential.username unset, none in the DSN), a query
        # user= is the effective principal and must be preserved.
        query.pop("user", None)
    netloc = f"{user}:{quote(credential.password, safe='')}@{hostspec}"

    query.setdefault("sslmode", ["require"])
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(netloc=netloc, query=new_query))


# ── Factory builders ───────────────────────────────────────────────────
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
) -> PoolFactory:
    """Build a :data:`~taskq.connections.PoolFactory` backed by *provider*.

    Each invocation fetches a fresh :class:`PgCredential` from *provider*
    and calls ``asyncpg.create_pool`` with the credential as keyword
    arguments — ``password=`` always, ``user=`` when the credential
    carries a username. Keyword arguments take precedence over both DSN
    userinfo and query parameters in asyncpg's resolver, so a stale
    credential baked into *dsn* can never shadow the fresh one, and the
    token never appears in the DSN string. The pool is owned by the
    worker (entered on its ``AsyncExitStack``).

    Token refresh: the credential is fetched when the factory is invoked,
    not on each ``acquire()``. For long-lived workers, send ``SIGHUP`` to
    the worker process on a schedule shorter than the credential lifetime
    — this factory is re-invoked automatically to rebuild the pool with a
    fresh credential (see ``taskq.worker.deps.reload_credentials``); no
    restart needed.
    """
    import asyncpg  # Why: deferred so this module is import-safe without asyncpg at module load.

    async def factory() -> asyncpg.Pool:
        credential = await provider.get_pg_credential()
        kwargs: dict[str, Any] = {
            "dsn": _ensure_sslmode_require(dsn),
            "password": credential.password,
            "min_size": min_size,
            "max_size": max_size,
            "max_inactive_connection_lifetime": max_inactive_connection_lifetime,
        }
        if credential.username is not None:
            kwargs["user"] = credential.username
        if command_timeout is not None:
            kwargs["command_timeout"] = command_timeout
        pool = await asyncpg.create_pool(**kwargs)
        assert pool is not None  # asyncpg returns None only for record_class paths
        return pool

    return factory


def make_dedicated_conn_factory(
    dsn: str,
    provider: PgCredentialProvider,
) -> ConnFactory:
    """Build a :data:`~taskq.connections.ConnFactory` backed by *provider*.

    Used for the worker's ``notify_conn`` / ``leader_conn`` or
    :class:`taskq.TaskQ`'s ``pg_conn_factory``. Like
    :func:`make_pg_pool_factory`, the credential is passed as keyword
    arguments (precedence over userinfo and query params; the token
    never appears in the DSN string).
    """
    import asyncpg

    async def factory() -> asyncpg.Connection:
        credential = await provider.get_pg_credential()
        kwargs: dict[str, Any] = {
            "dsn": _ensure_sslmode_require(dsn),
            "password": credential.password,
        }
        if credential.username is not None:
            kwargs["user"] = credential.username
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
    (TLS) URL — with a plain ``redis://`` URL the bearer token is sent
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
        ``get_credentials``) on every (re)connect — the base class's
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

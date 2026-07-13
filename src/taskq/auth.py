"""Vendor-neutral credential providers and connection factories.

This module provides the reusable primitives for **rotating-credential**
Postgres and Redis connections — the abstract interfaces that any auth
provider (Azure Entra ID, AWS IAM RDS, HashiCorp Vault, a custom OAuth
flow, a secrets manager, …) plugs into. Provider-specific implementations
live in the ``taskq[aad]``, ``taskq[aws]``, and ``taskq[vault]`` extras;
users with other providers implement :class:`PgCredentialProvider` /
:class:`RedisCredentialProvider` directly and get all the factory
builders for free.

See :doc:`/guides/managed-identities` for the deployment guide.

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
  :class:`~taskq.connections.WorkerConnections` consumes.
* :func:`enrich_pg_dsn` — shared DSN helper (inject password, force
  ``sslmode=require``, optionally override user).

The factories fetch a fresh credential each time they are invoked (at
pool / connection construction time). For long-lived workers, recreate
the pool on a schedule shorter than the credential lifetime. Redis
reconnects re-fetch automatically via the redis-py ``CredentialProvider``
adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from taskq.connections import ConnFactory, PoolFactory, RedisFactory

if TYPE_CHECKING:
    import asyncpg

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


def enrich_pg_dsn(dsn: str, credential: PgCredential) -> str:
    """Apply *credential* to *dsn*: inject the password and force ``sslmode=require``.

    When ``credential.username`` is set, it overrides the DSN's userinfo
    user (needed by Vault dynamic DB creds). The password is placed in
    the query string (``password=``) to avoid userinfo-encoding edge
    cases and to preserve existing query parameters; the query value
    takes precedence at asyncpg's resolver over any password in the
    userinfo.

    ``sslmode=require`` is forced because every cloud provider that
    issues rotating tokens (Azure, AWS, GCP) mandates TLS for token auth.
    """
    parsed = urlparse(str(dsn))
    query = parse_qs(parsed.query)
    query["password"] = [credential.password]
    query["sslmode"] = ["require"]
    if credential.username is not None:
        query["user"] = [credential.username]
    new_query = urlencode({k: v[0] for k, v in query.items()})
    return urlunparse(parsed._replace(query=new_query))


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

    Each invocation fetches a fresh :class:`PgCredential` from *provider*,
    enriches *dsn* with it, and calls ``asyncpg.create_pool``. The pool is
    owned by the worker (entered on its ``AsyncExitStack``).

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
        enriched = enrich_pg_dsn(dsn, credential)
        kwargs: dict[str, Any] = {
            "dsn": enriched,
            "min_size": min_size,
            "max_size": max_size,
            "max_inactive_connection_lifetime": max_inactive_connection_lifetime,
        }
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
    :class:`taskq.TaskQ`'s ``pg_conn_factory``.
    """
    import asyncpg

    async def factory() -> asyncpg.Connection:
        credential = await provider.get_pg_credential()
        enriched = enrich_pg_dsn(dsn, credential)
        return await asyncpg.connect(enriched)

    return factory


def make_redis_client_factory(
    url: str | None,
    provider: RedisCredentialProvider,
    **client_kwargs: Any,
) -> RedisFactory:
    """Build a :data:`~taskq.connections.RedisFactory` backed by *provider*.

    ``url`` is the Redis URL **without** credentials. The factory attaches
    a redis-py ``CredentialProvider`` that delegates to *provider*, so
    reconnects re-fetch the credential automatically.

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
        client_kwargs.setdefault("decode_responses", False)
        return redis_async.Redis.from_url(
            url,
            credential_provider=adapter,
            **client_kwargs,
        )

    return factory

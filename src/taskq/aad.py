"""Microsoft Entra ID (AAD) credential providers.

This module is part of the **``taskq[aad]``** optional extra. It provides
:class:`~taskq.auth.PgCredentialProvider` and
:class:`~taskq.auth.RedisCredentialProvider` implementations backed by
Microsoft Entra ID (Azure Active Directory) managed identities, plus the
raw token fetchers for users building their own providers. Install with::

    pip install 'taskq-py[aad]'

Usage
-----

::

    from azure.identity.aio import DefaultAzureCredential
    from taskq.auth import make_pg_pool_factory, make_redis_client_factory
    from taskq.aad import EntraIdProvider

    cred = DefaultAzureCredential()
    provider = EntraIdProvider(cred)

    WorkerConnections(
        dispatcher_pool_factory=make_pg_pool_factory(
            settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
        ),
        redis_client_factory=make_redis_client_factory(settings.redis_url, provider),
    )

The provider implements **both** Protocols — pass the same instance to
:func:`~taskq.auth.make_pg_pool_factory` and
:func:`~taskq.auth.make_redis_client_factory`. For PG-only or Redis-only
deployments, use :class:`EntraIdPgProvider` / :class:`EntraIdRedisProvider`
individually.

Credentials
-----------

The helpers accept any object exposing ``get_token(*scopes) -> AccessToken``
— i.e. :class:`azure.core.credentials.TokenCredential` (sync) **or** its
async counterpart from :mod:`azure.identity.aio` (e.g.
:class:`azure.identity.aio.DefaultAzureCredential`). See
:data:`AadCredential`. The credential is **caller-owned**: create it once
per process (async credentials are async context managers — close them in
your lifespan). Pass ``None`` to use a default async
``DefaultAzureCredential`` per call.

This module never imports ``azure.identity`` at module top level — the
import is deferred to call time so ``import taskq.aad`` is safe without
the extra installed.
"""

from __future__ import annotations

import base64
import inspect
import json
from typing import Any, Protocol, runtime_checkable

from taskq.auth import (
    PgCredential,
    PgCredentialProvider,
    RedisCredential,
    RedisCredentialProvider,
)

__all__ = [
    "PG_TOKEN_SCOPE",
    "REDIS_TOKEN_SCOPE",
    "AadCredential",
    "EntraIdPgProvider",
    "EntraIdProvider",
    "EntraIdRedisProvider",
    "fetch_pg_access_token",
    "fetch_redis_credentials",
]

# Azure resource scopes for AAD token requests.
PG_TOKEN_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"  # noqa: S105  # Why: Azure AAD resource scope URI for PostgreSQL, not a password.
REDIS_TOKEN_SCOPE = "https://redis.azure.com/.default"  # noqa: S105  # Why: Azure AAD resource scope URI for Redis, not a password.


# ── Credential protocol ────────────────────────────────────────────────


@runtime_checkable
class AadCredential(Protocol):
    """Structural protocol for an Azure credential with ``get_token``.

    Matches both the **sync** :class:`azure.core.credentials.TokenCredential`
    (``get_token`` returns :class:`~azure.core.credentials.AccessToken`)
    and the **async** credentials from :mod:`azure.identity.aio`
    (``get_token`` returns an awaitable). :func:`_get_token` await-detects
    the result so either form works.
    """

    def get_token(
        self,
        *scopes: str,
        claims: str | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Request an access token for *scopes* (sync or async)."""
        ...


# ── Credential / token helpers ─────────────────────────────────────────


def _require_azure_identity() -> Any:
    """Import ``azure.identity`` lazily.

    Raises :class:`ImportError` with install instructions if the ``[aad]``
    extra is not installed.
    """
    try:
        import azure.identity  # type: ignore[import-not-found]  # Why: optional [aad] extra; deferred so the module is import-safe without it.
    except ImportError as exc:
        raise ImportError(
            "taskq[aad] is required for Microsoft Entra ID authentication. "
            "Install it with: pip install 'taskq-py[aad]'"
        ) from exc
    return azure.identity


def _default_credential() -> AadCredential:
    """Return a default async ``DefaultAzureCredential``."""
    azure_identity_aio = _require_azure_identity().aio
    return azure_identity_aio.DefaultAzureCredential()  # type: ignore[no-any-return]  # Why: azure.identity.aio has no stubs-exposed concrete return type; the instance satisfies AadCredential structurally.


async def _get_token(credential: AadCredential, scope: str) -> str:
    """Call ``credential.get_token(scope)`` and return ``.token``.

    Await-detects the result so both sync (:mod:`azure.identity`) and async
    (:mod:`azure.identity.aio`) credentials work.
    """
    result = credential.get_token(scope)
    if inspect.isawaitable(result):
        result = await result
    return result.token


async def fetch_pg_access_token(credential: AadCredential | None = None) -> str:
    """Fetch a fresh AAD access token for Azure Database for PostgreSQL.

    ``credential`` defaults to a fresh async
    :class:`azure.identity.aio.DefaultAzureCredential`; pass your own
    (sync or async) to reuse a process-wide credential.
    """
    cred = credential if credential is not None else _default_credential()
    return await _get_token(cred, PG_TOKEN_SCOPE)


async def fetch_redis_credentials(
    credential: AadCredential | None = None,
    *,
    username: str | None = None,
) -> tuple[str, str]:
    """Fetch AAD credentials ``(username, password)`` for Azure Cache for Redis.

    The password is the AAD token. The username is the managed identity's
    **object ID** — decoded from the JWT ``oid`` claim — unless ``username``
    is passed explicitly (recommended in production: pass the object ID to
    avoid relying on JWT shape).
    """
    cred = credential if credential is not None else _default_credential()
    token = await _get_token(cred, REDIS_TOKEN_SCOPE)
    if username is not None:
        return username, token
    oid = _decode_jwt_oid(token)
    if oid is None:
        raise ValueError(
            "Could not decode 'oid' claim from the AAD token. Pass username= "
            "explicitly with the managed identity's object ID."
        )
    return oid, token


def _decode_jwt_oid(jwt: str) -> str | None:
    """Decode a JWT's payload and return the ``oid`` claim, or ``None``."""
    parts = jwt.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        claims = json.loads(decoded)
    except (ValueError, json.JSONDecodeError):
        return None
    oid = claims.get("oid")
    return oid if isinstance(oid, str) else None


# ── Provider implementations ───────────────────────────────────────────


class EntraIdPgProvider(PgCredentialProvider):
    """:class:`~taskq.auth.PgCredentialProvider` backed by Microsoft Entra ID.

    Returns the AAD token as the Postgres password; the DSN's existing
    user (the AAD principal name) is preserved.

    ``credential`` defaults to a fresh async ``DefaultAzureCredential``
    per call; pass a process-wide credential to reuse it.
    """

    def __init__(self, credential: AadCredential | None = None) -> None:
        self._credential = credential

    async def get_pg_credential(self) -> PgCredential:
        token = await fetch_pg_access_token(self._credential)
        return PgCredential(password=token)


class EntraIdRedisProvider(RedisCredentialProvider):
    """:class:`~taskq.auth.RedisCredentialProvider` backed by Microsoft Entra ID.

    Returns ``(managed-identity object ID, AAD token)``. Pass ``username``
    explicitly in production to avoid JWT decoding on every reconnect.
    """

    def __init__(
        self,
        credential: AadCredential | None = None,
        *,
        username: str | None = None,
    ) -> None:
        self._credential = credential
        self._username = username

    async def get_redis_credential(self) -> RedisCredential:
        username, token = await fetch_redis_credentials(
            self._credential, username=self._username
        )
        return RedisCredential(username=username, password=token)


class EntraIdProvider(EntraIdPgProvider, EntraIdRedisProvider):
    """AAD provider implementing **both** PG and Redis Protocols.

    Convenience class for deployments that use AAD for both Postgres and
    Redis — pass one instance to :func:`~taskq.auth.make_pg_pool_factory`
    and :func:`~taskq.auth.make_redis_client_factory`.
    """

    def __init__(
        self,
        credential: AadCredential | None = None,
        *,
        redis_username: str | None = None,
    ) -> None:
        EntraIdPgProvider.__init__(self, credential)
        EntraIdRedisProvider.__init__(self, credential, username=redis_username)

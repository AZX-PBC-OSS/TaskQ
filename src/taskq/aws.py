"""AWS IAM database authentication providers for Amazon RDS Postgres.

This module is part of the **``taskq[aws]``** optional extra. It provides
a :class:`~taskq.auth.PgCredentialProvider` implementation backed by AWS
IAM RDS authentication, plus the raw token fetcher for users building
their own providers. Install with::

    pip install 'taskq-py[aws]'

Usage
-----

::

    from taskq.auth import make_pg_pool_factory
    from taskq.aws import RdsIamProvider

    provider = RdsIamProvider(settings.pg_dsn_direct, region="us-east-1")

    WorkerConnections(
        dispatcher_pool_factory=make_pg_pool_factory(
            settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
        ),
    )

How AWS IAM RDS auth works
--------------------------

AWS RDS Postgres supports IAM database authentication: instead of a static
password, you request a SigV4-signed auth token from the RDS API
(``generate_db_auth_token``) and use it as the Postgres password. The token
is valid for **15 minutes** (:data:`RDS_TOKEN_LIFETIME_SECONDS`). The
factory builders hand the token to asyncpg as a ``password=`` callable
that is invoked per physical connection, so every connection the pool
opens - at creation, on growth, on idle-recycle replacement - signs a
fresh token; no reload schedule is needed for token freshness. ``SIGHUP``
/ ``TASKQ_RELOAD_INTERVAL`` remain the way to force a full pool rebuild
(see ``taskq.worker.deps.reload_credentials``).

``boto3`` is synchronous. ``generate_db_auth_token`` itself is local
SigV4 signing, but resolving the ambient credential chain
(``get_frozen_credentials``) may perform blocking STS/IMDS HTTPS calls
when credentials are near expiry, so the provider offloads the call to
a thread rather than stalling the event loop. This module never imports
``boto3`` at module top level, the import is deferred so
``import taskq.aws`` is safe without the extra installed.

Prerequisites
-------------

* Enable IAM database authentication on the RDS instance.
* Create a database user mapped to an IAM principal
  (``CREATE USER myiamuser; GRANT rds_iam TO myiamuser;``).
* Grant the IAM principal (user/role) permission to call
  ``rds-db:connect`` via an IAM policy.
* The DSN's ``user`` must be the IAM-mapped database user.
* ``sslmode=require`` is enforced by :func:`~taskq.auth.enrich_pg_dsn`.
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import unquote, urlparse

from taskq.auth import PgCredential, PgCredentialProvider

__all__ = [
    "RDS_TOKEN_LIFETIME_SECONDS",
    "RdsIamProvider",
    "fetch_rds_iam_token",
]

# AWS IAM RDS auth tokens are valid for 15 minutes.
RDS_TOKEN_LIFETIME_SECONDS = 900


# ── boto3 import helper ────────────────────────────────────────────────


def _require_boto3() -> Any:
    """Import ``boto3`` lazily.

    Raises :class:`ImportError` with install instructions if the ``[aws]``
    extra is not installed.
    """
    try:
        import boto3  # type: ignore[import-not-found]  # Why: optional [aws] extra; deferred so the module is import-safe without it.
    except ImportError as exc:
        raise ImportError(
            "taskq[aws] is required for AWS IAM RDS authentication. "
            "Install it with: pip install 'taskq-py[aws]'"
        ) from exc
    return boto3


def _parse_hostspec(hostspec: str) -> tuple[str, int]:
    """Parse one libpq hostspec (``host``, ``host:port``, ``[::1]:port``).

    Bracket-aware (an IPv6 literal's colons must not feed the port split)
    and per-hostspec: the port cast sees one host's port, never the joined
    ``5432,pg2:5432`` string urlparse's ``.port`` chokes on.
    """
    if hostspec.startswith("["):
        host, _, rest = hostspec[1:].partition("]")
        port_str = rest[1:] if rest.startswith(":") else ""
    else:
        host, _, port_str = hostspec.partition(":")
    return (host or "localhost"), (int(port_str) if port_str else 5432)


def _parse_dsn(dsn: str) -> tuple[str, int, str]:
    """Extract ``(hostname, port, username)`` from a Postgres DSN.

    The username is percent-decoded (urlparse keeps the raw encoding, but
    the IAM token is signed for the literal DB username). May be empty ,
    the caller decides whether that's an error (an explicit ``username=``
    parameter can rescue a userless DSN).

    Multi-host DSNs (``postgresql://u@pg1:5432,pg2:5432/db``, the libpq
    failover form) parse like :func:`taskq.auth.enrich_pg_dsn` reads them:
    the netloc is partitioned at the LAST ``@`` (a userinfo password may
    itself contain ``@``), then the FIRST hostspec - the one libpq tries
    first, and therefore the one a token should be signed for - provides
    the ``(hostname, port)`` the token fetch addresses. The port cast
    runs per hostspec, so the joined form never reaches urlparse's
    ``.port`` (which cannot cast ``5432,pg2:5432``). Every hostspec's
    credential slot is the DSN's single shared userinfo password, so a
    token issued for the first host serves every hostspec through the
    same rewriting :func:`taskq.auth.enrich_pg_dsn` applies.
    """
    parsed = urlparse(str(dsn))
    username = unquote(parsed.username or "")
    # Last-@ partition, enrich_pg_dsn's discipline: userinfo (which may
    # contain @) is separated from the (possibly multi-)hostspec exactly
    # the way the DSN rewriters do, so the two cannot disagree about
    # where the credential ends and the hosts begin.
    hostspec = parsed.netloc.rpartition("@")[2]
    first, comma, _ = hostspec.partition(",")
    if not comma:
        # Single host: urlparse's own parsing, unchanged (it lowercases
        # the host and raises its own error on a malformed port).
        return parsed.hostname or "localhost", parsed.port or 5432, username
    hostname, port = _parse_hostspec(first)
    return hostname, port, username


def _build_rds_client(region: str | None) -> Any:
    """Build a ``boto3.client('rds')`` from the ambient credential chain.

    Blocking (service-model load, credential-chain resolution); callers on
    the event loop offload it to a thread.
    """
    boto3 = _require_boto3()
    client_kwargs: dict[str, Any] = {}
    if region is not None:
        client_kwargs["region_name"] = region
    return boto3.client("rds", **client_kwargs)


# ── Token fetcher ──────────────────────────────────────────────────────


def fetch_rds_iam_token(
    *,
    hostname: str,
    port: int,
    username: str,
    region: str | None = None,
    client: Any | None = None,
) -> str:
    """Fetch an AWS RDS IAM auth token for use as the Postgres password.

    ``client`` defaults to a ``boto3.client('rds')`` built with the
    ambient AWS credential chain (env vars, instance role, etc.). Pass
    ``region`` to pin the region when the client is not supplied.

    The token is a SigV4-signed URL valid for 15 minutes
    (:data:`RDS_TOKEN_LIFETIME_SECONDS`). ``region`` is passed through to
    botocore untouched, ``None`` lets botocore fall back to the client's
    ambient region (an empty string would produce a signature scoped to
    ``date//rds-db/aws4_request``, which RDS rejects).

    This is a synchronous function and may perform blocking network I/O
    (STS/IMDS credential refresh); async callers should offload it to a
    thread, as :class:`RdsIamProvider` does.
    """
    resolved = _build_rds_client(region) if client is None else client
    return resolved.generate_db_auth_token(
        DBHostname=hostname,
        Port=port,
        DBUsername=username,
        Region=region,
    )


# ── Provider implementation ────────────────────────────────────────────


class RdsIamProvider(PgCredentialProvider):
    """:class:`~taskq.auth.PgCredentialProvider` backed by AWS IAM RDS auth.

    Returns the IAM auth token as the Postgres password; the DSN's
    existing user (the IAM-mapped DB user) is preserved.

    ``client`` defaults to a ``boto3.client('rds')`` from the ambient
    credential chain, built once on first use and reused for the
    provider's lifetime; pass ``region`` to pin it. ``username`` defaults
    to the DSN's userinfo user.

    Multi-host DSNs (``postgresql://iamuser@pg1:5432,pg2:5432/taskq``,
    the libpq failover form) are accepted: the token is signed for the
    FIRST hostspec (the one libpq tries first) and returned as the
    credential's password, which - through
    :func:`taskq.auth.enrich_pg_dsn` or the factory builders in
    :mod:`taskq.auth` - lands in the DSN's single shared userinfo
    password slot, i.e. every hostspec's password slot. A failover to a
    later host whose endpoint differs re-signs on the next pool rebuild;
    see the deployment guide's multi-host section.

    Why the client is built once: a token is signed for every physical
    connection the pool opens, and ``boto3.client`` is not a cheap call -
    it loads the service model each time and runs the default-session
    setup, which is not safe to race from the threads concurrent pool
    growth would put it on. The first fetch builds the client under a
    lock; later fetches sign with it.
    """

    def __init__(
        self,
        dsn: str,
        *,
        region: str | None = None,
        client: Any | None = None,
        username: str | None = None,
    ) -> None:
        hostname, port, parsed_username = _parse_dsn(dsn)
        self._hostname = hostname
        self._port = port
        self._username = username if username is not None else parsed_username
        if not self._username:
            # Deliberately no DSN in the message, it may carry a userinfo
            # password, which must not land in tracebacks / log aggregation.
            raise ValueError(
                "DSN has no username; AWS IAM RDS auth requires the "
                "IAM-mapped database user in the DSN userinfo (or pass username=)."
            )
        self._region = region
        self._client = client
        # Safe to create outside a running loop (the CLI builds the provider
        # at settings time): asyncio.Lock binds to the loop on first use,
        # not at construction.
        self._client_lock = asyncio.Lock()

    async def _resolve_client(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                self._client = await asyncio.to_thread(_build_rds_client, self._region)
        return self._client

    async def get_pg_credential(self) -> PgCredential:
        client = await self._resolve_client()
        # Offloaded to a thread: although generate_db_auth_token is local
        # SigV4 signing, the ambient credential chain may perform blocking
        # STS/IMDS HTTPS refreshes, which must not stall the event loop.
        token = await asyncio.to_thread(
            fetch_rds_iam_token,
            hostname=self._hostname,
            port=self._port,
            username=self._username,
            region=self._region,
            client=client,
        )
        return PgCredential(password=token)

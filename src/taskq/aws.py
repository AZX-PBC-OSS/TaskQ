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
is valid for **15 minutes** (:data:`RDS_TOKEN_LIFETIME_SECONDS`), so each
call to a factory fetches a fresh token. For long-lived workers, send
``SIGHUP`` to the worker process on a schedule shorter than 15 minutes —
the factory is re-invoked automatically to rebuild the pool with a fresh
token (see ``taskq.worker.deps.reload_credentials``); no restart needed.

``boto3`` is synchronous; ``generate_db_auth_token`` is local SigV4 signing
(no network I/O), so it is safe to call directly from an async provider.
This module never imports ``boto3`` at module top level — the import is
deferred so ``import taskq.aws`` is safe without the extra installed.

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

from typing import Any

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


def _parse_dsn(dsn: str) -> tuple[str, int, str]:
    """Extract ``(hostname, port, username)`` from a Postgres DSN."""
    from urllib.parse import urlparse

    parsed = urlparse(str(dsn))
    hostname = parsed.hostname or "localhost"
    port = parsed.port or 5432
    username = parsed.username or ""
    if not username:
        raise ValueError(
            f"DSN {dsn!r} has no username; AWS IAM RDS auth requires the "
            "IAM-mapped database user in the DSN userinfo (or pass username=)."
        )
    return hostname, port, username


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
    (:data:`RDS_TOKEN_LIFETIME_SECONDS`). ``generate_db_auth_token`` is
    local SigV4 signing — no network I/O — so this is safe to call from
    an async context without an executor.
    """
    if client is None:
        boto3 = _require_boto3()
        client_kwargs: dict[str, Any] = {}
        if region is not None:
            client_kwargs["region_name"] = region
        resolved = boto3.client("rds", **client_kwargs)
    else:
        resolved = client
    return resolved.generate_db_auth_token(
        DBHostname=hostname,
        Port=port,
        DBUsername=username,
        Region=region or "",
    )


# ── Provider implementation ────────────────────────────────────────────


class RdsIamProvider(PgCredentialProvider):
    """:class:`~taskq.auth.PgCredentialProvider` backed by AWS IAM RDS auth.

    Returns the IAM auth token as the Postgres password; the DSN's
    existing user (the IAM-mapped DB user) is preserved.

    ``client`` defaults to a ``boto3.client('rds')`` from the ambient
    credential chain; pass ``region`` to pin it. ``username`` defaults to
    the DSN's userinfo user.
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
        self._region = region
        self._client = client

    async def get_pg_credential(self) -> PgCredential:
        # generate_db_auth_token is local SigV4 signing (no network I/O),
        # so no executor is needed — safe to call from an async context.
        token = fetch_rds_iam_token(
            hostname=self._hostname,
            port=self._port,
            username=self._username,
            region=self._region,
            client=self._client,
        )
        return PgCredential(password=token)

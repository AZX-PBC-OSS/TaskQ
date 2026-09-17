"""HashiCorp Vault dynamic database credential providers.

This module is part of the **``taskq[vault]``** optional extra. It provides
a :class:`~taskq.auth.PgCredentialProvider` implementation backed by
Vault's database secrets engine, which issues **dynamic** username +
password pairs with a configurable lease TTL. Install with::

    pip install 'taskq-py[vault]'

Usage
-----

::

    import hvac
    from taskq.auth import make_pg_pool_factory
    from taskq.vault import VaultDynamicDbProvider

    client = hvac.Client(url="https://vault.example", token="...")
    provider = VaultDynamicDbProvider(client, role="taskq-readonly")

    WorkerConnections(
        dispatcher_pool_factory=make_pg_pool_factory(
            settings.pg_dsn_direct, provider, max_size=settings.dispatcher_pool_size,
        ),
    )

How Vault dynamic DB creds work
-------------------------------

Vault's database secrets engine generates a fresh database user + password
pair on each ``generate_credentials`` call, scoped to a named role. The
returned lease has a TTL (configurable per role); when it expires Vault
revokes the database user. This module fetches a fresh pair each time the
factory is invoked, and :func:`~taskq.auth.enrich_pg_dsn` overrides both
the DSN's ``user`` and ``password`` with the dynamic values.

Unlike token-as-password providers (AAD, AWS IAM RDS), Vault issues a
**fresh username** on each lease — the ``PgCredential.username`` field is
always set — and the pair is only valid together. The factory builders
therefore pin one lease per pool / dedicated connection: ``user=`` is the
lease's username and every physical connection asyncpg opens
authenticates with that lease's password (see
:func:`~taskq.auth.make_pg_pool_factory`). Rotation happens when the
factory is re-invoked, so run the worker with ``TASKQ_RELOAD_INTERVAL``
(or send ``SIGHUP``) on a schedule shorter than the role's lease TTL —
the pool is rebuilt on a fresh lease (see
``taskq.worker.deps.reload_credentials``); no restart needed. Each
issued lease is logged (``vault-lease-issued``: ``lease_id``,
``lease_duration``, ``username``) so the reload schedule can be checked
against the TTL Vault actually granted.

``hvac`` is synchronous; ``generate_credentials`` does network I/O, so the
provider offloads it to a thread via :func:`asyncio.to_thread` to avoid
blocking the event loop. This module never imports ``hvac`` at module top
level — the import is deferred so ``import taskq.vault`` is safe without
the extra installed.

Prerequisites
-------------

* Enable the database secrets engine on the Vault server
  (``vault secrets enable database``).
* Configure a connection and a role pointing at your Postgres
  (``vault write database/config/my-pg …`` and
  ``vault write database/roles/taskq-readonly …``).
* The DSN's host/port/dbname must point at the Postgres Vault provisions
  creds for; the user/password in the DSN are overridden by the dynamic
  values.
"""

from __future__ import annotations

import asyncio
from typing import Any

from taskq.auth import PgCredential, PgCredentialProvider
from taskq.obs import get_logger

logger = get_logger(__name__)

__all__ = [
    "VaultDynamicDbProvider",
]


# ── Provider implementation ────────────────────────────────────────────


class VaultDynamicDbProvider(PgCredentialProvider):
    """:class:`~taskq.auth.PgCredentialProvider` backed by Vault's database secrets engine.

    Fetches a dynamic ``(username, password)`` pair — a new Vault lease —
    from ``secrets.database.generate_credentials`` on each
    :meth:`get_pg_credential` call. The factory builders call it once per
    pool / connection build and pin the pair, so one lease is issued per
    build, not per physical connection. The ``hvac`` client is synchronous
    and the call does network I/O, so it is offloaded to a thread via
    :func:`asyncio.to_thread`.

    ``client`` is an ``hvac.Client`` instance (caller-owned — you manage
    its lifecycle). ``role`` is the Vault database role name. ``mount_point``
    defaults to ``"database"`` (the standard engine mount path).
    """

    def __init__(
        self,
        client: Any,
        role: str,
        *,
        mount_point: str = "database",
    ) -> None:
        self._client = client
        self._role = role
        self._mount_point = mount_point
        # The TTL of the lease this provider last issued, in seconds. Nothing
        # here schedules rotation with it: it is the bound an operator's
        # reload schedule has to beat, surfaced so the pool factory's
        # pinned-pair startup warning can name it (see taskq.auth).
        self.last_lease_duration: float | None = None

    async def get_pg_credential(self) -> PgCredential:
        def _fetch() -> dict[str, Any]:
            response: dict[str, Any] = self._client.secrets.database.generate_credentials(
                name=self._role,
                mount_point=self._mount_point,
            )
            return response

        response = await asyncio.to_thread(_fetch)
        data: dict[str, Any] = response["data"]
        username, password = data["username"], data["password"]
        self.last_lease_duration = response.get("lease_duration")
        # The lease TTL is the bound the operator's reload schedule must beat;
        # logging it beside the identity Vault issued is what lets a later
        # "password authentication failed for user v-…" be traced to an
        # expired lease rather than a misconfigured role.
        logger.info(
            "vault-lease-issued",
            role=self._role,
            mount_point=self._mount_point,
            username=username,
            lease_id=response.get("lease_id"),
            lease_duration=response.get("lease_duration"),
            renewable=response.get("renewable"),
        )
        return PgCredential(username=username, password=password)

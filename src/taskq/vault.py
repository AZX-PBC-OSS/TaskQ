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
always set. For long-lived workers, send ``SIGHUP`` to the worker process
on a schedule shorter than the lease TTL — the factory is re-invoked
automatically to rebuild the pool with a fresh lease (see
``taskq.worker.deps.reload_credentials``); no restart needed.

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

__all__ = [
    "VaultDynamicDbProvider",
]


# ── Provider implementation ────────────────────────────────────────────


class VaultDynamicDbProvider(PgCredentialProvider):
    """:class:`~taskq.auth.PgCredentialProvider` backed by Vault's database secrets engine.

    Fetches a dynamic ``(username, password)`` pair from
    ``secrets.database.generate_credentials`` on each
    :meth:`get_pg_credential` call. The ``hvac`` client is synchronous and
    the call does network I/O, so it is offloaded to a thread via
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

    async def get_pg_credential(self) -> PgCredential:
        def _fetch() -> tuple[str, str]:
            response = self._client.secrets.database.generate_credentials(
                name=self._role,
                mount_point=self._mount_point,
            )
            data: dict[str, Any] = response["data"]
            return data["username"], data["password"]

        username, password = await asyncio.to_thread(_fetch)
        return PgCredential(username=username, password=password)

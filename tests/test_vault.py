"""Tests for taskq.vault — HashiCorp Vault dynamic DB credential providers.

Uses a fake hvac client (no real Vault calls) to verify the provider
implementation. The ``[vault]`` extra (hvac) is installed in the dev
environment.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from taskq.auth import PgCredential, PgCredentialProvider
from taskq.vault import VaultDynamicDbProvider

# ── Fake hvac client ───────────────────────────────────────────────────


def _fake_hvac_client(username: str = "v-root-dyn-user-abc", password: str = "dyn-pw-xyz") -> Any:  # noqa: S107  # Why: test fixture password, not a real credential.
    """Build a fake hvac client whose generate_credentials returns a canned pair."""
    client = MagicMock()
    client.secrets.database.generate_credentials.return_value = {
        "data": {"username": username, "password": password}
    }
    return client


# ── VaultDynamicDbProvider ─────────────────────────────────────────────


async def test_vault_provider_returns_dynamic_username_and_password() -> None:
    """VaultDynamicDbProvider returns a PgCredential with both username and password."""
    client = _fake_hvac_client(username="v-user-123", password="v-pw-456")
    provider = VaultDynamicDbProvider(client, role="taskq-readonly")
    result = await provider.get_pg_credential()
    assert isinstance(result, PgCredential)
    assert result.username == "v-user-123"
    assert result.password == "v-pw-456"


async def test_vault_provider_calls_generate_credentials_with_role() -> None:
    """The provider calls generate_credentials with the role name and mount_point."""
    client = _fake_hvac_client()
    provider = VaultDynamicDbProvider(client, role="my-role", mount_point="db")
    await provider.get_pg_credential()
    client.secrets.database.generate_credentials.assert_called_once_with(
        name="my-role", mount_point="db"
    )


async def test_vault_provider_default_mount_point() -> None:
    """The default mount_point is 'database'."""
    client = _fake_hvac_client()
    provider = VaultDynamicDbProvider(client, role="my-role")
    await provider.get_pg_credential()
    client.secrets.database.generate_credentials.assert_called_once_with(
        name="my-role", mount_point="database"
    )


async def test_vault_provider_fetches_fresh_creds_each_call() -> None:
    """Each call to get_pg_credential fetches a fresh credential pair."""
    client = _fake_hvac_client()
    provider = VaultDynamicDbProvider(client, role="my-role")
    await provider.get_pg_credential()
    await provider.get_pg_credential()
    assert client.secrets.database.generate_credentials.call_count == 2


def test_vault_provider_protocol_matching() -> None:
    """VaultDynamicDbProvider satisfies PgCredentialProvider at runtime."""
    provider = VaultDynamicDbProvider(_fake_hvac_client(), role="my-role")
    assert isinstance(provider, PgCredentialProvider)

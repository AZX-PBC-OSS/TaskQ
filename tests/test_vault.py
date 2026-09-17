"""Tests for taskq.vault — HashiCorp Vault dynamic DB credential providers.

Uses a fake hvac client (no real Vault calls) to verify the provider
implementation. Requires the ``[vault]`` extra (hvac); skips when the
extra is not installed.
"""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog

pytest.importorskip("hvac", reason="requires taskq[vault]")

import hvac.exceptions

from taskq.auth import PgCredential, PgCredentialProvider, make_pg_pool_factory
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


# ── Error paths ────────────────────────────────────────────────────────


async def test_vault_provider_propagates_hvac_error() -> None:
    """An hvac error from generate_credentials propagates unchanged through
    asyncio.to_thread — callers must see the real Vault failure, not a wrapper."""
    client = _fake_hvac_client()
    client.secrets.database.generate_credentials.side_effect = hvac.exceptions.VaultError(
        "permission denied"
    )
    provider = VaultDynamicDbProvider(client, role="my-role")
    with pytest.raises(hvac.exceptions.VaultError):
        await provider.get_pg_credential()


async def test_vault_provider_missing_data_key_raises_key_error() -> None:
    """A Vault response without a 'data' key raises KeyError (pinned current
    behavior — the provider does not pre-validate the response shape)."""
    client = _fake_hvac_client()
    client.secrets.database.generate_credentials.return_value = {}
    provider = VaultDynamicDbProvider(client, role="my-role")
    with pytest.raises(KeyError):
        await provider.get_pg_credential()


async def test_vault_provider_missing_username_or_password_raises_key_error() -> None:
    """A Vault response missing 'username'/'password' under 'data' raises
    KeyError (pinned current behavior)."""
    client = _fake_hvac_client()
    client.secrets.database.generate_credentials.return_value = {"data": {"username": "u"}}
    provider = VaultDynamicDbProvider(client, role="my-role")
    with pytest.raises(KeyError):
        await provider.get_pg_credential()


# ── Pool factory integration (username+password are one lease) ────────


def _fake_hvac_client_with_distinct_leases() -> Any:
    """Fake hvac client issuing a NEW username/password lease per call —
    exactly what Vault's database secrets engine does."""
    client = MagicMock()
    counter = {"n": 0}

    def _generate(name: str, mount_point: str) -> dict[str, Any]:
        counter["n"] += 1
        n = counter["n"]
        return {
            "lease_id": f"database/creds/{name}/lease-{n}",
            "lease_duration": 3600,
            "renewable": True,
            "data": {"username": f"v-{name}-{n}", "password": f"pw-{n}"},
        }

    client.secrets.database.generate_credentials.side_effect = _generate
    return client


async def _resolve_password(value: Any) -> str:
    """Resolve ``password=`` the way asyncpg's ``_connect_addr`` does."""
    result = value() if callable(value) else value
    if inspect.isawaitable(result):
        result = await result
    return str(result)


async def test_pool_factory_hands_every_connection_the_pinned_lease_password() -> None:
    """A pool built on a Vault lease pins ``user=`` to that lease's username;
    every physical connection asyncpg opens must authenticate with THAT
    lease's password. Re-fetching per connection would burn a new lease
    each time and hand asyncpg a password for a username the pool was
    never built with."""
    client = _fake_hvac_client_with_distinct_leases()
    provider = VaultDynamicDbProvider(client, role="taskq")
    factory = make_pg_pool_factory("postgresql://ignored@host/db", provider)

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=MagicMock())) as mock_create:
        await factory()

    kwargs = mock_create.call_args.kwargs
    assert kwargs["user"] == "v-taskq-1"
    # Pool creation, growth and idle-recycle replacement: three physical connections.
    for _ in range(3):
        assert await _resolve_password(kwargs["password"]) == "pw-1"
    assert client.secrets.database.generate_credentials.call_count == 1


async def test_pool_rebuild_takes_a_fresh_lease() -> None:
    """Rebuilding the pool (SIGHUP / reload_credentials re-invoking the factory)
    is the rotation point: it takes a fresh lease and pins its username."""
    client = _fake_hvac_client_with_distinct_leases()
    provider = VaultDynamicDbProvider(client, role="taskq")
    factory = make_pg_pool_factory("postgresql://ignored@host/db", provider)

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=MagicMock())) as mock_create:
        await factory()
        await factory()

    first, second = (call.kwargs for call in mock_create.call_args_list)
    assert (first["user"], await _resolve_password(first["password"])) == ("v-taskq-1", "pw-1")
    assert (second["user"], await _resolve_password(second["password"])) == ("v-taskq-2", "pw-2")
    assert client.secrets.database.generate_credentials.call_count == 2


async def test_vault_provider_logs_issued_lease_without_password() -> None:
    """Each issued lease is reported with the TTL Vault granted and the
    username - the facts an operator needs to size the reload schedule and to
    trace a later auth failure to an expired lease - and never the password."""
    client = _fake_hvac_client_with_distinct_leases()
    provider = VaultDynamicDbProvider(client, role="taskq")
    with structlog.testing.capture_logs() as logs:
        await provider.get_pg_credential()
    entry = next(log for log in logs if log["event"] == "vault-lease-issued")
    assert entry["username"] == "v-taskq-1"
    assert entry["lease_id"] == "database/creds/taskq/lease-1"
    assert entry["lease_duration"] == 3600
    assert "pw-1" not in repr(entry)

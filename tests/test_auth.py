"""Tests for taskq.auth — vendor-neutral credential providers and factories.

Verifies the base interfaces (Protocols, credential carriers), the DSN
enrichment helper, and the factory builders using fake providers — no
real Postgres/Redis/Azure/AWS/Vault required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from taskq.auth import (
    PgCredential,
    PgCredentialProvider,
    RedisCredential,
    RedisCredentialProvider,
    enrich_pg_dsn,
    make_dedicated_conn_factory,
    make_pg_pool_factory,
    make_redis_client_factory,
)

# ── Fake providers ─────────────────────────────────────────────────────


class _FakePgProvider:
    """Fake PgCredentialProvider returning a canned credential."""

    def __init__(self, password: str = "tok-123", username: str | None = None) -> None:  # noqa: S107  # Why: test fixture password, not a real credential.
        self._password = password
        self._username = username
        self.calls = 0

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(password=self._password, username=self._username)


class _FakeRedisProvider:
    """Fake RedisCredentialProvider returning a canned credential."""

    def __init__(self, username: str = "redis-user", password: str = "redis-tok") -> None:  # noqa: S107  # Why: test fixture password, not a real credential.
        self._username = username
        self._password = password
        self.calls = 0

    async def get_redis_credential(self) -> RedisCredential:
        self.calls += 1
        return RedisCredential(username=self._username, password=self._password)


class _FakeDualProvider(_FakePgProvider, _FakeRedisProvider):
    """Implements both Protocols — one instance for PG + Redis."""

    def __init__(self) -> None:
        _FakePgProvider.__init__(self)
        _FakeRedisProvider.__init__(self)


# ── Credential carriers ────────────────────────────────────────────────


def test_pg_credential_defaults_username_none() -> None:
    """PgCredential.username defaults to None (preserve DSN user)."""
    cred = PgCredential(password="tok")
    assert cred.password == "tok"
    assert cred.username is None


def test_pg_credential_frozen() -> None:
    """PgCredential is frozen (immutable)."""
    cred = PgCredential(password="tok")
    with pytest.raises(Exception):  # noqa: B017  # Why: dataclass frozen raises FrozenInstanceError (a subclass of AttributeError)
        cred.password = "new"  # type: ignore[misc]


def test_redis_credential_frozen() -> None:
    """RedisCredential is frozen (immutable)."""
    cred = RedisCredential(username="u", password="p")
    with pytest.raises(Exception):  # noqa: B017  # Why: same as above
        cred.password = "new"  # type: ignore[misc]


# ── Protocol structural matching ───────────────────────────────────────


def test_pg_provider_protocol_runtime_checkable() -> None:
    """PgCredentialProvider is runtime-checkable and matches structural impls."""
    assert isinstance(_FakePgProvider(), PgCredentialProvider)
    assert isinstance(_FakeDualProvider(), PgCredentialProvider)


def test_redis_provider_protocol_runtime_checkable() -> None:
    """RedisCredentialProvider is runtime-checkable and matches structural impls."""
    assert isinstance(_FakeRedisProvider(), RedisCredentialProvider)
    assert isinstance(_FakeDualProvider(), RedisCredentialProvider)


# ── enrich_pg_dsn ──────────────────────────────────────────────────────


def test_enrich_pg_dsn_injects_password_and_forces_ssl() -> None:
    """enrich_pg_dsn puts the password in the query string and forces sslmode=require."""
    cred = PgCredential(password="my-token")
    result = enrich_pg_dsn("postgresql://user@host:5432/db", cred)
    assert "password=my-token" in result
    assert "sslmode=require" in result
    assert "host:5432" in result


def test_enrich_pg_dsn_preserves_existing_params() -> None:
    """Existing query parameters are preserved when enriching."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn(
        "postgresql://user@host:5432/db?application_name=taskq", cred
    )
    assert "application_name=taskq" in result
    assert "password=tok" in result
    assert "sslmode=require" in result


def test_enrich_pg_dsn_overrides_user_when_username_set() -> None:
    """When credential.username is set, the DSN user is overridden (Vault dynamic creds)."""
    cred = PgCredential(password="dyn-pw", username="dyn-user-abc")
    result = enrich_pg_dsn("postgresql://old-user@host:5432/db", cred)
    assert "user=dyn-user-abc" in result
    assert "password=dyn-pw" in result


def test_enrich_pg_dsn_preserves_user_when_username_none() -> None:
    """When credential.username is None, the DSN user is preserved (AAD/AWS token auth)."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn("postgresql://my-user@host:5432/db", cred)
    assert "my-user@" in result
    assert "user=" not in result  # user= query param not added


# ── make_pg_pool_factory ───────────────────────────────────────────────


async def test_make_pg_pool_factory_fetches_credential_and_creates_pool() -> None:
    """The factory fetches a credential from the provider then calls create_pool."""
    provider = _FakePgProvider(password="tok-999")
    factory = make_pg_pool_factory(
        "postgresql://user@host:5432/db", provider, max_size=8, command_timeout=5
    )

    fake_pool = MagicMock()
    fake_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=fake_pool)) as mock_create:
        pool = await factory()

    assert pool is fake_pool
    assert provider.calls == 1
    mock_create.assert_awaited_once()
    call_kwargs = mock_create.call_args.kwargs
    assert call_kwargs["max_size"] == 8
    assert call_kwargs["command_timeout"] == 5
    assert "password=tok-999" in call_kwargs["dsn"]
    assert "sslmode=require" in call_kwargs["dsn"]


async def test_make_pg_pool_factory_with_username_override() -> None:
    """When the provider returns a username, it's injected into the DSN."""
    provider = _FakePgProvider(password="pw", username="vault-user")
    factory = make_pg_pool_factory("postgresql://old@host:5432/db", provider)

    fake_pool = MagicMock()
    with patch("asyncpg.create_pool", new=AsyncMock(return_value=fake_pool)) as mock_create:
        await factory()

    dsn = mock_create.call_args.kwargs["dsn"]
    assert "user=vault-user" in dsn
    assert "password=pw" in dsn


# ── make_dedicated_conn_factory ────────────────────────────────────────


async def test_make_dedicated_conn_factory_fetches_credential_and_connects() -> None:
    """The factory fetches a credential then calls asyncpg.connect."""
    provider = _FakePgProvider(password="conn-tok")
    factory = make_dedicated_conn_factory("postgresql://user@host:5432/db", provider)

    fake_conn = MagicMock()
    with patch("asyncpg.connect", new=AsyncMock(return_value=fake_conn)) as mock_connect:
        conn = await factory()

    assert conn is fake_conn
    assert provider.calls == 1
    call_args = mock_connect.call_args
    assert "password=conn-tok" in call_args.args[0]
    assert "sslmode=require" in call_args.args[0]


# ── make_redis_client_factory ──────────────────────────────────────────


async def test_make_redis_client_factory_raises_when_url_none() -> None:
    """The factory raises RuntimeError when url is None and called."""
    provider = _FakeRedisProvider()
    factory = make_redis_client_factory(None, provider)
    with pytest.raises(RuntimeError, match="Redis URL is not configured"):
        await factory()


async def test_make_redis_client_factory_builds_with_credential_provider() -> None:
    """The factory attaches a credential_provider that delegates to the provider."""
    provider = _FakeRedisProvider(username="my-oid", password="redis-tok")
    factory = make_redis_client_factory(
        "rediss://cache.redis.cache.windows.net:6380", provider
    )

    fake_redis = MagicMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake_redis) as mock_from_url:
        client = await factory()

    assert client is fake_redis
    mock_from_url.assert_called_once()
    call_kwargs = mock_from_url.call_args.kwargs
    assert "credential_provider" in call_kwargs
    assert call_kwargs["decode_responses"] is False

    # redis-py's async connection calls get_credentials_async on every
    # (re)connect — not get_credentials — so that's the method that must
    # actually delegate to the provider.
    adapter = call_kwargs["credential_provider"]
    username, password = await adapter.get_credentials_async()
    assert username == "my-oid"
    assert password == "redis-tok"
    assert provider.calls == 1

    # The sync get_credentials is intentionally unsupported (async-only client).
    with pytest.raises(NotImplementedError):
        adapter.get_credentials()

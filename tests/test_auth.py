"""Tests for taskq.auth — vendor-neutral credential providers and factories.

Verifies the base interfaces (Protocols, credential carriers), the DSN
enrichment helper, and the factory builders using fake providers — no
real Postgres/Redis/Azure/AWS/Vault required.

DSN-semantics tests verify through asyncpg's own resolver
(``_parse_connect_dsn_and_args``) rather than string matching, because
asyncpg's precedence rules (userinfo beats query params, kwargs beat
both) are exactly what this module relies on.
"""

from __future__ import annotations

import dataclasses
import ssl as ssl_module
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
import structlog.testing
from asyncpg.connect_utils import (
    _parse_connect_dsn_and_args,  # pyright: ignore[reportAttributeAccessIssue]  # Why: private resolver — the only way to verify DSN semantics through asyncpg's real precedence rules; pinned to asyncpg 0.31.
)

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


def _resolve(dsn: str, **kwargs: Any) -> Any:
    """Run asyncpg's real DSN resolver and return the connection params."""
    _addrs, params = _parse_connect_dsn_and_args(
        dsn=dsn,
        host=None,
        port=None,
        user=kwargs.get("user"),
        password=kwargs.get("password"),
        passfile=None,
        database=None,
        ssl=None,
        service=None,
        servicefile=None,
        direct_tls=None,
        server_settings=None,
        target_session_attrs=None,
        krbsrvname=None,
        gsslib=None,
    )
    return params


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
    with pytest.raises(dataclasses.FrozenInstanceError):
        cred.password = "new"  # type: ignore[misc]


def test_redis_credential_frozen() -> None:
    """RedisCredential is frozen (immutable)."""
    cred = RedisCredential(username="u", password="p")
    with pytest.raises(dataclasses.FrozenInstanceError):
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
    """The enriched DSN resolves (via asyncpg's resolver) to the credential."""
    cred = PgCredential(password="my-token")
    result = enrich_pg_dsn("postgresql://user@host:5432/db", cred)
    params = _resolve(result)
    assert params.user == "user"
    assert params.password == "my-token"
    # sslmode=require → TLS context that does NOT verify the certificate.
    assert isinstance(params.ssl, ssl_module.SSLContext)
    assert params.ssl.verify_mode == ssl_module.CERT_NONE
    assert "host:5432" in result


def test_enrich_pg_dsn_preserves_existing_params() -> None:
    """Existing query parameters are preserved when enriching."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn("postgresql://user@host:5432/db?application_name=taskq", cred)
    assert "application_name=taskq" in result
    params = _resolve(result)
    assert params.password == "tok"
    assert isinstance(params.ssl, ssl_module.SSLContext)


def test_enrich_pg_dsn_credential_wins_over_userinfo_password() -> None:
    """A stale userinfo password MUST NOT shadow the injected credential.

    asyncpg applies userinfo before query params (both behind
    ``if password is None`` guards), so a query-string password loses —
    the credential must replace the userinfo password itself.
    """
    cred = PgCredential(password="fresh-token")
    result = enrich_pg_dsn("postgresql://olduser:oldpass@host/db", cred)
    params = _resolve(result)
    assert params.user == "olduser"
    assert params.password == "fresh-token"


def test_enrich_pg_dsn_overrides_user_when_username_set() -> None:
    """When credential.username is set, the resolved user is overridden (Vault dynamic creds)."""
    cred = PgCredential(password="dyn-pw", username="dyn-user-abc")
    result = enrich_pg_dsn("postgresql://old-user:old-pw@host:5432/db", cred)
    params = _resolve(result)
    assert params.user == "dyn-user-abc"
    assert params.password == "dyn-pw"


def test_enrich_pg_dsn_preserves_user_when_username_none() -> None:
    """When credential.username is None, the DSN user is preserved (AAD/AWS token auth)."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn("postgresql://my-user@host:5432/db", cred)
    params = _resolve(result)
    assert params.user == "my-user"


def test_enrich_pg_dsn_preserves_query_carried_user() -> None:
    """A user carried in the QUERY string (no userinfo in the DSN) must
    survive enrichment — dropping it would silently fall back to the OS
    user at connect time."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn("postgresql://host/db?user=principal%40tenant.onmicrosoft.com", cred)
    params = _resolve(result)
    assert params.user == "principal@tenant.onmicrosoft.com"
    assert params.password == "tok"


@pytest.mark.parametrize("mode", ["verify-ca", "verify-full"])
def test_enrich_pg_dsn_does_not_downgrade_stronger_sslmode(mode: str) -> None:
    """An explicit sslmode must survive enrichment — require skips
    certificate verification, so downgrading verify-* would expose the
    injected token to a MITM. (Asserted on the query param directly:
    resolving verify-* would need a root cert on disk.)"""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn(f"postgresql://user@host/db?sslmode={mode}", cred)
    assert parse_qs(urlparse(result).query)["sslmode"] == [mode]


def test_enrich_pg_dsn_special_char_password_round_trips() -> None:
    """AWS IAM tokens contain &=/%:+ — they must survive userinfo encoding."""
    cred = PgCredential(password="X-Amz-Sig=a+b/c==&x%y:z")
    result = enrich_pg_dsn("postgresql://user@host/db", cred)
    params = _resolve(result)
    assert params.password == "X-Amz-Sig=a+b/c==&x%y:z"


def test_enrich_pg_dsn_preserves_blank_and_multi_valued_params() -> None:
    """Blank-valued and repeated query params must not be silently dropped."""
    cred = PgCredential(password="tok")
    result = enrich_pg_dsn("postgresql://user@host/db?options=&x=1&x=2", cred)
    assert "options=" in result
    assert result.count("x=") == 2


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
    # The credential travels as kwargs (which beat userinfo and query
    # params in asyncpg's resolver) — not embedded in the DSN string.
    assert call_kwargs["password"] == "tok-999"
    assert "tok-999" not in call_kwargs["dsn"]
    assert "sslmode=require" in call_kwargs["dsn"]


async def test_make_pg_pool_factory_with_username_override() -> None:
    """When the provider returns a username, it's passed as the user kwarg."""
    provider = _FakePgProvider(password="pw", username="vault-user")
    factory = make_pg_pool_factory("postgresql://old@host:5432/db", provider)

    fake_pool = MagicMock()
    with patch("asyncpg.create_pool", new=AsyncMock(return_value=fake_pool)) as mock_create:
        await factory()

    call_kwargs = mock_create.call_args.kwargs
    assert call_kwargs["user"] == "vault-user"
    assert call_kwargs["password"] == "pw"
    assert "pw" not in call_kwargs["dsn"]


async def test_make_pg_pool_factory_kwargs_beat_stale_userinfo() -> None:
    """End-to-end through asyncpg's resolver: a DSN carrying stale
    userinfo credentials must not shadow the provider's fresh credential."""
    provider = _FakePgProvider(password="fresh-tok", username="fresh-user")
    factory = make_pg_pool_factory("postgresql://olduser:oldpass@host/db", provider)

    fake_pool = MagicMock()
    with patch("asyncpg.create_pool", new=AsyncMock(return_value=fake_pool)) as mock_create:
        await factory()

    call_kwargs = mock_create.call_args.kwargs
    params = _resolve(
        call_kwargs["dsn"],
        user=call_kwargs.get("user"),
        password=call_kwargs.get("password"),
    )
    assert params.user == "fresh-user"
    assert params.password == "fresh-tok"


async def test_make_pg_pool_factory_refetches_credential_per_invocation() -> None:
    """Each factory invocation fetches a fresh credential — the SIGHUP rotation contract."""
    provider = _FakePgProvider(password="tok")
    factory = make_pg_pool_factory("postgresql://user@host/db", provider)

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=MagicMock())):
        await factory()
        await factory()

    assert provider.calls == 2


# ── make_dedicated_conn_factory ────────────────────────────────────────


async def test_make_dedicated_conn_factory_fetches_credential_and_connects() -> None:
    """The factory fetches a credential then calls asyncpg.connect with kwargs."""
    provider = _FakePgProvider(password="conn-tok")
    factory = make_dedicated_conn_factory("postgresql://user@host:5432/db", provider)

    fake_conn = MagicMock()
    with patch("asyncpg.connect", new=AsyncMock(return_value=fake_conn)) as mock_connect:
        conn = await factory()

    assert conn is fake_conn
    assert provider.calls == 1
    call_kwargs = mock_connect.call_args.kwargs
    assert call_kwargs["password"] == "conn-tok"
    assert "conn-tok" not in call_kwargs["dsn"]
    assert "sslmode=require" in call_kwargs["dsn"]


async def test_make_dedicated_conn_factory_with_username_override() -> None:
    """The user kwarg is passed only when the provider returns a username."""
    provider = _FakePgProvider(password="pw", username="dyn-user")
    factory = make_dedicated_conn_factory("postgresql://old:oldpw@host/db", provider)

    with patch("asyncpg.connect", new=AsyncMock(return_value=MagicMock())) as mock_connect:
        await factory()

    call_kwargs = mock_connect.call_args.kwargs
    assert call_kwargs["user"] == "dyn-user"
    params = _resolve(
        call_kwargs["dsn"],
        user=call_kwargs.get("user"),
        password=call_kwargs.get("password"),
    )
    assert params.user == "dyn-user"
    assert params.password == "pw"


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
    factory = make_redis_client_factory("rediss://cache.redis.cache.windows.net:6380", provider)

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


async def test_make_redis_client_factory_warns_on_plaintext_scheme() -> None:
    """redis:// sends the bearer token unencrypted — the factory must warn."""
    provider = _FakeRedisProvider()
    factory = make_redis_client_factory("redis://cache.example.com:6379", provider)

    with (
        patch("redis.asyncio.Redis.from_url", return_value=MagicMock()),
        structlog.testing.capture_logs() as captured,
    ):
        await factory()

    warnings = [e for e in captured if e.get("log_level") == "warning"]
    assert any("plaintext" in str(e.get("event", "")) for e in warnings)


async def test_make_redis_client_factory_no_warning_on_tls_scheme() -> None:
    """rediss:// encrypts the token — no plaintext warning."""
    provider = _FakeRedisProvider()
    factory = make_redis_client_factory("rediss://cache.example.com:6380", provider)

    with (
        patch("redis.asyncio.Redis.from_url", return_value=MagicMock()),
        structlog.testing.capture_logs() as captured,
    ):
        await factory()

    warnings = [e for e in captured if e.get("log_level") == "warning"]
    assert not any("plaintext" in str(e.get("event", "")) for e in warnings)

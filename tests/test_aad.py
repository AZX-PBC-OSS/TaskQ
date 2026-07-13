"""Tests for taskq.aad — Microsoft Entra ID credential providers.

Uses fake credentials (no real Azure calls) to verify the provider
implementations satisfy the Protocols and return the right credential
carriers. The ``[aad]`` extra (azure-identity) is installed in the dev
environment.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from taskq.aad import (
    PG_TOKEN_SCOPE,
    AadCredential,
    EntraIdPgProvider,
    EntraIdProvider,
    EntraIdRedisProvider,
    fetch_pg_access_token,
    fetch_redis_credentials,
)
from taskq.auth import PgCredential, PgCredentialProvider, RedisCredential, RedisCredentialProvider

# ── Fake credential ────────────────────────────────────────────────────


class _FakeAccessToken:
    def __init__(self, token: str) -> None:
        self.token = token


class _FakeCredential:
    """Fake credential returning a canned token for any scope."""

    def __init__(self, token: str = "fake-token-123") -> None:  # noqa: S107  # Why: test fixture token, not a real password.
        self.token = token
        self.calls: list[str] = []

    def get_token(self, *scopes: str, **_kw: object) -> _FakeAccessToken:
        self.calls.append(",".join(scopes))
        return _FakeAccessToken(self.token)


class _FakeAsyncCredential:
    """Fake async credential — get_token returns an awaitable."""

    def __init__(self, token: str = "fake-async-token-456") -> None:  # noqa: S107  # Why: test fixture token, not a real password.
        self.token = token
        self.calls: list[str] = []

    async def get_token(self, *scopes: str, **_kw: object) -> _FakeAccessToken:
        self.calls.append(",".join(scopes))
        return _FakeAccessToken(self.token)


def _make_jwt(oid: str | None = "obj-id-abc") -> str:
    """Build a minimal JWT with an ``oid`` claim (header.payload.sig)."""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none"}).encode()).rstrip(b"=")
    payload_dict: dict[str, Any] = {"iss": "test", "aud": "test"}
    if oid is not None:
        payload_dict["oid"] = oid
    payload = base64.urlsafe_b64encode(json.dumps(payload_dict).encode()).rstrip(b"=")
    sig = b""
    return f"{header.decode()}.{payload.decode()}.{sig.decode()}"


# ── Token fetchers ─────────────────────────────────────────────────────


async def test_fetch_pg_access_token_sync_credential() -> None:
    """fetch_pg_access_token works with a sync credential."""
    cred = _FakeCredential()
    token = await fetch_pg_access_token(cred)
    assert token == "fake-token-123"
    assert cred.calls == [PG_TOKEN_SCOPE]


async def test_fetch_pg_access_token_async_credential() -> None:
    """fetch_pg_access_token works with an async credential."""
    cred = _FakeAsyncCredential()
    token = await fetch_pg_access_token(cred)
    assert token == "fake-async-token-456"
    assert cred.calls == [PG_TOKEN_SCOPE]


async def test_fetch_redis_credentials_decodes_oid_from_jwt() -> None:
    """fetch_redis_credentials decodes the username from the JWT oid claim."""
    jwt = _make_jwt(oid="my-oid-999")
    cred = _FakeCredential(token=jwt)
    username, password = await fetch_redis_credentials(cred)
    assert username == "my-oid-999"
    assert password == jwt


async def test_fetch_redis_credentials_explicit_username_skips_jwt_decode() -> None:
    """Passing username= avoids JWT decoding."""
    cred = _FakeCredential(token="tok")
    username, password = await fetch_redis_credentials(cred, username="explicit-user")
    assert username == "explicit-user"
    assert password == "tok"


async def test_fetch_redis_credentials_raises_when_oid_missing() -> None:
    """When the JWT has no oid claim and no username is given, raise ValueError."""
    jwt = _make_jwt(oid=None)
    cred = _FakeCredential(token=jwt)
    with pytest.raises(ValueError, match="oid"):
        await fetch_redis_credentials(cred)


# ── Provider implementations ───────────────────────────────────────────


async def test_entra_id_pg_provider_returns_token_as_password() -> None:
    """EntraIdPgProvider returns a PgCredential with the AAD token as password."""
    cred = _FakeCredential(token="pg-tok")
    provider = EntraIdPgProvider(cred)
    result = await provider.get_pg_credential()
    assert isinstance(result, PgCredential)
    assert result.password == "pg-tok"
    assert result.username is None  # DSN user preserved


async def test_entra_id_redis_provider_returns_username_and_token() -> None:
    """EntraIdRedisProvider returns a RedisCredential with oid + token."""
    jwt = _make_jwt(oid="my-oid")
    cred = _FakeCredential(token=jwt)
    provider = EntraIdRedisProvider(cred)
    result = await provider.get_redis_credential()
    assert isinstance(result, RedisCredential)
    assert result.username == "my-oid"
    assert result.password == jwt


async def test_entra_id_redis_provider_explicit_username() -> None:
    """EntraIdRedisProvider with username= skips JWT decode."""
    cred = _FakeCredential(token="tok")
    provider = EntraIdRedisProvider(cred, username="explicit")
    result = await provider.get_redis_credential()
    assert result.username == "explicit"
    assert result.password == "tok"


async def test_entra_id_provider_implements_both_protocols() -> None:
    """EntraIdProvider implements both PG and Redis Protocols."""
    cred = _FakeCredential(token="tok")
    provider = EntraIdProvider(cred, redis_username="my-oid")

    # Both Protocol methods work
    pg_cred = await provider.get_pg_credential()
    redis_cred = await provider.get_redis_credential()
    assert pg_cred.password == "tok"
    assert redis_cred.username == "my-oid"
    assert redis_cred.password == "tok"

    # Runtime-checkable Protocol matching
    assert isinstance(provider, PgCredentialProvider)
    assert isinstance(provider, RedisCredentialProvider)


# ── AadCredential protocol ─────────────────────────────────────────────


def test_aad_credential_protocol_matches_sync_and_async() -> None:
    """AadCredential is runtime-checkable and matches both sync and async credentials."""
    assert isinstance(_FakeCredential(), AadCredential)
    assert isinstance(_FakeAsyncCredential(), AadCredential)


# ── Missing extra ──────────────────────────────────────────────────────


async def test_aad_missing_extra_raises_importerror(monkeypatch: pytest.MonkeyPatch) -> None:
    """When azure.identity is not importable, fetch_pg_access_token raises ImportError."""
    import builtins

    real_import = builtins.__import__

    def _fail_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "azure.identity" or name.startswith("azure.identity."):
            raise ImportError("simulated missing extra")
        return real_import(name, *args, **kwargs)  # type: ignore[no-any-return]

    monkeypatch.setattr(builtins, "__import__", _fail_import)

    with pytest.raises(ImportError, match=r"taskq\[aad\]"):
        await fetch_pg_access_token()

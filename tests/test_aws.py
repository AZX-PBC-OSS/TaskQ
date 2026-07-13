"""Tests for taskq.aws — AWS IAM RDS credential providers.

Uses a fake boto3 client (no real AWS calls) to verify the provider
implementation. The ``[aws]`` extra (boto3) is installed in the dev
environment.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from taskq.auth import PgCredential, PgCredentialProvider
from taskq.aws import RDS_TOKEN_LIFETIME_SECONDS, RdsIamProvider, fetch_rds_iam_token

# ── fetch_rds_iam_token ────────────────────────────────────────────────


def test_fetch_rds_iam_token_calls_generate_db_auth_token() -> None:
    """fetch_rds_iam_token delegates to the boto3 client's generate_db_auth_token."""
    fake_client = MagicMock()
    fake_client.generate_db_auth_token.return_value = "signed-token-url"
    token = fetch_rds_iam_token(
        hostname="my-db.abc.us-east-1.rds.amazonaws.com",
        port=5432,
        username="myiamuser",
        region="us-east-1",
        client=fake_client,
    )
    assert token == "signed-token-url"
    fake_client.generate_db_auth_token.assert_called_once_with(
        DBHostname="my-db.abc.us-east-1.rds.amazonaws.com",
        Port=5432,
        DBUsername="myiamuser",
        Region="us-east-1",
    )


def test_fetch_rds_iam_token_defaults_region_to_empty() -> None:
    """When region is None, an empty string is passed (boto3 resolves from env)."""
    fake_client = MagicMock()
    fake_client.generate_db_auth_token.return_value = "tok"
    fetch_rds_iam_token(hostname="host", port=5432, username="user", client=fake_client)
    fake_client.generate_db_auth_token.assert_called_once_with(
        DBHostname="host", Port=5432, DBUsername="user", Region=""
    )


# ── RdsIamProvider ─────────────────────────────────────────────────────


async def test_rds_iam_provider_returns_token_as_password() -> None:
    """RdsIamProvider returns a PgCredential with the IAM token as password."""
    fake_client = MagicMock()
    fake_client.generate_db_auth_token.return_value = "iam-token-xyz"
    provider = RdsIamProvider(
        "postgresql://myiamuser@my-db.abc.us-east-1.rds.amazonaws.com:5432/mydb",
        region="us-east-1",
        client=fake_client,
    )
    result = await provider.get_pg_credential()
    assert isinstance(result, PgCredential)
    assert result.password == "iam-token-xyz"
    assert result.username is None  # DSN user preserved


async def test_rds_iam_provider_uses_dsn_username() -> None:
    """RdsIamProvider extracts the username from the DSN."""
    fake_client = MagicMock()
    fake_client.generate_db_auth_token.return_value = "tok"
    provider = RdsIamProvider("postgresql://mydbuser@host:5432/db", client=fake_client)
    await provider.get_pg_credential()
    call_kwargs = fake_client.generate_db_auth_token.call_args.kwargs
    assert call_kwargs["DBUsername"] == "mydbuser"


async def test_rds_iam_provider_username_override() -> None:
    """Passing username= overrides the DSN user."""
    fake_client = MagicMock()
    fake_client.generate_db_auth_token.return_value = "tok"
    provider = RdsIamProvider(
        "postgresql://dsnuser@host:5432/db", client=fake_client, username="override-user"
    )
    await provider.get_pg_credential()
    call_kwargs = fake_client.generate_db_auth_token.call_args.kwargs
    assert call_kwargs["DBUsername"] == "override-user"


def test_rds_iam_provider_protocol_matching() -> None:
    """RdsIamProvider satisfies PgCredentialProvider at runtime."""
    fake_client = MagicMock()
    provider = RdsIamProvider("postgresql://user@host:5432/db", client=fake_client)
    assert isinstance(provider, PgCredentialProvider)


def test_rds_iam_provider_rejects_dsn_without_username() -> None:
    """A DSN with no username raises ValueError."""
    with pytest.raises(ValueError, match="no username"):
        RdsIamProvider("postgresql://host:5432/db", client=MagicMock())


def test_rds_token_lifetime_is_15_minutes() -> None:
    """RDS IAM tokens are valid for 15 minutes (900 seconds)."""
    assert RDS_TOKEN_LIFETIME_SECONDS == 900


# ── Missing extra ──────────────────────────────────────────────────────


def test_aws_missing_extra_raises_importerror(monkeypatch: pytest.MonkeyPatch) -> None:
    """When boto3 is not importable, fetch_rds_iam_token raises ImportError."""
    import builtins

    real_import = builtins.__import__

    def _fail_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "boto3" or name.startswith("boto3."):
            raise ImportError("simulated missing extra")
        return real_import(name, *args, **kwargs)  # type: ignore[no-any-return]

    monkeypatch.setattr(builtins, "__import__", _fail_import)

    with pytest.raises(ImportError, match=r"taskq\[aws\]"):
        fetch_rds_iam_token(hostname="host", port=5432, username="user")

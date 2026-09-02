"""Shared fixtures for OIDC SSO tests: test RSA key, JWKS, signed id_tokens.

Generates a single RSA keypair at module import and provides helpers to build
signed id_tokens and JWKS dicts so tests can mock an OIDC provider without
any real IdP dependency. A second "foreign" keypair signs tokens that the
served JWKS does NOT contain, for negative signature-validation tests.
"""

from __future__ import annotations

import time
from typing import Any

from cryptography.hazmat.primitives.asymmetric import rsa
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey

_PRIV_KEY: RSAKey = RSAKey.import_key(
    rsa.generate_private_key(public_exponent=65537, key_size=2048)
)
_PRIV_KEY.ensure_kid()
_KID: str = str(_PRIV_KEY.kid)
_SIGN_KEYSET: KeySet = KeySet([_PRIV_KEY])
_VERIFY_JWKS: dict[str, Any] = {"keys": [_PRIV_KEY.as_dict(private=False)]}

# Signs id_tokens whose public half is NOT in ``jwks_dict()`` — an IdP (or
# attacker) key the relying party has never seen.
_FOREIGN_KEY: RSAKey = RSAKey.import_key(
    rsa.generate_private_key(public_exponent=65537, key_size=2048)
)
_FOREIGN_KEY.ensure_kid()
FOREIGN_SIGN_KEYSET: KeySet = KeySet([_FOREIGN_KEY])


def jwks_dict() -> dict[str, Any]:
    """The JWKS dict (public keys) served from the mocked ``jwks_uri``."""
    return _VERIFY_JWKS


def make_id_token(
    claims: dict[str, Any] | None = None,
    *,
    issuer: str = "https://idp.test.invalid",
    client_id: str = "test-client",
    subject: str = "user-123",
    email: str | None = "user@example.com",
    extra: dict[str, Any] | None = None,
    signing_keyset: KeySet | None = None,
) -> str:
    """Build a signed RS256 id_token with the given claims.

    ``signing_keyset`` defaults to the JWKS-published key; pass
    :data:`FOREIGN_SIGN_KEYSET` to sign with a key the relying party cannot
    resolve (negative signature-validation cases).
    """
    now = int(time.time())
    base: dict[str, Any] = {
        "iss": issuer,
        "sub": subject,
        "aud": client_id,
        "exp": now + 3600,
        "iat": now,
    }
    if email is not None:
        base["email"] = email
    if claims:
        base.update(claims)
    if extra:
        base.update(extra)
    kid = str((signing_keyset or _SIGN_KEYSET).keys[0].kid)
    header = {"alg": "RS256", "kid": kid, "typ": "JWT"}
    return jwt.encode(header, base, signing_keyset or _SIGN_KEYSET)


def make_discovery(issuer: str = "https://idp.test.invalid") -> dict[str, Any]:
    """A minimal OIDC discovery document for the mocked issuer."""
    return {
        "issuer": issuer,
        "authorization_endpoint": f"{issuer}/authorize",
        "token_endpoint": f"{issuer}/token",
        "jwks_uri": f"{issuer}/jwks",
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }


def make_token_response(
    *,
    id_token_claims: dict[str, Any] | None = None,
    issuer: str = "https://idp.test.invalid",
    client_id: str = "test-client",
    extra_id_claims: dict[str, Any] | None = None,
    signing_keyset: KeySet | None = None,
) -> dict[str, Any]:
    """A token-endpoint response containing a signed id_token."""
    return {
        "access_token": "fake-access-token",
        "token_type": "Bearer",
        "expires_in": 3600,
        "id_token": make_id_token(
            id_token_claims,
            issuer=issuer,
            client_id=client_id,
            extra=extra_id_claims,
            signing_keyset=signing_keyset,
        ),
    }

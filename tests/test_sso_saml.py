"""Tests for the SAML SSO backend against a fixture IdP.

Uses a self-signed test cert/key to build signed SAML Response XML fixtures —
no real IdP dependency. Skips entirely if python3-saml (or its libxmlsec1
system dependency) is not installed.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("onelogin.saml2.auth")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.saml import SAMLAuthConfig, create_saml_auth
from tests._sso_saml_crypto import (
    ACS_URL,
    IDP_CERT_PEM,
    IDP_ENTITY_ID,
    SP_ENTITY_ID,
    build_saml_response,
)

_SSO_URL = "https://idp.test.example.com/sso"
_SESSION_SECRET = "s" * 32


def _config(
    *,
    group_attribute: str | None = None,
    allowed_groups: frozenset[str] = frozenset(),
) -> SAMLAuthConfig:
    return SAMLAuthConfig(
        entity_id=SP_ENTITY_ID,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        idp_sso_url=_SSO_URL,
        idp_x509_cert=IDP_CERT_PEM,
        session_secret=_SESSION_SECRET,
        secure_cookie=False,
        group_attribute=group_attribute,
        allowed_groups=allowed_groups,
    )


_TEST_BASE_URL = "http://testserver.local"


def _make_app(config: SAMLAuthConfig, base_path: str = "/admin") -> FastAPI:
    bundle = create_saml_auth(config, base_path=base_path)
    app = FastAPI()
    app.include_router(bundle.router, prefix=base_path)

    @app.get(f"{base_path}/protected")  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
    async def protected(  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
        claims: Any = Depends(bundle.dependency),
    ) -> dict[str, Any]:
        return {"sub": claims.subject, "groups": sorted(claims.groups)}

    return app


def _client(app: FastAPI) -> TestClient:
    # python3-saml validates the ACS URL against a multi-label-domain regex;
    # use a base_url with a dotted host so it matches the configured ACS URL.
    return TestClient(app, base_url=_TEST_BASE_URL)


def _post_saml_response(client: TestClient, response_b64: str, base_path: str = "/admin") -> Any:
    return client.post(
        f"{base_path}/callback",
        data={"SAMLResponse": response_b64},
        follow_redirects=False,
    )


# ── Full login → ACS callback → session → authorized request round trip ───


def test_full_round_trip_default_auth_only() -> None:
    """group_attribute=None → any authenticated user passes."""
    config = _config()
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response(nameid="user-saml-1")
    resp = _post_saml_response(client, saml_response)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin"

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["sub"] == "user-saml-1"


def test_callback_sets_session_cookie() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response()
    resp = _post_saml_response(client, saml_response)
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "HttpOnly" in set_cookie


# ── /login redirects to IdP ───────────────────────────────────────────────


def test_login_redirects_to_idp() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    resp = client.get("/admin/login", follow_redirects=False)
    assert resp.status_code == 302
    location = resp.headers["location"]
    assert _SSO_URL in location
    assert "SAMLRequest" in location


# ── Unsigned / tampered assertion rejected ────────────────────────────────


def test_unsigned_assertion_rejected() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response(sign=False)
    resp = _post_saml_response(client, saml_response)
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_tampered_assertion_rejected() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response()
    # Flip a character in the base64 payload to break the signature.
    tampered = saml_response[:50] + ("A" if saml_response[50] != "A" else "B") + saml_response[51:]
    resp = _post_saml_response(client, tampered)
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]


# ── /metadata returns valid XML ───────────────────────────────────────────


def test_metadata_returns_valid_xml() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    resp = client.get("/admin/metadata")
    assert resp.status_code == 200
    assert "xml" in resp.headers.get("content-type", "")
    body = resp.text
    assert "EntityDescriptor" in body
    assert SP_ENTITY_ID in body
    assert "AssertionConsumerService" in body


# ── group_attribute set: allowlist behavior ───────────────────────────────


def test_group_attribute_user_in_allowed_group_passes() -> None:
    config = _config(group_attribute="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response(
        nameid="user-saml-1",
        attributes={"groups": ["admins", "viewers"]},
    )
    _post_saml_response(client, saml_response)

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["groups"] == ["admins", "viewers"]


def test_group_attribute_user_not_in_allowed_group_401() -> None:
    config = _config(group_attribute="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response(
        nameid="user-saml-1",
        attributes={"groups": ["viewers"]},
    )
    _post_saml_response(client, saml_response)

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 401


def test_group_attribute_absent_with_allowlist_fails_closed() -> None:
    """group_attribute set but absent in assertion + allowed_groups → fail closed."""
    config = _config(group_attribute="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response(nameid="user-saml-1", attributes={})
    resp = _post_saml_response(client, saml_response)
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── logout clears session ─────────────────────────────────────────────────


def test_saml_logout_clears_session() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    saml_response = build_saml_response()
    _post_saml_response(client, saml_response)

    resp = client.get("/admin/logout", follow_redirects=False)
    assert resp.status_code == 302
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "Max-Age=0" in set_cookie or "expires=" in set_cookie.lower()

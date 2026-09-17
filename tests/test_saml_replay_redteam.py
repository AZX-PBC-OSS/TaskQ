"""Red-team checks for the cross-site SAML fix's dual AuthnRequest binding.

The cookie-less fallback (see saml.py's _PendingAuthnRequests) — opt-in
since #240 via ``allow_cookieless_fallback`` — accepts a callback with no
usable correlation cookie when the assertion's *validated* InResponseTo
names an AuthnRequest this process issued and has not spent. These tests
probe whether that fallback can be abused to replay a captured,
correctly-signed assertion, or to double-spend one AuthnRequest ID into
two sessions.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("onelogin.saml2.auth")

from fastapi.testclient import TestClient

from taskq.web.admin.auth.saml import SAMLAuthConfig
from tests._sso_saml_crypto import (
    ACS_URL,
    IDP_CERT_PEM,
    IDP_ENTITY_ID,
    SP_ENTITY_ID,
    build_saml_response,
)
from tests.test_sso_saml import _do_login, _make_app

_SSO_URL = "https://idp.test.invalid/sso"
_SESSION_SECRET = "s" * 32
_TEST_BASE_URL = "http://testserver.invalid"


def _config(**kwargs: Any) -> SAMLAuthConfig:
    # allow_cookieless_fallback: these tests red-team the fallback itself,
    # which is opt-in (default off) since #240 — a cookie-less callback on a
    # default deployment is refused before any of the behavior under test
    # here is reached.
    return SAMLAuthConfig(
        entity_id=SP_ENTITY_ID,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        idp_sso_url=_SSO_URL,
        idp_x509_cert=IDP_CERT_PEM,
        session_secret=_SESSION_SECRET,
        secure_cookie=False,
        allow_cookieless_fallback=True,
        **kwargs,
    )


def _client(app: Any) -> TestClient:
    return TestClient(app, base_url=_TEST_BASE_URL)


def test_replaying_the_same_cookieless_assertion_twice_mints_only_one_session() -> None:
    """A captured, correctly-signed assertion answering a real pending
    AuthnRequest must not be usable twice — the second POST (attacker
    replaying a sniffed/logged assertion) must be rejected even though the
    first legitimately spent the pending-request gate."""
    config = _config()
    app = _make_app(config)
    login_client = _client(app)

    request_id = _do_login(login_client)
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)

    first_client = TestClient(app, base_url=_TEST_BASE_URL)
    first = first_client.post(
        "/admin/callback", data={"SAMLResponse": saml_response}, follow_redirects=False
    )
    assert first.status_code == 302
    assert first.headers["location"] == "/admin"
    assert "taskq_session=" in first.headers.get("set-cookie", "")

    second_client = TestClient(app, base_url=_TEST_BASE_URL)
    second = second_client.post(
        "/admin/callback", data={"SAMLResponse": saml_response}, follow_redirects=False
    )
    assert "error=authentication+failed" in second.headers.get("location", ""), (
        "replaying the same assertion a second time must not mint a second "
        f"session; got {second.headers.get('location')!r}"
    )
    assert "taskq_session=" not in second.headers.get("set-cookie", "")


def test_two_logins_then_one_callback_only_spends_its_own_request_id() -> None:
    """Two concurrent /login calls issue two distinct pending AuthnRequest
    IDs. Answering only the first must not spend or otherwise disturb the
    second's ability to later be answered legitimately."""
    config = _config()
    app = _make_app(config)
    client_a = _client(app)
    client_b = _client(app)

    request_id_a = _do_login(client_a)
    request_id_b = _do_login(client_b)
    assert request_id_a != request_id_b

    response_a = build_saml_response(nameid="user-a", in_response_to=request_id_a)
    resp_a = TestClient(app, base_url=_TEST_BASE_URL).post(
        "/admin/callback", data={"SAMLResponse": response_a}, follow_redirects=False
    )
    assert resp_a.status_code == 302
    assert resp_a.headers["location"] == "/admin"

    response_b = build_saml_response(nameid="user-b", in_response_to=request_id_b)
    resp_b = TestClient(app, base_url=_TEST_BASE_URL).post(
        "/admin/callback", data={"SAMLResponse": response_b}, follow_redirects=False
    )
    assert resp_b.status_code == 302
    assert resp_b.headers["location"] == "/admin", (
        "answering the first of two independently-issued AuthnRequests must "
        "not consume or invalidate the second"
    )

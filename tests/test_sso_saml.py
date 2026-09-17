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

_SSO_URL = "https://idp.test.invalid/sso"
_SESSION_SECRET = "s" * 32


def _config(
    *,
    group_attribute: str | None = None,
    allowed_groups: frozenset[str] = frozenset(),
    secure_cookie: bool = False,
) -> SAMLAuthConfig:
    return SAMLAuthConfig(
        entity_id=SP_ENTITY_ID,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        idp_sso_url=_SSO_URL,
        idp_x509_cert=IDP_CERT_PEM,
        session_secret=_SESSION_SECRET,
        secure_cookie=secure_cookie,
        group_attribute=group_attribute,
        allowed_groups=allowed_groups,
    )


_TEST_BASE_URL = "http://testserver.invalid"


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


def _do_login(client: TestClient, base_path: str = "/admin") -> str:
    """Start a login and return the AuthnRequest ID it issued.

    The ACS endpoint accepts only an assertion answering this browser's own
    AuthnRequest, so every callback POST in these tests follows a /login and
    threads the issued ID into its fixture assertion.
    """
    from onelogin.saml2.auth import OneLogin_Saml2_Auth

    issued: list[str] = []
    original_login = OneLogin_Saml2_Auth.login

    def spy_login(self: Any, *args: Any, **kwargs: Any) -> Any:
        url = original_login(self, *args, **kwargs)
        request_id = self.get_last_request_id()
        if isinstance(request_id, str):
            issued.append(request_id)
        return url

    OneLogin_Saml2_Auth.login = spy_login
    try:
        resp = client.get(f"{base_path}/login", follow_redirects=False)
        assert resp.status_code == 302
    finally:
        OneLogin_Saml2_Auth.login = original_login
    assert issued, "/login issued no AuthnRequest ID"
    return issued[-1]


# ── Full login → ACS callback → session → authorized request round trip ───


def test_full_round_trip_default_auth_only() -> None:
    """group_attribute=None → any authenticated user passes."""
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    resp = _post_saml_response(client, saml_response)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin"

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["sub"] == "user-saml-1"


def test_login_succeeds_when_idp_acs_post_is_genuinely_cross_site() -> None:
    """A hosted IdP (Entra, Okta, OneLogin, Google...) lives on a different
    registrable domain than the admin UI, so the ACS callback it POSTs to is a
    genuine cross-site POST, and a real browser may withhold the AuthnRequest
    correlation cookie from it: an explicit ``SameSite=Lax`` always is, and
    even ``SameSite=None`` loses to third-party cookie blocking and privacy
    modes.

    The shipped policy is therefore two-layered. The correlation cookie is
    marked ``SameSite=None`` when ``secure_cookie`` is on and kept ``Lax``
    when it is not (browsers reject ``None`` without ``Secure``, and a
    plain-http dev deployment has no cross-site IdP to serve). And the
    callback still completes login with no usable cookie when a fully
    validated assertion's ``InResponseTo`` names an AuthnRequest this process
    issued and has not yet spent.

    FastAPI's TestClient does not enforce SameSite cookie semantics itself,
    so the cookie drop is simulated by driving the callback with a second
    client that shares no cookie jar with the one that performed /login,
    rather than relying on TestClient for cross-site cookie behavior.
    """
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)

    # What a real browser does on the cross-site ACS POST from the IdP: the
    # correlation cookie is withheld. The fresh client shares no cookie jar
    # with the one that performed /login, but carries a valid, correctly
    # signed assertion answering that real pending AuthnRequest — exactly
    # what the IdP sends back in a genuine hosted-IdP deployment.
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    cookieless_client = TestClient(app, base_url=_TEST_BASE_URL)
    resp = cookieless_client.post(
        "/admin/callback",
        data={"SAMLResponse": saml_response},
        follow_redirects=False,
    )

    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin", (
        "a correctly-signed IdP response answering a pending AuthnRequest must "
        "complete login even when the browser withheld the correlation cookie "
        f"from the cross-site POST; got redirect to {resp.headers.get('location')!r}"
    )
    assert "taskq_session=" in resp.headers.get("set-cookie", ""), (
        "a legitimate cross-site SAML login must mint a session"
    )


def test_callback_sets_session_cookie() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(in_response_to=request_id)
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
    _do_login(client)
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
    _do_login(client)
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

    request_id = _do_login(client)
    saml_response = build_saml_response(
        nameid="user-saml-1",
        attributes={"groups": ["admins", "viewers"]},
        in_response_to=request_id,
    )
    resp = _post_saml_response(client, saml_response)
    assert "error=authentication+failed" not in resp.headers.get("location", "")

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["groups"] == ["admins", "viewers"]


def test_group_attribute_user_not_in_allowed_group_401() -> None:
    config = _config(group_attribute="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(
        nameid="user-saml-1",
        attributes={"groups": ["viewers"]},
        in_response_to=request_id,
    )
    resp = _post_saml_response(client, saml_response)
    assert "error=authentication+failed" not in resp.headers.get("location", ""), (
        "callback must accept the assertion — the 401 under test is the "
        "dependency's group gate, not a rejected login"
    )

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 401


def test_group_attribute_absent_with_allowlist_fails_closed() -> None:
    """group_attribute set but absent in assertion + allowed_groups → fail closed."""
    config = _config(group_attribute="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(
        nameid="user-saml-1", attributes={}, in_response_to=request_id
    )
    resp = _post_saml_response(client, saml_response)
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── logout clears session ─────────────────────────────────────────────────


def test_saml_logout_clears_session() -> None:
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(in_response_to=request_id)
    resp = _post_saml_response(client, saml_response)
    assert "error=authentication+failed" not in resp.headers.get("location", "")

    resp = client.get("/admin/logout", follow_redirects=False)
    assert resp.status_code == 302
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "Max-Age=0" in set_cookie or "expires=" in set_cookie.lower()


# ── Cross-site ACS must not be bought by weakening cookie policy ──────────


def _request_cookie_header(resp: Any) -> str:
    """The Set-Cookie header for the AuthnRequest correlation cookie."""
    header = next(
        (
            header
            for header in resp.headers.get_list("set-cookie")
            if header.startswith("taskq_saml_request=")
        ),
        "",
    )
    assert header, "login set no AuthnRequest correlation cookie to inspect"
    return header


def test_request_cookie_is_samesite_none_and_secure_when_secure_cookie() -> None:
    """secure_cookie=True (the default, and the hosted-IdP posture): the ACS
    POST is cross-site, so the correlation cookie must be marked
    ``SameSite=None`` or the browser withholds it — and browsers accept
    ``None`` only alongside ``Secure``, so the two are pinned together. The
    relaxation is confined to this one short-lived cookie; the session
    cookie's policy is pinned separately below."""
    client = _client(_make_app(_config(secure_cookie=True)))

    resp = client.get("/admin/login", follow_redirects=False)

    assert resp.status_code == 302
    cookie = _request_cookie_header(resp).lower()
    assert "samesite=none" in cookie
    assert "; secure" in cookie
    assert "; httponly" in cookie


def test_request_cookie_stays_samesite_lax_when_not_secure_cookie() -> None:
    """secure_cookie=False (plain-http dev): ``SameSite=None`` without
    ``Secure`` is rejected outright by browsers, so the correlation cookie
    keeps ``Lax`` — there is no cross-site IdP to serve in that configuration."""
    client = _client(_make_app(_config()))

    resp = client.get("/admin/login", follow_redirects=False)

    assert resp.status_code == 302
    cookie = _request_cookie_header(resp).lower()
    assert "samesite=lax" in cookie
    assert "samesite=none" not in cookie


def test_request_cookie_is_scoped_to_the_acs_callback_path() -> None:
    """The correlation cookie is marked for cross-site delivery, so every path
    it is offered on is one a third-party page can cause it to be sent to; it
    is needed on exactly one. Pin the ``Path`` attribute to the mount's ACS
    route — a non-default base_path, so a hardcoded literal fails here too."""
    client = _client(_make_app(_config(), base_path="/console"))

    resp = client.get("/console/login", follow_redirects=False)

    assert resp.status_code == 302
    assert "path=/console/callback" in _request_cookie_header(resp).lower()


def test_session_cookie_keeps_its_hardened_policy_after_cross_site_login() -> None:
    """The session cookie stays HttpOnly and non-cross-site after ACS login.

    Making the cross-site ACS callback work is about the short-lived
    AuthnRequest binding, not the session. The session cookie is the standing
    credential for the whole admin UI: relaxing it to ``SameSite=None`` to get
    a hosted IdP working would hand every cross-site request that reaches the
    admin mount an authenticated identity, converting an SSO-integration fix
    into a CSRF surface that outlives it by the session lifetime.

    Pinned on the exact response that completes a login, so a fix that reaches
    green by loosening the session cookie fails here instead of shipping.
    """
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    resp = _post_saml_response(client, saml_response)

    session_cookie = next(
        (
            header
            for header in resp.headers.get_list("set-cookie")
            if header.startswith("taskq_session=")
        ),
        "",
    )
    assert session_cookie, "login set no session cookie to inspect"
    assert "HttpOnly" in session_cookie, (
        f"session cookie must stay HttpOnly, got {session_cookie!r}"
    )
    lowered = session_cookie.lower()
    assert "samesite=none" not in lowered, (
        "session cookie must not be relaxed to SameSite=None to make the "
        f"cross-site ACS callback work, got {session_cookie!r}"
    )
    assert "samesite=lax" in lowered or "samesite=strict" in lowered, (
        "session cookie must carry an explicit non-cross-site SameSite "
        f"attribute, got {session_cookie!r}"
    )


def test_cookieless_acs_post_without_a_pending_login_is_still_rejected() -> None:
    """A correctly-signed assertion answering no live login mints no session.

    The cross-site fix relaxes *where the AuthnRequest binding is stored*, not
    *whether it is checked*. Accepting any well-signed assertion would let a
    captured or IdP-initiated response log an attacker's browser in, so the
    binding must still refuse a response that answers no AuthnRequest this
    deployment actually issued — the property the cookie gate was protecting.
    """
    config = _config()
    app = _make_app(config)

    # No /login: nothing is pending anywhere, neither in a cookie nor in any
    # server-side store a fix might introduce.
    saml_response = build_saml_response(
        nameid="user-saml-1", in_response_to="_never-issued-by-this-deployment"
    )
    client = _client(app)
    resp = client.post(
        "/admin/callback",
        data={"SAMLResponse": saml_response},
        follow_redirects=False,
    )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers.get("location", "")
    assert "taskq_session=" not in resp.headers.get("set-cookie", ""), (
        "an assertion answering no pending AuthnRequest must not establish a session"
    )

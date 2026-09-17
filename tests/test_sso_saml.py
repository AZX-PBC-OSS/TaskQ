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
    allow_cookieless_fallback: bool = False,
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
        allow_cookieless_fallback=allow_cookieless_fallback,
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


def _correlation_cookie(client: TestClient) -> str:
    """The current ``taskq_saml_request`` cookie value held by *client*.

    Capture it right after the /login it must answer: a later /login on the
    same client overwrites the jar entry with a fresh cookie.
    """
    value = client.cookies.get("taskq_saml_request")
    assert value, "client holds no AuthnRequest correlation cookie to capture"
    return value


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
    plain-http dev deployment has no cross-site IdP to serve). And a
    deployment that must serve cookie-blocking browsers opts in with
    ``allow_cookieless_fallback``: the callback then completes login with
    no usable cookie when a fully validated assertion's ``InResponseTo``
    names an AuthnRequest this process issued and has not yet spent. The
    tradeoff is stated as plainly as it can be: nothing ties that response
    to the browser posting it, so an attacker who starts a login,
    authenticates as themselves, and captures the signed response without
    posting it can have a cookie-less victim's browser post it within the
    5-minute window and receive a session for the attacker's NameID
    (login CSRF). That is why the flag defaults to off — a deployment that
    accepts the tradeoff opts in deliberately.

    FastAPI's TestClient does not enforce SameSite cookie semantics itself,
    so the cookie drop is simulated by driving the callback with a second
    client that shares no cookie jar with the one that performed /login,
    rather than relying on TestClient for cross-site cookie behavior.
    """
    config = _config(allow_cookieless_fallback=True)
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

    The cross-site fallback relaxes *where the AuthnRequest binding is
    stored*, not *whether it is checked*. Accepting any well-signed
    assertion would let a captured or IdP-initiated response log an
    attacker's browser in, so the binding must still refuse a response that
    answers no AuthnRequest this deployment actually issued — the property
    the cookie gate was protecting.

    Runs with the fallback opted in, so the refusal exercised here is the
    fallback's own pending-set gate, not the off-by-default flag.
    """
    config = _config(allow_cookieless_fallback=True)
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


# ── Multi-replica deployments (#239): the cookie is the cross-process binding ─


def test_callback_on_a_sibling_replica_sharing_session_secret_succeeds() -> None:
    """A callback served by a different process than the one that issued the
    login must succeed when the correlation cookie is present and valid.

    Two admin replicas behind one load balancer — or a single host running
    ``uvicorn --workers N`` — are separate processes: the pending-AuthnRequest
    set of the process that served ``/login`` is invisible to the process that
    serves the ACS POST. The signed correlation cookie is the binding that
    crosses processes: every replica sharing ``session_secret`` can verify
    it, python3-saml compares the response's ``InResponseTo`` against it, and
    it is single-use with a 300 s TTL. Requiring the process-local pending
    set on top of a valid cookie rejected every cross-process callback with
    "SAML response answers no pending AuthnRequest" (#239) — the failure
    this test pins as fixed.
    """
    config = _config()
    # Two bundles from one config: same session_secret, same SP/IdP pair —
    # the deployment shape of two replicas (or two worker processes) of one
    # admin mount.
    replica_a = _make_app(config)
    replica_b = _make_app(config)
    client_a = _client(replica_a)

    request_id = _do_login(client_a)
    # The browser carries replica A's cookie to whichever replica receives
    # the IdP's ACS POST — here replica B, which never saw the login.
    correlation_cookie = _correlation_cookie(client_a)
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)

    client_b = _client(replica_b)
    # The browser presents replica A's correlation cookie to replica B.
    client_b.cookies.set("taskq_saml_request", correlation_cookie)
    resp = client_b.post(
        "/admin/callback",
        data={"SAMLResponse": saml_response},
        follow_redirects=False,
    )

    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin", (
        "a cookie-valid callback must complete on any replica sharing "
        f"session_secret, not only on the process that issued the login; "
        f"got redirect to {resp.headers.get('location')!r}"
    )
    assert "taskq_session=" in resp.headers.get("set-cookie", ""), (
        "a cookie-valid cross-replica login must mint a session"
    )

    # The minted session is valid on the serving replica, as the browser
    # will immediately use it there.
    resp = client_b.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["sub"] == "user-saml-1"


def test_pending_set_flood_eviction_cannot_break_the_cookie_bound_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unauthenticated /login spam cannot evict a real login's binding on the
    cookie path.

    The pending set is capped (10k entries) and evicts its
    soonest-to-expire entry when full, so a flood of /login requests can
    push a real login's pending ID out before its callback arrives (#239's
    flood half). The cookie-valid path must not depend on that set: the
    signed, single-use cookie alone completes the login. The opt-in
    cookie-less fallback still does depend on it — pinned here too, so the
    eviction is proven to have happened rather than assumed.
    """
    import taskq.web.admin.auth.saml as saml_module

    # Shrink the cap so a handful of /login calls forces eviction — the
    # same soonest-to-expire policy the production 10k cap applies.
    monkeypatch.setattr(saml_module, "_PENDING_REQUEST_MAX_ENTRIES", 2)
    config = _config(allow_cookieless_fallback=True)
    app = _make_app(config)
    client = _client(app)

    real_request_id = _do_login(client)
    real_cookie = _correlation_cookie(client)
    # The flood: unauthenticated /login GETs, each minting a fresh pending
    # ID. With the cap at 2, four extra logins evict the real login's ID
    # (each new add evicts the soonest-to-expire entry, i.e. the oldest).
    for _ in range(4):
        _do_login(client)

    # Eviction proven through the fallback: the flood pushed the real
    # login's ID out of the process-local set, so even the opted-in
    # fallback can no longer answer it.
    evicted_response = build_saml_response(nameid="user-saml-1", in_response_to=real_request_id)
    cookieless_client = TestClient(app, base_url=_TEST_BASE_URL)
    resp = cookieless_client.post(
        "/admin/callback",
        data={"SAMLResponse": evicted_response},
        follow_redirects=False,
    )
    assert "error=authentication+failed" in resp.headers.get("location", ""), (
        "expected the flood to evict the real login's pending ID — if the "
        "fallback still accepts it, the eviction premise of this test is wrong"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")

    # The cookie-bound path survives the same eviction: it never consults
    # the pending set, so eviction pressure stops being a way to break (or
    # DoS) logins on the default path.
    cookie_response = build_saml_response(nameid="user-saml-1", in_response_to=real_request_id)
    cookie_client = TestClient(app, base_url=_TEST_BASE_URL)
    cookie_client.cookies.set("taskq_saml_request", real_cookie)
    resp = cookie_client.post(
        "/admin/callback",
        data={"SAMLResponse": cookie_response},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin", (
        "a cookie-valid login must complete even when its pending ID was "
        f"evicted; got redirect to {resp.headers.get('location')!r}"
    )
    assert "taskq_session=" in resp.headers.get("set-cookie", "")


# ── Single-use properties the relaxation must not lose ──────────────────────


def test_second_presentation_of_the_same_response_is_refused_on_the_issuing_process() -> None:
    """The assertion stays single-use on the process that consumed it.

    The cookie-valid path no longer requires the pending-set spend (#239),
    so the replay cache is what refuses a second POST of the same signed
    response — here with the attacker re-supplying the still-valid
    correlation cookie explicitly, since the browser's copy was cleared on
    first use.
    """
    config = _config()
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    correlation_cookie = _correlation_cookie(client)
    saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    resp = _post_saml_response(client, saml_response)
    assert resp.headers["location"] == "/admin"

    # The success response cleared the browser's cookie; a replaying party
    # who captured the POST re-supplies it. The cookie still verifies and
    # still matches InResponseTo — only the consumed-assertion record can
    # refuse this second presentation.
    replay_client = TestClient(app, base_url=_TEST_BASE_URL)
    replay_client.cookies.set("taskq_saml_request", correlation_cookie)
    resp = replay_client.post(
        "/admin/callback",
        data={"SAMLResponse": saml_response},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers.get("location", ""), (
        "a second presentation of the same assertion must not mint another session"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_cookieless_fallback_spends_the_pending_id_single_use() -> None:
    """The opt-in fallback keeps the AuthnRequest ID single-use (#180's intent).

    A fresh, correctly-signed response answering an already-spent pending ID
    is refused: a second POST must begin a new login. Uses a *new*
    assertion (fresh assertion ID) so the refusal can only come from the
    pending-set spend, not the replay cache.
    """
    config = _config(allow_cookieless_fallback=True)
    app = _make_app(config)
    client = _client(app)

    request_id = _do_login(client)
    first = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    cookieless_client = TestClient(app, base_url=_TEST_BASE_URL)
    resp = cookieless_client.post(
        "/admin/callback", data={"SAMLResponse": first}, follow_redirects=False
    )
    assert resp.headers["location"] == "/admin"

    second = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
    resp = cookieless_client.post(
        "/admin/callback", data={"SAMLResponse": second}, follow_redirects=False
    )
    assert "error=authentication+failed" in resp.headers.get("location", ""), (
        "the pending AuthnRequest ID is single-use: a second response answering it must be refused"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── The cookie-less fallback is opt-in and defaults off (#240) ──────────────


def test_cookieless_callback_is_refused_when_the_fallback_is_not_opted_in() -> None:
    """A cookie-less callback is refused cleanly unless the operator opted in.

    With no usable correlation cookie, nothing ties a validated response to
    the browser posting it: an attacker who starts a login and captures the
    signed response for their own account can have a cookie-less victim's
    browser post it and end up with a session for the attacker's NameID
    (login CSRF, #240). Default policy therefore refuses the callback — a
    clean redirect, not a 500, with the remedy (the opt-in flag) in the
    server-side log.
    """
    config = _config()  # allow_cookieless_fallback defaults to False
    app = _make_app(config)
    client = _client(app)

    # The attacker's login: a pending ID exists on this very process, so
    # the old always-on fallback would have accepted the victim's POST.
    request_id = _do_login(client)
    saml_response = build_saml_response(nameid="attacker-nameid", in_response_to=request_id)

    cookieless_client = TestClient(app, base_url=_TEST_BASE_URL)
    resp = cookieless_client.post(
        "/admin/callback",
        data={"SAMLResponse": saml_response},
        follow_redirects=False,
    )

    assert resp.status_code == 302, "the refusal must be a clean redirect, not a 500"
    assert "error=authentication+failed" in resp.headers.get("location", "")
    assert "taskq_session=" not in resp.headers.get("set-cookie", ""), (
        "a cookie-less callback on a default (fallback-off) deployment must "
        "not mint a session — this is the #240 login-CSRF shape"
    )

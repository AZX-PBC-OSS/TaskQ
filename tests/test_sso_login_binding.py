"""The SSO backends bind an IdP response to the login attempt that asked for it.

Two independent bindings keep a response that was never solicited by *this*
browser's login from being accepted as a fresh authentication:

**OIDC - the ID token carries a ``nonce``.**  ``state`` + PKCE bind the
*code*; only the nonce binds the *ID token* - the credential the session
is minted from. authlib's ``validate_nonce()`` is a no-op unless
``params["nonce"]`` is truthy, so ``oidc.py`` mints a per-login ``nonce``
into the authorization URL and the signed state cookie and threads it into
``CodeIDToken``'s ``params``; an ID token carrying no nonce - or another
login's nonce - fails validation.

**SAML - the binding holds for the IdP-initiated shape too.**
``saml.py`` passes ``request_id`` into ``process_response`` and keeps a
consumed-assertion-ID cache, but inside python3-saml the comparison is
guarded by ``if in_response_to is not None and request_id is not None:`` -
a response carrying no ``InResponseTo`` at all would pass that comparison
untouched, so with a live request cookie any IdP-initiated assertion for
this SP's audience could mint a session without answering any
AuthnRequest.  The callback therefore enforces the equality itself: an
accepted response's ``InResponseTo`` must equal the issued request ID, and
the replay cache refuses a second presentation of the same assertion ID.

Every test below pins one of those bindings end to end.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("authlib")
pytest.importorskip("respx")
pytest.importorskip("httpx2")

import respx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.oidc import OIDCAuthConfig, create_oidc_auth
from tests._sso_oidc_crypto import jwks_dict, make_discovery, make_token_response
from tests.http_mock import mock_http

pytestmark = [pytest.mark.saml]
_ISSUER = "https://idp.test.invalid"
_CLIENT_ID = "test-client"
_CLIENT_SECRET = "test-secret"
_DISCOVERY_URL = f"{_ISSUER}/.well-known/openid-configuration"
_TOKEN_URL = f"{_ISSUER}/token"
_REDIRECT_URI = "http://localhost:8080/admin/callback"
_SESSION_SECRET = "n" * 32
_BASE_PATH = "/admin"


# ── OIDC helpers ──────────────────────────────────────────────────────────


def _oidc_config() -> OIDCAuthConfig:
    return OIDCAuthConfig(
        issuer=_ISSUER,
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
        redirect_uri=_REDIRECT_URI,
        session_secret=_SESSION_SECRET,
        secure_cookie=False,
    )


def _oidc_app(config: OIDCAuthConfig) -> FastAPI:
    bundle = create_oidc_auth(config, base_path=_BASE_PATH)
    app = FastAPI()
    app.include_router(bundle.router, prefix=_BASE_PATH)
    return app


@contextmanager
def _mock_provider(
    token_response: dict[str, Any] | None = None,
) -> Generator[respx.MockRouter, None, None]:
    """Mock discovery, JWKS and the token endpoint across both httpx stacks."""
    disc = make_discovery(_ISSUER)
    tok = token_response or make_token_response()
    with mock_http() as router:
        router.get(_DISCOVERY_URL).mock(return_value=httpx.Response(200, json=disc))
        router.get(disc["jwks_uri"]).mock(return_value=httpx.Response(200, json=jwks_dict()))
        router.post(_TOKEN_URL).mock(return_value=httpx.Response(200, json=tok))
        yield router


def _login(client: TestClient) -> dict[str, list[str]]:
    """Drive ``/login`` and return the parsed authorization-URL query."""
    resp = client.get(f"{_BASE_PATH}/login", follow_redirects=False)
    assert resp.status_code == 302, resp.status_code
    return parse_qs(urlparse(resp.headers["location"]).query)


# ── DEFECT 1: OIDC nonce ──────────────────────────────────────────────────


def test_oidc_authorization_url_carries_a_nonce() -> None:
    """The authorization request must mint a per-login ``nonce``.

    Without it the IdP has nothing to echo into the ID token, so no ID token
    can ever be bound to this browser's login attempt.
    """
    client = TestClient(_oidc_app(_oidc_config()))
    with _mock_provider():
        query = _login(client)

    assert "nonce" in query, f"authorization URL has no nonce: {sorted(query)}"
    nonce = query["nonce"][0]
    assert len(nonce) >= 16, f"nonce is too short to be unguessable: {nonce!r}"


def test_oidc_nonce_is_fresh_for_every_login() -> None:
    """A nonce reused across logins is replayable, so it must not be static."""
    client = TestClient(_oidc_app(_oidc_config()))
    with _mock_provider():
        first = _login(client)
        second = _login(client)

    assert "nonce" in first and "nonce" in second, "authorization URL has no nonce"
    assert first["nonce"][0] != second["nonce"][0], "nonce is constant across logins"


def test_oidc_callback_passes_the_nonce_into_id_token_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """authlib only enforces the nonce when ``params["nonce"]`` is truthy.

    ``CodeIDToken.validate_nonce()`` returns immediately when the param is
    absent or falsy, and ``nonce`` is not in ``ESSENTIAL_CLAIMS`` - so unless
    the callback threads the login's nonce into ``params`` the check is dead
    code. Spy on the construction and assert the nonce arrives.
    """
    from authlib.oidc.core import CodeIDToken

    seen_params: list[dict[str, Any]] = []
    original_init = CodeIDToken.__init__

    # Signature mirrors authlib's CodeIDToken.__init__ exactly rather than
    # collapsing the tail into **kwargs: tests/test_double_signature_drift.py
    # guards against doubles narrower than what they replace, because a
    # narrow double keeps passing after the real signature changes - and the
    # whole point of this spy is to observe the `params` argument.
    def spy_init(
        self: Any,
        claims: Any,
        header: Any,
        options: Any = None,
        params: Any = None,
    ) -> None:
        seen_params.append(dict(params or {}))
        original_init(self, claims, header, options=options, params=params)

    monkeypatch.setattr(CodeIDToken, "__init__", spy_init)

    client = TestClient(_oidc_app(_oidc_config()))
    with _mock_provider():
        query = _login(client)
        nonce_values = query.get("nonce", [])
        state = query["state"][0]
        token_response = make_token_response(
            extra_id_claims={"nonce": nonce_values[0]} if nonce_values else None
        )
        with _mock_provider(token_response):
            client.get(
                f"{_BASE_PATH}/callback",
                params={"code": "fake-code", "state": state},
                follow_redirects=False,
            )

    assert seen_params, "CodeIDToken was never constructed - the callback did not validate"
    assert seen_params[-1].get("nonce"), (
        "no nonce in the ID-token validation params, so authlib's validate_nonce() "
        f"is a no-op: params={seen_params[-1]!r}"
    )
    assert nonce_values, "no nonce was issued at /login, so none can be enforced"
    assert seen_params[-1]["nonce"] == nonce_values[0], (
        f"validation nonce {seen_params[-1]['nonce']!r} is not the one issued at "
        f"/login ({nonce_values[0]!r})"
    )


def test_oidc_rejects_an_id_token_with_no_nonce() -> None:
    """End-to-end: an ID token carrying no nonce must not mint a session.

    This is the injection primitive - an ID token obtained through any other
    flow (another RP, an IdP-initiated login, a captured token) has no nonce
    tying it to this browser, so the callback must refuse it rather than
    mint a session.
    """
    client = TestClient(_oidc_app(_oidc_config()))
    with _mock_provider():
        state = _login(client)["state"][0]
        resp = client.get(
            f"{_BASE_PATH}/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert "error=authentication+failed" in resp.headers["location"], (
        f"nonce-less ID token was accepted; location={resp.headers['location']!r}"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_oidc_rejects_an_id_token_whose_nonce_belongs_to_another_login() -> None:
    """A nonce from a *different* login attempt must not validate.

    Proves the check is a real comparison against this session's nonce rather
    than a mere presence test.
    """
    client = TestClient(_oidc_app(_oidc_config()))
    with _mock_provider():
        state = _login(client)["state"][0]
        foreign = make_token_response(extra_id_claims={"nonce": "nonce-from-another-login"})
        with _mock_provider(foreign):
            resp = client.get(
                f"{_BASE_PATH}/callback",
                params={"code": "fake-code", "state": state},
                follow_redirects=False,
            )

    assert "error=authentication+failed" in resp.headers["location"], (
        f"foreign nonce was accepted; location={resp.headers['location']!r}"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── DEFECT 2: SAML InResponseTo / replay ──────────────────────────────────

_SAML_SSO_URL = "https://idp.test.invalid/sso"
_SAML_BASE_URL = "http://testserver.invalid"


def _saml_bits() -> tuple[Any, Any]:
    """Import the SAML backend lazily so the OIDC tests run without the extra."""
    from taskq.web.admin.auth.saml import SAMLAuthConfig, create_saml_auth

    return SAMLAuthConfig, create_saml_auth


def _saml_config() -> Any:
    from tests._sso_saml_crypto import ACS_URL, IDP_CERT_PEM, IDP_ENTITY_ID, SP_ENTITY_ID

    saml_auth_config, _ = _saml_bits()
    return saml_auth_config(
        entity_id=SP_ENTITY_ID,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        idp_sso_url=_SAML_SSO_URL,
        idp_x509_cert=IDP_CERT_PEM,
        session_secret="s" * 32,
        secure_cookie=False,
    )


def _saml_client(config: Any) -> TestClient:
    _, create = _saml_bits()
    bundle = create(config, base_path=_BASE_PATH)
    app = FastAPI()
    app.include_router(bundle.router, prefix=_BASE_PATH)
    # python3-saml validates the ACS URL against a multi-label-domain regex.
    return TestClient(app, base_url=_SAML_BASE_URL)


def _post_assertion(client: TestClient, response_b64: str) -> Any:
    return client.post(
        f"{_BASE_PATH}/callback",
        data={"SAMLResponse": response_b64},
        follow_redirects=False,
    )


@contextmanager
def _spy_process_response(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[list[Any], None, None]:
    """Record the ``request_id`` every ``process_response`` call receives."""
    from onelogin.saml2.auth import OneLogin_Saml2_Auth

    calls: list[Any] = []
    original = OneLogin_Saml2_Auth.process_response

    def spy(self: Any, request_id: Any = None) -> Any:
        calls.append(request_id)
        return original(self, request_id)

    monkeypatch.setattr(OneLogin_Saml2_Auth, "process_response", spy)
    yield calls


@contextmanager
def _spy_login_request_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[list[str], None, None]:
    """Record the AuthnRequest ID every ``login`` call issues."""
    from onelogin.saml2.auth import OneLogin_Saml2_Auth

    issued: list[str] = []
    original = OneLogin_Saml2_Auth.login

    def spy(self: Any, *args: Any, **kwargs: Any) -> Any:
        url = original(self, *args, **kwargs)
        request_id = self.get_last_request_id()
        if isinstance(request_id, str):
            issued.append(request_id)
        return url

    monkeypatch.setattr(OneLogin_Saml2_Auth, "login", spy)
    yield issued


def test_saml_callback_passes_a_request_id_to_process_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``process_response(request_id=...)`` is the only InResponseTo binding.

    python3-saml guards the check with ``if in_response_to is not None and
    request_id is not None:``, so calling it with no ``request_id`` disables
    the check entirely and any unsolicited assertion is accepted.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())
    with _spy_process_response(monkeypatch) as calls:
        client.get(f"{_BASE_PATH}/login", follow_redirects=False)
        _post_assertion(client, build_saml_response(nameid="user-saml-binding"))

    assert calls, "process_response was never called"
    assert calls[-1] is not None, (
        "process_response called with request_id=None - the InResponseTo check "
        "is a dead branch and unsolicited assertions are accepted"
    )


def test_saml_login_stores_the_authn_request_id_for_the_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The AuthnRequest ID minted at /login must survive to the ACS callback.

    The callback's InResponseTo equality check needs the very ID ``/login``
    generated, so the login path persists it (the signed request cookie) for
    the callback to read. This pins the round trip: whatever /login generated
    is what the callback enforces.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from onelogin.saml2.auth import OneLogin_Saml2_Auth

    from tests._sso_saml_crypto import build_saml_response

    issued: list[str] = []
    original_login = OneLogin_Saml2_Auth.login

    def spy_login(self: Any, *args: Any, **kwargs: Any) -> Any:
        url = original_login(self, *args, **kwargs)
        request_id = self.get_last_request_id()
        if isinstance(request_id, str):
            issued.append(request_id)
        return url

    monkeypatch.setattr(OneLogin_Saml2_Auth, "login", spy_login)

    client = _saml_client(_saml_config())
    with _spy_process_response(monkeypatch) as calls:
        client.get(f"{_BASE_PATH}/login", follow_redirects=False)
        _post_assertion(client, build_saml_response(nameid="user-saml-roundtrip"))

    assert issued, "/login did not generate an AuthnRequest ID"
    assert calls, "process_response was never called"
    assert calls[-1] == issued[-1], (
        f"callback enforced {calls[-1]!r}, but /login issued {issued[-1]!r} - "
        "the AuthnRequest ID is not carried across the login"
    )


def test_saml_rejects_an_unsolicited_assertion() -> None:
    """An assertion posted with no prior /login must not mint a session.

    This is the IdP-initiated / drive-by primitive: anyone who can obtain one
    signed assertion for any user can POST it straight to the ACS endpoint.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())
    resp = _post_assertion(client, build_saml_response(nameid="user-unsolicited"))

    assert "error=authentication+failed" in resp.headers["location"], (
        f"unsolicited assertion was accepted; location={resp.headers['location']!r}"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_saml_rejects_an_assertion_that_answers_no_authn_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An assertion carrying no InResponseTo must not mint a session.

    The unsolicited test above covers the no-cookie case; this is the
    forced-login shape that survives it. python3-saml guards its
    InResponseTo comparison with ``if in_response_to is not None and
    request_id is not None:`` (onelogin/saml2/response.py), so a response
    carrying no InResponseTo at all sails past
    ``process_response(request_id=...)`` even with the live request cookie
    in place: any IdP-initiated assertion for this SP's audience, POSTed to
    the ACS within the cookie window, mints a session without answering any
    AuthnRequest.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())
    with _spy_login_request_ids(monkeypatch) as issued:
        client.get(f"{_BASE_PATH}/login", follow_redirects=False)

    assert issued, "/login did not issue an AuthnRequest ID"
    assert client.cookies.get("taskq_saml_request"), "no live AuthnRequest cookie"

    # build_saml_response emits no InResponseTo unless given one - exactly
    # the IdP-initiated shape.
    resp = _post_assertion(client, build_saml_response(nameid="user-forced-login"))

    assert "error=authentication+failed" in resp.headers["location"], (
        "an assertion answering no AuthnRequest was accepted while a live "
        f"request cookie existed; location={resp.headers['location']!r}"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_saml_rejects_an_assertion_answering_a_foreign_authn_request() -> None:
    """A mismatched InResponseTo with a live cookie must not mint a session.

    Pins python3-saml's own comparison - the branch that is live when both
    sides are present: an assertion naming a request this browser never
    issued must be refused.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())
    client.get(f"{_BASE_PATH}/login", follow_redirects=False)
    assert client.cookies.get("taskq_saml_request"), "no live AuthnRequest cookie"

    resp = _post_assertion(
        client,
        build_saml_response(nameid="user-foreign-irt", in_response_to="request-id-never-issued"),
    )

    assert "error=authentication+failed" in resp.headers["location"], (
        f"an assertion answering a foreign AuthnRequest was accepted; "
        f"location={resp.headers['location']!r}"
    )
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def _assertion_id(response_b64: str) -> str:
    xml = base64.b64decode(response_b64).decode("utf-8")
    match = re.search(r'<saml:Assertion[^>]*\bID="([^"]+)"', xml)
    assert match is not None, "could not locate the Assertion ID in the fixture XML"
    return match.group(1)


def test_saml_rejects_a_replayed_assertion_id() -> None:
    """The same assertion must not authenticate twice.

    The consumed-assertion-ID cache (``saml.py``'s ``_AssertionReplayCache``)
    is consulted on every accepted assertion: a byte-identical assertion that
    succeeded once must be refused on its second presentation. The assertion
    is still inside its NotOnOrAfter window, so signature and time checks both
    still pass - only the replay cache can reject it.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())
    assertion = build_saml_response(nameid="user-replay")
    assert _assertion_id(assertion), "fixture assertion has no ID"

    first = _post_assertion(client, assertion)
    client.cookies.clear()
    second = _post_assertion(client, assertion)

    assert "error=authentication+failed" in second.headers["location"], (
        "a byte-identical assertion authenticated twice - no replay cache is "
        f"consulted; first={first.headers['location']!r} "
        f"second={second.headers['location']!r}"
    )
    assert "taskq_session=" not in second.headers.get("set-cookie", "")


def test_saml_replay_rejection_happens_even_when_every_other_check_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end discriminating pin for the replay cache.

    The test above can go green at the request-binding gate: with no
    ``/login`` at all, BOTH presentations are refused for "no pending SAML
    AuthnRequest" and the replay cache is never reached. This pin first
    drives two fully accepted logins - asserting each acceptance and its
    minted session so it cannot pass vacuously - then re-presents assertion
    B byte-identically while its 1h NotOnOrAfter window is live and the
    request binding is one the ACS still accepts: the browser's login-2
    cookie value, captured before B's first acceptance and restored after
    the callback dropped it (the AuthnRequest ID is single-use, so the
    request cookie is cleared on every outcome). That is the state in which
    the signature, timestamps, and InResponseTo binding all still pass,
    leaving a consumed-assertion-ID record as the only possible rejector -
    a spy on ``process_response`` proves the replayed POST got that far by
    arriving with login-2's request ID.
    """
    pytest.importorskip("onelogin.saml2.auth")
    from tests._sso_saml_crypto import build_saml_response

    client = _saml_client(_saml_config())

    def _assert_accepted(resp: Any, what: str) -> None:
        assert "error=authentication+failed" not in resp.headers.get("location", ""), (
            f"{what} was not accepted; location={resp.headers['location']!r}"
        )
        assert any("taskq_session=" in header for header in resp.headers.get_list("set-cookie")), (
            f"{what} was accepted but no session cookie was minted"
        )

    with _spy_login_request_ids(monkeypatch) as issued:
        # Login 1 → assertion A, answering login 1's request, accepted.
        client.get(f"{_BASE_PATH}/login", follow_redirects=False)
        assertion_a = build_saml_response(nameid="user-replay-e2e-a", in_response_to=issued[-1])
        _assert_accepted(_post_assertion(client, assertion_a), "assertion A")

        # Login 2 → assertion B, a DIFFERENT assertion ID, also accepted.
        client.get(f"{_BASE_PATH}/login", follow_redirects=False)
        request_b = issued[-1]
        cookie_b = client.cookies.get("taskq_saml_request")
        assert cookie_b, "login 2 set no AuthnRequest cookie"
        assertion_b = build_saml_response(nameid="user-replay-e2e-b", in_response_to=request_b)
        assert _assertion_id(assertion_a) != _assertion_id(assertion_b), (
            "assertions A and B share an assertion ID - B's first presentation "
            "would already be a replay"
        )
        _assert_accepted(_post_assertion(client, assertion_b), "assertion B")

    # The captured (cookie, assertion) pair from login 2 is the replay
    # attack: byte-identical assertion re-POSTed with the request binding it
    # still answers. Without restoring the cookie the rejection below would
    # come from the cookie or binding gate, not the replay cache.
    client.cookies.clear()
    client.cookies.set("taskq_saml_request", cookie_b, domain="testserver.invalid", path="/")

    with _spy_process_response(monkeypatch) as calls:
        replayed = _post_assertion(client, assertion_b)

    assert calls == [request_b], (
        f"the replayed POST did not reach process_response with login-2's "
        f"request ID {request_b!r} (calls={calls!r}) - the rejection below "
        "came from the cookie or binding gate, not the replay cache"
    )

    assert "error=authentication+failed" in replayed.headers["location"], (
        "a byte-identical assertion authenticated twice while its NotOnOrAfter "
        f"window and the request binding were live; location={replayed.headers['location']!r}"
    )
    assert "taskq_session=" not in replayed.headers.get("set-cookie", "")

"""Tests for the OIDC SSO backend against a mocked OIDC provider.

httpx2-native transport mocking — no real IdP dependency. An autouse fixture
swaps ``httpx2.AsyncClient`` for a subclass that injects an
``httpx2.MockTransport`` when the caller passes no transport of its own,
routing every outbound call through an in-memory IdP (discovery, JWKS, token
endpoint). Production makes its direct discovery/JWKS calls via
``import httpx2 as httpx``, and authlib 1.8+'s ``AsyncOAuth2Client`` is
httpx2-based too, so a single interception point inside httpx2 covers both
call paths with consistent types. A test RSA key signs id_tokens.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx2
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("authlib")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.oidc import OIDCAuthConfig, OIDCTokenContext, create_oidc_auth
from tests._sso_oidc_crypto import (
    FOREIGN_SIGN_KEYSET,
    jwks_dict,
    make_discovery,
    make_token_response,
)

# Deliberately import authlib's httpx2 integration at COLLECTION time — the
# import-order scenario the interception fixture must survive. authlib binds
# ``class AsyncOAuth2Client(_OAuth2Client, httpx2.AsyncClient)`` ONCE, when
# ``oauth2_client.py`` is first imported, so any module importing it before a
# test's patch lands (this one, or any future module-level import elsewhere)
# would otherwise leave the class bound to the REAL AsyncClient and the
# MockTransport interception silently broken. The importlib form pins the
# same binding side-effect without dragging pyright Unknown types into this
# typed test module. (Placement: module level is what matters — it runs at
# collection, before any fixture or test; none of the imports above pull
# authlib in, so running last among them changes nothing.)
importlib.import_module("authlib.integrations.httpx_client")

_ISSUER = "https://idp.test.example.com"
_CLIENT_ID = "test-client"


_CLIENT_SECRET = "test-secret"
_REDIRECT_URI = "http://localhost:8080/admin/callback"
_SESSION_SECRET = "x" * 32


class _MockIdP:
    """In-memory OIDC provider: routes ``(method, host, path)`` → canned
    responses for the ``httpx2.MockTransport`` handler.

    Unmatched requests fail loudly, so a stray production call cannot
    silently escape to the network. Every request the handler serves is
    recorded in ``requests`` (cleared by :meth:`reset`), so tests can
    capture the authorization/token exchange traffic.
    """

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str, str], httpx2.Response] = {}
        self.requests: list[httpx2.Request] = []

    def on(self, method: str, url: str, response: httpx2.Response) -> None:
        """Register (or override) a canned response for an endpoint URL."""
        parsed = httpx2.URL(url)
        self._routes[(method.upper(), parsed.host, parsed.path)] = response

    def reset(self) -> None:
        self._routes.clear()
        self.requests.clear()

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        route = self._routes.get((request.method, request.url.host, request.url.path))
        if route is None:
            raise AssertionError(f"unexpected request: {request.method} {request.url}")
        return route


_idp = _MockIdP()


class _IdpTransportAsyncClient(httpx2.AsyncClient):
    """``httpx2.AsyncClient`` that routes transport-less instances through the
    mocked IdP, so every outbound call made inside a test is intercepted.

    Why a subclass rather than a bare factory function: authlib's
    ``AsyncOAuth2Client`` both subclasses the ``httpx2.AsyncClient`` attribute
    and calls ``httpx2.AsyncClient.__init__`` explicitly, so the patched
    attribute must remain a real class.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if kwargs.get("transport") is None:
            kwargs["transport"] = httpx2.MockTransport(_idp.handler)
        super().__init__(*args, **kwargs)


def _rebind_authlib_client_base_under_patch() -> None:
    """Re-bind authlib's ``AsyncOAuth2Client`` base to the patched client.

    authlib resolves ``httpx2.AsyncClient`` ONCE — at class-definition time
    in ``oauth2_client.py`` — so when its integration module was imported
    before the per-test patch (see the module-level import above), the
    already-defined class keeps the REAL AsyncClient base and the patch
    does not intercept it. Reloading the submodule under the live patch
    re-executes the class definition against the patched attribute, and
    reloading the package re-binds ``authlib.integrations.httpx_client
    .AsyncOAuth2Client`` — the name production imports — to the reloaded
    class. No-op when the current binding already subclasses the patched
    client (first import under a live patch, or an earlier test's reload).
    """
    pkg = importlib.import_module("authlib.integrations.httpx_client")
    client_cls: type = pkg.AsyncOAuth2Client
    if _IdpTransportAsyncClient in client_cls.__bases__:
        return
    importlib.reload(importlib.import_module("authlib.integrations.httpx_client.oauth2_client"))
    importlib.reload(pkg)


@pytest.fixture(autouse=True)
def _route_httpx2_through_mock_idp(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Swap ``httpx2.AsyncClient`` for the MockTransport-injecting subclass.

    One httpx2-native interception point covers both outbound call paths —
    production's direct discovery/JWKS fetches and authlib's token exchange —
    with no cross-module type bridging.

    Import-order robustness: authlib binds ``AsyncOAuth2Client.__bases__``
    at class-definition time, so the patch only intercepts it if that class
    is (re)defined while the patch is live — hence the rebind helper above
    when it was imported earlier. The trailing assert proves the
    interception regardless of import order: authlib calls
    ``httpx2.AsyncClient.__init__(self, ...)`` unbound from its own
    ``__init__``, so a base still bound to the REAL AsyncClient would
    surface mid-test as a confusing ``super(type, obj)`` TypeError instead
    of this clear failure.
    """
    monkeypatch.setattr(httpx2, "AsyncClient", _IdpTransportAsyncClient)
    _rebind_authlib_client_base_under_patch()
    pkg = importlib.import_module("authlib.integrations.httpx_client")
    client_cls: type = pkg.AsyncOAuth2Client
    assert _IdpTransportAsyncClient in client_cls.__bases__


def _config(
    *,
    group_claim: str | None = None,
    allowed_groups: frozenset[str] = frozenset(),
    group_resolver: Any = None,
    scope: str = "openid profile email",
) -> OIDCAuthConfig:
    return OIDCAuthConfig(
        issuer=_ISSUER,
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
        redirect_uri=_REDIRECT_URI,
        session_secret=_SESSION_SECRET,
        secure_cookie=False,
        scope=scope,
        group_claim=group_claim,
        allowed_groups=allowed_groups,
        group_resolver=group_resolver,
    )


def _make_app(config: OIDCAuthConfig, base_path: str = "/admin") -> FastAPI:
    bundle = create_oidc_auth(config, base_path=base_path)
    app = FastAPI()
    app.include_router(bundle.router, prefix=base_path)

    @app.get(f"{base_path}/protected")  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
    async def protected(  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
        claims: Any = Depends(bundle.dependency),
    ) -> dict[str, Any]:
        return {"sub": claims.subject, "groups": sorted(claims.groups)}

    return app


@contextmanager
def _mock_provider(
    token_response: dict[str, Any] | None = None,
    *,
    discovery: dict[str, Any] | None = None,
) -> Generator[_MockIdP, None, None]:
    """Mock the OIDC provider: registers canned discovery, JWKS, and token
    responses on the in-memory IdP for the duration of the context. Every
    httpx2 client built while the autouse transport fixture is active is
    routed through the IdP's MockTransport handler."""
    disc = discovery or make_discovery(_ISSUER)
    tok = token_response or make_token_response()
    jwks_data = jwks_dict()

    _idp.reset()
    _idp.on("GET", f"{_ISSUER}/.well-known/openid-configuration", httpx2.Response(200, json=disc))
    _idp.on("GET", disc["jwks_uri"], httpx2.Response(200, json=jwks_data))
    _idp.on("POST", f"{_ISSUER}/token", httpx2.Response(200, json=tok))
    try:
        yield _idp
    finally:
        _idp.reset()


def _extract_state(location: str) -> str:
    qs = parse_qs(urlparse(location).query)
    return qs["state"][0]


def _do_login(client: TestClient, base_path: str = "/admin") -> str:
    resp = client.get(f"{base_path}/login", follow_redirects=False)
    assert resp.status_code == 302
    return _extract_state(resp.headers["location"])


# ── Full login → callback → session → authorized request round trip ───────


def test_full_round_trip_default_auth_only() -> None:
    """group_claim=None → any authenticated user passes."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/admin"

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["sub"] == "user-123"


def test_callback_sets_session_cookie() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "HttpOnly" in set_cookie


# ── state mismatch rejected ───────────────────────────────────────────────


def test_state_mismatch_rejected() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": "wrong-state"},
            follow_redirects=False,
        )
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    # session cookie must not be set
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_missing_state_cookie_rejected() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": "anything"},
            follow_redirects=False,
        )
    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]


# ── group_claim set: allowlist behavior ───────────────────────────────────


def test_group_claim_user_in_allowed_group_passes() -> None:
    config = _config(group_claim="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider(make_token_response(extra_id_claims={"groups": ["admins", "viewers"]})):
        state = _do_login(client)
        client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["groups"] == ["admins", "viewers"]


def test_group_claim_user_not_in_allowed_group_401() -> None:
    config = _config(group_claim="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider(make_token_response(extra_id_claims={"groups": ["viewers"]})):
        state = _do_login(client)
        client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 401


# ── group_resolver invoked when group claim absent ────────────────────────


def test_group_resolver_invoked_when_claim_absent() -> None:
    """group_claim set but absent in token + resolver configured → resolver result used."""
    resolved: dict[str, Any] = {}

    async def resolver(ctx: OIDCTokenContext) -> frozenset[str]:
        resolved["called"] = True
        resolved["access_token"] = ctx.access_token
        return frozenset({"resolved-admins"})

    config = _config(
        group_claim="groups",
        allowed_groups=frozenset({"resolved-admins"}),
        group_resolver=resolver,
    )
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider(make_token_response(extra_id_claims={})):
        state = _do_login(client)
        client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resolved.get("called") is True
    assert resolved.get("access_token") == "fake-access-token"
    resp = client.get("/admin/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200


def test_no_resolver_no_claim_with_allowlist_fails_closed() -> None:
    """group_claim set, claim absent, no resolver, allowed_groups non-empty → fail closed."""
    config = _config(group_claim="groups", allowed_groups=frozenset({"admins"}))
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider(make_token_response(extra_id_claims={})):
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── IdP error during callback → generic error, no exception text ──────────


def test_idp_error_redirects_with_generic_code() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        state = _do_login(client)
        # IdP redirects back with an error param
        resp = client.get(
            "/admin/callback",
            params={"error": "access_denied", "state": state},
            follow_redirects=False,
        )
    assert resp.status_code == 302
    location = resp.headers["location"]
    assert "error=authentication+failed" in location
    assert "access_denied" not in location


def test_token_endpoint_failure_redirects_with_generic_code() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        # Override token endpoint to return an error
        state = _do_login(client)
    # Re-mock with a failing token endpoint for the callback
    with _mock_provider(make_token_response()) as idp:
        idp.on("POST", f"{_ISSUER}/token", httpx2.Response(500, text="boom"))
        # re-do login to get a fresh state cookie (previous context exited)
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
    assert resp.status_code == 302
    location = resp.headers["location"]
    assert "error=authentication+failed" in location
    assert "boom" not in location


# ── logout clears session ─────────────────────────────────────────────────


def test_oidc_logout_clears_session() -> None:
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        state = _do_login(client)
        client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    resp = client.get("/admin/logout", follow_redirects=False)
    assert resp.status_code == 302
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "Max-Age=0" in set_cookie or "expires=" in set_cookie.lower()


# ── PKCE authorization-request surface (login redirect) ────────────────────


def _state_cookie_payload(client: TestClient) -> dict[str, str]:
    """Decode this client's ``taskq_oidc_state`` cookie: the signed
    ``{"state", "cv"}`` record production issues at login (same serializer
    secret/salt as ``oidc._state_serializer`` — ``cv`` is the PKCE
    code_verifier)."""
    from itsdangerous import URLSafeTimedSerializer

    raw = client.cookies["taskq_oidc_state"]
    payload = URLSafeTimedSerializer(_SESSION_SECRET, salt="taskq-oidc-state").loads(raw)
    assert isinstance(payload, dict)
    return {str(key): str(value) for key, value in payload.items()}


def test_login_redirect_targets_discovered_authorization_endpoint_with_pkce() -> None:
    """The login redirect goes to the DISCOVERED authorization endpoint and
    carries the full authorization-code + PKCE request surface: client_id,
    redirect_uri, scope, state, and a code_challenge that is the RFC 7636
    S256 digest of the state cookie's verifier."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        resp = client.get("/admin/login", follow_redirects=False)

    assert resp.status_code == 302
    discovered = urlparse(make_discovery(_ISSUER)["authorization_endpoint"])
    location = urlparse(resp.headers["location"])
    assert (location.scheme, location.netloc, location.path) == (
        discovered.scheme,
        discovered.netloc,
        discovered.path,
    )
    query = parse_qs(location.query)
    assert query["client_id"] == [_CLIENT_ID]
    assert query["redirect_uri"] == [_REDIRECT_URI]
    assert query["scope"] == ["openid profile email"]
    assert query["code_challenge_method"] == ["S256"]
    challenge = query["code_challenge"][0]
    assert challenge, "login redirect carries no code_challenge"
    # PKCE S256 binding: challenge == unpadded base64url(sha256(verifier)).
    cookie = _state_cookie_payload(client)
    expected_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(cookie["cv"].encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    assert challenge == expected_challenge
    assert query["state"][0] == cookie["state"]


# ── PKCE token-request capture (callback exchange) ─────────────────────────


def _token_request_posts() -> list[httpx2.Request]:
    """The POSTs the callback made to the token endpoint (call while the
    ``_mock_provider`` context is live — its exit resets the request log)."""
    return [r for r in _idp.requests if r.method == "POST" and r.url.path == "/token"]


def test_callback_token_request_carries_pkce_verifier_from_state_cookie() -> None:
    """The token exchange POST carries the SAME code_verifier the state
    cookie recorded at login — the PKCE round-trip binding between the
    authorization request and the token request."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider():
        state = _do_login(client)
        cookie = _state_cookie_payload(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
        token_posts = _token_request_posts()

    assert resp.status_code == 302
    assert resp.headers["location"] == "/admin"  # successful exchange
    assert len(token_posts) == 1, f"expected exactly one token request, saw {len(token_posts)}"
    body = parse_qs(token_posts[0].content.decode())
    assert body["grant_type"] == ["authorization_code"]
    assert body["code"] == ["fake-code"]
    assert body["code_verifier"] == [cookie["cv"]]


# ── Negative ID-token validation ───────────────────────────────────────────


@pytest.mark.parametrize(
    "token_response",
    [
        make_token_response(signing_keyset=FOREIGN_SIGN_KEYSET),
        make_token_response(issuer="https://attacker.example"),
        make_token_response(client_id="other-client"),
        make_token_response(id_token_claims={"exp": int(time.time()) - 10}),
    ],
    ids=["wrong-signing-key", "iss-mismatch", "aud-mismatch", "expired"],
)
def test_callback_rejects_invalid_id_tokens(token_response: dict[str, Any]) -> None:
    """Every ID-token validation failure redirects to the generic error page
    and never sets a session cookie: a token signed by a key absent from the
    JWKS, an issuer mismatch, an audience mismatch, and an expired token."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider(token_response):
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── Discovery / JWKS fetch failures ────────────────────────────────────────


def test_login_discovery_failure_returns_error_redirect() -> None:
    """A failing discovery document at login → generic error redirect."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider() as idp:
        idp.on(
            "GET",
            f"{_ISSUER}/.well-known/openid-configuration",
            httpx2.Response(500, text="boom"),
        )
        resp = client.get("/admin/login", follow_redirects=False)

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]


def test_callback_discovery_failure_returns_error_redirect() -> None:
    """A failing discovery refetch at callback → generic error redirect, no
    session cookie (login succeeded first, so the state cookie is valid)."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider() as idp:
        state = _do_login(client)
        idp.on(
            "GET",
            f"{_ISSUER}/.well-known/openid-configuration",
            httpx2.Response(500, text="boom"),
        )
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


def test_callback_jwks_failure_returns_error_redirect() -> None:
    """A failing JWKS fetch at callback → generic error redirect, no session
    cookie."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider() as idp:
        state = _do_login(client)
        idp.on("GET", f"{_ISSUER}/jwks", httpx2.Response(500, text="boom"))
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")

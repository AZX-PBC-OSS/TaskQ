"""Tests for the OIDC SSO backend against a mocked OIDC provider.

The backend talks to the IdP over two different HTTP stacks within one request:
``oidc.py`` does ``import httpx2 as httpx`` for the discovery and JWKS fetches,
while authlib's ``AsyncOAuth2Client`` performs the token exchange on whichever
stack authlib itself binds to. ``tests.http_mock.mock_http`` is respx aimed at
httpcore AND httpcore2 through one route table, so every call is intercepted on
the stack production actually uses. A test RSA key signs id_tokens.

This replaces a setup where respx saw ``httpcore`` only and an autouse fixture
swapped ``httpx2.AsyncClient`` for ``httpx.AsyncClient`` to compensate. That
bridge made the mocks apply while quietly moving every assertion onto a client
class production never constructs; ``test_discovery_and_jwks_run_on_httpx2``
below fails if it ever comes back.
"""

from __future__ import annotations

import base64
import hashlib
import time
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
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.oidc import OIDCAuthConfig, OIDCTokenContext, create_oidc_auth
from tests._sso_oidc_crypto import (
    FOREIGN_SIGN_KEYSET,
    jwks_dict,
    make_discovery,
    make_token_response,
)
from tests.http_mock import mock_http, stacks_for

_ISSUER = "https://idp.test.invalid"
_CLIENT_ID = "test-client"
_DISCOVERY_URL = f"{_ISSUER}/.well-known/openid-configuration"
_TOKEN_URL = f"{_ISSUER}/token"


_CLIENT_SECRET = "test-secret"
_REDIRECT_URI = "http://localhost:8080/admin/callback"
_SESSION_SECRET = "x" * 32


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
) -> Generator[respx.MockRouter, None, None]:
    """Mock the OIDC provider across every installed httpx stack.

    Discovery and JWKS arrive on httpx2 (the backend's own client) and the token
    exchange on authlib's client; ``mock_http`` covers both cores through one
    router, with neither stack substituted for the other.
    """
    disc = discovery or make_discovery(_ISSUER)
    tok = token_response or make_token_response()
    jwks_data = jwks_dict()

    with mock_http() as router:
        router.get(_DISCOVERY_URL).mock(return_value=httpx.Response(200, json=disc))
        router.get(disc["jwks_uri"]).mock(return_value=httpx.Response(200, json=jwks_data))
        router.post(_TOKEN_URL).mock(return_value=httpx.Response(200, json=tok))
        yield router


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
    with _mock_provider(make_token_response()) as router:
        router.post(_TOKEN_URL).mock(return_value=httpx.Response(500, text="boom"))
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


def _token_request_posts(router: respx.MockRouter) -> list[httpx.Request]:
    """The POSTs the callback made to the token endpoint (call while the
    ``_mock_provider`` context is live — the router only records calls made
    while its routes are mounted)."""
    return [
        call.request
        for call in router.calls
        if call.request.method == "POST" and str(call.request.url) == _TOKEN_URL
    ]


def test_callback_token_request_carries_pkce_verifier_from_state_cookie() -> None:
    """The token exchange POST carries the SAME code_verifier the state
    cookie recorded at login — the PKCE round-trip binding between the
    authorization request and the token request."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider() as router:
        state = _do_login(client)
        cookie = _state_cookie_payload(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
        token_posts = _token_request_posts(router)

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

    with _mock_provider() as router:
        router.get(_DISCOVERY_URL).mock(return_value=httpx.Response(500, text="boom"))
        resp = client.get("/admin/login", follow_redirects=False)

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]


def test_callback_discovery_failure_returns_error_redirect() -> None:
    """A failing discovery refetch at callback → generic error redirect, no
    session cookie (login succeeded first, so the state cookie is valid)."""
    config = _config()
    app = _make_app(config)
    client = TestClient(app)

    with _mock_provider() as router:
        state = _do_login(client)
        router.get(_DISCOVERY_URL).mock(return_value=httpx.Response(500, text="boom"))
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

    with _mock_provider() as router:
        state = _do_login(client)
        jwks_url = make_discovery(_ISSUER)["jwks_uri"]
        router.get(jwks_url).mock(return_value=httpx.Response(500, text="boom"))
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )

    assert resp.status_code == 302
    assert "error=authentication+failed" in resp.headers["location"]
    assert "taskq_session=" not in resp.headers.get("set-cookie", "")


# ── The mocks must apply on the stack production actually uses ────────────


def test_discovery_and_jwks_run_on_httpx2() -> None:
    """A full round trip must reach the mock over httpx2, not httpx.

    ``oidc.py`` builds its discovery/JWKS client from ``import httpx2 as httpx``.
    The retired ``_bridge_httpx2`` fixture rebound ``httpx2.AsyncClient`` to
    ``httpx.AsyncClient`` so plain respx could see those calls, which meant every
    OIDC test ran on a client class production never constructs. Recording the
    stack that served each request makes a silent revert to that arrangement
    fail here instead of passing quietly.
    """
    client = TestClient(_make_app(_config()))
    jwks_url = make_discovery(_ISSUER)["jwks_uri"]

    with _mock_provider():
        state = _do_login(client)
        resp = client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/admin"

        assert stacks_for(_DISCOVERY_URL) == {"httpx2"}, (
            f"discovery ran on {stacks_for(_DISCOVERY_URL) or 'no stack'}; oidc.py "
            "imports httpx2, so anything else means a fixture substituted the client"
        )
        assert stacks_for(jwks_url) == {"httpx2"}, (
            f"JWKS ran on {stacks_for(jwks_url) or 'no stack'}"
        )


def test_token_exchange_is_intercepted_on_authlibs_own_stack() -> None:
    """authlib's token exchange must be mocked wherever authlib binds itself.

    authlib 1.7.x subclasses ``httpx.AsyncClient``; 1.8 prefers ``httpx2`` when
    it is importable. Either is fine - what must not happen is the request
    escaping the mock because the helper covered only one core. Pin that the
    call was served, and that it was served on the stack authlib's client class
    actually derives from.
    """
    import importlib

    from authlib.integrations.httpx_client import AsyncOAuth2Client

    expected = next(
        name
        for name in ("httpx2", "httpx")
        if issubclass(AsyncOAuth2Client, importlib.import_module(name).AsyncClient)
    )

    client = TestClient(_make_app(_config()))
    with _mock_provider():
        state = _do_login(client)
        client.get(
            "/admin/callback",
            params={"code": "fake-code", "state": state},
            follow_redirects=False,
        )
        served = stacks_for(_TOKEN_URL, method="POST")
        assert served, "the token exchange escaped the mock entirely"
        assert served == {expected}, (
            f"AsyncOAuth2Client derives from {expected}.AsyncClient but the token "
            f"request was served via {served}"
        )


async def test_unrouted_httpx2_request_fails_closed() -> None:
    """An unregistered URL must raise, never reach the network.

    respx's default behaviour, pinned here for the httpx2 side specifically:
    before this mocker existed an httpx2 call went straight past respx and out
    to the configured issuer for real.
    """
    import httpx2

    with mock_http() as router:
        router.get(_DISCOVERY_URL).mock(return_value=httpx.Response(200, json={}))
        with pytest.raises(respx.models.AllMockedAssertionError):
            async with httpx2.AsyncClient() as unrouted:
                await unrouted.get(f"{_ISSUER}/not-registered")

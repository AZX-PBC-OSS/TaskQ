"""Tests for the OIDC SSO backend against a mocked OIDC provider.

Uses respx to intercept all httpx traffic — both the direct discovery/JWKS
fetch (httpx2, aliased to httpx in production code) and authlib's httpx-based
token endpoint — no real IdP dependency. A test RSA key signs id_tokens.

respx patches httpx's transport, not httpx2's. The production code uses
``import httpx2 as httpx`` for direct calls. The ``_bridge_httpx2`` fixture
temporarily replaces ``httpx2.AsyncClient`` with ``httpx.AsyncClient`` so
respx can intercept direct calls alongside authlib's token-endpoint calls.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import httpx2
import pytest
import respx

pytest.importorskip("fastapi")
pytest.importorskip("authlib")
pytest.importorskip("respx")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.oidc import OIDCAuthConfig, OIDCTokenContext, create_oidc_auth
from tests._sso_oidc_crypto import jwks_dict, make_discovery, make_token_response

_ISSUER = "https://idp.test.example.com"
_CLIENT_ID = "test-client"


@pytest.fixture(autouse=True)
def _bridge_httpx2(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Replace httpx2.AsyncClient with httpx.AsyncClient so respx can intercept
    the direct discovery/JWKS calls that production code makes via httpx2."""
    monkeypatch.setattr(httpx2, "AsyncClient", httpx.AsyncClient)


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
    """Mock the OIDC provider: respx intercepts every httpx call the backend
    makes — discovery, JWKS, and the token endpoint — since the OIDC module
    uses plain httpx.AsyncClient throughout (authlib's AsyncOAuth2Client
    subclasses httpx.AsyncClient too, so one interception point covers both).
    """
    disc = discovery or make_discovery(_ISSUER)
    tok = token_response or make_token_response()
    jwks_data = jwks_dict()

    router = respx.mock(assert_all_called=False)
    router.get(f"{_ISSUER}/.well-known/openid-configuration").mock(
        return_value=httpx.Response(200, json=disc)
    )
    router.get(disc["jwks_uri"]).mock(return_value=httpx.Response(200, json=jwks_data))
    router.post(f"{_ISSUER}/token").mock(return_value=httpx.Response(200, json=tok))
    router.start()

    try:
        yield router
    finally:
        router.stop()


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
        router.post(f"{_ISSUER}/token").mock(return_value=httpx.Response(500, text="boom"))
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

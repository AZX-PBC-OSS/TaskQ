"""Tests for the shared SSO session module (_session.py).

Protocol-agnostic — covers cookie issuance/verification, expiry, tampering,
the group allowlist, and logout. Tested once here so both backends inherit the
guarantees without duplication.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("itsdangerous")

from fastapi import Depends, FastAPI
from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from taskq.web.admin.auth._session import (
    IdentityClaims,
    SessionManager,
    create_auth_dependency,
)


def _make_claims(
    subject: str = "user-1",
    email: str | None = "user@example.com",
    groups: frozenset[str] = frozenset(),
) -> IdentityClaims:
    return IdentityClaims(subject=subject, email=email, groups=groups, raw={})


# ── Cookie issuance / verification round trip ─────────────────────────────


def test_cookie_round_trip() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    claims = _make_claims(groups=frozenset({"admins", "viewers"}))
    cookie = manager.create_session_cookie(claims)
    result = manager.verify_session_cookie(cookie)
    assert result is not None
    assert result.subject == "user-1"
    assert result.email == "user@example.com"
    assert result.groups == frozenset({"admins", "viewers"})


def test_cookie_stores_only_subject_email_groups() -> None:
    """Raw token data must not leak into the cookie payload."""
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    claims = IdentityClaims(
        subject="u1",
        email=None,
        groups=frozenset(),
        raw={"access_token": "secret-value", "extra": "pii"},
    )
    cookie = manager.create_session_cookie(claims)
    result = manager.verify_session_cookie(cookie)
    assert result is not None
    # raw in the verified claims is the cookie payload, not the original raw
    assert "access_token" not in result.raw
    assert "extra" not in result.raw


# ── Expired cookie rejected ───────────────────────────────────────────────


def test_expired_cookie_rejected() -> None:
    issuer = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    # itsdangerous uses integer-second timestamps, so max_age=0 rejects any
    # cookie whose signed second differs from the verification second.
    verifier = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=0)
    cookie = issuer.create_session_cookie(_make_claims())
    time.sleep(1.1)
    assert verifier.verify_session_cookie(cookie) is None


# ── Tampered cookie rejected ──────────────────────────────────────────────


def test_tampered_cookie_rejected() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims())
    tampered = cookie[:-4] + "AAAA"
    assert manager.verify_session_cookie(tampered) is None


def test_wrong_secret_rejected() -> None:
    manager_a = SessionManager(secret="secret-a-32bytes-long-aaaaaa!", max_age_seconds=3600)
    manager_b = SessionManager(secret="secret-b-32bytes-long-bbbbbb!", max_age_seconds=3600)
    cookie = manager_a.create_session_cookie(_make_claims())
    assert manager_b.verify_session_cookie(cookie) is None


# ── Dependency: group allowlist ───────────────────────────────────────────


def _make_app(
    manager: SessionManager,
    allowed_groups: frozenset[str],
    login_path: str = "/login",
) -> FastAPI:
    dep = create_auth_dependency(manager, allowed_groups, login_path=login_path)
    app = FastAPI()

    @app.get("/protected")  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
    async def protected(  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
        claims: IdentityClaims = Depends(dep),
    ) -> dict[str, Any]:
        return {"sub": claims.subject, "groups": sorted(claims.groups)}

    @app.get("/logout")  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
    async def logout() -> RedirectResponse:  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
        resp = RedirectResponse(url="/", status_code=302)
        manager.clear_session_cookie(resp)
        return resp

    return app


def test_no_cookie_returns_401_for_api() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset())
    client = TestClient(app)
    resp = client.get("/protected", headers={"accept": "application/json"})
    assert resp.status_code == 401


def test_no_cookie_redirects_for_browser() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset(), login_path="/login")
    client = TestClient(app)
    resp = client.get("/protected", headers={"accept": "text/html"}, follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login"


def test_valid_session_passes_with_no_allowlist() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset())
    client = TestClient(app)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"any-group"})))
    client.cookies.set("taskq_session", cookie)
    resp = client.get("/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["sub"] == "user-1"


def test_user_in_allowed_group_passes() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset({"admins"}))
    client = TestClient(app)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"admins", "viewers"})))
    client.cookies.set("taskq_session", cookie)
    resp = client.get("/protected", headers={"accept": "application/json"})
    assert resp.status_code == 200


def test_user_not_in_allowed_group_returns_401() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset({"admins"}))
    client = TestClient(app)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"viewers"})))
    client.cookies.set("taskq_session", cookie)
    resp = client.get("/protected", headers={"accept": "application/json"})
    assert resp.status_code == 401


# ── Logout clears cookie ──────────────────────────────────────────────────


def test_logout_clears_cookie() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    app = _make_app(manager, frozenset())
    client = TestClient(app)
    cookie = manager.create_session_cookie(_make_claims())
    client.cookies.set("taskq_session", cookie)
    resp = client.get("/logout", follow_redirects=False)
    assert resp.status_code == 302
    set_cookie = resp.headers.get("set-cookie", "")
    assert "taskq_session=" in set_cookie
    assert "Max-Age=0" in set_cookie or "expires=" in set_cookie.lower()

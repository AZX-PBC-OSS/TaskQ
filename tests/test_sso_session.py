"""Tests for the shared SSO session module (_session.py).

Protocol-agnostic - covers cookie issuance/verification, expiry, tampering,
the group allowlist, and logout. Tested once here so both backends inherit the
guarantees without duplication.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any, ClassVar

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("itsdangerous")

from fastapi import Depends, FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.testclient import TestClient

from taskq.web.admin.auth._session import (
    IdentityClaims,
    SessionManager,
    create_auth_dependency,
    create_session_verifier,
)

pytestmark = [pytest.mark.fastapi]


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


# ── repr does not leak the signing key ────────────────────────────────────


def test_repr_masks_secret() -> None:
    """repr(SessionManager) never embeds the cookie-signing key.

    The secret can forge session cookies, so it is a credential; the default
    dataclass repr would print it into any log or debugger that reprs the
    manager. Non-secret knobs stay visible so the repr stays useful.
    """
    manager = SessionManager(secret="session-signing-key-DO-NOT-PRINT", max_age_seconds=3600)
    r = repr(manager)
    assert "session-signing-key-DO-NOT-PRINT" not in r
    assert "taskq_session" in r  # cookie_name still visible
    assert "max_age_seconds=3600" in r


def test_secret_remains_required_first_constructor_argument() -> None:
    """field(repr=False) adds no default: secret stays required, so every
    existing SessionManager(secret=..., ...) call site is untouched."""
    with pytest.raises(TypeError, match="secret"):
        SessionManager()  # type: ignore[reportCallIssue]  # Why: deliberately omitting the required secret to pin that the repr change added no default.


# ── Session re-check exposed for long-lived SSE streams (#316) ────────────


def _request_with_cookie(cookie: str | None) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if cookie is not None:
        headers.append((b"cookie", f"taskq_session={cookie}".encode()))
    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers,
        "query_string": b"",
    }
    return Request(scope)


def test_dependency_exposes_session_verifier_attribute() -> None:
    """The auth dependency carries the re-check the SSE routers derive (#316).

    A stream that authenticates only at request acceptance keeps delivering
    frames after its session is invalidated; the routers that own such streams
    re-invoke this attribute's callable before every streamed event and at
    every keepalive tick.
    """
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    dep = create_auth_dependency(manager, frozenset())
    assert getattr(dep, "session_verifier", None) is not None


async def test_session_verifier_accepts_a_valid_session() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"admins"})))
    verifier = create_session_verifier(manager, frozenset())
    assert await verifier(_request_with_cookie(cookie)) is True


async def test_session_verifier_rejects_a_rotated_secret() -> None:
    """Rotating session_secret invalidates the live stream: the cookie the
    stream was opened with no longer verifies under the new manager."""
    issuer = SessionManager(secret="secret-a-32bytes-long-aaaaaa!", max_age_seconds=3600)
    cookie = issuer.create_session_cookie(_make_claims())
    rotated = SessionManager(secret="secret-b-32bytes-long-bbbbbb!", max_age_seconds=3600)
    verifier = create_session_verifier(rotated, frozenset())
    assert await verifier(_request_with_cookie(cookie)) is False


async def test_session_verifier_rejects_an_expired_session() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims())
    expired_horizon = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=0)
    verifier = create_session_verifier(expired_horizon, frozenset())
    # Why the sleep: itsdangerous compares the cookie's age against
    # max_age_seconds; an immediate verify sees age < 1s and passes. The
    # same one-tick wait the expiry test above the fold uses.
    await asyncio.sleep(1.1)
    assert await verifier(_request_with_cookie(cookie)) is False


async def test_session_verifier_enforces_the_allowlist() -> None:
    """Tightening allowed_groups ends live streams on the next re-check."""
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"viewers"})))
    verifier = create_session_verifier(manager, frozenset({"admins"}))
    assert await verifier(_request_with_cookie(cookie)) is False


async def test_session_verifier_rejects_a_missing_cookie() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    verifier = create_session_verifier(manager, frozenset())
    assert await verifier(_request_with_cookie(None)) is False


async def test_session_verifier_no_allowlist_accepts_any_valid_session() -> None:
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims(groups=frozenset({"any"})))
    verifier = create_session_verifier(manager, frozenset())
    assert await verifier(_request_with_cookie(cookie)) is True


async def test_secret_rotation_by_assignment_reaches_a_live_streams_verifier() -> None:
    """Rotation via ``manager.secret = ...`` ends a live stream's re-check.

    A live stream holds the verifier derived at router-construction time: a
    closure over the SessionManager OBJECT, not a snapshot of its secret.
    verify_session_cookie therefore re-derives its signer when the secret
    has rotated, so the stream's next tick fails the stale cookie (fail
    closed). Pinning this: the pre-rotation-aware verifier kept validating
    the old cookie after the assignment, so a rotated deployment's live
    streams lived forever under the old key while sso.md claimed rotation
    ends them within one keepalive interval.
    """
    manager = SessionManager(secret="secret-a-32bytes-long-aaaaaa!", max_age_seconds=3600)
    dep = create_auth_dependency(manager, frozenset())
    dep_obj: Any = dep  # Why Any: the attribute is attached dynamically by create_auth_dependency; pyright cannot see it on the plain function type.
    verifier: Callable[..., Any] = dep_obj.session_verifier
    cookie = manager.create_session_cookie(_make_claims())
    request = _request_with_cookie(cookie)

    assert await verifier(request) is True
    manager.secret = "secret-b-32bytes-long-bbbbbb!"
    assert await verifier(request) is False, (
        "a rotated session_secret must fail the live stream's next re-check"
    )
    # Sessions minted after the rotation verify under the new key.
    new_cookie = manager.create_session_cookie(_make_claims())
    assert await verifier(_request_with_cookie(new_cookie)) is True


async def test_secret_restored_within_one_tick_revalidates() -> None:
    """The honest window, pinned: a stateless cookie re-check reads the
    CURRENT secret per tick, so a secret restored before the stream's next
    tick re-validates the session the operator briefly killed. That one-tick
    re-validation after a rotate-and-restore is inherent to a stateless
    design (there is no store to record revocation in); pinning it so a
    future change that silently widens the window has to confront this test.
    """
    secret_a = "secret-a-32bytes-long-aaaaaa!"
    manager = SessionManager(secret=secret_a, max_age_seconds=3600)
    dep = create_auth_dependency(manager, frozenset())
    dep_obj: Any = dep  # Why Any: the attribute is attached dynamically by create_auth_dependency; pyright cannot see it on the plain function type.
    verifier: Callable[..., Any] = dep_obj.session_verifier
    cookie = manager.create_session_cookie(_make_claims())
    request = _request_with_cookie(cookie)

    manager.secret = "secret-b-32bytes-long-bbbbbb!"
    manager.secret = secret_a  # restored before the next tick
    assert await verifier(request) is True


async def test_session_verifier_consults_only_the_cookie() -> None:
    """The re-check performs NO store round trip: any request attribute
    beyond ``cookies`` implies a session-store/database/IdP hit, and a
    1k-stream wall re-checking once per 60 s would turn the re-check into a
    per-second query storm. Pins the cookie-only contract sso.md states --
    including its corollary: server-side revocation of a still-validly-signed,
    still-unexpired cookie is impossible by construction."""
    manager = SessionManager(secret="test-secret-key-32bytes-long!!", max_age_seconds=3600)
    cookie = manager.create_session_cookie(_make_claims())
    verifier = create_session_verifier(manager, frozenset())

    class _ProbeRequest:
        # ClassVar: the probe is never instantiated with state; the class
        # attribute IS the cookie jar the contract allows the verifier to read.
        cookies: ClassVar[dict[str, str]] = {manager.cookie_name: cookie}

        def __getattr__(self, name: str) -> Any:
            raise AssertionError(
                f"the session re-check touched request.{name!r}: it must be "
                "cookie-only (no store, no DB round trip)"
            )

    assert await verifier(_ProbeRequest()) is True  # type: ignore[arg-type]  # Why: the probe deliberately satisfies only the cookie surface the contract allows.

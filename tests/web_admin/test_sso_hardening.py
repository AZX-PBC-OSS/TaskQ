"""Session-cookie scoping and the empty-allowlist warning on the SSO backends."""

from typing import Any

import pytest
import structlog.types
from fastapi.responses import Response

from taskq.web.admin.auth._session import (  # pyright: ignore[reportPrivateUsage]  # Why: SessionManager's cookie flags are the behaviour under test and are not re-exported.
    IdentityClaims,
    SessionManager,
)

_CLAIMS = IdentityClaims(subject="u1", email=None, groups=frozenset(), raw={})


def _cookie_line(response: Response, name: str) -> str:
    for line in response.raw_headers:
        if line[0].lower() == b"set-cookie" and line[1].startswith(name.encode()):
            return line[1].decode()
    raise AssertionError(f"no Set-Cookie for {name!r}")


def test_session_cookie_defaults_to_root_path() -> None:
    manager = SessionManager(secret="s" * 32)
    response = Response()
    manager.set_session_cookie(response, _CLAIMS)
    assert "Path=/" in _cookie_line(response, "taskq_session")


def test_session_cookie_is_scoped_to_the_mount_path() -> None:
    """A cookie sent to every path on the host app is handed to unrelated
    routes that have no business seeing an admin session."""
    manager = SessionManager(secret="s" * 32, cookie_path="/admin")
    response = Response()
    manager.set_session_cookie(response, _CLAIMS)
    assert "Path=/admin" in _cookie_line(response, "taskq_session")


def test_session_cookie_is_cleared_on_the_same_path() -> None:
    """A delete_cookie on a different path does not clear the cookie at all,
    so logout would leave a live session behind."""
    manager = SessionManager(secret="s" * 32, cookie_path="/admin")
    response = Response()
    manager.clear_session_cookie(response)
    assert "Path=/admin" in _cookie_line(response, "taskq_session")


def _oidc_config(**overrides: Any) -> Any:
    from taskq.web.admin.auth import OIDCAuthConfig

    kwargs: dict[str, Any] = {
        "issuer": "https://issuer.example/v2.0",
        "client_id": "cid",
        "client_secret": "sec",
        "redirect_uri": "https://app.example/admin/callback",
        "session_secret": "s" * 32,
    }
    kwargs.update(overrides)
    return OIDCAuthConfig(**kwargs)


def test_oidc_warns_when_any_authenticated_tenant_user_gets_admin(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """``allowed_groups`` empty means completing SSO against the tenant is the
    whole authorization check — intentional, but the operator must be told."""
    pytest.importorskip("authlib")
    from taskq.web.admin.auth import create_oidc_auth

    create_oidc_auth(_oidc_config(), base_path="/admin")

    events = [e for e in structlog_capture if e.get("event") == "admin-sso-no-group-allowlist"]
    assert events, [e.get("event") for e in structlog_capture]
    assert events[0]["log_level"] == "warning"


def test_oidc_does_not_warn_when_an_allowlist_is_configured(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    pytest.importorskip("authlib")
    from taskq.web.admin.auth import create_oidc_auth

    create_oidc_auth(
        _oidc_config(group_claim="groups", allowed_groups=frozenset({"admins"})),
        base_path="/admin",
    )

    events = [e for e in structlog_capture if e.get("event") == "admin-sso-no-group-allowlist"]
    assert not events, events


def _saml_config(**overrides: Any) -> Any:
    from taskq.web.admin.auth import SAMLAuthConfig

    kwargs: dict[str, Any] = {
        "entity_id": "sp",
        "acs_url": "https://app.example/admin/callback",
        "idp_entity_id": "idp",
        "idp_sso_url": "https://idp.example/sso",
        "idp_x509_cert": "cert",
        "session_secret": "s" * 32,
    }
    kwargs.update(overrides)
    return SAMLAuthConfig(**kwargs)


def test_saml_warns_when_any_authenticated_tenant_user_gets_admin(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    pytest.importorskip("onelogin.saml2")
    from taskq.web.admin.auth import create_saml_auth

    create_saml_auth(_saml_config(), base_path="/admin")

    events = [e for e in structlog_capture if e.get("event") == "admin-sso-no-group-allowlist"]
    assert events, [e.get("event") for e in structlog_capture]


def test_sso_session_cookie_is_scoped_to_the_admin_mount(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """End-to-end: the backend factory must propagate ``base_path`` into the
    session cookie's ``Path``."""
    pytest.importorskip("authlib")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin.auth import create_oidc_auth
    from taskq.web.admin.auth._session import (  # pyright: ignore[reportPrivateUsage]  # Why: the token derivation is the behaviour under test and is not re-exported.
        logout_csrf_token,
    )

    bundle = create_oidc_auth(_oidc_config(), base_path="/admin")
    app = FastAPI()
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app)
    session_cookie = _crafted_session_cookie()
    client.cookies.set("taskq_session", session_cookie)

    response = client.post(
        "/admin/logout",
        data={"csrf_token": logout_csrf_token("s" * 32, session_cookie)},
        follow_redirects=False,
    )

    lines = [line for line in response.headers.get_list("set-cookie") if "taskq_session" in line]
    assert lines, response.headers.get_list("set-cookie")
    assert "Path=/admin" in lines[0], lines[0]


# ── logout is a POST with a session-bound CSRF token, on both backends ────


def _crafted_session_cookie() -> str:
    """A valid signed session-cookie value for the shared test secret."""
    manager = SessionManager(secret="s" * 32)
    response = Response()
    manager.set_session_cookie(response, _CLAIMS)
    value = next(
        (line for line in response.raw_headers if line[0].lower() == b"set-cookie"),
        (b"", b""),
    )[1].decode()
    return value.split(";", 1)[0].split("=", 1)[1]


def _logout_pins_for(
    app: Any,
    *,
    base_path: str = "/admin",
) -> None:
    """Shared logout hardening assertions for one backend's mounted router.

    A forced top-level navigation is a GET and used to clear the admin
    session on both backends; the pins below hold either backend to the same
    contract, which is why they live with the shared session machinery's
    tests rather than in one backend's family.
    """
    from fastapi.testclient import TestClient

    from taskq.web.admin.auth._session import (  # pyright: ignore[reportPrivateUsage]  # Why: the token derivation is the behaviour under test and is not re-exported.
        logout_csrf_token,
    )

    client = TestClient(app)
    session_cookie = _crafted_session_cookie()
    client.cookies.set("taskq_session", session_cookie)

    # GET — the shape a forced top-level navigation produces — is refused.
    got = client.get(f"{base_path}/logout", follow_redirects=False)
    assert got.status_code == 405, got.status_code
    assert not any("taskq_session=" in header for header in got.headers.get_list("set-cookie")), (
        "the refused GET still cleared the session cookie"
    )

    # POST without the token, and with a wrong token, is refused.
    for data in ({}, {"csrf_token": "0" * 64}):
        posted = client.post(f"{base_path}/logout", data=data, follow_redirects=False)
        assert posted.status_code == 403, (data, posted.status_code)
        assert not any(
            "taskq_session=" in header for header in posted.headers.get_list("set-cookie")
        ), f"POST with {data or 'no token'} still cleared the session cookie"

    # POST with the token derived from the live session clears it, on the
    # session cookie's own path.
    token = logout_csrf_token("s" * 32, session_cookie)
    posted = client.post(
        f"{base_path}/logout",
        data={"csrf_token": token},
        follow_redirects=False,
    )
    assert posted.status_code == 302, posted.status_code
    cleared = [
        header
        for header in posted.headers.get_list("set-cookie")
        if header.startswith("taskq_session=")
    ]
    assert cleared, "the accepted POST cleared no session cookie"
    line = cleared[0].lower()
    assert "max-age=0" in line or "expires=" in line, cleared[0]
    assert f"path={base_path}" in line, cleared[0]


def test_oidc_logout_is_post_with_a_session_bound_csrf_token() -> None:
    pytest.importorskip("authlib")
    from fastapi import FastAPI

    from taskq.web.admin.auth import create_oidc_auth

    bundle = create_oidc_auth(_oidc_config(), base_path="/admin")
    app = FastAPI()
    app.include_router(bundle.router, prefix="/admin")
    _logout_pins_for(app)


def test_saml_logout_is_post_with_a_session_bound_csrf_token() -> None:
    pytest.importorskip("onelogin.saml2")
    from fastapi import FastAPI

    from taskq.web.admin.auth import create_saml_auth

    bundle = create_saml_auth(_saml_config(), base_path="/admin")
    app = FastAPI()
    app.include_router(bundle.router, prefix="/admin")
    _logout_pins_for(app)

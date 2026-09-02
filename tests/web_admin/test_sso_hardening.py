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

    bundle = create_oidc_auth(_oidc_config(), base_path="/admin")
    app = FastAPI()
    app.include_router(bundle.router, prefix="/admin")
    response = TestClient(app).get("/admin/logout", follow_redirects=False)

    lines = [line for line in response.headers.get_list("set-cookie") if "taskq_session" in line]
    assert lines, response.headers.get_list("set-cookie")
    assert "Path=/admin" in lines[0], lines[0]

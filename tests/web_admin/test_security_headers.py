"""Framing defence and CSRF-cookie ``Secure`` behaviour on the admin UI.

Both gaps matter specifically behind a TLS-terminating edge (Azure Application
Gateway / App Service): the app sees plain ``http`` internally, so anything
derived from ``request.url.scheme`` is wrong there.

Every assertion here is on an observable response header — never on source text.
"""

from collections.abc import Callable

import pytest
import structlog.types
from fastapi.testclient import TestClient


def _set_cookie(client: TestClient, path: str, name: str) -> str:
    """Return the raw ``Set-Cookie`` line for *name* from a GET of *path*."""
    response = client.get(path)
    assert response.status_code == 200, response.status_code
    for line in response.headers.get_list("set-cookie"):
        if line.startswith(f"{name}="):
            return line
    raise AssertionError(f"no Set-Cookie for {name!r}: {response.headers.get_list('set-cookie')}")


# ── Gap 1: clickjacking / UI redress ──────────────────────────────────────


def test_html_response_forbids_being_framed(make_app: Callable[..., TestClient]) -> None:
    """An admin page must not be loadable inside a third-party frame.

    CSRF is no defence: the framed page is the real, same-origin, already-
    authenticated page, so a click the attacker tricks the user into carries
    both the session cookie and a valid CSRF token.
    """
    client = make_app()
    response = client.get("/queues")

    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_framing_defence_is_configurable_to_same_origin(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host app embedding the admin UI in its own dashboard can opt into
    same-origin framing; the default stays the closed one."""
    monkeypatch.setenv("TASKQ_ADMIN_UI_FRAME_ANCESTORS", "self")
    client = make_app()
    response = client.get("/queues")

    assert response.headers["x-frame-options"] == "SAMEORIGIN"
    assert "frame-ancestors 'self'" in response.headers["content-security-policy"]


def test_invalid_frame_ancestors_value_is_rejected(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo must fail loudly rather than silently emitting a header that
    permits framing."""
    from dotenvmodel import ConstraintViolationError

    monkeypatch.setenv("TASKQ_ADMIN_UI_FRAME_ANCESTORS", "everyone")
    with pytest.raises(ConstraintViolationError, match=r"admin_ui_frame_ancestors"):
        make_app()


# ── Gap 2: CSRF cookie Secure behind TLS termination ──────────────────────


def test_secure_cookies_defaults_to_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """The suite-wide fixture turns this off for http TestClient traffic, so
    pin the shipped default here."""
    from taskq.settings import TaskQSettings

    monkeypatch.delenv("TASKQ_ADMIN_UI_SECURE_COOKIES", raising=False)
    assert TaskQSettings.load().admin_ui_secure_cookies is True


def test_csrf_cookie_is_secure_over_plain_http_behind_a_tls_terminator(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TestClient speaks http://testserver — exactly what the app sees behind
    Azure Application Gateway. Deriving ``secure`` from the observed scheme
    silently drops the flag there; the configured value must not."""
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "true")
    client = make_app()
    line = _set_cookie(client, "/queues", "taskq_csrf_token")
    assert "Secure" in line, line


def test_csrf_cookie_secure_can_be_disabled_for_local_http_dev(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    client = make_app()
    line = _set_cookie(client, "/queues", "taskq_csrf_token")
    assert "Secure" not in line, line


def test_insecure_cookie_over_https_warns(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """Serving real HTTPS with secure cookies switched off is a misconfiguration
    that costs the session on the next plaintext hop — say so loudly."""
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    client = make_app()
    client.get("https://testserver/queues")

    events = [e for e in structlog_capture if e.get("event") == "admin-ui-cookie-scheme-mismatch"]
    assert events, [e.get("event") for e in structlog_capture]
    assert events[0]["log_level"] == "warning"


def test_scheme_mismatch_warning_fires_once_not_per_request(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    client = make_app()
    for _ in range(3):
        client.get("https://testserver/queues")

    events = [e for e in structlog_capture if e.get("event") == "admin-ui-cookie-scheme-mismatch"]
    assert len(events) == 1, events


def test_matching_scheme_and_configuration_does_not_warn(
    make_app: Callable[..., TestClient],
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "true")
    client = make_app()
    client.get("https://testserver/queues")

    events = [e for e in structlog_capture if e.get("event") == "admin-ui-cookie-scheme-mismatch"]
    assert not events, events

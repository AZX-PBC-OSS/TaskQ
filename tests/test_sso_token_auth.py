"""Tests for the bearer-token auth dependency (token_auth)."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from taskq.web.admin.auth.token import token_auth

pytestmark = [pytest.mark.fastapi]
_TOKEN = "test-secret-token-12345"


def _make_app(expected_token: str) -> FastAPI:
    dep = token_auth(expected_token)
    app = FastAPI()

    @app.get("/protected")  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
    async def protected(_user: str = Depends(dep)) -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]  # Why: registered via decorator.
        return {"status": "ok"}

    return app


def test_valid_token_passes() -> None:
    app = _make_app(_TOKEN)
    client = TestClient(app)
    resp = client.get(
        "/protected",
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_missing_token_returns_401() -> None:
    app = _make_app(_TOKEN)
    client = TestClient(app)
    resp = client.get("/protected")
    assert resp.status_code == 401


def test_wrong_token_returns_401() -> None:
    app = _make_app(_TOKEN)
    client = TestClient(app)
    resp = client.get(
        "/protected",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_empty_expected_token_raises_value_error() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        token_auth("")


# ── Session re-check exposed for long-lived SSE streams (#316) ────────────


def _request_with_bearer(token: str | None) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers,
        "query_string": b"",
    }
    return Request(scope)


def test_token_auth_exposes_session_verifier_attribute() -> None:
    """The dependency carries the re-check SSE streams derive from it."""
    dep = token_auth(_TOKEN)
    assert getattr(dep, "session_verifier", None) is not None


async def test_session_verifier_accepts_the_live_token() -> None:
    dep = token_auth(_TOKEN)
    verifier = dep.session_verifier  # pyright: ignore[reportFunctionMemberAccess]  # Why: pinned by the test above.
    assert await verifier(_request_with_bearer(_TOKEN)) is True


async def test_session_verifier_rejects_revoked_token() -> None:
    """A token that no longer matches (rotation) fails the re-check."""
    dep = token_auth(_TOKEN)
    verifier = dep.session_verifier  # pyright: ignore[reportFunctionMemberAccess]
    assert await verifier(_request_with_bearer("rotated-away-token")) is False


async def test_session_verifier_rejects_missing_authorization() -> None:
    dep = token_auth(_TOKEN)
    verifier = dep.session_verifier  # pyright: ignore[reportFunctionMemberAccess]
    assert await verifier(_request_with_bearer(None)) is False


async def test_session_verifier_rejects_wrong_scheme() -> None:
    dep = token_auth(_TOKEN)
    verifier = dep.session_verifier  # pyright: ignore[reportFunctionMemberAccess]
    assert await verifier(_request_with_bearer("Basic " + _TOKEN)) is False

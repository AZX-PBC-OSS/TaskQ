"""Tests for the bearer-token auth dependency (token_auth)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin.auth.token import token_auth

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

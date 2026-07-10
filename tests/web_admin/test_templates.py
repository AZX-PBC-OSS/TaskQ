"""Tests for base template rendering, static file serving, and template safety."""

from collections.abc import Callable
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from pathlib import Path

from jinja2 import nodes as jinja2_nodes

from taskq.web.admin import create_router

from . import _StubPool


def _render_base(
    monkeypatch: pytest.MonkeyPatch,
    pool: _StubPool,
    realtime_mode: str = "polling",
    mode_label: str = "polling mode",
) -> str:
    """Render _base.html with the given realtime_mode/mode_label and return the HTML string."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("_base.html")
    return template.render(realtime_mode=realtime_mode, mode_label=mode_label)


def test_base_template_polling_mode_shows_badge_and_meta_refresh(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Polling mode: badge reads 'polling mode' and meta refresh is present."""
    html = _render_base(monkeypatch=monkeypatch, pool=stub_pool)
    assert "polling mode" in html
    assert '<meta http-equiv="refresh" content="2">' in html


def test_base_template_realtime_mode_shows_badge_no_meta_refresh(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Real-time mode: badge reads 'real-time mode' and meta refresh is absent."""
    html = _render_base(
        monkeypatch=monkeypatch,
        pool=stub_pool,
        realtime_mode="realtime",
        mode_label="real-time mode",
    )
    assert "real-time mode" in html
    assert '<meta http-equiv="refresh"' not in html


def test_base_template_contains_htmx_script_tag(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: _base.html includes an HTMX script tag referencing the /static/ path."""
    html = _render_base(monkeypatch=monkeypatch, pool=stub_pool)
    assert '<script src="/static/htmx.min.js"></script>' in html


def test_base_template_contains_css_stylesheet_link(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: _base.html links to local /static/admin.css."""
    html = _render_base(monkeypatch=monkeypatch, pool=stub_pool)
    assert "/static/admin.css" in html


def test_base_template_contains_navigation_links(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: nav contains links to all required pages."""
    html = _render_base(monkeypatch=monkeypatch, pool=stub_pool)
    for path in ["/queues", "/workers", "/schedules", "/rate-limits", "/reservations", "/leader"]:
        assert f'href="{path}"' in html


def test_base_template_uses_semantic_elements(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: HTML uses semantic elements: <header>, <nav>, <main>, <footer>."""
    html = _render_base(monkeypatch=monkeypatch, pool=stub_pool)
    for tag in [
        "<header",
        "</header>",
        "<nav",
        "</nav>",
        "<main",
        "</main>",
        "<footer",
        "</footer>",
    ]:
        assert tag in html


def test_base_template_has_content_block_slot(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: _base.html provides a {% block content %} slot."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "_base.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router; assert env is not None covers it.
    ast = env.parse(source)  # pyright: ignore[reportUnknownMemberType]  # Why: env.parse returns Template; Jinja2 API contract is well-known.
    block_names = [node.name for node in ast.find_all(jinja2_nodes.Block)]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: jinja2 AST traversal; find_all returns Block nodes with .name attribute.
    assert "content" in block_names


def test_static_file_serves_admin_css(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: GET /static/admin.css returns HTTP 200 with CSS content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/static/admin.css")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "css" in ct


def test_templates_no_safe_filter() -> None:
    """/ : No |safe filter on any variable in templates."""
    templates_dir = (
        Path(__file__).resolve().parent.parent.parent / "src" / "taskq" / "web" / "templates"
    )
    for template_path in templates_dir.rglob("*.html"):
        content = template_path.read_text()
        assert "|safe" not in content, (
            f"|safe found in {template_path.relative_to(templates_dir.parent.parent.parent)}"
        )


def test_static_file_returns_200_for_existing_file(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /static/htmx.min.js returns HTTP 200 with correct Content-Type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/static/htmx.min.js")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert (
        "javascript" in ct
    )  # DoD says application/javascript but Python mimetypes returns text/javascript (RFC 9239); both are valid JS media types


def test_static_file_returns_404_for_nonexistent(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /static/nonexistent.js returns HTTP 404."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/static/nonexistent.js")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 404  # pyright: ignore[reportUnknownVariableType]


def test_static_file_path_traversal_returns_404(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /static/../admin.py (path traversal) returns HTTP 404, not 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/static/../admin/__init__.py")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 404  # pyright: ignore[reportUnknownVariableType]


def test_static_file_rejects_sibling_directory_with_shared_prefix(tmp_path: Path) -> None:
    """A sibling directory whose name shares a string prefix with static_dir is rejected.

    Regression for the unsafe ``str.startswith`` traversal check: a request
    that resolves to ``<parent>/static_evil/...`` would pass a naive
    ``startswith("<parent>/static")`` guard because the string prefix matches,
    even though the path is outside the static directory. ``is_relative_to``
    correctly rejects it.

    The ``..`` segment is URL-encoded as ``%2e%2e`` so the HTTP client does not
    normalise it away before it reaches the route handler.
    """
    from fastapi import APIRouter, FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin._static import register

    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "real.css").write_text("body { }")

    sibling = tmp_path / "static_evil"
    sibling.mkdir()
    secret = sibling / "secret.txt"
    secret.write_text("secret")

    app = FastAPI()
    router = APIRouter()
    register(router, static_dir)
    app.include_router(router)

    client = TestClient(app)
    # Resolves to <tmp_path>/static_evil/secret.txt — outside static_dir but
    # shares the "/static" string prefix. %2e%2e avoids client-side path
    # normalisation so the traversal reaches the handler.
    response = client.get("/static/%2e%2e/static_evil/secret.txt")  # pyright: ignore[reportUnknownVariableType]
    assert response.status_code == 404  # pyright: ignore[reportUnknownVariableType]

    # Sanity: a legitimate file inside static_dir still serves.
    response = client.get("/static/real.css")  # pyright: ignore[reportUnknownVariableType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]

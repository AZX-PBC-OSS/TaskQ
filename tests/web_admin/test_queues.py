"""Tests for queue routes and templates in taskq.web.admin."""

from collections.abc import Callable
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router

from . import _StubPool

# ── Queue routes: discovery and registration ───────────────────────────


def test_queue_routes_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /queues and GET /queues/{queue} routes are present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/queues" in route_paths
    assert any(p == "/queues/{queue:path}" for p in route_paths)  # pyright: ignore[reportUnknownVariableType]


def test_queue_overview_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /queues returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_queue_detail_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /queues/{queue} returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


# ── Queue detail: invalid status filter ────────────────────────────────


def test_queue_detail_invalid_status_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Invalid ?status= value returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?status=invalid")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: invalid cursor validation ────────────────────────────


def test_queue_detail_invalid_cursor_at_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Non-ISO8601 cursor_at returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get(  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
        "/queues/default?cursor_at=not-a-date&cursor_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_invalid_cursor_id_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Non-UUID cursor_id returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_at=2025-01-01T00:00:00&cursor_id=not-a-uuid")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_partial_cursor_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Only one of cursor_at/cursor_id provided returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_at=2025-01-01T00:00:00")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_cursor_id_only_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Only cursor_id provided (without cursor_at) returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_id=00000000-0000-0000-0000-000000000000")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: no cursor on first page ──────────────────────────────


def test_queue_detail_first_page_no_cursor_params(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """First page loads without cursor params and returns 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: status tab defaults to pending ───────────────────────


def test_queue_detail_default_status_is_pending(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Default status filter is 'pending' — the pending tab carries the active class."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    html = response.text  # pyright: ignore[reportUnknownVariableType]
    assert "border-blue-500" in html and "pending" in html


# ── Templates extend _base.html ────────────────────────────────────────


def test_queues_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: queues.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "queues.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_queue_detail_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: queue_detail.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "queue_detail.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


# ── Template rendering with data ────────────────────────────────────────


def test_queues_template_renders_queue_data(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """queues.html renders queue names and counts from template data."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queues.html")
    html = template.render(
        queues=[
            {"queue": "default", "pending_count": 5, "scheduled_count": 2, "running_count": 1},
            {"queue": "emails", "pending_count": 0, "scheduled_count": 0, "running_count": 3},
        ]
    )
    assert "default" in html
    assert "emails" in html
    assert "/queues/default" in html
    assert "/queues/emails" in html


def test_queue_detail_template_renders_pagination_link(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """queue_detail.html renders next-page link when has_next is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "status": "pending",
                "scheduled_at": "2025-01-01T00:00:00",
                "attempt": 1,
                "max_attempts": 3,
            }
        ],
        has_next=True,
        next_cursor_at="2025-01-01T00:01:00",
        next_cursor_id="def-456",
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" in html
    assert "cursor_at=2025-01-01T00%3A01%3A00" in html
    assert "cursor_id=def-456" in html


def test_queue_detail_template_urlencodes_timezone_cursor(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Timezone-aware cursor_at with + is URL-encoded so + is not parsed as a space."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "status": "pending",
                "scheduled_at": "2025-01-01T00:00:00",
                "attempt": 1,
                "max_attempts": 3,
            }
        ],
        has_next=True,
        next_cursor_at="2025-01-01T00:00:00+00:00",
        next_cursor_id="00000000-0000-0000-0000-000000000000",
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" in html
    assert "cursor_at=2025-01-01T00%3A00%3A00%2B00%3A00" in html
    assert "cursor_id=00000000-0000-0000-0000-000000000000" in html


def test_queue_detail_template_no_pagination_link_when_last_page(
    monkeypatch: pytest.MonkeyPatch,
    stub_pool: _StubPool,
) -> None:
    """queue_detail.html does not render next-page link when has_next is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[],
        has_next=False,
        next_cursor_at=None,
        next_cursor_id=None,
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" not in html

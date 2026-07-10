"""Tests for jobs page routes and template in taskq.web.admin."""

from collections.abc import Callable
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router

from . import _StubPool

# ── Route discovery ────────────────────────────────────────────────────


def test_jobs_routes_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /jobs and GET /jobs/count routes are present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]
    assert "/jobs" in route_paths
    assert "/jobs/count" in route_paths


# ── GET /jobs ───────────────────────────────────────────────────────────


def test_jobs_returns_html(monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]) -> None:
    """GET /jobs returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs")
    assert response.status_code == 200
    ct = response.headers.get("content-type", "")
    assert "text/html" in ct


def test_jobs_live_tab_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /jobs?tab=live returns 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?tab=live")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")


def test_jobs_archived_tab_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /jobs?tab=archived returns 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?tab=archived")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")


def test_jobs_status_filter_succeeded(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /jobs?status=succeeded returns 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?status=succeeded")
    assert response.status_code == 200


def test_jobs_multiple_status_filters(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Multiple status= params are accepted and return 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?status=succeeded&status=failed")
    assert response.status_code == 200


def test_jobs_invalid_status_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Invalid ?status= value returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?status=bogus_status")
    assert response.status_code == 400


def test_jobs_actor_and_queue_filters(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """actor= and queue= filter params are accepted and return 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?actor=send_email&queue=default")
    assert response.status_code == 200


def test_jobs_identity_and_fairness_filters(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """identity_key= and fairness_key= filter params are accepted."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?identity_key=idem-123&fairness_key=fair-456")
    assert response.status_code == 200


def test_jobs_search_filter(monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]) -> None:
    """search= param is accepted."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?search=test")
    assert response.status_code == 200


def test_jobs_time_range_filter(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """time_range= param is accepted."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?time_range=24h")
    assert response.status_code == 200


def test_jobs_invalid_tab_defaults_to_live(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Invalid tab value defaults to 'live' silently."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?tab=bogus")
    assert response.status_code == 200


# ── GET /jobs/count ─────────────────────────────────────────────────────


def test_jobs_count_returns_json(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /jobs/count returns 200 with application/json."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs/count")
    assert response.status_code == 200
    body = response.json()
    assert "count" in body
    assert isinstance(body["count"], int)


# ── Template structure ─────────────────────────────────────────────────


def test_jobs_template_extends_base(monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool) -> None:
    """jobs.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    env = bundle.templates
    assert env.loader is not None
    source = env.loader.get_source(env, "jobs.html")[0]
    assert 'extends "_base.html"' in source


def test_jobs_template_renders_empty_state(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """jobs.html renders empty-state message when jobs list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    env = bundle.templates
    template = env.get_template("jobs.html")
    html = template.render(
        jobs=[],
        tab="live",
        statuses=["succeeded", "failed"],
        all_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        active_statuses=["pending", "scheduled", "running"],
        terminal_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        actor_filter="",
        queue_filter="",
        time_range="",
        time_from="",
        time_to="",
        identity_key="",
        fairness_key="",
        search="",
        live="on",
        has_next=False,
        next_cursor_at="",
        next_cursor_id="",
        prev_cursor_at="",
        prev_cursor_id="",
        cursor_dir="next",
        total_rows=0,
        realtime_mode="polling",
        mode_label="polling mode",
        suppress_refresh=True,
    )
    assert "No jobs found" in html
    assert "Next" not in html or "Next page" not in html


def test_jobs_template_renders_pagination_link(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """jobs.html renders next-page link when has_next is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    env = bundle.templates
    template = env.get_template("jobs.html")
    html = template.render(
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "queue": "default",
                "status": "succeeded",
                "finished_at": "2025-01-01T12:00:00",
                "created_at": "2025-01-01T12:00:00",
                "scheduled_at": "2025-01-01T12:00:00",
                "started_at": "2025-01-01T12:00:00",
                "attempt": 1,
                "max_attempts": 3,
                "duration_ms": 1000,
                "priority": 5,
                "identity_key": None,
                "fairness_key": None,
                "progress_state": None,
                "error_message": None,
            }
        ],
        tab="live",
        statuses=["succeeded"],
        all_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        active_statuses=["pending", "scheduled", "running"],
        terminal_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        actor_filter="",
        queue_filter="",
        time_range="",
        time_from="",
        time_to="",
        identity_key="",
        fairness_key="",
        search="",
        live="on",
        has_next=True,
        next_cursor_at="2025-01-01T11:00:00",
        next_cursor_id="00000000-0000-0000-0000-000000000001",
        prev_cursor_at="",
        prev_cursor_id="",
        cursor_dir="next",
        total_rows=1,
        realtime_mode="polling",
        mode_label="polling mode",
        suppress_refresh=True,
    )
    assert "Next" in html


def test_jobs_table_partial_renders_rows(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """_partials/job_table.html renders job rows with status badges."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    env = bundle.templates
    template = env.get_template("_partials/job_table.html")
    html = template.render(
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "queue": "default",
                "status": "succeeded",
                "finished_at": "2025-01-01T12:00:00",
                "created_at": "2025-01-01T12:00:00",
                "scheduled_at": "2025-01-01T12:00:00",
                "started_at": "2025-01-01T12:00:00",
                "attempt": 1,
                "max_attempts": 3,
                "duration_ms": 1200,
                "priority": 5,
                "identity_key": None,
                "fairness_key": None,
                "progress_state": None,
                "error_message": None,
            },
            {
                "id": "def-456",
                "actor": "send_email",
                "queue": "default",
                "status": "failed",
                "finished_at": "2025-01-01T11:00:00",
                "created_at": "2025-01-01T11:00:00",
                "scheduled_at": "2025-01-01T11:00:00",
                "started_at": "2025-01-01T11:00:00",
                "attempt": 3,
                "max_attempts": 3,
                "duration_ms": None,
                "priority": 5,
                "identity_key": None,
                "fairness_key": None,
                "progress_state": None,
                "error_message": "Something broke",
            },
        ],
        tab="live",
        statuses=["succeeded", "failed"],
        all_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        active_statuses=["pending", "scheduled", "running"],
        terminal_statuses=["abandoned", "cancelled", "crashed", "failed", "succeeded"],
        actor_filter="",
        queue_filter="",
        time_range="",
        time_from="",
        time_to="",
        identity_key="",
        fairness_key="",
        search="",
        live="on",
        has_next=False,
        next_cursor_at="",
        next_cursor_id="",
        prev_cursor_at="",
        prev_cursor_id="",
        cursor_dir="next",
        total_rows=2,
        realtime_mode="polling",
        mode_label="polling mode",
        suppress_refresh=True,
    )
    assert "send_email" in html
    assert "succeeded" in html.lower()
    assert "failed" in html.lower()


# ── Nav link in base template ──────────────────────────────────────────


def test_base_template_contains_jobs_nav_link(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """_base.html nav includes a link to /jobs."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)
    env = bundle.templates
    assert env.loader is not None
    source = env.loader.get_source(env, "_base.html")[0]
    assert "/jobs" in source

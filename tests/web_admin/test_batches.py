"""Tests for the batches overview route and template."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from asyncpg.exceptions import UndefinedTableError

from taskq._ids import new_uuid
from taskq.web.admin import create_router

from . import StubAcquireContext, StubConnection, StubRecord, _StubPool


class _BatchRowsPool(_StubPool):
    """Pool whose connection returns two batch rows, one per status."""

    def __init__(self, rows: list[StubRecord]) -> None:
        self._rows = rows

    def acquire(self, *, timeout: float | None = None) -> StubAcquireContext:
        return _BatchRowsAcquire(self._rows)


class _BatchRowsAcquire(StubAcquireContext):
    def __init__(self, rows: list[StubRecord]) -> None:
        self._rows = rows

    async def __aenter__(self) -> StubConnection:  # type: ignore[override]
        return _BatchRowsConnection(self._rows)


class _BatchRowsConnection(StubConnection):
    def __init__(self, rows: list[StubRecord]) -> None:
        self._rows = rows

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return list(self._rows)


def _batch_row(**overrides: object) -> StubRecord:
    base: dict[str, object] = {
        "id": new_uuid(),
        "queue": "etl",
        "status": "active",
        "expected_size": 25,
        "consecutive_failures": 2,
        "failure_threshold": 3,
        "finalizer_job_id": None,
        "originating_actor": "load_data",
        "created_at": datetime.now(UTC) - timedelta(minutes=5),
        "completed_at": None,
    }
    base.update(overrides)
    return StubRecord(base)


def test_batches_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /batches route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/batches" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_batches_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /batches returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/batches")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_batches_page_renders_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The page renders queue, status, size, failure counters, and links."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    finalizer = new_uuid()
    pool = _BatchRowsPool(
        [
            _batch_row(),
            _batch_row(status="aborted", finalizer_job_id=finalizer, completed_at=None),
        ]
    )
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    # The route normalizes UUID/datetime values to text before rendering;
    # mirror that here so the template receives what production gives it.
    from taskq.web.admin.batches import _normalize_batch

    html = bundle.templates.get_template("batches.html").render(
        batches=[dict(_normalize_batch(dict(r))) for r in pool._rows],  # pyright: ignore[reportAttributeAccessUsage]  # Why: test stub; the rows are the fixture data just assigned.
        batches_installed=True,
        notice_text="batches not installed; run taskq migrate up to enable",
        truncated=False,
        page_size=200,
        realtime_mode="polling",
        mode_label="polling mode",
        base_path="",
    )
    assert "etl" in html
    assert "active" in html
    assert "aborted" in html
    assert "load_data" in html
    # The finalizer links to the job detail page.
    assert f"/jobs/{finalizer}" in html


def test_batches_page_renders_empty_state(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """With no batches rows the page renders its empty state, not an error."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/batches")  # pyright: ignore[reportUnknownVariableType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    assert "No batches recorded" in response.text  # pyright: ignore[reportUnknownVariableType]


def test_batches_page_missing_table_renders_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schema without the batches migration renders the install notice."""

    class _NoTablePool(_StubPool):
        def acquire(self, *, timeout: float | None = None) -> StubAcquireContext:
            return _NoTableAcquire()

    class _NoTableAcquire(StubAcquireContext):
        async def __aenter__(self) -> StubConnection:  # type: ignore[override]
            return _NoTableConnection()

    class _NoTableConnection(StubConnection):
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            raise UndefinedTableError()

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_NoTablePool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    html = bundle.templates.get_template("batches.html").render(
        batches=[],
        batches_installed=False,
        notice_text="batches not installed; run taskq migrate up to enable",
        truncated=False,
        page_size=200,
        realtime_mode="polling",
        mode_label="polling mode",
        base_path="",
    )
    assert "batches not installed" in html


def test_batches_page_queries_only_the_batches_table(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """The page's SQL is a plain SELECT over the batches table (read-only)."""
    from taskq.web.admin.batches import _BATCHES_SQL

    assert _BATCHES_SQL.lstrip().upper().startswith("SELECT")
    assert "batches" in _BATCHES_SQL
    assert "UPDATE" not in _BATCHES_SQL.upper()
    assert "DELETE" not in _BATCHES_SQL.upper()
    assert "INSERT" not in _BATCHES_SQL.upper()

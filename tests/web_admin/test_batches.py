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

pytestmark = [pytest.mark.fastapi]


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


# ── Batch drilldown: GET /batches/{batch_id} (issue #337) ────────────────


class _BatchDetailConnection(StubConnection):
    """Connection answering the drilldown's reads, routed by SQL shape.

    ``fetchrow`` serves the batches-table row (or raises UndefinedTableError
    when the migration has not run); ``fetch`` serves the member status
    counts and the member rows; ``fetchval`` serves the archived-member
    count and the router's clock probe."""

    def __init__(
        self,
        batch_row: StubRecord | None = None,
        counts: list[StubRecord] | None = None,
        members: list[StubRecord] | None = None,
        archived_members: int = 0,
        batches_missing: bool = False,
    ) -> None:
        self._batch_row = batch_row
        self._counts = counts or []
        self._members = members or []
        self._archived_members = archived_members
        self._batches_missing = batches_missing

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        if "batches" in query:
            if self._batches_missing:
                raise UndefinedTableError()
            return self._batch_row
        return None

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if "GROUP BY status" in query:
            return self._counts
        if "batch_id" in query:
            return self._members
        return []

    async def fetchval(self, query: str, *args: object) -> object:
        if query.strip() == "SELECT clock_timestamp()":
            return datetime.now(UTC)
        if "jobs_archive" in query:
            return self._archived_members
        return 0


class _BatchDetailPool(_StubPool):
    def __init__(self, conn: _BatchDetailConnection) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> StubAcquireContext:
        return _BatchDetailAcquire(self._conn)


class _BatchDetailAcquire(StubAcquireContext):
    def __init__(self, conn: _BatchDetailConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> StubConnection:  # type: ignore[override]
        return self._conn


def _batch_detail_app(monkeypatch: pytest.MonkeyPatch, conn: _BatchDetailConnection) -> Any:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_BatchDetailPool(conn))  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _member_row(job_id: Any, **overrides: object) -> StubRecord:
    base: dict[str, object] = {
        "id": job_id,
        "actor": "load_data",
        "queue": "etl",
        "status": "pending",
        "attempt": 0,
        "max_attempts": 3,
        "retry_kind": "transient",
        "created_at": datetime.now(UTC) - timedelta(minutes=4),
        "finished_at": None,
    }
    base.update(overrides)
    return StubRecord(base)


def test_batch_detail_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /batches/{batch_id} route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/batches/{batch_id}" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_batch_detail_renders_batch_facts_status_counts_and_members(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The drilldown renders the batch's own facts, the member status
    counts, and the member jobs (linked to their detail pages)."""
    batch_id = new_uuid()
    member_a, member_b = new_uuid(), new_uuid()
    conn = _BatchDetailConnection(
        batch_row=_batch_row(id=batch_id),
        counts=[
            StubRecord(status="pending", count=3),
            StubRecord(status="running", count=1),
        ],
        members=[
            _member_row(member_a),
            _member_row(member_b, status="running", attempt=1),
        ],
        archived_members=7,
    )
    client = _batch_detail_app(monkeypatch, conn)

    response = client.get(f"/batches/{batch_id}")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    html = response.text  # pyright: ignore[reportUnknownAttributeType]
    # The batch's own facts.
    assert "etl" in html
    assert "load_data" in html
    # The member status counts.
    assert "pending" in html and "running" in html
    # The member jobs, linked to their detail pages.
    assert f"/jobs/{member_a}" in html
    assert f"/jobs/{member_b}" in html
    # The archived members are accounted for, not silently invisible.
    assert "7" in html


def test_batch_detail_missing_batch_returns_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown batch id is a 404, not an empty page."""
    conn = _BatchDetailConnection(batch_row=None)
    client = _batch_detail_app(monkeypatch, conn)

    response = client.get(f"/batches/{new_uuid()}")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 404  # pyright: ignore[reportUnknownMemberType]


def test_batch_detail_invalid_uuid_returns_422(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """A non-UUID batch id fails route validation (FastAPI default), the
    same contract /jobs/{job_id} serves."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _batch_detail_app(monkeypatch, _BatchDetailConnection())

    response = client.get("/batches/not-a-uuid")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 422  # pyright: ignore[reportUnknownMemberType]


def test_batch_detail_missing_batches_table_renders_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schema without the batches migration renders the install notice,
    the same degrade the list page shows."""
    conn = _BatchDetailConnection(batches_missing=True)
    client = _batch_detail_app(monkeypatch, conn)

    response = client.get(f"/batches/{new_uuid()}")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "batches not installed" in response.text  # pyright: ignore[reportUnknownAttributeType]


def test_batch_detail_renders_truncation_notice_when_member_cap_bites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A batch with more members than the cap renders the cap notice, the
    batches list page's own truncation idiom."""
    from taskq.web.admin.batches import _BATCH_MEMBERS_CAP

    batch_id = new_uuid()
    conn = _BatchDetailConnection(
        batch_row=_batch_row(id=batch_id),
        counts=[StubRecord(status="pending", count=_BATCH_MEMBERS_CAP)],
        members=[_member_row(new_uuid()) for _ in range(_BATCH_MEMBERS_CAP)],
    )
    client = _batch_detail_app(monkeypatch, conn)

    response = client.get(f"/batches/{batch_id}")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Showing the" in response.text  # pyright: ignore[reportUnknownAttributeType]


def test_batches_list_links_batch_ids_to_the_drilldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The list page's Batch cell links to the batch's own drilldown, not
    to a job-detail URL that 404s on a batch id."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    batch_id = new_uuid()
    pool = _BatchRowsPool([_batch_row(id=batch_id)])
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
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
    assert f"/batches/{batch_id}" in html, "the Batch cell must link the batch's own drilldown page"

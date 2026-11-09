"""Tests for the batches overview route and template."""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from asyncpg.exceptions import UndefinedTableError
from fastapi import FastAPI  # Why: importorskip guard must precede.

from taskq._ids import new_base62, new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state

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
#
# The drilldown's row-driven arms run the real router over the module's
# real pool (the tests/web_admin/test_batch_detail_pg.py pattern): the
# batch, its member jobs and its archived members are real rows, so the
# rendered facts, counts, links and cap notice are the drilldown's own SQL
# answering the database.


async def _seed_batch_with_members(
    conn: asyncpg.Connection,
    schema: str,
    *,
    live_members: int = 0,
    member_statuses: tuple[str, ...] | None = None,
    member_ids: tuple[Any, ...] | None = None,
    archived_members: int = 0,
    expected_size: int | None = None,
) -> Any:
    """One active batch with real member rows.

    *member_statuses* names each live member's status explicitly; with
    *live_members* instead, every member is pending. *member_ids* supplies
    the members' ids (paired in order with *member_statuses*).
    *archived_members* inserts that many jobs_archive rows carrying the
    batch id."""
    batch_id = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".batches (
                id, queue, status, expected_size, consecutive_failures,
                failure_threshold, originating_actor, created_at)
            VALUES ($1, 'etl', 'active', $2, 2, NULL, 'load_data', $3)""",
        batch_id,
        expected_size
        if expected_size is not None
        else len(member_ids or ()) + live_members + archived_members,
        now - timedelta(minutes=5),
    )
    statuses = member_statuses if member_statuses is not None else ("pending",) * live_members
    for i, status in enumerate(statuses):
        jid = member_ids[i] if member_ids is not None else new_uuid()
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs (
                    id, actor, queue, payload, payload_schema_ver, max_attempts,
                    retry_kind, status, priority, attempt, metadata,
                    created_at, scheduled_at)
                VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 1, 3,
                    'transient', $2::{schema}.job_status, 0, $3,
                    $4::jsonb, $5, $5)""",
            jid,
            status,
            1 if status == "running" else 0,
            f'{{"batch_id": "{batch_id}"}}',
            now - timedelta(minutes=4),
        )
    for _ in range(archived_members):
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs_archive (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    attempt, metadata, created_at, scheduled_at, started_at,
                    finished_at, archived_at, expire_at)
                VALUES ($1, 'load_data', 'etl', '{{}}'::jsonb, 3, 'transient',
                    'succeeded'::{schema}.job_status, 1, $2::jsonb, $3, $3, $3,
                    $3, $4, $5)""",
            new_uuid(),
            f'{{"batch_id": "{batch_id}"}}',
            now - timedelta(minutes=6),
            now - timedelta(minutes=5),
            now + timedelta(days=365),
        )
    return batch_id


def _batch_detail_app(pool: asyncpg.Pool, schema: str) -> Any:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _get_batch_detail(pool: asyncpg.Pool, schema: str, batch_id: object) -> Any:
    app = _batch_detail_app(pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.get(f"/batches/{batch_id}")


def test_batch_detail_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /batches/{batch_id} route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/batches/{batch_id}" in route_paths  # pyright: ignore[reportUnknownVariableType]


async def test_batch_detail_renders_batch_facts_status_counts_and_members(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The drilldown renders the batch's own facts, the member status
    counts, and the member jobs (linked to their detail pages) - all from
    real rows."""
    schema = module_pg_schema.schema_name
    member_a, member_b = new_uuid(), new_uuid()
    batch_id = await _seed_batch_with_members(
        clean_pg_conn,
        schema,
        member_statuses=("pending", "running"),
        member_ids=(member_a, member_b),
        archived_members=7,
    )

    response = await _get_batch_detail(module_pg_pool, schema, batch_id)
    assert response.status_code == 200
    html = response.text
    # The batch's own facts.
    assert "etl" in html
    assert "load_data" in html
    # The member status counts.
    assert "pending" in html and "running" in html
    # The member jobs, linked to their detail pages.
    assert f"/jobs/{member_a}" in html
    assert f"/jobs/{member_b}" in html
    # The archived members are accounted for, not silently invisible: the
    # notice names their count.
    assert "7 archived members" in html


async def test_batch_detail_missing_batch_returns_404(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """An unknown batch id is a 404, not an empty page."""
    response = await _get_batch_detail(module_pg_pool, module_pg_schema.schema_name, new_uuid())
    assert response.status_code == 404


async def test_batch_detail_invalid_uuid_returns_422(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A non-UUID batch id fails route validation (FastAPI default), the
    same contract /jobs/{job_id} serves."""
    response = await _get_batch_detail(module_pg_pool, module_pg_schema.schema_name, "not-a-uuid")
    assert response.status_code == 422


async def test_batch_detail_missing_batches_table_renders_notice(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A schema without the batches migration renders the install notice,
    the same degrade the list page shows: the router points at a schema no
    migration has applied, so Postgres itself raises UndefinedTableError."""
    unmigrated = f"unmigrated_{new_base62()}".lower()
    response = await _get_batch_detail(module_pg_pool, unmigrated, new_uuid())
    assert response.status_code == 200
    assert "batches not installed" in response.text


async def test_batch_detail_renders_truncation_notice_when_member_cap_bites(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A batch with more members than the cap renders the cap notice, the
    batches list page's own truncation idiom. The member rows are real
    rows - the cap the read enforces is the real LIMIT."""
    from taskq.web.admin.batches import _BATCH_MEMBERS_CAP

    schema = module_pg_schema.schema_name
    batch_id = await _seed_batch_with_members(
        clean_pg_conn, schema, live_members=_BATCH_MEMBERS_CAP
    )
    response = await _get_batch_detail(module_pg_pool, schema, batch_id)
    assert response.status_code == 200
    assert "Showing the" in response.text


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

"""Tests for the shared per-actor executor stats helper and the actors page.

The ``/api/history/stats`` JSON endpoint and the actors page render the
same aggregate read through ``taskq.web.admin._actor_stats``; these tests
pin the SQL shape of both groupings, the JSON wrapper's passthrough
contract, and the merged actors-table rendering (executor columns,
failure share, archive-only actors, truncation notice).
"""

from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router
from taskq.web.admin._actor_stats import STATS_LIMIT, _build_stats_sql, fetch_actor_stats

from . import StubRecord, _StubPool

# ── Configurable stub pool/connection ───────────────────────────────────


class _FetchConn:
    """Connection returning preset ``fetch`` results in call order."""

    def __init__(self, fetch_results: list[list[StubRecord]]) -> None:
        self._results: list[list[StubRecord]] = list(fetch_results)

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if self._results:
            return self._results.pop(0)
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def fetchval(self, query: str, *args: object) -> object:
        if "clock_timestamp()" in query:
            # The router's clock-offset probe: answer like Postgres would.
            return datetime.now(UTC)
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _FetchAcquireCtx:
    def __init__(self, conn: _FetchConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FetchConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _FetchPool:
    """Minimal pool duck type yielding a single configurable connection."""

    def __init__(self, conn: _FetchConn) -> None:
        self._conn = conn

    def acquire(self) -> _FetchAcquireCtx:
        return _FetchAcquireCtx(self._conn)


def _build_app(pool: object, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Build a TestClient with the admin router (auto-discovered)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state

    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _summary_row(actor: str = "send_email") -> StubRecord:
    """One actor_config summary row as list_actor_summaries returns it."""
    return StubRecord(
        actor=actor,
        queue="default",
        max_concurrent=4,
        max_pending=100,
        updated_at="2025-01-01T00:00:00+00:00",
        active_job_count=2,
        enabled_schedule_count=1,
    )


def _stats_row(actor: str = "send_email", **overrides: Any) -> StubRecord:
    """One per-actor stats row as fetch_actor_stats returns it."""
    row: dict[str, Any] = {
        "actor": actor,
        "total": 10,
        "failed": 2,
        "p50_duration_ms": 90,
        "p95_duration_ms": 200,
        "last_activity_at": datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
    }
    row.update(overrides)
    return StubRecord(**row)


# ── SQL shape ────────────────────────────────────────────────────────────


def test_stats_sql_per_queue_groups_by_actor_and_queue() -> None:
    """The per-queue variant keeps the JSON contract's (actor, queue) grouping."""
    sql = _build_stats_sql("taskq", per_queue=True)
    assert 'FROM "taskq".jobs_archive j' in sql
    assert 'LEFT JOIN "taskq".job_attempts_archive a ON a.job_id = j.id' in sql
    assert "GROUP BY j.actor,\n    j.queue" in sql
    assert f"LIMIT {STATS_LIMIT}" in sql


def test_stats_sql_per_actor_groups_by_actor_only() -> None:
    """The actors-page variant drops the queue column and groups by actor alone."""
    sql = _build_stats_sql("taskq", per_queue=False)
    assert "GROUP BY j.actor\n" in sql
    assert "    j.queue,\n" not in sql
    assert "max(j.finished_at) AS last_activity_at" in sql
    assert f"LIMIT {STATS_LIMIT}" in sql


def test_stats_sql_interpolates_no_caller_text() -> None:
    """Only the validated schema identifier is interpolated; no parameters needed."""
    for per_queue in (True, False):
        sql = _build_stats_sql("taskq", per_queue=per_queue)
        assert "$" not in sql


# ── GET /api/history/stats: thin wrapper over the helper ────────────────


def test_history_stats_passthrough_includes_last_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The JSON endpoint returns the helper's rows unchanged under "actors"."""
    rows = [_stats_row()]
    conn = _FetchConn(fetch_results=[rows])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/api/history/stats")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    actors = response.json()["actors"]  # pyright: ignore[reportUnknownVariableType]
    assert len(actors) == 1
    assert actors[0]["actor"] == "send_email"
    assert actors[0]["total"] == 10
    assert actors[0]["failed"] == 2
    assert actors[0]["p50_duration_ms"] == 90
    assert actors[0]["p95_duration_ms"] == 200
    assert actors[0]["last_activity_at"] is not None


def test_history_stats_empty_pool_returns_empty_actors(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """With no archive rows the endpoint returns {"actors": []}."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _build_app(stub_pool, monkeypatch)
    response = client.get("/api/history/stats")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    assert response.json() == {"actors": []}  # pyright: ignore[reportUnknownVariableType]


# ── fetch_actor_stats helper ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_actor_stats_returns_dicts() -> None:
    """fetch_actor_stats maps rows to dicts for the callers."""
    conn = _FetchConn(fetch_results=[[_stats_row(actor="a"), _stats_row(actor="b")]])
    rows = await fetch_actor_stats(conn, schema="taskq")  # pyright: ignore[reportArgumentType]  # Why: test duck-type connection.
    assert [r["actor"] for r in rows] == ["a", "b"]
    assert isinstance(rows[0], dict)


# ── GET /actors: route discovery and empty state ─────────────────────────


def test_actors_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /actors is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/actors" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_actors_page_returns_html(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /actors returns 200 with text/html content type."""
    client = _build_app(_FetchPool(_FetchConn(fetch_results=[[], []])), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]


def test_actors_page_empty_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no config rows and no archive stats the page renders its empty state."""
    client = _build_app(_FetchPool(_FetchConn(fetch_results=[[], []])), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    assert "No actor_config rows." in response.text  # pyright: ignore[reportUnknownVariableType]


# ── GET /actors: merged executor stats rendering ─────────────────────────


def test_actors_page_renders_executor_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    """The actors table renders jobs total, failure share, percentiles, and activity."""
    conn = _FetchConn(fetch_results=[[_summary_row()], [_stats_row()]])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    html = response.text  # pyright: ignore[reportUnknownVariableType]
    assert "Jobs (archive)" in html
    assert "p50 / p95 (ms)" in html
    assert "10" in html  # jobs total
    assert "2 (20.0%)" in html  # failure count and share
    assert "90 / 200" in html  # p50 / p95


def test_actors_page_config_actor_without_stats_renders_dashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A config actor with no archive history renders placeholder dashes, not zeros."""
    conn = _FetchConn(fetch_results=[[_summary_row(actor="cold_actor")], []])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    html = response.text  # pyright: ignore[reportUnknownVariableType]
    assert "cold_actor" in html
    assert "archive only" not in html


def test_actors_page_stats_only_actor_has_no_deregister_form(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An archive-only actor (no config row) renders without a deregister form."""
    conn = _FetchConn(fetch_results=[[], [_stats_row(actor="ghost_actor", total=5, failed=0)]])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    html = response.text  # pyright: ignore[reportUnknownVariableType]
    assert "ghost_actor" in html
    assert "/actors/ghost_actor/deregister" not in html
    assert "archive only" in html
    assert "5" in html


def test_actors_page_shows_truncation_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stats read at the cap renders the truncation notice."""
    rows = [_stats_row(actor=f"actor_{i}") for i in range(STATS_LIMIT)]
    conn = _FetchConn(fetch_results=[[], rows])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    assert "most active actors" in response.text  # pyright: ignore[reportUnknownVariableType]


def test_actors_page_no_truncation_notice_under_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Under the cap no truncation notice renders."""
    conn = _FetchConn(fetch_results=[[_summary_row()], [_stats_row()]])
    client = _build_app(_FetchPool(conn), monkeypatch)
    response = client.get("/actors")  # pyright: ignore[reportUnknownMemberType]
    assert "most active actors" not in response.text  # pyright: ignore[reportUnknownVariableType]


# ── _merge_actor_stats unit behavior ─────────────────────────────────────


def test_merge_actor_stats_appends_stats_only_actors_hot_first() -> None:
    """Config rows keep name order; archive-only actors append hottest first."""
    from taskq.web.admin.actors import _merge_actor_stats

    config_rows: list[dict[str, object]] = [dict(_summary_row(actor="aaa"))]
    stats_rows: list[dict[str, object]] = [
        dict(_stats_row(actor="zzz", total=50)),
        dict(_stats_row(actor="aaa", total=10)),
    ]
    merged = _merge_actor_stats(config_rows, stats_rows)
    assert [r["actor"] for r in merged] == ["aaa", "zzz"]
    assert merged[0]["has_config"] is True
    assert merged[1]["has_config"] is False
    assert merged[1]["jobs_total"] == 50


def test_merge_actor_stats_failure_share() -> None:
    """The failure share is the failed fraction of total, as a percent string."""
    from taskq.web.admin.actors import _failure_share, _merge_actor_stats

    merged = _merge_actor_stats([], [dict(_stats_row(total=340, failed=17))])
    assert merged[0]["failure_share"] == "5.0%"
    assert _failure_share(0, 0) is None
    assert _failure_share(None, None) is None
    assert _failure_share(10, 0) == "0.0%"

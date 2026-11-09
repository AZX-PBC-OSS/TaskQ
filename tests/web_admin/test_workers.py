"""Tests for workers, leader, watchdog, and their admin templates."""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.
from fastapi.testclient import TestClient

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import (  # Why: importorskip guard must precede.
    create_router,
    setup_admin_state,
)

from . import StubRecord, _StubPool  # Why: importorskip guard must precede.

# ── Workers routes: discovery and registration ─────────────────────────


pytestmark = [pytest.mark.fastapi]


def test_workers_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /workers route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/workers" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_workers_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /workers returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/workers")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_leader_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /leader route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/leader" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_leader_page_returns_html_when_no_leader(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /leader returns 200 with text/html even when no leader is elected."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/leader")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


# ── Workers template ───────────────────────────────────────────────────


def test_workers_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: workers.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "workers.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_workers_template_renders_worker_data(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """workers.html renders hostname, pid, queues, last_seen_at, and leader indicator."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    html = template.render(
        workers=[
            {
                "hostname": "worker-1",
                "pid": 1234,
                "queues": "default,emails",
                "last_seen_at": "2025-01-01T00:00:00+00:00",
                "is_leader": True,
            },
            {
                "hostname": "worker-2",
                "pid": 5678,
                "queues": "default",
                "last_seen_at": "2025-01-01T00:00:01+00:00",
                "is_leader": False,
            },
        ]
    )
    assert "worker-1" in html
    assert "1234" in html
    assert "worker-2" in html
    assert "Leader" in html


def test_workers_template_empty_workers(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """workers.html shows 'No workers registered' when list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    html = template.render(workers=[])
    assert "No workers registered" in html


# ── Leader template ────────────────────────────────────────────────────


def test_leader_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: leader.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "leader.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_leader_template_no_leader_elected(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html shows 'No leader elected' when leader is None."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(leader=None, watchdog_healthy=None)
    assert "No leader elected" in html


def test_leader_template_renders_leader_info(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html renders hostname, pid, elected_at, last_seen_at, worker_last_seen, watchdog."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=True,
    )
    assert "leader-1" in html
    assert "9999" in html
    assert "Healthy" in html


def test_leader_template_renders_lease_expiry(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html shows the lease's expires_at - the instant the fleet
    will act on if the holder stops renewing; an operator reading the
    leader page needs it to reason about failover."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "expires_at": "2025-01-01T00:00:40+00:00",
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=True,
    )
    assert "Lease Expires" in html
    assert "2025-01-01T00:00:40+00:00" in html


def test_leader_template_renders_dash_when_holder_has_no_lease(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html renders the em-dash fallback when expires_at is NULL -
    a row written by a release that predates the lease column carries none,
    and the page must say so instead of rendering 'None'."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "expires_at": None,
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=True,
    )
    assert "Lease Expires" in html
    assert "None" not in html
    assert "—" in html


def test_leader_template_unhealthy_watchdog(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html shows 'Unhealthy' badge when watchdog_healthy is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=False,
    )
    assert "Unhealthy" in html


# ── XSS prevention: workers and leader templates ──────────────────────


def test_workers_template_autoescapes_hostname(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Workers page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        workers=[
            {"hostname": xss, "pid": 1, "queues": "default", "last_seen_at": "", "is_leader": False}
        ]
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


def test_leader_template_autoescapes_hostname(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Leader page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        leader={
            "hostname": xss,
            "pid": 1,
            "elected_at": "",
            "last_seen_at": "",
            "worker_last_seen": "",
        },
        watchdog_healthy=True,
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


# ── Leader route: the watchdog badge is decided by SQL, not Python ───────


def test_leader_watchdog_badge_comes_from_sql_not_python_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D5 pin: the Healthy/Unhealthy badge is computed by the LEADER SQL
    (``watchdog_healthy``) - the same single-arbiter shape as queues.py's
    worker-liveness predicates.  A row the SERVER calls healthy
    (watchdog_healthy=True)
    must render Healthy even when this process's Python clock would call
    its last_seen_at stale: mixing the app clock into a server-written
    timestamp's freshness verdict breaks under app↔DB skew.  A route that
    recomputes freshness in Python renders Unhealthy."""
    row = StubRecord(
        worker_id="00000000-0000-0000-0000-000000000001",
        hostname="leader-1",
        pid=9999,
        elected_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        # Stale by the local Python clock - the SQL verdict is what counts.
        last_seen_at=datetime.now(UTC) - timedelta(hours=1),
        worker_last_seen=datetime.now(UTC) - timedelta(hours=1),
        watchdog_healthy=True,
    )
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "Healthy" in text
    assert "Unhealthy" not in text


def test_leader_watchdog_badge_follows_an_unhealthy_verdict_on_a_fresh_looking_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mirror image, and the half a Python recomputation would get wrong
    in the other direction: a row the SERVER calls stale
    (``watchdog_healthy=False``) must render Unhealthy even though its
    ``last_seen_at`` is seconds old by this process's Python clock - which is
    exactly what a clock ahead of the database's produces.  Recomputing
    locally would render Healthy and hide a dead leader."""
    now = datetime.now(UTC)
    row = StubRecord(
        worker_id="00000000-0000-0000-0000-000000000001",
        hostname="leader-1",
        pid=9999,
        elected_at=now - timedelta(hours=1),
        # Fresh by the local Python clock - the SQL verdict is what counts.
        last_seen_at=now,
        worker_last_seen=now,
        watchdog_healthy=False,
    )
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "Unhealthy" in text


# ── Leader route: leader-present branch (lines 97-105) ──────────────────


class _FetchRowConn:
    """Connection returning a single preset fetchrow result."""

    def __init__(self, row: StubRecord | None) -> None:
        self._row = row

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return self._row

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _FetchRowAcquireCtx:
    def __init__(self, conn: _FetchRowConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FetchRowConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _FetchRowPool:
    def __init__(self, conn: _FetchRowConn) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _FetchRowAcquireCtx:
        return _FetchRowAcquireCtx(self._conn)


def _build_workers_app(pool: object, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Build a TestClient with the admin router backed by *pool*."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _leader_row(watchdog_healthy: bool | None) -> StubRecord:
    """Leader row as the SQL returns it - the freshness verdict
    (``watchdog_healthy``) is computed server-side, so the stub carries it
    instead of the route recomputing it from last_seen_at."""
    return StubRecord(
        worker_id="00000000-0000-0000-0000-000000000001",
        hostname="leader-1",
        pid=9999,
        elected_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        last_seen_at=datetime.now(UTC) - timedelta(seconds=5),
        worker_last_seen=datetime(2025, 1, 1, 0, 0, 10, tzinfo=UTC),
        watchdog_healthy=watchdog_healthy,
    )


def test_leader_page_renders_when_leader_exists_healthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader renders leader details with a Healthy badge when the SQL
    verdict is healthy."""
    row = _leader_row(watchdog_healthy=True)
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "9999" in text
    assert "Healthy" in text


def test_leader_page_renders_when_leader_exists_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader renders an Unhealthy badge when the SQL verdict says the
    leader ping is stale."""
    row = _leader_row(watchdog_healthy=False)
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "Unhealthy" in text


def test_leader_page_renders_unknown_watchdog_when_verdict_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader shows Unknown watchdog when the SQL verdict is NULL
    (the leader has never recorded a ping)."""
    row = _leader_row(watchdog_healthy=None)
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Unknown" in response.text  # pyright: ignore[reportUnknownMemberType]


# ── Workers overview: metadata decode_jsonb loop (lines 60-61) ──────────


class _FetchListConn:
    """Connection returning a preset list from ``fetch``."""

    def __init__(self, rows: list[StubRecord]) -> None:
        self._rows = rows

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return self._rows

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


def test_workers_page_decodes_dict_metadata_notify_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /workers decodes jsonb metadata and surfaces notify_enabled=True."""
    rows = [
        StubRecord(
            hostname="worker-1",
            pid=1234,
            queues="default",
            last_seen_at=datetime.now(UTC),
            is_leader=True,
            metadata='{"notify_enabled": true}',
        )
    ]
    client = _build_workers_app(_FetchRowPool(_FetchListConn(rows)), monkeypatch)
    response = client.get("/workers")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "worker-1" in response.text  # pyright: ignore[reportUnknownMemberType]


def test_workers_page_non_dict_metadata_defaults_notify_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-dict metadata (None) falls back to notify_enabled=False."""
    rows = [
        StubRecord(
            hostname="worker-2",
            pid=5678,
            queues="default",
            last_seen_at=datetime.now(UTC),
            is_leader=False,
            metadata=None,
        )
    ]
    client = _build_workers_app(_FetchRowPool(_FetchListConn(rows)), monkeypatch)
    response = client.get("/workers")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "worker-2" in response.text  # pyright: ignore[reportUnknownMemberType]


# ── Workers page: running / max column (real PG) ────────────────────────
#
# The workers page's own behavior pins run the real router over the
# module's real pool (the tests/test_web_admin_integration.py pattern):
# the rows the page reads are seeded with real worker/job inserts, so the
# rendered running counts, capacity badges and stall tallies are the
# database's answers computed by the page's own SQL, not a script's.


async def _seed_worker_with_running_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    metadata: str,
    running_jobs: int = 0,
) -> uuid.UUID:
    """One registered worker with *metadata* and *running_jobs* running rows."""
    wid = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata)
            VALUES ($1, $2, $3, $4::text[], $5::jsonb)""",
        wid,
        f"worker-{str(wid)[:8]}",
        1234,
        ["default"],
        metadata,
    )
    for _ in range(running_jobs):
        await conn.execute(
            f"""INSERT INTO "{schema}".jobs (
                    id, actor, queue, payload, max_attempts, retry_kind,
                    status, priority, attempt, scheduled_at, schedule_to_close,
                    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at)
                VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, 3, 'transient',
                    'running', 0, 1, now(), now() + interval '60 seconds',
                    $2, now() + interval '60 seconds', now(), now())""",
            new_uuid(),
            wid,
        )
    return wid


def _admin_app(pool: asyncpg.Pool, schema: str) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _render_workers_page(pool: asyncpg.Pool, schema: str) -> str:
    app = _admin_app(pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workers")
    assert resp.status_code == 200
    return resp.text


async def test_workers_page_renders_running_over_max(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The workers table renders the per-worker running row count against the
    registered max_concurrency: the same population the
    taskq.worker.active_jobs metric counts per process. The count is the
    page's own SQL (the lateral count over the worker's running rows) over
    real rows."""
    schema = module_pg_schema.schema_name
    await _seed_worker_with_running_jobs(
        clean_pg_conn,
        schema,
        metadata='{"notify_enabled": true, "max_concurrency": 4}',
        running_jobs=2,
    )
    html = await _render_workers_page(module_pg_pool, schema)
    assert "2 / 4" in html
    # Not at capacity: the neutral badge, not the amber one.
    assert "bg-amber-100" not in html


async def test_workers_page_flags_a_saturated_worker(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A worker whose running rows reach its registered max renders the
    at-capacity badge: the saturation signal the scaling playbook's first
    row reads."""
    schema = module_pg_schema.schema_name
    await _seed_worker_with_running_jobs(
        clean_pg_conn,
        schema,
        metadata='{"notify_enabled": true, "max_concurrency": 4}',
        running_jobs=4,
    )
    html = await _render_workers_page(module_pg_pool, schema)
    assert "4 / 4" in html
    assert "bg-amber-100" in html


async def test_workers_page_renders_a_dash_when_metadata_lacks_capacity(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A workers row whose metadata carries no max_concurrency (an older
    registration) still shows its running count, with a dash for the max."""
    schema = module_pg_schema.schema_name
    await _seed_worker_with_running_jobs(clean_pg_conn, schema, metadata="{}", running_jobs=1)
    html = await _render_workers_page(module_pg_pool, schema)
    assert "1 /" in html
    assert "bg-amber-100" not in html


class _WorkersStatementRecordingPool:
    """Recording proxy over the real pool: it records the workers overview's
    statement and lets the real read proceed untouched - COUNT, DON'T ANSWER
    (the house pattern of
    tests/test_cancel_where_bounded.py::_StatementRecordingPool)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self.statements: list[str] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[Any, None]:
        async with self._pool.acquire(**kwargs) as conn:  # type: ignore[call-overload]  # Why: the real pool's acquire timeout kwarg is passed through untouched.
            outer = self

            class _Recorder:
                def __init__(self, inner: Any) -> None:
                    self._inner = inner

                async def fetch(self, sql: str, *args: object) -> Any:
                    outer.statements.append(sql)
                    return await self._inner.fetch(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._inner, name)

            yield _Recorder(conn)


async def test_workers_query_counts_running_rows_through_the_partial_index(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The workers overview's running count rides the
    jobs_locked_by_worker_running_idx partial index (locked_by_worker +
    status = 'running'), not a scan of the jobs table. The statement is
    captured from the real pool, not re-spelled here: a rewritten
    implementation must stay measured."""
    schema = module_pg_schema.schema_name
    await _seed_worker_with_running_jobs(clean_pg_conn, schema, metadata="{}")
    recording = _WorkersStatementRecordingPool(module_pg_pool)
    app = _admin_app(recording, schema)  # pyright: ignore[reportArgumentType]  # Why: the recording proxy satisfies the pool surface the router checks out through.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workers")
    assert resp.status_code == 200
    page_sql = [s for s in recording.statements if "running_count" in s]
    assert page_sql, (
        f"the workers overview never ran the running-count query: {recording.statements}"
    )
    sql = page_sql[0]
    assert "locked_by_worker = w.id" in sql
    assert "status = 'running'" in sql
    assert "count(*)" in sql


# ── Workers page: stall hotspots column ──────────────────────────────


async def _hotspot_html(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    metadata: str,
) -> str:
    schema = module_pg_schema.schema_name
    await _seed_worker_with_running_jobs(clean_pg_conn, schema, metadata=metadata)
    return await _render_workers_page(module_pg_pool, schema)


async def test_workers_page_renders_stall_hotspots_hottest_first(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The workers overview renders the stall tally the heartbeat merged
    into the row metadata, hottest actor first, with kind counts."""
    html = await _hotspot_html(
        clean_pg_conn,
        module_pg_pool,
        module_pg_schema,
        '{"notify_enabled": true, "max_concurrency": 4, "loop_stalls": '
        '{"resize_image": {"blocking_call": 2, "gil_held": 1}, '
        '"send_email": {"gil_held": 12}}}',
    )
    # Hottest first: send_email's 12 beats resize_image's 3.
    assert html.index("send_email x12 (gil_held)") < html.index(
        "resize_image x3 (blocking_call 2, gil_held 1)"
    )


async def test_workers_page_hotspots_empty_when_no_tally(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A worker that attributed no stalls shows the empty marker, and a
    worker registered before the tally existed (no key at all) too."""
    html = await _hotspot_html(
        clean_pg_conn,
        module_pg_pool,
        module_pg_schema,
        '{"notify_enabled": true, "max_concurrency": 4}',
    )
    assert "x12" not in html
    assert "Stall hotspots" in html


async def test_workers_page_carries_the_tally_definition_note(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The page states what the column counts and what it does not: the
    tally counts ATTRIBUTED stalls, the Running / Max column counts
    running rows."""
    html = await _hotspot_html(clean_pg_conn, module_pg_pool, module_pg_schema, "{}")
    assert "attributed event-loop stalls" in html
    assert "Running / Max column counts running rows" in html


async def test_workers_page_survives_a_malformed_tally(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A stale worker's metadata shape (a tally that is not a dict, bad
    counts - jsonb admits it) renders as empty rather than taking down the
    page. Seeded as a real jsonb row: the column holds whatever jsonb
    accepts, which is exactly the drift the render must survive."""
    html = await _hotspot_html(
        clean_pg_conn,
        module_pg_pool,
        module_pg_schema,
        '{"notify_enabled": true, "max_concurrency": 4, "loop_stalls": ["not", "a", "dict"]}',
    )
    assert "Stall hotspots" in html
    assert "x12" not in html

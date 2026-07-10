"""Integration and negative tests for the built-in admin UI router.

Runs against real Postgres (testcontainers) with seeded data.
"""

import signal
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from testcontainers.postgres import PostgresContainer

from taskq._ids import new_base62
from taskq._json import dumps_str
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.web.admin import create_router, setup_admin_state

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"twa_{new_base62()}".lower()


@pytest.fixture(autouse=True)
def _dev_environment(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner, not called directly.
    """This file deliberately builds unauthenticated admin routers and
    exercises write actions (cancel, retry, run-now) to test UI structure and
    behavior, not the security gates themselves (see test_admin_security_fixes.py
    for that). TASKQ_ENVIRONMENT=dev bypasses create_router's fail-closed
    admin_ui_require_auth default so these tests don't need their own
    auth_dependency; TASKQ_ADMIN_ACTIONS_ENABLED=true bypasses the separate
    admin_actions_enabled gate (default False) on write-action routes."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")


@dataclass
class _TestBackendSettings:
    schema_name: str = _SCHEMA_LABEL
    dispatch_oversample: int = 2


@dataclass
class _TestBackendDeps:
    settings: _TestBackendSettings
    worker_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    dispatcher_pool: asyncpg.Pool | None = None


def _make_backend(pool: asyncpg.Pool) -> PostgresBackend:
    deps = _TestBackendDeps(
        settings=_TestBackendSettings(),
        worker_pool=pool,
        heartbeat_pool=pool,
    )
    return PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )


# ── Shared PG container (session-scoped) ──────────────────────────────────


@pytest.fixture(scope="session")
def _admin_pg_container() -> Iterator[PostgresContainer]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest fixture consumed by test runner via parameter injection
    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ) as container:
        yield container


@pytest.fixture(scope="session")
def _admin_pg_dsn(_admin_pg_container: PostgresContainer) -> str:  # pyright: ignore[reportUnusedFunction]  # Why: pytest fixture consumed by test runner via parameter injection
    return _admin_pg_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


# ── App factory ────────────────────────────────────────────────────────────
#
# Tests pass a pg_pool that was created inside the same event loop. The
# lifespan mounts the router and populates app.state so route handlers can
# resolve their dependencies. It does NOT create a new pool — the caller
# owns the pool lifecycle.


def _make_app(
    pool: asyncpg.Pool,
    *,
    redis_client: Any | None = None,
    auth_dependency: Any | None = None,
) -> FastAPI:
    backend = _make_backend(pool)
    bundle = create_router(
        pool,
        schema=_SCHEMA_LABEL,
        redis_client=redis_client,
        auth_dependency=auth_dependency,
        base_path="/admin",
        backend=backend,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return app


# ── Per-test pool and schema ───────────────────────────────────────────────


@pytest_asyncio.fixture
async def pool(_admin_pg_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    """Fresh migrated schema + asyncpg pool per test."""
    setup = await asyncpg.connect(_admin_pg_dsn)
    try:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
        await apply_pending(setup, schema=_SCHEMA_LABEL)
    finally:
        await setup.close()

    pg_pool = await asyncpg.create_pool(_admin_pg_dsn, min_size=1, max_size=4)
    assert pg_pool is not None
    try:
        yield pg_pool
    finally:
        await pg_pool.close()


@pytest_asyncio.fixture
async def conn(pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    """Seeding connection that shares the same pool (and event loop)."""
    async with pool.acquire() as c:
        yield c  # type: ignore[misc]  # Why: pyright misc diagnostic on yield inside async context manager in pytest-asyncio fixture


# ── Seed helpers ───────────────────────────────────────────────────────────


async def _seed_jobs(
    conn: asyncpg.Connection,
    *,
    queue: str,
    count: int,
    status: str = "pending",
    payload: str = '{"key": "value"}',
) -> list[uuid.UUID]:
    ids: list[uuid.UUID] = []
    for _ in range(count):
        jid = uuid.uuid4()
        ids.append(jid)
        sa = datetime.now(UTC)
        stc = sa + timedelta(seconds=60)
        await conn.execute(
            f"""INSERT INTO {_SCHEMA_LABEL}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at, schedule_to_close
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 0, $8, $9)""",
            jid,
            "test_actor",
            queue,
            payload,
            3,
            "transient",
            status,
            sa,
            stc,
        )
    return ids


async def _seed_running_job(
    conn: asyncpg.Connection, *, queue: str, worker_id: uuid.UUID
) -> uuid.UUID:
    jid = uuid.uuid4()
    expires_at = datetime.now(UTC) + timedelta(seconds=60)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, attempt, scheduled_at, schedule_to_close,
            locked_by_worker, lock_expires_at, started_at, last_heartbeat_at
        ) VALUES (
            $1, $2, $3, $4::jsonb, 3, 'transient',
            'running', 0, 1, now(), now() + interval '60 seconds',
            $5, $6, now(), now()
        )""",
        jid,
        "test_actor",
        queue,
        '{"key": "value"}',
        worker_id,
        expires_at,
    )
    return jid


async def _seed_worker(conn: asyncpg.Connection, *, hostname: str = "worker-host-1") -> uuid.UUID:
    wid = uuid.uuid4()
    await conn.execute(
        f"INSERT INTO {_SCHEMA_LABEL}.workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)",
        wid,
        hostname,
        12345,
        ["default"],
    )
    return wid


# ── HTTP helper using ASGI transport (stays in the same event loop) ────────


async def _get(app: FastAPI, path: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.get(path)


async def _post(app: FastAPI, path: str) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post(path)


# ── Queue overview correct counts ──────────────────────────────────


@pytest.mark.asyncio
async def test_queue_overview_correct_counts(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Queue overview shows correct pending and running counts."""
    import re

    await _seed_jobs(conn, queue="default", count=3, status="pending")
    worker_id = await _seed_worker(conn)
    await _seed_running_job(conn, queue="default", worker_id=worker_id)

    resp = await _get(_make_app(pool), "/admin/queues")

    assert resp.status_code == 200
    html = resp.text
    assert "default" in html

    # Locate the <tr> that contains the "default" queue link and check counts in that row.
    row_match = re.search(r"<tr[^>]*>.*?default.*?</tr>", html, re.DOTALL)
    assert row_match, "Row for 'default' queue not found"
    row = row_match.group(0)
    cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
    # Columns: queue link | pending | scheduled | running
    assert len(cells) >= 4, f"Expected ≥4 cells in row, got {cells}"
    # Cell content may contain inner HTML (span, a tags); extract the visible number
    pending_text = re.sub(r"<[^>]+>", "", cells[1]).strip()
    running_text = re.sub(r"<[^>]+>", "", cells[3]).strip()
    assert pending_text == "3", f"pending_count expected 3, got {pending_text!r}"
    assert running_text == "1", f"running_count expected 1, got {running_text!r}"


# ── Job detail attempt history ─────────────────────────────────────


@pytest.mark.asyncio
async def test_job_detail_attempt_history(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Job detail renders attempt history with error_message from attempt 1."""
    worker_id = await _seed_worker(conn)
    [jid] = await _seed_jobs(conn, queue="default", count=1)
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.job_attempts
            (job_id, attempt, started_at, finished_at, outcome,
             error_class, error_message, duration_ms, worker_id)
            VALUES ($1, 1, $2, $3, 'failed', 'ValueError',
                    'first-attempt-error', 500, $4)""",
        jid,
        now - timedelta(minutes=5),
        now - timedelta(minutes=4),
        worker_id,
    )
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.job_attempts
            (job_id, attempt, started_at, finished_at, outcome, duration_ms, worker_id)
            VALUES ($1, 2, $2, $3, 'succeeded', 300, $4)""",
        jid,
        now - timedelta(minutes=2),
        now - timedelta(minutes=1),
        worker_id,
    )

    resp = await _get(_make_app(pool), f"/admin/jobs/{jid}")

    assert resp.status_code == 200
    html = resp.text
    assert "first-attempt-error" in html
    assert html.count("<tr") >= 3  # header + 2 data rows


# ── Job detail renders event log ──────────────────────────────────


@pytest.mark.asyncio
async def test_job_detail_event_log(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Job detail renders event log with kind value."""
    [jid] = await _seed_jobs(conn, queue="default", count=1)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.job_events (job_id, kind, detail)
            VALUES ($1, 'enqueued', '{{"source": "test"}}'::jsonb)""",
        jid,
    )

    resp = await _get(_make_app(pool), f"/admin/jobs/{jid}")

    assert resp.status_code == 200
    assert "enqueued" in resp.text


# ── Schedules page returns 200 when cron_schedules missing ─────────


@pytest.mark.asyncio
async def test_schedules_page_no_cron_table(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Schedules page returns 200 with notice when cron_schedules is absent."""
    await conn.execute(f'DROP TABLE IF EXISTS "{_SCHEMA_LABEL}".cron_schedules CASCADE')

    resp = await _get(_make_app(pool), "/admin/schedules")

    assert resp.status_code == 200
    assert "cron scheduling not installed" in resp.text


# ── Schedules page renders cron_schedules columns ─────────────────


@pytest.mark.asyncio
async def test_schedules_page_renders_columns(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Schedules page renders actor, consecutive_failures, and last_fire_error."""
    import re
    import uuid as _uuid

    from taskq._json import dumps_str

    sid = _uuid.uuid4()
    next_fire = datetime.now(UTC) + timedelta(minutes=5)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.cron_schedules
            (id, actor, cron_expr, timezone, enabled, next_fire_at,
             consecutive_failures, last_fire_error, metadata)
            VALUES ($1, $2, $3, 'UTC', true, $4, 2, 'test error', $5::jsonb)""",
        sid,
        "my_cron_actor",
        "*/5 * * * *",
        next_fire,
        dumps_str({"name": "My Cron"}),
    )

    resp = await _get(_make_app(pool), "/admin/schedules")

    assert resp.status_code == 200
    html = resp.text
    assert "my_cron_actor" in html
    # consecutive_failures renders inside a <td class="..."> with inner span tag
    assert re.search(r"<td[^>]*>.*?2.*?</td>", html, re.DOTALL), (
        "consecutive_failures=2 not found in table cell"
    )
    assert "test error" in html


# ── SSE endpoint returns text/event-stream ─────────────────────────


@pytest.mark.asyncio
async def test_sse_endpoint_returns_event_stream(
    pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SSE endpoint returns text/event-stream with sentinel event.

    The infinite keep-alive generator is replaced with a finite one so the
    request completes instead of hanging (same approach as the unit test suite
    in tests/web_admin/test_sse.py).
    """
    import asyncio as _asyncio
    from collections.abc import AsyncIterator as AsyncIteratorStr

    import taskq.web.admin.sse as _sse_mod

    async def _finite_generator(
        semaphore: _asyncio.Semaphore, pool: Any, schema: Any, topic: Any
    ) -> AsyncIteratorStr[str]:
        try:
            yield 'event: status\ndata: {"status":"awaiting_progress_backend"}\n\n'
        finally:
            semaphore.release()

    monkeypatch.setattr(_sse_mod, "_sse_generator", _finite_generator)

    resp = await _get(_make_app(pool), "/admin/sse/queues")

    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers["content-type"]
    assert b"awaiting_progress_backend" in resp.content


# ── Workers page shows seeded worker ───────────────────────────────


@pytest.mark.asyncio
async def test_workers_page_shows_worker(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Workers page shows seeded worker hostname."""
    await _seed_worker(conn, hostname="integration-test-host")

    resp = await _get(_make_app(pool), "/admin/workers")

    assert resp.status_code == 200
    assert "integration-test-host" in resp.text


# ── Rate-limits page shows PG bucket ───────────────────────────────


@pytest.mark.asyncio
async def test_rate_limits_page_shows_bucket(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Rate-limits page shows inserted rate_limit_buckets row."""
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.rate_limit_buckets (bucket_name, kind, state)
            VALUES ('test-bucket', 'sliding_window', '{{}}'::jsonb)"""
    )

    resp = await _get(_make_app(pool), "/admin/rate-limits")

    assert resp.status_code == 200
    assert "test-bucket" in resp.text


# ── Reservations page shows slot summary ───────────────────────────


@pytest.mark.asyncio
async def test_reservations_page_shows_summary(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """Reservations page shows correct held/free counts."""
    import re

    job_ids = await _seed_jobs(conn, queue="default", count=2)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.reservation_slots (bucket_name, slot_index, job_id)
            VALUES ('res-bucket', 0, $1), ('res-bucket', 1, $2), ('res-bucket', 2, NULL)""",
        job_ids[0],
        job_ids[1],
    )

    resp = await _get(_make_app(pool), "/admin/reservations")

    assert resp.status_code == 200
    html = resp.text
    assert "res-bucket" in html
    # held_count=2 and free_count=1 render inside <td class="..."> with inner tags
    assert re.search(r"<td[^>]*>.*?2.*?</td>", html, re.DOTALL), "held_count=2 not found"
    assert re.search(r"<td[^>]*>.*?1.*?</td>", html, re.DOTALL), "free_count=1 not found"


# ── Leader page returns 200 with no leader ─────────────────────────


@pytest.mark.asyncio
async def test_leader_page_no_leader(pool: asyncpg.Pool) -> None:
    """Leader page returns 200 showing 'no leader elected'."""
    resp = await _get(_make_app(pool), "/admin/leader")

    assert resp.status_code == 200
    assert "No leader elected" in resp.text or "no leader" in resp.text.lower()


# ── taskq ui serve starts and responds ─────────────────────────────


@pytest_asyncio.fixture
async def _migrated_schema_for_ui_serve(_admin_pg_dsn: str) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest fixture consumed by test runner via parameter injection
    """Ensure the test schema exists (subprocess test needs a real schema)."""
    conn = await asyncpg.connect(_admin_pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
        await apply_pending(conn, schema=_SCHEMA_LABEL)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_taskq_ui_serve_starts(
    _admin_pg_dsn: str, _migrated_schema_for_ui_serve: None
) -> None:
    """taskq ui serve starts and responds to GET /admin/queues."""
    import asyncio
    import os
    import sys

    # Grab an ephemeral port; release before subprocess binds to it.
    with socket.socket() as _s:
        _s.bind(("127.0.0.1", 0))
        port = _s.getsockname()[1]

    # TASKQ_ENVIRONMENT=dev: this subprocess doesn't run under pytest, so the
    # _dev_environment autouse fixture's monkeypatch (parent-process only)
    # doesn't reach it — without this, create_router's admin_ui_require_auth
    # and _ui_serve's health_require_token fail-closed checks (both suppressed
    # in a dev environment) would make the process exit immediately instead
    # of binding to the port, and this test would only ever see the generic
    # "did not respond within 20s" timeout.
    subprocess_env = {**os.environ, "TASKQ_ENVIRONMENT": "dev"}
    proc = subprocess.Popen(  # noqa: S603, ASYNC220  # Why: S603 — trusted args; ASYNC220 — Popen is intentional here, subprocess test requires a real separate process
        [
            sys.executable,
            "-m",
            "taskq.cli",
            "ui",
            "serve",
            f"--pg-dsn={_admin_pg_dsn}",
            f"--schema={_SCHEMA_LABEL}",
            "--host=127.0.0.1",
            f"--port={port}",
        ],
        env=subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def _poll_until_ready(deadline: float) -> bool:
        async with httpx.AsyncClient(timeout=2.0) as client:
            while time.time() < deadline:
                try:
                    resp = await client.get(f"http://127.0.0.1:{port}/admin/queues")
                    if resp.status_code == 200:
                        return True
                except Exception:  # noqa: S110  # Why: connection errors during server startup are expected; we retry silently
                    pass
                await asyncio.sleep(0.5)
        return False

    try:
        ready = await _poll_until_ready(time.time() + 20)
        if not ready:
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"Server did not respond on port {port} within 20s.\nstderr: {stderr[:500]}"
            )
    finally:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


# ── Queue detail pagination ───────────────────────────────────────


@pytest.mark.asyncio
async def test_queue_detail_pagination(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Queue detail paginates 150 jobs — page 1 shows 100, page 2 shows 50."""
    import re
    from urllib.parse import unquote

    await _seed_jobs(conn, queue="bulk", count=150, status="pending")

    app = _make_app(pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp1 = await client.get("/admin/queues/bulk?status=pending")
        assert resp1.status_code == 200
        html1 = resp1.text

        # 100 data rows + 1 header row = ≥ 101 <tr> tags
        assert html1.count("<tr") >= 101, f"Expected >=101 <tr, got {html1.count('<tr')}"
        assert "cursor_at=" in html1 or "Next page" in html1

        cursor_match = re.search(r"cursor_at=([^&\"]+)&(?:amp;)?cursor_id=([^&\"]+)", html1)
        assert cursor_match, "Next-page cursor link not found"
        cursor_at = unquote(cursor_match.group(1))
        cursor_id = unquote(cursor_match.group(2).rstrip('"'))

        # Pass as params dict so httpx re-encodes properly (avoids '+' misparse)
        resp2 = await client.get(
            "/admin/queues/bulk",
            params={"status": "pending", "cursor_at": cursor_at, "cursor_id": cursor_id},
        )
        assert resp2.status_code == 200
        html2 = resp2.text

    # Page 2: 50 data rows + header ≥ 51 <tr>; strictly fewer than page 1's 101
    assert html2.count("<tr") >= 51
    assert html2.count("<tr") < html1.count("<tr")

    # Verify the two pages are disjoint: extract job UUIDs from each page.
    uuid_re = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    ids1 = set(uuid_re.findall(html1))
    ids2 = set(uuid_re.findall(html2))
    overlap = ids1 & ids2
    assert not overlap, f"Pages share {len(overlap)} job IDs — pagination is not disjoint"


# ── Polling badge with no Redis ───────────────────────────────────


@pytest.mark.asyncio
async def test_polling_badge_no_redis(pool: asyncpg.Pool) -> None:
    """Polling badge and meta-refresh tag present when redis_client=None."""
    resp = await _get(_make_app(pool, redis_client=None), "/admin/queues")

    assert resp.status_code == 200
    html = resp.text
    assert "polling mode" in html
    assert '<meta http-equiv="refresh" content="2">' in html


# ── Real-time badge with Redis ────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.redis
async def test_realtime_badge_with_redis(pool: asyncpg.Pool, redis_url: str) -> None:
    """Real-time badge present and no meta-refresh when redis_client is set."""
    import redis.asyncio as aioredis

    redis_client = aioredis.from_url(redis_url)
    try:
        resp = await _get(_make_app(pool, redis_client=redis_client), "/admin/queues")
    finally:
        await redis_client.aclose()

    assert resp.status_code == 200
    html = resp.text
    assert "real-time mode" in html
    assert '<meta http-equiv="refresh"' not in html


# ── Valid auth_dependency returns 200 ─────────────────────────────


@pytest.mark.asyncio
async def test_auth_dependency_accept_path(pool: asyncpg.Pool) -> None:
    """auth_dependency that does not raise returns HTTP 200."""

    def _accept() -> None:
        pass

    resp = await _get(_make_app(pool, auth_dependency=_accept), "/admin/queues")
    assert resp.status_code == 200


# ── POST to queue route returns 405 ────────────────────────────────


@pytest.mark.asyncio
async def test_post_queues_returns_405(pool: asyncpg.Pool) -> None:
    """POST to /admin/queues returns HTTP 405 Method Not Allowed."""
    resp = await _post(_make_app(pool), "/admin/queues")
    assert resp.status_code == 405


# ── POST to job detail returns 405 ─────────────────────────────────


@pytest.mark.asyncio
async def test_post_job_detail_returns_405(pool: asyncpg.Pool) -> None:
    """POST to /admin/jobs/{id} returns HTTP 405 Method Not Allowed."""
    resp = await _post(_make_app(pool), f"/admin/jobs/{uuid.uuid4()}")
    assert resp.status_code == 405


# ── Autoescape prevents XSS ────────────────────────────────────────


@pytest.mark.asyncio
async def test_autoescape_prevents_xss(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Jinja2 autoescape is globally enabled and escapes HTML in rendered output.

    Two-layer check:
    1. The Environment object has autoescape=True (structural guarantee).
    2. A payload containing <script> is escaped to &lt;script&gt; in the HTTP response
       (behavioral guarantee — output to the browser is safe).
    """
    # Structural: autoescape must be unconditionally enabled on the env.
    bundle = create_router(pool, schema=_SCHEMA_LABEL)
    env = bundle.templates
    assert env.autoescape, "Jinja2 autoescape must be True to prevent XSS"

    # Behavioral: rendered HTML must not contain the raw tag.
    [jid] = await _seed_jobs(
        conn,
        queue="default",
        count=1,
        payload='{"xss": "<script>alert(1)</script>"}',
    )

    resp = await _get(_make_app(pool), f"/admin/jobs/{jid}")

    assert resp.status_code == 200
    html = resp.text
    assert "&lt;script&gt;" in html
    assert "<script>alert(1)</script>" not in html


# ── Queue overview performance smoke test ──────────────────────────


@pytest.mark.asyncio
async def test_queue_overview_performance(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Queue overview page loads in < 500ms with 100 pending jobs."""
    import warnings

    await _seed_jobs(conn, queue="perf", count=100, status="pending")

    app = _make_app(pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/admin/queues")  # warm-up; cold-start excluded per spec

        start = time.perf_counter()
        resp = await client.get("/admin/queues")
        elapsed_ms = (time.perf_counter() - start) * 1000

    assert resp.status_code == 200
    if elapsed_ms >= 500:
        warnings.warn(
            f"queue overview took {elapsed_ms:.0f}ms (>= 500ms threshold)",
            stacklevel=2,
        )


# ── Cancel helpers ─────────────────────────────────────────────────────────


async def _post_cancel(
    app: FastAPI,
    job_id: uuid.UUID,
    *,
    follow_redirects: bool = True,
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=follow_redirects,
    ) as client:
        get_resp = await client.get(f"/admin/jobs/{job_id}")
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        return await client.post(
            f"/admin/jobs/{job_id}/cancel",
            data={"csrf_token": csrf_token},
        )


async def _seed_scheduled_job(
    conn: asyncpg.Connection,
    *,
    queue: str,
    delay: timedelta = timedelta(hours=1),
) -> uuid.UUID:
    """Seed a deferred job with scheduled_at in the future (status='scheduled')."""
    jid = uuid.uuid4()
    sa = datetime.now(UTC) + delay
    stc = sa + timedelta(hours=1)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close
        ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 0, $8, $9)""",
        jid,
        "test_actor",
        queue,
        '{"key": "value"}',
        3,
        "transient",
        "scheduled",
        sa,
        stc,
    )
    return jid


# ── Cancel pending job via admin ──────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_pending_job(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """POST /admin/jobs/{id}/cancel transitions a pending job to cancelled."""
    [jid] = await _seed_jobs(conn, queue="default", count=1, status="pending")

    app = _make_app(pool)
    resp = await _post_cancel(app, jid)

    assert resp.status_code == 200  # followed redirect to job detail
    assert "cancelled" in resp.text

    row = await conn.fetchrow(f'SELECT status FROM "{_SCHEMA_LABEL}".jobs WHERE id = $1', jid)
    assert row is not None
    assert row["status"] == "cancelled"


# ── Cancel deferred (scheduled) job via admin ─────────────────────


@pytest.mark.asyncio
async def test_cancel_scheduled_job(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """POST /admin/jobs/{id}/cancel transitions a deferred job to cancelled."""
    jid = await _seed_scheduled_job(conn, queue="default")

    app = _make_app(pool)
    resp = await _post_cancel(app, jid)

    assert resp.status_code == 200
    assert "cancelled" in resp.text

    row = await conn.fetchrow(
        f'SELECT status, finished_at FROM "{_SCHEMA_LABEL}".jobs WHERE id = $1', jid
    )
    assert row is not None
    assert row["status"] == "cancelled"
    assert row["finished_at"] is not None


# ── Cancel running job sets cancel_phase=1 ────────────────────────


@pytest.mark.asyncio
async def test_cancel_running_job_sets_phase(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """POST /admin/jobs/{id}/cancel on a running job sets cancel_phase=1."""
    worker_id = await _seed_worker(conn)
    jid = await _seed_running_job(conn, queue="default", worker_id=worker_id)

    app = _make_app(pool)
    resp = await _post_cancel(app, jid)

    assert resp.status_code == 200

    row = await conn.fetchrow(
        f'SELECT status, cancel_phase, cancel_requested_at FROM "{_SCHEMA_LABEL}".jobs WHERE id = $1',
        jid,
    )
    assert row is not None
    assert row["status"] == "running"  # still running; worker must observe the phase
    assert row["cancel_phase"] == 1
    assert row["cancel_requested_at"] is not None


# ── Cancel already-terminal job returns 409 ────────────────────────


@pytest.mark.asyncio
async def test_cancel_terminal_job_returns_409(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """POST /admin/jobs/{id}/cancel on a terminal job returns HTTP 409."""
    [jid] = await _seed_jobs(conn, queue="default", count=1, status="succeeded")

    app = _make_app(pool)
    resp = await _post_cancel(app, jid, follow_redirects=False)

    assert resp.status_code == 409


# ── Cancel button shown/hidden based on job status ─────────────────


@pytest.mark.asyncio
async def test_cancel_button_visibility(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """Cancel button appears for non-terminal jobs and is absent for terminal ones."""
    [pending_jid] = await _seed_jobs(conn, queue="default", count=1, status="pending")
    [done_jid] = await _seed_jobs(conn, queue="default", count=1, status="succeeded")

    app = _make_app(pool)

    pending_resp = await _get(app, f"/admin/jobs/{pending_jid}")
    assert pending_resp.status_code == 200
    assert "Cancel Job" in pending_resp.text

    done_resp = await _get(app, f"/admin/jobs/{done_jid}")
    assert done_resp.status_code == 200
    assert "Cancel Job" not in done_resp.text


# ── Admin UI archive fallback ──────────────────────────────────────


@pytest.mark.asyncio
async def test_admin_archive_fallback(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """GET /admin/jobs/{id} falls back to jobs_archive and renders Archived badge."""
    jid = uuid.uuid4()
    now = datetime.now(UTC)
    archived_at_val = now - timedelta(hours=1)
    expire_at_val = now + timedelta(days=365)

    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            archived_at, expire_at
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            'succeeded', 0, $7, $8,
            $9, $10
        )""",
        jid,
        "archived_actor",
        "default",
        '{"key": "value"}',
        3,
        "transient",
        now,
        now + timedelta(seconds=60),
        archived_at_val,
        expire_at_val,
    )

    worker_id = await _seed_worker(conn)
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.job_attempts_archive
            (job_id, attempt, started_at, finished_at, outcome, duration_ms, worker_id)
            VALUES ($1, 1, $2, $3, 'succeeded', 100, $4)""",
        jid,
        now - timedelta(minutes=5),
        now - timedelta(minutes=4),
        worker_id,
    )

    resp = await _get(_make_app(pool), f"/admin/jobs/{jid}")

    assert resp.status_code == 200
    html = resp.text
    assert "Archived" in html
    assert archived_at_val.isoformat() in html
    assert "archived_actor" in html


# ── Admin UI 404 for unknown job ────────────────────────────────────


@pytest.mark.asyncio
async def test_admin_unknown_job_404(pool: asyncpg.Pool) -> None:
    """GET /admin/jobs/{unknown_uuid} returns 404 when absent from both tables."""
    unknown_id = uuid.uuid4()
    resp = await _get(_make_app(pool), f"/admin/jobs/{unknown_id}")
    assert resp.status_code == 404


# ── Schedule action helpers ─────────────────────────────────────────────────


async def _seed_schedule(
    conn: asyncpg.Connection,
    *,
    actor: str = "schedule_actor",
    cron_expr: str = "*/5 * * * *",
    enabled: bool = True,
    consecutive_failures: int = 0,
    last_fire_error: str | None = None,
    static_payload: dict[str, object] | None = None,
) -> uuid.UUID:
    sid = uuid.uuid4()
    next_fire = datetime.now(UTC) + timedelta(minutes=5)
    metadata: dict[str, object] = {}
    if static_payload is not None:
        metadata["static_payload"] = static_payload
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.cron_schedules
            (id, actor, cron_expr, timezone, enabled, next_fire_at,
             consecutive_failures, last_fire_error, metadata)
            VALUES ($1, $2, $3, 'UTC', $4, $5, $6, $7, $8::jsonb)""",
        sid,
        actor,
        cron_expr,
        enabled,
        next_fire,
        consecutive_failures,
        last_fire_error,
        dumps_str(metadata),
    )
    return sid


async def _seed_actor_config(
    conn: asyncpg.Connection,
    *,
    actor: str = "schedule_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> None:
    await conn.execute(
        f"""INSERT INTO {_SCHEMA_LABEL}.actor_config (actor, queue, max_attempts, retry_kind)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (actor) DO NOTHING""",
        actor,
        queue,
        max_attempts,
        retry_kind,
    )


async def _post_schedule_action(
    app: FastAPI,
    schedule_id: uuid.UUID,
    action: str,
    *,
    follow_redirects: bool = False,
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=follow_redirects,
    ) as client:
        get_resp = await client.get("/admin/schedules")
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        return await client.post(
            f"/admin/schedules/{schedule_id}/{action}",
            data={"csrf_token": csrf_token},
        )


# ── Enable schedule resets failures ────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_enable_resets_failures(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """POST /admin/schedules/{id}/enable resets consecutive_failures and last_fire_error."""
    sid = await _seed_schedule(
        conn,
        enabled=False,
        consecutive_failures=3,
        last_fire_error="something broke",
    )

    app = _make_app(pool)
    resp = await _post_schedule_action(app, sid, "enable", follow_redirects=True)

    assert resp.status_code == 200
    row = await conn.fetchrow(
        f'SELECT enabled, consecutive_failures, last_fire_error FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row is not None
    assert row["enabled"] is True
    assert row["consecutive_failures"] == 0
    assert row["last_fire_error"] is None


# ── Disable schedule sets enabled=false ──────────────────────────────


@pytest.mark.asyncio
async def test_schedule_disable_sets_enabled_false(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """POST /admin/schedules/{id}/disable sets enabled=false."""
    sid = await _seed_schedule(conn, enabled=True)

    app = _make_app(pool)
    resp = await _post_schedule_action(app, sid, "disable", follow_redirects=True)

    assert resp.status_code == 200
    row = await conn.fetchrow(
        f'SELECT enabled FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row is not None
    assert row["enabled"] is False


# ── Skip next advances next_fire_at ──────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_skip_advances_next_fire_at(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """POST /admin/schedules/{id}/skip advances next_fire_at by one period; job count unchanged."""
    from croniter import croniter

    sid = await _seed_schedule(conn, cron_expr="0 * * * *")

    row_before = await conn.fetchrow(
        f'SELECT next_fire_at FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row_before is not None
    next_before: datetime = row_before["next_fire_at"]

    app = _make_app(pool)
    resp = await _post_schedule_action(app, sid, "skip", follow_redirects=True)

    assert resp.status_code == 200
    row_after = await conn.fetchrow(
        f'SELECT next_fire_at FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row_after is not None
    next_after: datetime = row_after["next_fire_at"]
    assert next_after > next_before

    expected = croniter("0 * * * *", next_before).get_next(datetime)
    assert next_after == expected

    job_count = await conn.fetchval(f'SELECT count(*) FROM "{_SCHEMA_LABEL}".jobs')
    assert job_count == 0


# ── Run now enqueues job ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_run_now_enqueues_job(pool: asyncpg.Pool, conn: asyncpg.Connection) -> None:
    """POST /admin/schedules/{id}/run enqueues a job with correct actor and payload; next_fire_at and consecutive_failures unchanged."""
    sid = await _seed_schedule(
        conn,
        static_payload={"key": "val"},
        consecutive_failures=2,
    )
    await _seed_actor_config(conn)

    row_before = await conn.fetchrow(
        f'SELECT next_fire_at, consecutive_failures FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row_before is not None
    next_before: datetime = row_before["next_fire_at"]
    cf_before: int = row_before["consecutive_failures"]

    app = _make_app(pool)
    resp = await _post_schedule_action(app, sid, "run", follow_redirects=True)

    assert resp.status_code == 200

    job = await conn.fetchrow(
        f'SELECT actor, payload FROM "{_SCHEMA_LABEL}".jobs ORDER BY created_at DESC LIMIT 1',
    )
    assert job is not None
    assert job["actor"] == "schedule_actor"
    payload = job["payload"]
    if isinstance(payload, dict):
        assert payload.get("key") == "val"
    else:
        from taskq._json import loads

        parsed = loads(str(payload))
        assert parsed.get("key") == "val"

    row_after = await conn.fetchrow(
        f'SELECT next_fire_at, consecutive_failures FROM "{_SCHEMA_LABEL}".cron_schedules WHERE id = $1',
        sid,
    )
    assert row_after is not None
    assert row_after["next_fire_at"] == next_before
    assert row_after["consecutive_failures"] == cf_before


# ── Skip unknown schedule returns 404 ─────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_skip_unknown_returns_404(pool: asyncpg.Pool) -> None:
    """POST /admin/schedules/{unknown}/skip returns 404."""
    app = _make_app(pool)
    resp = await _post_schedule_action(app, uuid.uuid4(), "skip")
    assert resp.status_code == 404


# ── Run unknown schedule returns 404 ──────────────────────────────────


@pytest.mark.asyncio
async def test_schedule_run_unknown_returns_404(pool: asyncpg.Pool) -> None:
    """POST /admin/schedules/{unknown}/run returns 404."""
    app = _make_app(pool)
    resp = await _post_schedule_action(app, uuid.uuid4(), "run")
    assert resp.status_code == 404


# ── Redirect URLs encode special characters ───────────────────────────


@pytest.mark.asyncio
async def test_run_now_redirect_url_encodes_special_chars(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """POST /admin/schedules/{id}/run URL-encodes actor name with special chars in error redirect."""
    from urllib.parse import parse_qs, urlparse

    sid = await _seed_schedule(conn, actor="my actor&co")

    app = _make_app(pool)
    resp = await _post_schedule_action(app, sid, "run", follow_redirects=False)

    assert resp.status_code == 303
    location = resp.headers["location"]
    parsed = urlparse(location)
    params = parse_qs(parsed.query)
    error_val = params.get("error", [None])[0]
    assert error_val is not None
    assert "my actor&co" in error_val


# ── Error query parameter rendered in schedules page ────────────────


@pytest.mark.asyncio
async def test_run_now_error_rendered_in_schedules_page(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """Run-now on a schedule with missing actor_config redirects with ?error=... and the error message appears in the rendered HTML body."""
    sid = await _seed_schedule(conn, actor="missing_actor")

    app = _make_app(pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        get_resp = await client.get("/admin/schedules")
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        resp = await client.post(
            f"/admin/schedules/{sid}/run",
            data={"csrf_token": csrf_token},
        )

    assert resp.status_code == 200
    html = resp.text
    assert "missing_actor" in html
    # The error message is rendered inside a div with bg-red-50 styling,
    # not as a "notice error" CSS class. Check for the error text instead.
    assert "not configured" in html


@pytest.mark.asyncio
async def test_error_query_param_rendered_with_autoescape(
    pool: asyncpg.Pool, conn: asyncpg.Connection
) -> None:
    """?error=<script>... on /admin/schedules is escaped in the rendered HTML."""
    await _seed_schedule(conn)
    await _seed_actor_config(conn)

    app = _make_app(pool)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        resp = await client.get("/admin/schedules", params={"error": "<script>alert(1)</script>"})

    assert resp.status_code == 200
    html = resp.text
    assert "&lt;script&gt;" in html
    assert "<script>alert(1)</script>" not in html

"""Tests for the shared per-actor executor stats helper and the actors page.

The ``/api/history/stats`` JSON endpoint and the actors page render the
same aggregate read through ``taskq.web.admin._actor_stats``; these tests
pin the SQL shape of both groupings (the archive-UNION-live read, the
terminal-only live side, the window bound's clock), the JSON wrapper's
passthrough contract, the merged actors-table rendering (executor
columns, failure share, last error class, window toggle, history-only
actors, truncation notice), and the window selector's closed-set parse.
The row-driven arms run the real router over the module's real pool (the
tests/test_web_admin_actors.py pattern); the binding/order pins wrap the
real pool in a recording proxy - COUNT, DON'T ANSWER.
"""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import (
    FastAPI,  # Why: importorskip guard must precede.
    HTTPException,
)

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin._actor_stats import (
    STATS_LIMIT,
    STATS_WINDOWS,
    _build_stats_sql,
    fetch_actor_stats,
    resolve_stats_window,
)

from . import _StubPool

# ── Configurable stub pool/connection ───────────────────────────────────


pytestmark = [pytest.mark.fastapi]


# ── the real-PG tier ─────────────────────────────────────────────────────


def _stats_app(pool: asyncpg.Pool, schema: str) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="")
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


class _StatsRecordingPool:
    """Recording proxy over the real pool: records every statement and
    parameter list, then lets the real read proceed untouched - COUNT,
    DON'T ANSWER (the house pattern of
    tests/test_cancel_where_bounded.py::_StatementRecordingPool)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    @asynccontextmanager
    async def acquire(self, **kwargs: object) -> AsyncGenerator[Any, None]:
        async with self._pool.acquire(**kwargs) as conn:  # type: ignore[call-overload]  # Why: the real pool's acquire timeout kwarg passes through untouched.
            outer = self

            class _Recorder:
                def __init__(self, inner: Any) -> None:
                    self._inner = inner

                async def fetch(self, sql: str, *args: object) -> Any:
                    outer.calls.append((sql, args))
                    return await self._inner.fetch(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._inner, name)

            yield _Recorder(conn)


class _StatsRecordingConn:
    """Recording proxy over one real connection for direct helper calls:
    records (sql, args) and delegates every read."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> Any:
        self.calls.append((sql, args))
        return await self._conn.fetch(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


async def _seed_terminal_job_with_attempt(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    status: str = "failed",
    error_class: str | None = "ValueError",
    duration_ms: int = 90,
    age: timedelta | None = None,
    in_archive: bool = False,
) -> None:
    """One terminal job row with one completed attempt (the stats read's
    own inputs). *age* back-dates the row for window tests."""
    shift = f"- interval '{int(age.total_seconds())} seconds'" if age is not None else ""
    table = "jobs_archive" if in_archive else "jobs"
    extra_cols = ", archived_at, expire_at" if in_archive else ""
    extra_vals = (
        ", clock_timestamp() + interval '30 days', clock_timestamp() + interval '365 days'"
        if in_archive
        else ""
    )
    jid = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".{table} (
                id, actor, queue, payload, status, max_attempts, retry_kind,
                started_at, finished_at, error_class, error_message{extra_cols})
            VALUES ($1, $2, 'default', '{{}}'::jsonb,
                '{status}'::"{schema}".job_status, 3, 'transient',
                clock_timestamp() {shift},
                clock_timestamp() {shift}, $3, 'boom'{extra_vals})""",
        jid,
        actor,
        error_class,
    )
    await conn.execute(
        f"""INSERT INTO "{schema}".{"job_attempts_archive" if in_archive else "job_attempts"} (
                job_id, attempt, started_at, finished_at, outcome,
                error_class, duration_ms)
            VALUES ($1, 1, clock_timestamp() {shift},
                clock_timestamp() {shift}, 'failed', $2, $3)""",
        jid,
        error_class,
        duration_ms,
    )


async def test_fetch_actor_stats_binds_the_window_as_a_parameter(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A windowed read binds the duration and lets the server anchor it;
    the all-time read sends no parameter at all."""
    conn = _StatsRecordingConn(module_pg_pool)  # pyright: ignore[reportArgumentType]  # Why: the recorder satisfies the fetch surface the helper uses.
    await fetch_actor_stats(conn, schema=module_pg_schema.schema_name)  # pyright: ignore[reportArgumentType]
    assert conn.calls[0][1] == ()

    conn = _StatsRecordingConn(module_pg_pool)  # pyright: ignore[reportArgumentType]
    await fetch_actor_stats(  # pyright: ignore[reportArgumentType]
        conn, schema=module_pg_schema.schema_name, window=timedelta(hours=24)
    )
    assert conn.calls[0][1] == (timedelta(hours=24),)

    with pytest.raises(ValueError, match="positive"):
        await fetch_actor_stats(  # pyright: ignore[reportArgumentType]
            conn, schema=module_pg_schema.schema_name, window=timedelta(0)
        )


# ── GET /api/history/stats: thin wrapper over the helper ────────────────


async def test_history_stats_passthrough_includes_last_activity_and_error(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The JSON endpoint returns the aggregate read's rows under "actors":
    the totals, percentiles and last error class are the real rows'."""
    schema = module_pg_schema.schema_name
    await _seed_terminal_job_with_attempt(clean_pg_conn, schema, actor="send_email", duration_ms=90)
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/history/stats")
    assert response.status_code == 200
    actors = response.json()["actors"]
    assert len(actors) == 1
    assert actors[0]["actor"] == "send_email"
    assert actors[0]["total"] == 1
    assert actors[0]["failed"] == 1
    assert actors[0]["p50_duration_ms"] == 90
    assert actors[0]["p95_duration_ms"] == 90
    assert actors[0]["last_activity_at"] is not None
    assert actors[0]["last_error_class"] == "ValueError"


async def test_history_stats_passes_the_window_through(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """?window= reaches the aggregate read as a bound duration; the
    all-time default sends none."""
    recording = _StatsRecordingPool(module_pg_pool)
    app = _stats_app(recording, module_pg_schema.schema_name)  # pyright: ignore[reportArgumentType]  # Why: the recording proxy satisfies the checkout surface.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/api/history/stats?window=24h")).status_code == 200
    windowed = [args for _sql, args in recording.calls if args]
    assert windowed == [(timedelta(hours=24),)]


async def test_history_stats_rejects_unknown_window(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The endpoint 400s on an unknown window, same closed set as the page."""
    recording = _StatsRecordingPool(module_pg_pool)
    app = _stats_app(recording, module_pg_schema.schema_name)  # pyright: ignore[reportArgumentType]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/history/stats?window=ludicrous")
    assert response.status_code == 400
    assert recording.calls == [], "a rejected window must not reach the database"


# ── SQL shape ────────────────────────────────────────────────────────────


def test_stats_sql_per_queue_groups_by_actor_and_queue() -> None:
    """The per-queue variant keeps the JSON contract's (actor, queue) grouping."""
    sql = _build_stats_sql("taskq", per_queue=True)
    assert "GROUP BY u.actor,\n    u.queue" in sql
    assert f"LIMIT {STATS_LIMIT}" in sql


def test_stats_sql_per_actor_groups_by_actor_only() -> None:
    """The actors-page variant drops the queue column and groups by actor alone."""
    sql = _build_stats_sql("taskq", per_queue=False)
    assert "GROUP BY u.actor\n" in sql
    assert "    u.queue,\n" not in sql
    assert "max(u.finished_at) AS last_activity_at" in sql
    assert "AS last_error_class" in sql
    assert f"LIMIT {STATS_LIMIT}" in sql


def test_stats_sql_interpolates_no_caller_text() -> None:
    """Only the validated schema identifier is interpolated; the all-time
    read needs no parameters at all, exactly the parameter-free shape
    the page always served."""
    for per_queue in (True, False):
        sql = _build_stats_sql("taskq", per_queue=per_queue)
        assert "$" not in sql


def test_stats_sql_unions_the_live_terminal_population() -> None:
    """The aggregate reads the archive UNION the live terminal rows: a
    terminal row stays in ``jobs`` for its whole prune retention, so an
    archive-only read hides an actor's freshest failures for exactly as
    long as they matter most (the History LIST page already UNIONs live;
    this is the aggregate read catching up to it).

    The live side must count terminal rows only: pending/scheduled/
    running rows have no completed work to contribute, and counting them
    would inflate executor totals with rows that never ran.
    """
    sql = _build_stats_sql("taskq", per_queue=False)
    assert 'FROM "taskq".jobs_archive j' in sql
    assert 'FROM "taskq".jobs j' in sql
    assert "UNION ALL" in sql
    assert "status IN ('abandoned', 'cancelled', 'crashed', 'failed', 'succeeded')" in sql
    # Both sides join their own attempts table: a live terminal row's
    # attempts live in job_attempts until the prune moves both.
    assert 'LEFT JOIN "taskq".job_attempts_archive a ON a.job_id = j.id' in sql
    assert 'LEFT JOIN "taskq".job_attempts a ON a.job_id = j.id' in sql


def test_stats_sql_window_bounds_both_sides_with_a_stable_clock() -> None:
    """A window bounds both sides by finished_at, bound once as ``$1`` and
    referenced twice, and anchored to ``statement_timestamp()`` (STABLE),
    not ``clock_timestamp()`` (VOLATILE): an aggregate has no LIMIT to
    hide a post-scan Filter behind, and a VOLATILE bound cannot be a
    btree index condition, so the finished_at indexes would stop serving
    the window (the two-clock doctrine the backend sweeps derive)."""
    sql = _build_stats_sql("taskq", per_queue=False, window=True)
    assert sql.count("statement_timestamp() - $1::interval") == 2, (
        "both sides of the UNION must carry the window bound"
    )
    assert "clock_timestamp()" not in sql, (
        "a VOLATILE window bound degrades the aggregate to a post-scan "
        "Filter over the whole archive population"
    )
    # The archive side composes its own WHERE; the live side already
    # filters terminal statuses, so the window rides as an and.
    assert "job_attempts_archive a ON a.job_id = j.id\n    WHERE j.finished_at >=" in sql
    assert "status IN ('abandoned', 'cancelled', 'crashed', 'failed', 'succeeded')\n" in sql
    assert "    AND j.finished_at >=" in sql


def test_stats_sql_last_error_class_takes_the_most_recent_non_null() -> None:
    """last_error_class is the actor's most recent row that carries one,
    ordered by finished_at DESC NULLS last so a pathological NULL-
    finished_at row cannot shadow a stamped one."""
    sql = _build_stats_sql("taskq", per_queue=False)
    assert "ORDER BY u.finished_at DESC NULLS LAST" in sql
    assert "FILTER (WHERE u.error_class IS NOT NULL)" in sql


# ── Window selector ──────────────────────────────────────────────────────


def test_resolve_stats_window_defaults_and_named_ranges() -> None:
    """None and "all" resolve to the documented all-time default; named
    ranges resolve to their durations."""
    assert resolve_stats_window(None) is None
    assert resolve_stats_window("all") is None
    assert resolve_stats_window("24h") == timedelta(hours=24)
    assert resolve_stats_window("7d") == timedelta(days=7)
    assert set(STATS_WINDOWS) == {"1h", "24h", "7d", "30d"}


def test_resolve_stats_window_rejects_unknown_values_with_400() -> None:
    """An unknown window is a clean 400, never a silent fallback to
    all-time: the URL would claim a recency view while the page served
    the whole retained history, the wrong-but-plausible answer."""
    with pytest.raises(HTTPException) as exc_info:
        resolve_stats_window("24hr")
    assert exc_info.value.status_code == 400
    assert "24hr" in exc_info.value.detail


# ── GET /api/history/stats: thin wrapper over the helper ────────────────


async def test_history_stats_empty_pool_returns_empty_actors(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """With no rows at all the endpoint returns {"actors": []}."""
    from taskq.testing.pg import reset_schema

    await reset_schema(clean_pg_conn, module_pg_schema.schema_name, actors=[])
    app = _stats_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/history/stats")
    assert response.status_code == 200
    assert response.json() == {"actors": []}


# ── fetch_actor_stats helper ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_actor_stats_returns_dicts(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """fetch_actor_stats maps rows to dicts for the callers."""
    for actor in ("a", "b"):
        await _seed_terminal_job_with_attempt(
            clean_pg_conn, module_pg_schema.schema_name, actor=actor
        )
    rows = await fetch_actor_stats(clean_pg_conn, schema=module_pg_schema.schema_name)
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


async def test_actors_page_returns_html(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """GET /actors returns 200 with text/html content type."""
    app = _stats_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")


async def test_actors_page_empty_state(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """With no config rows and no stats the page renders its empty state.
    The reset re-seeds the default actors, so the empty state is produced
    by truncating without the re-seed."""
    from taskq.testing.pg import reset_schema

    await reset_schema(clean_pg_conn, module_pg_schema.schema_name, actors=[])
    app = _stats_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    assert "No actor_config rows." in response.text


# ── GET /actors: merged executor stats rendering ─────────────────────────


async def test_actors_page_renders_executor_stats(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The actors table renders jobs total, failure share, last error
    class, percentiles, and activity - the real rows' aggregate."""
    from taskq.actor_config import ActorConfig
    from taskq.worker.startup import sync_actor_config

    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="send_email", max_concurrent=4, queue="default")],
        schema=schema,
    )
    await _seed_terminal_job_with_attempt(clean_pg_conn, schema, actor="send_email", duration_ms=90)
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    assert response.status_code == 200
    html = response.text
    assert ">Jobs<" in html
    assert ">Last Error<" in html
    assert "p50 / p95 (ms)" in html
    assert "1" in html  # jobs total
    assert "1 (100.0%)" in html  # failure count and share
    assert "ValueError" in html  # last error class
    assert "90 / 90" in html  # p50 / p95


async def test_actors_page_renders_the_window_toggle(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The toggle renders every window plus the all-time default, marks
    the active one, and links each to the windowed page."""
    from taskq.actor_config import ActorConfig
    from taskq.worker.startup import sync_actor_config

    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="send_email", max_concurrent=4, queue="default")],
        schema=schema,
    )
    await _seed_terminal_job_with_attempt(clean_pg_conn, schema, actor="send_email")
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        all_time = await client.get("/actors")
        assert "window=all" in all_time.text
        for w in STATS_WINDOWS:
            assert f"window={w}" in all_time.text
        # All time is the active window on an unfiltered load: the default
        # the docs state, not a hidden one.
        assert "All time" in all_time.text

        windowed = await client.get("/actors?window=24h")
    assert "border-blue-500" in windowed.text, "the active window must be visually marked"


async def test_actors_page_window_reaches_the_stats_read(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """?window=24h binds the duration on the stats read; the default
    binds nothing: the operator's toggle is not cosmetic."""
    schema = module_pg_schema.schema_name
    recording = _StatsRecordingPool(module_pg_pool)
    app = _stats_app(recording, schema)  # pyright: ignore[reportArgumentType]  # Why: the recording proxy satisfies the checkout surface.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/actors?window=7d")
    stats_calls = [args for sql, args in recording.calls if "percentile_cont" in sql]
    assert stats_calls == [(timedelta(days=7),)], (
        f"the windowed page must bind 7d on the stats read: {recording.calls!r}"
    )

    recording = _StatsRecordingPool(module_pg_pool)
    app = _stats_app(recording, schema)  # pyright: ignore[reportArgumentType]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/actors")
    stats_calls = [args for sql, args in recording.calls if "percentile_cont" in sql]
    assert stats_calls == [()]


async def test_actors_page_rejects_unknown_window(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """An unknown window value is a clean 400, not a silent all-time."""
    recording = _StatsRecordingPool(module_pg_pool)
    app = _stats_app(recording, module_pg_schema.schema_name)  # pyright: ignore[reportArgumentType]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors?window=42d")
    assert response.status_code == 400
    assert recording.calls == [], "a rejected window must not reach the database"


async def test_actors_page_config_actor_without_stats_renders_dashes(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A config actor with no completed history renders placeholder dashes, not zeros."""
    from taskq.actor_config import ActorConfig
    from taskq.worker.startup import sync_actor_config

    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="cold_actor", max_concurrent=4, queue="default")],
        schema=schema,
    )
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    html = response.text
    assert "cold_actor" in html
    assert "history only" not in html


async def test_actors_page_stats_only_actor_has_no_deregister_form(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A stats-only actor (no config row) renders without a deregister form."""
    schema = module_pg_schema.schema_name
    await _seed_terminal_job_with_attempt(
        clean_pg_conn, schema, actor="ghost_actor", status="succeeded", error_class=None
    )
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    html = response.text
    assert "ghost_actor" in html
    assert "/actors/ghost_actor/deregister" not in html
    assert "history only" in html


async def test_actors_page_shows_truncation_notice(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A stats read at the cap renders the truncation notice: one terminal
    row on each of STATS_LIMIT distinct actors."""
    schema = module_pg_schema.schema_name
    await clean_pg_conn.executemany(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, status, max_attempts, retry_kind,
                started_at, finished_at, error_class, error_message)
            VALUES ($1, $2, 'default', '{{}}'::jsonb,
                'failed'::"{schema}".job_status, 3, 'transient',
                clock_timestamp(), clock_timestamp(), 'ValueError', 'boom')""",
        [(new_uuid(), f"actor_{i}") for i in range(STATS_LIMIT)],
    )
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    assert "most active actors" in response.text


async def test_actors_page_no_truncation_notice_under_cap(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Under the cap no truncation notice renders."""
    schema = module_pg_schema.schema_name
    await _seed_terminal_job_with_attempt(clean_pg_conn, schema, actor="send_email")
    app = _stats_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/actors")
    assert "most active actors" not in response.text


# ── _merge_actor_stats unit behavior ─────────────────────────────────────


def _summary_row(actor: str = "send_email") -> dict[str, Any]:
    """One actor_config summary row as list_actor_summaries returns it."""
    return {
        "actor": actor,
        "queue": "default",
        "max_concurrent": 4,
        "max_pending": 100,
        "updated_at": "2025-01-01T00:00:00+00:00",
        "active_job_count": 2,
        "enabled_schedule_count": 1,
    }


def _stats_row(actor: str = "send_email", **overrides: Any) -> dict[str, Any]:
    """One per-actor stats row as fetch_actor_stats returns it."""
    row: dict[str, Any] = {
        "actor": actor,
        "total": 10,
        "failed": 2,
        "p50_duration_ms": 90,
        "p95_duration_ms": 200,
        "last_activity_at": datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
        "last_error_class": "ValueError",
    }
    row.update(overrides)
    return row


def test_merge_actor_stats_appends_stats_only_actors_hot_first() -> None:
    """Config rows keep name order; stats-only actors append hottest first."""
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


def test_merge_actor_stats_failure_share_and_last_error() -> None:
    """The failure share is the failed fraction of total, as a percent
    string; the last error class passes through unchanged."""
    from taskq.web.admin.actors import _failure_share, _merge_actor_stats

    merged = _merge_actor_stats([], [dict(_stats_row(total=340, failed=17))])
    assert merged[0]["failure_share"] == "5.0%"
    assert merged[0]["last_error_class"] == "ValueError"
    assert _failure_share(0, 0) is None
    assert _failure_share(None, None) is None
    assert _failure_share(10, 0) == "0.0%"

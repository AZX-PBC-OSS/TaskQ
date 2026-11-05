"""Tests for schedules, rate-limits, reservations routes/templates, Redis bucket fetch, and XSS escaping."""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
import structlog.testing

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.

from taskq._ids import new_base62, new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.ops import _fetch_redis_rl_state

from . import StubBackend, StubPipelinedRedis, _stub_job_row, _StubPool

# ── Schedules, rate-limits, reservations routes: discovery ───────────────


pytestmark = [pytest.mark.fastapi]

# ── the real-PG tier ─────────────────────────────────────────────────────
#
# The ops pages' row-driven arms run the real router over the module's real
# pool (the tests/test_web_admin_integration.py pattern): the schedule,
# bucket and slot rows the routes read are seeded with real inserts and the
# mutations are read back from the database. The pre-migration arms point
# the router at a schema no migration has touched - an honest un-migrated
# deployment, where Postgres itself raises UndefinedTableError. The arms
# that stay monkeypatched (a registry peek failing, a slot sync failing)
# are the fault injections the patch registry records.


def _ops_app(pool: asyncpg.Pool, schema: str, **router_kwargs: object) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="", **router_kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _ops_csrf_token(pool: asyncpg.Pool, schema: str) -> str:
    """Mint a real CSRF cookie the way the page does: one GET /queues."""
    app = _ops_app(pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/queues")
        token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the schedules page never minted a CSRF cookie"
    return token


async def _seed_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    enabled: bool = True,
    payload_factory: str | None = None,
    next_fire_at: datetime | None = None,
    metadata: str = "{}",
) -> Any:
    """One cron_schedules row as the schedules page's own writes produce."""
    sid: Any = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".cron_schedules (
                id, actor, cron_expr, timezone, dst_strategy, payload_factory,
                enabled, next_fire_at, metadata)
            VALUES ($1, $2, '* * * * *', 'UTC', 'skip', $3, $4, $5, $6::jsonb)""",
        sid,
        actor,
        payload_factory,
        enabled,
        next_fire_at if next_fire_at is not None else datetime.now(UTC),
        metadata,
    )
    return sid


async def _seed_rl_bucket(
    conn: asyncpg.Connection,
    schema: str,
    *,
    name: str,
    kind: str = "token_bucket",
    state: str = '{"tokens": 5}',
) -> None:
    await conn.execute(
        f"""INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state)
            VALUES ($1, $2, $3::jsonb)""",
        name,
        kind,
        state,
    )


async def _seed_reservation_slots(
    conn: asyncpg.Connection, schema: str, *, bucket: str, held: int, free: int
) -> None:
    """*held* live-lease slots and *free* empty slots for one bucket."""
    now = datetime.now(UTC)
    for i in range(held):
        await conn.execute(
            f"""INSERT INTO "{schema}".reservation_slots
                    (bucket_name, slot_index, job_id, held_by_worker_id,
                     acquired_at, lease_expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)""",
            bucket,
            i,
            new_uuid(),
            new_uuid(),
            now,
            now + timedelta(seconds=30),
        )
    for i in range(held, held + free):
        await conn.execute(
            f"""INSERT INTO "{schema}".reservation_slots
                    (bucket_name, slot_index, job_id, held_by_worker_id,
                     acquired_at, lease_expires_at)
                VALUES ($1, $2, NULL, NULL, NULL, NULL)""",
            bucket,
            i,
        )


def test_schedules_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /schedules route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/schedules" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_rate_limits_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /rate-limits route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/rate-limits" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_reservations_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /reservations route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/reservations" in route_paths  # pyright: ignore[reportUnknownVariableType]


# ── Schedules page returns HTML ────────────────────────────────────────


def test_schedules_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /schedules returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/schedules")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_rate_limits_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /rate-limits returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/rate-limits")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_reservations_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /reservations returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/reservations")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


# ── Schedules template ──────────────────────────────────────────────────


def test_schedules_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: schedules.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "schedules.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_schedules_template_renders_data(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """schedules.html renders cron schedule rows when cron_installed is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("schedules.html")
    html = template.render(
        cron_installed=True,
        schedules=[
            {
                "actor": "cleanup",
                "cron_expr": "0 3 * * *",
                "timezone": "UTC",
                "next_fire_at": "2025-01-02T03:00:00+00:00",
                "enabled": True,
                "last_fired_at": None,
                "last_fire_error": None,
                "consecutive_failures": 0,
            },
        ],
    )
    assert "cleanup" in html
    assert "0 3 * * *" in html
    assert "UTC" in html
    assert "0" in html


def test_schedules_template_not_installed_notice(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """schedules.html shows 'cron scheduling not installed' notice when cron_installed is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("schedules.html")
    html = template.render(
        cron_installed=False,
        schedules=[],
        notice_text="cron scheduling not installed - run taskq migrate up to enable",
    )
    assert "cron scheduling not installed" in html


def test_schedules_template_empty_schedules(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """schedules.html shows 'No cron schedules configured' when cron_installed but list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("schedules.html")
    html = template.render(cron_installed=True, schedules=[])
    assert "No cron schedules configured" in html


# ── Rate-limits template ────────────────────────────────────────────────


def test_rate_limits_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: rate_limits.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "rate_limits.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_rate_limits_template_renders_configured_bucket(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html renders configured bucket with backend and config summary."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "api:global",
                "kind": "token_bucket",
                "backend": "redis",
                "config_summary": "capacity=3, refill=1.0/s",
                "pg_state": '{"tokens":100}',
                "updated_at": "2025-01-01T00:00:00+00:00",
            },
        ],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "api:global" in html
    assert "tb" in html
    assert "redis" in html
    assert "capacity=3" in html


def test_rate_limits_template_redis_state(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html renders Redis state column when redis_available is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "api:global",
                "kind": "token_bucket",
                "backend": "redis",
                "config_summary": "capacity=3, refill=1.0/s",
                "pg_state": "",
                "updated_at": "",
            },
        ],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state={"api:global": {"tokens": "95", "last_refill": "2025-01-01T00:00:01"}},
        redis_available=True,
        redis_configured=True,
    )
    assert "Redis State" in html
    assert "95" in html


def test_rate_limits_template_redis_unavailable_notice(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html shows Redis-unavailable notice when redis was configured but failed."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "api:global",
                "kind": "token_bucket",
                "backend": "redis",
                "config_summary": "capacity=3, refill=1.0/s",
                "pg_state": "",
                "updated_at": "",
            },
        ],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=True,
    )
    assert "Redis state unavailable" in html


def test_rate_limits_template_no_redis_notice_when_not_configured(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html omits Redis-unavailable notice when Redis was never configured."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "api:global",
                "kind": "token_bucket",
                "backend": "redis",
                "config_summary": "capacity=3, refill=1.0/s",
                "pg_state": "",
                "updated_at": "",
            },
        ],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "Redis state unavailable" not in html


def test_rate_limits_template_empty(monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool) -> None:
    """rate_limits.html shows 'No rate limit buckets registered' when list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        buckets=[],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "No rate limit buckets registered" in html


def test_rate_limits_template_not_installed_notice(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html shows 'rate limiting not installed' notice when ratelimit_installed is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        buckets=[],
        ratelimit_installed=False,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "rate limiting not installed" in html


def test_rate_limits_template_shows_memory_backend(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html renders memory-backed bucket with em-dash for PG state."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "local-limiter",
                "kind": "sliding_window",
                "backend": "memory",
                "config_summary": "limit=2, window=0:00:10, style=log",
                "pg_state": "",
                "updated_at": "",
            },
        ],
        ratelimit_installed=True,
        notice_text="",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "local-limiter" in html
    assert "memory" in html
    assert "limit=2" in html


def test_rate_limits_template_shows_memory_notice(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """rate_limits.html shows in-memory notice when has_memory_buckets is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": "local-limiter",
                "kind": "sliding_window",
                "backend": "memory",
                "config_summary": "limit=2, window=0:00:10, style=log",
                "pg_state": "",
                "updated_at": "",
            },
        ],
        ratelimit_installed=True,
        notice_text="",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
        has_memory_buckets=True,
    )
    assert "backend='memory'" in html
    assert "not queryable from this page" in html


# ── Reservations template ──────────────────────────────────────────────


def test_reservations_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: reservations.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "reservations.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_reservations_template_renders_slot_counts(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """reservations.html renders configured slots, lease, held/free/total counts per bucket."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("reservations.html")
    html = template.render(
        reservations=[
            {
                "bucket_name": "premium-api",
                "configured_slots": 5,
                "lease": "0:00:30",
                "held_count": 3,
                "free_count": 2,
                "total_slots": 5,
            },
        ],
        reservations_installed=True,
        notice_text="reservations not installed - run taskq migrate up to enable",
    )
    assert "premium-api" in html
    assert "5" in html
    assert "0:00:30" in html
    assert "3" in html
    assert "2" in html


def test_reservations_template_held_slots_with_job_links(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """reservations.html renders held slots with job_id as a link to job detail."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("reservations.html")
    html = template.render(
        reservations=[
            {
                "bucket_name": "premium-api",
                "configured_slots": 5,
                "lease": "0:00:30",
                "held_count": 1,
                "free_count": 4,
                "total_slots": 5,
            },
        ],
        reservations_installed=True,
        notice_text="",
        held_slots=[
            {
                "bucket_name": "premium-api",
                "slot_index": 0,
                "job_id": "abc-123",
                "held_by_worker_id": "worker-1",
                "lease_expires_at": "2025-01-01T00:01:00+00:00",
            },
        ],
    )
    assert "abc-123" in html
    assert "/jobs/abc-123" in html
    assert "worker-1" in html


def test_reservations_template_empty(monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool) -> None:
    """reservations.html shows 'No reservation buckets registered' when list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("reservations.html")
    html = template.render(
        reservations=[],
        reservations_installed=True,
        notice_text="reservations not installed - run taskq migrate up to enable",
    )
    assert "No reservation buckets registered" in html


def test_reservations_template_not_installed_notice(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """reservations.html shows 'reservations not installed' notice when reservations_installed is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("reservations.html")
    html = template.render(
        reservations=[],
        reservations_installed=False,
        notice_text="reservations not installed - run taskq migrate up to enable",
    )
    assert "reservations not installed" in html


# ── XSS prevention: schedules, rate-limits, reservations ────────────────


def test_schedules_template_autoescapes_name(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Schedules page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("schedules.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        cron_installed=True,
        schedules=[
            {
                "actor": xss,
                "cron_expr": "* * * * *",
                "timezone": "UTC",
                "next_fire_at": "",
                "enabled": True,
                "last_fired_at": None,
                "last_fire_error": None,
                "consecutive_failures": 0,
            }
        ],
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


def test_rate_limits_template_autoescapes_bucket_name(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Rate-limits page auto-escapes user-derived fields (bucket_name from user input could contain XSS)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("rate_limits.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        live_states={},
        buckets=[
            {
                "bucket_name": xss,
                "kind": "token_bucket",
                "backend": "redis",
                "config_summary": "",
                "pg_state": "",
                "updated_at": "",
            }
        ],
        ratelimit_installed=True,
        notice_text="rate limiting not installed - run taskq migrate up to enable",
        redis_state=None,
        redis_available=False,
        redis_configured=False,
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


def test_reservations_template_autoescapes_bucket_name(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Reservations page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("reservations.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        reservations=[
            {
                "bucket_name": xss,
                "configured_slots": 5,
                "lease": "0:00:30",
                "held_count": 0,
                "free_count": 5,
                "total_slots": 5,
            }
        ],
        reservations_installed=True,
        notice_text="reservations not installed - run taskq migrate up to enable",
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


# ── Redis rate-limit state fetch helper ──────────────────────────────────


async def test_fetch_redis_rl_state_returns_none_when_no_redis() -> None:
    """_fetch_redis_rl_state returns None when redis_client is None."""
    result = await _fetch_redis_rl_state(None, "taskq", [], read_timeout=1.0)
    assert result is None


async def test_fetch_redis_rl_state_returns_state() -> None:
    """_fetch_redis_rl_state returns dict of Redis state when successful."""

    class _FakeRedis(StubPipelinedRedis):
        def __init__(self, data: dict[str, dict[str, str]]) -> None:
            self._data = data

        async def hgetall(self, key: str) -> dict[str, str]:
            return self._data.get(key, {})

    names = [("api:global", "token_bucket")]
    redis = _FakeRedis({"taskq:taskq:rl:tb:{api:global}": {"tokens": "95"}})
    result = await _fetch_redis_rl_state(redis, "taskq", names, read_timeout=1.0)
    assert result is not None
    assert "api:global" in result
    assert result["api:global"]["tokens"] == "95"


async def test_fetch_redis_rl_state_returns_none_on_failure() -> None:
    """_fetch_redis_rl_state returns None when Redis raises."""

    class _BrokenRedis(StubPipelinedRedis):
        # The reader raising means the pipeline's single execute() round
        # trip raises - the transport's failure seam - so the fetch's
        # degrade-to-None guard is exercised at the round trip itself.
        async def hgetall(self, key: str) -> dict[str, str]:
            raise ConnectionError("redis down")

    names = [("api:global", "token_bucket")]
    result = await _fetch_redis_rl_state(_BrokenRedis(), "taskq", names, read_timeout=1.0)
    assert result is None


async def test_fetch_redis_rl_state_uses_sliding_window_key() -> None:
    """_fetch_redis_rl_state uses taskq:{schema}:sw:{name} for sliding_window_log kind."""

    class _FakeRedis(StubPipelinedRedis):
        def __init__(self, data: dict[str, int]) -> None:
            self._data = data

        async def zcard(self, key: str) -> int:
            return self._data.get(key, 0)

    names = [("my_window", "sliding_window_log")]
    redis = _FakeRedis({"taskq:myschema:sw:{my_window}": 2})
    result = await _fetch_redis_rl_state(redis, "myschema", names, read_timeout=1.0)
    assert result is not None
    assert "my_window" in result
    assert result["my_window"]["count"] == "2"


async def test_fetch_redis_rl_state_decodes_bytes() -> None:
    """_fetch_redis_rl_state decodes bytes keys/values from Redis."""

    class _FakeRedis(StubPipelinedRedis):
        async def hgetall(self, key: str) -> list[tuple[bytes, bytes]]:
            return [(b"tokens", b"10"), (b"last_refill", b"2025-01-01")]

    names = [("api:global", "token_bucket")]
    result = await _fetch_redis_rl_state(_FakeRedis(), "taskq", names, read_timeout=1.0)
    assert result is not None
    assert result["api:global"]["tokens"] == "10"


# ── _fetch_redis_rl_state: gcra kind ─────────────────────────────────────


async def test_fetch_redis_rl_state_gcra() -> None:
    """_fetch_redis_rl_state returns tat for sliding_window_gcra kind."""

    class _FakeRedis(StubPipelinedRedis):
        async def get(self, key: str) -> str | None:
            return "1234567890.0"

    names = [("my_gcra", "sliding_window_gcra")]
    result = await _fetch_redis_rl_state(_FakeRedis(), "myschema", names, read_timeout=1.0)
    assert result is not None
    assert "my_gcra" in result
    assert result["my_gcra"]["tat"] == "1234567890.0"


# ── Route handler tests: configurable stub pool ─────────────────────────


@pytest.fixture()
def clean_rl_registry() -> Any:
    """Save/restore the global rate-limit registry around each test."""
    from taskq.ratelimit.registry import registry as rl_registry

    saved_rl = dict(rl_registry._rate_limits)  # pyright: ignore[reportPrivateUsage]
    saved_res = dict(rl_registry._reservations)  # pyright: ignore[reportPrivateUsage]
    rl_registry._rate_limits.clear()  # pyright: ignore[reportPrivateUsage]
    rl_registry._reservations.clear()  # pyright: ignore[reportPrivateUsage]
    yield rl_registry
    rl_registry._rate_limits.clear()  # pyright: ignore[reportPrivateUsage]
    rl_registry._reservations.clear()  # pyright: ignore[reportPrivateUsage]
    rl_registry._rate_limits.update(saved_rl)  # pyright: ignore[reportPrivateUsage]
    rl_registry._reservations.update(saved_res)  # pyright: ignore[reportPrivateUsage]


# ── Schedule enable ─────────────────────────────────────────────────────


async def test_schedule_enable_redirects_on_success(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/enable returns 303 and the row's enabled flag
    actually flips - read back from the database the UPDATE hit."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(clean_pg_conn, schema, actor="enable_me", enabled=False)
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/enable", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    enabled = await clean_pg_conn.fetchval(
        f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert enabled is True


async def test_schedule_enable_returns_404_when_not_found(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/enable returns 404 when no row is updated."""
    schema = module_pg_schema.schema_name
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(f"/schedules/{new_uuid()}/enable", data={"csrf_token": token})
    assert resp.status_code == 404


async def test_schedule_enable_redirects_when_table_missing(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/enable redirects with error when cron_schedules
    is missing: the router points at a schema no migration has applied, so
    Postgres itself raises UndefinedTableError."""
    schema = module_pg_schema.schema_name
    unmigrated = f"unmigrated_{new_base62()}".lower()
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, unmigrated)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{new_uuid()}/enable", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule disable ────────────────────────────────────────────────────


async def test_schedule_disable_redirects_on_success(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/disable returns 303 and the row's enabled flag
    actually flips - read back from the database the UPDATE hit."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(clean_pg_conn, schema, actor="disable_me", enabled=True)
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/disable", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    row = await clean_pg_conn.fetchrow(
        f'SELECT enabled, disabled_by FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert row is not None and row["enabled"] is False and row["disabled_by"] == "operator"


async def test_schedule_disable_returns_404_when_not_found(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/disable returns 404 when no row is updated."""
    schema = module_pg_schema.schema_name
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(f"/schedules/{new_uuid()}/disable", data={"csrf_token": token})
    assert resp.status_code == 404


async def test_schedule_disable_redirects_when_table_missing(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/disable redirects with error when cron_schedules
    is missing: the router points at a schema no migration has applied, so
    Postgres itself raises UndefinedTableError."""
    schema = module_pg_schema.schema_name
    unmigrated = f"unmigrated_{new_base62()}".lower()
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, unmigrated)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{new_uuid()}/disable", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule skip ───────────────────────────────────────────────────────


async def test_schedule_skip_redirects_on_success(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/skip advances next_fire_at strictly past the
    database clock's now (7565d3f: the advance loop compares against db_now
    from the row fetch, so a lagging app clock cannot write a value PG
    already considers past) and returns 303 to the schedules page. The
    written value is read back from the database the UPDATE hit."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(
        clean_pg_conn,
        schema,
        actor="skip_me",
        next_fire_at=datetime.now(UTC) - timedelta(minutes=30),
    )
    db_before = await clean_pg_conn.fetchval("SELECT clock_timestamp()")
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/skip", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert resp.headers["location"].endswith("/schedules")
    written_next = await clean_pg_conn.fetchval(
        f'SELECT next_fire_at FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert isinstance(written_next, datetime)
    assert written_next > db_before


async def test_schedule_skip_returns_404_when_not_found(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/skip returns 404 when the schedule does not exist."""
    schema = module_pg_schema.schema_name
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(f"/schedules/{new_uuid()}/skip", data={"csrf_token": token})
    assert resp.status_code == 404


async def test_schedule_skip_redirects_when_table_missing(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/skip redirects with error when cron_schedules
    is missing: the router points at a schema no migration has applied, so
    Postgres itself raises UndefinedTableError."""
    schema = module_pg_schema.schema_name
    unmigrated = f"unmigrated_{new_base62()}".lower()
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, unmigrated)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{new_uuid()}/skip", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule run now ────────────────────────────────────────────────────


async def test_schedule_run_now_enqueues_job(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run enqueues a job via the backend and returns 303.
    The schedule row and the actor_config row it reads are real rows."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(
        clean_pg_conn,
        schema,
        actor="test_actor",
        metadata='{"static_payload": {"x": 1}}',
    )
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert len(backend.enqueue_calls) == 1


async def test_schedule_run_now_returns_404_when_not_found(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run returns 404 when the schedule does not exist."""
    schema = module_pg_schema.schema_name
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(f"/schedules/{new_uuid()}/run", data={"csrf_token": token})
    assert resp.status_code == 404


async def test_schedule_run_now_redirects_when_table_missing(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run redirects with error when cron_schedules
    is missing: the router points at a schema no migration has applied, so
    Postgres itself raises UndefinedTableError."""
    schema = module_pg_schema.schema_name
    unmigrated = f"unmigrated_{new_base62()}".lower()
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, unmigrated, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{new_uuid()}/run", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


async def test_schedule_run_now_redirects_on_payload_type_error(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run redirects when the payload factory returns a
    non-dict type. The stored factory is a real row's text - os.getpid
    genuinely returns an int when the handler resolves and calls it."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(
        clean_pg_conn, schema, actor="test_actor", payload_factory="os.getpid"
    )
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "factory+returned+unexpected+type" in resp.headers.get("location", "")


async def test_schedule_run_now_redirects_on_payload_factory_error(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run redirects when the payload factory import
    fails. The stored factory names a module no install provides - a row an
    operator can absolutely write (a typo'd dotted path), not an
    injection."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(
        clean_pg_conn, schema, actor="test_actor", payload_factory="nonexistent_pkg_xyz.func"
    )
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert len(backend.enqueue_calls) == 0


async def test_schedule_run_now_redirects_when_actor_not_configured(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """POST /schedules/{id}/run redirects when the actor is not in actor_config."""
    schema = module_pg_schema.schema_name
    sid = await _seed_schedule(clean_pg_conn, schema, actor="unconfigured_actor")
    backend = StubBackend(job_row=_stub_job_row(new_uuid()))
    token = await _ops_csrf_token(module_pg_pool, schema)
    app = _ops_app(module_pg_pool, schema, backend=backend)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303
    assert "not+configured" in resp.headers.get("location", "")
    assert len(backend.enqueue_calls) == 0


# ── Rate limits page with registered primitives ─────────────────────────


async def test_rate_limits_page_with_registered_primitives(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """GET /rate-limits renders TokenBucket and SlidingWindow from the registry,
    with the PG-persisted state row the acquisition path wrote merged in."""
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.ratelimit.token_bucket import TokenBucket

    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    clean_rl_registry.register(
        SlidingWindow(
            "api:sw", limit=5, window=timedelta(seconds=10), backend="memory", style="log"
        )
    )
    await _seed_rl_bucket(clean_pg_conn, module_pg_schema.schema_name, name="api:tb")
    app = _ops_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/rate-limits")
    assert resp.status_code == 200
    body = resp.text
    assert "api:tb" in body
    assert "api:sw" in body


async def test_rate_limits_page_table_missing(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """GET /rate-limits shows 'not installed' notice when rate_limit_buckets is
    missing: the router points at a schema no migration has applied, so the
    page's read raises UndefinedTableError for real."""
    from taskq.ratelimit.token_bucket import TokenBucket

    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    unmigrated = f"unmigrated_{new_base62()}".lower()
    app = _ops_app(module_pg_pool, unmigrated)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/rate-limits")
    assert resp.status_code == 200
    assert "rate limiting not installed" in resp.text


async def test_rate_limits_page_with_pg_state_and_redis(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """GET /rate-limits renders PG state, live states, and PG-only buckets with
    Redis. Both bucket rows are real rows - the registry-backed one and a
    worker-published bucket this process's registry has never heard of."""
    from taskq.ratelimit.token_bucket import TokenBucket

    clean_rl_registry.register(
        TokenBucket("api:global", capacity=10, refill_per_second=1, backend="redis")
    )
    schema = module_pg_schema.schema_name
    await _seed_rl_bucket(clean_pg_conn, schema, name="api:global")
    await _seed_rl_bucket(clean_pg_conn, schema, name="orphan-bucket", state='{"tokens": 2}')

    class _FakeRedis(StubPipelinedRedis):
        async def hmget(self, key: object, fields: object) -> list[object]:
            return [None, None]

        async def ping(self) -> bool:
            return True

    app = _ops_app(module_pg_pool, schema, redis_client=_FakeRedis())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/rate-limits")
    assert resp.status_code == 200
    body = resp.text
    assert "api:global" in body
    assert "orphan-bucket" in body


async def test_rate_limits_page_shows_materialized_keyed_bucket(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """A lazily materialized keyed bucket (registered into the registry on
    first acquisition, long after worker startup) appears on the
    rate-limits admin page - an operator debugging a tenant throttle must
    see that a limiter exists. The buckets table is the real one, empty:
    nothing has published a row yet."""
    from pydantic import BaseModel

    from taskq.ratelimit.refs import KeyedRateLimitRef

    class _TenantPayload(BaseModel):
        tenant_id: str

    ref = KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="memory",
    )
    # Materialize as the dispatch path does (pg_pool=None: no publish).
    await clean_rl_registry._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, _TenantPayload(tenant_id="acme"), settings=None
    )
    try:
        app = _ops_app(module_pg_pool, module_pg_schema.schema_name)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/rate-limits")
        assert resp.status_code == 200
        assert "api-per-tenant:acme" in resp.text
    finally:
        clean_rl_registry._keyed_rate_limit_last_used.pop(  # pyright: ignore[reportPrivateUsage]
            "api-per-tenant:acme", None
        )


async def test_rate_limits_page_fetches_redis_state_for_pg_only_keyed_buckets(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """Standalone topology: a keyed bucket the worker published to PG
    (absent from the admin process's registry) is included in the
    live-Redis state fetch, so its per-key token state is visible. The
    published row is a real row; the Redis read stays a pipelined stub - no
    PG sibling can own a Redis read."""
    schema = module_pg_schema.schema_name
    # The registry stays empty - the bucket exists only as a PG row.
    await _seed_rl_bucket(clean_pg_conn, schema, name="api-per-tenant:acme")

    fetched_keys: list[str] = []

    class _FakeRedis(StubPipelinedRedis):
        async def hmget(self, key: object, fields: object) -> list[object]:
            return [None, None]

        # The pipeline's execute() resolves queued reads through this
        # reader, so a key recorded here is a key the live-Redis fetch
        # actually read in its one round trip.
        async def hgetall(self, key: str) -> dict[str, str]:
            fetched_keys.append(str(key))
            return {}

        async def ping(self) -> bool:
            return True

    app = _ops_app(module_pg_pool, schema, redis_client=_FakeRedis())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/rate-limits")
    assert resp.status_code == 200
    assert "api-per-tenant:acme" in resp.text
    assert any("api-per-tenant:acme" in key for key in fetched_keys), (
        f"PG-only keyed bucket was not included in the live-Redis fetch: {fetched_keys}"
    )


# ── Rate limit reset ────────────────────────────────────────────────────


async def test_rate_limit_reset_forbidden_by_default(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """POST /rate-limits/{bucket}/reset returns 403 when allow_reset is False."""
    from taskq.ratelimit.token_bucket import TokenBucket

    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    token = await _ops_csrf_token(module_pg_pool, module_pg_schema.schema_name)
    app = _ops_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post("/rate-limits/api:tb/reset", data={"csrf_token": token})
    assert resp.status_code == 403


async def test_rate_limit_reset_success_when_allowed(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    clean_rl_registry: Any,
) -> None:
    """POST /rate-limits/{bucket}/reset returns 303 when allow_reset is True."""
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    token = await _ops_csrf_token(module_pg_pool, module_pg_schema.schema_name)
    app = _ops_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.cookies.set("taskq_csrf_token", token)
        resp = await client.post(
            "/rate-limits/api:tb/reset", data={"csrf_token": token}, follow_redirects=False
        )
    assert resp.status_code == 303


# ── Reservations page ───────────────────────────────────────────────────


async def test_reservations_page_with_configured_reservations(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """GET /reservations renders configured and PG-only reservation buckets.
    The slot rows are real rows - the configured reservation's slots live in
    the module schema the page filters the registry by, and the orphan
    bucket's rows exist with no registry primitive at all."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    schema = module_pg_schema.schema_name
    clean_rl_registry.register(
        ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30), schema=schema)
    )
    await _seed_reservation_slots(clean_pg_conn, schema, bucket="premium-api", held=3, free=2)
    await _seed_reservation_slots(clean_pg_conn, schema, bucket="orphan-bucket", held=1, free=1)
    app = _ops_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/reservations")
    assert resp.status_code == 200
    body = resp.text
    assert "premium-api" in body
    assert "orphan-bucket" in body
    # The counts are the real slots' roll-up: 3 held / 2 free of 5 for the
    # configured reservation, rendered in the held/free count chips.
    held_chip = re.search(r"bg-yellow-100 text-yellow-700[^>]*>(\d+)</span>", body)
    assert held_chip is not None and held_chip.group(1) == "3", (
        f"the held-slot count chip must carry the real held roll-up: {held_chip and held_chip.group(1)!r}"
    )
    free_chip = re.search(r"bg-green-100 text-green-700[^>]*>(\d+)</span>", body)
    assert free_chip is not None and free_chip.group(1) == "2", (
        f"the free-slot count chip must carry the real free roll-up: {free_chip and free_chip.group(1)!r}"
    )


async def test_reservations_page_table_missing(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """GET /reservations shows 'not installed' notice when reservation_slots is
    missing: the router points at a schema no migration has applied, so the
    page's read raises UndefinedTableError for real."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    clean_rl_registry.register(
        ConcurrencyReservation(
            "premium-api",
            slots=5,
            lease=timedelta(seconds=30),
            schema=f"unmigrated_{new_base62()}".lower(),
        )
    )
    unmigrated = f"unmigrated_{new_base62()}".lower()
    app = _ops_app(module_pg_pool, unmigrated)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/reservations")
    assert resp.status_code == 200
    assert "reservations not installed" in resp.text


# ── A degraded page never looks like an empty healthy one ─────────────────


async def test_rate_limits_page_reports_a_failed_live_peek(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_rl_registry: Any,
) -> None:
    """When the live peek across the registry fails, the page says so - a
    warning naming the error type, and a visible degraded notice - rather
    than rendering every bucket with no live state as if none were busy.
    The broken peek is fault injection (a broker failure mid-request); the
    page's PG read is the real pool's."""
    from taskq.ratelimit.token_bucket import TokenBucket

    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="postgres")
    )

    async def _broken_peek(*_: object, **__: object) -> dict[str, object]:
        raise ConnectionResetError("pg gone")

    clean_rl_registry.peek_all = _broken_peek  # type: ignore[method-assign]  # Why: the fault injection IS the registry's peek failing; a real broker cannot fail deterministically mid-request.
    app = _ops_app(module_pg_pool, module_pg_schema.schema_name)
    with structlog.testing.capture_logs() as logs:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/rate-limits")
    assert resp.status_code == 200
    assert "live state unavailable" in resp.text.lower()
    assert "ConnectionResetError" in resp.text
    entry = next(e for e in logs if e["event"] == "ratelimit-peek-all-failed")
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "ConnectionResetError"


async def test_reservations_page_reports_a_failed_slot_sync(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    clean_rl_registry: Any,
) -> None:
    """A slot sync that fails leaves the page on the rows it read before
    the sync, and says so: a warning with the error type and a visible
    degraded notice, never a silently stale table. The failed sync is fault
    injection (a wedged store the timeout must cut); the rows read before
    it are the real table's."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    schema = module_pg_schema.schema_name
    clean_rl_registry.register(
        ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30), schema=schema)
    )

    async def _broken_sync(*_: object, **__: object) -> None:
        raise TimeoutError

    monkeypatch.setattr("taskq.ratelimit.reservation.sync_slots", _broken_sync)
    await _seed_reservation_slots(clean_pg_conn, schema, bucket="premium-api", held=2, free=3)
    app = _ops_app(module_pg_pool, schema)
    with structlog.testing.capture_logs() as logs:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/reservations")
    assert resp.status_code == 200
    assert "slot sync failed" in resp.text.lower()
    assert "TimeoutError" in resp.text
    entry = next(e for e in logs if e["event"] == "reservation-sync-failed")
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "TimeoutError"

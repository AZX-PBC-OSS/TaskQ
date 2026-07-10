"""Tests for schedules, rate-limits, reservations routes/templates, Redis bucket fetch, and XSS escaping."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from asyncpg.exceptions import UndefinedTableError

from taskq.web.admin import create_router
from taskq.web.admin.ops import _fetch_redis_rl_state

from . import StubBackend, StubRecord, _stub_job_row, _StubPool

# ── Schedules, rate-limits, reservations routes: discovery ───────────────


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
        notice_text="cron scheduling not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="reservations not installed — run taskq migrate up to enable",
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
        notice_text="reservations not installed — run taskq migrate up to enable",
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
        notice_text="reservations not installed — run taskq migrate up to enable",
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
        notice_text="rate limiting not installed — run taskq migrate up to enable",
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
        notice_text="reservations not installed — run taskq migrate up to enable",
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


# ── Redis rate-limit state fetch helper ──────────────────────────────────


async def test_fetch_redis_rl_state_returns_none_when_no_redis() -> None:
    """_fetch_redis_rl_state returns None when redis_client is None."""
    result = await _fetch_redis_rl_state(None, "taskq", [])
    assert result is None


async def test_fetch_redis_rl_state_returns_state() -> None:
    """_fetch_redis_rl_state returns dict of Redis state when successful."""

    class _FakeRedis:
        def __init__(self, data: dict[str, dict[str, str]]) -> None:
            self._data = data

        async def hgetall(self, key: str) -> dict[str, str]:
            return self._data.get(key, {})

    names = [("api:global", "token_bucket")]
    redis = _FakeRedis({"taskq:taskq:rl:tb:{api:global}": {"tokens": "95"}})
    result = await _fetch_redis_rl_state(redis, "taskq", names)
    assert result is not None
    assert "api:global" in result
    assert result["api:global"]["tokens"] == "95"


async def test_fetch_redis_rl_state_returns_none_on_failure() -> None:
    """_fetch_redis_rl_state returns None when Redis raises."""

    class _BrokenRedis:
        async def hgetall(self, key: str) -> dict[str, str]:
            raise ConnectionError("redis down")

    names = [("api:global", "token_bucket")]
    result = await _fetch_redis_rl_state(_BrokenRedis(), "taskq", names)
    assert result is None


async def test_fetch_redis_rl_state_uses_sliding_window_key() -> None:
    """_fetch_redis_rl_state uses taskq:{schema}:sw:{name} for sliding_window_log kind."""

    class _FakeRedis:
        def __init__(self, data: dict[str, int]) -> None:
            self._data = data

        async def zcard(self, key: str) -> int:
            return self._data.get(key, 0)

    names = [("my_window", "sliding_window_log")]
    redis = _FakeRedis({"taskq:myschema:sw:{my_window}": 2})
    result = await _fetch_redis_rl_state(redis, "myschema", names)
    assert result is not None
    assert "my_window" in result
    assert result["my_window"]["count"] == "2"


async def test_fetch_redis_rl_state_decodes_bytes() -> None:
    """_fetch_redis_rl_state decodes bytes keys/values from Redis."""

    class _FakeRedis:
        async def hgetall(self, key: str) -> list[tuple[bytes, bytes]]:
            return [(b"tokens", b"10"), (b"last_refill", b"2025-01-01")]

    names = [("api:global", "token_bucket")]
    result = await _fetch_redis_rl_state(_FakeRedis(), "taskq", names)
    assert result is not None
    assert result["api:global"]["tokens"] == "10"


# ── _fetch_redis_rl_state: GCRA kind ─────────────────────────────────────


async def test_fetch_redis_rl_state_gcra() -> None:
    """_fetch_redis_rl_state returns tat for sliding_window_gcra kind."""

    class _FakeRedis:
        async def get(self, key: str) -> str | None:
            return "1234567890.0"

    names = [("my_gcra", "sliding_window_gcra")]
    result = await _fetch_redis_rl_state(_FakeRedis(), "myschema", names)
    assert result is not None
    assert "my_gcra" in result
    assert result["my_gcra"]["tat"] == "1234567890.0"


# ── Route handler tests: configurable stub pool ─────────────────────────


class _StubTxn:
    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, *args: object) -> None:
        pass


class _ScriptedConn:
    """asyncpg connection stub with SQL-substring-matched responses."""

    def __init__(
        self,
        *,
        fetch_map: dict[str, list[StubRecord]] | None = None,
        fetchrow_map: dict[str, StubRecord | None] | None = None,
        execute_map: dict[str, str] | None = None,
        undefined_table_on: set[str] | None = None,
    ) -> None:
        self._fetch_map = fetch_map or {}
        self._fetchrow_map = fetchrow_map or {}
        self._execute_map = execute_map or {}
        self._undef = undefined_table_on or set()

    def _match(self, mapping: dict[str, object], query: str, default: object) -> object:
        for key, value in mapping.items():
            if key in query:
                return value
        return default

    def _check_undef(self, query: str) -> None:
        for pattern in self._undef:
            if pattern in query:
                raise UndefinedTableError()

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        self._check_undef(query)
        return self._match(self._fetch_map, query, [])  # type: ignore[return-value]

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        self._check_undef(query)
        return self._match(self._fetchrow_map, query, None)  # type: ignore[return-value]

    async def fetchval(self, query: str, *args: object) -> object:
        return 0

    async def execute(self, query: str, *args: object) -> str:
        self._check_undef(query)
        return self._match(self._execute_map, query, "UPDATE 0")  # type: ignore[return-value]

    def transaction(self) -> _StubTxn:
        return _StubTxn()


class _ScriptedPool:
    """asyncpg pool stub that always yields the same _ScriptedConn."""

    def __init__(self, conn: _ScriptedConn) -> None:
        self._conn = conn

    def acquire(self) -> object:
        conn = self._conn

        class _Ctx:
            async def __aenter__(self) -> _ScriptedConn:
                return conn

            async def __aexit__(self, *a: object) -> None:
                pass

        return _Ctx()


def _make_client_with_pool(
    pool: _ScriptedPool,
    *,
    backend: StubBackend | None = None,
    redis_client: object | None = None,
) -> Any:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import create_router, setup_admin_state

    kwargs: dict[str, object] = {}
    if backend is not None:
        kwargs["backend"] = backend
    if redis_client is not None:
        kwargs["redis_client"] = redis_client
    bundle = create_router(pool, **kwargs)  # pyright: ignore[reportArgumentType]
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _get_csrf_token(client: Any) -> str:
    """GET /queues to set the CSRF cookie, then return the token."""
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token", "")


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


def test_schedule_enable_redirects_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/enable returns 303 when the update affects one row."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(execute_map={"SET enabled = true": "UPDATE 1"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303


def test_schedule_enable_returns_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/enable returns 404 when no row is updated."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(execute_map={"SET enabled = true": "UPDATE 0"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{uuid4()}/enable", data={"csrf_token": token})
    assert resp.status_code == 404


def test_schedule_enable_redirects_when_table_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/enable redirects with error when cron_schedules is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(undefined_table_on={"cron_schedules"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule disable ────────────────────────────────────────────────────


def test_schedule_disable_redirects_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/disable returns 303 when the update affects one row."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(execute_map={"SET enabled = false": "UPDATE 1"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/disable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303


def test_schedule_disable_returns_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/disable returns 404 when no row is updated."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(execute_map={"SET enabled = false": "UPDATE 0"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{uuid4()}/disable", data={"csrf_token": token})
    assert resp.status_code == 404


def test_schedule_disable_redirects_when_table_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/disable redirects with error when cron_schedules is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(undefined_table_on={"cron_schedules"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/disable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule skip ───────────────────────────────────────────────────────


def test_schedule_skip_redirects_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/skip advances next_fire_at to the future and returns 303."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(
        fetchrow_map={
            "cron_expr, timezone, next_fire_at": StubRecord(
                {
                    "cron_expr": "* * * * *",
                    "timezone": "UTC",
                    "next_fire_at": datetime.now(UTC) - timedelta(minutes=30),
                }
            ),
        },
        execute_map={"SET next_fire_at = $2": "UPDATE 1"},
    )
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/skip", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303


def test_schedule_skip_returns_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/skip returns 404 when the schedule does not exist."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(fetchrow_map={"cron_expr, timezone, next_fire_at": None})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{uuid4()}/skip", data={"csrf_token": token})
    assert resp.status_code == 404


def test_schedule_skip_redirects_when_table_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/skip redirects with error when cron_schedules is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConn(undefined_table_on={"cron_schedules"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/skip", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


# ── Schedule run now ────────────────────────────────────────────────────


def test_schedule_run_now_enqueues_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/run enqueues a job via the backend and returns 303."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(
        fetchrow_map={
            "payload_factory, enabled, metadata": StubRecord(
                {
                    "actor": "test_actor",
                    "payload_factory": None,
                    "enabled": True,
                    "metadata": {"static_payload": {"x": 1}},
                }
            ),
            "actor_config WHERE actor": StubRecord(
                {"queue": "default", "max_attempts": 3, "retry_kind": "transient"}
            ),
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/run", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert len(backend.enqueue_calls) == 1


def test_schedule_run_now_returns_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/run returns 404 when the schedule does not exist."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(fetchrow_map={"payload_factory, enabled, metadata": None})
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{uuid4()}/run", data={"csrf_token": token})
    assert resp.status_code == 404


def test_schedule_run_now_redirects_when_table_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/run redirects with error when cron_schedules is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(undefined_table_on={"cron_schedules"})
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/run", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "cron+scheduling+not+installed" in resp.headers.get("location", "")


def test_schedule_run_now_redirects_on_payload_type_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST /schedules/{id}/run redirects when the payload factory returns a non-dict type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(
        fetchrow_map={
            "payload_factory, enabled, metadata": StubRecord(
                {
                    "actor": "test_actor",
                    "payload_factory": "os.getpid",
                    "enabled": True,
                    "metadata": {},
                }
            ),
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/run", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "factory+returned+unexpected+type" in resp.headers.get("location", "")


def test_schedule_run_now_redirects_on_payload_factory_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /schedules/{id}/run redirects when the payload factory import fails."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(
        fetchrow_map={
            "payload_factory, enabled, metadata": StubRecord(
                {
                    "actor": "test_actor",
                    "payload_factory": "nonexistent_pkg_xyz.func",
                    "enabled": True,
                    "metadata": {},
                }
            ),
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/run", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert len(backend.enqueue_calls) == 0


def test_schedule_run_now_redirects_when_actor_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /schedules/{id}/run redirects when the actor is not in actor_config."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    conn = _ScriptedConn(
        fetchrow_map={
            "payload_factory, enabled, metadata": StubRecord(
                {
                    "actor": "unconfigured_actor",
                    "payload_factory": None,
                    "enabled": True,
                    "metadata": {},
                }
            ),
            "actor_config WHERE actor": None,
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{uuid4()}/run", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "not+configured" in resp.headers.get("location", "")
    assert len(backend.enqueue_calls) == 0


# ── Rate limits page with registered primitives ─────────────────────────


def test_rate_limits_page_with_registered_primitives(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """GET /rate-limits renders TokenBucket and SlidingWindow from the registry."""
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    clean_rl_registry.register(
        SlidingWindow(
            "api:sw", limit=5, window=timedelta(seconds=10), backend="memory", style="log"
        )
    )
    conn = _ScriptedConn(
        fetch_map={
            "rate_limit_buckets ORDER": [
                StubRecord(
                    {
                        "bucket_name": "api:tb",
                        "kind": "token_bucket",
                        "state": '{"tokens":5}',
                        "updated_at": "2025-01-01",
                    }
                ),
            ],
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200
    body = resp.text
    assert "api:tb" in body
    assert "api:sw" in body


def test_rate_limits_page_table_missing(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """GET /rate-limits shows 'not installed' notice when rate_limit_buckets is missing."""
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    conn = _ScriptedConn(undefined_table_on={"rate_limit_buckets"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200
    assert "rate limiting not installed" in resp.text


def test_rate_limits_page_with_pg_state_and_redis(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """GET /rate-limits renders PG state, live states, and PG-only buckets with Redis."""
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        TokenBucket("api:global", capacity=10, refill_per_second=1, backend="redis")
    )
    conn = _ScriptedConn(
        fetch_map={
            "rate_limit_buckets ORDER": [
                StubRecord(
                    {
                        "bucket_name": "api:global",
                        "kind": "token_bucket",
                        "state": '{"tokens":5}',
                        "updated_at": "2025-01-01T00:00:00+00:00",
                    }
                ),
                StubRecord(
                    {
                        "bucket_name": "orphan-bucket",
                        "kind": "token_bucket",
                        "state": "",
                        "updated_at": "",
                    }
                ),
            ],
        },
    )

    class _FakeRedis:
        async def hmget(self, key: object, fields: object) -> list[object]:
            return [None, None]

        async def hgetall(self, key: object) -> dict[str, str]:
            return {}

        async def get(self, key: object) -> str | None:
            return None

        async def zcard(self, key: object) -> int:
            return 0

        async def ping(self) -> bool:
            return True

    client = _make_client_with_pool(_ScriptedPool(conn), redis_client=_FakeRedis())
    resp = client.get("/rate-limits")
    assert resp.status_code == 200
    body = resp.text
    assert "api:global" in body
    assert "orphan-bucket" in body


# ── Rate limit reset ────────────────────────────────────────────────────


def test_rate_limit_reset_forbidden_by_default(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """POST /rate-limits/{bucket}/reset returns 403 when allow_reset is False."""
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    conn = _ScriptedConn()
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post("/rate-limits/api:tb/reset", data={"csrf_token": token})
    assert resp.status_code == 403


def test_rate_limit_reset_success_when_allowed(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """POST /rate-limits/{bucket}/reset returns 303 when allow_reset is True."""
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    clean_rl_registry.register(
        TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory")
    )
    conn = _ScriptedConn()
    client = _make_client_with_pool(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        "/rate-limits/api:tb/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303


# ── Reservations page ───────────────────────────────────────────────────


def test_reservations_page_with_configured_reservations(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """GET /reservations renders configured and PG-only reservation buckets."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30), schema="taskq")
    )
    conn = _ScriptedConn(
        fetch_map={
            "GROUP BY bucket_name": [
                StubRecord(
                    {
                        "bucket_name": "premium-api",
                        "held_count": 3,
                        "free_count": 2,
                        "total_slots": 5,
                    }
                ),
                StubRecord(
                    {
                        "bucket_name": "orphan-bucket",
                        "held_count": 1,
                        "free_count": 1,
                        "total_slots": 2,
                    }
                ),
            ],
            "job_id IS NOT NULL": [
                StubRecord(
                    {
                        "bucket_name": "premium-api",
                        "slot_index": 0,
                        "job_id": "abc-123",
                        "held_by_worker_id": "worker-1",
                        "lease_expires_at": "2025-01-01T00:01:00+00:00",
                    }
                ),
            ],
            "slot_index FROM": [],
        },
    )
    client = _make_client_with_pool(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200
    body = resp.text
    assert "premium-api" in body
    assert "orphan-bucket" in body


def test_reservations_page_table_missing(
    monkeypatch: pytest.MonkeyPatch, clean_rl_registry: Any
) -> None:
    """GET /reservations shows 'not installed' notice when reservation_slots is missing."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    clean_rl_registry.register(
        ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30), schema="taskq")
    )
    conn = _ScriptedConn(undefined_table_on={"reservation_slots"})
    client = _make_client_with_pool(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200
    assert "reservations not installed" in resp.text

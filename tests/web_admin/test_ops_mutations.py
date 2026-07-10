"""Tests for schedules/rate-limits/reservations mutating routes in ops.py.

Covers success, not-found, UndefinedTableError, and validation-error branches
for schedule enable/disable/skip/run, rate-limit reset, and the rate-limits /
reservations pages — using a scripted asyncpg pool/connection stub so we can
control fetch/fetchrow/execute results and force errors on demand.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from asyncpg.exceptions import UndefinedTableError

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.ratelimit.decision import RateLimitState
from taskq.ratelimit.registry import registry as rl_registry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.web.admin.ops import _fetch_redis_rl_state

from . import StubBackend, StubConnection, StubRecord, _stub_job_row


def _get_csrf_token(client: Any) -> str:
    """GET the queues page to set the CSRF cookie, then return the token value."""
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token", "")


class _ScriptedConnection(StubConnection):
    """Connection stub whose fetch/fetchrow/execute results are pre-scripted."""

    def __init__(
        self,
        *,
        fetch_results: list[list[StubRecord]] | None = None,
        fetchrow_results: list[StubRecord | None] | None = None,
        execute_results: list[str] | None = None,
        raise_on: dict[str, Exception] | None = None,
    ) -> None:
        self._fetch_results = list(fetch_results or [])
        self._fetchrow_results = list(fetchrow_results or [])
        self._execute_results = list(execute_results or [])
        self._raise_on = raise_on or {}

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if "fetch" in self._raise_on:
            raise self._raise_on["fetch"]
        return self._fetch_results.pop(0) if self._fetch_results else []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        if "fetchrow" in self._raise_on:
            raise self._raise_on["fetchrow"]
        return self._fetchrow_results.pop(0) if self._fetchrow_results else None

    async def execute(self, query: str, *args: object) -> str:
        if "execute" in self._raise_on:
            raise self._raise_on["execute"]
        return self._execute_results.pop(0) if self._execute_results else "UPDATE 1"


class _AcquireCtx:
    def __init__(self, conn: _ScriptedConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ScriptedConnection:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _ScriptedPool:
    """Pool stub that always yields the same scripted connection on acquire()."""

    def __init__(self, conn: _ScriptedConnection) -> None:
        self._conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


def _make_app(pool: Any, **kwargs: Any) -> Any:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import create_router, setup_admin_state

    bundle = create_router(pool, **kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


# ── Schedule enable ──────────────────────────────────────────────────────


def test_schedule_enable_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(execute_results=["UPDATE 1"])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert resp.headers["location"].endswith("/schedules")  # pyright: ignore[reportUnknownMemberType]


def test_schedule_enable_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(execute_results=["UPDATE 0"])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/enable", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]


def test_schedule_enable_redirects_when_cron_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(raise_on={"execute": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "error=" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]


# ── Schedule disable ─────────────────────────────────────────────────────


def test_schedule_disable_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(execute_results=["UPDATE 1"])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/disable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]


def test_schedule_disable_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(execute_results=["UPDATE 0"])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/disable", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]


def test_schedule_disable_redirects_when_cron_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(raise_on={"execute": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/disable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "error=" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]


# ── Schedule skip ────────────────────────────────────────────────────────


def test_schedule_skip_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    past = datetime.now(UTC) - timedelta(minutes=2)
    row = StubRecord(cron_expr="* * * * *", timezone="UTC", next_fire_at=past)
    conn = _ScriptedConnection(fetchrow_results=[row])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/skip", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert resp.headers["location"].endswith("/schedules")  # pyright: ignore[reportUnknownMemberType]


def test_schedule_skip_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(fetchrow_results=[None])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/skip", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]


def test_schedule_skip_redirects_when_cron_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(raise_on={"fetchrow": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/skip", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "error=" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]


# ── Schedule run-now ─────────────────────────────────────────────────────


def test_schedule_run_now_redirects_when_cron_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(raise_on={"fetchrow": UndefinedTableError("missing")})
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "error=" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]


def test_schedule_run_now_404_when_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(fetchrow_results=[None])
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token})
    assert resp.status_code == 404  # pyright: ignore[reportUnknownMemberType]


def test_schedule_run_now_redirects_on_factory_type_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A resolve_payload TypeError (factory returned wrong type) redirects with an error."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    async def _raise_type_error(
        payload_factory: str | None, raw_metadata: object
    ) -> dict[str, object]:
        raise TypeError("factory returned unexpected type")

    monkeypatch.setattr("taskq.web.admin.ops.resolve_payload", _raise_type_error)

    row = StubRecord(actor="cleanup", payload_factory="pkg.factory", enabled=True, metadata={})
    conn = _ScriptedConnection(fetchrow_results=[row])
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "factory+returned+unexpected+type" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 0


def test_schedule_run_now_redirects_on_factory_generic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generic exception from resolve_payload redirects with a generic error code."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    async def _raise_value_error(
        payload_factory: str | None, raw_metadata: object
    ) -> dict[str, object]:
        raise ValueError("boom")

    monkeypatch.setattr("taskq.web.admin.ops.resolve_payload", _raise_value_error)

    row = StubRecord(actor="cleanup", payload_factory="pkg.factory", enabled=True, metadata={})
    conn = _ScriptedConnection(fetchrow_results=[row])
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "payload+factory+error" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 0


def test_schedule_run_now_redirects_when_actor_not_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row = StubRecord(
        actor="cleanup", payload_factory=None, enabled=True, metadata={"static_payload": {}}
    )
    conn = _ScriptedConnection(fetchrow_results=[schedule_row, None])
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert "not+configured" in resp.headers["location"]  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 0


def test_schedule_run_now_succeeds_and_enqueues(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    schedule_row = StubRecord(
        actor="cleanup", payload_factory=None, enabled=True, metadata={"static_payload": {"x": 1}}
    )
    actor_config_row = StubRecord(queue="default", max_attempts=3, retry_kind="transient")
    conn = _ScriptedConnection(fetchrow_results=[schedule_row, actor_config_row])
    backend = StubBackend(job_row=_stub_job_row(uuid4()))
    client = _make_app(_ScriptedPool(conn), backend=backend)
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/run", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert resp.headers["location"].endswith("/schedules")  # pyright: ignore[reportUnknownMemberType]
    assert len(backend.enqueue_calls) == 1
    assert backend.enqueue_calls[0].actor == "cleanup"
    assert backend.enqueue_calls[0].payload == {"x": 1}


# ── Rate-limit reset ─────────────────────────────────────────────────────


def test_rate_limit_reset_forbidden_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset is disabled by default (admin_ui_allow_rate_limit_reset=False)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection()
    client = _make_app(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post("/rate-limits/api%3Aglobal/reset", data={"csrf_token": token})
    assert resp.status_code == 403  # pyright: ignore[reportUnknownMemberType]


def test_rate_limit_reset_succeeds_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """When enabled and the bucket is memory-backed, reset succeeds and redirects."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="memory")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})
    conn = _ScriptedConnection()
    client = _make_app(_ScriptedPool(conn))
    token = _get_csrf_token(client)
    resp = client.post(
        "/rate-limits/api%3Aglobal/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    assert resp.headers["location"].endswith("/rate-limits")  # pyright: ignore[reportUnknownMemberType]


# ── Rate-limits page ─────────────────────────────────────────────────────


def test_rate_limits_page_renders_with_no_configured_buckets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /rate-limits with an empty registry and empty PG rows renders successfully."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(rl_registry, "_rate_limits", {})
    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /rate-limits shows the not-installed notice when the table is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(rl_registry, "_rate_limits", {})
    conn = _ScriptedConnection(raise_on={"fetch": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "rate limiting not installed" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_merges_pg_state_and_extra_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured bucket merges PG state; a PG-only bucket (not in registry) is appended."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="memory")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})
    configured_row = StubRecord(
        bucket_name="api:global", kind="token_bucket", state="{}", updated_at=""
    )
    orphan_row = StubRecord(
        bucket_name="legacy-bucket", kind="token_bucket", state="{}", updated_at=""
    )
    conn = _ScriptedConnection(fetch_results=[[configured_row, orphan_row]])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "api:global" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "legacy-bucket" in resp.text  # pyright: ignore[reportUnknownMemberType]


# ── Reservations page ────────────────────────────────────────────────────


def test_reservations_page_renders_with_no_configured_reservations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(rl_registry, "_reservations", {})
    conn = _ScriptedConnection(fetch_results=[[], []])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_reservations_page_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setattr(rl_registry, "_reservations", {})
    conn = _ScriptedConnection(raise_on={"fetch": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "reservations not installed" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_reservations_page_merges_pg_state_and_extra_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured reservation merges PG counts; a PG-only bucket is appended."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    reservation = ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30))
    monkeypatch.setattr(rl_registry, "_reservations", {"premium-api": reservation})

    configured_row = StubRecord(
        bucket_name="premium-api", held_count=2, free_count=3, total_slots=5
    )
    orphan_row = StubRecord(bucket_name="legacy-bucket", held_count=0, free_count=1, total_slots=1)

    conn = _ScriptedConnection(fetch_results=[[configured_row, orphan_row], []])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "premium-api" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "legacy-bucket" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_reservations_page_defaults_when_no_pg_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """A configured reservation with no matching PG row gets held=0/free=configured_slots."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    reservation = ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30))
    monkeypatch.setattr(rl_registry, "_reservations", {"premium-api": reservation})
    conn = _ScriptedConnection(fetch_results=[[], []])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "premium-api" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_reservations_page_sync_slots_success_refetches(monkeypatch: pytest.MonkeyPatch) -> None:
    """When sync_slots succeeds, the reservations/held-slots rows are re-fetched."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    reservation = ConcurrencyReservation("premium-api", slots=5, lease=timedelta(seconds=30))
    monkeypatch.setattr(rl_registry, "_reservations", {"premium-api": reservation})

    async def _fake_sync_slots(reservations: object, pool: object, *, schema: str) -> None:
        return None

    monkeypatch.setattr("taskq.ratelimit.reservation.sync_slots", _fake_sync_slots)

    first_row = StubRecord(bucket_name="premium-api", held_count=0, free_count=5, total_slots=5)
    refetched_row = StubRecord(bucket_name="premium-api", held_count=1, free_count=4, total_slots=5)
    conn = _ScriptedConnection(fetch_results=[[first_row], [], [refetched_row], []])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/reservations")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "premium-api" in resp.text  # pyright: ignore[reportUnknownMemberType]


# ── Schedules page: UndefinedTableError branch ──────────────────────────


def test_schedules_page_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /schedules shows the not-installed notice when cron_schedules is missing."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    conn = _ScriptedConnection(raise_on={"fetch": UndefinedTableError("missing")})
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/schedules")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "cron scheduling not installed" in resp.text  # pyright: ignore[reportUnknownMemberType]


# ── Schedule skip: cron expression never reaches a future fire time ─────


def test_schedule_skip_400_when_no_future_fire_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """The for/else 400 branch fires when compute_next_fire_after never advances past now."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    stale = datetime(2000, 1, 1, tzinfo=UTC)

    def _fake_compute_next_fire_after(
        cron_expr: str, timezone_name: str, after: datetime, dst_strategy: str = "skip"
    ) -> list[datetime]:
        return [stale]

    monkeypatch.setattr(
        "taskq.web.admin.ops.compute_next_fire_after", _fake_compute_next_fire_after
    )
    row = StubRecord(cron_expr="* * * * *", timezone="UTC", next_fire_at=stale)
    conn = _ScriptedConnection(fetchrow_results=[row])
    client = _make_app(_ScriptedPool(conn))
    sid = uuid4()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/skip", data={"csrf_token": token})
    assert resp.status_code == 400  # pyright: ignore[reportUnknownMemberType]


# ── _fetch_redis_rl_state: remaining branches ────────────────────────────


async def test_fetch_redis_rl_state_token_bucket_empty_raw_skips() -> None:
    """An empty hgetall result for a token bucket does not populate the result dict."""

    class _FakeRedis:
        async def hgetall(self, key: str) -> dict[str, str]:
            return {}

    result = await _fetch_redis_rl_state(_FakeRedis(), "taskq", [("api:global", "token_bucket")])
    assert result == {}


async def test_fetch_redis_rl_state_gcra_present() -> None:
    """sliding_window_gcra kind reads the TAT value via GET."""

    class _FakeRedis:
        async def get(self, key: str) -> bytes:
            return b"12345.0"

    result = await _fetch_redis_rl_state(
        _FakeRedis(), "taskq", [("my_gcra", "sliding_window_gcra")]
    )
    assert result is not None
    assert result["my_gcra"]["tat"] == "12345.0"


async def test_fetch_redis_rl_state_gcra_absent() -> None:
    """sliding_window_gcra kind with no TAT key present does not populate the result."""

    class _FakeRedis:
        async def get(self, key: str) -> None:
            return None

    result = await _fetch_redis_rl_state(
        _FakeRedis(), "taskq", [("my_gcra", "sliding_window_gcra")]
    )
    assert result == {}


async def test_fetch_redis_rl_state_sliding_window_log_zero_count_skips() -> None:
    """A zero ZCARD count for sliding_window_log does not populate the result."""

    class _FakeRedis:
        async def zcard(self, key: str) -> int:
            return 0

    result = await _fetch_redis_rl_state(
        _FakeRedis(), "taskq", [("my_window", "sliding_window_log")]
    )
    assert result == {}


async def test_fetch_redis_rl_state_unknown_kind_is_skipped() -> None:
    """An unrecognized kind falls through the if/elif chain without action."""

    class _FakeRedis:
        pass

    result = await _fetch_redis_rl_state(_FakeRedis(), "taskq", [("mystery", "unknown_kind")])
    assert result == {}


# ── Rate-limits page: sliding-window / unknown-kind / redis-backend branches ──


def test_rate_limits_page_renders_sliding_window_and_unknown_kind_buckets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry entries that are SlidingWindow or duck-typed unknown kinds render."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    sw = SlidingWindow("api:window", limit=10, window=timedelta(seconds=60), backend="redis")

    class _UnknownPrimitive:
        backend = "memory"

    monkeypatch.setattr(
        rl_registry,
        "_rate_limits",
        {"api:window": sw, "api:mystery": _UnknownPrimitive()},
    )
    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "api:window" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "api:mystery" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_live_states_populate_all_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """peek_all results with retry_after/capacity/limit/window/style/refill populate live_states."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="memory")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})

    fake_state = RateLimitState(
        bucket_name="api:global",
        backend="memory",
        is_exhausted=True,
        tokens_remaining=0.0,
        remaining=0.0,
        retry_after=timedelta(seconds=5),
        capacity=3.0,
        limit=10,
        window=timedelta(seconds=60),
        style="log",
        refill_per_second=1.0,
    )

    fake_state_no_capacity = RateLimitState(
        bucket_name="api:window",
        backend="memory",
        is_exhausted=False,
        tokens_remaining=0.0,
        remaining=5.0,
        retry_after=None,
        capacity=None,
        limit=None,
        window=None,
        style=None,
        refill_per_second=None,
    )

    async def _fake_peek_all(
        *, redis_client: object, pg_pool: object, clock: object, settings: object
    ) -> dict[str, RateLimitState]:
        return {"api:global": fake_state, "api:window": fake_state_no_capacity}

    monkeypatch.setattr(rl_registry, "peek_all", _fake_peek_all)
    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_peek_all_failure_is_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A peek_all failure is caught and logged; the page still renders."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="memory")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})

    async def _fake_peek_all_raises(
        *, redis_client: object, pg_pool: object, clock: object, settings: object
    ) -> dict[str, RateLimitState]:
        raise RuntimeError("peek failed")

    monkeypatch.setattr(rl_registry, "peek_all", _fake_peek_all_raises)
    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn))
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_with_redis_client_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """When a redis_client is configured, redis_available/redis_state are populated."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="redis")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})

    class _FakeRedis:
        async def hgetall(self, key: str) -> dict[str, str]:
            return {"tokens": "2"}

    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn), redis_client=_FakeRedis())
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Redis State" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_rate_limits_page_redis_configured_but_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """redis_available flips back to False when _fetch_redis_rl_state returns None."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bucket = TokenBucket("api:global", capacity=3, refill_per_second=1.0, backend="redis")
    monkeypatch.setattr(rl_registry, "_rate_limits", {"api:global": bucket})

    async def _fake_fetch_redis_rl_state(redis_client: object, schema: str, names: object) -> None:
        return None

    monkeypatch.setattr("taskq.web.admin.ops._fetch_redis_rl_state", _fake_fetch_redis_rl_state)

    class _FakeRedis:
        pass

    conn = _ScriptedConnection(fetch_results=[[]])
    client = _make_app(_ScriptedPool(conn), redis_client=_FakeRedis())
    resp = client.get("/rate-limits")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Redis state unavailable" in resp.text  # pyright: ignore[reportUnknownMemberType]

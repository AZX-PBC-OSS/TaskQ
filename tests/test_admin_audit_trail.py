"""End-to-end tests for the admin audit trail (issue #324).

Real Postgres (module-scoped migrated schema): an authenticated operator
(cancel, schedule enable, actor deregister) leaves one ``admin_audit`` row
per mutation with the principal from the auth dependency's ``IdentityClaims``;
the cancel folds the principal into the job's ``cancel_request`` event;
the disabled (403) paths write nothing; the no-auth dev router records
``anonymous``; and the job detail page renders the entries.

RED evidence (pre-fix tree): every assertion below that reads
``admin_audit`` fails on the un-fixed tree -- the table does not exist,
which is exactly the defect: no audit trail.
"""

import uuid
from datetime import timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.actor_config import ActorConfig
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import (
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    DEFAULT_MAX_RETRY_BACKOFF,
    MAX_RESULT_BYTES,
)
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth import IdentityClaims
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

_CLAIMS = IdentityClaims(subject="ops-admin@example.com", email=None, groups=frozenset(), raw={})


def _fixed_auth() -> Any:
    async def _dependency() -> IdentityClaims:
        return _CLAIMS

    return _dependency


# Module-level singleton default (B008): a default of None means the
# no-auth DEV router below, so "not provided" needs its own sentinel value.
_AUTHENTICATED = _fixed_auth()


class _TestBackendSettings:
    """Satisfies the declared ``BackendSettings`` protocol (mirrors
    test_web_admin_integration.py's double)."""

    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = MAX_RESULT_BYTES
    event_writer_batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE
    event_writer_statement_timeout_ms: float = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0
    idempotency_lock_timeout_ms: float = 5000.0
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _TestBackendDeps:
    settings: _TestBackendSettings
    worker_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    dispatcher_pool: asyncpg.Pool | None = None

    def __init__(self, schema: str, pool: asyncpg.Pool) -> None:
        self.settings = _TestBackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


def _make_backend(pool: asyncpg.Pool, schema: str) -> PostgresBackend:
    return PostgresBackend(
        _TestBackendDeps(schema, pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double declares every field the routes read; mirrors test_web_admin_integration.py.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )


def _make_admin_app(
    pool: asyncpg.Pool,
    schema: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    admin_actions_enabled: bool = True,
    auth_dependency: Any = _AUTHENTICATED,
) -> FastAPI:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true" if admin_actions_enabled else "false")
    # httpx will not store a Secure cookie delivered over http://testserver;
    # without this the CSRF cookie never reaches the jar and every POST 403s.
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    backend = _make_backend(pool, schema)
    bundle = create_router(
        pool,
        schema=schema,
        base_path="/admin",
        backend=backend,
        auth_dependency=auth_dependency,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    return app


async def _get_csrf_then_post(
    app: FastAPI,
    get_url: str,
    post_url: str,
    data: dict[str, str] | None = None,
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        get_resp = await client.get(get_url)
        assert get_resp.status_code == 200
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        assert csrf_token, "GET must set the taskq_csrf_token cookie"
        return await client.post(post_url, data={"csrf_token": csrf_token, **(data or {})})


async def _seed_pending_job(conn: asyncpg.Connection, schema: str, job_id: uuid.UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind) "
        f"VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, "
        f"'pending'::\"{schema}\".job_status, 3, 'transient')",
        job_id,
    )


async def _seed_schedule(conn: asyncpg.Connection, schema: str, schedule_id: uuid.UUID) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, cron_expr, enabled, next_fire_at) "
        "VALUES ($1, 'sched-actor', '* * * * *', false, clock_timestamp())",
        schedule_id,
    )


async def _audit_rows(conn: asyncpg.Connection, schema: str) -> list[asyncpg.Record]:
    return await conn.fetch(f'SELECT * FROM "{schema}".admin_audit ORDER BY id')


async def test_cancel_by_authenticated_principal_writes_audit_row_and_folds_into_event(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await _seed_pending_job(clean_pg_conn, schema, jid)

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(
        app,
        "/admin/jobs",
        f"/admin/jobs/{jid}/cancel",
        data={"reason": "duplicate run"},
    )
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    row = rows[0]
    assert row["principal_subject"] == "ops-admin@example.com"
    assert row["action"] == "job.cancel"
    assert row["target_type"] == "job"
    assert row["target_id"] == str(jid)
    assert row["reason"] == "duplicate run"

    # The per-job event log carries the principal too: the newest
    # cancel_request event's detail folds it in next to the reason.
    detail: dict[str, Any] = await clean_pg_conn.fetchval(
        f'SELECT detail FROM "{schema}".job_events '
        "WHERE job_id = $1 AND kind = 'cancel_request' "
        "ORDER BY id DESC LIMIT 1",
        jid,
    )
    detail_obj = _json_loads(detail) if isinstance(detail, str) else detail
    assert detail_obj["reason"] == "duplicate run"
    assert detail_obj["principal_subject"] == "ops-admin@example.com"


async def test_schedule_enable_writes_audit_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    sid = new_uuid()
    await _seed_schedule(clean_pg_conn, schema, sid)

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(app, "/admin/schedules", f"/admin/schedules/{sid}/enable")
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["principal_subject"] == "ops-admin@example.com"
    assert rows[0]["action"] == "schedule.enable"
    assert rows[0]["target_type"] == "schedule"
    assert rows[0]["target_id"] == str(sid)

    # Same transaction: the mutation actually applied.
    enabled: bool | None = await clean_pg_conn.fetchval(
        f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert enabled is True


async def test_actor_deregister_writes_audit_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="audit-dereg-actor", max_concurrent=1, queue="default")],
        schema=schema,
    )

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(
        app,
        "/admin/actors",
        "/admin/actors/audit-dereg-actor/deregister",
        data={"purge_queue": "true"},
    )
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["principal_subject"] == "ops-admin@example.com"
    assert rows[0]["action"] == "actor.deregister"
    assert rows[0]["target_type"] == "actor"
    assert rows[0]["target_id"] == "audit-dereg-actor"
    detail_obj = (
        _json_loads(rows[0]["detail"]) if isinstance(rows[0]["detail"], str) else rows[0]["detail"]
    )
    assert detail_obj["purge_queue"] is True


async def test_disabled_actions_write_nothing(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """admin_actions_enabled=false refuses the mutation with 403 and writes
    no audit row (and no half-mutation either)."""
    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await _seed_pending_job(clean_pg_conn, schema, jid)
    sid = new_uuid()
    await _seed_schedule(clean_pg_conn, schema, sid)

    app = _make_admin_app(module_pg_pool, schema, monkeypatch, admin_actions_enabled=False)
    cancel_resp = await _get_csrf_then_post(
        app, "/admin/jobs", f"/admin/jobs/{jid}/cancel", data={"reason": "nope"}
    )
    assert cancel_resp.status_code == 403
    enable_resp = await _get_csrf_then_post(
        app, "/admin/schedules", f"/admin/schedules/{sid}/enable"
    )
    assert enable_resp.status_code == 403

    assert await _audit_rows(clean_pg_conn, schema) == []
    status: str | None = await clean_pg_conn.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', jid
    )
    assert status == "pending"


async def test_dev_mode_records_anonymous(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dev router (auth_dependency=None) records the explicit
    'anonymous' principal rather than crashing on the missing claims."""
    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await _seed_pending_job(clean_pg_conn, schema, jid)

    app = _make_admin_app(module_pg_pool, schema, monkeypatch, auth_dependency=None)
    resp = await _get_csrf_then_post(
        app, "/admin/jobs", f"/admin/jobs/{jid}/cancel", data={"reason": "dev cancel"}
    )
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["principal_subject"] == "anonymous"
    assert rows[0]["action"] == "job.cancel"


async def test_job_detail_page_renders_audit_entries(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await _seed_pending_job(clean_pg_conn, schema, jid)

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(
        app,
        "/admin/jobs",
        f"/admin/jobs/{jid}/cancel",
        data={"reason": "operator asked"},
    )
    assert resp.status_code == 303

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        page = await client.get(f"/admin/jobs/{jid}")

    assert page.status_code == 200
    assert "Admin Audit Trail" in page.text
    assert "ops-admin@example.com" in page.text
    assert "job.cancel" in page.text
    assert "operator asked" in page.text


async def test_retry_writes_audit_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        " started_at, finished_at, error_class, error_message) "
        f"VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, "
        f"'failed'::\"{schema}\".job_status, 3, 'transient', "
        "clock_timestamp(), clock_timestamp(), 'ValueError', 'boom')",
        jid,
    )

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(app, "/admin/jobs", f"/admin/jobs/{jid}/retry")
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    assert rows[0]["principal_subject"] == "ops-admin@example.com"
    assert rows[0]["action"] == "job.retry"
    assert rows[0]["target_id"] == str(jid)

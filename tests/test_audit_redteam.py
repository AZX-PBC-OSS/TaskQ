"""End-to-end red-team pins for the admin audit trail (attack review of
#324's fix), against a real migrated Postgres.

These attack the fix for what it missed, at the transaction boundary the
unit stubs cannot reach:

* same-transaction semantics (attack 5): an audit insert that fails AFTER
  the mutation's own statement must roll the mutation back (fail-closed),
  and a COMMIT that fails after both statements succeeded must roll BOTH
  back -- pinned with test-local triggers, one immediate (statement-time
  failure) and one deferred (commit-time failure);
* principal integrity (attack 2): a hostile custom auth dependency whose
  subject carries control characters and unbounded length must still
  produce a bounded, control-free audit row AND a successful mutation;
* purge scope (attack 3): the destructive multi-row queue purge inside
  ``purge_queue=true`` deregistration is audited with its scope (which
  queue, how many rows), not just a boolean flag;
* retry attribution (attack 4): ``retry_job`` writes no job_events row,
  so the admin_audit row is the retry's only attribution -- pinned so a
  future event-writing retry knows it must carry the principal too.
"""

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
    test_admin_audit_trail.py's double)."""

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
        _TestBackendDeps(schema, pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double declares every field the routes read; mirrors test_admin_audit_trail.py.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )


def _make_admin_app(
    pool: asyncpg.Pool,
    schema: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    auth_dependency: Any = _AUTHENTICATED,
) -> FastAPI:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
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
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        get_resp = await client.get(get_url)
        assert get_resp.status_code == 200
        csrf_token = get_resp.cookies.get("taskq_csrf_token", "")
        assert csrf_token, "GET must set the taskq_csrf_token cookie"
        return await client.post(post_url, data={"csrf_token": csrf_token, **(data or {})})


async def _seed_schedule(conn: asyncpg.Connection, schema: str, schedule_id: object) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, cron_expr, enabled, next_fire_at) "
        "VALUES ($1, 'sched-actor', '* * * * *', false, clock_timestamp())",
        schedule_id,
    )


async def _audit_rows(conn: asyncpg.Connection, schema: str) -> list[asyncpg.Record]:
    return await conn.fetch(f'SELECT * FROM "{schema}".admin_audit ORDER BY id')


# ── Attack 5: same-transaction semantics are fail-closed ─────────────────


_AUDIT_BOMB_FN = """
CREATE OR REPLACE FUNCTION {schema}.audit_bomb() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit bomb: the audit insert must fail the transaction';
    RETURN NULL;
END
$$ LANGUAGE plpgsql
"""


async def test_audit_insert_failure_rolls_back_the_mutation(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Statement-time failure: the schedule's ENABLE applied, then the
    same-transaction audit INSERT blew up. The mutation must roll back
    WITH the audit row: a schedule that flips enabled while the record of
    who flipped it vanishes is the unattributable-state defect. This is
    the fail-closed answer, pinned against a real transaction."""
    schema = module_pg_schema.schema_name
    sid = new_uuid()
    await _seed_schedule(clean_pg_conn, schema, sid)

    # The bomb: any schedule.enable audit insert fails at statement time.
    await clean_pg_conn.execute(_AUDIT_BOMB_FN.format(schema=schema))
    await clean_pg_conn.execute(
        f'CREATE TRIGGER audit_bomb_stmt BEFORE INSERT ON "{schema}".admin_audit '
        "FOR EACH ROW WHEN (NEW.action = 'schedule.enable') "
        f"EXECUTE FUNCTION {schema}.audit_bomb()"
    )

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(app, "/admin/schedules", f"/admin/schedules/{sid}/enable")
    assert resp.status_code == 500  # the failure must NOT be laundered into success

    enabled: bool | None = await clean_pg_conn.fetchval(
        f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert enabled is False, "the enable must roll back with its failed audit insert"
    assert await _audit_rows(clean_pg_conn, schema) == []

    await clean_pg_conn.execute(f'DROP TRIGGER audit_bomb_stmt ON "{schema}".admin_audit')

    # And the route recovers: the same mutation succeeds once the audit
    # insert can land again (the failure was the trigger, not the route).
    resp = await _get_csrf_then_post(app, "/admin/schedules", f"/admin/schedules/{sid}/enable")
    assert resp.status_code == 303
    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1 and rows[0]["action"] == "schedule.enable"


async def test_commit_failure_after_successful_audit_insert_rolls_back_both(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Commit-time failure: BOTH statements (the mutation and the audit
    insert) succeeded inside the transaction, then COMMIT itself failed.
    PostgreSQL commits atomically, so both roll back -- the audit row
    cannot survive a mutation it describes, and vice versa. Pinned with a
    DEFERRABLE INITIALLY DEFERRED constraint trigger, whose constraint
    check runs exactly at COMMIT."""
    schema = module_pg_schema.schema_name
    sid = new_uuid()
    await _seed_schedule(clean_pg_conn, schema, sid)

    await clean_pg_conn.execute(_AUDIT_BOMB_FN.format(schema=schema))
    await clean_pg_conn.execute(
        f'CREATE CONSTRAINT TRIGGER audit_bomb_commit AFTER INSERT ON "{schema}".admin_audit '
        "DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW WHEN (NEW.action = 'schedule.enable') "
        f"EXECUTE FUNCTION {schema}.audit_bomb()"
    )

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(app, "/admin/schedules", f"/admin/schedules/{sid}/enable")
    assert resp.status_code == 500

    enabled: bool | None = await clean_pg_conn.fetchval(
        f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1', sid
    )
    assert enabled is False, "the commit failure must roll back BOTH the mutation and the audit row"
    assert await _audit_rows(clean_pg_conn, schema) == []

    await clean_pg_conn.execute(f'DROP TRIGGER audit_bomb_commit ON "{schema}".admin_audit')


# ── Attack 2: hostile principal, end to end ──────────────────────────────


async def test_hostile_principal_is_bounded_and_the_mutation_still_lands(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A custom auth dependency returning a hostile string (NUL, newlines,
    unbounded length) must not break the text bind, poison the audit row,
    or block the mutation: the subject lands bounded and control-free."""
    from taskq.web.admin._audit import SUBJECT_MAX_LENGTH

    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind) "
        f"VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, "
        f"'pending'::\"{schema}\".job_status, 3, 'transient')",
        jid,
    )

    hostile = "ops\x00admin\nevil\r\x1b[31m" + "A" * (SUBJECT_MAX_LENGTH * 4)

    async def _hostile_auth() -> str:
        return hostile

    app = _make_admin_app(module_pg_pool, schema, monkeypatch, auth_dependency=_hostile_auth)
    resp = await _get_csrf_then_post(app, "/admin/jobs", f"/admin/jobs/{jid}/cancel")
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    subject: str = rows[0]["principal_subject"]
    assert len(subject) <= SUBJECT_MAX_LENGTH
    for ch in "\x00\n\r\x1b":
        assert ch not in subject, f"control char {ch!r} survived into the audit row"

    # The cancel itself was not blocked by the hostile principal (a
    # pending job cancels straight to 'cancelled'; only the running arm
    # stamps cancel_requested_at).
    status: str | None = await clean_pg_conn.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', jid
    )
    assert status == "cancelled"


# ── Attack 3: the queue purge is audited with its scope ──────────────────


async def test_purge_queue_audit_row_carries_the_purge_scope(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``purge_queue=true`` is the destructive multi-row arm of
    deregistration. The audit row must name the queue it purged and carry
    the per-stage row counts -- 'purge: true' alone cannot answer 'what
    exactly did this button delete'."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="audit-purge-actor", max_concurrent=1, queue="audit-purge-q")],
        schema=schema,
    )
    # The purge arm deletes the orphaned queue's definition row (and only
    # when nothing non-terminal still references the queue), so the actor
    # is deregistered clean and the queue row is what must disappear.
    # Terminal history on the queue stays, deliberately.
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".queues '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(name, mode, max_concurrent) VALUES ('audit-purge-q', 'strict_fifo', 4) "
        "ON CONFLICT (name) DO NOTHING"
    )

    app = _make_admin_app(module_pg_pool, schema, monkeypatch)
    resp = await _get_csrf_then_post(
        app,
        "/admin/actors",
        "/admin/actors/audit-purge-actor/deregister",
        data={"purge_queue": "true"},
    )
    assert resp.status_code == 303

    rows = await _audit_rows(clean_pg_conn, schema)
    assert len(rows) == 1
    detail_obj = (
        _json_loads(rows[0]["detail"]) if isinstance(rows[0]["detail"], str) else rows[0]["detail"]
    )
    assert detail_obj["purge_queue"] is True
    assert detail_obj["queue"] == "audit-purge-q", "the trail must name the purged queue"
    assert detail_obj["queue_purged"] is True
    assert "jobs_cancelled" in detail_obj and "schedules_disabled" in detail_obj

    # And the purge really happened: the queue definition row is gone.
    remaining = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".queues WHERE name = 'audit-purge-q'"
    )
    assert int(remaining or 0) == 0


# ── Attack 4: retry's only attribution is the audit row ──────────────────


async def test_retry_writes_no_job_event_so_the_audit_row_is_the_attribution(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike cancel, ``Backend.retry_job`` writes NO job_events row (it is
    one UPDATE plus a wake NOTIFY), so there is nothing to fold the
    principal into: the admin_audit row is the retry's ONLY attribution.
    Pinned so that if retry ever grows an event, the fold question is
    answered deliberately rather than by omission."""
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
    assert len(rows) == 1 and rows[0]["action"] == "job.retry"
    events = await clean_pg_conn.fetch(
        f'SELECT kind FROM "{schema}".job_events WHERE job_id = $1', jid
    )
    assert events == [], "retry writes no event; the audit row is the attribution"

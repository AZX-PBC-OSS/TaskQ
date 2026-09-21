"""Tests for the admin audit trail (issue #324): who did what, durably.

Unit tier: scripted asyncpg connection stubs pin that every admin mutation
route writes its ``admin_audit`` row (same transaction on the
admin-owned-SQL paths, own bounded checkout on the backend-mediated ones),
that the principal comes from the auth dependency's return value, that the
dev no-auth path records the explicit ``anonymous`` subject, and that the
disabled (403) paths write nothing. The end-to-end tier against a real
migrated Postgres lives in tests/test_admin_audit_trail.py.
"""

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_uuid
from taskq.web.admin._audit import ANONYMOUS_SUBJECT, principal_subject
from taskq.web.admin.auth import IdentityClaims

from . import StubBackend, StubConnection, StubRecord, _stub_job_row

pytestmark = [pytest.mark.fastapi]

_CLAIMS = IdentityClaims(subject="ops-admin@example.com", email=None, groups=frozenset(), raw={})


def _fixed_auth() -> Any:
    async def _dependency() -> IdentityClaims:
        return _CLAIMS

    return _dependency


def _get_csrf_token(client: Any) -> str:
    """GET the queues page to set the CSRF cookie, then return the token value."""
    client.get("/queues")
    return client.cookies.get("taskq_csrf_token", "")


class _RecordingConnection(StubConnection):
    """Connection stub that records execute() calls and transaction() uses."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_depth = 0
        self.max_transaction_depth = 0

    def transaction(self) -> Any:
        self.transaction_depth += 1
        self.max_transaction_depth = max(self.max_transaction_depth, self.transaction_depth)
        return super().transaction()

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "UPDATE 1"


class _AcquireCtx:
    def __init__(self, conn: _RecordingConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RecordingConnection:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _RecordingPool:
    """Pool stub that always yields the same recording connection."""

    def __init__(self, conn: _RecordingConnection) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
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


def _audit_inserts(conn: _RecordingConnection) -> list[tuple[str, tuple[object, ...]]]:
    return [(sql, args) for sql, args in conn.execute_calls if "admin_audit" in sql]


def _job_record(jid: Any) -> StubRecord:
    """The jobs-table row the job detail route's fetchrow returns."""
    return StubRecord(**asdict(_stub_job_row(jid)))


# ── principal_subject normalization ─────────────────────────────────────


def test_principal_subject_identity_claims() -> None:
    assert principal_subject(_CLAIMS) == "ops-admin@example.com"


def test_principal_subject_none_is_explicit_anonymous() -> None:
    """The no-auth dev path resolves to the explicit 'anonymous' subject,
    never NULL and never a crash."""
    assert principal_subject(None) == ANONYMOUS_SUBJECT


def test_principal_subject_string_principal() -> None:
    """A custom auth dependency may return a bare subject string."""
    assert principal_subject("deployment-operator") == "deployment-operator"


# ── Schedule enable / disable / skip (admin-owned SQL, same transaction) ─


def test_schedule_enable_writes_audit_row_in_the_same_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), auth_dependency=_fixed_auth())
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    sql, args = inserts[0]
    assert "INSERT INTO" in sql and "admin_audit" in sql
    # (principal_subject, action, target_type, target_id, reason, detail)
    assert args[0] == "ops-admin@example.com"
    assert args[1] == "schedule.enable"
    assert args[2] == "schedule"
    assert args[3] == str(sid)
    # The audit row was written INSIDE the mutation's transaction block,
    # not after it: the same-transaction guarantee is the pin.
    assert conn.max_transaction_depth >= 1


def test_schedule_disable_writes_audit_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), auth_dependency=_fixed_auth())
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/disable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][1] == "schedule.disable"


def test_schedule_skip_writes_audit_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    db_now = datetime.now(UTC)
    row = StubRecord(
        cron_expr="* * * * *",
        timezone="UTC",
        dst_strategy="skip",
        next_fire_at=db_now - timedelta(minutes=2),
        db_now=db_now,
    )

    class _SkipConn(_RecordingConnection):
        async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
            return row

    conn = _SkipConn()
    client = _make_app(_RecordingPool(conn), auth_dependency=_fixed_auth())
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/skip", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][1] == "schedule.skip"
    assert inserts[0][1][0] == "ops-admin@example.com"


def test_schedule_enable_403_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The disabled path is refused before any statement runs: no mutation,
    and above all no audit row pretending one happened."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), auth_dependency=_fixed_auth())
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(f"/schedules/{sid}/enable", data={"csrf_token": token})
    assert resp.status_code == 403  # pyright: ignore[reportUnknownMemberType]
    assert conn.execute_calls == []


def test_schedule_enable_without_auth_records_anonymous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dev router (auth_dependency=None) records the explicit
    'anonymous' principal instead of crashing on a missing claims object."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn))
    sid = new_uuid()
    token = _get_csrf_token(client)
    resp = client.post(
        f"/schedules/{sid}/enable", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][0] == ANONYMOUS_SUBJECT


# ── Job cancel (backend-mediated) ────────────────────────────────────────


def test_cancel_writes_audit_row_and_round_trips_the_form_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="pending"))
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), backend=backend, auth_dependency=_fixed_auth())
    token = _get_csrf_token(client)
    resp = client.post(
        f"/jobs/{jid}/cancel",
        data={"csrf_token": token, "reason": "duplicate run"},
        follow_redirects=False,
    )
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    # The reason round-trips form -> write_cancel_request.
    assert len(backend.cancel_calls) == 1
    assert backend.cancel_calls[0][1] == "duplicate run"
    # And the audit row carries the principal, the action, the target, and
    # the same reason.
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    args = inserts[0][1]
    assert args[0] == "ops-admin@example.com"
    assert args[1] == "job.cancel"
    assert args[2] == "job"
    assert args[3] == str(jid)
    assert args[4] == "duplicate run"
    # The principal is folded into the cancel_request event too (the
    # per-job event log carries it): the fold UPDATE was issued.
    folds = [
        sql for sql, _ in conn.execute_calls if "job_events" in sql and "jsonb_build_object" in sql
    ]
    assert len(folds) == 1


def test_cancel_403_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="pending"))
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), backend=backend, auth_dependency=_fixed_auth())
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token})
    assert resp.status_code == 403  # pyright: ignore[reportUnknownMemberType]
    assert conn.execute_calls == []
    assert len(backend.cancel_calls) == 0


def test_cancel_without_auth_records_anonymous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="pending"))
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), backend=backend)
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/cancel", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    assert inserts[0][1][0] == ANONYMOUS_SUBJECT


# ── Job retry (backend-mediated) ─────────────────────────────────────────


def test_retry_writes_audit_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="failed"))
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), backend=backend, auth_dependency=_fixed_auth())
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token}, follow_redirects=False)
    assert resp.status_code == 303  # pyright: ignore[reportUnknownMemberType]
    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    args = inserts[0][1]
    assert args[0] == "ops-admin@example.com"
    assert args[1] == "job.retry"
    assert args[3] == str(jid)


def test_retry_403_writes_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="failed"))
    conn = _RecordingConnection()
    client = _make_app(_RecordingPool(conn), backend=backend, auth_dependency=_fixed_auth())
    token = _get_csrf_token(client)
    resp = client.post(f"/jobs/{jid}/retry", data={"csrf_token": token})
    assert resp.status_code == 403  # pyright: ignore[reportUnknownMemberType]
    assert conn.execute_calls == []


# ── Job detail page renders the trail ────────────────────────────────────


_AUDIT_ROW = StubRecord(
    occurred_at=datetime.now(UTC),
    principal_subject="ops-admin@example.com",
    action="job.cancel",
    reason="duplicate run",
    detail='{"principal_subject": "ops-admin@example.com"}',
)


class _DetailConn(_RecordingConnection):
    """Connection stub scripting the job detail route's four reads:
    job fetchrow, attempts fetch, events fetch, admin_audit fetch."""

    def __init__(self, *, job: StubRecord | None, audit: list[StubRecord]) -> None:
        super().__init__()
        self._job = job
        self._audit = audit

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        if "admin_audit" in query:
            return None
        return self._job

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if "admin_audit" in query:
            return self._audit
        return []  # attempts, then events


def test_job_detail_renders_audit_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job detail page renders the per-job audit trail."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = new_uuid()
    conn = _DetailConn(job=_job_record(jid), audit=[_AUDIT_ROW])
    client = _make_app(_RecordingPool(conn))
    resp = client.get(f"/jobs/{jid}")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Admin Audit Trail" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "ops-admin@example.com" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "job.cancel" in resp.text  # pyright: ignore[reportUnknownMemberType]
    assert "duplicate run" in resp.text  # pyright: ignore[reportUnknownMemberType]


def test_job_detail_without_audit_table_renders_empty_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schema the audit migration has not reached degrades to the empty
    section, never a 500 (the same contract the schedules page has for a
    missing cron_schedules)."""
    import asyncpg as _asyncpg

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = new_uuid()

    class _PreMigrationConn(_DetailConn):
        async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
            if "admin_audit" in query:
                raise _asyncpg.exceptions.UndefinedTableError("missing")
            return self._job

        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "admin_audit" in query:
                raise _asyncpg.exceptions.UndefinedTableError("missing")
            return []

    conn = _PreMigrationConn(job=_job_record(jid), audit=[])
    client = _make_app(_RecordingPool(conn))
    resp = client.get(f"/jobs/{jid}")
    assert resp.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "No admin actions recorded." in resp.text  # pyright: ignore[reportUnknownMemberType]

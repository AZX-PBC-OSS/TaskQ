"""Red-team pins for the admin audit trail (attack review of #324's fix).

The original defect (no audit trail at all) is fixed; these tests attack
what that fix MISSED:

* the silent-degradation window: a backend-mediated mutation whose audit
  checkout keeps failing lands un-attributed forever, so the failure must
  be alertable (a metric counter, not only a log line);
* principal integrity: a hostile/buggy custom auth dependency returning a
  string with control characters or unbounded length must not poison the
  audit row, the folded event detail, or the text bind;
* coverage: ``POST /rate-limits/{bucket}/reset`` is an admin-UI operator
  mutation and was not audited at all;
* honesty of the cancel fold when the cancel lost the race (the backend
  returned ``False``): no audit row, and above all no fold onto an OLDER
  cancel_request event this request did not write.
"""

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_uuid
from taskq.web.admin._audit import principal_subject
from taskq.web.admin.auth import IdentityClaims

from . import StubBackend, _stub_job_row
from .test_admin_audit_trail import _RecordingConnection, _RecordingPool

pytestmark = [pytest.mark.fastapi]

_CLAIMS = IdentityClaims(subject="ops-admin@example.com", email=None, groups=frozenset(), raw={})

_FAILED_METRIC = "taskq.admin.audit.record_failed"


def _fixed_auth() -> Any:
    async def _dependency() -> IdentityClaims:
        return _CLAIMS

    return _dependency


_AUTHENTICATED = _fixed_auth()


# ── 1. the degradation window must be alertable, not only logged ─────────


def test_cancel_lands_when_audit_checkout_keeps_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backend call succeeds, the audit checkout is exhausted: the
    mutation lands (that part is unavoidable post-commit), and the
    degradation must be COUNTED on an OTel counter
    (``taskq.admin.audit.record_failed``) so an operator can alert on it
    -- a log line alone is not alertable and the window can persist
    indefinitely (pool wedged, audit table not migrated)."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    import taskq.web.admin._audit as audit_mod
    from taskq.testing.otel import counter_value

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-redteam")
    monkeypatch.setattr(audit_mod, "_record_failed_counter", meter.create_counter(_FAILED_METRIC))

    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="pending"))
    conn = _RecordingConnection()

    class _ExhaustedPool(_RecordingPool):
        """Every checkout fails: the wedged-pool degradation shape."""

        def acquire(self, *, timeout: float | None = None) -> Any:
            raise TimeoutError("pool exhausted")

        # BoundedPool's checkout-timeout report reads these.
        def get_size(self) -> int:
            return 0

        def get_idle_size(self) -> int:
            return 0

    client = _client_for(_ExhaustedPool(conn), backend=backend)
    resp = _post(client, f"/jobs/{jid}/cancel", data={"reason": "still lands"})

    # The mutation landed: post-commit, warn-mode is the only honest option.
    assert resp.status_code == 303
    assert len(backend.cancel_calls) == 1
    assert _audit_inserts(conn) == []

    # ... and the degradation is alertable: the counter saw it. A degraded
    # cancel counts TWICE -- the audit record and the event fold both ride
    # the wedged checkout, and each is a distinct lost attribution.
    assert counter_value(reader, _FAILED_METRIC) == 2


def test_audit_counter_counts_every_failure_not_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three consecutive degraded mutations count 3: the alert story is a
    rate on the counter, so each occurrence must add, not dedupe."""
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    import taskq.web.admin._audit as audit_mod
    from taskq.testing.otel import counter_value

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-redteam")
    monkeypatch.setattr(audit_mod, "_record_failed_counter", meter.create_counter(_FAILED_METRIC))

    backend = StubBackend(job_row=_stub_job_row(new_uuid(), status="pending"))
    conn = _RecordingConnection()

    class _ExhaustedPool(_RecordingPool):
        def acquire(self, *, timeout: float | None = None) -> Any:
            raise TimeoutError("pool exhausted")

        def get_size(self) -> int:
            return 0

        def get_idle_size(self) -> int:
            return 0

    client = _client_for(_ExhaustedPool(conn), backend=backend)
    for _ in range(3):
        resp = _post(client, f"/jobs/{new_uuid()}/cancel", data={"reason": "x"})
        assert resp.status_code == 303

    assert counter_value(reader, _FAILED_METRIC) == 6


# ── 2. principal integrity: bound the subject's shape ────────────────────


def test_principal_subject_has_no_control_characters() -> None:
    """A hostile or buggy auth dependency returning a string with control
    characters (newlines, NUL, ESC) must not carry them into the audit
    row, the folded event detail, or the asyncpg text bind (a NUL makes
    the bind fail, which on the safe path silently degrades the trail)."""
    hostile = "ops\x00admin\nevil\rline\x1b[31m\ttab"
    subject = principal_subject(hostile)
    for ch in "\x00\n\r\x1b\t":
        assert ch not in subject, f"control char {ch!r} survived into principal_subject"


def test_principal_subject_is_length_bounded() -> None:
    """An unbounded subject (a bug, or a hostile claims provider) must not
    write an unbounded row into the trail: it is capped."""
    from taskq.web.admin._audit import SUBJECT_MAX_LENGTH

    subject = principal_subject("A" * (SUBJECT_MAX_LENGTH * 4))
    assert len(subject) <= SUBJECT_MAX_LENGTH


def test_principal_subject_object_fallback_is_bounded_too() -> None:
    """The str() fallback for arbitrary principal shapes gets the same
    treatment (a repr can contain anything)."""
    from taskq.web.admin._audit import SUBJECT_MAX_LENGTH

    class _Hostile:
        def __str__(self) -> str:
            return "x\x00y\n" + "B" * (SUBJECT_MAX_LENGTH * 4)

    subject = principal_subject(_Hostile())
    assert len(subject) <= SUBJECT_MAX_LENGTH
    assert "\x00" not in subject and "\n" not in subject


# ── 3. coverage: the rate-limit reset mutation was un-audited ────────────


def test_rate_limit_reset_writes_audit_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """``POST /rate-limits/{bucket}/reset`` is an admin-UI operator
    mutation (it reopens a throttled bucket); it must leave an audit row
    naming the operator, like every other mutation route."""
    from fastapi.testclient import TestClient

    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    registry = RateLimitRegistry()
    registry.register(TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory"))

    conn = _RecordingConnection()
    backend = _make_admin_app_for_test(
        _RecordingPool(conn), auth_dependency=_fixed_auth(), rate_limit_registry=registry
    )
    client = TestClient(backend)
    token = _get_csrf_token_from(client)
    resp = client.post(
        "/rate-limits/api:tb/reset", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303

    inserts = _audit_inserts(conn)
    assert len(inserts) == 1
    _sql, args = inserts[0]
    assert args[0] == "ops-admin@example.com"  # principal_subject
    assert args[1] == "rate_limit.reset"  # action
    assert args[2] == "rate_limit_bucket"  # target_type
    assert args[3] == "api:tb"  # target_id


def test_rate_limit_reset_403_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The disabled reset path (default) writes no audit row."""
    from fastapi.testclient import TestClient

    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    registry = RateLimitRegistry()
    registry.register(TokenBucket("api:tb", capacity=10, refill_per_second=1, backend="memory"))

    conn = _RecordingConnection()
    backend = _make_admin_app_for_test(
        _RecordingPool(conn), auth_dependency=_fixed_auth(), rate_limit_registry=registry
    )
    client = TestClient(backend)
    token = _get_csrf_token_from(client)
    resp = client.post("/rate-limits/api:tb/reset", data={"csrf_token": token})
    assert resp.status_code == 403
    assert _audit_inserts(conn) == []


# ── 4. honesty of the cancel fold when the cancel lost the race ──────────


def test_cancel_lost_race_writes_no_row_and_never_folds_an_older_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancel whose ``write_cancel_request`` returned False (another
    writer's cancel_request is already in flight) applied nothing itself:
    no audit row may claim it, and the principal fold must NOT run -- the
    newest cancel_request event belongs to the OTHER writer, and folding
    this operator's identity onto it would falsify the event log."""
    jid = new_uuid()
    backend = StubBackend(job_row=_stub_job_row(jid, status="pending"), cancel_result=False)
    conn = _RecordingConnection()
    client = _client_for(_RecordingPool(conn), backend=backend)
    resp = _post(client, f"/jobs/{jid}/cancel", data={"reason": "too late"})

    assert resp.status_code == 303
    assert len(backend.cancel_calls) == 1
    assert _audit_inserts(conn) == []
    folds = [
        sql for sql, _ in conn.execute_calls if "job_events" in sql and "jsonb_build_object" in sql
    ]
    assert folds == []


# ── helpers ──────────────────────────────────────────────────────────────


def _audit_inserts(conn: _RecordingConnection) -> list[tuple[str, tuple[object, ...]]]:
    return [(sql, args) for sql, args in conn.execute_calls if "admin_audit" in sql]


def _make_admin_app_for_test(
    pool: Any,
    *,
    backend: Any = None,
    # Module-level singleton default (B008): a default of None means the
    # no-auth DEV router below, so "not provided" needs its own sentinel.
    auth_dependency: Any = _AUTHENTICATED,
    rate_limit_registry: Any = None,
) -> Any:
    """Bare FastAPI app (TestClient, no CSRF-cookie jar quirks)."""
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    bundle = create_router(
        pool,
        base_path="",
        backend=backend,
        auth_dependency=auth_dependency,
        rate_limit_registry=rate_limit_registry,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


def _client_for(pool: Any, **kwargs: Any) -> Any:
    """The app wrapped in a TestClient (the app alone has no cookie jar)."""
    from fastapi.testclient import TestClient

    return TestClient(_make_admin_app_for_test(pool, **kwargs))


def _get_csrf_token_from(client: Any) -> str:
    client.get("/")
    return client.cookies.get("taskq_csrf_token", "")


def _post(client: Any, url: str, *, data: dict[str, str] | None = None) -> Any:
    """CSRF dance: the token comes from a prior GET's cookie."""
    token = _get_csrf_token_from(client)
    return client.post(url, data={"csrf_token": token, **(data or {})}, follow_redirects=False)

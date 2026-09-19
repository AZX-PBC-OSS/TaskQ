"""Red-team attacks on the admin UI's mutation inputs, renders, and logs.

Hunt scope: ``src/taskq/web/admin/`` - cancel/retry mutation inputs, the job
detail render, and log framing for caller-controlled fields.

Papered defects and pins
------------------------
1. REAL DEFECT - cancel ``reason`` is the only unguarded mutation input
   (jobs.py:653)::

       reason: str | None = Query(default=None),

   Every admin *list* filter is NUL-guarded via ``parse_text_filter``
   (jobs.py:365-371: "each of these reaches a ``text`` … parameter, which
   asyncpg rejects with an opaque 22021") - but ``reason`` flows unguarded
   into ``backend.write_cancel_request`` → ``_insert_cancel_request_event``
   (backend/_terminal.py:184-192) → ``jsonb_param(detail)``. PostgreSQL
   rejects ``\\u0000`` inside jsonb strings (verified against PG 18:
   ``UntranslatableCharacterError: unsupported Unicode escape sequence``),
   so ``POST /jobs/{id}/cancel?reason=…%00…`` is an opaque 500 instead of
   the family's clean 400. RED: ``test_cancel_reason_nul_is_400_not_500``.

2. REAL DEFECT - the job detail page renders ``result`` unbounded
   (jobs.py:625 + templates/job_detail.html:219)::

       for _jsonb_key in ("progress_state", "payload", "metadata", "result"):
           job_dict[_jsonb_key] = decode_jsonb(job_dict.get(_jsonb_key))

   ``error_traceback`` is display-truncated at 2000 chars
   (``_TRACEBACK_DISPLAY_LIMIT``, jobs.py:98,316-323) - the page's own
   convention for big operator-facing text - but ``result`` (and
   payload/metadata) render at full stored size. ``result_max_bytes`` is a
   *configurable storage* cap, not a render cap, so any deployment that
   raises it turns one job-detail click into a multi-MB HTML response.
   RED: ``test_job_detail_result_render_is_bounded``.

3. REAL DEFECT (log framing) - the rate-limit reset route logs the
   URL-controlled ``bucket_name`` verbatim (ops.py:650-653)::

       logger.warning(
           "rate-limit-reset-bucket-not-registered",
           bucket_name=bucket_name,)

   A ``%0A`` in the path parameter puts a raw newline into the log event -
   under any line-oriented renderer (structlog console/KV) that forges a
   whole log line. The route family's own validation convention
   (parse_text_filter) is not applied to this route at all.
   RED: ``test_rate_limit_reset_log_framing_no_raw_control_chars``.

4. GREEN PIN - the SSO group allowlist gates *mutations*, not only reads:
   a signed session outside ``allowed_groups`` is refused (401) on
   ``POST /jobs/{id}/cancel`` before the handler runs. The
   ``warn_if_no_group_allowlist`` text says "admin read access"
   (_session.py:55) - this pin documents that the same single gate also
   covers every write endpoint, which the warning understates.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg.exceptions
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

import structlog.testing
from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq._ids import new_uuid
from taskq._json import dumps_str
from taskq.testing.jobs import make_job_row
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth._session import (
    IdentityClaims,
    SessionManager,
    create_auth_dependency,
)

pytestmark = [pytest.mark.fastapi]


@pytest.fixture(autouse=True)
def _admin_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the runner; pyright does not track fixture usage.
    """Dev environment + actions enabled + http-compatible cookies, so the
    mutation endpoints under attack are reachable exactly as the existing
    web_admin suite mounts them."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")


# ── Stub doubles ──────────────────────────────────────────────────────────


class _DetailConn:
    """Fake asyncpg connection serving one job row for the detail query."""

    def __init__(self, job_row: dict[str, Any] | None) -> None:
        self._job_row = job_row

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        if "jobs_archive" in query:
            return None
        if ".jobs" in query and "WHERE id" in query:
            return self._job_row
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return []

    async def fetchval(self, query: str, *args: object) -> object:
        if "clock_timestamp()" in query:
            return datetime.now(UTC)
        return 0

    async def execute(self, query: str, *args: object) -> str:
        return "UPDATE 1"


class _AcquireCtx:
    def __init__(self, conn: _DetailConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _DetailConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, conn: _DetailConn) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


class _NulRejectingBackend:
    """Backend double that models PostgreSQL's jsonb NUL rejection exactly.

    ``write_cancel_request`` inserts a ``job_events`` row whose ``detail``
    jsonb carries the caller's ``reason`` (backend/_terminal.py:184-192,
    ``jsonb_param`` → orjson → ``$n::jsonb``). PG rejects ``\\u0000`` in
    jsonb strings with UntranslatableCharacterError (verified against a
    real PG 18), so this double raises the same driver exception the real
    backend would surface for a NUL-bearing reason.
    """

    def __init__(self, job_row: Any) -> None:
        self._job_row = job_row
        self.cancel_calls: list[tuple[Any, str | None]] = []

    async def get(self, job_id: Any) -> Any:
        return self._job_row

    async def write_cancel_request(self, job_id: Any, reason: str | None) -> bool:
        if reason is not None and "\x00" in reason:
            raise asyncpg.exceptions.UntranslatableCharacterError(
                "unsupported Unicode escape sequence\nDETAIL:  \\u0000 cannot be converted to text."
            )
        self.cancel_calls.append((job_id, reason))
        return True


class _RecordingBackend:
    """Backend double that accepts everything and records the call."""

    def __init__(self, job_row: Any) -> None:
        self._job_row = job_row
        self.cancel_calls: list[tuple[Any, str | None]] = []

    async def get(self, job_id: Any) -> Any:
        return self._job_row

    async def write_cancel_request(self, job_id: Any, reason: str | None) -> bool:
        self.cancel_calls.append((job_id, reason))
        return True


def _make_client(
    pool: _StubPool,
    *,
    backend: Any | None = None,
    auth_dependency: Any | None = None,
) -> TestClient:
    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub satisfies the asyncpg.Pool surface the routes read.
        backend=backend,
        auth_dependency=auth_dependency,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app, raise_server_exceptions=False)


def _csrf_post(client: TestClient, path: str) -> Any:
    """CSRF-valid POST that does NOT follow the 303 - the stub pool serves no
    detail row, so following the redirect would 404 for reasons unrelated to
    the behavior under test."""
    client.cookies.set("taskq_csrf_token", "rt-csrf")
    return client.post(path, data={"csrf_token": "rt-csrf"}, follow_redirects=False)


def _big_result_job_row(job_id: Any, result_json: str) -> dict[str, Any]:
    """A complete jobs-table row for the detail page, big result included."""
    now = datetime.now(UTC).isoformat()
    return {
        "id": str(job_id),
        "actor": "rt_web_big_result_actor",
        "queue": "default",
        "status": "succeeded",
        "created_at": now,
        "scheduled_at": now,
        "started_at": now,
        "finished_at": now,
        "attempt": 1,
        "max_attempts": 3,
        "priority": 0,
        "retry_kind": "transient",
        "schedule_to_close": None,
        "start_to_close": None,
        "heartbeat_timeout": None,
        "identity_key": None,
        "idempotency_key": None,
        "idempotency_scope": "",
        "fairness_key": None,
        "tags": [],
        "locked_by_worker": None,
        "lock_expires_at": None,
        "cancel_requested_at": None,
        "cancel_phase": 0,
        "trace_id": None,
        "span_id": None,
        "result_size_bytes": len(result_json),
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "payload": "{}",
        "metadata": "{}",
        "result": result_json,
        "progress_state": None,
        "progress_seq": 0,
    }


# ── 1. Cancel reason NUL → 400, not an opaque 500 ─────────────────────────


def test_cancel_reason_nul_is_400_not_500() -> None:
    """A NUL in the cancel reason must be a clean 400 at the route.

    CONTRACT: every caller-controlled text that reaches a PG bind is
    NUL-guarded at the route - the admin family's own contract
    (jobs.py:365-371 guards every list filter with parse_text_filter for
    exactly this class of driver rejection). ``reason`` is the one mutation
    input with no guard (jobs.py:653); it reaches the ``job_events`` jsonb
    insert (backend/_terminal.py:184-192), and PostgreSQL rejects ``\\u0000``
    in jsonb strings (verified: UntranslatableCharacterError).

    CURRENT VIOLATION: the driver exception is unhandled - the POST
    surfaces as an opaque 500 instead of the family's 400.
    """
    job_id = new_uuid()
    backend = _NulRejectingBackend(make_job_row(status="pending"))
    client = _make_client(_StubPool(_DetailConn(None)), backend=backend)

    # Control: a clean reason cancels fine (proves the route itself works).
    ok = _csrf_post(client, f"/jobs/{job_id}/cancel?reason=operator-requested")
    assert ok.status_code == 303, (
        f"control pin: a NUL-free reason must cancel (303); got {ok.status_code}"
    )

    rejected = _csrf_post(client, f"/jobs/{job_id}/cancel?reason=boom%00mid")
    assert rejected.status_code == 400, (
        "CONTRACT: POST /jobs/{id}/cancel?reason=…%00… must be rejected with 400 at "
        "the route - reason reaches the job_events jsonb insert "
        "(backend/_terminal.py:184-192) and PostgreSQL rejects \\u0000 in jsonb "
        "strings with UntranslatableCharacterError, the exact opaque-driver-error "
        "class the admin family's parse_text_filter contract exists to prevent "
        "(jobs.py:365-371). CURRENT VIOLATION: reason is the only unguarded "
        f"mutation input (jobs.py:653) and the driver exception is unhandled - "
        f"got {rejected.status_code} {rejected.text[:200]!r}"
    )
    assert backend.cancel_calls == [(job_id, "operator-requested")], (
        "CONTRACT: the NUL-bearing request must never reach the backend - the "
        "guard rejects before the write. CURRENT: only the control call recorded."
    )


# ── 2. Job detail result render must be bounded ───────────────────────────


def test_job_detail_result_render_is_bounded() -> None:
    """A multi-MB job result must not be rendered verbatim into the page.

    CONTRACT: operator-page rendering of stored blobs is display-bounded -
    the page's own convention truncates ``error_traceback`` at 2000 chars
    (``_TRACEBACK_DISPLAY_LIMIT``, jobs.py:98,316-323). ``result`` must get
    the same treatment: ``result_max_bytes`` is a *configurable storage*
    cap (constants.MAX_RESULT_BYTES is only the default), so any deployment
    that raises it turns one job-detail click into a multi-MB HTML
    response (memory + bandwidth on the app hosting the admin UI).

    CURRENT VIOLATION: jobs.py:625 only decodes; templates/job_detail.html:219
    renders ``{{ job.result }}`` at full stored size.
    """
    job_id = new_uuid()
    blob = dumps_str({"rows": "A" * 4_000_000})
    row = _big_result_job_row(job_id, blob)
    client = _make_client(_StubPool(_DetailConn(row)))

    resp = client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200, f"the detail page must render; got {resp.status_code}"
    assert len(resp.text) < 1_000_000, (
        "CONTRACT: the job detail page must display-bounded-render stored blobs "
        "the same way it truncates error_traceback at _TRACEBACK_DISPLAY_LIMIT=2000 "
        "(jobs.py:98,316-323) - a multi-MB result must come back truncated, not as "
        "a multi-MB HTML response. CURRENT VIOLATION: jobs.py:625 decodes result "
        "with no bound and templates/job_detail.html:219 renders "
        f"'{{{{ job.result }}}}' verbatim - got a {len(resp.text):,}-byte page for "
        "a ~4MB stored result."
    )


# ── 3. Rate-limit reset log framing ──────────────────────────────────────


def test_rate_limit_reset_log_framing_no_raw_control_chars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newline in the bucket_name path param must not forge log lines.

    CONTRACT: caller-controlled fields must not reach log events with raw
    control characters - under any line-oriented renderer (structlog
    console/KV) a raw newline in ``bucket_name`` forges a whole subsequent
    log line, corrupting log parsing/alerting.

    CURRENT VIOLATION: ops.py:650-653 logs the URL-controlled bucket_name
    verbatim, and the route applies none of the family's input validation
    (no parse_text_filter) to the path parameter.
    """
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    client = _make_client(_StubPool(_DetailConn(None)))

    with structlog.testing.capture_logs() as captured:
        resp = _csrf_post(client, "/rate-limits/forged%0Aevent%3Ainjected/reset")

    # The 404-on-unregistered-bucket half is already correct behavior.
    assert resp.status_code == 404, (
        f"CONTRACT: an unregistered bucket is a clean 404 (ops.py:654-661), "
        f"never a 500; got {resp.status_code}"
    )
    logged = [e for e in captured if e.get("event") == "rate-limit-reset-bucket-not-registered"]
    assert logged, "the unregistered-bucket 404 must be logged, not silent"
    for entry in logged:
        value = str(entry.get("bucket_name", ""))
        assert "\n" not in value and "\r" not in value, (
            "CONTRACT: URL-controlled fields must never reach log events with raw "
            "control characters - 'bucket_name=forged\\nevent:injected' forges a "
            "whole log line under any line-oriented renderer. CURRENT VIOLATION: "
            f"ops.py:650-653 logs the raw path parameter verbatim; got "
            f"bucket_name={value!r}"
        )


# ── 4. Group allowlist gates mutations, not only reads ────────────────────


def test_sso_group_allowlist_gates_mutations_not_only_reads() -> None:
    """GREEN pin: an out-of-allowlist SSO session is refused on the cancel
    mutation endpoint; an in-allowlist session reaches the handler.

    CONTRACT (pin): ``create_router(auth_dependency=...)`` gates every admin
    route - mutations included - through the SSO group allowlist
    (_factory.py:547-548 inserts the auth dependency first, so it runs
    before the route's CSRF check). The ``warn_if_no_group_allowlist``
    text speaks of "admin read access" (_session.py:55); this pin documents
    that the same single gate also decides every write endpoint.
    """
    job_id = new_uuid()
    backend = _RecordingBackend(make_job_row(status="pending"))
    manager = SessionManager(
        secret="rt-web-red-team-session-secret",
        max_age_seconds=3600,
        secure_cookie=False,
    )
    dep = create_auth_dependency(manager, frozenset({"ops"}))
    client = _make_client(_StubPool(_DetailConn(None)), backend=backend, auth_dependency=dep)

    outsider = manager.create_session_cookie(
        IdentityClaims(subject="dev-1", email=None, groups=frozenset({"developers"}), raw={})
    )
    client.cookies.set("taskq_session", outsider)
    denied = _csrf_post(client, f"/jobs/{job_id}/cancel?reason=rt-outside-allowlist")
    assert denied.status_code == 401, (
        "CONTRACT: the SSO group allowlist gates admin mutations, not only reads - "
        "an authenticated-but-out-of-allowlist session must be refused (401 for "
        f"non-HTML clients) before the handler runs; got {denied.status_code}"
    )
    assert backend.cancel_calls == [], "the refused request must never reach the backend"

    member = manager.create_session_cookie(
        IdentityClaims(subject="op-1", email=None, groups=frozenset({"ops"}), raw={})
    )
    client.cookies.set("taskq_session", member)
    allowed = _csrf_post(client, f"/jobs/{job_id}/cancel?reason=rt-inside-allowlist")
    assert allowed.status_code == 303, (
        f"CONTRACT: an in-allowlist session reaches the cancel handler; got "
        f"{allowed.status_code} {allowed.text[:200]!r}"
    )
    assert backend.cancel_calls == [(job_id, "rt-inside-allowlist")]

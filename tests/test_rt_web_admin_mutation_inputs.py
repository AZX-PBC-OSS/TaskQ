"""Adversarial review attacks on the admin UI's mutation inputs, renders, and logs.

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
   (Since the #324/#337 audit-trail change, ``reason`` is a FORM field
   posted from the cancel form's reason input; the NUL guard at the route
   is unchanged and still pinned.)

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

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import asyncpg
import asyncpg.exceptions
import httpx
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

import structlog.testing
from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq._json import dumps_str
from taskq.testing.fixtures import ModulePgSchema
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


# ── the real-PG tier ─────────────────────────────────────────────────────
#
# The arms run the real router over the module's real pool (the
# tests/test_web_admin_integration.py pattern): the detail page's reads
# answer from the real schema, with the rows a test needs seeded as real
# rows. The NUL-rejecting backend double stays: it models PG's own jsonb
# NUL refusal at the backend seam the route reaches through.


def _make_app(
    pool: asyncpg.Pool,
    schema: str,
    *,
    backend: Any | None = None,
    auth_dependency: Any | None = None,
) -> FastAPI:
    bundle = create_router(
        pool,
        schema=schema,
        base_path="",
        backend=backend,
        auth_dependency=auth_dependency,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


@asynccontextmanager
async def _client(
    pool: asyncpg.Pool,
    schema: str,
    *,
    backend: Any | None = None,
    auth_dependency: Any | None = None,
) -> AsyncGenerator[Any, None]:
    app = _make_app(pool, schema, backend=backend, auth_dependency=auth_dependency)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as c:
        c.cookies.set("taskq_csrf_token", "rt-csrf")
        yield c


async def _csrf_post(c: Any, path: str, data: dict[str, str] | None = None) -> Any:
    """CSRF-valid POST that does not follow the 303."""
    return await c.post(path, data={"csrf_token": "rt-csrf", **(data or {})})


async def _seed_big_result_job(
    conn: asyncpg.Connection, schema: str, job_id: Any, result_json: str
) -> None:
    """A complete succeeded jobs row, big result included."""
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, payload_schema_ver, max_attempts,
                retry_kind, status, priority, attempt, metadata, result,
                result_size_bytes, created_at, scheduled_at, started_at, finished_at)
            VALUES ($1, 'rt_web_big_result_actor', 'default', '{{}}'::jsonb, 1, 3,
                'transient', 'succeeded', 0, 1, '{{}}'::jsonb, $2::jsonb, $3,
                $4, $4, $4, $4)""",
        job_id,
        result_json,
        len(result_json),
        now,
    )


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


# ── 1. Cancel reason NUL → 400, not an opaque 500 ─────────────────────────


async def test_cancel_reason_nul_is_400_not_500(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
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
    async with _client(module_pg_pool, module_pg_schema.schema_name, backend=backend) as c:
        # Control: a clean reason cancels fine (proves the route itself works).
        # reason is a form field now (the job detail page's cancel form collects
        # it), so it posts in the body, not the query string.
        ok = await _csrf_post(c, f"/jobs/{job_id}/cancel", data={"reason": "operator-requested"})
        assert ok.status_code == 303, (
            f"control pin: a NUL-free reason must cancel (303); got {ok.status_code}"
        )

        rejected = await _csrf_post(c, f"/jobs/{job_id}/cancel", data={"reason": "boom\x00mid"})
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


async def test_job_detail_result_render_is_bounded(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
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
    await _seed_big_result_job(clean_pg_conn, module_pg_schema.schema_name, job_id, blob)

    async with _client(module_pg_pool, module_pg_schema.schema_name) as c:
        resp = await c.get(f"/jobs/{job_id}")
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


async def test_rate_limit_reset_log_framing_no_raw_control_chars(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
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
    async with _client(module_pg_pool, module_pg_schema.schema_name) as c:
        with structlog.testing.capture_logs() as captured:
            resp = await _csrf_post(c, "/rate-limits/forged%0Aevent%3Ainjected/reset")

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


async def test_sso_group_allowlist_gates_mutations_not_only_reads(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
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
        secret="rt-web-adversarial-session-secret",
        max_age_seconds=3600,
        secure_cookie=False,
    )
    dep = create_auth_dependency(manager, frozenset({"ops"}))
    async with _client(
        module_pg_pool, module_pg_schema.schema_name, backend=backend, auth_dependency=dep
    ) as c:
        outsider = manager.create_session_cookie(
            IdentityClaims(subject="dev-1", email=None, groups=frozenset({"developers"}), raw={})
        )
        c.cookies.set("taskq_session", outsider)
        denied = await _csrf_post(
            c, f"/jobs/{job_id}/cancel", data={"reason": "rt-outside-allowlist"}
        )
        assert denied.status_code == 401, (
            "CONTRACT: the SSO group allowlist gates admin mutations, not only reads - "
            "an authenticated-but-out-of-allowlist session must be refused (401 for "
            f"non-HTML clients) before the handler runs; got {denied.status_code}"
        )
        assert backend.cancel_calls == [], "the refused request must never reach the backend"

        member = manager.create_session_cookie(
            IdentityClaims(subject="op-1", email=None, groups=frozenset({"ops"}), raw={})
        )
        c.cookies.set("taskq_session", member)
        allowed = await _csrf_post(
            c, f"/jobs/{job_id}/cancel", data={"reason": "rt-inside-allowlist"}
        )
    assert allowed.status_code == 303, (
        f"CONTRACT: an in-allowlist session reaches the cancel handler; got "
        f"{allowed.status_code} {allowed.text[:200]!r}"
    )
    assert backend.cancel_calls == [(job_id, "rt-inside-allowlist")]

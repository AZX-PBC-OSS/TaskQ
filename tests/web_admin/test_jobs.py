"""Tests for job detail routes, templates, traceback truncation, and XSS prevention."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq._ids import new_uuid
from taskq.web.admin import create_router
from taskq.web.admin.jobs import _normalize_row, _truncate_traceback

from . import StubConnection, StubRecord, _StubPool

# ── Job detail route: discovery and registration ───────────────────────


def test_job_detail_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /jobs/{job_id} route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/jobs/{job_id}" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_job_detail_not_found_returns_404(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Job not found returns HTTP 404."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs/00000000-0000-0000-0000-000000000000")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 404  # pyright: ignore[reportUnknownVariableType]


def test_job_detail_invalid_uuid_returns_422(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Invalid UUID in path returns HTTP 422 (FastAPI default validation)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs/not-a-uuid")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 422  # pyright: ignore[reportUnknownVariableType]


# ── Job detail template ────────────────────────────────────────────────


def _detail_job_data(**overrides: object) -> dict[str, object]:
    """The minimal job mapping job_detail.html renders, with per-test overrides."""
    data: dict[str, object] = {
        "id": "00000000-0000-0000-0000-000000000010",
        "actor": "sync_data",
        "queue": "default",
        "status": "running",
        "priority": 0,
        "attempt": 168,
        "max_attempts": 3,
        "retry_kind": "indefinite",
        "scheduled_at": "2025-01-01T00:00:00+00:00",
        "started_at": "2025-01-01T00:00:01+00:00",
        "finished_at": None,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "trace_id": None,
        "payload": "{}",
        "metadata": "{}",
    }
    data.update(overrides)
    return data


def test_job_detail_marks_max_attempts_inert_for_indefinite_retry(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """An indefinite-kind job ignores max_attempts entirely (retries.md §2):
    the stored ceiling is inert, so the Attempt cell must not advertise it
    as a live budget — a row can legitimately sit at attempt 168 over a
    stored 3, and "168 / 3" reads as a lie about what is enforced."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("job_detail.html")

    html = template.render(job=_detail_job_data(), attempts=[], events=[])

    assert "168 / — (indefinite)" in html, (
        "the inert ceiling must render as — (indefinite), keeping the real attempt count visible"
    )
    assert "168 / 3" not in html, (
        "rendering the stored max_attempts as-is advertises a budget the job is not enforcing"
    )


def test_job_detail_renders_the_ceiling_for_bounded_kinds(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """The control: a bounded retry_kind renders the real ceiling."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("job_detail.html")

    html = template.render(
        job=_detail_job_data(retry_kind="transient", attempt=2), attempts=[], events=[]
    )

    assert "2 / 3" in html
    assert "— (indefinite)" not in html


def test_jobs_list_marks_max_attempts_inert_for_indefinite_retry(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """The jobs list's Attempt column carries the same marker (the row is
    where an operator scanning a queue first meets the inert field)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    html = _render_job_table(
        stub_pool,
        jobs=[_render_job_table_row(retry_kind="indefinite", attempt=168)],
    )
    assert "168 / — (indefinite)" in html
    assert "168/3" not in html

    html = _render_job_table(
        stub_pool,
        jobs=[_render_job_table_row(retry_kind="transient", attempt=2)],
    )
    assert "2 / 3" in html
    assert "— (indefinite)" not in html


def test_jobs_list_queries_fetch_retry_kind() -> None:
    """The marker needs retry_kind selected: a column the query never
    fetches can never be rendered (the lease-column pin's shape)."""
    from taskq.web.admin.jobs import _ARCHIVE_COLS, _LIVE_COLS

    for name, cols in (("_LIVE_COLS", _LIVE_COLS), ("_ARCHIVE_COLS", _ARCHIVE_COLS)):
        assert "retry_kind" in cols.lower(), (
            f"{name} must select retry_kind so the Attempt cell can mark an "
            "indefinite row's ceiling inert"
        )


def test_job_detail_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: job_detail.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "job_detail.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_job_detail_template_uses_job_card_partial(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: job_detail.html imports job_card from _partials/job_card.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "job_detail.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'from "_partials/job_card.html" import status_badge' in source


def test_job_detail_template_renders_all_job_columns(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """job_detail.html renders all required job columns."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    job_data: dict[str, object] = {
        "id": "00000000-0000-0000-0000-000000000001",
        "actor": "send_email",
        "queue": "default",
        "status": "running",
        "priority": 0,
        "attempt": 1,
        "max_attempts": 3,
        "scheduled_at": "2025-01-01T00:00:00+00:00",
        "started_at": "2025-01-01T00:00:01+00:00",
        "finished_at": None,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "trace_id": "abc123",
        "payload": '{"to": "user@example.com"}',
        "metadata": '{"source": "api"}',
    }
    html = template.render(job=job_data, attempts=[], events=[])
    assert "send_email" in html
    assert "default" in html
    assert "running" in html
    assert "abc123" in html
    assert "user@example.com" in html


def test_job_detail_template_renders_attempt_history(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """job_detail.html renders attempt history with error fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    job_data: dict[str, object] = {
        "id": "00000000-0000-0000-0000-000000000002",
        "actor": "process_order",
        "queue": "orders",
        "status": "failed",
        "priority": 0,
        "attempt": 2,
        "max_attempts": 3,
        "scheduled_at": "2025-01-01T00:00:00+00:00",
        "started_at": None,
        "finished_at": "2025-01-01T00:01:00+00:00",
        "error_class": "ValueError",
        "error_message": "invalid input",
        "error_traceback": "Traceback...",
        "trace_id": None,
        "payload": "{}",
        "metadata": "{}",
    }
    attempts_data: list[dict[str, object]] = [
        {
            "attempt": 1,
            "started_at": "2025-01-01T00:00:01+00:00",
            "finished_at": "2025-01-01T00:00:10+00:00",
            "outcome": "failed",
            "duration_ms": 9000,
            "worker_id": "00000000-0000-0000-0000-000000000099",
            "error_class": "TimeoutError",
            "error_message": "deadline exceeded",
            "error_traceback": "Traceback (most recent call last):\nTimeoutError",
        },
        {
            "attempt": 2,
            "started_at": "2025-01-01T00:00:20+00:00",
            "finished_at": "2025-01-01T00:00:25+00:00",
            "outcome": "failed",
            "duration_ms": 5000,
            "worker_id": "00000000-0000-0000-0000-000000000098",
            "error_class": "ValueError",
            "error_message": "invalid input",
            "error_traceback": None,
        },
    ]
    html = template.render(job=job_data, attempts=attempts_data, events=[])
    assert "TimeoutError" in html
    assert "deadline exceeded" in html
    assert "9.0s" in html
    assert "ValueError" in html
    assert "invalid input" in html
    assert "Attempt History" in html


def test_job_detail_template_renders_event_log(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """job_detail.html renders event log from job_events."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    job_data: dict[str, object] = {
        "id": "00000000-0000-0000-0000-000000000003",
        "actor": "send_email",
        "queue": "default",
        "status": "succeeded",
        "priority": 0,
        "attempt": 1,
        "max_attempts": 3,
        "scheduled_at": "2025-01-01T00:00:00+00:00",
        "started_at": None,
        "finished_at": None,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "trace_id": None,
        "payload": "{}",
        "metadata": "{}",
    }
    events_data: list[dict[str, object]] = [
        {
            "occurred_at": "2025-01-01T00:00:00+00:00",
            "kind": "state_change",
            "detail": '{"from": "pending", "to": "running"}',
        },
        {
            "occurred_at": "2025-01-01T00:00:05+00:00",
            "kind": "state_change",
            "detail": '{"from": "running", "to": "succeeded"}',
        },
    ]
    html = template.render(job=job_data, attempts=[], events=events_data)
    assert "Event Log" in html
    assert "state_change" in html


def test_job_detail_template_empty_events_not_error(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Empty event log renders 'No events recorded' (not an error)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    html = template.render(
        job={
            "id": "1",
            "actor": "a",
            "queue": "q",
            "status": "pending",
            "priority": 0,
            "attempt": 0,
            "max_attempts": 3,
            "scheduled_at": "",
            "started_at": None,
            "finished_at": None,
            "error_class": None,
            "error_message": None,
            "error_traceback": None,
            "trace_id": None,
            "payload": "{}",
            "metadata": "{}",
        },
        attempts=[],
        events=[],
    )
    assert "No events recorded" in html


# ── Traceback truncation ─────────────────────────────────────────


def test_truncate_traceback_short() -> None:
    """Short traceback is not truncated."""
    assert _truncate_traceback("short error") == "short error"


def test_truncate_traceback_none() -> None:
    """None traceback stays None."""
    assert _truncate_traceback(None) is None


def test_truncate_traceback_long() -> None:
    """Traceback over 2000 chars is truncated with remainder count, total <= 2000."""
    long_tb = "x" * 2500
    result = _truncate_traceback(long_tb)
    assert result is not None
    assert len(result) <= 2000
    assert "500 more characters" in result


def test_truncate_traceback_exactly_2000() -> None:
    """Traceback exactly 2000 chars is not truncated."""
    tb_2000 = "x" * 2000
    result = _truncate_traceback(tb_2000)
    assert result == tb_2000


def test_truncate_traceback_2001() -> None:
    """Traceback 2001 chars is truncated to exactly 2000 chars including suffix."""
    tb_2001 = "x" * 2001
    result = _truncate_traceback(tb_2001)
    assert result is not None
    assert len(result) == 2000
    assert "1 more characters" in result


# ── XSS prevention: autoescape on job detail fields ──────────────────────


def test_job_detail_autoescapes_payload_and_error_fields(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Job detail page auto-escapes user-derived fields (no raw <script>)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    xss_payload = '<script>alert("xss")</script>'
    html = template.render(
        job={
            "id": "00000000-0000-0000-0000-000000000004",
            "actor": "test",
            "queue": "default",
            "status": "failed",
            "priority": 0,
            "attempt": 1,
            "max_attempts": 3,
            "scheduled_at": "",
            "started_at": None,
            "finished_at": None,
            "error_class": None,
            "error_message": xss_payload,
            "error_traceback": xss_payload,
            "trace_id": None,
            "payload": xss_payload,
            "metadata": xss_payload,
        },
        attempts=[],
        events=[],
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


# ── Job detail traceback truncation regression ────────────────────


def test_job_detail_template_truncates_job_error_traceback(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """regression: job row error_traceback is truncated in the Job State section."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("job_detail.html")
    long_tb = "x" * 2500
    truncated_tb = long_tb[:1968] + "\n... (500 more characters)"
    html = template.render(
        job={
            "id": "00000000-0000-0000-0000-000000000005",
            "actor": "test",
            "queue": "default",
            "status": "failed",
            "priority": 0,
            "attempt": 1,
            "max_attempts": 3,
            "scheduled_at": "",
            "started_at": None,
            "finished_at": None,
            "error_class": "Error",
            "error_message": "boom",
            "error_traceback": truncated_tb,
            "trace_id": None,
            "payload": "{}",
            "metadata": "{}",
        },
        attempts=[],
        events=[],
    )
    assert "500 more characters" in html
    assert long_tb not in html


# ── _normalize_row unit tests ──────────────────────────────────────────────


class TestNormalizeRow:
    def test_converts_uuid_to_string(self) -> None:
        uid = new_uuid()
        result = _normalize_row({"id": uid})
        assert isinstance(result["id"], str)
        assert result["id"] == str(uid)

    def test_converts_datetime_to_isoformat(self) -> None:
        dt = datetime(2025, 1, 15, 12, 30, 0, tzinfo=UTC)
        result = _normalize_row({"created_at": dt})
        assert result["created_at"] == "2025-01-15T12:30:00+00:00"

    def test_passes_through_tags_list(self) -> None:
        result = _normalize_row({"tags": ["urgent", "batch"]})
        assert result["tags"] == ["urgent", "batch"]

    def test_passes_through_string_values(self) -> None:
        result = _normalize_row({"status": "running", "actor": "test"})
        assert result == {"status": "running", "actor": "test"}

    def test_handles_mixed_row(self) -> None:
        uid = new_uuid()
        dt = datetime(2025, 6, 1, tzinfo=UTC)
        result = _normalize_row({"id": uid, "started_at": dt, "tags": ["a"], "name": "job1"})
        assert result["id"] == str(uid)
        assert result["started_at"] == dt.isoformat()
        assert result["tags"] == ["a"]
        assert result["name"] == "job1"

    def test_handles_empty_dict(self) -> None:
        assert _normalize_row({}) == {}


# ── _build_where: tags filter ───────────────────────────────────────────


def test_build_where_with_tags() -> None:
    """_build_where appends a tags overlap clause when tags is provided."""
    from taskq.web.admin.jobs import _build_where

    where_clause, params = _build_where(
        statuses=["pending"],
        actor=None,
        queue=None,
        time_from=None,
        time_to=None,
        identity_key=None,
        fairness_key=None,
        search=None,
        tags=["urgent", "batch"],
    )
    assert "tags &&" in where_clause
    assert params[-1] == ["urgent", "batch"]


# ── Filter input bounds: status duplicates/cap, tags caps ───────────────


def test_parse_job_statuses_dedupes_preserving_order() -> None:
    """A repeated status filter value is redundant, not an error — the
    closed set has 8 members, so duplicates only bloat the ANY() array
    bind."""
    from taskq.web.admin._constants import parse_job_statuses

    assert parse_job_statuses(["pending", "failed", "pending"]) == ["pending", "failed"]
    # Valid distinct requests are unchanged.
    assert parse_job_statuses(["succeeded", "failed"]) == ["succeeded", "failed"]


def test_parse_job_statuses_dedupes_a_long_repeated_list_rather_than_rejecting_it() -> None:
    """Invalid values are rejected before the dedup, so a list longer than the
    closed set can only be duplicates — which the dedup already absorbs. It is
    a well-formed request, not a 400."""
    from taskq.web.admin._constants import parse_job_statuses

    assert parse_job_statuses(["pending"] * 9) == ["pending"]


def test_parse_job_tags_length_cap_and_dedupes() -> None:
    from fastapi import HTTPException

    from taskq.web.admin._constants import parse_job_tags

    # Duplicates and whitespace are folded away, first-occurrence order.
    assert parse_job_tags("urgent, batch ,urgent") == ["urgent", "batch"]
    # Absent filter stays absent.
    assert parse_job_tags(None) is None
    # Valid requests unchanged.
    assert parse_job_tags("urgent,batch") == ["urgent", "batch"]

    # No item-count cap: a long overlap filter is a valid query, and the
    # text[] bind and the O(n) parse cost nothing the URL length does not
    # already bound.
    many = [f"tag{i}" for i in range(64)]
    assert parse_job_tags(",".join(many)) == many

    # The enqueue side never stores a tag longer than _MAX_TAG_LENGTH
    # (client/_args.py), so a longer filter term can never match anything.
    with pytest.raises(HTTPException) as exc_info:
        parse_job_tags("x" * 256)
    assert exc_info.value.status_code == 400


def test_jobs_route_accepts_duplicate_statuses(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Duplicated status lists stay valid requests — the dedup absorbs them."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?status=pending&status=failed&status=pending")
    assert response.status_code == 200


def test_jobs_route_rejects_oversized_tags_but_not_many_tags(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    many = client.get("/jobs?tags=" + ",".join(f"tag{i}" for i in range(17)))
    assert many.status_code == 200
    too_long = client.get("/jobs?tags=" + "x" * 256)
    assert too_long.status_code == 400


def test_jobs_route_accepts_duplicate_tags(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?tags=urgent,batch,urgent")
    assert response.status_code == 200


def test_jobs_route_rejects_nul_in_text_filters(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """A %00 in any scalar text filter is a clean 400, not an asyncpg 22021 500.

    Every one of these is bound as a text parameter by ``_build_where``;
    asyncpg rejects a NUL with ``CharacterNotInRepertoireError`` (SQLSTATE
    22021) — the same driver-level class the client path already guards
    via ``JobFilter``'s NUL checks. The stub pool tolerates a NUL (200) and
    a real pool 500s; neither is the admin contract.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    for param in ("actor", "queue", "search", "identity_key", "fairness_key"):
        response = client.get(f"/jobs?{param}=bad%00name")
        assert response.status_code == 400, (param, response.status_code)
    # Time bounds bind as text::timestamptz — same bind class.
    for param in ("time_from", "time_to"):
        response = client.get(f"/jobs?{param}=2026-01-01T00:00:00%00Z")
        assert response.status_code == 400, (param, response.status_code)


def test_jobs_route_rejects_nul_in_tags_filter(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Tags travel as a text[] bind — a NUL item is the same 22021 class."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/jobs?tags=urg%00ent")
    assert response.status_code == 400


def test_jobs_count_route_rejects_nul_in_text_filters(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """The count endpoint binds the same text params as /jobs."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    for param in ("actor", "queue"):
        response = client.get(f"/jobs/count?{param}=bad%00name")
        assert response.status_code == 400, (param, response.status_code)


# ── _build_order: unknown sort falls back to first entry ────────────────


def test_build_order_unknown_sort_falls_back() -> None:
    """_build_order uses the first sortable column when sort is not recognized."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_order

    ordering = _build_order("nonexistent", "asc", _SORTABLE_LIVE)
    first = next(iter(_SORTABLE_LIVE.values()))
    assert ordering.columns[0].name == first.name
    assert ordering.columns[0].kind == first.kind
    assert "ASC" in ordering.order_by_sql()


def test_build_order_ties_the_id_tiebreaker_to_the_sort_direction() -> None:
    """``id`` runs WITH the primary column, in both directions.

    A row-wise tuple comparison can only express a page seam when every
    column of the tuple sorts the same way; an ``id`` pinned to one
    direction leaves half the sorts unpageable.
    """
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_order

    for order in ("asc", "desc"):
        ordering = _build_order("created_at", order, _SORTABLE_LIVE)
        assert ordering.columns[-1].name == "id"
        assert {c.descending for c in ordering.columns} == {order == "desc"}


# ── _build_paginated_sql: cursor and direction ──────────────────────────


def test_build_paginated_sql_cursor_ts() -> None:
    """_build_paginated_sql builds a timestamptz cursor clause for ts columns."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    sql, params = _build_paginated_sql(
        schema="taskq",
        table="jobs",
        cols="*",
        sortable=_SORTABLE_LIVE,
        where="status = ANY($1)",
        params=[["pending"]],
        cursor_at="2025-01-01T00:00:00+00:00",
        cursor_id="00000000-0000-0000-0000-000000000001",
        cursor_dir="next",
        sort="created_at",
        order="desc",
    )
    assert "timestamptz" in sql
    assert len(params) == 3


def test_build_paginated_sql_cursor_int() -> None:
    """_build_paginated_sql builds an int cursor clause for int columns."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    sql, params = _build_paginated_sql(
        schema="taskq",
        table="jobs",
        cols="*",
        sortable=_SORTABLE_LIVE,
        where="status = ANY($1)",
        params=[["pending"]],
        cursor_at="2",
        cursor_id="00000000-0000-0000-0000-000000000001",
        cursor_dir="next",
        sort="attempt",
        order="asc",
    )
    assert "::int" in sql
    assert len(params) == 3


def test_build_paginated_sql_cursor_text_default() -> None:
    """_build_paginated_sql builds a default cursor clause for text columns."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    sql, params = _build_paginated_sql(
        schema="taskq",
        table="jobs",
        cols="*",
        sortable=_SORTABLE_LIVE,
        where="status = ANY($1)",
        params=[["pending"]],
        cursor_at="send_email",
        cursor_id="00000000-0000-0000-0000-000000000001",
        cursor_dir="next",
        sort="actor",
        order="asc",
    )
    assert "timestamptz" not in sql
    assert "::int" not in sql
    assert len(params) == 3


def test_build_paginated_sql_prev_direction_reverses_order() -> None:
    """_build_paginated_sql wraps in a subquery with reversed order for prev direction."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    sql, _params = _build_paginated_sql(
        schema="taskq",
        table="jobs",
        cols="*",
        sortable=_SORTABLE_LIVE,
        where="status = ANY($1)",
        params=[["pending"]],
        cursor_at="2025-01-01T00:00:00+00:00",
        cursor_id="00000000-0000-0000-0000-000000000001",
        cursor_dir="prev",
        sort="created_at",
        order="desc",
    )
    assert "SELECT * FROM (" in sql
    assert "ASC" in sql  # reversed from DESC


def test_build_paginated_sql_prev_direction_without_a_cursor_is_the_first_page() -> None:
    """``cursor_dir=prev`` with no usable cursor has nothing to walk back
    from: the query is the unpaged first page, not the reversed tail of the
    result set."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    for cursor_id in (None, "not-a-uuid"):
        sql, params = _build_paginated_sql(
            schema="taskq",
            table="jobs",
            cols="*",
            sortable=_SORTABLE_LIVE,
            where="status = ANY($1)",
            params=[["pending"]],
            cursor_at="2025-01-01T00:00:00+00:00",
            cursor_id=cursor_id,
            cursor_dir="prev",
            sort="created_at",
            order="desc",
        )
        assert "SELECT * FROM (" not in sql, cursor_id
        assert "ASC" not in sql, cursor_id
        assert params == [["pending"]], cursor_id


class _OneJobConnection(StubConnection):
    """Connection whose jobs-list query returns one row so the table (and its
    pagination block) renders; every other query stays empty."""

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if ".jobs WHERE" in query:
            return [StubRecord(_render_job_table_row(id=new_uuid()))]
        return []


class _OneJobPool(_StubPool):
    def acquire(self, *, timeout: float | None = None) -> Any:
        conn = _OneJobConnection()

        class _Ctx:
            async def __aenter__(self) -> StubConnection:
                return conn

            async def __aexit__(self, *args: object) -> None:
                pass

        return _Ctx()


def test_jobs_route_malformed_cursor_renders_the_first_page_without_a_prev_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale bookmark's cursor is dropped and the first page served; the
    page must then not claim to be paged-into. With one row and no more,
    neither a "Previous" nor a "Next" link may render - a link built from a
    cursor that was never applied walks the operator into the wrong page."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_OneJobPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app)

    response = client.get("/jobs?tab=live&cursor_at=garbage&cursor_id=garbage&cursor_dir=prev")
    assert response.status_code == 200
    assert "cursor_dir=prev" not in response.text
    assert "cursor_dir=next" not in response.text


# ── _parse_time_range: explicit instants vs. a relative window ──────────


def test_parse_time_range_explicit_from_to() -> None:
    """_parse_time_range returns explicit from/to (absolute, caller-owned
    instants) without consulting time_range; ``within`` stays None."""
    from taskq.web.admin.jobs import _parse_time_range

    result = _parse_time_range(
        time_range="1h",
        time_from="2025-01-01T00:00:00+00:00",
        time_to="2025-01-02T00:00:00+00:00",
    )
    assert result == ("2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00", None)


def test_parse_time_range_named_range_resolves_to_interval() -> None:
    """A named range resolves to a timedelta (``within``) that _build_where
    evaluates against clock_timestamp() server-side, never to an absolute
    app-clock bound (7565d3f: created_at is DB-clock-written; an app-clock
    bound would shift the window by the skew). Unknown ranges and no filter
    resolve to no within at all."""
    from taskq.web.admin.jobs import _parse_time_range

    assert _parse_time_range(time_range="1h", time_from=None, time_to=None) == (
        None,
        None,
        timedelta(hours=1),
    )
    assert _parse_time_range(time_range="bogus", time_from=None, time_to=None) == (
        None,
        None,
        None,
    )
    assert _parse_time_range(time_range=None, time_from=None, time_to=None) == (
        None,
        None,
        None,
    )


def test_build_where_binds_a_named_range_as_a_duration_not_an_instant() -> None:
    """No app-clock instant may reach the query for a relative window: the
    duration itself is bound, leaving the anchoring to the server.  (That the
    window really is measured from the database clock is pinned end-to-end
    against real Postgres in test_web_admin_integration.py.)"""
    from taskq.web.admin.jobs import _build_where

    _sql, params = _build_where(
        ["pending"], None, None, None, None, None, None, None, within=timedelta(hours=1)
    )

    assert timedelta(hours=1) in params
    assert not any(isinstance(p, datetime) for p in params)


# ── Pagination links must carry the active sort ─────────────────────────


def _render_job_table(stub_pool: _StubPool, **context: Any) -> str:
    """Render the jobs table partial with one paged row.

    The row's own fields are irrelevant here — every assertion in this
    section is about the pagination links, so the row exists only to make
    the table render at all.
    """
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    defaults: dict[str, Any] = {
        "jobs": [_render_job_table_row()],
        "tab": "archived",
        "statuses": ["failed"],
        "all_statuses": ["failed"],
        "active_statuses": [],
        "terminal_statuses": ["failed"],
        "has_next": True,
        "has_prev": True,
        "next_cursor_at": "2025-01-01T11:00:00",
        "next_cursor_id": str(new_uuid()),
        "prev_cursor_at": "2025-01-01T13:00:00",
        "prev_cursor_id": str(new_uuid()),
        "cursor_dir": "next",
        "live": "off",
    }
    return bundle.templates.get_template("_partials/job_table.html").render(
        **{**defaults, **context}
    )


def _render_job_table_row(**overrides: Any) -> dict[str, Any]:
    """One jobs-list row as the page hands it to the table partial."""
    row: dict[str, Any] = {
        "id": "abc-123",
        "actor": "send_email",
        "queue": "default",
        "status": "failed",
        "created_at": "2025-01-01T12:00:00",
        "scheduled_at": "2025-01-01T12:00:00",
        "started_at": None,
        "finished_at": None,
        "duration_ms": None,
        "attempt": 1,
        "max_attempts": 3,
        "retry_kind": "transient",
        "priority": 5,
        "identity_key": None,
        "fairness_key": None,
        "progress_state": None,
        "error_message": None,
    }
    row.update(overrides)
    return row


def _page_links(html: str) -> list[str]:
    """The Previous/Next hrefs, in document order."""
    return [
        line.split('href="', 1)[1].split('"', 1)[0]
        for line in html.splitlines()
        if "cursor_dir=" in line and 'href="' in line
    ]


def test_pagination_links_carry_the_active_sort(stub_pool: _StubPool) -> None:
    """Clicking a column header then Next must keep sorting by that column.

    The cursor encodes the value of the SORT column. A page link that
    drops ``sort``/``order`` sends that value back to be compared against
    the *default* column, so Next either serves nonsense or — as here,
    where the cursor no longer matches anything — silently re-serves the
    page the operator is already on.
    """
    links = _page_links(_render_job_table(stub_pool, sort="finished_at", order="asc"))

    assert links, "expected Previous and Next links"
    for href in links:
        assert "sort=finished_at" in href, href
        assert "order=asc" in href, href


def test_pagination_links_keep_the_filter_state(stub_pool: _StubPool) -> None:
    """Sort is added to the page links, not swapped in for the filters."""
    links = _page_links(
        _render_job_table(stub_pool, sort="finished_at", order="desc", actor_filter="send_email")
    )

    assert links
    for href in links:
        assert "actor=send_email" in href, href
        assert "status=failed" in href, href


def test_pagination_links_survive_an_empty_cursor_value(stub_pool: _StubPool) -> None:
    """A NULL sort value is an empty ``cursor_at``, not a dropped link.

    ``finished_at DESC NULLS LAST`` ends in the unfinished rows, so the
    seam between two of them carries no value — only the id.
    """
    links = _page_links(
        _render_job_table(stub_pool, sort="finished_at", order="desc", next_cursor_at="")
    )

    assert any("cursor_dir=next" in href and "cursor_at=&" in href for href in links), links


# ── Live-tab lease state: the zombie-running shape is visible ────────────


def _live_row(**overrides: Any) -> dict[str, Any]:
    """One live-tab row carrying the lock columns the lease cell reads."""
    row: dict[str, Any] = {
        "id": "abc-123",
        "actor": "send_email",
        "queue": "default",
        "status": "running",
        "created_at": "2025-01-01T12:00:00",
        "scheduled_at": "2025-01-01T12:00:00",
        "started_at": "2025-01-01T12:00:01",
        "finished_at": None,
        "duration_ms": None,
        "attempt": 1,
        "max_attempts": 3,
        "priority": 5,
        "identity_key": None,
        "fairness_key": None,
        "progress_state": None,
        "error_message": None,
        "locked_by_worker": str(new_uuid()),
        "lock_expires_at": "2025-01-01T12:01:00",
        "lease_expired": False,
    }
    row.update(overrides)
    return row


def _render_live_table(stub_pool: _StubPool, jobs: list[dict[str, Any]]) -> str:
    """Render the jobs table partial on the live tab with *jobs*."""
    return _render_job_table(stub_pool, tab="live", jobs=jobs, statuses=["running"])


def test_live_jobs_list_fetches_the_lease_columns() -> None:
    """The live-tab list query must fetch ``lock_expires_at`` and compute
    ``lease_expired`` server-side (the DB clock wrote the lease; the row's
    expired-ness is a stored predicate, not a Python-clock guess)."""
    from taskq.web.admin.jobs import _LIVE_COLS

    cols = _LIVE_COLS.lower()
    assert "lock_expires_at" in cols, (
        "the live /jobs list must fetch lock_expires_at — a lease the page "
        "never selects can never be rendered"
    )
    assert "lease_expired" in cols, (
        "the live /jobs list must compute lease_expired server-side against "
        "the database clock — comparing the row's timestamptz in Python mixes "
        "clock domains on the one field where 'past' is the whole signal"
    )


def test_live_jobs_table_renders_lease_column_with_expired_badge(
    stub_pool: _StubPool,
) -> None:
    """A running row whose lease is past renders the lease state — expiry
    time plus the holding worker, with the expired state marked visually,
    following the status-badge pattern.

    The zombie shape (running, lease past, row still running) is exactly
    what the admin page must surface: without the lease column, an
    operator staring at /jobs sees a healthy running job.
    """
    worker_id = str(new_uuid())
    expired_row = _live_row(
        locked_by_worker=worker_id, lock_expires_at="2025-01-01T12:00:30", lease_expired=True
    )
    healthy_row = _live_row(lease_expired=False)

    html = _render_live_table(stub_pool, [expired_row, healthy_row])

    assert "Lease" in html, "the live tab must carry a Lease column"
    assert worker_id in html, (
        "the holding worker must render — which worker holds the stuck lease "
        "is the first question at 3am"
    )
    assert "expired" in html, (
        "an expired lease must be visually distinct from a live one — the "
        "zombie shape must not read as a healthy running job"
    )
    # The healthy row shows its lease time without the expired marking.
    healthy_cell_marker = ">12:01:00<" in html or "12:01:00" in html
    assert healthy_cell_marker, "a healthy running row must show its lease expiry"


def test_live_jobs_table_non_running_rows_have_no_lease_state(
    stub_pool: _StubPool,
) -> None:
    """Pending/terminal rows hold no lease — their lease cell renders the
    same muted dash every other empty cell uses, not a badge."""
    pending_row = _live_row(status="pending", lease_expired=False)
    pending_row.pop("locked_by_worker")
    pending_row.pop("lock_expires_at")
    pending_row.pop("lease_expired")

    html = _render_live_table(stub_pool, [pending_row])

    assert "Lease" in html
    assert "expired" not in html.replace("Lease", ""), (
        "a row that holds no lease must not render an expired badge — the "
        "badge means a running row's lease is past, nothing else"
    )


# ── started_at sort: running-longest view ───────────────────────────────


def test_started_at_is_a_sortable_column_on_both_tabs() -> None:
    """``started_at`` pages like the other timestamp columns.

    An operator answers "what has been running longest" with
    ``status=running&sort=started_at&order=asc``; that only works when the
    column joins the keyset ordering instead of falling back to the
    default (created_at) sort, which would silently re-serve one page.
    """
    from taskq.web.admin.jobs import _SORTABLE_ARCHIVE, _SORTABLE_LIVE

    assert "started_at" in _SORTABLE_LIVE
    assert "started_at" in _SORTABLE_ARCHIVE
    assert _SORTABLE_LIVE["started_at"].kind == "ts"
    assert _SORTABLE_LIVE["started_at"].nullable


def test_started_at_sort_pages_by_keyset() -> None:
    """sort=started_at builds a timestamptz cursor clause and NULLS LAST."""
    from taskq.web.admin.jobs import _SORTABLE_LIVE, _build_paginated_sql

    sql, params = _build_paginated_sql(
        schema="taskq",
        table="jobs",
        cols="*",
        sortable=_SORTABLE_LIVE,
        where="status = ANY($1)",
        params=[["running"]],
        cursor_at="2025-01-01T00:00:00+00:00",
        cursor_id="00000000-0000-0000-0000-000000000001",
        cursor_dir="next",
        sort="started_at",
        order="asc",
    )
    assert "timestamptz" in sql
    assert "started_at" in sql
    assert "NULLS LAST" in sql.upper()
    assert len(params) == 3


def test_started_at_sort_header_links_to_the_column(stub_pool: _StubPool) -> None:
    """The Started column header is a sort control, not a static label."""
    html = _render_job_table(
        stub_pool,
        tab="live",
        sort="started_at",
        order="asc",
    )
    assert "sort=started_at" in html
    assert "▲" in html  # active sort indicator rendered on the asc link

"""Tests for queue routes and templates in taskq.web.admin."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin import create_router

from . import StubRecord, _StubPool

# ── Queue routes: discovery and registration ───────────────────────────


pytestmark = [pytest.mark.fastapi]


def test_queue_routes_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /queues and GET /queues/{queue} routes are present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/queues" in route_paths
    assert any(p == "/queues/{queue:path}" for p in route_paths)  # pyright: ignore[reportUnknownVariableType]


def test_queue_overview_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /queues returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_queue_detail_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /queues/{queue} returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_queue_detail_nul_in_queue_name_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """A %00 in the path's queue name is a clean 400, not an asyncpg 22021 500.

    The name is bound as a text parameter by every detail query - the same
    driver-level NUL class the list filters guard against.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/bad%00name")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]
    assert "NUL" in response.text  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: invalid status filter ────────────────────────────────


def test_queue_detail_invalid_status_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Invalid ?status= value returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?status=invalid")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: invalid cursor validation ────────────────────────────


def test_queue_detail_invalid_cursor_at_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Non-ISO8601 cursor_at returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get(  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
        "/queues/default?cursor_at=not-a-date&cursor_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_invalid_cursor_id_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Non-UUID cursor_id returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_at=2025-01-01T00:00:00&cursor_id=not-a-uuid")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_partial_cursor_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Only one of cursor_at/cursor_id provided returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_at=2025-01-01T00:00:00")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


def test_queue_detail_cursor_id_only_returns_400(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """DoD: Only cursor_id provided (without cursor_at) returns HTTP 400."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default?cursor_id=00000000-0000-0000-0000-000000000000")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 400  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: no cursor on first page ──────────────────────────────


def test_queue_detail_first_page_no_cursor_params(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """First page loads without cursor params and returns 200."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]


# ── Queue detail: status tab defaults to pending ───────────────────────


def test_queue_detail_default_status_is_pending(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """Default status filter is 'pending' - the pending tab carries the active class."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/queues/default")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    html = response.text  # pyright: ignore[reportUnknownVariableType]
    assert "border-blue-500" in html and "pending" in html


# ── Templates extend _base.html ────────────────────────────────────────


def test_queues_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: queues.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "queues.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_queue_detail_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: queue_detail.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "queue_detail.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


# ── Template rendering with data ────────────────────────────────────────


def test_queues_template_renders_queue_data(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """queues.html renders queue names and counts from template data."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queues.html")
    html = template.render(
        queues=[
            {"queue": "default", "pending_count": 5, "scheduled_count": 2, "running_count": 1},
            {"queue": "emails", "pending_count": 0, "scheduled_count": 0, "running_count": 3},
        ]
    )
    assert "default" in html
    assert "emails" in html
    assert "/queues/default" in html
    assert "/queues/emails" in html


def test_queue_detail_template_renders_pagination_link(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """queue_detail.html renders next-page link when has_next is True."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "status": "pending",
                "scheduled_at": "2025-01-01T00:00:00",
                "attempt": 1,
                "max_attempts": 3,
            }
        ],
        has_next=True,
        next_cursor_at="2025-01-01T00:01:00",
        next_cursor_id="def-456",
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" in html
    assert "cursor_at=2025-01-01T00%3A01%3A00" in html
    assert "cursor_id=def-456" in html


def test_queue_detail_template_urlencodes_timezone_cursor(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Timezone-aware cursor_at with + is URL-encoded so + is not parsed as a space."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[
            {
                "id": "abc-123",
                "actor": "send_email",
                "status": "pending",
                "scheduled_at": "2025-01-01T00:00:00",
                "attempt": 1,
                "max_attempts": 3,
            }
        ],
        has_next=True,
        next_cursor_at="2025-01-01T00:00:00+00:00",
        next_cursor_id="00000000-0000-0000-0000-000000000000",
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" in html
    assert "cursor_at=2025-01-01T00%3A00%3A00%2B00%3A00" in html
    assert "cursor_id=00000000-0000-0000-0000-000000000000" in html


def test_queue_detail_template_no_pagination_link_when_last_page(
    monkeypatch: pytest.MonkeyPatch,
    stub_pool: _StubPool,
) -> None:
    """queue_detail.html does not render next-page link when has_next is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("queue_detail.html")
    html = template.render(
        queue_name="default",
        status="pending",
        jobs=[],
        has_next=False,
        next_cursor_at=None,
        next_cursor_id=None,
        allowed_statuses=["pending", "running", "scheduled"],
    )
    assert "Next page" not in html


def _queue_detail_job_row(**overrides: Any) -> dict[str, Any]:
    """One queue-detail row as the route hands it to the template."""
    row: dict[str, Any] = {
        "id": "abc-123",
        "actor": "send_email",
        "status": "pending",
        "scheduled_at": "2025-01-01T00:00:00",
        "attempt": 1,
        "max_attempts": 3,
        "retry_kind": "transient",
    }
    row.update(overrides)
    return row


def _render_queue_detail(stub_pool: _StubPool, jobs: list[dict[str, Any]]) -> str:
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("queue_detail.html")
    return template.render(
        queue_name="default",
        status="pending",
        jobs=jobs,
        has_next=False,
        next_cursor_at=None,
        next_cursor_id=None,
        allowed_statuses=["pending", "running", "scheduled"],
    )


def test_queue_detail_marks_max_attempts_inert_for_indefinite_retry(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """An indefinite-kind row ignores max_attempts entirely (retries.md §2):
    the stored ceiling is inert, so the queue-detail Attempt cell renders
    the shared ``attempt_budget`` marker - a live row can sit at attempt
    168 over a stored 3, and "168/3" reads as a lie about what is enforced."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    html = _render_queue_detail(
        stub_pool,
        [_queue_detail_job_row(attempt=168, retry_kind="indefinite")],
    )
    assert "168 / — (indefinite)" in html, (
        "the inert ceiling must render as — (indefinite), keeping the real attempt count visible"
    )
    assert "168/3" not in html, (
        "rendering the stored max_attempts as-is advertises a budget the job is not enforcing"
    )

    html = _render_queue_detail(
        stub_pool,
        [_queue_detail_job_row(attempt=2, retry_kind="transient")],
    )
    assert "2 / 3" in html
    assert "— (indefinite)" not in html


def test_queue_detail_queries_select_retry_kind() -> None:
    """The marker needs retry_kind selected: a column the query never
    fetches can never be rendered (the jobs-list pin's shape)."""
    from taskq.web.admin.queues import _QUEUE_DETAIL_SQL_CURSOR, _QUEUE_DETAIL_SQL_FIRST

    for name, sql in (
        ("_QUEUE_DETAIL_SQL_FIRST", _QUEUE_DETAIL_SQL_FIRST),
        ("_QUEUE_DETAIL_SQL_CURSOR", _QUEUE_DETAIL_SQL_CURSOR),
    ):
        assert "retry_kind" in sql.lower(), (
            f"{name} must select retry_kind so the queue-detail Attempt cell "
            "can mark an indefinite row's ceiling inert"
        )


# ── Queue list: live workers and stranded roll-ups ────────────────────
#
# The list view carries the leader-sampled pressure signals per queue:
# live workers (the queue-depth sampler's read over workers_last_seen_idx)
# and stranded pending/scheduled rows (the stranded-jobs detector's shape,
# grouped by routing queue). Both are parameterized reads over existing
# partial indexes with the batches page's 200-row cap.


class _ScriptedConn:
    """Connection returning preset ``fetch`` results in call order."""

    def __init__(self, fetch_results: list[list[StubRecord]]) -> None:
        self._results = list(fetch_results)

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if self._results:
            return self._results.pop(0)
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def fetchval(self, query: str, *args: object) -> object:
        if "clock_timestamp()" in query:
            # The router's clock-offset probe: answer like Postgres would.
            return datetime.now(UTC)
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _ScriptedAcquireCtx:
    def __init__(self, conn: _ScriptedConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ScriptedConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _ScriptedPool:
    def __init__(self, conn: _ScriptedConn) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _ScriptedAcquireCtx:
        return _ScriptedAcquireCtx(self._conn)


def test_queue_list_renders_live_workers_and_stranded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The queues table renders the live-worker count and stranded flag per queue."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("queues.html")
    html = template.render(
        queues=[
            {
                "queue": "default",
                "pending_count": 5,
                "scheduled_count": 2,
                "running_count": 1,
                "failed_count": 0,
                "live_workers": 3,
                "stranded_count": 0,
            },
            {
                "queue": "stuck",
                "pending_count": 7,
                "scheduled_count": 0,
                "running_count": 0,
                "failed_count": 0,
                "live_workers": 0,
                "stranded_count": 4,
            },
        ],
        orphan_queues=frozenset(),
    )
    assert "Live Workers" in html
    assert "Stranded" in html
    assert "3" in html


def test_queue_list_defaults_new_columns_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue rows without the new keys render 0, not an undefined error."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("queues.html")
    html = template.render(
        queues=[
            {"queue": "default", "pending_count": 5, "scheduled_count": 2, "running_count": 1},
        ],
        orphan_queues=frozenset(),
    )
    assert "default" in html


def test_queue_overview_merges_live_workers_and_stranded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /queues merges the live-worker and stranded reads into the per-queue rows."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state

    overview = [
        StubRecord(
            queue="default",
            pending_count=5,
            scheduled_count=2,
            running_count=1,
            failed_count=0,
        )
    ]
    orphans: list[StubRecord] = []
    workers = [StubRecord(queue="default", worker_count=3)]
    stranded = [StubRecord(queue="default", stranded_count=4)]
    conn = _ScriptedConn(fetch_results=[overview, orphans, workers, stranded])
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_ScriptedPool(conn))  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app)
    response = client.get("/queues")
    assert response.status_code == 200
    assert "Live Workers" in response.text
    assert "Stranded" in response.text
    # The stranded badge carries the count and its remediation hint.
    assert "can never dispatch" in response.text


def test_queue_pressure_reads_are_parameterized_and_bounded() -> None:
    """The live-worker and stranded reads take the liveness window as $1,
    bound the server clock with statement_timestamp(), and cap their rows."""
    from taskq.web.admin.queues import (
        _QUEUE_LIVE_WORKERS_SQL,
        _QUEUE_ROW_CAP,
        _QUEUE_STRANDED_SQL,
    )

    for name, sql in (
        ("_QUEUE_LIVE_WORKERS_SQL", _QUEUE_LIVE_WORKERS_SQL),
        ("_QUEUE_STRANDED_SQL", _QUEUE_STRANDED_SQL),
    ):
        assert "$1" in sql, f"{name} must bind the liveness window as a parameter"
        assert "statement_timestamp()" in sql, (
            f"{name} must use the STABLE server clock so the bound stays a "
            "btree index condition (the samplers' two-clock rule)"
        )
        assert f"LIMIT {_QUEUE_ROW_CAP}" in sql, f"{name} must cap its row count"

    stranded = _QUEUE_STRANDED_SQL.format(schema="taskq")
    live = _QUEUE_LIVE_WORKERS_SQL.format(schema="taskq")
    assert 'FROM "taskq".workers' in live
    # The stranded read covers only the live statuses: no terminal row
    # enters it, and nothing reads the archive here.
    assert "status IN ('pending', 'scheduled')" in stranded
    for terminal in ("succeeded", "failed", "crashed", "abandoned", "cancelled"):
        assert terminal not in stranded
        assert terminal not in live


def test_queue_live_workers_read_matches_the_leader_sampler_shape() -> None:
    """The page's live-worker read is the queue-depth sampler's SQL, capped."""
    from taskq.web.admin.queues import _QUEUE_LIVE_WORKERS_SQL
    from taskq.worker._leader_sweeps import _QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE

    sampler_core = (
        _QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE.format(schema="taskq").split("WHERE", 1)[1].strip()
    )
    assert sampler_core in _QUEUE_LIVE_WORKERS_SQL.format(schema="taskq"), (
        "the queues page must reuse the leader sampler's live-worker SQL "
        "shape, not a second definition of 'live'"
    )

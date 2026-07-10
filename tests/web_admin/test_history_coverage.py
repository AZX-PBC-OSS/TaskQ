"""Coverage tests for taskq.web.admin._history (history list + stats routes).

``_history.register`` is not auto-discovered (the module name starts with an
underscore), so these tests attach it explicitly to a router built via
``create_router`` and exercise the endpoints through a ``TestClient``.

A configurable stub pool/connection lets us drive the cursor-pagination and
summary branches that the default empty ``StubPool`` cannot reach.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin._history import (
    _CURSOR_NULL_SENTINEL,
    _compute_success_rate,
)
from taskq.web.admin._history import (
    register as register_history,
)

from . import StubRecord, _StubPool

# ── Configurable stub pool/connection ───────────────────────────────────


class _FetchConn:
    """Connection returning preset ``fetch`` results in call order."""

    def __init__(self, fetch_results: list[list[StubRecord]]) -> None:
        self._results: list[list[StubRecord]] = list(fetch_results)

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        if self._results:
            return self._results.pop(0)
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _FetchAcquireCtx:
    def __init__(self, conn: _FetchConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FetchConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _FetchPool:
    """Minimal pool duck type yielding a single configurable connection."""

    def __init__(self, conn: _FetchConn) -> None:
        self._conn = conn

    def acquire(self) -> _FetchAcquireCtx:
        return _FetchAcquireCtx(self._conn)


def _build_history_app(pool: object, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Build a TestClient with history routes attached to the admin router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    register_history(bundle.router)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _history_job_row(
    *,
    job_id: UUID | None = None,
    status: str = "succeeded",
    finished_at: datetime | None = None,
) -> StubRecord:
    return StubRecord(
        id=job_id or uuid4(),
        actor="send_email",
        queue="default",
        status=status,
        finished_at=finished_at,
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=datetime(2025, 1, 1, tzinfo=UTC),
        duration_ms=1500.0,
        attempt=1,
        max_attempts=3,
        is_archived=True,
        status_priority=1,
    )


# ── Route discovery ─────────────────────────────────────────────────────


def test_history_routes_registered_via_register(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """register() attaches /history and /api/history/stats to the router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    register_history(bundle.router)
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/history" in route_paths  # pyright: ignore[reportUnknownMemberType]
    assert "/api/history/stats" in route_paths  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: empty state ───────────────────────────────────────────


def test_history_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /history returns 200 with text/html content type."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "text/html" in response.headers.get("content-type", "")  # pyright: ignore[reportUnknownMemberType]
    assert "No completed jobs found" in response.text  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: status filtering ──────────────────────────────────────


def test_history_status_filter_succeeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /history?status=succeeded returns 200."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?status=succeeded")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_history_multiple_status_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple status= params are accepted and return 200."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?status=succeeded&status=failed")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_history_invalid_status_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid ?status= value returns HTTP 400."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?status=bogus_status")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: actor/queue filters (empty-string normalization) ──────


def test_history_empty_string_filters_normalized_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty-string actor/queue/cursor params are normalized to None (200)."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?actor=&queue=")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_history_actor_and_queue_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """actor= and queue= filter params are accepted and return 200."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?actor=send_email&queue=default")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: cursor validation (400 cases) ─────────────────────────


def test_history_partial_cursor_at_only_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only cursor_at provided (without cursor_id) returns HTTP 400."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?cursor_at=2025-01-01T00:00:00")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]


def test_history_partial_cursor_id_only_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only cursor_id provided (without cursor_at) returns HTTP 400."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get(  # pyright: ignore[reportUnknownMemberType]
        "/history?cursor_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]


def test_history_invalid_cursor_at_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-ISO8601 cursor_at returns HTTP 400."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get(  # pyright: ignore[reportUnknownMemberType]
        "/history?cursor_at=not-a-date&cursor_id=00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]


def test_history_invalid_cursor_id_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-UUID cursor_id returns HTTP 400."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get(  # pyright: ignore[reportUnknownMemberType]
        "/history?cursor_at=2025-01-01T00:00:00&cursor_id=not-a-uuid"
    )
    assert response.status_code == 400  # pyright: ignore[reportUnknownMemberType]


def test_history_empty_cursor_params_normalized_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty-string cursor params are normalized to None → first page (200)."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/history?cursor_at=&cursor_id=")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: valid cursor (cursor-path SQL) ────────────────────────


def test_history_valid_cursor_returns_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid cursor_at/cursor_id pair selects the cursor SQL path (200)."""
    conn = _FetchConn(fetch_results=[[], []])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get(  # pyright: ignore[reportUnknownMemberType]
        "/history?cursor_at=2025-01-01T00:00:00&cursor_id=00000000-0000-0000-0000-000000000001"
    )
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


def test_history_null_sentinel_cursor_returns_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The __NULL__ sentinel cursor_at maps to far-future timestamp (200)."""
    conn = _FetchConn(fetch_results=[[], []])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get(  # pyright: ignore[reportUnknownMemberType]
        f"/history?cursor_at={_CURSOR_NULL_SENTINEL}&cursor_id=00000000-0000-0000-0000-000000000002"
    )
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]


# ── GET /history: pagination (has_next + next cursor) ───────────────────


def test_history_pagination_has_next_when_over_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """More rows than _PAGE_SIZE sets has_next and renders a next-page link."""
    from taskq.web.admin._constants import (
        _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: need the page size to build exactly one-over rows.
    )

    rows = [
        _history_job_row(
            job_id=UUID(f"00000000-0000-0000-0000-{i:012d}"),
            finished_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        for i in range(_PAGE_SIZE + 1)
    ]
    summary = [StubRecord(status="succeeded", cnt=_PAGE_SIZE + 1)]
    conn = _FetchConn(fetch_results=[rows, summary])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/history?status=succeeded")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "Next page" in text
    # Next cursor id is the last *displayed* row (index _PAGE_SIZE - 1).
    assert "00000000-0000-0000-0000-000000000049" in text


def test_history_no_next_link_when_page_not_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fewer rows than _PAGE_SIZE omits the next-page link."""
    rows = [_history_job_row() for _ in range(3)]
    summary = [StubRecord(status="succeeded", cnt=3)]
    conn = _FetchConn(fetch_results=[rows, summary])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/history?status=succeeded")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Next page" not in response.text  # pyright: ignore[reportUnknownMemberType]


def test_history_next_cursor_uses_null_sentinel_for_null_finished_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A last-displayed row with null finished_at yields the __NULL__ cursor."""
    from taskq.web.admin._constants import (
        _PAGE_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: need page size to place a null-finished row as the last displayed row.
    )

    rows = [
        _history_job_row(
            job_id=UUID(f"00000000-0000-0000-0000-{i:012d}"),
            finished_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        )
        for i in range(_PAGE_SIZE - 1)
    ]
    # The last *displayed* row (index _PAGE_SIZE - 1) has no finished_at so the
    # next cursor uses the __NULL__ sentinel.
    rows.append(
        _history_job_row(
            job_id=UUID("00000000-0000-0000-0000-000000000049"),
            finished_at=None,
        )
    )
    # One extra row pushes us over the page size so has_next is True.
    rows.append(
        _history_job_row(
            job_id=UUID("00000000-0000-0000-0000-000000000050"),
            finished_at=datetime(2025, 1, 1, 11, 0, 0, tzinfo=UTC),
        )
    )
    summary = [StubRecord(status="succeeded", cnt=_PAGE_SIZE + 1)]
    conn = _FetchConn(fetch_results=[rows, summary])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/history?status=succeeded")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "Next page" in text
    assert _CURSOR_NULL_SENTINEL in text


# ── GET /history: total_display formatting ──────────────────────────────


def test_history_total_display_shows_plus_when_over_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary count >= _COUNT_CAP renders the '1000+' form."""
    from taskq.web.admin._history import (
        _COUNT_CAP,  # pyright: ignore[reportPrivateUsage]  # Why: need cap value to build an over-cap summary.
    )

    summary = [StubRecord(status="succeeded", cnt=_COUNT_CAP)]
    conn = _FetchConn(fetch_results=[[], summary])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/history")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "1,000+" in response.text  # pyright: ignore[reportUnknownMemberType]


def test_history_total_display_plain_when_under_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary count below _COUNT_CAP renders the plain comma form."""
    summary = [StubRecord(status="succeeded", cnt=7), StubRecord(status="failed", cnt=3)]
    conn = _FetchConn(fetch_results=[[], summary])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/history")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "10" in text  # total shown = 10
    assert "1,000+" not in text


# ── GET /api/history/stats ──────────────────────────────────────────────


def test_history_stats_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /api/history/stats returns 200 with application/json."""
    client = _build_history_app(_StubPool(), monkeypatch)
    response = client.get("/api/history/stats")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "application/json" in response.headers.get("content-type", "")  # pyright: ignore[reportUnknownMemberType]
    body = response.json()  # pyright: ignore[reportUnknownMemberType]
    assert body == {"actors": []}


def test_history_stats_returns_actor_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /api/history/stats returns actor metric rows from the backend."""
    stats_rows = [
        StubRecord(
            actor="send_email",
            queue="default",
            total=10,
            succeeded=8,
            failed=2,
            cancelled=0,
            crashed=0,
            abandoned=0,
            avg_duration_ms=100,
            p50_duration_ms=90,
            p95_duration_ms=200,
        )
    ]
    conn = _FetchConn(fetch_results=[stats_rows])
    client = _build_history_app(_FetchPool(conn), monkeypatch)
    response = client.get("/api/history/stats")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    body = response.json()  # pyright: ignore[reportUnknownMemberType]
    actors = body["actors"]  # pyright: ignore[reportUnknownMemberType]
    assert len(actors) == 1  # pyright: ignore[reportUnknownMemberType]
    assert actors[0]["actor"] == "send_email"  # pyright: ignore[reportUnknownMemberType]
    assert actors[0]["total"] == 10  # pyright: ignore[reportUnknownMemberType]


# ── _compute_success_rate unit tests ────────────────────────────────────


class TestComputeSuccessRate:
    def test_mixed_terminal(self) -> None:
        assert _compute_success_rate({"succeeded": 8, "failed": 2}) == 80.0

    def test_no_terminal_returns_none(self) -> None:
        assert _compute_success_rate({}) is None

    def test_zero_terminal_returns_none(self) -> None:
        assert _compute_success_rate({"succeeded": 0, "failed": 0}) is None

    def test_all_succeeded(self) -> None:
        assert _compute_success_rate({"succeeded": 5, "failed": 0, "crashed": 0}) == 100.0

    def test_only_failed(self) -> None:
        assert _compute_success_rate({"failed": 10}) == 0.0

    def test_ignores_non_terminal_statuses(self) -> None:
        # pending is not terminal, so only succeeded counts in the denominator.
        assert _compute_success_rate({"pending": 5, "succeeded": 5}) == 100.0

    def test_includes_crashed_and_abandoned(self) -> None:
        result = _compute_success_rate({"succeeded": 5, "crashed": 2, "abandoned": 3})
        assert result == 50.0

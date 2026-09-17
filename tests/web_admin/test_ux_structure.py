"""Structural UX pins for the admin UI.

Covers the navigation/IA and integration surfaces a visual pass cares about:
every page's nav active state, base_path awareness of links and redirects,
the styled history page, the workers table's header/cell alignment, the
pagination bar's disabled states, and the job-detail progress driver being
loaded in polling mode.
"""

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.
from fastapi.testclient import TestClient

from taskq._ids import new_job_id
from taskq.web.admin import (  # Why: importorskip guard must precede.
    create_router,
    setup_admin_state,
)

from . import StubBackend, StubPool, _stub_job_row  # Why: importorskip guard must precede.

_PREFIX = "/taskq"

_PAGES: tuple[str, ...] = (
    "queues",
    "jobs",
    "history",
    "workers",
    "actors",
    "batches",
    "schedules",
    "rate-limits",
    "reservations",
    "leader",
)


def _make_prefixed_app(stub_pool: StubPool, **router_kwargs: object) -> TestClient:
    """TestClient mounting the admin router under a prefix, host-app style.

    Mirrors how a hosting application mounts the bundle: ``base_path`` must
    match the ``include_router`` prefix so every template-built URL resolves.
    """
    bundle = create_router(stub_pool, base_path=_PREFIX, **router_kwargs)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix=_PREFIX)
    return TestClient(app)


def _nav_block(html: str) -> str:
    """Return the top nav's markup (the second <nav> on the page is a page-local tab strip)."""
    return html[html.find("<nav") : html.find("</nav>")]


def _active_nav_hrefs(nav: str) -> list[str]:
    """Hrefs of nav links carrying the active-state class pair."""
    import re

    pairs = re.findall(r'<a href="([^"]+)"\s+class="([^"]*)"', nav)
    return [href for href, cls in pairs if "border-blue-400" in cls]


# ── Navigation / IA ──────────────────────────────────────────────────────


@pytest.mark.parametrize("page", _PAGES)
def test_nav_active_state_marks_exactly_the_served_page(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool, page: str
) -> None:
    """Each page highlights exactly its own nav entry, under the mount prefix."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    resp = client.get(f"{_PREFIX}/{page}")
    assert resp.status_code == 200
    active = _active_nav_hrefs(_nav_block(resp.text))
    assert active == [f"{_PREFIX}/{page}"]


def test_nav_has_no_base_pathless_absolute_links(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """No nav or page link points outside the mount prefix."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    for page in _PAGES:
        html = client.get(f"{_PREFIX}/{page}").text
        import re

        hrefs = re.findall(r'href="([^"]+)"', html)
        stray = [
            h
            for h in hrefs
            if h.startswith("/") and not h.startswith(_PREFIX) and not h.startswith("//")
        ]
        assert stray == [], f"{page} links outside the prefix: {stray}"


def test_bare_prefix_index_redirects_into_the_queues_page(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The mounted index resolves to the queues page through the redirect."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    resp = client.get(f"{_PREFIX}/", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.url.path == f"{_PREFIX}/queues"


# ── History page styling ────────────────────────────────────────────────


def test_history_page_uses_the_shared_table_and_badge_styling(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The history page renders the same card/table chrome as the other list
    pages: it previously shipped semantic-HTML-only markup with class names no
    stylesheet defined, so it rendered as unstyled browser defaults between
    the dark chrome and the footer."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    html = client.get(f"{_PREFIX}/history").text
    assert "bg-white dark:bg-slate-900" in html
    assert "rounded-lg px-4 py-2.5" in html  # metric-card chrome present
    for legacy in ("metrics-bar", "filter-form", "pagination-next", "badge-grey"):
        assert legacy not in html
    # the status-badge macro is the row renderer when rows exist
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    source = bundle.templates.loader.get_source(bundle.templates, "history.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    assert "status_badge(" in source


def test_history_page_renders_uuid_row_ids_from_real_rows(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """A history row's id arrives as a uuid.UUID (the asyncpg decoder), not a
    string: slicing it directly raised TypeError and 500ed the page on the
    first render with data, which the empty-render test cannot see."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from datetime import UTC, datetime

    from taskq._ids import new_uuid

    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    template = bundle.templates.get_template("history.html")
    job_id = new_uuid()
    html = template.render(
        jobs=[
            {
                "id": job_id,
                "actor": "send_email",
                "queue": "email",
                "status": "succeeded",
                "created_at": datetime.now(UTC),
                "started_at": datetime.now(UTC),
                "finished_at": datetime.now(UTC),
                "duration_ms": 1234.0,
                "attempt": 1,
                "max_attempts": 3,
                "retry_kind": "transient",
                "is_archived": True,
            }
        ],
        statuses=[],
        all_statuses=["succeeded", "failed"],
        actor_filter=None,
        queue_filter=None,
        has_next=False,
        next_cursor_at=None,
        next_cursor_id=None,
        summary={"succeeded": 1},
        total_display="1",
        success_rate=100.0,
        realtime_mode="polling",
        mode_label="polling mode",
    )
    assert str(job_id)[:8] in html


def test_history_page_empty_state_is_styled_and_contrast_pinned(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The history empty state uses the readable slate-500/dark-slate-400 pair."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    html = client.get(f"{_PREFIX}/history").text
    assert "text-slate-500 dark:text-slate-400 text-sm" in html


# ── Workers table header/cell alignment ─────────────────────────────────


def test_workers_table_headers_match_the_cell_order(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The thead order must read the row cells' order: NOTIFY, Last Seen,
    Stall hotspots, Running / Max. The headers previously listed the middle
    four columns in a different order than the cells, mislabeling every row."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    env = bundle.templates
    source = env.loader.get_source(env, "workers.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    thead = source[source.find("<thead>") : source.find("</thead>")]
    import re

    labels = re.findall(r">([A-Za-z /]+)</th>", thead)
    assert labels == [
        "Hostname",
        "PID",
        "Queues",
        "NOTIFY",
        "Last Seen",
        "Stall hotspots",
        "Running / Max",
        "Status",
    ]


def test_workers_page_sets_the_active_nav_entry(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """workers.html pins active_page so the nav highlights the Workers tab
    (the actors page shipped without this and never highlighted)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    env = bundle.templates
    source = env.loader.get_source(env, "actors.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    assert 'set active_page = "actors"' in source


# ── Pagination bar ──────────────────────────────────────────────────────


def test_pagination_bar_renders_disabled_ends(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """When only one end of the cursor is available the other renders as an
    aria-disabled control instead of vanishing, so the bar keeps its shape."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    env = bundle.templates
    source = env.loader.get_source(env, "_partials/job_table.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    assert source.count("aria-disabled") == 2


def test_pagination_and_sort_swaps_push_the_url(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """Cursor and sort navigation are deep-linkable: htmx pushes the request
    URL so the filters/sort/page survive a reload and the back button works."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    env = bundle.templates
    source = env.loader.get_source(env, "_partials/job_table.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    assert source.count('hx-push-url="true"') == 3  # sort header macro + both page links
    jobs_source = env.loader.get_source(env, "jobs.html")[0]  # pyright: ignore[reportOptionalMemberAccess]
    assert 'hx-push-url="true"' in jobs_source  # the filter form


# ── Job detail progress driver ──────────────────────────────────────────


def _detail_job_data() -> dict[str, object]:
    """The minimal job mapping job_detail.html renders (test_jobs.py's shape)."""
    return {
        "id": "00000000-0000-0000-0000-000000000010",
        "actor": "sync_data",
        "queue": "default",
        "status": "running",
        "priority": 0,
        "attempt": 1,
        "max_attempts": 3,
        "retry_kind": "transient",
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


def test_job_detail_loads_the_progress_driver_in_polling_mode(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The progress script block renders without Redis too: realtime.js opens
    the SSE stream in real-time mode and polls the state endpoint otherwise,
    so gating the include on realtime_mode left no-Redis deployments with a
    progress section nothing ever updated."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    template = bundle.templates.get_template("job_detail.html")
    html = template.render(job=_detail_job_data(), attempts=[], events=[])
    assert "static/realtime.js" in html
    assert "POLL_INTERVAL_MS" in html


# ── SSE mode probe ──────────────────────────────────────────────────────


def test_sse_mode_endpoint_reports_absent_redis(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """GET /sse/mode answers the page-render verdict through the same cached
    ping the pages use; with no redis client configured it is polling mode."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    resp = client.get(f"{_PREFIX}/sse/mode")
    assert resp.status_code == 200
    assert resp.json() == {"realtime": False}


def test_sse_mode_is_not_shadowed_by_the_topic_route(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The probe is registered before the {topic} catch-all: a route order
    regression would 400 the path the job-detail script polls."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]
    paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]
    assert paths.index("/sse/mode") < paths.index("/sse/{topic}")


def test_unknown_sse_topic_still_400s(monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool) -> None:
    """Adding /sse/mode does not loosen the topic allowlist."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    resp = client.get(f"{_PREFIX}/sse/bogus")
    assert resp.status_code == 400


# ── Cancel redirect under a prefix ──────────────────────────────────────


def test_cancel_redirect_carries_the_base_path(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The cancel POST's 303 lands back on the job page inside the mount:
    the relative ../../ URL it used before only resolved at the root and
    climbed out of a prefixed mount into a 404 (or the host's routes)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    jid = new_job_id()
    backend = StubBackend(job_row=_stub_job_row(jid))
    client = _make_prefixed_app(stub_pool, backend=backend)
    client.get(f"{_PREFIX}/queues")
    token = client.cookies.get("taskq_csrf_token", "")
    resp = client.post(
        f"{_PREFIX}/jobs/{jid}/cancel", data={"csrf_token": token}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == f"{_PREFIX}/jobs/{jid}"


# ── Dark-mode first paint ───────────────────────────────────────────────


def test_dark_class_is_applied_before_first_paint(
    monkeypatch: pytest.MonkeyPatch, stub_pool: StubPool
) -> None:
    """The pre-paint script in <head> reads the same localStorage key the
    toggle persists to, so a dark-mode navigation does not flash white while
    waiting for Alpine to boot."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_prefixed_app(stub_pool)
    html = client.get(f"{_PREFIX}/queues").text
    head = html[: html.find("</head>")]
    assert "documentElement.classList.add" in head
    assert head.find("documentElement.classList.add") < head.find("alpinejs")

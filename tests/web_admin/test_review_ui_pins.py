"""UI-review pins: page-by-page render correctness, refresh transports,
asset paths, and render bounds, written red against the served pages.

This module is the reviewer's pass over every admin page (the review that
produced it served each page and compared what renders against a computed
oracle). The pins it leaves behind hold the four contracts the review found
broken or unproven:

1. the refresh-transport matrix: a page renders with EXACTLY ONE refresh
   transport per mode - the meta refresh for the pages with no JS
   machinery, in EVERY mode (a page with no transport that suppresses the
   meta refresh because the portal badge says "real-time mode" is a frozen
   page wearing a real-time badge), the page's own machinery (admin.js,
   the htmx poll, realtime.js) where one exists;
2. the render bounds: every read-only page caps its SQL like the queues
   overview's pinned 200 cap and says so when the cap bites;
3. the reset confirm never interpolates a bucket name into inline JS;
4. the served asset/script graph resolves under a prefix deployment
   (ASGI ``root_path``), and the page's JS config carries RAW filter
   values (no HTML-entity mangling inside ``<script>``).
"""

from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state

from . import StubRecord

pytestmark = [pytest.mark.fastapi]


# ── Scripted connections ─────────────────────────────────────────────────


class _ClockConn:
    """Connection answering every ``fetch`` with an empty list and every
    ``fetchrow`` with *fetchrow* (None unless given) - the empty-fleet
    render every no-transport page must survive."""

    def __init__(self, fetchrow: StubRecord | None = None) -> None:
        self._fetchrow = fetchrow

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return self._fetchrow

    async def fetchval(self, query: str, *args: object) -> object:
        if "clock_timestamp()" in query:
            return datetime.now(UTC)
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""

    def transaction(self) -> object:  # pragma: no cover - never mutates
        raise NotImplementedError


class _ClockPool:
    def __init__(self, conn: _ClockConn) -> None:
        self._conn = conn
        # The rate-limits page hands registry.peek_all the RAW pool; an
        # empty registry never touches it, so None is a safe placeholder.
        self.pool = None

    def acquire(self, *, timeout: float | None = None) -> Any:
        return _AcqCtx(self._conn)


class _AcqCtx:
    def __init__(self, conn: _ClockConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ClockConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


def _realtime_ctx_override(
    redis_client: object | None = None,
) -> tuple[
    str, str
]:  # Why: mirrors get_realtime_ctx's real signature shape (the param exists so the drift pin sees it; the None default keeps FastAPI from resolving it as a query param); the stub ignores the client - its verdict is deterministic.
    return ("realtime", "real-time mode")


def _force_realtime(modules: list[Any], monkeypatch: pytest.MonkeyPatch) -> None:
    for mod in modules:
        monkeypatch.setattr(mod, "get_realtime_ctx", _realtime_ctx_override)


def _make_client(pool: Any) -> TestClient:
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


# ── 1. the refresh-transport matrix ──────────────────────────────────────
#
# _base.html keyed the meta refresh off the PORTAL badge mode
# (realtime_mode != "realtime"), but only three surfaces own a JS
# transport: /jobs (admin.js), /queues (the htmx poll, realtime mode
# only), and the job detail page (realtime.js, every mode). Every other
# page served with a healthy Redis rendered with NO refresh transport at
# all: a frozen page under a "real-time mode" badge - the healthier the
# deployment, the deader the page. The contract is per-page: the meta
# refresh is suppressed only where a page declares its own transport
# (suppress_refresh), never by the badge mode alone.

_NO_TRANSPORT_PAGES = [
    "/history",
    "/workers",
    "/actors",
    "/batches",
    "/schedules",
    "/rate-limits",
    "/reservations",
    "/leader",
]


def test_pages_without_a_js_transport_keep_the_meta_refresh_in_realtime_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In REALTIME mode a page with no JS transport must still meta-refresh:
    the badge mode alone never suppresses the page's only liveness."""
    import taskq.web.admin.actors as actors_module
    import taskq.web.admin.batches as batches_module
    import taskq.web.admin.history as history_module
    import taskq.web.admin.ops as ops_module
    import taskq.web.admin.workers as workers_module

    _force_realtime(
        [actors_module, batches_module, history_module, ops_module, workers_module],
        monkeypatch,
    )
    client = _make_client(_ClockPool(_ClockConn()))
    for page in _NO_TRANSPORT_PAGES:
        response = client.get(page)
        assert response.status_code == 200, page
        assert 'http-equiv="refresh"' in response.text, (
            f"{page} has no JS transport, so the meta refresh is its only "
            "liveness - real-time mode must not take it away"
        )


def test_job_detail_runs_exactly_one_refresh_transport_per_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job detail page's transport is realtime.js in EVERY mode (its own
    script block says so: polling deployments would otherwise have a
    progress section nothing ever updated). The meta refresh alongside it is
    the #337 double-fetch on this page: polling mode used to render BOTH.
    """
    import taskq.web.admin.jobs as jobs_module

    job_id = "0198e2a7-9c1b-7c6a-b2f5-3f4a5b6c7d8e"
    job = StubRecord(
        id=job_id,
        actor="send_email",
        queue="default",
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC),
        started_at=datetime(2026, 9, 27, 12, 0, 1, tzinfo=UTC),
        finished_at=None,
        progress_state={"step": "stage", "percent": 50},
        progress_seq=2,
        payload=None,
        metadata=None,
        result=None,
        error_class=None,
        error_message=None,
        error_traceback=None,
        tags=[],
        trace_id=None,
        span_id=None,
        locked_by_worker="w12345678",
        lock_expires_at=datetime(2026, 9, 27, 12, 5, 0, tzinfo=UTC),
        cancel_requested_at=None,
        cancel_phase=0,
    )
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=job)))

    polling_html = client.get(f"/jobs/{job_id}").text
    assert f"/jobs/{job_id}/cancel" in polling_html, "the job page rendered"
    assert 'http-equiv="refresh"' not in polling_html, (
        "polling mode's job page runs realtime.js's poll - a meta refresh "
        "alongside it is the double-fetch #337 named"
    )
    assert "realtime.js" in polling_html, "realtime.js is the page's transport"

    _force_realtime([jobs_module], monkeypatch)
    realtime_html = _make_client(_ClockPool(_ClockConn(fetchrow=job))).get(f"/jobs/{job_id}").text
    assert 'http-equiv="refresh"' not in realtime_html
    assert "realtime.js" in realtime_html


# ── 2. the render bounds ─────────────────────────────────────────────────
#
# The queues overview's 200-row cap is pinned (test_queues.py); the same
# review found four read-only pages fetching with NO LIMIT at all: a
# 50k-row cron_schedules / workers / rate_limit_buckets / reservation_slots
# population renders every row of it. Every page gets the batches idiom: a
# LIMIT and a truncation notice that says so.


def test_every_read_only_page_caps_its_rows() -> None:
    """No read-only page's SQL may be unbounded: schedules, workers,
    rate-limit buckets, reservation buckets and held slots all LIMIT."""
    from taskq.web.admin.ops import (
        _HELD_SLOTS_SQL,
        _RATE_LIMITS_SQL,
        _RESERVATIONS_SQL,
        _SCHEDULES_SQL,
    )
    from taskq.web.admin.workers import _WORKERS_SQL

    for name, sql in (
        ("_SCHEDULES_SQL", _SCHEDULES_SQL),
        ("_WORKERS_SQL", _WORKERS_SQL),
        ("_RATE_LIMITS_SQL", _RATE_LIMITS_SQL),
        ("_RESERVATIONS_SQL", _RESERVATIONS_SQL),
        ("_HELD_SLOTS_SQL", _HELD_SLOTS_SQL),
    ):
        assert re.search(r"LIMIT \d+", sql), (
            f"{name} must cap its row count - a 50k-row table population "
            "renders every row of it otherwise"
        )


def test_schedules_page_announces_when_the_cap_bites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A capped schedules render says so (the batches page's idiom); under
    the cap it must not cry wolf."""
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state as _sas
    from taskq.web.admin.ops import _SCHEDULES_PAGE_SIZE

    rows = [
        StubRecord(
            id=f"00000000-0000-0000-0000-{i:012d}",
            actor=f"actor_{i}",
            cron_expr="* * * * *",
            timezone="UTC",
            enabled=True,
            next_fire_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
            last_fired_at=None,
            last_fire_error=None,
            consecutive_failures=0,
            metadata=None,
        )
        for i in range(_SCHEDULES_PAGE_SIZE)
    ]

    class _SchedConn(_ClockConn):
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "cron_schedules" in query:
                return rows
            return []

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_ClockPool(_SchedConn()))  # pyright: ignore[reportArgumentType]
    app = FastAPI()
    _sas(app, bundle)
    app.include_router(bundle.router)
    client = TestClient(app)
    html = client.get("/schedules").text
    assert "Showing the" in html, "a capped schedules render must say the cap bit"
    assert str(_SCHEDULES_PAGE_SIZE) in html

    class _OneConn(_SchedConn):  # pyright: ignore[reportUnusedClass]  # Why: defined to seed exactly one row; the base's SQL routing is inherited.
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "cron_schedules" in query:
                return rows[:1]
            return []

    bundle = create_router(_ClockPool(_OneConn()))  # pyright: ignore[reportArgumentType]
    app = FastAPI()
    _sas(app, bundle)
    app.include_router(bundle.router)
    assert "Showing the" not in TestClient(app).get("/schedules").text, (
        "an under-cap render must not cry wolf"
    )


def test_held_slots_section_announces_when_its_cap_bites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The held-slots table caps too and says so."""
    from fastapi.testclient import TestClient

    from taskq.web.admin import setup_admin_state as _sas
    from taskq.web.admin.ops import _HELD_SLOTS_PAGE_SIZE

    slots = [
        StubRecord(
            bucket_name="emails",
            slot_index=i,
            job_id=f"0198e2a7-9c1b-7c6a-b2f5-{i:012d}",
            held_by_worker_id="w12345678",
            lease_expires_at=datetime(2026, 9, 27, 12, 5, tzinfo=UTC),
        )
        for i in range(_HELD_SLOTS_PAGE_SIZE)
    ]

    class _SlotsConn(_ClockConn):
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "SELECT bucket_name, slot_index" in query:
                return slots
            if "GROUP BY bucket_name" in query:
                # The bucket aggregate: one registered bucket, so the page's
                # bucket table renders and the held-slots section with it.
                return [StubRecord(bucket_name="emails", held_count=1, free_count=0, total_slots=1)]
            return []

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_ClockPool(_SlotsConn()))  # pyright: ignore[reportArgumentType]
    app = FastAPI()
    _sas(app, bundle)
    app.include_router(bundle.router)
    html = TestClient(app).get("/reservations").text
    assert "Showing the" in html, "a capped held-slots render must say the cap bit"


# ── 3. the reset confirm is not inline-JS interpolation ──────────────────
#
# The rate-limits reset button interpolated the bucket name into an inline
# onclick="return confirm('... {{ bucket }} ...')". Autoescape's &#39; does
# NOT protect it: the HTML parser decodes entities in attribute values
# BEFORE the JS runs, so a bucket name carrying '); breaks the JS string
# and executes. The actors page's delegated-listener idiom (data attribute
# + a submit listener) is the correct shape; this page gets the same.


def test_rate_limit_reset_confirm_never_interpolates_the_bucket_name_into_inline_js(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reset form carries the bucket name as DATA and confirms through a
    delegated listener; no onclick handler, no bucket name inside a JS
    string context."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")

    class _BucketsConn(_ClockConn):
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "rate_limit_buckets" in query:
                return [
                    StubRecord(
                        bucket_name="emails'); alert(1); ('",
                        kind="token_bucket",
                        state={},
                        updated_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
                    )
                ]
            return []

    client = _make_client(_ClockPool(_BucketsConn()))
    html = client.get("/rate-limits").text
    assert "onclick=" not in html, (
        "the reset confirm must not be an inline JS handler: attribute-entity "
        "decoding turns the bucket name into a script breakout"
    )
    assert 'data-bucket-name="emails&#39;); alert(1); (&#39;"' in html, (
        "the bucket name rides as escaped DATA, never inside JS"
    )


# ── 4. asset graph and the JS config's raw values ────────────────────────


def test_scripts_serve_alpine_from_the_shipped_static_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chrome loads Alpine from the SAME local /static dir as htmx: an
    air-gapped ops deployment (the admin UI's natural habitat) must not
    lose every Alpine-driven control - the dark toggle, the status
    combobox, the jobs page's live machinery - to a CDN unreachable."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn()))
    html = client.get("/queues").text
    assert "cdn.jsdelivr.net" not in html, "Alpine must load locally"
    assert "/static/alpine.min.js" in html


def test_lucide_icons_render_on_first_paint_and_survive_htmx_swaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing called lucide.createIcons() at boot: every data-lucide icon
    (the jobs tabs, the row's view action, the empty-state inbox, the
    pagination chevrons) rendered as an EMPTY <i> on first paint - the row
    action was a 28px invisible click target until the first poll swap
    happened to re-create them. The boot must create them, and htmx swaps
    (which replace icon-bearing fragments) must re-create them."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn()))
    for page in ("/jobs", "/queues", "/history"):
        html = client.get(page).text
        assert "lucide.createIcons()" in html, f"{page} must create its icons on first paint"
        assert "htmx:afterSwap" in html, (
            f"{page}'s htmx-swapped fragments must re-create their icons"
        )


def test_jobs_page_js_config_carries_raw_filter_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The jobs page's <script> config interpolated filter values through
    HTML escaping: inside a <script> block entities are never decoded, so
    a search for O'Brien reached the JS as O&#39;Brien - the config the
    live machinery reads carried corrupted data. JS-string-literal
    encoding (| tojson) is the contract for a script context; the pinned
    value is what the BROWSER's JS engine sees, evaluated under Node (the
    repository's only JS runtime, the live-refresh harnesses' idiom)."""
    import json
    import subprocess

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn()))
    html = client.get("/jobs", params={"search": "O'Brien"}).text
    match = re.search(r"window\.__taskqJobConfig = (\{.*?\});", html, re.S)
    assert match is not None, "the jobs page must embed its JS config"
    assert "&#39;" not in match.group(1), (
        "no HTML entity may survive inside the script block: the browser decodes none of them there"
    )
    node = shutil.which("node")
    if node is None:
        pytest.skip("the config's runtime value is evaluated under Node")
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the script is this test's own constant and the config arrives on stdin.
        [
            node,
            "-e",
            'const src = require("fs").readFileSync(0, "utf8");'
            " const window = {};"
            ' const cfg = eval("(" + src + ")");'
            " console.log(JSON.stringify(cfg));",
        ],
        input=match.group(1),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    assert config["search"] == "O'Brien", (
        f"the config the live machinery reads must carry the raw submitted "
        f"value: {config['search']!r}"
    )


# ── 5. the prefix-deployment asset graph (a proof, not a defect) ─────────


def test_every_local_url_on_every_page_resolves_under_a_prefix_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deployed under a non-root root_path (uvicorn --root-path behind a
    non-stripping proxy), with base_path carrying the full external
    prefix, EVERY local URL every page emits must resolve: a 404ing
    stylesheet or script is a deployment-breaking render."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    external_prefix = "/taskq"
    bundle = create_router(_ClockPool(_ClockConn()), base_path=f"{external_prefix}/admin")  # pyright: ignore[reportArgumentType]
    app = FastAPI(root_path=external_prefix)
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app, root_path=external_prefix)

    pages = [
        "/taskq/admin/",
        "/taskq/admin/queues",
        "/taskq/admin/queues/default",
        "/taskq/admin/jobs",
        "/taskq/admin/history",
        "/taskq/admin/workers",
        "/taskq/admin/actors",
        "/taskq/admin/batches",
        "/taskq/admin/schedules",
        "/taskq/admin/rate-limits",
        "/taskq/admin/reservations",
        "/taskq/admin/leader",
    ]
    local_urls: set[str] = set()
    for page in pages:
        response = client.get(page)
        assert response.status_code == 200, page
        for url in re.findall(r'(?:src|href)="([^"]+)"', response.text):
            if url.startswith("/"):
                local_urls.add(url.split("?")[0])

    assert local_urls, "the walk must have collected the pages' local URLs"
    for url in sorted(local_urls):
        response = client.get(url)
        assert response.status_code == 200, (
            f"{url} emitted by a prefix-deployed page does not resolve"
        )

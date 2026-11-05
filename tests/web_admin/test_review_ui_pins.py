"""UI-review pins: page-by-page render correctness, refresh transports,
asset paths, and render bounds, written red against the served pages.

This module is the adversarial pass over every admin page (the pass that
produced it served each page and compared what renders against a computed
oracle). The pins it leaves behind hold the four contracts the review found
broken or unproven:

1. the refresh-transport matrix: a page renders with EXACTLY one refresh
   transport per mode - the meta refresh for the pages with no JS
   machinery, in every mode (a page with no transport that suppresses the
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

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test
# identifier, and every value is $-bound (the tests/web_admin/test_batch_detail_pg.py
# header's own rule for the real-PG seeding this file carries).

from __future__ import annotations

import asyncio
import re
import shutil
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state

from . import StubBackend, StubRecord

pytestmark = [pytest.mark.fastapi]


# ── the scripted clock double ────────────────────────────────────────────
#
# Kept for tests/web_admin/test_review_ui_attack.py, whose hostile-input
# injections ride it: the XSS/attack arms need a page render that survives
# a render-time clock probe, and the attacks themselves (metacharacter
# rows no honest writer produces) are that pack's declared fault
# injections. The converted pins above no longer use it - their pages
# answer from the real schema.


class _ClockConn:
    """Connection answering every ``fetch`` with an empty list and every
    ``fetchval`` with the server clock - the shared conn the attack pack's
    scripted renders must survive."""

    def __init__(self, fetchrow: StubRecord | None = None) -> None:
        self._fetchrow = fetchrow

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return self._fetchrow

    async def fetchval(self, query: str, *args: object) -> object:
        norm = " ".join(query.split())
        if norm == "SELECT clock_timestamp()":
            return datetime.now(UTC)
        if "SELECT EXISTS" in norm and ".workers" in norm:
            # The queue-detail page's has-alive-worker probe: no workers
            # row exists in the scripted world.
            return False
        if "SELECT COUNT(*)" in norm and ".jobs" in norm:
            # The jobs pages' header count: an empty filtered set.
            return 0
        if "jobs_archive" in norm and "batch_id" in norm:
            # The batch drilldown's archived-member count: none.
            return 0
        raise AssertionError(f"unmatched statement: {query}")

    async def execute(self, query: str, *args: object) -> str:
        return ""

    def transaction(self) -> object:  # pragma: no cover - never mutates
        raise NotImplementedError


class _ClockPool:  # pyright: ignore[reportUnusedClass]  # Why: the attack pack imports it (from .test_review_ui_pins import _ClockConn, _ClockPool); pyright sees only this module's uses.
    def __init__(self, conn: _ClockConn) -> None:
        self._conn = conn
        # The rate-limits page hands registry.peek_all the raw pool; an
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


# ── the real-PG tier ─────────────────────────────────────────────────────
#
# The render pins drive the served pages over the module's real pool (the
# tests/test_web_admin_integration.py pattern): every page's reads answer
# from the real migrated schema (an empty one for the render-only pins, a
# seeded one where the pin names rows), so the transports, caps and asset
# graphs render against the same statements production issues.


def _admin_app(
    pool: asyncpg.Pool,
    schema: str,
    base_path: str = "",
    router_prefix: str = "",
    **router_kwargs: object,
) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path=base_path, **router_kwargs)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix=router_prefix)
    return app


async def _get(pool: asyncpg.Pool, schema: str, path: str, **kwargs: object) -> Any:
    """GET *path* from the admin app on the real pool; *kwargs* beyond
    httpx's own ride to the router (backend, registry, prefix)."""
    router_kwargs: dict[str, object] = {}
    client_kwargs: dict[str, object] = {}
    for key, value in kwargs.items():
        if key in ("backend", "rate_limit_registry", "router_prefix", "base_path"):
            router_kwargs[key] = value
        else:
            client_kwargs[key] = value
    app = _admin_app(pool, schema, **router_kwargs)  # type: ignore[arg-type]  # Why: the router kwargs are the create_router surface; httpx's are the client's.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        **client_kwargs,  # type: ignore[arg-type]  # Why: httpx.ASGITransport's root_path rides the same kwargs.
    ) as client:
        return await client.get(path)


async def _seed_running_job(conn: asyncpg.Connection, schema: str, *, jid: Any) -> None:
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, attempt, scheduled_at, schedule_to_close,
                locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
                progress_state, progress_seq)
            VALUES ($1, 'send_email', 'default', '{{}}'::jsonb, 3, 'transient',
                'running', 0, 1, now(), now() + interval '60 seconds',
                $2, now() + interval '60 seconds', now(), now(),
                '{{"step": "stage", "percent": 50}}'::jsonb, 2)""",
        jid,
        new_uuid(),
    )


def _realtime_ctx_override(
    redis_client: object | None = None,
) -> tuple[
    str, str
]:  # Why: mirrors get_realtime_ctx's real signature shape (the param exists so the drift pin sees it; the None default keeps FastAPI from resolving it as a query param); the stub ignores the client - its verdict is deterministic.
    return ("realtime", "real-time mode")


def _force_realtime(modules: list[Any], monkeypatch: pytest.MonkeyPatch) -> None:
    for mod in modules:
        monkeypatch.setattr(mod, "get_realtime_ctx", _realtime_ctx_override)


# ── 1. the refresh-transport matrix ──────────────────────────────────────
#
# _base.html keyed the meta refresh off the portal badge mode
# (realtime_mode != "realtime"), but only three surfaces own a JS
# transport: /jobs (admin.js), /queues (the htmx poll, realtime mode
# only), and the job detail page (realtime.js, every mode). Every other
# page served with a healthy Redis rendered with no refresh transport at
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


async def test_pages_without_a_js_transport_keep_the_meta_refresh_in_realtime_mode(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
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
    schema = module_pg_schema.schema_name
    for page in _NO_TRANSPORT_PAGES:
        response = await _get(module_pg_pool, schema, page)
        assert response.status_code == 200, page
        assert 'http-equiv="refresh"' in response.text, (
            f"{page} has no JS transport, so the meta refresh is its only "
            "liveness - real-time mode must not take it away"
        )


async def test_job_detail_runs_exactly_one_refresh_transport_per_mode(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job detail page's transport is realtime.js in every mode (its own
    script block says so: polling deployments would otherwise have a
    progress section nothing ever updated). The meta refresh alongside it is
    the #337 double-fetch on this page: polling mode must never render both.
    """
    import taskq.web.admin.jobs as jobs_module

    schema = module_pg_schema.schema_name
    job_id = new_uuid()
    await _seed_running_job(clean_pg_conn, schema, jid=job_id)

    polling_html = (
        await _get(module_pg_pool, schema, f"/jobs/{job_id}", backend=StubBackend())
    ).text
    assert f"/jobs/{job_id}/cancel" in polling_html, "the job page rendered"
    assert 'http-equiv="refresh"' not in polling_html, (
        "polling mode's job page runs realtime.js's poll - a meta refresh "
        "alongside it is the double-fetch #337 named"
    )
    assert "realtime.js" in polling_html, "realtime.js is the page's transport"

    _force_realtime([jobs_module], monkeypatch)
    realtime_html = (
        await _get(module_pg_pool, schema, f"/jobs/{job_id}", backend=StubBackend())
    ).text
    assert 'http-equiv="refresh"' not in realtime_html
    assert "realtime.js" in realtime_html


# ── 2. the render bounds ─────────────────────────────────────────────────
#
# The queues overview's 200-row cap is pinned (test_queues.py); the same
# review found four read-only pages fetching with no LIMIT at all: a
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


async def test_schedules_page_announces_when_the_cap_bites(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A capped schedules render says so (the batches page's idiom); under
    the cap it must not cry wolf. The schedules are real rows - the cap the
    read enforces is the real LIMIT."""
    from taskq.web.admin.ops import _SCHEDULES_PAGE_SIZE

    schema = module_pg_schema.schema_name
    await clean_pg_conn.executemany(
        f"""INSERT INTO "{schema}".cron_schedules (
                id, actor, cron_expr, timezone, enabled, next_fire_at)
            VALUES ($1, $2, '* * * * *', 'UTC', true, now())""",
        [(new_uuid(), f"actor_{i}") for i in range(_SCHEDULES_PAGE_SIZE)],
    )
    html = (await _get(module_pg_pool, schema, "/schedules")).text
    assert "Showing the" in html, "a capped schedules render must say the cap bit"
    assert str(_SCHEDULES_PAGE_SIZE) in html

    # Under the cap: one schedule row, no notice.
    await clean_pg_conn.execute(f'DELETE FROM "{schema}".cron_schedules')
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".cron_schedules (
                id, actor, cron_expr, timezone, enabled, next_fire_at)
            VALUES ($1, 'one_actor', '* * * * *', 'UTC', true, now())""",
        new_uuid(),
    )
    html = (await _get(module_pg_pool, schema, "/schedules")).text
    assert "Showing the" not in html, "an under-cap render must not cry wolf"


async def test_held_slots_section_announces_when_its_cap_bites(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The held-slots table caps too and says so. The slots are real rows -
    the held-slots read's real LIMIT and the bucket aggregate's real
    roll-up serve the section."""
    from taskq.web.admin.ops import _HELD_SLOTS_PAGE_SIZE

    schema = module_pg_schema.schema_name
    now = datetime.now(UTC)
    await clean_pg_conn.executemany(
        f"""INSERT INTO "{schema}".reservation_slots
                (bucket_name, slot_index, job_id, held_by_worker_id,
                 acquired_at, lease_expires_at)
            VALUES ('emails', $1, $2, $3, $4, $5)""",
        [
            (i, new_uuid(), new_uuid(), now, now + timedelta(minutes=5))
            for i in range(_HELD_SLOTS_PAGE_SIZE)
        ],
    )
    html = (await _get(module_pg_pool, schema, "/reservations")).text
    assert "Showing the" in html, "a capped held-slots render must say the cap bit"


# ── 3. the reset confirm is not inline-JS interpolation ──────────────────
#
# The rate-limits reset button interpolated the bucket name into an inline
# onclick="return confirm('... {{ bucket }} ...')". Autoescape's &#39; does
# not protect it: the HTML parser decodes entities in attribute values
# before the JS runs, so a bucket name carrying '); breaks the JS string
# and executes. The actors page's delegated-listener idiom (data attribute
# + a submit listener) is the correct shape; this page gets the same.


async def test_rate_limit_reset_confirm_never_interpolates_the_bucket_name_into_inline_js(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reset form carries the bucket name as DATA and confirms through a
    delegated listener; no onclick handler, no bucket name inside a JS
    string context."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")

    # U5: the reset form renders only for buckets this process's registry
    # knows, so the hostile name rides a registered primitive (the PG row
    # adds the same name as worker-published state, read-only there - the
    # form is the registry bucket's). The row is a real row: the bucket
    # column is text, and a name carrying metacharacters is a state an
    # honest writer can produce.
    from datetime import timedelta as _td

    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    hostile_name = "emails'); alert(1); ('"
    rl_registry = RateLimitRegistry()
    rl_registry.register(
        TokenBucket(
            name=hostile_name,
            capacity=3,
            refill_per_second=1.0,
            backend="memory",
            ttl=_td(seconds=10),
        )
    )
    schema = module_pg_schema.schema_name
    await clean_pg_conn.execute(
        f"""INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state)
            VALUES ($1, 'token_bucket', '{{}}'::jsonb)""",
        hostile_name,
    )

    app = _admin_app(module_pg_pool, schema, rate_limit_registry=rl_registry)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        html = (await client.get("/rate-limits")).text
    assert "onclick=" not in html, (
        "the reset confirm must not be an inline JS handler: attribute-entity "
        "decoding turns the bucket name into a script breakout"
    )
    assert 'data-bucket-name="emails&#39;); alert(1); (&#39;"' in html, (
        "the bucket name rides as escaped DATA, never inside JS"
    )


# ── 4. asset graph and the JS config's raw values ────────────────────────


async def test_scripts_serve_alpine_from_the_shipped_static_dir(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The chrome loads Alpine from the same local /static dir as htmx: an
    air-gapped ops deployment (the admin UI's natural habitat) must not
    lose every Alpine-driven control - the dark toggle, the status
    combobox, the jobs page's live machinery - to a CDN unreachable."""
    html = (await _get(module_pg_pool, module_pg_schema.schema_name, "/queues")).text
    assert "cdn.jsdelivr.net" not in html, "Alpine must load locally"
    assert "/static/alpine.min.js" in html


async def test_lucide_icons_render_on_first_paint_and_survive_htmx_swaps(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Nothing called lucide.createIcons() at boot: every data-lucide icon
    (the jobs tabs, the row's view action, the empty-state inbox, the
    pagination chevrons) rendered as an EMPTY <i> on first paint - the row
    action was a 28px invisible click target until the first poll swap
    happened to re-create them. The boot must create them, and htmx swaps
    (which replace icon-bearing fragments) must re-create them."""
    schema = module_pg_schema.schema_name
    for page in ("/jobs", "/queues", "/history"):
        html = (await _get(module_pg_pool, schema, page)).text
        assert "lucide.createIcons()" in html, f"{page} must create its icons on first paint"
        assert "htmx:afterSwap" in html, (
            f"{page}'s htmx-swapped fragments must re-create their icons"
        )


async def test_jobs_page_js_config_carries_raw_filter_values(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
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

    html = (
        await _get(
            module_pg_pool, module_pg_schema.schema_name, "/jobs", params={"search": "O'Brien"}
        )
    ).text
    match = re.search(r"window\.__taskqJobConfig = (\{.*?\});", html, re.S)
    assert match is not None, "the jobs page must embed its JS config"
    assert "&#39;" not in match.group(1), (
        "no HTML entity may survive inside the script block: the browser decodes none of them there"
    )
    node = shutil.which("node")
    if node is None:
        pytest.skip("the config's runtime value is evaluated under Node")
    result = await asyncio.to_thread(
        subprocess.run,  # Why: fixed argv, no shell; the script is this test's own constant and the config arrives on stdin.
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


async def test_every_local_url_on_every_page_resolves_under_a_prefix_deployment(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Deployed under a non-root root_path (uvicorn --root-path behind a
    non-stripping proxy), with base_path carrying the full external
    prefix, every local URL every page emits must resolve: a 404ing
    stylesheet or script is a deployment-breaking render."""
    schema = module_pg_schema.schema_name
    external_prefix = "/taskq"
    app = _admin_app(
        module_pg_pool,
        schema,
        base_path=f"{external_prefix}/admin",
        router_prefix="/admin",
    )
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
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, root_path=external_prefix),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        for page in pages:
            response = await client.get(page)
            assert response.status_code == 200, page
            for url in re.findall(r'(?:src|href)="([^"]+)"', response.text):
                if url.startswith("/"):
                    local_urls.add(url.split("?")[0])

        assert local_urls, "the walk must have collected the pages' local URLs"
        for url in sorted(local_urls):
            response = await client.get(url)
            assert response.status_code == 200, (
                f"{url} emitted by a prefix-deployed page does not resolve"
            )

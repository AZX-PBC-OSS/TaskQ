# ruff: noqa: S608  # Why: every schema interpolation is a fixed per-run test identifier, and every value is $-bound.
"""The admin UI's RENDERED layer and public job API, on a hypertable-converted schema.

The sibling module (``test_admin_on_hypertables.py``) proved the SERVER side:
the routes answer identically on a converted schema. This module extends the
proof to the layer the operator and the integrator actually touch: the pages'
JavaScript behaviour and the ``/jobs/api/...`` wire surface — driven against
pages SERVED from a hypertable-converted schema and compared, surface by
surface, against the SAME pages served from a vanilla schema. Both engines are
seeded IDENTICALLY (one seed plan, applied twice) on one TimescaleDB container,
so every differential is exact and every JS assertion has the DOM as its
oracle. The JS itself is driven under Node with the browser stubbed — the
``tests/web_admin`` harness idiom (a stub browser whose mutations and requests
are logged); the harnesses here are the existing ones parameterised by the
SERVED page data, not a new framework.

The operator/user question each test answers:

* served jobs page (1): "does the page I open configure the live table
  identically when the rows come from hypertables?" — the real-time badge,
  the ``window.__taskqJobConfig`` script and the mode probe, engine-diffed.
* stalled broker (2, the #487 contract): "when the events broker stalls, does
  my table keep updating calmly — and identically?" — SSE dies, the poll
  carries the page, the swap payloads do not churn, the EventSource is left
  to reconnect (closing it made the drop permanent).
* status events (3): "when a job changes under me, does the row update —
  locally when the payload carries the status, from the server when it does
  not (the cancel NOTIFY names the job only) — identically?"
* live refresh (4): "when a new job is enqueued while I watch, does it reach
  the RENDERED table through the poll's swap?" — the swap is driven with the
  real HX partial each engine serves. This test was RED against the stock
  page: the served HX fragment carried no ``#job-table-container`` element
  for ``refreshTable`` to find, so the live poll fetched forever and
  rendered never — the red proof behind the template fix that puts the
  container id on the fragment's own root.
* progress cadence (5): "is the detail page's poll calm on hypertable-served
  bodies — 304 when nothing changed, zero DOM writes on a re-flushed
  snapshot, a patch of exactly the changed nodes for a real change —
  identically?"
* frames (6): "do live progress frames render once, dedup a duplicate replay
  and drop a trailing sequence (the BigInt cursor gate) — identically?"
* terminal (7, 8): "when the job finishes, does the page render the terminal
  state and stand down (and, with no progress at all — the common shape —
  write nothing while still standing down), identically?"
* BigInt boundary (9): "does a sequence at the double-precision boundary
  still render (the ETag carries the exact digits), identically?"
* the public API (10-13): "is the wire surface third-party integrations see —
  the poll-state snapshot/304/ETag semantics with the terminal rows' honest
  exception, the SSE snapshot-then-live frames, the reconnect-at-cursor
  behaviour, the terminal close — engine-identical?"
* chunk boundary (14): "does a job list paginating across a TimescaleDB chunk
  boundary render the same rows, in the same order, through the same JS,
  identically to a schema with no chunks at all?" — with the chunk premise
  proven against the converted schema's own chunk inventory.
* the orphaned-attempt window (15): "the hypertable-only data shape (an
  attempt row outliving its dropped parent) — does it surface NOWHERE: not in
  the walks, not in the API, not on the pages, identically?"
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shutil
import subprocess
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")

from fastapi import FastAPI  # Why: importorskip guards the optional extra first.

from taskq._ids import new_job_id, new_uuid
from taskq.constants import progress_channel
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import (
    creator_labels,
    skip_test_without_docker,
)
from taskq.timescale import enable_hypertables
from taskq.web.admin import create_router, setup_admin_state

pytestmark = pytest.mark.integration

ADMIN_JS = Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "admin.js"
REALTIME_JS = (
    Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "realtime.js"
)

_TIMESCALE_IMAGE = (
    os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or "timescale/timescaledb:2.30.1-pg18"
)
_REDIS_IMAGE = "redis:7"

# Every seeded timestamp derives from this FIXED instant (the differential
# requires both engines to seed byte-identical rows).
_BASE = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)

# The running job's progress state, seeded as jsonb: it carries the shapes
# the JS fingerprint machine must treat identically on both engines — a
# timestamptz TEXT render (PG's own spelling) and the null patterns (a null
# field, and nulls inside a nested list) the UI's data can hold.
_R_STATE = (
    '{"percent": 42, "step": "upload", '
    '"data": {"eta": "2026-09-20 12:34:56.789012+00:00", "note": null, '
    '"retries": [{"at": null}, {"at": 3}]}}'
)
_T1_STATE = '{"percent": 100, "step": "finalize", "detail": null}'

# The archived-tab population: the newest 50 rows all finish inside ONE
# day-chunk, the oldest 5 inside a different, older day-chunk — so the
# page-size-50 pagination boundary IS a chunk boundary (pinned on the
# converted schema's chunk inventory in the chunk test).
_N_NEW = 50  # the admin page size is 50: these fill page 1 exactly
_N_OLD = 5  # these are page 2, on the older chunk
_N_ARCHIVE = _N_NEW + _N_OLD

_PAGE_SIZE = 50


# ── The seed plan (built ONCE, applied to BOTH engines identically) ──────


@dataclass(frozen=True, slots=True)
class _ArchiveRow:
    id: uuid.UUID
    status: str
    finished_at: datetime

    @property
    def created_at(self) -> datetime:
        return self.finished_at - timedelta(minutes=10)


@dataclass(slots=True)
class _SeedPlan:
    worker_id: uuid.UUID
    running: uuid.UUID  # live, running, rich progress_state, seq 7
    terminal_progress: uuid.UUID  # live, succeeded, progress_state, seq 12
    terminal_null: uuid.UUID  # live, succeeded, progress_state NULL, seq 3
    # live, running at seed; the terminal-render test flips it terminal
    # mid-test on both engines (a terminal state the page has NOT rendered
    # is the only one the render can be pinned on).
    late_terminal: uuid.UUID = None  # type: ignore[assignment]
    archive: list[_ArchiveRow] = field(default_factory=list)


def _build_plan() -> _SeedPlan:
    plan = _SeedPlan(
        worker_id=new_uuid(),
        running=new_job_id(),
        terminal_progress=new_job_id(),
        terminal_null=new_job_id(),
        late_terminal=new_job_id(),
    )
    for i in range(_N_ARCHIVE):
        if i < _N_OLD:  # the OLDEST rows: the other side of the chunk boundary
            finished = _BASE - timedelta(days=3) + timedelta(minutes=10 * i)
        else:  # the newest 50: page 1 of the archived tab, one chunk
            finished = _BASE - timedelta(days=1) + timedelta(minutes=10 * (i - _N_OLD))
        if i % 11 == 0:
            status = "cancelled"
        elif i % 3 == 0:
            status = "failed"
        else:
            status = "succeeded"
        plan.archive.append(_ArchiveRow(id=new_job_id(), status=status, finished_at=finished))
    return plan


# ── Seeding (both engines, identical bytes) ───────────────────────────────


async def _seed_schema(conn: asyncpg.Connection, schema: str, plan: _SeedPlan) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, last_seen_at, metadata)
        VALUES ($1, 'worker-alpha', 101, '{{default}}'::text[], $2, '{{}}'::jsonb)""",
        plan.worker_id,
        now,
    )

    # The live census: running (with the rich progress state), succeeded
    # WITH a progress state, succeeded with a NULL one (the null pattern),
    # and a second running job the terminal-render test flips terminal
    # mid-test.
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, created_at, scheduled_at, started_at,
            locked_by_worker, lock_expires_at, last_heartbeat_at,
            progress_state, progress_seq)
        VALUES ($1, 'js_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient', 'running',
            1, $2, $2, $3, $4, $5, $6, $7::jsonb, 7)""",
        plan.running,
        _BASE + timedelta(hours=1),
        _BASE + timedelta(hours=1, minutes=1),
        plan.worker_id,
        _BASE + timedelta(hours=2),
        _BASE + timedelta(hours=1, minutes=2),
        _R_STATE,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, created_at, scheduled_at, started_at,
            locked_by_worker, lock_expires_at, last_heartbeat_at,
            progress_state, progress_seq)
        VALUES ($1, 'js_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient', 'running',
            1, $2, $2, $3, $4, $5, $6, '{{"percent": 10, "step": "start"}}'::jsonb, 5)""",
        plan.late_terminal,
        _BASE + timedelta(hours=4),
        _BASE + timedelta(hours=4, minutes=1),
        plan.worker_id,
        _BASE + timedelta(hours=5),
        _BASE + timedelta(hours=4, minutes=2),
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, created_at, scheduled_at, started_at,
            finished_at, progress_state, progress_seq)
        VALUES ($1, 'js_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient', 'succeeded',
            1, $2, $2, $3, $4, $5::jsonb, 12)""",
        plan.terminal_progress,
        _BASE + timedelta(hours=2),
        _BASE + timedelta(hours=2, minutes=1),
        _BASE + timedelta(hours=2, minutes=5),
        _T1_STATE,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, created_at, scheduled_at, started_at,
            finished_at, progress_state, progress_seq)
        VALUES ($1, 'js_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient', 'succeeded',
            1, $2, $2, $3, $4, '{{}}'::jsonb, 3)""",
        plan.terminal_null,
        _BASE + timedelta(hours=3),
        _BASE + timedelta(hours=3, minutes=1),
        _BASE + timedelta(hours=3, minutes=5),
    )

    # The archived population (the hypertable's own table): fixed stamps,
    # mixed statuses, error_class NULL on every succeeded row (the null
    # pattern the attempt cells render).
    await conn.executemany(
        f"""INSERT INTO {schema}.jobs_archive (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, tags, created_at, scheduled_at,
            started_at, finished_at, error_class, archived_at, expire_at)
        VALUES ($1, 'archive_actor', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient', $2,
            1, '{{gamma}}'::text[], $3, $3, $4, $5, $6, $7, $8)""",
        [
            (
                r.id,
                r.status,
                r.created_at,
                r.created_at + timedelta(minutes=1),
                r.finished_at,
                "ValueError" if r.status == "failed" else None,
                r.finished_at + timedelta(minutes=1),
                r.finished_at + timedelta(days=365),
            )
            for r in plan.archive
        ],
    )

    # Attempts for the first three archive rows: the succeeded outcomes
    # carry NULL error_class (the archived detail page's null pattern).
    await conn.executemany(
        f"""INSERT INTO {schema}.job_attempts_archive (job_id, attempt, started_at,
            finished_at, outcome, error_class)
        VALUES ($1, $2, $3, $4, $5, $6)""",
        [
            (
                r.id,
                attempt,
                r.created_at + timedelta(minutes=attempt),
                r.finished_at - timedelta(minutes=1),
                "succeeded" if (attempt == 1 and r.status == "succeeded") else "failed",
                None if (attempt == 1 and r.status == "succeeded") else "ValueError",
            )
            for r in plan.archive[:3]
            for attempt in (1, 2)
        ],
    )


# ── Containers, schemas, pools, apps (one module-wide lab) ───────────────


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def ts_dsn(timescale_container: Any) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


@pytest.fixture(scope="module")
def redis_broker() -> Iterator[Any]:
    """A real broker for the progress SSE bridge (module lifetime)."""
    skip_test_without_docker()
    from testcontainers.community.redis import RedisContainer

    with RedisContainer(image=_REDIS_IMAGE).with_kwargs(labels=creator_labels()) as container:
        yield container


@dataclass
class _Engine:
    schema: str
    app: FastAPI


@dataclass
class _Lab:
    dsn: str
    plan: _SeedPlan
    vanilla: _Engine
    ht: _Engine
    ht_redis_app: FastAPI
    van_redis_app: FastAPI
    redis_url: str


@pytest.fixture(scope="module")
async def lab(ts_dsn: str, redis_broker: Any) -> AsyncIterator[_Lab]:
    """Both engines, built once: migrate BOTH schemas, convert one to
    hypertables (a 4-day archive retention sizes the chunk interval at one
    day — the boundary the pagination test walks), seed both identically,
    and mount the real admin app on each — with and without Redis (the
    progress stream's two configurations). The dev-env trio is set HERE
    because ``create_router`` loads settings at module-fixture time."""
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    try:
        run_tag = new_uuid().hex[:10]
        van_schema = f"tsjs_van_{run_tag}"
        ht_schema = f"tsjs_ht_{run_tag}"

        setup_conn = await asyncpg.connect(ts_dsn)
        try:
            for schema in (van_schema, ht_schema):
                await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                await apply_pending(setup_conn, schema=schema)
            settings = WorkerSettings.load_from_dict(
                {
                    "TASKQ_PG_DSN": ts_dsn,
                    "TASKQ_SCHEMA_NAME": ht_schema,
                    "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
                    "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(timedelta(days=4).total_seconds())}s",
                    "TASKQ_EVENT_RETENTION_PERIOD": f"{int(timedelta(days=7).total_seconds())}s",
                }
            )
            report = await enable_hypertables(setup_conn, schema=ht_schema, settings=settings)
            assert set(report.converted) == {
                "job_events",
                "jobs_archive",
                "job_attempts_archive",
            }
            policy_jobs = await setup_conn.fetch(
                "SELECT job_id FROM timescaledb_information.jobs "
                "WHERE hypertable_schema = $1 AND proc_name LIKE 'policy%'",
                ht_schema,
            )
            for j in policy_jobs:
                await setup_conn.execute(
                    "SELECT alter_job($1, next_start => $2::timestamptz)",
                    j["job_id"],
                    datetime.now(UTC) + timedelta(days=3650),
                )
        finally:
            await setup_conn.close()

        plan = _build_plan()
        for schema in (van_schema, ht_schema):
            conn = await asyncpg.connect(ts_dsn)
            try:
                await _seed_schema(conn, schema, plan)
            finally:
                await conn.close()

        def _mount(pool: asyncpg.Pool, schema: str, redis_client: Any = None) -> FastAPI:
            bundle = create_router(
                pool, schema=schema, redis_client=redis_client, base_path="/admin"
            )
            app = FastAPI()
            setup_admin_state(app, bundle)
            app.include_router(bundle.router, prefix="/admin")
            return app

        van_pool = await asyncpg.create_pool(ts_dsn, min_size=1, max_size=8)
        ht_pool = await asyncpg.create_pool(ts_dsn, min_size=1, max_size=8)
        import redis.asyncio as redis_asyncio

        redis_url = (
            f"redis://{redis_broker.get_container_host_ip()}:"
            f"{redis_broker.get_exposed_port(6379)}/0"
        )
        redis_client = redis_asyncio.from_url(redis_url)
        try:
            lab = _Lab(
                dsn=ts_dsn,
                plan=plan,
                vanilla=_Engine(van_schema, _mount(van_pool, van_schema)),
                ht=_Engine(ht_schema, _mount(ht_pool, ht_schema)),
                ht_redis_app=_mount(ht_pool, ht_schema, redis_client=redis_client),
                van_redis_app=_mount(van_pool, van_schema, redis_client=redis_client),
                redis_url=redis_url,
            )
            yield lab
        finally:
            await van_pool.close()
            await ht_pool.close()
            await redis_client.aclose()
    finally:
        mp.undo()


# ── HTTP helpers (the web_admin idiom: httpx ASGI transport) ─────────────


async def _get(app: FastAPI, path: str, **kwargs: Any) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.get(path, **kwargs)


async def _html(app: FastAPI, path: str, **kwargs: Any) -> str:
    resp = await _get(app, path, **kwargs)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.text


def _diff(label: str, van: Any, ht: Any) -> None:
    """The differential itself: engine-identical means EXACTLY equal."""
    assert van == ht, f"the engines disagree on {label}:\nvanilla={van!r}\nhypertable={ht!r}"


# ── Extraction: the page data the JS consumes, read off the SERVED html ──

_BADGE_RE = re.compile(r'class="taskq-badge[^"]*"\s*data-mode="([^"]*)">([^<]*)<')
_CONFIG_RE = re.compile(r"(window\.__taskqJobConfig = \{.*?\};)", re.DOTALL)
_SECTION_RE = re.compile(
    r'id="progress-section"\s*data-job-id="([0-9a-f-]+)"\s*'
    r'data-progress-seq="(\d+)"\s*data-progress-state=\'([^\']*)\''
)
_ROW_RE = re.compile(r'data-job-id="([0-9a-f-]{36})"\s*data-status="([a-z]+)"')
_NEXT_LINK_RE = re.compile(r'<a href="([^"]+)"[^>]*>\s*Next(?: page)?\s*<')
_POLL_MS_RE = re.compile(r"const POLL_INTERVAL_MS = (\d+);")
_BASE_PATH_RE = re.compile(r'window\.TASKQ_BASE_PATH = "([^"]*)"')
# The relative-time prose two requests milliseconds apart can disagree on at
# a humanize boundary — stripped before whole-page differentials so the
# comparison tests structure, not wall-clock prose (the sibling module's
# _stable_html contract, minus the absolute stamps: those stay IN).
_VOLATILE_PROSE_RE = re.compile(
    r'name="csrf_token"[^>]*value="[^"]*"|value="[^"]*"[^>]*name="csrf_token"'
    r"|\bjust now\b|\b\d+ (?:seconds?|minutes?|hours?|days?) ago\b"
    r"|\bin \d+ (?:seconds?|minutes?|hours?|days?)\b"
)


def _stable(html: str) -> str:
    return _VOLATILE_PROSE_RE.sub("…", html)


def _badge(html: str) -> tuple[str, str]:
    m = _BADGE_RE.search(html)
    assert m is not None, "every admin page renders the real-time badge"
    return (m.group(1), m.group(2))


def _config_script(html: str) -> str:
    m = _CONFIG_RE.search(html)
    assert m is not None, "the jobs page must carry the component's config script"
    return m.group(1)


def _config_poll_ms(config_script: str) -> int:
    m = re.search(r"pollIntervalMs: (\d+)", config_script)
    assert m is not None, "the config script must carry the poll cadence"
    return int(m.group(1))


def _section(html: str) -> dict[str, str]:
    m = _SECTION_RE.search(html)
    assert m is not None, "the job detail page must carry the progress section seed"
    return {"job_id": m.group(1), "seq": m.group(2), "state_json": m.group(3)}


def _rows(html: str) -> list[tuple[str, str]]:
    return _ROW_RE.findall(html)


def _diff_served_page(label: str, van_html: str, ht_html: str) -> None:
    """Whole-page differential: everything but the CSRF token and the
    humanized prose — the absolute timestamptz renders stay IN, they are
    part of what must be identical."""
    _diff(label, _stable(van_html), _stable(ht_html))


# ── The Node harnesses (the existing web_admin idiom, page-parameterised) ─

#
# Both harnesses keep the established shape: the module source is read from
# disk, the browser surface is stubbed with logging stubs, timers are
# virtual, and the log — requests and DOM mutations — is the assertion. The
# one extension: the seed data (config script, section attributes, scripted
# poll bodies, SSE frames, the served HX fragment) arrives as a JSON page
# argument captured from the SERVED pages, so the same JS runs against
# vanilla-served and hypertable-served data.


def _run_node(
    harness: str, js_path: Path, scenario: str, page: dict[str, Any]
) -> dict[str, list[str]]:
    node = shutil.which("node")
    if node is None:
        if os.environ.get("CI"):
            pytest.fail(
                "node is not on PATH in CI: the admin JS tests would skip silently; "
                "the workflow's setup-node step is missing or broken"
            )
        pytest.skip("admin.js/realtime.js behaviour is driven under Node")
    # No per-spawn wall-clock deadline on purpose. The harness is fully
    # virtual (scripted fetches, stubbed timers, a synchronous log), so the
    # child's exit is the only event worth waiting for, and a fixed deadline
    # is a delay that races child startup, not a behaviour gate (the
    # sibling harnesses' rationale). A wedged child is the suite-wide
    # pytest-timeout budget's job (--timeout=300 in addopts): it fails the
    # hung test by name with a stack dump.
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the harness and scenario names are this file's own constants.
        [node, "-e", harness, "--", str(js_path), scenario, json.dumps(page)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Surface the child's stderr: a harness crash (a stub drift, a
        # scenario typo) must name the JS error, not exit status 1.
        pytest.fail(
            f"the harness Node process exited {result.returncode} "
            f"(its stderr follows)\n{result.stderr}"
        )
    return json.loads(result.stdout)


# The admin.js driver: the jobs-page component against the SERVED config
# script and the SERVED HX table fragment. The container's outerHTML swap is
# the poll's whole DOM outcome; the served rows are the only tr's the row
# lookup can find; the EventSource stub emits what the scenario scripts.
_ADMIN_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 3], "utf8");
const scenario = process.argv[process.argv.length - 2];
const page = JSON.parse(process.argv[process.argv.length - 1]);

const dom = [];
const net = [];
function mark(name) { dom.push(`seg:${name}`); net.push(`seg:${name}`); }

let now = 0;
let timers = [];
let nextTimer = 1;
let components = {};

// window: the SERVED config script evals onto it (the page's own script tag).
global.window = { htmx: {} };
new Function("window", page.configScript)(global.window);

// The table container: its outerHTML swap is the poll's whole DOM outcome.
const container = {
    _html: '<div id="job-table-container"></div>',
    get outerHTML() { return this._html; },
    set outerHTML(v) { this._html = v; dom.push(`swap:${v}`); },
};

// The served rows, as the only tr's document.querySelector can find.
const rowStubs = {};
for (const [id, status] of page.rows) {
    rowStubs[id] = {
        setAttribute(k, v) { dom.push(`row-attr:${id}:${k}=${v}`); },
        querySelector(sel) {
            if (sel !== "[data-status-badge]") return null;
            const badge = {};
            Object.defineProperty(badge, "textContent", {
                set(v) { dom.push(`row-badge-text:${id}=${v}`); },
            });
            let cls = "";
            Object.defineProperty(badge, "className", {
                get() { return cls; },
                set(v) { cls = v; dom.push(`row-badge-class:${id}=${v.slice(0, 60)}`); },
            });
            return badge;
        },
    };
}

global.document = {
    addEventListener(name, fn) { if (name === "alpine:init") global.alpineInit = fn; },
    body: {
        addEventListener() {},
        removeEventListener() {},
    },
    getElementById(id) {
        if (id === "job-table-container") return container;
        if (id === "job-filters") return { requestSubmit() { dom.push("submit"); } };
        return null;
    },
    querySelector(sel) {
        const m = sel.match(/^tr\[data-job-id="([0-9a-f-]+)"\]$/);
        return m ? (rowStubs[m[1]] || null) : null;
    },
    createElement() {
        return {
            _html: "",
            set innerHTML(v) { this._html = v; },
            get innerHTML() { return this._html; },
            // The honest model of the served poll response: the HX partial
            // the route serves carries an element with the container id
            // ONLY if the template put one there — a real browser answers
            // null otherwise, and the swap silently does not happen.
            querySelector(sel) {
                if (sel !== "#job-table-container") return null;
                return /<div[^>]*id="job-table-container"/.test(this._html)
                    ? { outerHTML: this._html }
                    : null;
            },
        };
    },
};

global.Alpine = { data(name, factory) { components[name] = factory; } };
global.FormData = class { *[Symbol.iterator]() {} };
global.EventSource = class {
    constructor(url) { this.url = url; this.handlers = {}; net.push("sse-open:" + url); global.lastEventSource = this; }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { net.push("sse-close"); }
    emit(name, evt) { this.handlers[name] && this.handlers[name](evt); }
    emitError() { this.handlers.error && this.handlers.error({}); }
};
global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };
global.fetch = (url, opts) => {
    net.push(`fetch:${url.split("?")[0]}`);
    net.push(`hx:${(opts && opts.headers && opts.headers["HX-Request"]) || ""}`);
    return Promise.resolve({ text: () => Promise.resolve(page.fragment) });
};

async function hop() { await new Promise((resolve) => setImmediate(resolve)); }

async function advanceOneTick() {
    const target = now + (page.pollIntervalMs || 1000);
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
        await hop();
    }
    now = target;
    await hop();
}

async function main() {
    new Function(src)();
    global.alpineInit();
    const comp = components.jobsPage();
    comp.init();
    dom.push(`cfg:${JSON.stringify({
        tab: comp.tab,
        basePath: comp.basePath,
        realtimeMode: comp.realtimeMode,
        pollIntervalMs: comp.pollIntervalMs,
        liveOn: comp.liveOn,
        totalRows: comp.totalRows,
    })}`);

    if (scenario === "stalled-broker-calm") global.lastEventSource.emitError();
    if (scenario === "drive-refresh") {
        // The archived tab runs no live poll (init returns early); the
        // table-refresh path itself is what the chunk test drives.
        mark("drive");
        comp.refreshTable();
        await hop();
    }
    for (let i = 0; i < (page.ticks || 0); i += 1) {
        mark(`poll:${i + 1}`);
        await advanceOneTick();
    }
    if (page.notify) {
        mark("notify");
        global.lastEventSource.emit("state_change", { data: JSON.stringify(page.notify) });
        await hop();
    }
    net.push(`verdict:${JSON.stringify({ polling: comp.pollTimer !== null, sse: comp.eventSource !== null })}`);
    process.stdout.write(JSON.stringify({ dom, net }));
}

main();
"""


# The realtime.js driver: the job-detail progress machine against the
# SERVED section seed (the exact cursor + state the template rendered), the
# SERVED poll bodies (real endpoint responses, 304s included) and scripted
# SSE frames. The DOM stub logs every mutation — the render/dedup contract
# lives in that log.
_REALTIME_HARNESS = r"""
const fs = require("fs");
const src = fs.readFileSync(process.argv[process.argv.length - 3], "utf8");
const scenario = process.argv[process.argv.length - 2];
const page = JSON.parse(process.argv[process.argv.length - 1]);

const dom = [];
const net = [];
function mark(name) { dom.push(`seg:${name}`); net.push(`seg:${name}`); }

let now = 0;
let timers = [];
let nextTimer = 1;

global.window = { TASKQ_BASE_PATH: page.basePath };
global.POLL_INTERVAL_MS = page.pollIntervalMs;

const badge = {
    attrs: { "data-mode": page.mode },
    textContent: "",
    setAttribute(k, v) { this.attrs[k] = v; dom.push(`badge:${k}=${v}`); },
    getAttribute(k) { return this.attrs[k]; },
};
const section = {
    attrs: {
        "data-job-id": page.jobId,
        "data-progress-seq": page.seq,
        "data-progress-state": page.stateJson,
    },
    getAttribute(k) { return this.attrs[k]; },
};

let nodeCounter = 0;
function makeStyle(name) {
    const style = {};
    let width = "";
    Object.defineProperty(style, "width", {
        get() { return width; },
        set(v) { width = v; dom.push(`width:${name}=${v}`); },
    });
    return style;
}
function makeNode(tag) {
    nodeCounter += 1;
    const name = `${tag}${nodeCounter}`;
    const node = {
        _name: name,
        children: [],
        className: "",
        style: makeStyle(name),
        appendChild(child) { dom.push(`append:${name}<${child._name}`); this.children.push(child); },
        remove() { dom.push(`remove:${name}`); },
        scrollIntoView() { dom.push(`scroll:${name}`); },
    };
    let text = "";
    Object.defineProperty(node, "textContent", {
        get() { return text; },
        set(v) { text = v; dom.push(`text:${name}=${v}`); },
    });
    return node;
}
const timeline = {
    children: [],
    appendChild(child) { dom.push(`timeline-append:${child._name}`); this.children.push(child); },
};

global.document = {
    addEventListener(name, fn) { if (name === "DOMContentLoaded") fn(); },
    getElementById(id) {
        if (id === "progress-section") return section;
        if (id === "progress-timeline") return timeline;
        return null;
    },
    querySelector(sel) { return sel === ".taskq-badge" ? badge : null; },
    createElement(tag) { return makeNode(tag); },
};

let pollCount = 0;
global.fetch = async (url, opts) => {
    net.push(`fetch:${url.split("?")[0]}`);
    if (opts && opts.headers && opts.headers["If-None-Match"] !== undefined) {
        net.push(`inm:${opts.headers["If-None-Match"]}`);
    }
    const scripted = page.polls[pollCount] || page.polls[page.polls.length - 1];
    pollCount += 1;
    if (scripted.status === 304) {
        return { status: 304, json: () => Promise.resolve(null), headers: { get() { return null; } } };
    }
    return {
        status: 200,
        json: () => Promise.resolve(scripted.body),
        headers: { get(name) { return name.toLowerCase() === "etag" ? scripted.etag : null; } },
    };
};

global.EventSource = class {
    constructor(url) {
        this.url = url;
        this.handlers = {};
        global.lastEventSource = this;
        net.push("sse-open:" + url);
    }
    addEventListener(name, fn) { this.handlers[name] = fn; }
    close() { net.push("sse-close"); }
    emit(name, evt) { this.handlers[name] && this.handlers[name](evt); }
    emitError() { this.handlers.error && this.handlers.error({}); }
};

global.setInterval = (fn, ms) => { const id = nextTimer++; timers.push({ id, fn, ms, due: now + ms }); return id; };
global.clearInterval = (id) => { timers = timers.filter((t) => t.id !== id); };

async function hop() { await new Promise((resolve) => setImmediate(resolve)); }

async function advanceOneTick() {
    const target = now + page.pollIntervalMs;
    while (true) {
        const due = timers.filter((t) => t.due <= target).sort((a, b) => a.due - b.due)[0];
        if (!due) break;
        now = due.due; due.due += due.ms; due.fn();
        await hop();
    }
    now = target;
    await hop();
}

async function main() {
    new Function(src)();
    if (page.flow === "frames") {
        for (let i = 0; i < page.frames.length; i += 1) {
            mark(`frame:${i + 1}`);
            const f = page.frames[i];
            global.lastEventSource.emit("progress", { data: JSON.stringify(f.data), lastEventId: f.id });
            await hop();
        }
    } else {
        if (page.flow === "stalled-broker") {
            mark("sse-error");
            global.lastEventSource.emitError();
            await hop();
        }
        for (let i = 0; i < (page.ticks || page.polls.length); i += 1) {
            mark(`poll:${i + 1}`);
            await advanceOneTick();
        }
    }
    mark(`mode:${badge.attrs["data-mode"]}`);
    process.stdout.write(JSON.stringify({ dom, net }));
}

main();
"""


# ── Page captures + log assertions ────────────────────────────────────────


async def _served_jobs_page(app: FastAPI) -> dict[str, Any]:
    """The jobs page's JS inputs, read off the SERVED page: the config
    script, the badge, and the real HX table fragment the poll fetches."""
    page_html = await _html(app, "/admin/jobs?tab=live")
    fragment = await _html(app, "/admin/jobs?tab=live", headers={"HX-Request": "true"})
    return {
        "configScript": _config_script(page_html),
        "badge": _badge(page_html),
        "fragment": fragment,
        "rows": _rows(fragment),
        "pageHtml": page_html,
    }


async def _served_detail_page(app: FastAPI, job_id: uuid.UUID) -> dict[str, Any]:
    """The detail page's JS inputs: the served badge mode, the section seed
    (the exact cursor + state the template rendered) and the poll cadence."""
    html = await _html(app, f"/admin/jobs/{job_id}")
    section = _section(html)
    poll_match = _POLL_MS_RE.search(html)
    base_path_match = _BASE_PATH_RE.search(html)
    assert poll_match is not None and base_path_match is not None, (
        "the detail page must carry the poll cadence and the base path"
    )
    return {
        "mode": _badge(html)[0],
        "jobId": section["job_id"],
        "seq": section["seq"],
        "stateJson": section["state_json"],
        "state": json.loads(section["state_json"]),
        "pollIntervalMs": int(poll_match.group(1)),
        "basePath": base_path_match.group(1),
        "html": html,
    }


def _admin_page(
    served: dict[str, Any], *, ticks: int, notify: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "configScript": served["configScript"],
        "fragment": served["fragment"],
        "rows": served["rows"],
        "ticks": ticks,
        "notify": notify,
        "pollIntervalMs": _config_poll_ms(served["configScript"]),
    }


def _rt_page(
    app_page: dict[str, Any],
    *,
    flow: str,
    polls: list[Any] | None = None,
    frames: list[Any] | None = None,
) -> dict[str, Any]:
    """The realtime harness's page input, seeded from the SERVED detail page
    (the big html key is dropped — the harness needs the seed, not the page)."""
    return {
        "mode": app_page["mode"],
        "jobId": app_page["jobId"],
        "seq": app_page["seq"],
        "stateJson": app_page["stateJson"],
        "pollIntervalMs": app_page["pollIntervalMs"],
        "basePath": app_page["basePath"],
        "flow": flow,
        "polls": polls or [],
        "frames": frames or [],
    }


def _diff_logs(label: str, van: dict[str, list[str]], ht: dict[str, list[str]]) -> None:
    """The DOM is the assertion: identical JS, identical served data, so the
    mutation/request logs must be EXACTLY equal across engines."""
    _diff(f"{label} (dom)", van["dom"], ht["dom"])
    _diff(f"{label} (net)", van["net"], ht["net"])


def _seg(log: dict[str, list[str]], name: str, which: str = "dom") -> list[str]:
    """The log entries written between two markers — exactly which driver
    event mutated (dom) or requested (net) what."""
    entries = log[which]
    start = entries.index(f"seg:{name}")
    nxt = [e for e in entries[start + 1 :] if e.startswith("seg:")]
    end = entries.index(nxt[0], start + 1) if nxt else len(entries)
    return entries[start + 1 : end]


def _verdict(log: dict[str, list[str]]) -> dict[str, bool]:
    (entry,) = [e for e in log["net"] if e.startswith("verdict:")]
    return json.loads(entry[len("verdict:") :])


def _swap_payloads(log: dict[str, list[str]]) -> list[str]:
    return [entry[len("swap:") :] for entry in log["dom"] if entry.startswith("swap:")]


def _dom_writes(log: dict[str, list[str]]) -> list[str]:
    return [
        e
        for e in log["dom"]
        if e.startswith(("timeline-append:", "append:", "text:", "width:", "remove:"))
    ]


# ── 1. The served jobs page: badge, config, and the live-refresh surface ──


async def test_served_jobs_page_configures_the_component_identically(lab: _Lab) -> None:
    """Operator question: does the page I open configure the live table
    identically when the rows come from hypertables? The served badge (mode
    + label), the served ``__taskqJobConfig`` script, the mode probe and the
    initial component state the harness derives from them must all be
    engine-identical — and the concrete values pinned (live tab, live on,
    the real poll cadence)."""
    van = await _served_jobs_page(lab.vanilla.app)
    ht = await _served_jobs_page(lab.ht.app)

    assert ht["badge"] == ("polling", "polling mode"), (
        "no Redis configured: the served badge must state polling calmly"
    )
    _diff_served_page("served jobs page", van["pageHtml"], ht["pageHtml"])
    _diff("served config script", van["configScript"], ht["configScript"])
    _diff("served table fragment", van["fragment"], ht["fragment"])
    _diff("served rows", van["rows"], ht["rows"])
    _diff(
        "sse mode probe",
        (await _get(lab.vanilla.app, "/admin/sse/mode")).json(),
        (await _get(lab.ht.app, "/admin/sse/mode")).json(),
    )

    log = _run_node(_ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(ht, ticks=0))
    (cfg,) = [e for e in log["dom"] if e.startswith("cfg:")]
    state = json.loads(cfg[len("cfg:") :])
    assert state["tab"] == "live" and state["liveOn"] is True
    assert state["basePath"] == "/admin"
    # The config's own census agrees with the served fragment's rows (the
    # row count moves when other tests enqueue; the AGREEMENT is the pin).
    assert state["totalRows"] == len(ht["rows"])
    assert state["pollIntervalMs"] > 0
    assert _verdict(log) == {"polling": True, "sse": True}


async def test_a_stalled_broker_leaves_the_poll_carrying_identical_swaps(lab: _Lab) -> None:
    """Operator question (the #487 contract): when the events broker stalls,
    does my table keep updating calmly — and identically? The SSE error must
    NOT close the EventSource (the browser's own reconnect carries it back;
    closing made a proxy idle timeout permanent), the poll must keep
    fetching, and every swap must write the same served rows — no churn,
    engine-identical."""
    van = await _served_jobs_page(lab.vanilla.app)
    ht = await _served_jobs_page(lab.ht.app)

    van_log = _run_node(_ADMIN_HARNESS, ADMIN_JS, "stalled-broker-calm", _admin_page(van, ticks=3))
    ht_log = _run_node(_ADMIN_HARNESS, ADMIN_JS, "stalled-broker-calm", _admin_page(ht, ticks=3))
    _diff_logs("stalled-broker swaps", van_log, ht_log)

    assert "sse-close" not in ht_log["net"], (
        "a stalled broker must not have its EventSource closed behind the page's back"
    )
    assert ht_log["net"].count("fetch:/admin/jobs") == 3, ht_log["net"]
    assert _verdict(ht_log) == {"polling": True, "sse": True}
    swaps = _swap_payloads(ht_log)
    assert len(swaps) == 3, "every poll tick swaps the served fragment in"
    assert swaps[0] == swaps[1] == swaps[2], (
        "the calm contract: repeated polls write byte-identical content, no churn"
    )
    assert _rows(swaps[0]) == ht["rows"], "the swap must carry exactly the served rows"


async def test_status_change_events_render_identically(lab: _Lab) -> None:
    """Operator question: when a job's status changes under me, does the row
    update — locally when the payload carries the status, from the server
    when it does not (the cancel NOTIFY names the job only) — identically?
    Both code paths are driven with the real served rows/fragment."""
    van = await _served_jobs_page(lab.vanilla.app)
    ht = await _served_jobs_page(lab.ht.app)
    first_row_id = ht["rows"][0][0]

    # Local apply: a payload WITH status patches the row's own nodes and
    # fetches nothing.
    notify = {"job_id": first_row_id, "status": "failed"}
    van_log = _run_node(
        _ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(van, ticks=1, notify=notify)
    )
    ht_log = _run_node(
        _ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(ht, ticks=1, notify=notify)
    )
    _diff_logs("status-change local apply", van_log, ht_log)
    notify_seg = _seg(ht_log, "notify")
    assert f"row-attr:{first_row_id}:data-status=failed" in notify_seg
    assert f"row-badge-text:{first_row_id}=failed" in notify_seg
    assert not any(e.startswith("fetch:") for e in _seg(ht_log, "notify", which="net")), (
        "a payload the client can apply locally must not fetch"
    )

    # Server refresh: a payload WITHOUT status fetches the truth and swaps.
    notify = {"type": "cancel", "job_id": first_row_id}
    van_log = _run_node(
        _ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(van, ticks=1, notify=notify)
    )
    ht_log = _run_node(
        _ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(ht, ticks=1, notify=notify)
    )
    _diff_logs("status-change server refresh", van_log, ht_log)
    assert "fetch:/admin/jobs" in _seg(ht_log, "notify", which="net"), (
        "a statusless NOTIFY must fetch the server's view"
    )
    assert _swap_payloads(ht_log), "the refresh must land in the DOM as a swap"


async def test_live_refresh_delivers_new_rows_through_the_poll_swap(lab: _Lab) -> None:
    """Operator question: when a new job is enqueued while I watch the live
    table, does it reach the RENDERED table through the poll's swap? A new
    row is inserted into BOTH engines (identical id, identical fixed
    stamps); each engine's fresh HX fragment is captured and fed to the
    harness as the poll response; the swap must carry the new row,
    engine-identically. This is the test that was RED against the stock
    page: the served HX fragment carried no ``#job-table-container`` element
    for ``refreshTable`` to find, so the live poll fetched forever and
    rendered never — the red proof behind the template fix that puts the
    container id on the fragment's own root."""
    new_id = new_job_id()
    created = _BASE + timedelta(hours=10)
    for engine in (lab.vanilla, lab.ht):
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(
                f"""INSERT INTO {engine.schema}.jobs (id, actor, queue, payload, max_attempts,
                    retry_kind, status, attempt, tags, created_at, scheduled_at, started_at)
                VALUES ($1, 'late_actor', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient', 'pending',
                    0, '{{beta}}'::text[], $2, $2, NULL)""",
                new_id,
                created,
            )
        finally:
            await conn.close()

    van = await _served_jobs_page(lab.vanilla.app)
    ht = await _served_jobs_page(lab.ht.app)

    # The new row must be IN the served fragments (the server half of the
    # claim), newest-first on the live tab.
    assert (str(new_id), "pending") == ht["rows"][0], (
        f"the new row must lead the live tab: {ht['rows'][:2]}"
    )
    _diff("served fragment with the new row", van["fragment"], ht["fragment"])

    van_log = _run_node(_ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(van, ticks=1))
    ht_log = _run_node(_ADMIN_HARNESS, ADMIN_JS, "serve-and-idle", _admin_page(ht, ticks=1))
    _diff_logs("live refresh with a new row", van_log, ht_log)

    swaps = _swap_payloads(ht_log)
    assert swaps, (
        "the poll's fetch must land in the DOM: the served HX fragment must "
        "carry the container id so refreshTable's swap finds it"
    )
    assert (str(new_id), "pending") in _rows(swaps[0]), (
        f"the newly enqueued job must reach the RENDERED table: {_rows(swaps[0])[:3]}"
    )


# ── 2. The job-detail page's JS against a converted-schema job ───────────


async def _set_running_progress(lab: _Lab, seq: int, state_text: str) -> None:
    """Advance the running job's durable cursor on BOTH engines identically
    (the worker-side re-flush, from the test's God view)."""
    for engine in (lab.vanilla, lab.ht):
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(
                f"UPDATE {engine.schema}.jobs SET progress_seq = $2, "
                f"progress_state = $3::jsonb WHERE id = $1",
                lab.plan.running,
                seq,
                state_text,
            )
        finally:
            await conn.close()


async def test_the_progress_cadence_on_served_bodies_is_calm_and_identical(lab: _Lab) -> None:
    """Operator question: is the detail page's poll calm on hypertable-served
    bodies — a 304 when nothing changed, ZERO DOM writes when the worker
    re-flushes the same snapshot with a bumped seq, a patch of exactly the
    changed nodes for a real change — identically on both engines? The poll
    responses are the REAL state endpoint's answers (status code, ETag,
    body), captured per engine after the same durable writes."""
    van_page = await _served_detail_page(lab.vanilla.app, lab.plan.running)
    ht_page = await _served_detail_page(lab.ht.app, lab.plan.running)
    _diff_served_page("detail page", van_page["html"], ht_page["html"])
    _diff("section seed state", van_page["state"], ht_page["state"])
    assert ht_page["state"]["data"]["eta"] == "2026-09-20 12:34:56.789012+00:00"
    assert ht_page["state"]["data"]["note"] is None
    base_seq = int(ht_page["seq"])
    assert base_seq == 7

    # Poll 1: the page's own cursor is current — the REAL endpoint answers
    # 304 (captured, not assumed).
    r304 = await _get(
        lab.ht.app,
        f"/admin/jobs/api/job/{lab.plan.running}/state",
        headers={"If-None-Match": f'"{base_seq}"'},
    )
    assert r304.status_code == 304

    # Poll 2: the worker re-flushes the SAME snapshot with a bumped seq (a
    # durable UPDATE on both engines); the real 200 body is the tick's input
    # and the fingerprint gate must drop it before any DOM work.
    await _set_running_progress(lab, base_seq + 1, _R_STATE)
    van_body2 = (
        await _get(lab.vanilla.app, f"/admin/jobs/api/job/{lab.plan.running}/state")
    ).json()
    ht_body2 = (await _get(lab.ht.app, f"/admin/jobs/api/job/{lab.plan.running}/state")).json()
    _diff("re-flush body", van_body2, ht_body2)
    assert ht_body2["progress_seq"] == base_seq + 1

    # Poll 3: a real change (percent 42 -> 55, the timestamptz/null shapes kept).
    await _set_running_progress(
        lab, base_seq + 2, _R_STATE.replace('"percent": 42', '"percent": 55')
    )
    van_body3 = (
        await _get(lab.vanilla.app, f"/admin/jobs/api/job/{lab.plan.running}/state")
    ).json()
    ht_body3 = (await _get(lab.ht.app, f"/admin/jobs/api/job/{lab.plan.running}/state")).json()
    _diff("changed body", van_body3, ht_body3)
    assert ht_body3["progress_state"]["percent"] == 55

    polls = [
        {"status": 304},
        {"status": 200, "etag": f'"{base_seq + 1}"', "body": ht_body2},
        {"status": 200, "etag": f'"{base_seq + 2}"', "body": ht_body3},
    ]
    van_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(van_page, flow="poll", polls=polls)
    )
    ht_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(ht_page, flow="poll", polls=polls)
    )
    _diff_logs("progress cadence", van_log, ht_log)

    assert _seg(ht_log, "poll:1") == [], "a 304 tick writes nothing"
    assert _seg(ht_log, "poll:2") == [], (
        "a re-flushed identical snapshot must be dropped by the fingerprint "
        "gate before any DOM work — zero writes"
    )
    # The first ACCEPTED-and-changed tick is poll 3: it builds the entry
    # once (the appends) and patches exactly the nodes the change feeds.
    poll3 = _seg(ht_log, "poll:3")
    assert any(e.startswith("timeline-append:") for e in poll3)
    assert "width:div3=55%" in poll3 and "text:div5=55% · upload" in poll3
    assert f'inm:"{base_seq}"' in _seg(ht_log, "poll:2", which="net"), (
        "the 304 tick must not advance the conditional-GET cursor: poll 2 "
        "still asks about the rendered sequence"
    )
    assert f'inm:"{base_seq + 1}"' in _seg(ht_log, "poll:3", which="net"), (
        "the poll must carry the advanced conditional-GET cursor"
    )


async def test_frames_render_dedup_and_gate_identically(lab: _Lab) -> None:
    """Operator question: do live progress frames — on a converted-schema
    job's stream — render once, dedup a duplicate replay (same fingerprint,
    fresh ts, bumped id), and DROP a trailing sequence (the BigInt cursor
    gate) — identically? The page is the redis-served one (badge realtime,
    stream open); the frames are the wire envelopes."""
    van_page = await _served_detail_page(lab.van_redis_app, lab.plan.running)
    ht_page = await _served_detail_page(lab.ht_redis_app, lab.plan.running)
    _diff("served mode", van_page["mode"], ht_page["mode"])
    assert ht_page["mode"] == "realtime", "the redis-mounted page must badge realtime"
    _diff(
        "section seed",
        (van_page["seq"], van_page["stateJson"]),
        (ht_page["seq"], ht_page["stateJson"]),
    )

    def envelope(seq: int, **over: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "seq": seq,
            "job_id": str(lab.plan.running),
            "actor": "js_actor",
            "ts": f"2026-09-20T13:00:{seq:02d}Z",
            "status": "running",
            "kind": "progress",
            "terminal": False,
            "step": "upload",
            "percent": 60,
        }
        base.update(over)
        return base

    boot_seq = int(ht_page["seq"])
    frames = [
        # 1: a fresh frame renders (ids are dynamic: other tests ratchet the
        # running job's durable cursor, and a trailing id would be dropped).
        {"id": str(boot_seq + 1), "data": envelope(boot_seq + 1, percent=60)},
        # 2: a duplicate replay — same identity fields, fresh ts, bumped id —
        # advances the cursor and must write NOTHING.
        {"id": str(boot_seq + 2), "data": envelope(boot_seq + 2, ts="2026-09-20T13:00:59Z")},
        # 3: a real change patches.
        {"id": str(boot_seq + 3), "data": envelope(boot_seq + 3, percent=70, step="index")},
        # 4: a TRAILING sequence (the reconnect replay behind the cursor) —
        # the BigInt gate drops it before any DOM work.
        {"id": str(boot_seq + 2), "data": envelope(boot_seq + 2, percent=1, step="poison")},
    ]

    van_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "frames", _rt_page(van_page, flow="frames", frames=frames)
    )
    ht_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "frames", _rt_page(ht_page, flow="frames", frames=frames)
    )
    _diff_logs("frames", van_log, ht_log)

    assert any(e.startswith("timeline-append:") for e in _seg(ht_log, "frame:1"))
    assert _seg(ht_log, "frame:2") == [], "a duplicate fingerprint must write nothing"
    # The meta line renders the frame's own ts (excluded from the FINGERPRINT,
    # not from the render): percent · step · the frame's clock.
    assert _seg(ht_log, "frame:3")[:2] == ["width:div3=70%", "text:div4=index"]
    assert _seg(ht_log, "frame:3")[2].startswith("text:div5=70% · index · ")
    assert _seg(ht_log, "frame:4") == [], "a trailing sequence must be dropped by the cursor gate"


async def test_the_terminal_body_stops_the_machine_identically(lab: _Lab) -> None:
    """Operator question: when the job finishes under the open page, does it
    render the terminal state and stand down — poll stopped — with the REAL
    terminal body the endpoint serves, identically? A terminal row always
    answers 200 (the downloaded terminal status is what stops the client's
    poll loop); the transition is performed mid-test on both engines so the
    terminal state is one the page has NOT already server-rendered."""
    van_page = await _served_detail_page(lab.vanilla.app, lab.plan.late_terminal)
    ht_page = await _served_detail_page(lab.ht.app, lab.plan.late_terminal)
    _diff(
        "late-terminal section seed",
        (van_page["seq"], van_page["state"]),
        (ht_page["seq"], ht_page["state"]),
    )
    assert ht_page["state"]["percent"] == 10

    # The job finishes under the open page: a durable terminal write on BOTH
    # engines, identically (status, finished_at, progress percent 100).
    terminal_state = '{"percent": 100, "step": "finalize"}'
    for engine in (lab.vanilla, lab.ht):
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(
                f"""UPDATE {engine.schema}.jobs SET status = 'succeeded',
                    finished_at = $2, progress_seq = 6, progress_state = $3::jsonb
                WHERE id = $1""",
                lab.plan.late_terminal,
                _BASE + timedelta(hours=4, minutes=30),
                terminal_state,
            )
        finally:
            await conn.close()

    van_body = (
        await _get(lab.vanilla.app, f"/admin/jobs/api/job/{lab.plan.late_terminal}/state")
    ).json()
    ht_body = (await _get(lab.ht.app, f"/admin/jobs/api/job/{lab.plan.late_terminal}/state")).json()
    _diff("terminal body", van_body, ht_body)
    assert ht_body["status"] == "succeeded"
    etag = f'"{ht_body["progress_seq"]}"'
    state_url = f"/jobs/api/job/{lab.plan.late_terminal}/state"

    polls = [{"status": 200, "etag": etag, "body": ht_body}]
    van_log = _run_node(
        _REALTIME_HARNESS,
        REALTIME_JS,
        "poll-cadence",
        {**_rt_page(van_page, flow="poll", polls=polls), "ticks": 3},
    )
    ht_log = _run_node(
        _REALTIME_HARNESS,
        REALTIME_JS,
        "poll-cadence",
        {**_rt_page(ht_page, flow="poll", polls=polls), "ticks": 3},
    )
    _diff_logs("terminal render", van_log, ht_log)

    writes = _seg(ht_log, "poll:1")
    assert "width:div3=100%" in writes and "text:div4=finalize" in writes, (
        "the terminal transition must render through the poll"
    )
    # Three ticks ran (the stub repeats the scripted poll), but the machine
    # stood down on the FIRST downloaded terminal body: exactly one fetch.
    assert ht_log["net"].count(f"fetch:/admin{state_url}") == 1, ht_log["net"]


async def test_the_empty_progress_terminal_writes_nothing_and_stops_identically(lab: _Lab) -> None:
    """Operator question: the common terminal shape — a job that finished
    with NO progress at all (``progress_state`` = the empty jsonb object the
    column's NOT NULL default holds, the null-pattern body the endpoint
    serves) — must stop the machine cleanly and write NOTHING (an empty
    state has no fingerprint to render), identically."""
    van_body = (
        await _get(lab.vanilla.app, f"/admin/jobs/api/job/{lab.plan.terminal_null}/state")
    ).json()
    ht_body = (await _get(lab.ht.app, f"/admin/jobs/api/job/{lab.plan.terminal_null}/state")).json()
    _diff("empty-state terminal body", van_body, ht_body)
    assert ht_body["progress_state"] == {}, "the seeded empty-progress shape must survive the wire"

    van_page = await _served_detail_page(lab.vanilla.app, lab.plan.terminal_null)
    ht_page = await _served_detail_page(lab.ht.app, lab.plan.terminal_null)
    etag = f'"{ht_body["progress_seq"]}"'

    polls = [{"status": 200, "etag": etag, "body": ht_body}]
    van_log = _run_node(
        _REALTIME_HARNESS,
        REALTIME_JS,
        "poll-cadence",
        {**_rt_page(van_page, flow="poll", polls=polls), "ticks": 3},
    )
    ht_log = _run_node(
        _REALTIME_HARNESS,
        REALTIME_JS,
        "poll-cadence",
        {**_rt_page(ht_page, flow="poll", polls=polls), "ticks": 3},
    )
    _diff_logs("null-state terminal", van_log, ht_log)

    assert _dom_writes(ht_log) == [], (
        f"an empty state has no fingerprint to render — zero writes: {_dom_writes(ht_log)}"
    )
    # The machine stood down on the first downloaded terminal body: three
    # ticks, one fetch.
    assert ht_log["net"].count(f"fetch:/admin/jobs/api/job/{lab.plan.terminal_null}/state") == 1, (
        ht_log["net"]
    )


async def test_the_bigint_cursor_boundary_renders_identically(lab: _Lab) -> None:
    """Operator question: does a sequence at the double-precision boundary
    still render (the ETag carries the exact decimal digits the server
    compared; the body's own number has already lost them), identically?
    The bodies are scripted at 2^53 — the shape #489's acceptProgress fix
    pins — fed through the same served-section boot the real pages use."""
    van_page = await _served_detail_page(lab.vanilla.app, lab.plan.running)
    ht_page = await _served_detail_page(lab.ht.app, lab.plan.running)

    polls = [
        {
            "status": 200,
            "etag": '"9007199254740992"',
            "body": {
                "status": "running",
                "progress_seq": 9007199254740992,
                "progress_state": {"percent": 50, "step": "stage", "detail": "half"},
            },
        },
        {
            # The body's progress_seq parses back as exactly 2^53 (the value
            # the cursor already holds); the ETag carries 2^53 + 1 exactly.
            "status": 200,
            "etag": '"9007199254740993"',
            "body": {
                "status": "running",
                "progress_seq": 9007199254740992,
                "progress_state": {"percent": 75, "step": "stage", "detail": "more"},
            },
        },
    ]
    van_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(van_page, flow="poll", polls=polls)
    )
    ht_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(ht_page, flow="poll", polls=polls)
    )
    _diff_logs("bigint boundary", van_log, ht_log)

    assert _seg(ht_log, "poll:2") == [
        "width:div3=75%",
        "text:div4=more",
        "text:div5=75% · stage",
    ], (
        "the tick at seq 2^53 + 1 must advance the exact cursor and render, "
        "not be dropped as a duplicate of 2^53"
    )


# ── 3. The public API surface: /admin/jobs/api/... engine-identical ──────


async def test_the_state_endpoint_is_engine_identical(lab: _Lab) -> None:
    """Integrator question: is the poll-state wire contract — the snapshot
    body, the 304/ETag conditional semantics (and the terminal rows' honest
    exception: a terminal body ALWAYS answers 200, because the downloaded
    terminal status is what stops the client's poll loop), the 404 for an
    archived id, the 503 degrade without a broker, and the 400 for a
    malformed reconnect cursor — engine-identical?"""
    running = lab.plan.running

    van = await _get(lab.vanilla.app, f"/admin/jobs/api/job/{running}/state")
    ht = await _get(lab.ht.app, f"/admin/jobs/api/job/{running}/state")
    assert van.status_code == ht.status_code == 200
    _diff("state body", van.json(), ht.json())
    _diff("state etag", van.headers["etag"], ht.headers["etag"])
    body = ht.json()
    assert body["status"] == "running"
    assert body["progress_state"]["data"]["eta"] == "2026-09-20 12:34:56.789012+00:00"
    assert body["progress_state"]["data"]["note"] is None
    assert body["progress_state"]["data"]["retries"] == [{"at": None}, {"at": 3}]

    # 304: the client's rendered seq is current (read dynamically — the seq
    # ratchets as other tests re-flush the running job).
    current = f'"{body["progress_seq"]}"'
    van304 = await _get(
        lab.vanilla.app, f"/admin/jobs/api/job/{running}/state", headers={"If-None-Match": current}
    )
    ht304 = await _get(
        lab.ht.app, f"/admin/jobs/api/job/{running}/state", headers={"If-None-Match": current}
    )
    assert van304.status_code == ht304.status_code == 304
    _diff("304 etag", van304.headers["etag"], ht304.headers["etag"])

    # The terminal exception: a MATCHING etag on a terminal row still
    # answers 200 with the full body — on both engines.
    terminal = lab.plan.terminal_progress
    van_t = await _get(lab.vanilla.app, f"/admin/jobs/api/job/{terminal}/state")
    ht_t = await _get(lab.ht.app, f"/admin/jobs/api/job/{terminal}/state")
    term_etag = ht_t.headers["etag"]
    van_t304 = await _get(
        lab.vanilla.app,
        f"/admin/jobs/api/job/{terminal}/state",
        headers={"If-None-Match": term_etag},
    )
    ht_t304 = await _get(
        lab.ht.app, f"/admin/jobs/api/job/{terminal}/state", headers={"If-None-Match": term_etag}
    )
    assert van_t304.status_code == ht_t304.status_code == 200
    _diff("terminal 200 body", van_t304.json(), ht_t304.json())
    _diff("terminal body (no header)", van_t.json(), ht_t.json())

    # 404: an archived id — the poll reads `jobs` only, on both engines.
    archived = lab.plan.archive[2].id
    for app, label in ((lab.vanilla.app, "vanilla"), (lab.ht.app, "hypertable")):
        resp = await _get(app, f"/admin/jobs/api/job/{archived}/state")
        assert resp.status_code == 404, f"{label}: {resp.status_code}"

    # 503: no broker configured — the documented degrade body.
    van503 = await _get(lab.vanilla.app, f"/admin/jobs/api/job/{running}/progress/stream")
    ht503 = await _get(lab.ht.app, f"/admin/jobs/api/job/{running}/progress/stream")
    assert van503.status_code == ht503.status_code == 503
    _diff("503 body", van503.text, ht503.text)
    assert ht503.json() == {"error": "redis_not_configured"}
    _diff("503 retry-after", van503.headers.get("retry-after"), ht503.headers.get("retry-after"))

    # 400: a malformed Last-Event-ID cannot masquerade as "no cursor" —
    # checked on the broker-mounted apps (the no-broker 503 guard answers
    # before the cursor is ever resolved, identically above).
    for app, label in ((lab.van_redis_app, "vanilla"), (lab.ht_redis_app, "hypertable")):
        resp = await _get(
            app,
            f"/admin/jobs/api/job/{running}/progress/stream",
            headers={"Last-Event-ID": "not-a-seq"},
        )
        assert resp.status_code == 400, f"{label}: {resp.status_code}"
        assert "Last-Event-ID" in resp.text


async def test_the_progress_stream_is_engine_identical(lab: _Lab) -> None:
    """Integrator question: is the SSE wire contract engine-identical — the
    first frame the PG snapshot (the jsonb TEXT as PG wrote it, timestamptz
    spelling and nulls included), a live pub/sub envelope forwarded
    VERBATIM, the same ``event:``/``id:``/``data:`` framing? Both engines'
    captured byte streams must be equal."""
    running = lab.plan.running
    current_seq = (await _get(lab.ht.app, f"/admin/jobs/api/job/{running}/state")).json()[
        "progress_seq"
    ]
    live_seq = current_seq + 1
    envelope = json.dumps(
        {
            "seq": live_seq,
            "job_id": str(running),
            "actor": "js_actor",
            "ts": "2026-09-20T13:01:00Z",
            "status": "running",
            "kind": "progress",
            "terminal": False,
            "step": "upload",
        },
        separators=(",", ":"),
    )
    stop = asyncio.Event()
    import redis.asyncio as redis_asyncio

    publisher = redis_asyncio.from_url(lab.redis_url)

    async def _publish(engine_schema: str) -> None:
        while not stop.is_set():
            await publisher.publish(progress_channel(engine_schema, running), envelope)
            await asyncio.sleep(0.2)

    try:
        van_pump = asyncio.create_task(_publish(lab.vanilla.schema))
        ht_pump = asyncio.create_task(_publish(lab.ht.schema))
        try:
            van_buf = await _stream_request(
                lab.van_redis_app,
                f"/admin/jobs/api/job/{running}/progress/stream",
                b'"seq":%d' % live_seq,
            )
            ht_buf = await _stream_request(
                lab.ht_redis_app,
                f"/admin/jobs/api/job/{running}/progress/stream",
                b'"seq":%d' % live_seq,
            )
        finally:
            stop.set()
            for pump in (van_pump, ht_pump):
                pump.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pump
    finally:
        await publisher.aclose()

    van_frames = _strip_keepalives(van_buf)
    ht_frames = _strip_keepalives(ht_buf)
    _diff("stream bytes", van_frames, ht_frames)
    # The snapshot frame: the jsonb TEXT as PG wrote it — keys jsonb-sorted,
    # single spaces, the timestamptz spelling and the nulls intact. (The
    # percent value itself is whatever the durable cursor last carried —
    # other tests re-flush it; its PRESENCE in the PG spelling is the pin.)
    assert b"event: progress" in ht_frames
    assert b'"eta": "2026-09-20 12:34:56.789012+00:00"' in ht_frames
    assert b'"note": null' in ht_frames
    assert b'"retries": [{"at": null}, {"at": 3}]' in ht_frames
    assert b'"percent":' in ht_frames
    assert f"id: {current_seq}".encode() in ht_frames
    # The live envelope forwarded verbatim.
    assert envelope.encode() in ht_frames
    assert f"id: {live_seq}".encode() in ht_frames


async def test_the_stream_reconnects_at_the_cursor_identically(lab: _Lab) -> None:
    """Integrator question: the reconnect-at-cursor semantics — a
    ``Last-Event-ID`` AT the current seq must suppress the snapshot (no
    duplicate frame; the stream sits on keepalives), a cursor BEHIND must
    replay exactly one catch-up frame from PG, and the query-parameter form
    must behave identically to the header. Engine-identical bytes."""
    running = lab.plan.running
    current_seq = (await _get(lab.ht.app, f"/admin/jobs/api/job/{running}/state")).json()[
        "progress_seq"
    ]

    # Cursor current: no catch-up frame — the first thing on the wire is a
    # keepalive comment.
    van = await _stream_request(
        lab.van_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        headers={"Last-Event-ID": str(current_seq)},
    )
    ht = await _stream_request(
        lab.ht_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        headers={"Last-Event-ID": str(current_seq)},
    )
    assert b"event:" not in _strip_keepalives(van)
    _diff("reconnect-at-current bytes", _strip_keepalives(van), _strip_keepalives(ht))

    # Cursor behind: exactly one catch-up frame, then keepalives.
    van = await _stream_request(
        lab.van_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        headers={"Last-Event-ID": str(current_seq - 1)},
    )
    ht = await _stream_request(
        lab.ht_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        headers={"Last-Event-ID": str(current_seq - 1)},
    )
    van_frames, ht_frames = _strip_keepalives(van), _strip_keepalives(ht)
    _diff("reconnect-behind bytes", van_frames, ht_frames)
    assert f"id: {current_seq}".encode() in ht_frames
    assert ht_frames.count(b"event: progress") == 1, (
        "exactly one catch-up frame, never a replay storm"
    )

    # The query-parameter form (the curl/debugging convenience) must match
    # the header's semantics engine-for-engine.
    van_q = await _stream_request(
        lab.van_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        query=f"last_event_id={current_seq}".encode(),
    )
    ht_q = await _stream_request(
        lab.ht_redis_app,
        f"/admin/jobs/api/job/{running}/progress/stream",
        b"keepalive",
        query=f"last_event_id={current_seq}".encode(),
    )
    _diff("reconnect-via-query bytes", _strip_keepalives(van_q), _strip_keepalives(ht_q))
    assert b"event:" not in _strip_keepalives(ht_q)


async def test_the_terminal_stream_closes_itself_identically(lab: _Lab) -> None:
    """Integrator question: a terminal job's stream must answer with the
    terminal snapshot frame, then ``event: done``, then END BY ITSELF (the
    generator returns — no client hangup needed), byte-identically."""
    terminal = lab.plan.terminal_progress
    terminal_seq = (await _get(lab.ht.app, f"/admin/jobs/api/job/{terminal}/state")).json()[
        "progress_seq"
    ]
    van = await _stream_request(
        lab.van_redis_app, f"/admin/jobs/api/job/{terminal}/progress/stream", b"event: done"
    )
    ht = await _stream_request(
        lab.ht_redis_app, f"/admin/jobs/api/job/{terminal}/progress/stream", b"event: done"
    )
    van_frames, ht_frames = _strip_keepalives(van), _strip_keepalives(ht)
    _diff("terminal stream bytes", van_frames, ht_frames)
    assert b"event: terminal" in ht_frames
    assert b"event: done" in ht_frames
    assert f"id: {terminal_seq}".encode() in ht_frames
    assert b"finalize" in ht_frames


# ── The ASGI stream harness (the sibling module's idiom, parameterised) ──


def _strip_keepalives(buf: bytes) -> bytes:
    return re.sub(rb": keepalive\n", b"", buf)


async def _stream_request(
    app: FastAPI,
    path: str,
    needle: bytes,
    *,
    headers: dict[str, str] | None = None,
    query: bytes = b"",
    timeout_s: float = 20.0,
) -> bytes:
    """Drive the ASGI app DIRECTLY and read the streaming body until
    *needle* appears (or the stream ends by itself) — the sibling module's
    ``_stream_until`` harness (test_admin_on_hypertables.py), parameterised
    by request headers and query string so the reconnect-at-cursor shapes
    can be driven. httpx's ASGITransport cannot be used for infinite SSE
    streams (it never delivers ``http.disconnect``); this harness sends the
    disconnect for real once the needle is seen."""
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query,
        "root_path": "",
        "headers": [(b"host", b"testserver")]
        + [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }
    messages: asyncio.Queue[Any] = asyncio.Queue()
    disconnected = asyncio.Event()

    async def receive() -> Any:
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: Any) -> None:
        await messages.put(message)

    async def run() -> None:
        try:
            await app(scope, receive, send)
        finally:
            await messages.put(None)  # sentinel: the app finished

    task = asyncio.create_task(run())
    buf = b""
    try:
        while True:
            message = await asyncio.wait_for(messages.get(), timeout_s)
            if message is None:
                break
            if message["type"] == "http.response.body":
                buf += message.get("body", b"")
                if needle in buf:
                    break
    finally:
        disconnected.set()  # the client disconnect, delivered for real
        with contextlib.suppress(Exception, TimeoutError):
            await asyncio.wait_for(task, 15)
        # sse-starlette's detached ``_shutdown_watcher`` parks forever (it
        # waits for a uvicorn shutdown flag no in-process test ever sets);
        # cancel it by name and retrieve the cancellation, the same reap
        # tests/web_progress/test_integration.py applies.
        for leftover in asyncio.all_tasks():
            if (
                leftover is not asyncio.current_task()
                and getattr(leftover.get_coro(), "__name__", None) == "_shutdown_watcher"
            ):
                leftover.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await leftover
    return buf


# ── 4. The chunk boundary in the UI's data ────────────────────────────────


def _title_stamps(html: str) -> list[str]:
    """The absolute timestamp renders (title attributes) in row order — the
    timestamptz spelling the operator sees, through the swap."""
    return re.findall(r'title="(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[^"]*)"', html)


async def test_the_archived_tab_paginates_across_the_chunk_boundary_identically(
    lab: _Lab,
    ts_dsn: str,
) -> None:
    """Operator question: does a job list paginating across a TimescaleDB
    chunk boundary render the same rows, in the same order, through the same
    JS — identically to a vanilla schema (where no chunk exists at all)?
    The seed puts the page-1/page-2 boundary ON a chunk boundary (newest 50
    rows in one day-chunk, oldest 5 in another); the premise is proven
    against the converted schema's own chunk inventory, the walk is diffed,
    and page 2's served fragment is driven through the JS swap."""
    conn = await asyncpg.connect(ts_dsn)
    try:
        chunks = await conn.fetch(
            "SELECT range_start, range_end FROM timescaledb_information.chunks "
            "WHERE hypertable_schema = $1 AND hypertable_name = 'jobs_archive' "
            "ORDER BY range_start",
            lab.ht.schema,
        )
    finally:
        await conn.close()

    # The chunk premise: page 1's rows live in a DIFFERENT chunk than page
    # 2's — the pagination boundary IS a chunk boundary.
    def _chunk_of(ts: datetime) -> Any:
        for c in chunks:
            if c["range_start"] <= ts < c["range_end"]:
                return c["range_start"]
        raise AssertionError(f"no chunk covers {ts}")

    page1_last = min(lab.plan.archive[_N_OLD:], key=lambda r: r.finished_at)
    page2_first = max(lab.plan.archive[:_N_OLD], key=lambda r: r.finished_at)
    assert len(chunks) >= 2
    assert _chunk_of(page1_last.finished_at) != _chunk_of(page2_first.finished_at), (
        "the seed must put the page boundary on a chunk boundary"
    )

    # The walk, both engines, row-identical; the oracle order (finished_at
    # DESC NULLS LAST, id DESC — every seeded row carries a finished_at).
    async def _walk(app: FastAPI) -> list[str]:
        seen: list[str] = []
        path: str | None = "/admin/jobs?tab=archived"
        pages = 0
        while path is not None:
            html = await _html(app, path)
            for jid, _status in _rows(html):
                if jid not in seen:
                    seen.append(jid)
            m = _NEXT_LINK_RE.search(html)
            path = m.group(1) if m else None
            pages += 1
            assert pages < 10, "the walk did not terminate"
        return seen

    van_walk = await _walk(lab.vanilla.app)
    ht_walk = await _walk(lab.ht.app)
    _diff("archived walk across the boundary", van_walk, ht_walk)
    by_finished_desc = sorted(lab.plan.archive, key=lambda r: (r.finished_at, r.id), reverse=True)
    expected = [str(r.id) for r in by_finished_desc]
    assert ht_walk == expected
    assert len(ht_walk) == _N_ARCHIVE
    assert ht_walk[:_PAGE_SIZE] == [str(r.id) for r in by_finished_desc[:_PAGE_SIZE]], (
        "page 1 must be exactly the newest chunk's 50 rows"
    )
    assert ht_walk[_PAGE_SIZE:] == [str(r.id) for r in by_finished_desc[_PAGE_SIZE:]], (
        "page 2 must be exactly the older chunk's rows"
    )

    # Page 2 — the rows on the OLDER chunk — rendered through the JS swap:
    # the served fragment drives refreshTable's swap, engine-identically.
    page1_van = await _html(lab.vanilla.app, "/admin/jobs?tab=archived")
    page1_ht = await _html(lab.ht.app, "/admin/jobs?tab=archived")
    next_van = _NEXT_LINK_RE.search(page1_van)
    next_ht = _NEXT_LINK_RE.search(page1_ht)
    assert next_van is not None and next_ht is not None, "55 rows must paginate"
    _diff("next cursor", next_van.group(1), next_ht.group(1))
    # The fragment the JS's own refresh fetch receives: the HX partial.
    page2_van = await _html(lab.vanilla.app, next_van.group(1), headers={"HX-Request": "true"})
    page2_ht = await _html(lab.ht.app, next_ht.group(1), headers={"HX-Request": "true"})
    _diff_served_page("page-2 fragment", page2_van, page2_ht)
    assert [r[0] for r in _rows(page2_ht)] == ht_walk[_PAGE_SIZE:], (
        "page 2 must be exactly the older chunk's rows"
    )

    van_log = _run_node(
        _ADMIN_HARNESS,
        ADMIN_JS,
        "drive-refresh",
        _admin_page(
            {
                "configScript": _config_script(page1_van),
                "fragment": page2_van,
                "rows": _rows(page2_van),
            },
            ticks=0,
        ),
    )
    ht_log = _run_node(
        _ADMIN_HARNESS,
        ADMIN_JS,
        "drive-refresh",
        _admin_page(
            {
                "configScript": _config_script(page1_ht),
                "fragment": page2_ht,
                "rows": _rows(page2_ht),
            },
            ticks=0,
        ),
    )
    _diff_logs("chunk-boundary swap", van_log, ht_log)
    (swap,) = _swap_payloads(ht_log)
    assert [r[0] for r in _rows(swap)] == ht_walk[_PAGE_SIZE:], (
        "the JS table must render exactly the older chunk's rows, in order"
    )
    # The timestamptz renders survive the swap: the fixed finished stamps of
    # the older chunk appear in the swapped-in HTML identically.
    assert _title_stamps(swap) == _title_stamps(page2_ht)


async def test_the_orphaned_attempt_window_surfaces_nowhere(lab: _Lab, ts_dsn: str) -> None:
    """Operator question: the hypertable-only data shape — an attempt row
    that outlives its parent archive row (the FK is dropped; the two tables
    chunk on different time columns) — does it surface NOWHERE? Not in the
    walks, not in the API (the orphan id's state endpoint is a 404 on both
    engines: attempts never masquerade as jobs), and the pages around it —
    including the null-pattern attempt cells and the timestamptz-bearing
    progress seed the JS canonicalizes — render identically."""
    orphan_parent = new_job_id()  # never seeded as a job row
    conn = await asyncpg.connect(ts_dsn)
    try:
        # The dropped-FK premise: impossible on vanilla, possible on the
        # hypertable — the conversion's one honest behavioral change.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                f"""INSERT INTO {lab.vanilla.schema}.job_attempts_archive
                    (job_id, attempt, started_at, finished_at, outcome)
                VALUES ($1, 1, $2, $3, 'succeeded')""",
                orphan_parent,
                _BASE,
                _BASE + timedelta(minutes=4),
            )
        await conn.execute(
            f"""INSERT INTO {lab.ht.schema}.job_attempts_archive
                (job_id, attempt, started_at, finished_at, outcome)
            VALUES ($1, 1, $2, $3, 'succeeded')""",
            orphan_parent,
            _BASE,
            _BASE + timedelta(minutes=4),
        )
    finally:
        await conn.close()

    # The walks: unchanged by the orphan, engine-identical (the orphan's
    # parent does not exist, so no page can render it).
    for path in ("/admin/jobs?tab=archived", "/admin/history"):
        van_html = await _html(lab.vanilla.app, path)
        ht_html = await _html(lab.ht.app, path)
        _diff_served_page(f"orphan-window walk {path}", van_html, ht_html)
        assert str(orphan_parent) not in ht_html

    # The API: the orphan id is not a job — 404 on both engines.
    for app, label in ((lab.vanilla.app, "vanilla"), (lab.ht.app, "hypertable")):
        resp = await _get(app, f"/admin/jobs/api/job/{orphan_parent}/state")
        assert resp.status_code == 404, f"{label}: {resp.status_code}"

    # The archived detail page still renders its OWN attempts — with the
    # null error_class pattern — identically.
    parent = lab.plan.archive[0]
    van_detail = await _html(lab.vanilla.app, f"/admin/jobs/{parent.id}")
    ht_detail = await _html(lab.ht.app, f"/admin/jobs/{parent.id}")
    _diff_served_page("orphan-window detail", van_detail, ht_detail)
    assert "Attempt History" in ht_detail

    # The JS angle: the canonicalize/fingerprint machine fed the SERVED
    # running-job state (the timestamptz spelling + the null patterns) must
    # dedup a re-flushed identical snapshot to zero writes — identically, in
    # the world where the orphan row exists.
    van_page = await _served_detail_page(lab.vanilla.app, lab.plan.running)
    ht_page = await _served_detail_page(lab.ht.app, lab.plan.running)
    body = (await _get(lab.ht.app, f"/admin/jobs/api/job/{lab.plan.running}/state")).json()
    etag = f'"{body["progress_seq"]}"'

    polls = [{"status": 200, "etag": etag, "body": body}]
    van_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(van_page, flow="poll", polls=polls)
    )
    ht_log = _run_node(
        _REALTIME_HARNESS, REALTIME_JS, "poll-cadence", _rt_page(ht_page, flow="poll", polls=polls)
    )
    _diff_logs("orphan-window canonicalize", van_log, ht_log)
    assert _dom_writes(ht_log) == [], (
        f"the served snapshot (timestamptz + nulls) must dedup to zero DOM "
        f"writes: {_dom_writes(ht_log)}"
    )

# ruff: noqa: S608  # Why: every schema interpolation is a fixed per-run test identifier, and every value is $-bound.
"""The admin/ops surface, end-to-end, on a hypertable-converted schema.

The TimescaleDB conversion (``enable_hypertables``) changes the storage
shape of exactly three tables the admin UI reads — ``jobs_archive``,
``job_attempts_archive``, ``job_events`` become hypertables: primary keys
widen to include the partition column, ``job_attempts_archive``'s FK to
``jobs_archive`` is dropped (nothing may reference a hypertable), and
chunk boundaries replace whole-table storage. The operator's tools read
those tables through the admin routes' hand-written SQL, so this module
drives the REAL router (``create_router`` + ``setup_admin_state``, httpx
ASGI transport — the ``tests/web_admin`` idiom) against a REAL converted
schema and proves every surface the audit names:

1. The jobs list and every filter shape (status, queue, actor, tag, text
   search, keyset cursor pagination) — row-identical to the SAME queries
   on a vanilla schema. Both schemas are seeded IDENTICALLY (same ids,
   same fixed timestamps — the seed plan is built once and applied to
   both) on the same TimescaleDB container, so responses are directly
   comparable and every differential is exact.
2. The history/archive views — the archive union live union walk with its
   three-column keyset seam, and the per-actor stats aggregate (all-time
   and windowed — the window is where chunk exclusion does its work).
   The walk's NULL-finished sentinel range is PINNED as the conversion's
   one row-shape divergence: the partition column is NOT NULL on the
   hypertable, so the row the ``__NULL__`` sentinel describes cannot
   exist there (harmless to the surface — no production write makes
   one — but documented, DML-time and migrate-time).
3. The SSE streams and the progress endpoints. The admin ``/sse/{topic}``
   stream is PG LISTEN/NOTIFY-based (no Redis): a real NOTIFY on the
   converted schema's events channel must reach the stream. The per-job
   progress stream proves its PG-snapshot read (``_PROGRESS_SQL`` reads
   ``jobs`` — NOT converted, but the route lives on the converted
   schema) and its Redis bridge against a real broker, plus the
   documented 503 ``redis_not_configured`` degrade without one.
4. The metrics/count endpoints: ``/jobs/count`` (both tabs, with
   filters) and ``/api/history/stats`` — the dashboard's aggregate
   counts must be CORRECT on hypertables, not merely present.
5. The ops-side pages (queues overview with its stranded/orphan/liveness
   queries, queue detail, workers, leader, schedules, rate-limits,
   reservations) and the ``SELECT 1`` ready-probe shape the health
   router runs.
6. The failure surfaces: the watchdog/loop-stall machinery is
   schema-agnostic by design — pinned here by a source scan (no
   schema-interpolated SQL in ``_watchdog.py``/``_transient.py``; the
   health router's only DB statement is ``SELECT 1``) plus the
   behavioral half: the watchdog's ``loop_stalls`` tally reaches the
   workers page through ``workers.metadata`` and must render on the
   converted schema.

THE PARENTLESS-ATTEMPT WINDOW (the conversion's one honest behavioral
change the admin surface can see): dropping
``job_attempts_archive``'s FK means an attempt-archive row can outlive
its parent archive row (the two retentions chunk on different time
columns — finished_at vs started_at — so a chunk drop can take the
parent and not the attempt). Such rows are IMPOSSIBLE on vanilla PG.
Pinned here, on the real converted schema, with the orphan row actually
planted: the history walk and the stats aggregate neither break nor
count it (the walk never reads the attempts table; the stats join is a
LEFT JOIN FROM ``jobs_archive``), and the archived job detail page still
renders its OWN attempts.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import asyncpg
import httpx
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")

from fastapi import FastAPI  # Why: importorskip guards the optional extra first.

from taskq._ids import new_job_id, new_uuid
from taskq.constants import events_channel, progress_channel
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import (
    creator_labels,
    skip_test_without_docker,
)
from taskq.timescale import enable_hypertables
from taskq.web.admin import create_router, setup_admin_state

pytestmark = pytest.mark.integration

_TIMESCALE_IMAGE = (
    os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or "timescale/timescaledb:2.30.1-pg18"
)
# The progress SSE bridge needs a real broker. redis:7 is locally
# available; the pubsub contract it serves is the same one Dragonfly
# (the suite's shared broker) implements.
_REDIS_IMAGE = "redis:7"

# Every seeded timestamp derives from this FIXED instant (not now()): the
# differential requires both engines to seed byte-identical rows, and the
# absolute expectations (orders, counts, windows) must hold for the whole
# module run. The instant is days in the past at authoring time — old
# enough for the 24h stats window to be empty, recent enough for the
# relative-time rendering to stay stable across a module run.
_BASE = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)
_FAR_PAST = _BASE - timedelta(days=31)  # an aged event chunk on the converted table
_FAR_FUTURE = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

_N_BULK = 55  # the admin page size is 50, so both tabs walk real pages
_N_ARCHIVE_ATTEMPTS = 20

_CENSUS_STATUSES = [
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]

_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})


# ── The seed plan (built ONCE, applied to BOTH engines identically) ──────


@dataclass(frozen=True, slots=True)
class _JobRow:
    """One seeded job row, live or archived — the oracle's own record."""

    id: uuid.UUID
    actor: str
    queue: str
    status: str
    created_at: datetime
    finished_at: datetime | None
    tags: list[str] = field(default_factory=list)
    identity_key: str | None = None


@dataclass(slots=True)
class _SeedPlan:
    """Everything the seeder inserts and the oracles re-derive."""

    worker_ids: list[uuid.UUID]
    census: dict[str, uuid.UUID]
    bulk: list[_JobRow]
    archive: list[_JobRow]
    archive_attempt_parents: list[uuid.UUID]
    schedule_id: uuid.UUID


def _census_rows(plan: _SeedPlan) -> list[_JobRow]:
    """One live job per status, actor ``census_actor`` / queue ``default``."""
    rows = []
    for i, status in enumerate(_CENSUS_STATUSES):
        created = _BASE - timedelta(days=1) + timedelta(minutes=i)
        finished = created + timedelta(minutes=5) if status in _TERMINAL else None
        rows.append(
            _JobRow(
                id=plan.census[status],
                actor="census_actor",
                queue="default",
                status=status,
                created_at=created,
                finished_at=finished,
                tags=["alpha", "census"],
                identity_key=f"ik-{status}",
            )
        )
    return rows


def _bulk_rows(ids: list[uuid.UUID]) -> list[_JobRow]:
    """55 live terminal jobs, actor ``bulk_actor`` / queue ``bulk``.

    Every 5th row failed (with an error_class); the rest succeeded. The
    staggered created_at gives the live tab a real keyset walk and the
    time filters real brackets.
    """
    rows = []
    for i, jid in enumerate(ids):
        status = "failed" if i % 5 == 0 else "succeeded"
        created = _BASE + timedelta(minutes=i)
        rows.append(
            _JobRow(
                id=jid,
                actor="bulk_actor",
                queue="bulk",
                status=status,
                created_at=created,
                finished_at=created + timedelta(minutes=5),
                tags=["beta"],
            )
        )
    return rows


def _archive_rows(ids: list[uuid.UUID]) -> list[_JobRow]:
    """55 archived terminal jobs, actor ``archive_actor`` / queue ``bulk``.

    Mixes statuses; finished_at staggers across days for the union walk's
    seam and the windowed stats. (NULL finished_at is deliberately NOT
    seeded: the conversion makes the partition column NOT NULL — see
    ``test_null_partition_key_is_the_documented_divergence`` for the
    pinned semantics.)
    """
    rows = []
    for i, jid in enumerate(ids):
        if i % 11 == 0:
            status = "cancelled"  # i = 0, 11, 22, 33, 44
        elif i % 3 == 0:
            status = "failed"
        else:
            status = "succeeded"
        finished = _BASE + timedelta(hours=2 * i)
        rows.append(
            _JobRow(
                id=jid,
                actor="archive_actor",
                queue="bulk",
                status=status,
                created_at=finished - timedelta(minutes=10),
                finished_at=finished,
                tags=["gamma"],
                identity_key=f"arch-{i}",
            )
        )
    return rows


def _build_plan() -> _SeedPlan:
    """Build the whole fixture ONCE: the same plan seeds both engines.

    The ids come from the project's own generators (UUIDv7 job ids — the
    enqueuing-clock ordering the admin keysets lean on), generated a
    single time so vanilla and hypertable seed the EXACT same rows.
    """
    return _SeedPlan(
        worker_ids=[new_uuid() for _ in range(2)],
        census={s: new_job_id() for s in _CENSUS_STATUSES},
        bulk=_bulk_rows([new_job_id() for _ in range(_N_BULK)]),
        archive=_archive_rows([new_job_id() for _ in range(_N_BULK)]),
        archive_attempt_parents=[],
        schedule_id=new_uuid(),
    )


# ── Seeding (both engines, identical bytes) ───────────────────────────────


async def _seed_schema(conn: asyncpg.Connection, schema: str, plan: _SeedPlan) -> None:
    now = datetime.now(UTC)

    # Workers: one leader carrying a watchdog loop_stalls tally + capacity
    # in its metadata (the tally's only route into the admin UI), one plain.
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, last_seen_at, metadata)
        VALUES ($1, 'worker-alpha', 101, $2, $3, $4::jsonb)""",
        plan.worker_ids[0],
        ["default"],
        now,
        json.dumps(
            {
                "max_concurrency": 4,
                "notify_enabled": True,
                "loop_stalls": {"census_actor": {"dispatch": 3, "heartbeat": 1}},
            }
        ),
    )
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, last_seen_at, metadata)
        VALUES ($1, 'worker-beta', 102, $2, $3, '{{}}'::jsonb)""",
        plan.worker_ids[1],
        ["bulk"],
        now,
    )
    await conn.execute(
        f"INSERT INTO {schema}.maintenance_leader (worker_id, elected_at, last_seen_at) "
        "VALUES ($1, $2, $3)",
        plan.worker_ids[0],
        now,
        now,
    )

    # Census: one live job per status on queue "default".
    for i, status in enumerate(_CENSUS_STATUSES):
        row = _census_rows(plan)[i]
        created = row.created_at
        base_cols = (
            "id, actor, queue, payload, max_attempts, retry_kind, status, priority, "
            "tags, identity_key, created_at, scheduled_at"
        )
        base_vals = (
            f"$1, 'census_actor', 'default', '{{\"v\":1}}'::jsonb, 3, 'transient', "
            f"'{status}', 0, $2::text[], $3, $4, $4"
        )
        args: list[Any] = [row.id, row.tags, row.identity_key, created]
        if status == "running":
            await conn.execute(
                f"""INSERT INTO {schema}.jobs ({base_cols}, attempt, started_at,
                    locked_by_worker, lock_expires_at, last_heartbeat_at,
                    progress_state, progress_seq)
                VALUES ({base_vals}, 1, $5, $6, $7, $8, $9::jsonb, 7)""",
                *args,
                created + timedelta(minutes=1),
                plan.worker_ids[0],
                _BASE + timedelta(hours=1),
                created + timedelta(minutes=2),
                json.dumps({"pct": 42, "step": "upload"}),
            )
        elif status in ("pending", "scheduled"):
            await conn.execute(
                f"INSERT INTO {schema}.jobs ({base_cols}) VALUES ({base_vals})", *args
            )
        else:
            error_class = "WorkerCrashed" if status == "crashed" else "ValueError"
            await conn.execute(
                f"""INSERT INTO {schema}.jobs ({base_cols}, attempt, started_at,
                    finished_at, error_class, error_message)
                VALUES ({base_vals}, 1, $5, $6, $7, 'boom')""",
                *args,
                created + timedelta(minutes=1),
                row.finished_at,
                error_class,
            )

    # Bulk live terminal population (queue "bulk").
    await conn.executemany(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, tags, created_at, scheduled_at,
            started_at, finished_at, error_class)
        VALUES ($1, 'bulk_actor', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient', $2,
            1, $3::text[], $4, $4, $5, $6, $7)""",
        [
            (
                r.id,
                r.status,
                r.tags,
                r.created_at,
                r.created_at + timedelta(minutes=1),
                r.finished_at,
                "ValueError" if r.status == "failed" else None,
            )
            for r in plan.bulk
        ],
    )

    # The archive population.
    await conn.executemany(
        f"""INSERT INTO {schema}.jobs_archive (id, actor, queue, payload, max_attempts,
            retry_kind, status, attempt, tags, identity_key, created_at, scheduled_at,
            started_at, finished_at, error_class, archived_at, expire_at)
        VALUES ($1, 'archive_actor', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient', $2,
            1, $3::text[], $4, $5, $5, $6, $7, $8, $9, $10)""",
        [
            (
                r.id,
                r.status,
                r.tags,
                r.identity_key,
                r.created_at,
                r.created_at + timedelta(minutes=1),
                r.finished_at,
                "ValueError" if r.status == "failed" else None,
                (r.finished_at or r.created_at) + timedelta(minutes=1),
                (r.finished_at or r.created_at) + timedelta(days=365),
            )
            for r in plan.archive
        ],
    )

    # Attempts: live (the census failed job) and archived (the first 20
    # archive rows, one attempt each so the stats join fans nothing).
    failed_census = next(r for r in _census_rows(plan) if r.status == "failed")
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts (job_id, attempt, started_at, finished_at,
            outcome, error_class)
        VALUES ($1, 1, $2, $3, 'failed', 'ValueError')""",
        failed_census.id,
        failed_census.created_at + timedelta(minutes=1),
        failed_census.finished_at,
    )
    plan.archive_attempt_parents.extend(r.id for r in plan.archive[:_N_ARCHIVE_ATTEMPTS])
    await conn.executemany(
        f"""INSERT INTO {schema}.job_attempts_archive (job_id, attempt, started_at,
            finished_at, outcome, error_class)
        VALUES ($1, 1, $2, $3, $4, $5)""",
        [
            (
                r.id,
                r.created_at + timedelta(minutes=1),
                (r.finished_at or r.created_at) + timedelta(minutes=5),
                "succeeded" if r.status == "succeeded" else "failed",
                "ValueError" if r.status == "failed" else None,
            )
            for r in plan.archive[:_N_ARCHIVE_ATTEMPTS]
        ],
    )

    # Events on the live census jobs: two fresh each, two AGED (31 days —
    # a separate chunk on the converted table) on two of them.
    event_rows: list[tuple[uuid.UUID, datetime, str]] = []
    for row in _census_rows(plan):
        event_rows.append((row.id, row.created_at + timedelta(minutes=1), "state_change"))
        event_rows.append((row.id, row.created_at + timedelta(minutes=2), "progress"))
    event_rows.append((plan.census["pending"], _FAR_PAST, "state_change"))
    event_rows.append((plan.census["running"], _FAR_PAST, "state_change"))
    await conn.executemany(
        f"INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail) "
        "VALUES ($1, $2, $3, '{\"seq\":1}'::jsonb)",
        event_rows,
    )

    # One cron schedule for the ops page.
    await conn.execute(
        f"""INSERT INTO {schema}.cron_schedules (id, actor, cron_expr, timezone,
            next_fire_at, enabled)
        VALUES ($1, 'bulk_actor', '*/5 * * * *', 'UTC', $2, true)""",
        plan.schedule_id,
        _BASE + timedelta(days=365),
    )


# ── Containers, schemas, pools, apps (one module-wide lab) ───────────────


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    """One timescaledb container for the whole module (the hypertables
    suite's idiom): skips without Docker, labeled for stale sweeps."""
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
    """One schema + its pool + the real admin app mounted on it."""

    schema: str
    pool: asyncpg.Pool
    app: FastAPI


@dataclass
class _Lab:
    dsn: str
    plan: _SeedPlan
    vanilla: _Engine
    ht: _Engine
    redis_url: str
    ht_redis_app: FastAPI


@pytest.fixture(scope="module")
async def lab(ts_dsn: str, redis_broker: Any) -> AsyncIterator[_Lab]:
    """Both engines, built once: migrate BOTH schemas, convert one to
    hypertables (long retentions, policies deferred — nothing may drop a
    seeded row mid-run), seed both identically, mount the real admin app
    on each. The dev-env trio is set HERE (not just via the autouse
    per-test fixture) because ``create_router`` loads settings at
    module-fixture time, which runs before the function-scoped autouse
    fixture would have set them.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("TASKQ_ENVIRONMENT", "dev")
    mp.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    mp.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    try:
        run_tag = new_uuid().hex[:10]
        van_schema = f"tsadm_van_{run_tag}"
        ht_schema = f"tsadm_ht_{run_tag}"

        setup_conn = await asyncpg.connect(ts_dsn)
        try:
            for schema in (van_schema, ht_schema):
                await setup_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                await apply_pending(setup_conn, schema=schema)
            # Convert ONE engine. Retentions are long enough that the
            # seeded population is safe even if a policy ran (they are
            # also deferred 10 years — the hypertables suite's idiom).
            settings = WorkerSettings.load_from_dict(
                {
                    "TASKQ_PG_DSN": ts_dsn,
                    "TASKQ_SCHEMA_NAME": ht_schema,
                    "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
                    "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(timedelta(days=30).total_seconds())}s",
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

        redis_client = redis_asyncio.from_url(
            f"redis://{redis_broker.get_container_host_ip()}:"
            f"{redis_broker.get_exposed_port(6379)}/0"
        )
        lab = _Lab(
            dsn=ts_dsn,
            plan=plan,
            vanilla=_Engine(van_schema, van_pool, _mount(van_pool, van_schema)),
            ht=_Engine(ht_schema, ht_pool, _mount(ht_pool, ht_schema)),
            redis_url=f"redis://{redis_broker.get_container_host_ip()}:"
            f"{redis_broker.get_exposed_port(6379)}/0",
            ht_redis_app=_mount(ht_pool, ht_schema, redis_client=redis_client),
        )
        yield lab
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


async def _html(app: FastAPI, path: str) -> str:
    resp = await _get(app, path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.text


async def _json(app: FastAPI, path: str, **kwargs: Any) -> Any:
    resp = await _get(app, path, **kwargs)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:400]}"
    return resp.json()


_JOB_HREF_RE = re.compile(
    r'href="/admin/jobs/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"'
)
_NEXT_LINK_RE = re.compile(r'<a href="([^"]+)"[^>]*>\s*Next(?: page)?\s*<')
_PREV_LINK_RE = re.compile(
    r'<a href="([^"]+)"[^>]*>\s*<i data-lucide="chevron-left"[^>]*>\s*</i>\s*Previous'
)
_SHOWING_RE = re.compile(r"Showing (\d+) results")
# Relative/absolute timestamps the Jinja filters render from the database
# clock: identical across the two engines for FIXED seeded stamps, but
# now()-seeded stamps (worker heartbeats) can cross a humanize boundary
# between two requests milliseconds apart. Stripped before any full-HTML
# differential so the comparison tests structure, not wall-clock prose.
_VOLATILE_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?"
    r"|\bdays? ago\b|\bhours? ago\b|\bminutes? ago\b|\bseconds? ago\b"
    r"|\bjust now\b|\bin \d+ (?:seconds?|minutes?|hours?|days?)\b"
)
_CSRF_RE = re.compile(r'name="csrf_token"[^>]*value="[^"]*"|value="[^"]*"[^>]*name="csrf_token"')


def _stable_html(html: str) -> str:
    return _CSRF_RE.sub("", _VOLATILE_RE.sub("…", html))


def _ordered_job_ids(html: str) -> list[str]:
    """The job ids the page renders, in row order (deduped: the id cell
    and the actions cell both link the same row)."""
    seen: list[str] = []
    for m in _JOB_HREF_RE.finditer(html):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


def _next_link(html: str) -> str | None:
    m = _NEXT_LINK_RE.search(html)
    return m.group(1) if m else None


async def _walk(app: FastAPI, first_path: str, *, max_pages: int = 20) -> list[str]:
    """Follow the Next link to exhaustion, collecting row order."""
    ids: list[str] = []
    path: str | None = first_path
    pages = 0
    while path is not None:
        html = await _html(app, path)
        ids.extend(_ordered_job_ids(html))
        path = _next_link(html)
        pages += 1
        assert pages < max_pages, f"the walk from {first_path} did not terminate"
    return ids


def _diff(label: str, van: Any, ht: Any) -> None:
    """The differential itself: row-identical means EXACTLY equal."""
    assert van == ht, f"the engines disagree on {label}:\nvanilla={van!r}\nhypertable={ht!r}"


# ── Oracles: expected orders/counts re-derived from the seed plan ────────


def _expected_live_order(rows: list[_JobRow]) -> list[str]:
    """The live tab's ORDER BY (created_at DESC, id DESC), in Python."""
    ordered = sorted(rows, key=lambda r: (r.created_at, r.id), reverse=True)
    return [str(r.id) for r in ordered]


def _expected_archived_order(rows: list[_JobRow]) -> list[str]:
    """The archived tab's ORDER BY (finished_at DESC NULLS LAST, id DESC)
    — the live ordering JobOrdering renders, NULLs absolute at the end."""
    non_null = sorted(
        (r for r in rows if r.finished_at is not None),
        key=lambda r: (r.finished_at, r.id),
        reverse=True,
    )
    nulls = sorted((r for r in rows if r.finished_at is None), key=lambda r: r.id, reverse=True)
    return [str(r.id) for r in [*non_null, *nulls]]


def _expected_history_order(rows: list[_JobRow]) -> list[str]:
    """The history walk's ORDER BY (COALESCE(finished, ∞) DESC,
    created_at DESC, id DESC) — the NULL-finished rows pinned to the TOP
    of the walk by the COALESCE ceiling."""
    ordered = sorted(
        rows,
        key=lambda r: (r.finished_at or _FAR_FUTURE, r.created_at, r.id),
        reverse=True,
    )
    return [str(r.id) for r in ordered]


def _all_live_rows(plan: _SeedPlan) -> list[_JobRow]:
    return [*plan.bulk, *_census_rows(plan)]


def _terminal_live_rows(plan: _SeedPlan) -> list[_JobRow]:
    return [r for r in _all_live_rows(plan) if r.status in _TERMINAL]


# ── 1. The jobs list: every filter shape, row-identical on hypertables ───


async def test_jobs_list_live_tab_is_row_identical_and_ordered(lab: _Lab) -> None:
    """Live tab, default view: identical HTML on both engines, the page
    holds exactly 50 rows, and the walked order matches the oracle."""
    van = await _html(lab.vanilla.app, "/admin/jobs?tab=live")
    ht = await _html(lab.ht.app, "/admin/jobs?tab=live")
    _diff("jobs live tab", _stable_html(van), _stable_html(ht))

    showing = _SHOWING_RE.search(ht)
    assert showing is not None, "the jobs page must carry its Showing line"
    # The page size constant, pinned here as the walk's premise.
    assert int(showing.group(1)) == 50
    full_van = await _walk(lab.vanilla.app, "/admin/jobs?tab=live")
    full_ht = await _walk(lab.ht.app, "/admin/jobs?tab=live")
    _diff("jobs live walk", full_van, full_ht)
    expected = _expected_live_order(_all_live_rows(lab.plan))
    assert full_ht == expected, "the live walk must reproduce (created DESC, id DESC)"
    assert len(full_ht) == 63  # Why: 55 bulk + 8 census rows, the seed's exact population.


async def test_jobs_list_archived_tab_is_row_identical_on_hypertable(lab: _Lab) -> None:
    """Archived tab: the hypertable jobs_archive serves the SAME rows in
    the SAME order as vanilla, through the finished_at-NULLS-LAST keyset
    — the ordering whose seam the widened (id, finished_at) unique now
    underwrites."""
    van = await _html(lab.vanilla.app, "/admin/jobs?tab=archived")
    ht = await _html(lab.ht.app, "/admin/jobs?tab=archived")
    _diff("jobs archived tab", _stable_html(van), _stable_html(ht))

    full_van = await _walk(lab.vanilla.app, "/admin/jobs?tab=archived")
    full_ht = await _walk(lab.ht.app, "/admin/jobs?tab=archived")
    _diff("jobs archived walk", full_van, full_ht)
    assert full_ht == _expected_archived_order(lab.plan.archive)
    assert len(full_ht) == 55  # Why: the seed's exact archive population.
    # The oldest finished_at trails the walk (the NULLS-LAST end is empty:
    # no seeded row lacks a finished_at — see the divergence pin below).
    oldest = min(lab.plan.archive, key=lambda r: (r.finished_at, r.id))
    assert full_ht[-1] == str(oldest.id)


async def test_jobs_list_filters_are_row_identical_on_hypertable(lab: _Lab) -> None:
    """Every filter shape: status, queue, actor, tag, text search,
    identity_key, absolute time brackets, and the relative window —
    walked to exhaustion on BOTH engines and compared to the oracle."""
    plan = lab.plan
    bulk_failed = next(r for r in plan.bulk if r.status == "failed")

    shapes: list[tuple[str, list[str], bool]] = [
        # status filter, live tab: the bulk failed rows + the census one.
        (
            "/admin/jobs?tab=live&status=failed",
            _expected_live_order([r for r in _all_live_rows(plan) if r.status == "failed"]),
            True,
        ),
        # status filter, archived tab.
        (
            "/admin/jobs?tab=archived&status=failed",
            _expected_archived_order([r for r in plan.archive if r.status == "failed"]),
            True,
        ),
        # queue filter.
        ("/admin/jobs?tab=live&queue=bulk", _expected_live_order(plan.bulk), True),
        # actor substring filter.
        ("/admin/jobs?tab=live&actor=bulk", _expected_live_order(plan.bulk), True),
        # tag overlap filter (tags && text[]).
        ("/admin/jobs?tab=live&tags=alpha", _expected_live_order(_census_rows(plan)), True),
        # text search: a fragment of the id's RANDOM tail (UUIDv7 ids share
        # their clock prefix, so the head would match every row) matches
        # exactly one job (id::text ILIKE).
        (
            f"/admin/jobs?tab=live&search={str(bulk_failed.id).split('-')[-1][:12]}",
            [str(bulk_failed.id)],
            True,
        ),
        # identity_key exact filter.
        ("/admin/jobs?tab=live&identity_key=ik-running", [str(plan.census["running"])], True),
        # absolute from/to window: the list page forms a window ONLY
        # when BOTH bounds are present (a lone bound is ignored by
        # _parse_time_range — pre-existing page semantics, pinned here
        # because the differential must exercise the real shapes). The
        # windows partition the seeds: bulk inside, census before.
        (
            f"/admin/jobs?tab=live&time_from={quote_plus(_BASE.isoformat())}"
            f"&time_to={quote_plus((_BASE + timedelta(hours=2)).isoformat())}",
            _expected_live_order(plan.bulk),
            False,
        ),
        (
            # The upper bound is INCLUSIVE (created_at <= time_to), so it
            # sits one second below _BASE — bulk[0] is created exactly at
            # _BASE and must stay out of the census window.
            f"/admin/jobs?tab=live&time_from={quote_plus((_BASE - timedelta(days=1)).isoformat())}"
            f"&time_to={quote_plus((_BASE - timedelta(seconds=1)).isoformat())}",
            _expected_live_order(_census_rows(plan)),
            False,
        ),
        # relative window: every seeded row is ~6 days old, so "24h" is
        # the empty result on BOTH engines (the clock_timestamp() bound).
        ("/admin/jobs?tab=live&time_range=24h", [], True),
    ]

    for path, expected, walk_all in shapes:
        if walk_all:
            van = await _walk(lab.vanilla.app, path)
            ht = await _walk(lab.ht.app, path)
        else:
            # Page 1 only: the pagination macro (filter_qs) does NOT
            # carry time_from/time_to, so the walk's page turns silently
            # drop the window — a pre-existing UI quirk this differential
            # refuses to launder (the window applies to the queried page;
            # the engines agree page-by-page either way). The expectation
            # is the window's TOP PAGE, not the whole window.
            van = _ordered_job_ids(await _html(lab.vanilla.app, path))
            ht = _ordered_job_ids(await _html(lab.ht.app, path))
            expected = expected[:50]
        _diff(f"filter walk {path}", van, ht)
        assert ht == expected, f"the filter shape {path} returned the wrong rows"


async def test_jobs_list_cursor_pagination_walks_both_directions(lab: _Lab) -> None:
    """Keyset pagination: page 2 via the rendered Next cursor, and page 1
    again via the rendered Previous cursor — the seam the hypertable's
    widened unique index must still serve exactly, in both directions."""
    page1 = await _html(lab.ht.app, "/admin/jobs?tab=live")
    next_href = _next_link(page1)
    assert next_href is not None, "63 live rows must paginate"
    page2 = await _html(lab.ht.app, next_href)
    page2_ids = _ordered_job_ids(page2)
    assert len(page2_ids) == 13  # Why: 63 - 50, the exact tail.

    prev_match = _PREV_LINK_RE.search(page2)
    assert prev_match is not None, "page 2 must offer a Previous link"
    page1_again_ht = await _walk(lab.ht.app, prev_match.group(1))
    page1_again_van = await _walk(lab.vanilla.app, prev_match.group(1))
    _diff("prev-page walk", page1_again_van, page1_again_ht)
    page1_ids = _ordered_job_ids(page1)
    assert page1_again_ht[: len(page1_ids)] == page1_ids, (
        "walking Back from page 2 must re-serve page 1 exactly (the walk "
        "then continues forward through page 2 again)"
    )
    assert len(page1_again_ht) == 63  # Why: 50 re-served + 13 walked forward.


# ── 2. The history/archive views on hypertables ──────────────────────────


async def test_history_union_walk_is_row_identical_and_correct(lab: _Lab) -> None:
    """/history: archive union live-terminal under the three-column seam —
    identical pages on both engines, the exact expected order, and the
    NULL-finished rows (the __NULL__ sentinel range) pinned to the TOP of
    the walk where they must render, not break it, on the hypertable."""
    van = await _html(lab.vanilla.app, "/admin/history")
    ht = await _html(lab.ht.app, "/admin/history")
    _diff("history page", _stable_html(van), _stable_html(ht))

    full_van = await _walk(lab.vanilla.app, "/admin/history")
    full_ht = await _walk(lab.ht.app, "/admin/history")
    _diff("history walk", full_van, full_ht)

    expected = _expected_history_order([*_terminal_live_rows(lab.plan), *lab.plan.archive])
    assert full_ht == expected
    assert len(full_ht) == 115  # Why: 55 archive + 55 bulk terminal + 5 census terminal.

    # The newest archive row leads the walk. Every seeded row carries a
    # finished_at on purpose: the walk's NULL-sentinel range is
    # UNREACHABLE on hypertable mode (the partition column is NOT NULL),
    # pinned in test_null_partition_key_is_the_documented_divergence.
    newest = max(lab.plan.archive, key=lambda r: (r.finished_at, r.id))
    assert full_ht[0] == str(newest.id)


async def test_history_filters_are_row_identical(lab: _Lab) -> None:
    """History status/queue/actor filters, walked and compared — the
    filter predicates run against the hypertable's chunks exactly as they
    run against the vanilla table."""
    for path in (
        "/admin/history?status=failed",
        "/admin/history?status=cancelled",
        "/admin/history?queue=bulk",
        "/admin/history?actor=archive",
    ):
        van = await _walk(lab.vanilla.app, path)
        ht = await _walk(lab.ht.app, path)
        _diff(f"history filter {path}", van, ht)


async def test_actor_stats_aggregate_is_correct_on_hypertables(lab: _Lab) -> None:
    """``/api/history/stats``: the per-(actor, queue) aggregate over the
    archive union live-terminal population — correct counts on the converted
    schema (all-time), and the windowed variant (the chunk-pruning read)
    agrees with vanilla in both a covering and an empty window."""
    plan = lab.plan

    def _expected() -> dict[tuple[str, str], dict[str, int]]:
        per: dict[tuple[str, str], dict[str, int]] = {}
        for r in [*plan.archive, *_terminal_live_rows(plan)]:
            bucket = per.setdefault(
                (r.actor, r.queue),
                {"total": 0, "succeeded": 0, "failed": 0, "cancelled": 0},
            )
            bucket["total"] += 1
            bucket[r.status] = bucket.get(r.status, 0) + 1
        return per

    van_all = await _json(lab.vanilla.app, "/admin/api/history/stats")
    ht_all = await _json(lab.ht.app, "/admin/api/history/stats")
    _diff("stats all-time", van_all, ht_all)

    got = {(row["actor"], row["queue"]): row for row in ht_all["actors"]}
    for key, counts in _expected().items():
        row = got[key]
        for name, value in counts.items():
            assert row[name] == value, f"stats[{key}][{name}] = {row[name]}, want {value}"

    for window in ("30d", "24h"):
        van_w = await _json(lab.vanilla.app, f"/admin/api/history/stats?window={window}")
        ht_w = await _json(lab.ht.app, f"/admin/api/history/stats?window={window}")
        _diff(f"stats window={window}", van_w, ht_w)

    wide = await _json(lab.ht.app, "/admin/api/history/stats?window=30d")
    empty = await _json(lab.ht.app, "/admin/api/history/stats?window=24h")
    assert wide["actors"], "the 30d window must cover the ~6-day-old seeds"
    assert empty["actors"] == [], "the 24h window covers no seeded row"


async def test_parentless_archive_attempts_are_documented_not_fatal(lab: _Lab) -> None:
    """The conversion drops ``job_attempts_archive``'s FK to
    ``jobs_archive`` — so an attempt can outlive its parent archive row
    (the two tables chunk on different time columns). Such a row is
    impossible on vanilla PG. Pinned here, on the real converted schema:
    the parentless row CAN be planted (and cannot be, on vanilla), the
    history walk and the stats aggregate neither break nor count it, and
    the archived job detail page still renders its OWN attempts."""
    stats_before = await _json(lab.ht.app, "/admin/api/history/stats")
    history_before = await _walk(lab.ht.app, "/admin/history")

    orphan_parent = new_job_id()  # deliberately NEVER seeded as a job row
    conn = await asyncpg.connect(lab.dsn)
    try:
        # Proof of the dropped FK: the same insert is IMPOSSIBLE on the
        # vanilla engine (FK violation) and possible on the hypertable.
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

    stats_after = await _json(lab.ht.app, "/admin/api/history/stats")
    history_after = await _walk(lab.ht.app, "/admin/history")
    assert stats_after == stats_before, (
        "a parentless attempt-archive row must be invisible to the stats "
        "aggregate (LEFT JOIN from jobs_archive), not counted"
    )
    assert history_after == history_before, (
        "a parentless attempt-archive row must not disturb the history walk"
    )

    # The detail page for a REAL archived job still renders its attempts.
    parent = lab.plan.archive_attempt_parents[0]
    ht_detail = await _html(lab.ht.app, f"/admin/jobs/{parent}")
    van_detail = await _html(lab.vanilla.app, f"/admin/jobs/{parent}")
    _diff("archived job detail", _stable_html(ht_detail), _stable_html(van_detail))
    assert "Attempt History" in ht_detail


async def test_null_partition_key_is_the_documented_divergence(
    lab: _Lab,
    ts_dsn: str,
) -> None:
    """THE one row shape the hypertable mode cannot hold: a NULL
    ``finished_at`` archive row — the row the history walk's
    ``__NULL__`` sentinel cursor exists to describe.

    TimescaleDB makes the partition column NOT NULL (``finished_at`` for
    ``jobs_archive``). Pinned in both directions, against the real
    engines:

    * the SAME insert vanilla PG accepts is rejected on the converted
      schema with ``NotNullViolationError`` — the DML-time half;
    * ``enable_hypertables`` over an archive that ALREADY holds such a
      row fails LOUDLY at migrate time with the same violation — never
      silently dropping or silently converting.

    No production write can produce the row (every terminal transition
    stamps ``finished_at = clock_timestamp()`` — backend/_sql_templates.py,
    all ten terminal arms), so the sentinel range is dead-on-arrival on
    hypertable mode, and a legacy archive holding NULLs fails its first
    ``taskq migrate up`` after opting in — the operator backfills first.
    The admin-side consequence: the history walk's NULL-ceiling/sentinel
    machinery is unreachable (harmless) on converted schemas.
    """
    now = datetime.now(UTC)

    async def _insert_null_finished(schema: str, jid: uuid.UUID) -> None:
        conn = await asyncpg.connect(ts_dsn)
        try:
            await conn.execute(
                f"""INSERT INTO {schema}.jobs_archive (
                        id, actor, queue, payload, max_attempts, retry_kind,
                        status, scheduled_at, finished_at, archived_at, expire_at)
                    VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                        $2, NULL, $3, $4)""",
                jid,
                now,
                now + timedelta(minutes=1),
                now + timedelta(days=365),
            )
        finally:
            await conn.close()

    # DML-time half: the same insert, opposite outcomes, back to back.
    with pytest.raises(asyncpg.NotNullViolationError, match="finished_at"):
        await _insert_null_finished(lab.ht.schema, new_job_id())
    vanilla_row = new_job_id()
    await _insert_null_finished(lab.vanilla.schema, vanilla_row)  # vanilla accepts
    # Leave no residue: every other test's differential assumes the two
    # engines hold identical populations, whatever order tests run in.
    conn = await asyncpg.connect(ts_dsn)
    try:
        await conn.execute(
            f"DELETE FROM {lab.vanilla.schema}.jobs_archive WHERE id = $1", vanilla_row
        )
    finally:
        await conn.close()

    # Migrate-time half: conversion of an archive ALREADY holding the row
    # must fail loudly, on a fresh schema on the shared container.
    schema = f"tsadm_null_{new_uuid().hex[:10]}"
    setup = await asyncpg.connect(ts_dsn)
    try:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(setup, schema=schema)
        await _insert_null_finished(schema, new_job_id())
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": ts_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            }
        )
        with pytest.raises(asyncpg.NotNullViolationError, match="finished_at"):
            await enable_hypertables(setup, schema=schema, settings=settings)
        # The refusal is loud but clean: the schema stays vanilla and the
        # row stays put (the conversion did not half-apply on this table).
        tables = await setup.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_name = 'jobs_archive'",
            schema,
        )
        assert len(tables) == 1
        n = await setup.fetchval(f"SELECT count(*) FROM {schema}.jobs_archive")
        assert n == 1
    finally:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await setup.close()


# ── 3. The SSE streams + progress endpoints on the converted schema ──────


async def _stream_until(
    app: FastAPI,
    path: str,
    needle: bytes,
    *,
    timeout_s: float = 20.0,
) -> bytes:
    """Drive the ASGI app DIRECTLY and read the streaming body until
    *needle* appears (or the stream ends by itself).

    httpx's ASGITransport cannot be used for infinite SSE streams: it
    never delivers ``http.disconnect`` when the client stops reading, so
    the app-side generator never finishes and the response close hangs
    forever (the run that found this spent its whole 120 s budget twice).
    This harness is the same transport level with a REAL client
    disconnect: once the needle is seen (or the app finishes), the
    disconnect message reaches the response, the generator's finally
    runs its bounded cleanup, and the app task completes.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"testserver")],
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
        # waits for a uvicorn shutdown flag no in-process test ever sets)
        # and holds no retrievable reference — cancel it by name and
        # retrieve the cancellation, the same reap
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


async def test_admin_sse_stream_delivers_real_notify_on_converted_schema(
    lab: _Lab,
) -> None:
    """The admin ``/sse/{topic}`` stream is PG LISTEN/NOTIFY based — the
    one stream with no Redis in the path. On the converted schema: the
    stream opens, and a real NOTIFY on the events channel reaches it as
    an ``event: state_change`` frame."""
    conn = await asyncpg.connect(lab.dsn)
    channel = events_channel(lab.ht.schema)
    payload = json.dumps({"type": "state_change", "job_id": str(lab.plan.census["running"])})
    notified = asyncio.Event()

    async def _notify_repeatedly() -> None:
        # The stream's LISTEN may not be subscribed yet when the first
        # frames arrive; repeating gives the subscribe a bounded,
        # deterministic window to land.
        while not notified.is_set():
            await conn.execute("SELECT pg_notify($1, $2)", channel, payload)
            await asyncio.sleep(0.2)

    try:
        notifier = asyncio.create_task(_notify_repeatedly())
        try:
            buf = await _stream_until(lab.ht.app, "/admin/sse/queues", b"state_change")
        finally:
            notified.set()
            notifier.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await notifier
        assert b"awaiting_progress_backend" in buf, buf[:400]
        assert payload.encode() in buf, "the NOTIFY payload must reach the stream"
    finally:
        await conn.close()


async def test_admin_sse_mode_probe_agrees_across_engines(lab: _Lab) -> None:
    """/sse/mode (the poll/realtime badge probe) answers identically with
    no Redis configured on either engine."""
    _diff(
        "sse mode probe",
        await _json(lab.vanilla.app, "/admin/sse/mode"),
        await _json(lab.ht.app, "/admin/sse/mode"),
    )


async def test_progress_poll_state_on_converted_schema(lab: _Lab) -> None:
    """The per-job poll-state endpoint: identical bodies for a running
    job (its progress snapshot), a 304 for an unchanged ETag, a 404 for
    an archived id (the poll reads ``jobs`` only — the archive is not
    consulted, on either engine), and the documented 503 degrade with no
    Redis."""
    running = lab.plan.census["running"]

    van_state = await _json(lab.vanilla.app, f"/admin/jobs/api/job/{running}/state")
    ht_state = await _json(lab.ht.app, f"/admin/jobs/api/job/{running}/state")
    _diff("poll state", van_state, ht_state)
    assert ht_state == {
        "status": "running",
        "progress_state": {"pct": 42, "step": "upload"},
        "progress_seq": 7,
    }

    etag = f'"{ht_state["progress_seq"]}"'
    for app in (lab.vanilla.app, lab.ht.app):
        resp = await _get(
            app, f"/admin/jobs/api/job/{running}/state", headers={"If-None-Match": etag}
        )
        assert resp.status_code == 304, resp.text
        assert resp.headers["etag"] == etag

    archived = lab.plan.archive[2]
    for app, label in ((lab.vanilla.app, "vanilla"), (lab.ht.app, "hypertable")):
        resp = await _get(app, f"/admin/jobs/api/job/{archived.id}/state")
        assert resp.status_code == 404, f"{label}: {resp.status_code}"

    van_503 = await _get(lab.vanilla.app, f"/admin/jobs/api/job/{running}/progress/stream")
    ht_503 = await _get(lab.ht.app, f"/admin/jobs/api/job/{running}/progress/stream")
    assert ht_503.status_code == 503
    assert ht_503.json() == {"error": "redis_not_configured"}
    _diff("progress 503 body", van_503.text, ht_503.text)


async def test_progress_sse_bridge_streams_on_converted_schema(lab: _Lab) -> None:
    """The full progress SSE bridge against a REAL broker, mounted on the
    converted schema: the first frame is the PG snapshot read from
    ``jobs`` (the un-converted parent of the hypertable family), a live
    pub/sub envelope flows through, and a terminal job's stream closes
    with ``event: done``."""
    running = lab.plan.census["running"]
    import redis.asyncio as redis_asyncio

    publisher = redis_asyncio.from_url(lab.redis_url)
    url = f"/admin/jobs/api/job/{running}/progress/stream"
    envelope = json.dumps(
        {
            "seq": 8,
            "job_id": str(running),
            "actor": "census_actor",
            "ts": _BASE.isoformat(),
            "status": "running",
            "kind": "progress",
            "terminal": False,
            "step": "upload",
        },
        separators=(",", ":"),
    )
    stop = asyncio.Event()

    async def _publish_repeatedly() -> None:
        # The bridge subscribes BEFORE its snapshot read, but the test
        # cannot observe when the subscription landed; repeating gives it
        # a bounded, deterministic window. Pub/sub does not buffer, so a
        # single pre-subscription publish would be silently lost.
        while not stop.is_set():
            await publisher.publish(progress_channel(lab.ht.schema, running), envelope)
            await asyncio.sleep(0.2)

    try:
        pump = asyncio.create_task(_publish_repeatedly())
        try:
            body = await _stream_until(lab.ht_redis_app, url, b'"seq":8')
        finally:
            stop.set()
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump
    finally:
        await publisher.aclose()

    # The first frame is the PG snapshot read from `jobs` (the
    # un-converted parent of the hypertable family); the live pub/sub
    # envelope is forwarded VERBATIM once it arrives.
    assert b"event: progress" in body
    # The snapshot data is the jsonb TEXT as PG wrote it (jsonb's own
    # serialization carries the space after the colon) passed through
    # _serialize_progress_state verbatim.
    assert b'"pct": 42' in body, body[:400]
    assert envelope.encode() in body, "the live envelope must be forwarded verbatim"

    # Terminal snapshot: snapshot frame, then done, then the stream ends
    # BY ITSELF (no needle needed — the harness reads to natural end).
    terminal = next(r for r in lab.plan.bulk if r.status == "succeeded")
    body = await _stream_until(
        lab.ht_redis_app, f"/admin/jobs/api/job/{terminal.id}/progress/stream", b"event: done"
    )
    assert b"event: terminal" in body
    assert b"event: done" in body


# ── 4. The metrics/count endpoints ────────────────────────────────────────


async def test_jobs_count_endpoints_are_correct_on_hypertables(lab: _Lab) -> None:
    """/jobs/count: exact totals on both tabs, with filters, identical
    across engines — the dashboard's number, proven on hypertables."""
    plan = lab.plan
    shapes: list[tuple[str, int]] = [
        ("/admin/jobs/count?tab=live", len(_all_live_rows(plan))),
        ("/admin/jobs/count?tab=archived", len(plan.archive)),
        (
            "/admin/jobs/count?tab=live&status=failed",
            sum(1 for r in _all_live_rows(plan) if r.status == "failed"),
        ),
        (
            "/admin/jobs/count?tab=archived&status=cancelled",
            sum(1 for r in plan.archive if r.status == "cancelled"),
        ),
        ("/admin/jobs/count?tab=live&queue=default", len(_census_rows(plan))),
        ("/admin/jobs/count?tab=archived&actor=archive&tags=gamma", len(plan.archive)),
        (
            f"/admin/jobs/count?tab=live&time_from={quote_plus(_BASE.isoformat())}"
            f"&time_to={quote_plus((_BASE + timedelta(hours=2)).isoformat())}",
            len(plan.bulk),
        ),
        ("/admin/jobs/count?tab=live&time_range=24h", 0),
    ]
    for path, expected in shapes:
        van = await _json(lab.vanilla.app, path)
        ht = await _json(lab.ht.app, path)
        _diff(f"count {path}", van, ht)
        assert ht == {"count": expected}, f"count shape {path} is wrong on the hypertable"


# ── 5. The ops/sweep-side pages on the converted schema ──────────────────


async def test_queue_overview_queries_on_converted_schema(lab: _Lab) -> None:
    """/queues: the overview roll-up, the live-workers liveness query,
    the stranded-jobs detector, and the orphan probe all answer on the
    converted schema, identically to vanilla."""
    van = await _html(lab.vanilla.app, "/admin/queues")
    ht = await _html(lab.ht.app, "/admin/queues")
    _diff("queues page", _stable_html(van), _stable_html(ht))
    # The census pending row has no actor_config row and (once the seeded
    # heartbeats age out of the liveness window) no live worker: the
    # stranded detector must surface queue "default" on both engines.
    assert ">default<" in ht
    assert "bulk" in ht


async def test_queue_detail_paged_query_on_converted_schema(lab: _Lab) -> None:
    """/queues/{queue}: the scheduled_at keyset walk over ``jobs`` —
    identical rows and cursors on the converted schema."""
    van = await _html(lab.vanilla.app, "/admin/queues/default?status=pending")
    ht = await _html(lab.ht.app, "/admin/queues/default?status=pending")
    _diff("queue detail", _stable_html(van), _stable_html(ht))
    assert _ordered_job_ids(ht) == [str(lab.plan.census["pending"])]

    van = await _html(lab.vanilla.app, "/admin/queues/bulk?status=succeeded")
    ht = await _html(lab.ht.app, "/admin/queues/bulk?status=succeeded")
    _diff("queue detail bulk", _stable_html(van), _stable_html(ht))
    assert _ordered_job_ids(van) == _ordered_job_ids(ht)
    assert len(_ordered_job_ids(ht)) > 0


async def test_workers_leader_pages_on_converted_schema(lab: _Lab) -> None:
    """/workers and /leader: the lateral running-count join and the
    server-side watchdog-freshness verdict answer identically."""
    van = await _html(lab.vanilla.app, "/admin/workers")
    ht = await _html(lab.ht.app, "/admin/workers")
    _diff("workers page", _stable_html(van), _stable_html(ht))
    assert "worker-alpha" in ht and "worker-beta" in ht
    # The running census job is locked by worker-alpha: 1 running row of
    # the 4 the worker's metadata declares (template renders newlines
    # around the cell text).
    assert "1 / 4" in ht

    van = await _html(lab.vanilla.app, "/admin/leader")
    ht = await _html(lab.ht.app, "/admin/leader")
    _diff("leader page", _stable_html(van), _stable_html(ht))
    assert "eader" in ht  # the Leader badge or the empty-leader heading


async def test_ops_pages_render_on_converted_schema(lab: _Lab) -> None:
    """Schedules, rate-limits, reservations: the ops pages' schema-bound
    reads (cron_schedules, rate_limit_buckets, reservation_slots — none
    converted, but the pages are mounted on the converted schema and
    must be byte-stable across engines)."""
    for path in ("/admin/schedules", "/admin/rate-limits", "/admin/reservations"):
        van = await _html(lab.vanilla.app, path)
        ht = await _html(lab.ht.app, path)
        _diff(f"ops page {path}", _stable_html(van), _stable_html(ht))
    assert "*/5 * * * *" in await _html(lab.ht.app, "/admin/schedules")


async def test_ready_probe_statement_runs_on_converted_schema(lab: _Lab) -> None:
    """The health router's ready probe is ``SELECT 1`` — the only DB
    statement in ``worker/health.py`` — and it runs on the converted
    schema's pool (the ready body's schema-bound surface, in full)."""
    async with lab.ht.pool.acquire() as conn:
        assert await conn.fetchval("SELECT 1") == 1


# ── 6. The failure surfaces: watchdog / loop-stall machinery ─────────────


def test_watchdog_machinery_is_schema_agnostic() -> None:
    """The watchdog/loop-stall/transient-error machinery must issue NO
    schema-interpolated SQL at all — its tally reaches the admin UI
    through ``workers.metadata``, written by the heartbeat, never by a
    direct query against the (converted) job tables. Pinned as a source
    contract: no FROM/INTO/UPDATE/JOIN against ``{schema}`` anywhere in
    ``_watchdog.py`` or ``_transient.py``."""
    worker_dir = Path(__import__("taskq.worker", fromlist=["__path__"]).__path__[0])
    sql_shape = re.compile(r"\b(?:FROM|INTO|UPDATE|JOIN)\b\s*[\"']?\{schema\}", re.IGNORECASE)
    for name in ("_watchdog.py", "_transient.py"):
        src = (worker_dir / name).read_text()
        assert not sql_shape.search(src), (
            f"{name} grew schema-interpolated SQL: the watchdog machinery "
            "must stay schema-agnostic or this pin (and the hypertable "
            "audit) must be revisited"
        )


def test_health_router_issues_only_select_one() -> None:
    """Same source contract for the health router: the live/ready shapes
    are schema-free — ``SELECT 1`` is the only statement (the statements
    live in ``worker/health.py``; ``web/health.py`` is its transport)."""
    web_dir = Path(__import__("taskq.web", fromlist=["__path__"]).__path__[0])
    src = (web_dir / "health.py").read_text()
    assert "{schema}" not in src, "the health router must not interpolate the schema"

    worker_dir = Path(__import__("taskq.worker", fromlist=["__path__"]).__path__[0])
    worker_src = (worker_dir / "health.py").read_text()
    executes = re.findall(r"execute\(\s*\"([^\"]+)\"", worker_src)
    assert executes, "health.py's DB statements must stay greppable"
    assert all(stmt == "SELECT 1" for stmt in executes), executes
    assert "{schema}" not in worker_src, "the health router must not interpolate the schema"


async def test_watchdog_stall_tally_renders_on_converted_schema(lab: _Lab) -> None:
    """The behavioral half: the watchdog's ``loop_stalls`` tally (written
    to ``workers.metadata``) renders through the workers page on the
    converted schema — the machinery's only schema-adjacent surface."""
    ht = await _html(lab.ht.app, "/admin/workers")
    assert "census_actor x4 (dispatch 3, heartbeat 1)" in ht, (
        "the watchdog tally must render hottest-first (kind counts "
        "descending inside the parens) through the workers page on the "
        "hypertable schema"
    )

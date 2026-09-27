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
7. The mutation actions, driven through the REAL router with a REAL
   ``PostgresBackend`` (route census, ``@router.post`` over
   ``src/taskq/web/admin``: job cancel, job retry, schedule
   enable/disable/skip/run-now, rate-limit reset, actor deregister —
   there are NO bulk-cancel, actor-config-edit, or queue
   set-mode/set-max-concurrent routes to test): each action lands on
   both engines at once and the resulting rows, the audit-trail rows
   (``admin_audit``, written because the dev env enables
   ``admin_actions_enabled``), and the re-rendered pages are identical.
8. The job detail view under the parentless-attempt window: a job
   whose archived attempts span chunks (one aged 31 days) renders its
   full attempt history honestly, and a parentless attempt row stays
   invisible-and-harmless to every other job's page.
9. Pagination across chunk boundaries: with a synthetic 1-day chunk
   grid on ``jobs_archive``, the archive tab's keyset walk — plain and
   with a time-window filter CARRIED THROUGH the page turns (the
   render's own Next link drops ``time_from``/``time_to``, so the walk
   constructs the cursor URLs the server must honor) — crosses the
   chunk seams with no skipped rows and no duplicates, and the count
   endpoint with the same window proves chunk exclusion ate nothing.
10. The failure renderings: a missing job, a hand-edited (malformed)
    cursor, a partial/invalid history cursor, a garbage time filter, a
    NUL filter, an invalid status, disabled admin actions — every
    error status and body identical across engines.

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
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import (
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    DEFAULT_MAX_RETRY_BACKOFF,
    MAX_RESULT_BYTES,
    events_channel,
    progress_channel,
)
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import (
    creator_labels,
    skip_test_without_docker,
)
from taskq.timescale import TimescaleDBUnavailableError, enable_hypertables
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth import IdentityClaims
from taskq.web.admin.ops import (  # pyright: ignore[reportPrivateUsage]  # Why: the run-now differential must wait out the real cooldown constant, not a drifting copy.
    _SCHEDULE_RUN_COOLDOWN_SECONDS,  # pyright: ignore[reportPrivateUsage]
)

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
    # The mutation surface mounts the REAL PostgresBackend and a fixed
    # authenticated principal (the dev no-auth principal is "anonymous" —
    # the audit trail's own tier proves that path); the read-only apps
    # above stay backend-less so their differentials stay page-pure.
    vanilla_actions: FastAPI
    ht_actions: FastAPI


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

        def _mount_actions(pool: asyncpg.Pool, schema: str) -> FastAPI:
            """The same real router with the real PG backend + a fixed
            authenticated principal — the mutation surface's mount
            (tests/test_admin_audit_trail.py's idiom)."""
            bundle = create_router(
                pool,
                schema=schema,
                redis_client=None,
                base_path="/admin",
                backend=_make_backend(pool, schema),
                auth_dependency=_fixed_auth(),
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
            vanilla_actions=_mount_actions(van_pool, van_schema),
            ht_actions=_mount_actions(ht_pool, ht_schema),
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


def _event_log_kinds(html: str) -> list[str]:
    """The event kinds the detail page's Event Log section renders, in order."""
    section = html.split("Event Log", 1)[1].split("Admin Audit Trail", 1)[0]
    return re.findall(r'font-mono">(\w+)</span>', section)


async def test_archived_detail_events_match_the_live_detail_events(lab: _Lab) -> None:
    """Issue #337: the detail route's archive arm rendered ``events = []``
    without reading the ledger at all, so an archived job whose
    ``job_events`` rows are intact claimed "No events recorded". Pinned
    differentially: the SAME job's event log renders identically through
    the live arm and the archive arm, on BOTH engines (on the converted
    one ``job_events`` is the hypertable the archive arm must read)."""
    now = datetime.now(UTC)

    async def _plant(engine: _Engine, jid: uuid.UUID) -> None:
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(
                f"""INSERT INTO {engine.schema}.jobs (id, actor, queue, payload,
                        max_attempts, retry_kind, status, attempt, created_at,
                        scheduled_at, started_at, finished_at)
                    VALUES ($1, 'event_probe', 'eventq', '{{}}'::jsonb, 3, 'transient',
                        'succeeded', 1, $2, $2, $3, $4)""",
                jid,
                _BASE,
                _BASE + timedelta(minutes=1),
                _BASE + timedelta(minutes=5),
            )
            await conn.executemany(
                f"INSERT INTO {engine.schema}.job_events (job_id, occurred_at, kind, detail) "
                "VALUES ($1, $2, $3, $4::jsonb)",
                [
                    (
                        jid,
                        _BASE + timedelta(minutes=1, seconds=1),
                        "state_change",
                        '{"from": "pending", "to": "running"}',
                    ),
                    (
                        jid,
                        _BASE + timedelta(minutes=1, seconds=2),
                        "progress",
                        '{"percent": 100}',
                    ),
                ],
            )
        finally:
            await conn.close()

    async def _archive_move(engine: _Engine, jid: uuid.UUID) -> None:
        """The lifecycle's own move, byte-for-byte: the archive row gains
        only archived_at/expire_at, and the live row goes away."""
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(
                f"""INSERT INTO {engine.schema}.jobs_archive (
                        id, actor, queue, payload, max_attempts, retry_kind,
                        status, attempt, created_at, scheduled_at, started_at,
                        finished_at, archived_at, expire_at)
                    SELECT id, actor, queue, payload, max_attempts, retry_kind,
                        status, attempt, created_at, scheduled_at, started_at,
                        finished_at, $2, $3
                    FROM {engine.schema}.jobs WHERE id = $1""",
                jid,
                now,
                _FAR_FUTURE,
            )
            await conn.execute(f"DELETE FROM {engine.schema}.jobs WHERE id = $1", jid)
        finally:
            await conn.close()

    async def _keep_events_over_the_archive(engine: _Engine, jid: uuid.UUID) -> None:
        """Re-plant the ledger rows the archive move's FK cascade removed.

        Production's archive sweep deletes the live row and
        ``job_events.job_id REFERENCES jobs ON DELETE CASCADE`` takes the
        events with it TODAY (the event-watermark migration documents the
        cascade: there is no job_events_archive) -- so the state this
        constructs, an archived job whose ``job_events`` rows are intact,
        is UNREACHABLE in production: the rows are re-inserted past the FK
        (the trigger bypass runs as the container superuser the lab
        already uses). What this differential pins is the archive arm's
        read-and-render contract -- the page renders whatever the ledger
        holds, identical to the live arm's render -- the forward-compatible
        property the read exists for; the production-reachable empty state
        is pinned honestly (the removed-at-archive notice) by
        test_attack337_cascade_truth.py."""
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute("SET session_replication_role = replica")
            try:
                await conn.executemany(
                    f"INSERT INTO {engine.schema}.job_events (job_id, occurred_at, kind, detail) "
                    "VALUES ($1, $2, $3, $4::jsonb)",
                    [
                        (
                            jid,
                            _BASE + timedelta(minutes=1, seconds=1),
                            "state_change",
                            '{"from": "pending", "to": "running"}',
                        ),
                        (
                            jid,
                            _BASE + timedelta(minutes=1, seconds=2),
                            "progress",
                            '{"percent": 100}',
                        ),
                    ],
                )
            finally:
                await conn.execute("SET session_replication_role = DEFAULT")
        finally:
            await conn.close()

    async def _cleanup(engine: _Engine, jid: uuid.UUID) -> None:
        conn = await asyncpg.connect(lab.dsn)
        try:
            await conn.execute(f"DELETE FROM {engine.schema}.job_events WHERE job_id = $1", jid)
            await conn.execute(f"DELETE FROM {engine.schema}.jobs_archive WHERE id = $1", jid)
            await conn.execute(f"DELETE FROM {engine.schema}.jobs WHERE id = $1", jid)
        finally:
            await conn.close()

    jid = new_job_id()
    try:
        # Identical bytes on both engines: one job, two events.
        for engine in (lab.vanilla, lab.ht):
            await _plant(engine, jid)

        live_kinds: dict[str, list[str]] = {}
        for engine in (lab.vanilla, lab.ht):
            live_html = await _html(engine.app, f"/admin/jobs/{jid}")
            live_kinds[engine.schema] = _event_log_kinds(live_html)
            assert live_kinds[engine.schema] == ["state_change", "progress"], (
                f"{engine.schema}: the live arm must render the planted events"
            )

        # The lifecycle's own move on both engines, then the ledger rows
        # re-planted over the cascade (see the helper's docstring: the
        # cascade is today's deletion; retention alignment is the issue's
        # prescription and the state this differential pins).
        for engine in (lab.vanilla, lab.ht):
            await _archive_move(engine, jid)
            await _keep_events_over_the_archive(engine, jid)

        for engine in (lab.vanilla, lab.ht):
            archived_html = await _html(engine.app, f"/admin/jobs/{jid}")
            assert "No events recorded" not in archived_html, (
                f"{engine.schema}: the events table still holds this job's rows; "
                "the archived page must render them"
            )
            archived_kinds = _event_log_kinds(archived_html)
            assert archived_kinds == live_kinds[engine.schema], (
                f"{engine.schema}: the archive arm must render the SAME event "
                f"log the live arm renders (live={live_kinds[engine.schema]!r} "
                f"archived={archived_kinds!r})"
            )
            # The event detail carries through too, not just the kind
            # (autoescape turns the jsonb's quotes into entities; the bare
            # key text is what survives them).
            assert "percent" in archived_html

        # The two engines agree byte-for-byte on the archived page (fixed
        # stamps only: the volatile-stamp stripper handles the rest).
        van_archived = await _html(lab.vanilla.app, f"/admin/jobs/{jid}")
        ht_archived = await _html(lab.ht.app, f"/admin/jobs/{jid}")
        _diff("archived detail events", _stable_html(van_archived), _stable_html(ht_archived))
    finally:
        for engine in (lab.vanilla, lab.ht):
            await _cleanup(engine, jid)


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
      row refuses LOUDLY at migrate time with the pre-flight census
      refusal — the row count and the remediation, BEFORE any DDL
      (never silently dropping or silently converting, and never the
      pre-fix shape: a NotNullViolationError mid-``migrate_data`` that
      stranded a half-converted schema with the archive's primary key
      already dropped).

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
        with pytest.raises(TimescaleDBUnavailableError, match="finished_at"):
            await enable_hypertables(setup, schema=schema, settings=settings)
        # The refusal is loud but clean: the census fires BEFORE any DDL,
        # so the schema stays vanilla and the row stays put (nothing
        # half-applied anywhere — the pre-fix failure left the archive's
        # primary key dropped and job_events already converted).
        tables = await setup.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_name = 'jobs_archive'",
            schema,
        )
        assert len(tables) == 1
        ht = await setup.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            schema,
        )
        assert ht == []
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


# ── 7. The mutation actions, engine-differential ─────────────────────────
#
# Route census (every ``@router.post`` in src/taskq/web/admin): job
# cancel (jobs.py), job retry + schedule enable/disable/skip/run-now +
# rate-limit reset (ops.py), actor deregister (actors.py). The admin
# surface has NO bulk-cancel, NO actor-config-edit, and NO queue
# set-mode/set-max-concurrent routes — there is nothing to differential
# on those names. The rate-limit reset targets ``rate_limit_buckets``
# (never a converted table) and needs a mounted registry — out of this
# module's scope. The converted-schema-relevant mutation kinds are
# proven below: each test drives the SAME action against the SAME
# freshly-planted twin on BOTH engines through the real router + the
# real ``PostgresBackend``, then diffs the resulting job row, the
# audit-trail rows, and the re-rendered page.
#
# ORDER-INDEPENDENCE (pytest-randomly runs this module shuffled): every
# test here plants its OWN twin rows and removes every residue in a
# ``finally`` — jobs, events, attempts, schedules, actor_config, and
# audit rows — because the module's earlier exact-population
# assertions (63 live rows, 115 history rows, the seed-plan walks) hold
# against the planted seed plan alone, whatever order the tests run in.

# The fixed authenticated operator the action apps run under (the
# audit trail's e2e tier's principal): the folded cancel-event detail
# and every rendered audit entry name this subject, so both engines'
# rows are byte-comparable.
_OPERATOR = IdentityClaims(subject="ops-admin@example.com", email=None, groups=frozenset(), raw={})


def _fixed_auth() -> Any:
    async def _dependency() -> IdentityClaims:
        return _OPERATOR

    return _dependency


class _BackendSettings:
    """Satisfies the declared ``BackendSettings`` protocol (the audit
    trail e2e tier's double)."""

    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = MAX_RESULT_BYTES
    event_writer_batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE
    event_writer_statement_timeout_ms: float = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0
    idempotency_lock_timeout_ms: float = 5000.0
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _BackendDeps:
    settings: _BackendSettings
    worker_pool: asyncpg.Pool
    heartbeat_pool: asyncpg.Pool
    dispatcher_pool: asyncpg.Pool | None = None

    def __init__(self, schema: str, pool: asyncpg.Pool) -> None:
        self.settings = _BackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


def _make_backend(pool: asyncpg.Pool, schema: str) -> PostgresBackend:
    return PostgresBackend(
        _BackendDeps(schema, pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double declares every field the routes read (tests/test_admin_audit_trail.py's idiom).
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )


async def _action_post(
    app: FastAPI,
    get_url: str,
    post_url: str,
    data: dict[str, str] | None = None,
) -> httpx.Response:
    """POST a mutation through the real router with the synchronizer-token
    CSRF handshake: GET first to arm the cookie, then post the token
    (tests/test_admin_audit_trail.py's ``_get_csrf_then_post``)."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    ) as client:
        get_resp = await client.get(get_url)
        assert get_resp.status_code == 200, f"{get_url} -> {get_resp.status_code}"
        token = client.cookies.get("taskq_csrf_token", "")
        assert token, "GET must set the taskq_csrf_token cookie"
        return await client.post(post_url, data={"csrf_token": token, **(data or {})})


async def _job_row(engine: _Engine, jid: uuid.UUID, *, table: str = "jobs") -> dict[str, Any]:
    async with engine.pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT * FROM "{engine.schema}".{table} WHERE id = $1', jid)
    return dict(row) if row is not None else {}


def _project(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {k: row.get(k) for k in keys}


def _jsonb(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


# ── Residue management (order independence) ──────────────────────────────


async def _plant_live_jobs(lab: _Lab, specs: list[dict[str, Any]]) -> list[uuid.UUID]:
    """Insert fresh live jobs, IDENTICALLY on both engines, and return
    their ids (the residue key)."""
    ids: list[uuid.UUID] = [new_job_id() for _ in specs]
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.executemany(
                f"""INSERT INTO "{engine.schema}".jobs (id, actor, queue, payload,
                    max_attempts, retry_kind, status, attempt, created_at, scheduled_at,
                    started_at, finished_at, error_class, error_message)
                VALUES ($1, 'mutation_probe', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient',
                    $2, 1, $3, $3, $4, $5, $6, 'boom')""",
                [
                    (
                        jid,
                        spec["status"],
                        spec["created_at"],
                        spec.get("started_at"),
                        spec.get("finished_at"),
                        spec.get("error_class"),
                    )
                    for jid, spec in zip(ids, specs, strict=True)
                ],
            )
    finally:
        await conn.close()
    return ids


async def _cleanup_live_jobs(lab: _Lab, ids: list[uuid.UUID]) -> None:
    """Remove planted live jobs and everything hanging off them (events,
    attempts) from BOTH engines — job_events carries the FK, so children
    first."""
    if not ids:
        return
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f"""DELETE FROM "{engine.schema}".job_events WHERE job_id = ANY($1)""",
                ids,
            )
            await conn.execute(
                f"""DELETE FROM "{engine.schema}".job_attempts WHERE job_id = ANY($1)""",
                ids,
            )
            await conn.execute(
                f"""DELETE FROM "{engine.schema}".jobs WHERE id = ANY($1)""",
                ids,
            )
    finally:
        await conn.close()


async def _cleanup_archive_rows(lab: _Lab, ids: list[uuid.UUID]) -> None:
    """Remove planted archive jobs (and their attempts) from both engines."""
    if not ids:
        return
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f'DELETE FROM "{engine.schema}".job_attempts_archive WHERE job_id = ANY($1)',
                ids,
            )
            await conn.execute(
                f'DELETE FROM "{engine.schema}".jobs_archive WHERE id = ANY($1)', ids
            )
    finally:
        await conn.close()


async def _cleanup_audit(
    lab: _Lab,
    *,
    target_type: str,
    target_ids: list[str],
) -> None:
    """Remove the audit rows the mutations under test wrote — the trail
    is append-only in production, but the module's other differentials
    must not see a later run's residue, whatever the order."""
    if not target_ids:
        return
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f'DELETE FROM "{engine.schema}".admin_audit '
                "WHERE target_type = $1 AND target_id = ANY($2)",
                target_type,
                target_ids,
            )
    finally:
        await conn.close()


_AUDIT_COLS_SQL = (
    "SELECT principal_subject, action, target_type, target_id, reason, detail, occurred_at "
    'FROM "{schema}".admin_audit '
    "WHERE action = $1 AND ($2::text IS NULL OR target_id = $2) ORDER BY id"
)


async def _audit_rows(
    engine: _Engine,
    action: str,
    target_id: str | None = None,
) -> list[dict[str, Any]]:
    """The audit-trail rows for one action, normalized for the
    differential: the ``occurred_at`` db-clock stamp is dropped (the two
    POSTs cannot share a clock reading), everything else must be
    dict-equal."""
    async with engine.pool.acquire() as conn:
        rows = await conn.fetch(_AUDIT_COLS_SQL.format(schema=engine.schema), action, target_id)
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d.pop("occurred_at")
        d["detail"] = _jsonb(d["detail"])
        out.append(d)
    return out


async def _job_events_of_kind(engine: _Engine, jid: uuid.UUID, kind: str) -> list[dict[str, Any]]:
    async with engine.pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT kind, detail FROM "{engine.schema}".job_events '
            "WHERE job_id = $1 AND kind = $2 ORDER BY id",
            jid,
            kind,
        )
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["detail"] = _jsonb(d["detail"])
        out.append(d)
    return out


def _planted_job_specs() -> dict[str, list[dict[str, Any]]]:
    """The fresh mutation targets, one per action kind, built per-test so
    no test ever shares rows with another (pytest-randomly)."""
    base = _BASE + timedelta(days=2)
    return {
        "retryable": [
            {
                "status": "crashed",
                "created_at": base,
                "started_at": base + timedelta(minutes=1),
                "finished_at": base + timedelta(minutes=5),
                "error_class": "WorkerCrashed",
            }
        ],
        "cancellable": [
            {
                "status": "scheduled",
                "created_at": base + timedelta(minutes=10),
                "started_at": None,
                "finished_at": None,
            }
        ],
        "running": [
            {
                "status": "running",
                "created_at": base + timedelta(minutes=20),
                "started_at": base + timedelta(minutes=21),
            }
        ],
        "terminal": [
            {
                "status": "succeeded",
                "created_at": base + timedelta(minutes=30),
                "started_at": base + timedelta(minutes=31),
                "finished_at": base + timedelta(minutes=35),
            }
        ],
    }


async def test_retry_action_lands_identically_on_hypertables(lab: _Lab) -> None:
    """Operator question: when I press Retry on a crashed job, does the
    hypertable-converted schema re-pend the row into EXACTLY the state
    vanilla re-pends it to — and does the audit trail record who pressed
    it, identically? The retry re-pends status='pending', clears every
    error column, and keeps the attempt counter — all compared row-exact."""
    ids = await _plant_live_jobs(lab, _planted_job_specs()["retryable"])
    jid = ids[0]
    try:
        assert (await _job_row(lab.vanilla, jid))["status"] == "crashed"
        assert (await _job_row(lab.ht, jid))["status"] == "crashed"

        resp_v = await _action_post(lab.vanilla_actions, "/admin/jobs", f"/admin/jobs/{jid}/retry")
        resp_h = await _action_post(lab.ht_actions, "/admin/jobs", f"/admin/jobs/{jid}/retry")
        assert resp_v.status_code == resp_h.status_code == 303, (
            f"vanilla={resp_v.status_code} hypertable={resp_h.status_code}"
        )

        van = await _job_row(lab.vanilla, jid)
        ht = await _job_row(lab.ht, jid)
        # The transition columns, exact: re-pended status, the untouched
        # attempt counter, the re-armed budget, and every error column
        # NULL. The db-clock stamps (scheduled_at) are excluded — the
        # re-pend is server-clock stamped by design.
        keys = (
            "status",
            "attempt",
            "max_attempts",
            "finished_at",
            "error_class",
            "error_message",
            "error_traceback",
            "cancel_phase",
            "result",
        )
        _diff("retry row transition", _project(van, keys), _project(ht, keys))
        assert van["status"] == "pending" and van["finished_at"] is None
        assert van["error_class"] is None and van["error_message"] is None

        # The audit trail: one job.retry row per engine, dict-equal
        # modulo the db-clock stamp.
        van_audit = await _audit_rows(lab.vanilla, "job.retry", str(jid))
        ht_audit = await _audit_rows(lab.ht, "job.retry", str(jid))
        _diff("retry audit rows", van_audit, ht_audit)
        assert len(van_audit) == 1
        assert van_audit[0]["principal_subject"] == "ops-admin@example.com"
        assert van_audit[0]["target_type"] == "job"

        # Rendered: the job detail page (now with the audit section) is
        # HTML-identical modulo the db clock on both engines.
        van_html = await _html(lab.vanilla.app, f"/admin/jobs/{jid}")
        ht_html = await _html(lab.ht.app, f"/admin/jobs/{jid}")
        _diff("retry detail page", _stable_html(van_html), _stable_html(ht_html))
        assert "ops-admin@example.com" in ht_html
    finally:
        await _cleanup_live_jobs(lab, ids)
        await _cleanup_audit(lab, target_type="job", target_ids=[str(j) for j in ids])


async def test_cancel_action_lands_identically_on_hypertables(lab: _Lab) -> None:
    """Operator question: does Cancel on a scheduled job terminalize the
    row the same way on hypertables, and does the cancel_request event —
    with the operator's reason AND identity folded into its detail jsonb
    — land byte-identically in the converted ``job_events`` hypertable?"""
    ids = await _plant_live_jobs(lab, _planted_job_specs()["cancellable"])
    jid = ids[0]
    try:
        resp_v = await _action_post(
            lab.vanilla_actions,
            "/admin/jobs",
            f"/admin/jobs/{jid}/cancel",
            data={"reason": "dup run"},
        )
        resp_h = await _action_post(
            lab.ht_actions,
            "/admin/jobs",
            f"/admin/jobs/{jid}/cancel",
            data={"reason": "dup run"},
        )
        assert resp_v.status_code == resp_h.status_code == 303

        van = await _job_row(lab.vanilla, jid)
        ht = await _job_row(lab.ht, jid)
        # The pending/scheduled cancel arm terminalizes DIRECTLY (status
        # 'cancelled' from the cancel_request statement's own arm) — it
        # does NOT stamp cancel_requested_at, which is the RUNNING arm's
        # in-flight marker. The db-clock stamp (finished_at) is compared
        # as not-NULL; everything else is exact.
        keys = ("status", "cancel_phase", "error_class", "error_message")
        _diff("cancel row transition", _project(van, keys), _project(ht, keys))
        assert van["status"] == "cancelled"
        assert van["cancel_requested_at"] is None and ht["cancel_requested_at"] is None
        assert van["finished_at"] is not None and ht["finished_at"] is not None

        # The events: cancel_request (with reason + folded principal) and
        # the state_change — dict-equal across engines, written into the
        # hypertable's widened-PK (id, occurred_at) shape.
        van_events = await _job_events_of_kind(lab.vanilla, jid, "cancel_request")
        ht_events = await _job_events_of_kind(lab.ht, jid, "cancel_request")
        _diff("cancel_request events", van_events, ht_events)
        assert len(van_events) == 1
        assert van_events[0]["detail"]["reason"] == "dup run"
        assert van_events[0]["detail"]["principal_subject"] == "ops-admin@example.com"
        _diff(
            "cancel state_change events",
            await _job_events_of_kind(lab.vanilla, jid, "state_change"),
            await _job_events_of_kind(lab.ht, jid, "state_change"),
        )

        # The audit trail, dict-equal modulo the clock.
        van_audit = await _audit_rows(lab.vanilla, "job.cancel", str(jid))
        ht_audit = await _audit_rows(lab.ht, "job.cancel", str(jid))
        _diff("cancel audit rows", van_audit, ht_audit)
        assert len(van_audit) == 1 and van_audit[0]["reason"] == "dup run"

        # Rendered: the detail page carries the audit entries and the
        # folded event, identically.
        van_html = await _html(lab.vanilla.app, f"/admin/jobs/{jid}")
        ht_html = await _html(lab.ht.app, f"/admin/jobs/{jid}")
        _diff("cancel detail page", _stable_html(van_html), _stable_html(ht_html))
        assert "ops-admin@example.com" in ht_html
    finally:
        await _cleanup_live_jobs(lab, ids)
        await _cleanup_audit(lab, target_type="job", target_ids=[str(j) for j in ids])


async def test_mutation_refusals_render_identically_on_hypertables(lab: _Lab) -> None:
    """Operator question: when the UI refuses me (retry a running job,
    cancel a terminal one, act on a job that does not exist), is the
    refusal the SAME status and the SAME body on hypertables — and does
    a refused action write NO audit row on either engine?"""
    specs = _planted_job_specs()
    ids = await _plant_live_jobs(lab, [*specs["running"], *specs["terminal"]])
    running, done = ids[0], ids[1]
    ghost = new_job_id()
    try:
        # Retry a RUNNING job: the one exclusion (a live attempt must
        # not race its own terminal write) — 409, identical body.
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/jobs", f"/admin/jobs/{running}/retry"
        )
        resp_h = await _action_post(lab.ht_actions, "/admin/jobs", f"/admin/jobs/{running}/retry")
        assert resp_v.status_code == resp_h.status_code == 409
        _diff("retry-running refusal body", resp_v.text, resp_h.text)

        # Cancel an already-terminal job: 409, identical body, and NO
        # audit row for the refused target on either engine.
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/jobs", f"/admin/jobs/{done}/cancel"
        )
        resp_h = await _action_post(lab.ht_actions, "/admin/jobs", f"/admin/jobs/{done}/cancel")
        assert resp_v.status_code == resp_h.status_code == 409
        _diff("cancel-terminal refusal body", resp_v.text, resp_h.text)
        _diff(
            "refused-cancel audit silence",
            await _audit_rows(lab.vanilla, "job.cancel", str(done)),
            await _audit_rows(lab.ht, "job.cancel", str(done)),
        )

        # A missing job: 404 with the identical detail on both engines.
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/jobs", f"/admin/jobs/{ghost}/retry"
        )
        resp_h = await _action_post(lab.ht_actions, "/admin/jobs", f"/admin/jobs/{ghost}/retry")
        assert resp_v.status_code == resp_h.status_code == 404
        _diff("missing-job refusal body", resp_v.text, resp_h.text)
    finally:
        await _cleanup_live_jobs(lab, ids)
        await _cleanup_audit(lab, target_type="job", target_ids=[str(j) for j in [*ids, ghost]])


async def _plant_schedule(lab: _Lab, actor: str) -> uuid.UUID:
    """A fresh schedule row, identical on both engines (the plan's
    schedule stays untouched — other tests' pages read it)."""
    sid = new_uuid()
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f"""INSERT INTO "{engine.schema}".cron_schedules
                    (id, actor, cron_expr, timezone, next_fire_at, enabled)
                VALUES ($1, $2, '*/5 * * * *', 'UTC', $3, true)""",
                sid,
                actor,
                # A realistic next fire, ONE HOUR ahead of the wall clock:
                # the skip handler recomputes forward from THIS stamp in
                # bounded 1000-step cron arithmetic (a sentinel-far one
                # overflows; a past one exhausts the budget -> 400). This
                # stamp is the skip's starting point only — the
                # differential never compares it, it compares the row the
                # skip WROTE against the db clock.
                datetime.now(UTC) + timedelta(hours=1),
            )
    finally:
        await conn.close()
    return sid


async def _cleanup_schedules_and_actor(lab: _Lab, sids: list[uuid.UUID], actor: str) -> None:
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f'DELETE FROM "{engine.schema}".cron_schedules WHERE id = ANY($1)', sids
            )
            await conn.execute(
                f'DELETE FROM "{engine.schema}".actor_config WHERE actor = $1', actor
            )
    finally:
        await conn.close()


async def test_schedule_enable_disable_skip_lands_identically(lab: _Lab) -> None:
    """Operator question: do the schedule toggles flip ``cron_schedules``
    identically on the converted schema, and does the audit trail record
    each toggle (enable/disable/skip) dict-equal?"""
    sid = await _plant_schedule(lab, "runnow_actor")
    sid_text = str(sid)
    try:
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/disable"
        )
        resp_h = await _action_post(
            lab.ht_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/disable"
        )
        assert resp_v.status_code == resp_h.status_code == 303
        for engine in (lab.vanilla, lab.ht):
            async with engine.pool.acquire() as conn:
                enabled = await conn.fetchval(
                    f'SELECT enabled FROM "{engine.schema}".cron_schedules WHERE id = $1', sid
                )
            assert enabled is False
        _diff(
            "disable audit rows",
            await _audit_rows(lab.vanilla, "schedule.disable", sid_text),
            await _audit_rows(lab.ht, "schedule.disable", sid_text),
        )

        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/enable"
        )
        resp_h = await _action_post(
            lab.ht_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/enable"
        )
        assert resp_v.status_code == resp_h.status_code == 303
        for engine in (lab.vanilla, lab.ht):
            async with engine.pool.acquire() as conn:
                enabled = await conn.fetchval(
                    f'SELECT enabled FROM "{engine.schema}".cron_schedules WHERE id = $1', sid
                )
            assert enabled is True
        _diff(
            "enable audit rows",
            await _audit_rows(lab.vanilla, "schedule.enable", sid_text),
            await _audit_rows(lab.ht, "schedule.enable", sid_text),
        )

        # Skip: next_fire_at is computed from the database clock (the DB
        # is the arbiter; the two POSTs cannot be compared row-exact for
        # a now()-derived stamp) — so the differential asserts the row
        # MOVED past the real clock on BOTH engines and that each
        # engine's audit detail names exactly the row value it wrote.
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/skip"
        )
        resp_h = await _action_post(
            lab.ht_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/skip"
        )
        assert resp_v.status_code == resp_h.status_code == 303
        now = datetime.now(UTC)
        for engine in (lab.vanilla, lab.ht):
            async with engine.pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT enabled, next_fire_at FROM "{engine.schema}".cron_schedules '
                    "WHERE id = $1",
                    sid,
                )
            assert row is not None and row["enabled"] is True
            assert row["next_fire_at"] > now, "skip must move next_fire_at past the db clock"
            audit = await _audit_rows(engine, "schedule.skip", sid_text)
            assert len(audit) == 1
            assert audit[0]["detail"]["next_fire_at"] == row["next_fire_at"].isoformat(), (
                "the audit detail must name the exact next_fire_at the skip wrote"
            )
    finally:
        await _cleanup_schedules_and_actor(lab, [sid], "runnow_actor")
        await _cleanup_audit(lab, target_type="schedule", target_ids=[sid_text])


async def test_schedule_run_now_enqueues_identically(lab: _Lab) -> None:
    """Operator question: does Run-now on a schedule fire the SAME job —
    same actor, queue, payload, retry curve, and the ``cron_schedule_id``
    provenance stamp — through the converted schema, with the audit row
    naming the enqueued job id on both engines?

    The two fires use the SAME schedule id on both engines (per-engine
    twins would differ in id), so the process-global run-now cooldown
    (ops.py's ``_last_schedule_run`` map, keyed by schedule id, shared
    by every app in this process) is waited out between the two POSTs —
    the same defer-to-the-clock determinism the policy fixtures
    apply."""
    # Run-now fires the PLAN's own schedule (actor 'bulk_actor'): the
    # (actor, name) unique admits one schedule per actor, so a fresh
    # twin schedule for 'bulk_actor' would collide with the seeded one.
    sid = lab.plan.schedule_id
    sid_text = str(sid)
    enqueued: list[uuid.UUID] = []
    try:
        # Run-now resolves the actor's STORED config: seed the twin
        # actor_config row identically on both engines.
        conn = await asyncpg.connect(lab.dsn)
        try:
            for engine in (lab.vanilla, lab.ht):
                await conn.execute(
                    f"""INSERT INTO "{engine.schema}".actor_config
                        (actor, queue, max_attempts, retry_kind, max_concurrent)
                    VALUES ('bulk_actor', 'bulk', 5, 'transient', 2)"""
                )
        finally:
            await conn.close()

        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/run"
        )
        assert resp_v.status_code == 303, resp_v.text
        van_audit = await _audit_rows(lab.vanilla, "schedule.run", sid_text)
        assert len(van_audit) == 1, "run-now must write exactly one schedule.run audit row"
        van_job_id = uuid.UUID(van_audit[0]["detail"]["enqueued_job_id"])
        van_row = await _job_row(lab.vanilla, van_job_id)
        assert van_row, "the run-now fire must have enqueued a real job row"

        # Wait out the process-global cooldown, then fire the same
        # schedule on the hypertable engine.
        await asyncio.sleep(_SCHEDULE_RUN_COOLDOWN_SECONDS + 0.5)
        resp_h = await _action_post(
            lab.ht_actions, "/admin/schedules", f"/admin/schedules/{sid_text}/run"
        )
        assert resp_h.status_code == 303, resp_h.text
        ht_audit = await _audit_rows(lab.ht, "schedule.run", sid_text)
        assert len(ht_audit) == 1
        ht_job_id = uuid.UUID(ht_audit[0]["detail"]["enqueued_job_id"])
        ht_row = await _job_row(lab.ht, ht_job_id)
        assert ht_row
        enqueued = [van_job_id, ht_job_id]

        # The resulting rows: everything the OPERATOR set is dict-equal
        # (ids and the db-clock stamps differ by construction — the
        # handler generates the job id and the server stamps the
        # clocks). The provenance metadata carries the same schedule id
        # on both engines.
        keys = (
            "actor",
            "queue",
            "status",
            "attempt",
            "max_attempts",
            "retry_kind",
            "priority",
            "retry_base_seconds",
            "retry_cap_seconds",
            "retry_backoff",
            "retry_jitter",
        )

        def _proj(row: dict[str, Any]) -> dict[str, Any]:
            metadata = _jsonb(row["metadata"])
            return _project(row, keys) | {
                "payload": _jsonb(row["payload"]),
                "metadata": {k: v for k, v in metadata.items() if k != "cron_schedule_id"},
                "cron_schedule_id": metadata["cron_schedule_id"],
            }

        _diff("run-now enqueued job shape", _proj(van_row), _proj(ht_row))
        assert _proj(van_row)["cron_schedule_id"] == sid_text
        assert _proj(van_row)["status"] == "pending"
        assert _proj(van_row)["actor"] == "bulk_actor" and _proj(van_row)["queue"] == "bulk"

        # The audit rows: identical modulo the per-engine enqueued job
        # id, each of which is exactly the row that engine's jobs table
        # holds.
        def _strip_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {**a, "detail": {k: v for k, v in a["detail"].items() if k != "enqueued_job_id"}}
                for a in rows
            ]

        _diff("run-now audit rows (modulo enqueued id)", _strip_id(van_audit), _strip_id(ht_audit))

        # Rendered: each engine's live tab finds the new pending row by
        # id search (the same text-search shape the filters differential
        # uses).
        for engine, jid in ((lab.vanilla, van_job_id), (lab.ht, ht_job_id)):
            html = await _html(engine.app, f"/admin/jobs?tab=live&search={jid}")
            assert str(jid) in html, "the run-now job must render on the live tab"
    finally:
        await _cleanup_live_jobs(lab, enqueued)
        # NOT the plan's schedule — only the actor_config twin this test
        # planted and the audit rows this fire wrote.
        await _cleanup_schedules_and_actor(lab, [], "bulk_actor")
        await _cleanup_audit(lab, target_type="schedule", target_ids=[sid_text])


async def test_actor_deregister_lands_identically(lab: _Lab) -> None:
    """Operator question: does deregistering an actor remove the config
    row and record the DESTRUCTIVE SCOPE (force/purge flags, queue, row
    counts) identically in the audit trail on the converted schema?"""
    conn = await asyncpg.connect(lab.dsn)
    try:
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f"""INSERT INTO "{engine.schema}".actor_config
                    (actor, queue, max_attempts, retry_kind, max_concurrent)
                VALUES ('dereg_actor', 'bulk', 2, 'transient', 1)"""
            )
    finally:
        await conn.close()

    try:
        resp_v = await _action_post(
            lab.vanilla_actions, "/admin/actors", "/admin/actors/dereg_actor/deregister"
        )
        resp_h = await _action_post(
            lab.ht_actions, "/admin/actors", "/admin/actors/dereg_actor/deregister"
        )
        assert resp_v.status_code == resp_h.status_code == 303

        for engine in (lab.vanilla, lab.ht):
            async with engine.pool.acquire() as conn:
                n = await conn.fetchval(
                    f'SELECT count(*) FROM "{engine.schema}".actor_config WHERE actor = $1',
                    "dereg_actor",
                )
            assert n == 0, "the config row must be gone on both engines"

        van_audit = await _audit_rows(lab.vanilla, "actor.deregister", "dereg_actor")
        ht_audit = await _audit_rows(lab.ht, "actor.deregister", "dereg_actor")
        _diff("deregister audit rows", van_audit, ht_audit)
        assert len(van_audit) == 1
        # The scope record: nothing to purge or cancel on this actor,
        # and the trail says so exactly, on both engines.
        assert van_audit[0]["detail"] == {
            "force": False,
            "purge_queue": False,
            "queue": "bulk",
            "queue_purged": False,
            "jobs_cancelled": 0,
            "schedules_disabled": 0,
            "terminal_jobs_remaining": 0,
        }

        # Rendered: the actors page (notice banner, config row gone) is
        # identical modulo the db clock.
        van_html = await _html(lab.vanilla.app, "/admin/actors?notice=deregistered")
        ht_html = await _html(lab.ht.app, "/admin/actors?notice=deregistered")
        _diff("deregister actors page", _stable_html(van_html), _stable_html(ht_html))
        assert "dereg_actor" not in ht_html
    finally:
        await _cleanup_audit(lab, target_type="actor", target_ids=["dereg_actor"])
        await _cleanup_schedules_and_actor(lab, [], "dereg_actor")


async def test_disabled_admin_actions_refuse_identically(
    lab: _Lab,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator question: with ``TASKQ_ADMIN_ACTIONS_ENABLED`` false, is
    the 403 refusal — the exact body, and the no-audit-row silence — the
    same on a hypertable-mounted router as on a vanilla one?"""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")

    def _mount_disabled(pool: asyncpg.Pool, schema: str) -> FastAPI:
        bundle = create_router(pool, schema=schema, redis_client=None, base_path="/admin")
        app = FastAPI()
        setup_admin_state(app, bundle)
        app.include_router(bundle.router, prefix="/admin")
        return app

    van_app = _mount_disabled(lab.vanilla.pool, lab.vanilla.schema)
    ht_app = _mount_disabled(lab.ht.pool, lab.ht.schema)

    ids = await _plant_live_jobs(lab, _planted_job_specs()["cancellable"])
    jid = ids[0]
    try:
        resp_v = await _action_post(van_app, "/admin/jobs", f"/admin/jobs/{jid}/cancel")
        resp_h = await _action_post(ht_app, "/admin/jobs", f"/admin/jobs/{jid}/cancel")
        assert resp_v.status_code == resp_h.status_code == 403
        _diff("disabled-actions 403 body", resp_v.text, resp_h.text)
        _diff(
            "disabled-actions audit silence",
            await _audit_rows(lab.vanilla, "job.cancel", str(jid)),
            await _audit_rows(lab.ht, "job.cancel", str(jid)),
        )
    finally:
        await _cleanup_live_jobs(lab, ids)


# ── 8. The job detail view under the parentless-attempt window ───────────


async def test_job_detail_renders_attempts_spanning_the_parentless_window(
    lab: _Lab,
    ts_dsn: str,
) -> None:
    """Operator question: an archived job's attempts now chunk on
    ``started_at`` while the job itself chunks on ``finished_at`` — when
    one attempt sits in an AGED chunk (31 days back) and the parentless
    window lets attempt rows outlive parents entirely, does the detail
    page still render the FULL attempt history, honestly counted,
    without breaking?"""
    job = lab.plan.archive[5]  # inside the seeded attempts population
    orphan = new_job_id()  # deliberately NEVER seeded as a job row
    conn = await asyncpg.connect(ts_dsn)
    try:
        # Attempt 2 for the job, started 31 days before the seed base —
        # a different ``started_at`` chunk of the hypertable than the
        # attempt-1 row. Identical on both engines (the differential);
        # only the hypertable's storage shape differs underneath.
        for engine in (lab.vanilla, lab.ht):
            await conn.execute(
                f"""INSERT INTO "{engine.schema}".job_attempts_archive
                    (job_id, attempt, started_at, finished_at, outcome, error_class, error_message)
                VALUES ($1, 2, $2, $3, 'failed', 'FarChunkProbe', 'aged chunk probe')""",
                job.id,
                _FAR_PAST,
                _FAR_PAST + timedelta(minutes=4),
            )
        # The parentless-attempt direction, pinned both ways: vanilla
        # REFUSES (the FK stands there), the hypertable accepts (the FK
        # is dropped) — and the orphan row must not disturb ANY job page.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                f"""INSERT INTO "{lab.vanilla.schema}".job_attempts_archive
                    (job_id, attempt, started_at, finished_at, outcome)
                VALUES ($1, 1, $2, $3, 'failed')""",
                new_job_id(),
                _BASE,
                _BASE + timedelta(minutes=4),
            )
        await conn.execute(
            f"""INSERT INTO "{lab.ht.schema}".job_attempts_archive
                (job_id, attempt, started_at, finished_at, outcome)
            VALUES ($1, 1, $2, $3, 'failed')""",
            orphan,
            _BASE,
            _BASE + timedelta(minutes=4),
        )
    finally:
        await conn.close()

    try:
        van_html = await _html(lab.vanilla.app, f"/admin/jobs/{job.id}")
        ht_html = await _html(lab.ht.app, f"/admin/jobs/{job.id}")
        _diff("spanning-attempts detail page", _stable_html(van_html), _stable_html(ht_html))
        assert "Attempt History" in ht_html
        # Honest count: the aged attempt renders, exactly once — the
        # page served BOTH attempts (the aged-chunk one and the
        # fresh-chunk one).
        assert ht_html.count("FarChunkProbe") == 1

        # The parentless attempt's own "job page" is a clean 404 on BOTH
        # engines (the parent row does not exist there either),
        # identical.
        resp_v = await _get(lab.vanilla.app, f"/admin/jobs/{orphan}")
        resp_h = await _get(lab.ht.app, f"/admin/jobs/{orphan}")
        assert resp_v.status_code == resp_h.status_code == 404
        _diff("orphan-attempt job page", resp_v.text, resp_h.text)
    finally:
        # No residue: the module's other differentials assume the seeded
        # attempt population, whatever order the tests run in.
        conn = await asyncpg.connect(ts_dsn)
        try:
            for engine in (lab.vanilla, lab.ht):
                await conn.execute(
                    f"""DELETE FROM "{engine.schema}".job_attempts_archive
                    WHERE (job_id = $1 AND attempt = 2) OR job_id = $2""",
                    job.id,
                    orphan,
                )
        finally:
            await conn.close()


# ── 9. Pagination across chunk boundaries ─────────────────────────────────

_N_PER_STAMP = 9
_BOUNDARY_FIRST_MIDNIGHT = datetime(2026, 9, 21, 0, 0, 0, tzinfo=UTC)
_N_BOUNDARIES = 10


def _boundary_rows() -> list[_JobRow]:
    """The chunk-boundary population plan (ids generated fresh per test,
    timestamps fixed): each pair of stamps brackets a UTC midnight —
    1 h either side — so on a 1-day chunk grid every midnight inside
    the span is a REAL chunk boundary at a KNOWN timestamp, and the rows
    page across seams both with the sort column (``finished_at`` IS the
    partition column) and with the created_at time-window filter."""
    rows: list[_JobRow] = []
    for k in range(_N_BOUNDARIES):
        midnight = _BOUNDARY_FIRST_MIDNIGHT + timedelta(days=k)
        for delta in (timedelta(hours=-1), timedelta(hours=1)):
            stamp = midnight + delta
            for _ in range(_N_PER_STAMP):
                rows.append(
                    _JobRow(
                        id=new_job_id(),
                        actor="boundary_actor",
                        queue="bulk",
                        status="succeeded",
                        created_at=stamp - timedelta(minutes=30),
                        finished_at=stamp,
                        tags=["boundary"],
                    )
                )
    rows.sort(key=lambda r: (r.finished_at, r.id))  # insertion order = walk order
    return rows


async def _plant_boundary_population(
    lab: _Lab,
    rows: list[_JobRow],
    conn: asyncpg.Connection,
) -> None:
    """Sharpen the hypertable's grid to 1-day chunks (future chunks
    only; existing ones keep their interval and every policy is
    deferred 10 years by the module fixture), then plant the boundary
    rows identically on both engines."""

    def _archive_tuple(r: _JobRow) -> tuple[Any, ...]:
        finished = r.finished_at
        assert finished is not None  # Why: every boundary row is terminal-dated by construction.
        return (
            r.id,
            r.tags,
            r.created_at,
            r.created_at + timedelta(minutes=1),
            finished,
            finished + timedelta(minutes=1),
            finished + timedelta(days=365),
        )

    await conn.execute(
        f"""SELECT set_chunk_time_interval(
                '"{lab.ht.schema}".jobs_archive'::regclass, INTERVAL '1 day')"""
    )
    for engine in (lab.vanilla, lab.ht):
        await conn.executemany(
            f"""INSERT INTO "{engine.schema}".jobs_archive
                (id, actor, queue, payload, max_attempts, retry_kind, status, attempt,
                 tags, created_at, scheduled_at, started_at, finished_at, archived_at, expire_at)
            VALUES ($1, 'boundary_actor', 'bulk', '{{"v":1}}'::jsonb, 3, 'transient',
                'succeeded', 1, $2::text[], $3, $3, $4, $5, $6, $7)""",
            [_archive_tuple(r) for r in rows],
        )
    # The premise, pinned against the catalog: the planted span covers
    # at least two chunks of the hypertable — a real seam inside the
    # walk, not a hypothetical one.
    stamps = [r.finished_at for r in rows if r.finished_at is not None]
    n_chunks = await conn.fetchval(
        f"""SELECT count(*) FROM show_chunks('"{lab.ht.schema}".jobs_archive'::regclass,
            older_than => $1::timestamptz, newer_than => $2::timestamptz)""",
        max(stamps) + timedelta(minutes=5),
        min(stamps) - timedelta(minutes=5),
    )
    assert n_chunks >= 2, f"the planted span must cover >= 2 chunks, got {n_chunks}"


async def test_archive_keyset_walk_is_exact_across_chunk_boundaries(lab: _Lab) -> None:
    """Operator question: with ``jobs_archive`` chunked on the very
    column the archive tab sorts and keys on, does the keyset walk cross
    the chunk seams with NO skipped rows and NO duplicates — the
    walk-oracle, over a population whose seams are real chunks?"""
    planted = _boundary_rows()
    conn = await asyncpg.connect(lab.dsn)
    try:
        await _plant_boundary_population(lab, planted, conn)
    finally:
        await conn.close()

    try:
        population = [*lab.plan.archive, *planted]
        expected = _expected_archived_order(population)
        assert len(expected) == len(population) == 235  # Why: 55 seed + 180 boundary rows.

        van = await _walk(lab.vanilla.app, "/admin/jobs?tab=archived")
        ht = await _walk(lab.ht.app, "/admin/jobs?tab=archived")
        _diff("chunk-boundary archive walk", van, ht)
        assert ht == expected, (
            "the archive walk across chunk boundaries must reproduce the "
            "(finished_at DESC, id DESC) order exactly — every row once, no gaps"
        )
    finally:
        await _cleanup_archive_rows(lab, [r.id for r in planted])


async def test_archive_walk_carries_the_time_window_through_page_turns(lab: _Lab) -> None:
    """Operator question: the render's own Next link drops the absolute
    ``time_from``/``time_to`` window on page turn (the pagination
    macro's known gap) — when the window IS carried through the cursor
    URLs the server honors, does the keyset seam survive the filter
    across chunk boundaries, with chunk exclusion eating NO in-window
    row? Walked to exhaustion on both engines against the Python
    oracle, plus the count endpoint with the same windows."""
    planted = _boundary_rows()
    conn = await asyncpg.connect(lab.dsn)
    try:
        await _plant_boundary_population(lab, planted, conn)
    finally:
        await conn.close()

    try:
        population = [*lab.plan.archive, *planted]
        by_id = {str(r.id): r for r in population}

        def _window_rows(time_from: datetime, time_to: datetime) -> list[_JobRow]:
            return [r for r in population if time_from <= r.created_at <= time_to]

        async def _windowed_walk(
            engine: _Engine, time_from: datetime, time_to: datetime
        ) -> list[str]:
            """Walk the archive tab carrying the window through EVERY
            page turn: each turn's cursor is built from the row the
            ordering just served (the exact values the rendered Next
            link would carry, had the macro not dropped the window)."""
            q_from = quote_plus(time_from.isoformat())
            q_to = quote_plus(time_to.isoformat())
            expected = _expected_archived_order(_window_rows(time_from, time_to))
            got: list[str] = []
            cursor: tuple[str, str] | None = None
            pages = 0
            while True:
                url = f"/admin/jobs?tab=archived&time_from={q_from}&time_to={q_to}"
                if cursor is not None:
                    url += (
                        f"&cursor_at={quote_plus(cursor[0])}&cursor_id={cursor[1]}&cursor_dir=next"
                    )
                ids = _ordered_job_ids(await _html(engine.app, url))
                want = expected[len(got) : len(got) + 50]
                assert ids == want, (
                    f"page {pages + 1} of the windowed walk broke the seam: "
                    f"served {ids!r}, want {want!r}"
                )
                got.extend(ids)
                if len(ids) < 50:
                    break
                last = by_id[ids[-1]]
                assert last.finished_at is not None
                cursor = (last.finished_at.isoformat(), ids[-1])
                pages += 1
                assert pages < 20, "the windowed walk did not terminate"
            return got

        windows = [
            # Spans four UTC-midnight chunk seams; the in-window
            # population (90 rows) runs past one full page -> a real
            # page turn with the seam inside the chunk neighbourhood.
            (datetime(2026, 9, 22, tzinfo=UTC), datetime(2026, 9, 25, tzinfo=UTC)),
            # Ends INSIDE a chunk (a window boundary that prunes nothing
            # the keyset still has to serve); 15 rows, one page.
            (
                datetime(2026, 9, 23, 12, 0, 0, tzinfo=UTC),
                datetime(2026, 9, 23, 23, 59, tzinfo=UTC),
            ),
        ]
        for time_from, time_to in windows:
            van = await _windowed_walk(lab.vanilla, time_from, time_to)
            ht = await _windowed_walk(lab.ht, time_from, time_to)
            _diff(f"windowed walk {time_from}..{time_to}", van, ht)
            assert len(van) == len(_window_rows(time_from, time_to)), (
                "the windowed walk must serve every in-window row exactly once"
            )
            if time_from == windows[0][0]:
                assert len(van) > 50, "the first window must force at least one page turn"

            # The count endpoint with the same window: chunk exclusion
            # must not eat in-window rows — the count IS the walk's
            # length.
            count_path = (
                f"/admin/jobs/count?tab=archived&time_from={quote_plus(time_from.isoformat())}"
                f"&time_to={quote_plus(time_to.isoformat())}"
            )
            _diff(
                f"windowed count {time_from}..{time_to}",
                await _json(lab.vanilla.app, count_path),
                await _json(lab.ht.app, count_path),
            )
            assert await _json(lab.ht.app, count_path) == {"count": len(van)}
    finally:
        await _cleanup_archive_rows(lab, [r.id for r in planted])


# ── 10. The failure renderings ────────────────────────────────────────────


async def test_invalid_cursors_and_filters_render_identically(lab: _Lab) -> None:
    """Operator question: when the URL is broken — a hand-edited cursor
    (the shape that 500'd every page turn pre-fix), a partial or invalid
    history cursor, a garbage or NUL-carrier filter — is every error
    status and body IDENTICAL on the converted schema, and does the
    malformed-cursor fallback still serve the honest first page?"""
    # A malformed jobs-list cursor falls back to the first page (200):
    # the page must be identical to the unpaged first page, on both
    # engines.
    plain = _stable_html(await _html(lab.ht.app, "/admin/jobs?tab=archived"))
    for bad in ("not-a-uuid", ""):
        fallback = _stable_html(
            await _html(lab.ht.app, f"/admin/jobs?tab=archived&cursor_id={quote_plus(bad)}")
        )
        assert fallback == plain, f"cursor_id={bad!r} must fall back to the first page"
        van_fallback = _stable_html(
            await _html(lab.vanilla.app, f"/admin/jobs?tab=archived&cursor_id={quote_plus(bad)}")
        )
        _diff(f"malformed cursor fallback {bad!r}", van_fallback, fallback)

    # The history page's cursor family: partial -> 400, bad timestamp
    # -> 400, bad uuid -> 400. Bodies identical cross-engine.
    history_bads = [
        "/admin/history?cursor_at=2026-09-20T00%3A00%3A00%2B00%3A00",  # partial
        "/admin/history?cursor_at=not-a-timestamp&cursor_created=2026-09-20T00%3A00%3A00%2B00%3A00&cursor_id=00000000-0000-0000-0000-000000000000",
        "/admin/history?cursor_at=2026-09-20T00%3A00%3A00%2B00%3A00&cursor_created=2026-09-20T00%3A00%3A00%2B00%3A00&cursor_id=nope",
    ]
    for path in history_bads:
        resp_v = await _get(lab.vanilla.app, path)
        resp_h = await _get(lab.ht.app, path)
        assert resp_v.status_code == resp_h.status_code == 400, path
        _diff(f"history 400 body {path}", resp_v.text, resp_h.text)

    # The jobs list filter family: garbage absolute time (the clean-400
    # fix — pre-fix an opaque driver 500), a NUL-carrier text filter,
    # an invalid status. Bodies identical cross-engine.
    filter_bads: list[dict[str, str]] = [
        {"time_from": "garbage"},
        {"time_to": "2026-13-45 99:99"},
        {"actor": "a\x00b"},
        {"search": "x\x00y"},
    ]
    for params in filter_bads:
        resp_v = await _get(lab.vanilla.app, "/admin/jobs", params=params)
        resp_h = await _get(lab.ht.app, "/admin/jobs", params=params)
        assert resp_v.status_code == resp_h.status_code == 400, params
        _diff(f"filter 400 body {params}", resp_v.text, resp_h.text)

    resp_v = await _get(lab.vanilla.app, "/admin/jobs", params={"status": "banana"})
    resp_h = await _get(lab.ht.app, "/admin/jobs", params={"status": "banana"})
    assert resp_v.status_code == resp_h.status_code == 400
    _diff("invalid status 400 body", resp_v.text, resp_h.text)


async def test_missing_job_detail_renders_identically(lab: _Lab) -> None:
    """Operator question: an id that exists on neither engine (pruned,
    mistyped) must answer the same 404 on hypertables — the detail
    route's archive fallthrough terminates in the identical error."""
    ghost = new_job_id()
    resp_v = await _get(lab.vanilla.app, f"/admin/jobs/{ghost}")
    resp_h = await _get(lab.ht.app, f"/admin/jobs/{ghost}")
    assert resp_v.status_code == resp_h.status_code == 404
    _diff("missing job 404 body", resp_v.text, resp_h.text)

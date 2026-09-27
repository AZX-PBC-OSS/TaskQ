# ruff: noqa: S608  # Why: schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""The operational-insights SQL layer against REAL containers, both modes.

Every metric's statement runs against a seeded scenario on a plain
Postgres 18 container AND a TimescaleDB 2.30.1 (PG18) container with the
retention tables converted to hypertables — the same test bodies
parameterized over both, so a statement that drifts on either surface
fails twice with the same name.  The scenarios are the operator shapes
the docs guide reads: a balanced fleet, a starved queue, an
overprovisioned queue, a runaway schedule, a deferred job's excluded
wait, and an archived job's UNION inclusion.

The EXPLAIN legs pin the hot paths' index eligibility on both modes:
no Seq Scan touches ``jobs``, ``job_attempts``, or the plain-mode
archive tables for any windowed statement (the hypertable archive is
excluded there — its chunk-level access is a Seq-Scan-per-chunk shape
by design, bounded by chunk pruning, and the live-side pins carry the
eligibility evidence).
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

# The module under test, for the EXPLAIN pin's template access (the
# named imports above cover the public surface; the pin renders the
# production SQL text, so it needs the module object).
import taskq.insights as insights_mod  # pyright: ignore[reportPrivateUsage]
from taskq._ids import new_job_id, new_uuid
from taskq.insights import (
    _build_cron_ledger_sql,
    _build_wait_sql,
    fetch_actor_backlog,
    fetch_cron_ledger,
    fetch_drain_estimates,
    fetch_overprovisioning,
    fetch_queue_imbalance,
    fetch_wait_distribution,
    fetch_worker_busy_ratio,
)
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.timescale import enable_hypertables

pytestmark = pytest.mark.integration

_PG_IMAGE = "postgres:18"
_TIMESCALE_IMAGE_DEFAULT = "timescale/timescaledb:2.30.1-pg18"
_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT

_NOW = datetime.now(UTC)


# ── Container + migrated-schema fixtures, one per mode ──────────────────


@pytest.fixture(scope="module")
def plain_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_PG_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def plain_dsn(plain_container: Any) -> str:
    return plain_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
def timescale_dsn(timescale_container: Any) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


async def _migrate(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)


@pytest.fixture(scope="module")
async def plain_env(plain_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """Plain-PG mode: a migrated schema with the EXPLAIN bulk corpus seeded."""
    conn = await asyncpg.connect(plain_dsn)
    schema = "insights_plain"
    try:
        await _migrate(conn, schema)
        await _seed_bulk_corpus(conn, schema)
        yield conn, schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


@pytest.fixture(scope="module")
async def timescale_env(timescale_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """Hypertable mode: same schema, converted, policies deferred to the far
    future (a ~now retention run would drop the bulk corpus's aged chunks
    mid-module; the retention-floor semantics are the existing sweeps
    suites' subject, not this file's)."""
    conn = await asyncpg.connect(timescale_dsn)
    schema = "insights_ts"
    try:
        await _migrate(conn, schema)
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": timescale_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
                # Short retentions so the CONVERSION clamps its chunk
                # interval to the 1-day floor (matching the existing
                # timescaledb suites); the POLICIES are deferred below.
                "TASKQ_ARCHIVE_RETENTION_PERIOD": "172800s",
                "TASKQ_EVENT_RETENTION_PERIOD": "86400s",
            }
        )
        report = await enable_hypertables(conn, schema=schema, settings=settings)
        assert set(report.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
        rows = await conn.fetch(
            "SELECT job_id FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name LIKE 'policy%'",
            schema,
        )
        for r in rows:
            await conn.execute(
                "SELECT alter_job($1, next_start => $2::timestamptz)",
                r["job_id"],
                datetime.now(UTC) + timedelta(days=3650),
            )
        await _seed_bulk_corpus(conn, schema)
        yield conn, schema
    finally:
        await conn.close()


@pytest.fixture(params=["plain", "timescale"], ids=["plain-pg", "hypertable"])
async def matrix(
    request: pytest.FixtureRequest,
    plain_env: tuple[asyncpg.Connection, str],
    timescale_env: tuple[asyncpg.Connection, str],
) -> tuple[asyncpg.Connection, str]:
    """The same test body against both containers: the both-modes matrix."""
    return plain_env if request.param == "plain" else timescale_env


# ── Seeding helpers ─────────────────────────────────────────────────────


async def _seed_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    actor: str,
    wait_s: float,
    finished_age_s: float,
    snooze_count: int = 0,
    rate_limit_blocked_count: int = 0,
    status: str = "succeeded",
    archived: bool = False,
    worker_id: uuid.UUID | None = None,
    duration_ms: int | None = None,
    metadata: dict[str, Any] | None = None,
    schedule_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """One terminal job whose wait (started_at - scheduled_at) is
    exactly *wait_s* and which finished *finished_age_s* ago."""
    jid = new_job_id()
    now = datetime.now(UTC)
    started = now - timedelta(seconds=finished_age_s + 1.0)
    scheduled = started - timedelta(seconds=wait_s)
    created = scheduled
    table = "jobs_archive" if archived else "jobs"
    meta = dict(metadata or {})
    if schedule_id is not None:
        meta["cron_schedule_id"] = str(schedule_id)
    expire_clause = ", expire_at" if archived else ""
    expire_value = ", statement_timestamp() + interval '365 days'" if archived else ""
    await conn.execute(
        f"""INSERT INTO {schema}.{table} (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, created_at, scheduled_at, started_at, finished_at,
                snooze_count, rate_limit_blocked_count, metadata{expire_clause}
            ) VALUES (
                $1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                $4::{schema}.job_status, $5, $6, $7, $8, $9, $10, $11::jsonb{expire_value}
            )""",
        jid,
        actor,
        queue,
        status,
        created,
        scheduled,
        started,
        now - timedelta(seconds=finished_age_s),
        snooze_count,
        rate_limit_blocked_count,
        json.dumps(meta),
    )
    if duration_ms is not None:
        attempt_table = "job_attempts_archive" if archived else "job_attempts"
        await conn.execute(
            f"""INSERT INTO {schema}.{attempt_table}
                    (job_id, attempt, started_at, finished_at, outcome, duration_ms, worker_id)
                VALUES ($1, 1, $2, $3, 'succeeded', $4, $5)""",
            jid,
            started,
            now - timedelta(seconds=finished_age_s),
            duration_ms,
            worker_id,
        )
    return jid


async def _seed_active_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queue: str,
    actor: str,
    status: str,
    scheduled_age_s: float | None,
) -> uuid.UUID:
    """One pending (when *scheduled_age_s* is set, due in the past) or
    scheduled (None / future) row for the depth and imbalance reads."""
    jid = new_job_id()
    now = datetime.now(UTC)
    scheduled_at = (
        now - timedelta(seconds=scheduled_age_s)
        if scheduled_age_s is not None
        else now + timedelta(hours=1)
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
            ) VALUES (
                $1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient', $4::{schema}.job_status, $5
            )""",
        jid,
        actor,
        queue,
        status,
        scheduled_at,
    )
    return jid


async def _seed_worker(
    conn: asyncpg.Connection,
    schema: str,
    *,
    queues: list[str],
    started_age_s: float = 3600.0,
    seen_age_s: float = 5.0,
) -> uuid.UUID:
    wid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.workers (id, hostname, pid, queues, started_at, last_seen_at)
            VALUES ($1, 'insights-host', 4242, $2, $3, $4)""",
        wid,
        queues,
        now - timedelta(seconds=started_age_s),
        now - timedelta(seconds=seen_age_s),
    )
    return wid


async def _seed_attempt(
    conn: asyncpg.Connection,
    schema: str,
    *,
    job_id: uuid.UUID,
    worker_id: uuid.UUID | None,
    started_age_s: float,
    duration_ms: int,
    archived: bool = False,
) -> None:
    now = datetime.now(UTC)
    table = "job_attempts_archive" if archived else "job_attempts"
    await conn.execute(
        f"""INSERT INTO {schema}.{table}
                (job_id, attempt, started_at, finished_at, outcome, duration_ms, worker_id)
            VALUES ($1, 1, $2, $3, 'succeeded', $4, $5)""",
        job_id,
        now - timedelta(seconds=started_age_s),
        now - timedelta(seconds=started_age_s) + timedelta(milliseconds=duration_ms),
        duration_ms,
        worker_id,
    )


async def _seed_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
) -> uuid.UUID:
    sid = new_uuid()
    await conn.execute(
        f"""INSERT INTO {schema}.cron_schedules
                (id, actor, cron_expr, timezone, dst_strategy, next_fire_at)
            VALUES ($1, $2, '*/5 * * * *', 'UTC', 'skip', statement_timestamp())""",
        sid,
        actor,
    )
    return sid


async def _seed_config(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    queue: str,
    max_concurrent: int | None,
) -> None:
    await conn.execute(
        f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
            VALUES ($1, $2, NULL, $3)
            ON CONFLICT (actor) DO UPDATE SET queue = EXCLUDED.queue,
                                              max_concurrent = EXCLUDED.max_concurrent""",
        actor,
        max_concurrent,
        queue,
    )


async def _seed_bulk_corpus(conn: asyncpg.Connection, schema: str) -> None:
    """The EXPLAIN corpus: ~3k terminal jobs and ~3k attempts spread over
    30 days in BOTH tiers plus live pending/scheduled populations, so the
    planner's statistics are representative and the windowed statements
    face the real selectivity shape (a 24h window over a month of
    history).  Without it every index pin would be vacuous: the planner
    seq-scans an empty table and the pin would pass for the wrong
    reason."""
    # Terminal live rows over 30 days, one attempt each.
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                created_at, scheduled_at, started_at, finished_at, metadata
            )
            SELECT gen_random_uuid(), 'bulk', 'bulk_q', '{{}}'::jsonb, 3, 'transient',
                   'succeeded'::{schema}.job_status,
                   ts, ts, ts + interval '5 seconds', ts + interval '6 seconds',
                   '{{}}'::jsonb
            FROM generate_series(
                statement_timestamp() - interval '30 days',
                statement_timestamp() - interval '2 hours',
                interval '15 minutes'
            ) AS ts"""
    )
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts (job_id, attempt, started_at, finished_at, outcome, duration_ms)
            SELECT id, 1, started_at, finished_at, 'succeeded', 400
            FROM {schema}.jobs WHERE actor = 'bulk'"""
    )
    # The same shape in the archive tier.
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                created_at, scheduled_at, started_at, finished_at, metadata, expire_at
            )
            SELECT gen_random_uuid(), 'bulk', 'bulk_q', '{{}}'::jsonb, 3, 'transient',
                   'succeeded'::{schema}.job_status,
                   ts, ts, ts + interval '5 seconds', ts + interval '6 seconds',
                   '{{}}'::jsonb, statement_timestamp() + interval '300 days'
            FROM generate_series(
                statement_timestamp() - interval '30 days',
                statement_timestamp() - interval '2 hours',
                interval '15 minutes'
            ) AS ts"""
    )
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts_archive (job_id, attempt, started_at, finished_at, outcome, duration_ms)
            SELECT id, 1, started_at, finished_at, 'succeeded', 400
            FROM {schema}.jobs_archive WHERE actor = 'bulk'"""
    )
    # Live dispatch populations for the depth arms.
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
            )
            SELECT gen_random_uuid(), 'bulk', 'bulk_q', '{{}}'::jsonb, 3, 'transient',
                   'pending'::{schema}.job_status, statement_timestamp() - interval '10 seconds'
            FROM generate_series(1, 300)"""
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
            )
            SELECT gen_random_uuid(), 'bulk', 'bulk_q', '{{}}'::jsonb, 3, 'transient',
                   'scheduled'::{schema}.job_status, statement_timestamp() + interval '1 hour'
            FROM generate_series(1, 300)"""
    )
    await conn.execute(
        f"ANALYZE {schema}.jobs, {schema}.jobs_archive, "
        f"{schema}.job_attempts, {schema}.job_attempts_archive, "
        f"{schema}.workers, {schema}.actor_config, {schema}.cron_schedules"
    )


# ── EXPLAIN machinery ───────────────────────────────────────────────────


async def _explain(conn: asyncpg.Connection, sql: str, *args: object) -> dict[str, Any]:
    """The ROOT plan node of ``EXPLAIN (FORMAT JSON)`` for *sql* (the
    statement yields a one-element JSON array whose object carries the
    tree under ``Plan``)."""
    rows = await conn.fetch("EXPLAIN (FORMAT JSON) " + sql, *args)
    payload = rows[0][0]
    arr = payload if isinstance(payload, list) else json.loads(payload)
    return arr[0]["Plan"]


def _seq_scan_relations(root: dict[str, Any]) -> set[str]:
    """The relations the plan's Seq Scan nodes touch (chunk-scoped scans
    report the parent relation's name)."""
    stack, found = [root], set()
    while stack:
        node = stack.pop()
        if node.get("Node Type") == "Seq Scan" and "Relation Name" in node:
            found.add(node["Relation Name"])
        for child in node.get("Plans", []):
            stack.append(child)
    return found


# The relations NO windowed statement may seq-scan, per mode.  The
# hypertable archive tables are excluded on the ts side: their
# chunk-level access is a Seq-Scan-per-chunk shape BY DESIGN (bounded
# by chunk pruning on the window column — the repo's TimescaleDB suites
# own that evidence); the live-side pins carry the eligibility evidence
# on both modes.  Two narrower exclusions are documented where the
# schema offers no serving index and the population is retention-bounded:
# the cron ledger's archive arm (the archive carries a tags GIN, not a
# metadata GIN — its implied finished_at bound is the anchor, pinned via
# the wait/drain plans) and the busy-ratio statement's
# job_attempts_archive arm (no started_at index on vanilla PG; on
# hypertables chunk pruning bounds it).
_PLAIN_PROTECTED = {"jobs", "jobs_archive", "job_attempts", "job_attempts_archive"}
_TS_PROTECTED = {"jobs", "job_attempts"}


# ── Wait distributions ──────────────────────────────────────────────────


async def test_wait_distribution_segments_clean_from_deferred(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """A snoozed job's short final-leg wait lands in the ``deferred``
    segment, never in the clean subset the SLO reads; the clean job's
    wait is its own row."""
    conn, schema = matrix
    q = "wait_seg_q"
    await _seed_terminal_job(
        conn, schema, queue=q, actor="wait_seg_a", wait_s=2.0, finished_age_s=60.0
    )
    await _seed_terminal_job(
        conn,
        schema,
        queue=q,
        actor="wait_seg_a",
        wait_s=5.0,
        finished_age_s=60.0,
        snooze_count=2,
    )
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=1))
    by_segment = {r["segment"]: r for r in rows if r["queue"] == q}
    assert set(by_segment) == {"clean", "deferred"}
    clean, deferred = by_segment["clean"], by_segment["deferred"]
    assert clean["count"] == 1 and clean["p50_wait_s"] == pytest.approx(2.0)
    assert deferred["count"] == 1 and deferred["p50_wait_s"] == pytest.approx(5.0)
    # The shape pin: every column the docs guide documents, exactly.
    assert set(clean) == {
        "queue",
        "segment",
        "count",
        "p50_wait_s",
        "p95_wait_s",
        "max_wait_s",
    }


async def test_wait_distribution_per_actor_groups_by_pair(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    conn, schema = matrix
    q = "wait_actor_q"
    await _seed_terminal_job(conn, schema, queue=q, actor="wa_one", wait_s=3.0, finished_age_s=60.0)
    await _seed_terminal_job(conn, schema, queue=q, actor="wa_two", wait_s=7.0, finished_age_s=60.0)
    rows = await fetch_wait_distribution(
        conn, schema=schema, window=timedelta(hours=1), per_actor=True
    )
    got = {(r["queue"], r["actor"]): r for r in rows if r["queue"] == q}
    assert set(got) == {(q, "wa_one"), (q, "wa_two")}
    assert got[(q, "wa_one")]["p50_wait_s"] == pytest.approx(3.0)
    assert got[(q, "wa_two")]["p50_wait_s"] == pytest.approx(7.0)


async def test_wait_distribution_includes_archived_job(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The UNION contract: a terminal row ALREADY pruned into the
    archive contributes its wait to the same distribution the live tier
    answers — no blind spot at the prune boundary."""
    conn, schema = matrix
    q = "wait_archive_q"
    await _seed_terminal_job(
        conn,
        schema,
        queue=q,
        actor="wait_arch_a",
        wait_s=100.0,
        finished_age_s=1800.0,
        archived=True,
    )
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=1))
    mine = [r for r in rows if r["queue"] == q]
    assert len(mine) == 1
    assert mine[0]["segment"] == "clean"
    assert mine[0]["count"] == 1
    assert mine[0]["p50_wait_s"] == pytest.approx(100.0)
    assert mine[0]["max_wait_s"] == pytest.approx(100.0)


# ── Imbalance ratios ────────────────────────────────────────────────────


async def test_queue_imbalance_balanced_fleet(matrix: tuple[asyncpg.Connection, str]) -> None:
    """The balanced shape: depth, armed wave, live workers, capacity and
    utilization all agree with the seeds."""
    conn, schema = matrix
    q = "bal_q"
    await _seed_config(conn, schema, actor="bal_actor", queue=q, max_concurrent=4)
    await _seed_worker(conn, schema, queues=[q])
    await _seed_worker(conn, schema, queues=[q])
    for _ in range(5):
        await _seed_active_job(
            conn, schema, queue=q, actor="bal_actor", status="pending", scheduled_age_s=30.0
        )
    await _seed_active_job(
        conn, schema, queue=q, actor="bal_actor", status="pending", scheduled_age_s=120.0
    )
    await _seed_active_job(
        conn, schema, queue=q, actor="bal_actor", status="scheduled", scheduled_age_s=None
    )
    rows = await fetch_queue_imbalance(conn, schema=schema)
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 6
    assert mine["live_workers"] == 2
    assert mine["effective_capacity"] == 8  # sum(max_concurrent=4) x 2 workers
    assert mine["utilization"] == pytest.approx(6 / 8)
    assert 110.0 < mine["oldest_due_age_s"] < 600.0  # the oldest due row
    assert mine["scheduled_depth"] == 1
    assert mine["wave_min_scheduled_at"] is not None
    assert mine["wave_max_scheduled_at"] == mine["wave_min_scheduled_at"]
    assert set(mine) == {
        "queue",
        "depth",
        "oldest_due_at",
        "oldest_due_age_s",
        "scheduled_depth",
        "wave_min_scheduled_at",
        "wave_max_scheduled_at",
        "live_workers",
        "actor_capacity",
        "effective_capacity",
        "utilization",
    }


async def test_queue_imbalance_starved_queue_has_null_utilization(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The starvation shape: due work, NO live worker — utilization is
    NULL (nothing can serve the queue), never a division by zero."""
    conn, schema = matrix
    q = "starve_q"
    await _seed_config(conn, schema, actor="starve_actor", queue=q, max_concurrent=4)
    for _ in range(50):
        await _seed_active_job(
            conn, schema, queue=q, actor="starve_actor", status="pending", scheduled_age_s=60.0
        )
    rows = await fetch_queue_imbalance(conn, schema=schema)
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 50
    assert mine["live_workers"] == 0
    assert mine["effective_capacity"] == 0
    assert mine["utilization"] is None


async def test_actor_backlog_vs_running_vs_capacity(matrix: tuple[asyncpg.Connection, str]) -> None:
    conn, schema = matrix
    q = "backlog_q"
    await _seed_config(conn, schema, actor="backlog_actor", queue=q, max_concurrent=4)
    for _ in range(7):
        await _seed_active_job(
            conn, schema, queue=q, actor="backlog_actor", status="pending", scheduled_age_s=30.0
        )
    for _ in range(2):
        await _seed_active_job(
            conn, schema, queue=q, actor="backlog_actor", status="running", scheduled_age_s=None
        )
    rows = await fetch_actor_backlog(conn, schema=schema)
    mine = {r["actor"]: r for r in rows}["backlog_actor"]
    assert mine["queue"] == q
    assert mine["backlog"] == 7
    assert mine["running"] == 2
    assert mine["max_concurrent"] == 4
    assert mine["saturation"] == pytest.approx(0.5)
    assert mine["unservable_backlog"] == 7 - (4 - 2)  # one claim wave absorbs 2
    assert set(mine) == {
        "actor",
        "queue",
        "backlog",
        "running",
        "max_concurrent",
        "saturation",
        "unservable_backlog",
    }


# ── Overprovisioning ────────────────────────────────────────────────────


async def test_overprovisioning_idle_queue_flagged(matrix: tuple[asyncpg.Connection, str]) -> None:
    """Live workers, zero due depth, zero terminalisations in the
    window: the flagged shape."""
    conn, schema = matrix
    q = "idle_q"
    await _seed_worker(conn, schema, queues=[q])
    await _seed_worker(conn, schema, queues=[q])
    rows = await fetch_overprovisioning(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["live_workers"] == 2
    assert mine["depth"] == 0
    assert mine["terminalisations"] == 0
    assert mine["overprovisioned"] is True


async def test_overprovisioning_productive_queue_not_flagged(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """Zero due depth but REAL throughput above the worker count: not
    overprovisioned — the verdict keys on the work done, not the queue
    emptiness alone."""
    conn, schema = matrix
    q = "busy_idle_q"
    await _seed_worker(conn, schema, queues=[q])
    await _seed_worker(conn, schema, queues=[q])
    for _ in range(5):
        await _seed_terminal_job(
            conn, schema, queue=q, actor="busy_idle_a", wait_s=1.0, finished_age_s=120.0
        )
    rows = await fetch_overprovisioning(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 0
    assert mine["terminalisations"] == 5
    assert mine["overprovisioned"] is False  # 5 completions ≥ 2 workers


async def test_worker_busy_ratio_and_drilldown(matrix: tuple[asyncpg.Connection, str]) -> None:
    """The per-worker idle query: keyed busy sums over the window, the
    archive twin included, with the one-worker drill-down."""
    conn, schema = matrix
    q = "busy_ratio_q"
    busy_worker = await _seed_worker(conn, schema, queues=[q], started_age_s=1800.0)
    idle_worker = await _seed_worker(conn, schema, queues=[q], started_age_s=1800.0)
    for i in range(3):
        jid = await _seed_terminal_job(
            conn,
            schema,
            queue=q,
            actor="busy_ratio_a",
            wait_s=1.0,
            finished_age_s=600.0 + i,
            worker_id=busy_worker,
        )
        await _seed_attempt(
            conn,
            schema,
            job_id=jid,
            worker_id=busy_worker,
            started_age_s=600.0 + i,
            duration_ms=1000,
        )
    rows = await fetch_worker_busy_ratio(conn, schema=schema, window=timedelta(hours=1))
    by_id = {r["worker_id"]: r for r in rows}
    assert by_id[busy_worker]["busy_ms"] == 3000
    assert by_id[idle_worker]["busy_ms"] == 0
    assert 0.0 < by_id[busy_worker]["busy_ratio"] < 0.01
    assert by_id[idle_worker]["busy_ratio"] == 0.0
    # The drill-down: one worker's row only, same numbers.
    drill = await fetch_worker_busy_ratio(
        conn, schema=schema, window=timedelta(hours=1), worker_id=busy_worker
    )
    assert len(drill) == 1
    assert drill[0]["worker_id"] == busy_worker
    assert drill[0]["busy_ms"] == 3000
    assert set(drill[0]) == {
        "worker_id",
        "hostname",
        "pid",
        "queues",
        "started_at",
        "last_seen_at",
        "busy_ms",
        "observed_ms",
        "busy_ratio",
    }


# ── Drain estimation ────────────────────────────────────────────────────


async def test_drain_estimate_from_throughput(matrix: tuple[asyncpg.Connection, str]) -> None:
    """depth ÷ (terminalisations/window) — the honest arithmetic, with
    the armed wave's span beside it."""
    conn, schema = matrix
    q = "drain_q"
    for _ in range(10):
        await _seed_active_job(
            conn, schema, queue=q, actor="drain_a", status="pending", scheduled_age_s=30.0
        )
    for _ in range(5):
        await _seed_terminal_job(
            conn, schema, queue=q, actor="drain_a", wait_s=1.0, finished_age_s=120.0
        )
    await _seed_active_job(
        conn, schema, queue=q, actor="drain_a", status="scheduled", scheduled_age_s=None
    )
    rows = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 10
    assert mine["terminalisations"] == 5
    assert mine["has_traffic"] is True
    assert mine["completions_per_second"] == pytest.approx(5 / 3600.0)
    assert mine["eta_seconds"] == pytest.approx(10 / (5 / 3600.0))  # 7200 s
    assert mine["scheduled_depth"] == 1
    assert set(mine) == {
        "queue",
        "depth",
        "terminalisations",
        "completions_per_second",
        "has_traffic",
        "eta_seconds",
        "scheduled_depth",
        "wave_min_scheduled_at",
        "wave_max_scheduled_at",
    }


async def test_drain_estimate_without_traffic_is_null_not_zero(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The confidence caveat, pinned: a window with no traffic yields
    has_traffic=false and eta NULL — never zero, which would read as
    'already drained'."""
    conn, schema = matrix
    q = "drain_quiet_q"
    for _ in range(4):
        await _seed_active_job(
            conn, schema, queue=q, actor="drain_quiet_a", status="pending", scheduled_age_s=30.0
        )
    rows = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 4
    assert mine["has_traffic"] is False
    assert mine["eta_seconds"] is None
    assert mine["completions_per_second"] == 0.0


# ── Cron fan-out ledger ─────────────────────────────────────────────────


async def test_cron_ledger_runaway_and_healthy(matrix: tuple[asyncpg.Connection, str]) -> None:
    """The verdict's two-window shape: a schedule whose fires outrun
    clearance in BOTH windows trends runaway; one that clears its fires
    does not; a budget-deferred schedule (which enqueues NOTHING) reads
    an honest zero row."""
    conn, schema = matrix
    now = datetime.now(UTC)
    runaway = await _seed_schedule(conn, schema, actor="ledger_runaway_a")
    healthy = await _seed_schedule(conn, schema, actor="ledger_healthy_a")
    deferred = await _seed_schedule(conn, schema, actor="ledger_deferred_a")

    # Runaway: 6 fires this window (1 cleared), 3 fires the prior
    # window (0 cleared) — the current window is a burst AND the prior
    # was one: the trend shape.
    for i in range(6):
        await _seed_terminal_job(
            conn,
            schema,
            queue="ledger_q",
            actor="ledger_runaway_a",
            wait_s=1.0,
            finished_age_s=60.0,
            status="succeeded" if i == 0 else "pending",
            schedule_id=runaway,
        )
    for _ in range(3):
        await _seed_terminal_job(
            conn,
            schema,
            queue="ledger_q",
            actor="ledger_runaway_a",
            wait_s=1.0,
            finished_age_s=5400.0,  # created ~90 min ago: the PRIOR window
            status="pending",
            schedule_id=runaway,
        )
    # Healthy: fires == clearance in both windows.
    for _ in range(2):
        await _seed_terminal_job(
            conn,
            schema,
            queue="ledger_q",
            actor="ledger_healthy_a",
            wait_s=1.0,
            finished_age_s=60.0,
            schedule_id=healthy,
        )
    _ = now, deferred
    rows = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    by_id = {r["schedule_id"]: r for r in rows}
    run = by_id[runaway]
    assert run["fires_window"] == 6
    assert run["cleared_window"] == 1
    assert run["fires_prior"] == 3
    assert run["cleared_prior"] == 0
    assert run["outstanding"] == 8  # every non-terminal fire
    assert run["runaway_trending"] is True
    healthy_row = by_id[healthy]
    assert healthy_row["fires_window"] == 2
    assert healthy_row["cleared_window"] == 2
    assert healthy_row["runaway_trending"] is False
    # The budget-deferred schedule enqueued nothing: a zero row, not an
    # absence — the ledger always answers one row per schedule.
    assert by_id[deferred]["fires_window"] == 0
    assert by_id[deferred]["outstanding"] == 0
    assert by_id[deferred]["runaway_trending"] is False
    assert set(run) == {
        "schedule_id",
        "actor",
        "cron_expr",
        "timezone",
        "dst_strategy",
        "enabled",
        "fires_window",
        "cleared_window",
        "fires_prior",
        "cleared_prior",
        "outstanding",
        "runaway_trending",
    }


async def test_cron_ledger_includes_archived_fires(matrix: tuple[asyncpg.Connection, str]) -> None:
    """A schedule fire already pruned into the archive counts in the
    ledger's windows — the UNION contract again, per schedule."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="ledger_arch_a")
    await _seed_terminal_job(
        conn,
        schema,
        queue="ledger_arch_q",
        actor="ledger_arch_a",
        wait_s=1.0,
        finished_age_s=600.0,
        archived=True,
        schedule_id=sid,
    )
    rows = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in rows}[sid]
    assert mine["fires_window"] == 1
    assert mine["cleared_window"] == 1  # archived rows are terminal by definition
    assert mine["outstanding"] == 0


# ── EXPLAIN pins ────────────────────────────────────────────────────────


async def test_explain_no_seq_scan_on_hot_paths(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """Every windowed statement's plan, on the seeded corpus: no Seq
    Scan touches the protected relations (see _PLAIN_PROTECTED /
    _TS_PROTECTED and the exclusions note above them).  The statements
    are rendered through the module's own builders — the production
    text, never a restated copy."""
    conn, schema = matrix
    ts_mode = schema == "insights_ts"
    windowed = _TS_PROTECTED if ts_mode else _PLAIN_PROTECTED

    statements: list[tuple[str, tuple[object, ...], set[str]]] = [
        (_build_wait_sql(schema, per_actor=False), (timedelta(hours=24),), windowed),
        (_build_wait_sql(schema, per_actor=True), (timedelta(hours=24),), windowed),
        (
            insights_mod._QUEUE_IMBALANCE_SQL.format(  # pyright: ignore[reportPrivateUsage]  # Why: the pin renders the module's own template so it validates the production text, never a copy.
                schema=schema,
                _DUE_NOW=insights_mod._DUE_NOW,  # pyright: ignore[reportPrivateUsage]
            ),
            (30,),
            windowed,
        ),
        (
            insights_mod._QUEUE_DRAIN_SQL.format(  # pyright: ignore[reportPrivateUsage]
                schema=schema,
                _TERMINAL_IN=insights_mod._TERMINAL_IN,  # pyright: ignore[reportPrivateUsage]
                _FINISHED_BOUND=insights_mod._FINISHED_BOUND,  # pyright: ignore[reportPrivateUsage]
                _DUE_NOW=insights_mod._DUE_NOW,  # pyright: ignore[reportPrivateUsage]
            ),
            (timedelta(hours=24),),
            windowed,
        ),
        # The ledger's archive arm is GIN-less by schema shape (see the
        # exclusions note): only the live hot path is pinned for it.
        (
            _build_cron_ledger_sql(schema),
            (timedelta(hours=1), timedelta(hours=2)),
            {"jobs"},
        ),
        # The busy-ratio statement's archive-attempts arm is
        # retention-bounded on vanilla PG (see the exclusions note).
        (
            insights_mod._WORKER_BUSY_SQL.format(schema=schema),  # pyright: ignore[reportPrivateUsage]
            (timedelta(hours=24), None),
            {"job_attempts"},
        ),
    ]
    for sql, args, protected in statements:
        root = await _explain(conn, sql, *args)
        seq = _seq_scan_relations(root)
        bad = seq & protected
        assert not bad, (
            f"Seq Scan on protected relation(s) {sorted(bad)} in plan:\n{json.dumps(root)[:4000]}"
        )


# ── Attack wave: census seams, confound edges, drain honesty, hygiene ───
#
# An adversarial pass over the seven read functions.  Each test attacks
# one claim from the module's contract: the UNION's census at the
# live/archive seam, the deferral/retry segmentation's documented
# answers, the drain estimate's NULL verdict and arithmetic sanity, the
# cron ledger's prior-window bound, and the binding seams' hygiene.


async def _seed_fire(
    conn: asyncpg.Connection,
    schema: str,
    schedule_id: uuid.UUID | None,
    actor: str,
    *,
    created_age_s: float | None = None,
    status: str = "succeeded",
    scheduled_age_s: float | None = None,
    started_age_s: float | None = None,
    finished_age_s: float | None = None,
    wait_s: float = 1.0,
    snooze_count: int = 0,
    rate_limit_blocked_count: int = 0,
    attempt: int = 0,
    archived: bool = False,
    extra_metadata: dict[str, Any] | None = None,
    queue: str = "attack_q",
) -> uuid.UUID:
    """One cron-fire-shaped row with FULL timestamp control (the
    existing helpers fix created_at to the wait arithmetic; the ledger's
    created_at windows need their own hands).  Terminal rows derive
    started/finished from the given ages and scheduled_at from the wait;
    created_at defaults to scheduled_at (a fire's enqueue ≈ its due
    stamp).  Non-terminal rows leave started_at/finished_at NULL."""
    jid = new_job_id()
    now = datetime.now(UTC)
    if status in ("pending", "scheduled"):
        scheduled = now - timedelta(seconds=scheduled_age_s or 0.0)
        started = finished = None
    else:
        assert started_age_s is not None and finished_age_s is not None
        started = now - timedelta(seconds=started_age_s)
        finished = now - timedelta(seconds=finished_age_s)
        scheduled = started - timedelta(seconds=wait_s)
    created = now - timedelta(seconds=created_age_s) if created_age_s is not None else scheduled
    meta: dict[str, Any] = dict(extra_metadata or {})
    if schedule_id is not None:
        meta["cron_schedule_id"] = str(schedule_id)
    table = "jobs_archive" if archived else "jobs"
    expire_clause = ", expire_at" if archived else ""
    expire_value = ", statement_timestamp() + interval '365 days'" if archived else ""
    await conn.execute(
        f"""INSERT INTO {schema}.{table} (
                id, actor, queue, payload, max_attempts, retry_kind, status, attempt,
                created_at, scheduled_at, started_at, finished_at,
                snooze_count, rate_limit_blocked_count, metadata{expire_clause}
            ) VALUES (
                $1, $2, $3, '{{"v": 1}}'::jsonb, 3, 'transient',
                $4::{schema}.job_status, $5, $6, $7, $8, $9, $10, $11, $12::jsonb{expire_value}
            )""",
        jid,
        actor,
        queue,
        status,
        attempt,
        created,
        scheduled,
        started,
        finished,
        snooze_count,
        rate_limit_blocked_count,
        json.dumps(meta),
    )
    return jid


@pytest.mark.parametrize(
    "bad",
    [
        "taskq; DROP TABLE public.jobs",  # statement smuggling past the match
        "taskq\n",  # the trailing-newline $-anchor regression constants.py documents
        'taskq"',  # quote-escape of the interpolation seam
        "taskq'",  # dblink-style literal escape attempt
        'taskq"--x',  # comment tail
        "public.tasks",  # qualified identifier
        "taskq$; --",  # dollar-quote opener
        "",  # empty
    ],
)
async def test_attack_schema_guard_rejects_injection_and_newline(bad: str) -> None:
    """The schema identifier is the ONLY interpolated value: every
    fetcher and builder must reject metacharacter, comment-tail,
    qualified and trailing-newline schemas BEFORE touching a
    connection — _IDENT_RE's \\A/\\Z anchors close the $-matches-before-
    newline hole (the exact regression constants.py's own comment
    records)."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await fetch_wait_distribution(None, schema=bad, window=timedelta(hours=1))  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await fetch_cron_ledger(None, schema=bad, window=timedelta(hours=1))  # pyright: ignore[reportArgumentType]
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _build_wait_sql(bad, per_actor=False)
    with pytest.raises(ValueError, match="invalid schema identifier"):
        _build_cron_ledger_sql(bad)


async def test_attack_union_census_seam_and_duplicate_trust(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The census at the live/archive seam: a row the prune ALREADY
    moved counts once (archive arm), its pre-prune live twin counts once
    (live arm), a 40-day-old terminal row outside every named window
    counts zero on BOTH tiers (the bound, not the tier, decides), and a
    same-id row physically present in BOTH tiers double counts — pinned
    deliberately: UNION ALL trusts the prune's atomic
    insert-then-delete exclusivity, and the alternative (a UNION on the
    four-column projection) would silently DEDUPE two genuinely
    distinct jobs whose queue/actor/segment/wait all agree, corrupting
    every percentile.  The trust is the correct trade."""
    conn, schema = matrix
    q = "attack_seam_q"
    # Post-prune: the row lives ONLY in the archive now.
    await _seed_terminal_job(
        conn,
        schema,
        queue=q,
        actor="attack_seam_a",
        wait_s=100.0,
        finished_age_s=600.0,
        archived=True,
    )
    # Pre-prune: a terminal row still inside the live retention.
    await _seed_terminal_job(
        conn, schema, queue=q, actor="attack_seam_a", wait_s=50.0, finished_age_s=600.0
    )
    # The seam's other side: terminalised 40 days ago, past the default
    # 30d prune retention (the sweep would have moved it) — outside
    # every named window on both tiers, so neither arm may count it.
    await _seed_terminal_job(
        conn,
        schema,
        queue=q,
        actor="attack_seam_a",
        wait_s=5.0,
        finished_age_s=40 * 24 * 3600.0,
        archived=True,
    )
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=1))
    mine = [r for r in rows if r["queue"] == q]
    assert len(mine) == 1 and mine[0]["segment"] == "clean"
    assert mine[0]["count"] == 2  # the archive row + the live row, once each
    assert mine[0]["p50_wait_s"] == pytest.approx(75.0)
    assert mine[0]["max_wait_s"] == pytest.approx(100.0)
    # The duplicate probe: physically the same id in BOTH tiers (a
    # prune that violated its own exclusivity) is counted by both arms.
    dup = new_job_id()
    for table in ("jobs", "jobs_archive"):
        expire = ", expire_at" if table == "jobs_archive" else ""
        value = ", statement_timestamp() + interval '365 days'" if table == "jobs_archive" else ""
        await conn.execute(
            f"""INSERT INTO {schema}.{table} (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    created_at, scheduled_at, started_at, finished_at, metadata{expire}
                ) VALUES (
                    $1, 'attack_dup_a', 'attack_dup_q', '{{"v": 1}}'::jsonb, 3, 'transient',
                    'succeeded'::{schema}.job_status,
                    statement_timestamp() - interval '10 minutes',
                    statement_timestamp() - interval '10 minutes' - interval '7 seconds',
                    statement_timestamp() - interval '10 minutes' - interval '5 seconds',
                    statement_timestamp() - interval '10 minutes', '{{}}'::jsonb{value}
                )""",
            dup,
        )
    dup_rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=1))
    dup_mine = [r for r in dup_rows if r["queue"] == "attack_dup_q"]
    assert len(dup_mine) == 1 and dup_mine[0]["count"] == 2  # the honest double count


async def test_attack_ledger_anchor_admits_prior_and_current_archived_fires(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The archive arm's finished_at anchor ($2 = 2 x window) must admit
    EVERY archived fire the created_at windows ask for: a fire created
    in the prior window finished strictly after creating, so its
    finished_at clears now - 2 x window — and a fire created in the
    CURRENT window a fortiori.  Neither fire may be dropped, and
    cleared_prior must count ONLY prior-window fires (the documented
    'prior equal window') — a current-window fire finishing fast and
    pruning fast may not inflate it."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="attack_anchor_a")
    # Prior-window fire: created 90 min ago, terminalised 72 min ago,
    # pruned (short-retention deployment).  finished 72m ago clears the
    # 2h anchor; created 90m ago lands in the PRIOR 1h window.
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_anchor_a",
        queue="attack_anchor_q",
        created_age_s=5400.0,
        started_age_s=4320.5,
        finished_age_s=4320.0,
        archived=True,
    )
    # Current-window fire: created 30 min ago, terminalised 25 min ago,
    # already pruned.
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_anchor_a",
        queue="attack_anchor_q",
        created_age_s=1800.0,
        started_age_s=1500.5,
        finished_age_s=1500.0,
        archived=True,
    )
    rows = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in rows}[sid]
    assert mine["fires_window"] == 1
    assert mine["cleared_window"] == 1
    assert mine["fires_prior"] == 1
    assert mine["cleared_prior"] == 1  # the prior fire ONLY — not the current one too
    assert mine["outstanding"] == 0
    assert mine["runaway_trending"] is False


async def test_attack_runaway_not_suppressed_by_current_window_archived_fires(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """THE runaway-suppression attack.  A short-retention deployment
    (prune retention < 2 x window — explicitly supported, the module's
    own 'retention floor is the analytics floor' doctrine): a schedule
    whose fires are NOT clearing (one stuck pending fire per window,
    two windows running) PLUS one fast fire this window that already
    terminalised AND pruned into the archive.  The documented verdict is
    TRUE — fires > cleared in both windows.  An archive-arm cleared_prior
    that lacks the prior window's upper created_at bound counts the
    fast fire's clearance as a PRIOR-window clearance, inflating
    cleared_prior to 1 and silently flipping the trend to FALSE."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="attack_runaway_a")
    # Prior window: one fire, created 90 min ago, STILL pending —
    # never claimed (fires_prior=1 live, cleared_prior=0 live).
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_runaway_a",
        queue="attack_runaway_q",
        created_age_s=5400.0,
        status="pending",
        scheduled_age_s=5400.0,
    )
    # Current window: one fire, created 10 min ago, STILL pending
    # (fires_window=1 live, cleared_window=0 live).
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_runaway_a",
        queue="attack_runaway_q",
        created_age_s=600.0,
        status="pending",
        scheduled_age_s=600.0,
    )
    # Current window: one fast fire, created 30 min ago, terminalised
    # 25 min ago, pruned into the archive by the short retention
    # (fires_window=1 archive, cleared_window=1 archive).
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_runaway_a",
        queue="attack_runaway_q",
        created_age_s=1800.0,
        started_age_s=1500.5,
        finished_age_s=1500.0,
        archived=True,
    )
    rows = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in rows}[sid]
    assert mine["fires_window"] == 2  # 1 live + 1 archived
    assert mine["cleared_window"] == 1  # only the archived fire terminalised
    assert mine["fires_prior"] == 1
    assert mine["cleared_prior"] == 0  # NO prior-window fire ever cleared
    assert mine["outstanding"] == 2
    assert mine["runaway_trending"] is True  # two windows of fan-out outrunning clearance


async def test_attack_deferred_five_times_wait_is_final_leg_only(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """A job deferred five times then succeeding: its wait appears ONLY
    in the deferred segment, with the FINAL leg's value
    (started_at - final scheduled_at) — the four deferred legs fold into
    no bucket, and the rate-limit counter alone (snooze_count = 0)
    segments deferred too."""
    conn, schema = matrix
    q = "attack_defer5_q"
    await _seed_fire(
        conn,
        schema,
        None,
        "attack_defer5_a",
        queue=q,
        created_age_s=3600.0,
        started_age_s=120.5,
        finished_age_s=120.0,
        wait_s=3.0,
        snooze_count=5,
    )
    # The rate-limit-only twin: blocked 4x by the bucket, never snoozed.
    await _seed_fire(
        conn,
        schema,
        None,
        "attack_defer5_a",
        queue=q,
        created_age_s=3600.0,
        started_age_s=60.5,
        finished_age_s=60.0,
        wait_s=9.0,
        rate_limit_blocked_count=4,
    )
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=6))
    mine = {r["segment"]: r for r in rows if r["queue"] == q}
    assert set(mine) == {"deferred"}  # NEITHER row reaches the clean subset
    assert mine["deferred"]["count"] == 2
    assert mine["deferred"]["p50_wait_s"] == pytest.approx(6.0)  # median of {3, 9}
    assert mine["deferred"]["max_wait_s"] == pytest.approx(9.0)


async def test_attack_retry_attempt_seven_lands_clean(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The documented answer, pinned: a job at attempt 7 from RETRIES
    (the retry arm re-stamps scheduled_at and touches NO deferral
    counter) lands in the CLEAN segment — its wait is the final
    attempt's claim latency, because the earlier legs left no
    per-attempt due stamp.  The deferral counters, not the attempt
    number, decide the segment."""
    conn, schema = matrix
    q = "attack_retry7_q"
    await _seed_fire(
        conn,
        schema,
        None,
        "attack_retry7_a",
        queue=q,
        created_age_s=7200.0,
        started_age_s=45.5,
        finished_age_s=45.0,
        wait_s=2.0,
        attempt=7,
    )
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=6))
    mine = [r for r in rows if r["queue"] == q]
    assert len(mine) == 1
    assert mine[0]["segment"] == "clean"  # attempt 7, counters zero → clean
    assert mine[0]["count"] == 1
    assert mine[0]["p50_wait_s"] == pytest.approx(2.0)


async def test_attack_budget_deferred_fire_is_clean_when_finally_enqueued(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """A budget-deferred cron fire enqueues NO row at deferral time (its
    deferral is invisible to this layer — the taskq.cron.budget_deferrals
    counter's record); when the budget finally admits it, the tick
    enqueues a FRESH row whose created_at/scheduled_at are the actual
    enqueue instant with zero deferral counters — so its wait lands in
    the CLEAN bucket (the operator's deferral choice is not the queue's
    latency), and the ledger counts the fire in the window containing
    the ACTUAL enqueue, not the skipped one."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="attack_budget_a")
    # The eventually-enqueued fire: created/scheduled 20 min ago (the
    # budget's admittance instant), claimed and terminalised normally.
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_budget_a",
        queue="attack_budget_q",
        created_age_s=1200.0,
        started_age_s=1194.5,
        finished_age_s=1194.0,
        wait_s=4.0,
    )
    ledger = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in ledger}[sid]
    assert mine["fires_window"] == 1  # counted at the actual enqueue instant
    assert mine["cleared_window"] == 1
    assert mine["runaway_trending"] is False
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=6))
    seg = {r["segment"]: r for r in rows if r["queue"] == "attack_budget_q"}
    assert set(seg) == {"clean"}  # clean bucket, zero deferral counters
    assert seg["clean"]["p50_wait_s"] == pytest.approx(4.0)


async def test_attack_dst_allof_twins_double_the_ledger_honestly(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The DST allof twins: the overlap hour's double enqueue IS two
    fires, and the forward-stamped SECOND occurrence is a third —
    fires_window counts every enqueue honestly (the ledger keys on
    created_at, not status), cleared_window doubles in lockstep as the
    two terminalise, and the future-armed twin rides in outstanding AND
    in fires (both by design).  Nothing corrupts: the verdict stays
    FALSE on an empty prior window.  In the wait distribution each
    terminal twin is its own clean observation (two rows, not one)."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="attack_allof_a")
    for _ in range(2):  # the twins: identical, both cleared
        await _seed_fire(
            conn,
            schema,
            sid,
            "attack_allof_a",
            queue="attack_allof_q",
            created_age_s=1200.0,
            started_age_s=1194.5,
            finished_age_s=1194.0,
            wait_s=2.0,
        )
    # The allof SECOND occurrence: forward-stamped, still armed.
    await _seed_fire(
        conn,
        schema,
        sid,
        "attack_allof_a",
        queue="attack_allof_armed_q",
        created_age_s=60.0,
        status="scheduled",
        scheduled_age_s=None,
    )
    ledger = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in ledger}[sid]
    assert mine["fires_window"] == 3  # both twins AND the forward-armed occurrence
    assert mine["cleared_window"] == 2  # doubling in lockstep
    assert mine["outstanding"] == 1  # the forward-armed twin
    assert mine["runaway_trending"] is False  # ratio uncorrupted
    rows = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=6))
    seg = [r for r in rows if r["queue"] == "attack_allof_q"]
    assert len(seg) == 1 and seg[0]["segment"] == "clean" and seg[0]["count"] == 2
    assert seg[0]["p50_wait_s"] == pytest.approx(2.0)


async def test_attack_drain_burst_rate_overstatement_is_documented_extrapolation(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The burst-overhang attack: the trailing window's terminalisations
    all came from a burst that is NOW over — the realised rate
    overstates the present, so eta_seconds understates the drain.  The
    estimate does NOT pretend to detect the burst: has_traffic stays
    TRUE and the arithmetic is exactly depth ÷ (terminalisations/window),
    because the docstring's contract is 'a THROUGHPUT extrapolation that
    assumes the next window looks like the last one' — the raw inputs
    ride beside the verdict so a dashboard can trend the overhang."""
    conn, schema = matrix
    q = "attack_burst_q"
    for _ in range(50):
        await _seed_active_job(
            conn,
            schema,
            queue=q,
            actor="attack_burst_a",
            status="pending",
            scheduled_age_s=30.0,
        )
    for i in range(100):  # the burst: 100 terminalisations inside the hour
        await _seed_terminal_job(
            conn,
            schema,
            queue=q,
            actor="attack_burst_a",
            wait_s=1.0,
            finished_age_s=600.0 + i,
        )
    rows = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["has_traffic"] is True
    assert mine["terminalisations"] == 100
    assert mine["eta_seconds"] == pytest.approx(50 / (100 / 3600.0))  # 1800 s — understated
    assert mine["completions_per_second"] == pytest.approx(100 / 3600.0)


async def test_attack_drain_armed_only_queue_renders_null_verdict(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """A queue whose only population is future-armed: the row must
    RENDER (the queues CTE admits the armed arm), with depth 0,
    has_traffic FALSE, eta NULL — no division by zero, no eta 0 — and
    the wave's span beside it."""
    conn, schema = matrix
    q = "attack_armed_q"
    for _ in range(3):
        await _seed_active_job(
            conn,
            schema,
            queue=q,
            actor="attack_armed_a",
            status="scheduled",
            scheduled_age_s=None,
        )
    rows = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 0
    assert mine["terminalisations"] == 0
    assert mine["has_traffic"] is False
    assert mine["eta_seconds"] is None
    assert mine["scheduled_depth"] == 3
    assert mine["wave_min_scheduled_at"] is not None


async def test_attack_drain_rate_half_per_minute_and_zero_depth_eta(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The arithmetic's edges: depth 1 against a realised rate of one
    completion per two minutes reads eta 120 s (sane, exact); depth 0
    WITH traffic reads eta 0.0 (legitimately 'already drained' —
    has_traffic TRUE distinguishes it from the undefined NULL verdict)."""
    conn, schema = matrix
    q = "attack_rate_q"
    await _seed_active_job(
        conn, schema, queue=q, actor="attack_rate_a", status="pending", scheduled_age_s=30.0
    )
    await _seed_terminal_job(
        conn, schema, queue=q, actor="attack_rate_a", wait_s=1.0, finished_age_s=60.0
    )
    rows = await fetch_drain_estimates(conn, schema=schema, window=timedelta(minutes=2))
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["depth"] == 1
    assert mine["terminalisations"] == 1
    assert mine["completions_per_second"] == pytest.approx(1 / 120.0)  # 0.5/min
    assert mine["eta_seconds"] == pytest.approx(120.0)
    # Depth 0 with traffic: eta 0.0, not NULL.
    q0 = "attack_zero_depth_q"
    await _seed_terminal_job(
        conn, schema, queue=q0, actor="attack_rate_a", wait_s=1.0, finished_age_s=60.0
    )
    rows0 = await fetch_drain_estimates(conn, schema=schema, window=timedelta(minutes=2))
    mine0 = {r["queue"]: r for r in rows0}[q0]
    assert mine0["has_traffic"] is True
    assert mine0["eta_seconds"] == 0.0


async def test_attack_metacharacter_names_round_trip_through_bindings(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """Actor/queue names carrying SQL metacharacters ride EVERY binding
    seam ($N) — never the f-string (only the validated schema is
    interpolated): the reads answer the exact rows and the hot table is
    untouched afterwards."""
    conn, schema = matrix
    evil_actor = "act'; DROP TABLE x; --"
    evil_queue = 'q"$1) UNION SELECT 1 --'
    await _seed_terminal_job(
        conn, schema, queue=evil_queue, actor=evil_actor, wait_s=6.0, finished_age_s=60.0
    )
    sid = await _seed_schedule(conn, schema, actor=evil_actor)
    await _seed_fire(
        conn,
        schema,
        sid,
        evil_actor,
        queue=evil_queue,
        created_age_s=1200.0,
        started_age_s=1194.5,
        finished_age_s=1194.0,
    )
    rows = await fetch_wait_distribution(
        conn, schema=schema, window=timedelta(hours=1), per_actor=True
    )
    mine = [r for r in rows if r["queue"] == evil_queue]
    assert len(mine) == 1 and mine[0]["actor"] == evil_actor
    assert mine[0]["segment"] == "clean" and mine[0]["count"] == 2
    drain = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    dmine = [r for r in drain if r["queue"] == evil_queue]
    assert len(dmine) == 1 and dmine[0]["terminalisations"] == 2
    ledger = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    lmine = {r["schedule_id"]: r for r in ledger}[sid]
    assert lmine["fires_window"] == 1
    # The injection never executed: the seeded rows are all still there.
    n = await conn.fetchval(f"SELECT count(*) FROM {schema}.jobs WHERE queue = $1", evil_queue)
    assert n == 2


async def test_attack_gin_containment_type_and_key_strictness(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """The GIN containment lateral's key handling: the cron tick stamps
    metadata {'cron_schedule_id': str(id)} — TEXT.  The bound
    jsonb_build_object('cron_schedule_id', s.id::text) matches exactly
    that (extra keys are fine, @> is containment), a NUMBER-valued stamp
    matches NOTHING (jsonb containment is type-strict — a divergence
    from the ->> '123' text-coercion the naive reader would write), and
    a NULL-metadata job matches nothing."""
    conn, schema = matrix
    sid = await _seed_schedule(conn, schema, actor="attack_gin_a")
    await _seed_fire(  # the real stamp shape + an extra key: matches
        conn,
        schema,
        sid,
        "attack_gin_a",
        queue="attack_gin_q",
        created_age_s=1200.0,
        started_age_s=1194.5,
        finished_age_s=1194.0,
        extra_metadata={"lane": "a"},
    )
    await _seed_fire(  # numeric stamp: must NOT match
        conn,
        schema,
        sid,
        "attack_gin_a",
        queue="attack_gin_num_q",
        created_age_s=1200.0,
        started_age_s=1194.5,
        finished_age_s=1194.0,
        extra_metadata={},  # replaced below — the seeder always stamps text
    )
    # Overwrite the second row's stamp with the numeric form.
    await conn.execute(
        f"""UPDATE {schema}.jobs SET metadata = '{{"cron_schedule_id": 123}}'::jsonb
            WHERE queue = 'attack_gin_num_q'""",
    )
    await conn.execute(  # a no-cron-key terminal row: must NOT match
        f"""INSERT INTO {schema}.jobs (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                created_at, scheduled_at, started_at, finished_at, metadata
            ) VALUES (
                gen_random_uuid(), 'attack_gin_a', 'attack_gin_null_q',
                '{{"v": 1}}'::jsonb, 3, 'transient', 'succeeded'::{schema}.job_status,
                statement_timestamp() - interval '20 minutes',
                statement_timestamp() - interval '20 minutes',
                statement_timestamp() - interval '20 minutes' - interval '1 seconds',
                statement_timestamp() - interval '20 minutes', '{{}}'::jsonb
            )""",
    )
    rows = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
    mine = {r["schedule_id"]: r for r in rows}[sid]
    assert mine["fires_window"] == 1  # only the text-stamped row
    assert mine["cleared_window"] == 1
    assert mine["outstanding"] == 0  # the numeric and NULL rows are not the schedule's


async def test_attack_elapsed_scheduled_row_is_armed_transient(
    matrix: tuple[asyncpg.Connection, str],
) -> None:
    """A 'scheduled' row whose snooze delay ELAPSED but whose
    scheduled→pending promotion (sweep 3's own transition) has not run
    yet is a TRANSIENT state: the imbalance read counts it in
    scheduled_depth with a PAST wave_min — pinned as the point-in-time
    census contract (the read does not pre-empt the sweep's ownership of
    the flip; between the stamp elapsing and the sweep tick the row is
    neither pending-due nor future-armed, and this read shows it as
    armed) — and the drain read carries the same population."""
    conn, schema = matrix
    q = "attack_elapsed_q"
    await _seed_active_job(
        conn,
        schema,
        queue=q,
        actor="attack_elapsed_a",
        status="scheduled",
        scheduled_age_s=60.0,  # elapsed: scheduled_at is 60 s in the PAST
    )
    rows = await fetch_queue_imbalance(conn, schema=schema)
    mine = {r["queue"]: r for r in rows}[q]
    assert mine["scheduled_depth"] == 1
    assert mine["depth"] == 0  # not pending — the sweep owns the flip
    assert mine["wave_min_scheduled_at"] is not None
    assert mine["wave_min_scheduled_at"] < datetime.now(UTC)  # the past-armed confound
    drain = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
    dmine = {r["queue"]: r for r in drain}[q]
    assert dmine["scheduled_depth"] == 1

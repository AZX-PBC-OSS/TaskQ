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

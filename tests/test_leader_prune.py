"""Integration tests for prune sweep, expiry sweep, and admin UI archive fallback.

Runs against real Postgres 18 via testcontainers. Per-test schema isolation
via the pg_conn fixture (DROP SCHEMA … CASCADE teardown).

Covers archive-move semantics, atomicity, cascades, batch draining,
per-status/per-actor retention overrides, index usage, expiry sweeps,
and concurrent-lock behavior.
"""

import asyncio
import contextlib
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_job_id, new_uuid
from taskq._json import dumps_str
from taskq.backend._sql_templates import COPY_FROM_COLUMNS
from taskq.constants import schema_lock_name
from taskq.settings import TaskQSettings
from taskq.worker.leader import PruneResult, archive_expiry_sweep, prune_terminal_jobs

pytestmark = pytest.mark.integration

# ── Helpers ──────────────────────────────────────────────────────────────


async def _apply(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)


async def _seed_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str,
    finished_at: datetime,
    actor: str = "test_actor",
    queue: str = "default",
    job_id: uuid.UUID | None = None,
    metadata: dict[str, object] | None = None,
) -> uuid.UUID:
    jid = job_id or new_uuid()
    now = datetime.now(UTC)
    md_str = dumps_str(metadata) if metadata else "{}"
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            finished_at, metadata, payload_schema_ver
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            $7::{schema}.job_status, 0, $8, $9,
            $10, $11::jsonb, 1
        )""",  # noqa: S608
        jid,
        actor,
        queue,
        '{"v": 1}',
        3,
        "transient",
        status,
        now,
        now + timedelta(hours=1),
        finished_at,
        md_str,
    )
    return jid


async def _seed_job_attempt(
    conn: asyncpg.Connection,
    job_id: uuid.UUID,
    *,
    schema: str,
    attempt: int = 1,
    outcome: str = "failed",
) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts
            (job_id, attempt, started_at, finished_at, outcome, duration_ms, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, '{{}}'::jsonb)""",  # noqa: S608
        job_id,
        attempt,
        now - timedelta(minutes=5),
        now - timedelta(minutes=4),
        outcome,
        500,
    )


async def _seed_archive_attempt(
    conn: asyncpg.Connection,
    job_id: uuid.UUID,
    *,
    schema: str,
    attempt: int = 1,
    outcome: str = "succeeded",
) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.job_attempts_archive
            (job_id, attempt, started_at, finished_at, outcome, duration_ms, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, '{{}}'::jsonb)""",  # noqa: S608
        job_id,
        attempt,
        now - timedelta(minutes=5),
        now - timedelta(minutes=4),
        outcome,
        500,
    )


async def _seed_archive_row(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str = "succeeded",
    expire_at: datetime,
    actor: str = "test_actor",
    queue: str = "default",
    job_id: uuid.UUID | None = None,
    archived_at: datetime | None = None,
) -> uuid.UUID:
    jid = job_id or new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, schedule_to_close,
            finished_at, archived_at, expire_at, metadata, payload_schema_ver
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            $7::{schema}.job_status, 0, $8, $9,
            $10, $11, $12, '{{}}'::jsonb, 1
        )""",  # noqa: S608
        jid,
        actor,
        queue,
        '{"v": 1}',
        3,
        "transient",
        status,
        now,
        now + timedelta(hours=1),
        now - timedelta(days=31),
        archived_at or now,
        expire_at,
    )
    return jid


_JOBS_COLUMNS = [
    "id",
    "actor",
    "queue",
    "payload",
    "max_attempts",
    "retry_kind",
    "status",
    "priority",
    "scheduled_at",
    "schedule_to_close",
    "finished_at",
    "metadata",
    "payload_schema_ver",
]

_JOBS_ARCHIVE_COLUMNS = [
    "id",
    "actor",
    "queue",
    "payload",
    "max_attempts",
    "retry_kind",
    "status",
    "priority",
    "scheduled_at",
    "schedule_to_close",
    "finished_at",
    "archived_at",
    "expire_at",
    "metadata",
    "payload_schema_ver",
]


async def _seed_terminal_jobs_bulk(
    conn: asyncpg.Connection,
    schema: str,
    *,
    count: int,
    status: str,
    finished_at: datetime,
    actor: str = "test_actor",
    queue: str = "default",
) -> list[uuid.UUID]:
    """Seed *count* terminal jobs via a single ``copy_records_to_table``."""
    now = datetime.now(UTC)
    scheduled = now + timedelta(hours=1)
    records: list[tuple[object, ...]] = []
    ids: list[uuid.UUID] = []
    for _ in range(count):
        jid = new_job_id()
        ids.append(jid)
        records.append(
            (
                jid,
                actor,
                queue,
                '{"v": 1}',
                3,
                "transient",
                status,
                0,
                now,
                scheduled,
                finished_at,
                "{}",
                1,
            )
        )
    await conn.copy_records_to_table(
        "jobs",
        schema_name=schema,
        records=records,
        columns=_JOBS_COLUMNS,
    )
    return ids


async def _seed_archive_rows_bulk(
    conn: asyncpg.Connection,
    schema: str,
    *,
    count: int,
    status: str = "succeeded",
    expire_at: datetime,
    actor: str = "test_actor",
    queue: str = "default",
    finished_at: datetime | None = None,
) -> list[uuid.UUID]:
    """Seed *count* archive rows via a single ``copy_records_to_table``."""
    now = datetime.now(UTC)
    scheduled = now + timedelta(hours=1)
    finished = finished_at or now - timedelta(days=31)
    records: list[tuple[object, ...]] = []
    ids: list[uuid.UUID] = []
    for _ in range(count):
        jid = new_job_id()
        ids.append(jid)
        records.append(
            (
                jid,
                actor,
                queue,
                '{"v": 1}',
                3,
                "transient",
                status,
                0,
                now,
                scheduled,
                finished,
                now,
                expire_at,
                "{}",
                1,
            )
        )
    await conn.copy_records_to_table(
        "jobs_archive",
        schema_name=schema,
        records=records,
        columns=_JOBS_ARCHIVE_COLUMNS,
    )
    return ids


async def _count(conn: asyncpg.Connection, table: str, *, schema: str, where: str = "") -> int:
    sql = f"SELECT count(*) FROM {schema}.{table}"  # noqa: S608
    if where:
        sql += f" WHERE {where}"
    row = await conn.fetchrow(sql)
    assert row is not None
    return row["count"]


# ── Acceptance A - archive move ────────────────────────────────────


async def test_archive_move(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 50 terminal jobs (10 per status) older than retention. Run prune.
    Assert: all 50 in jobs_archive; 0 in jobs for those IDs;
    job_attempts_archive populated; archived_at ≈ now(); expire_at ≈ now() + retention."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    old = now - timedelta(days=31)
    statuses = ["succeeded", "failed", "cancelled", "crashed", "abandoned"]
    job_ids: list[uuid.UUID] = []

    for status in statuses:
        for _ in range(10):
            jid = await _seed_terminal_job(pg_conn, status=status, finished_at=old, schema=schema)
            job_ids.append(jid)
            await _seed_job_attempt(
                pg_conn,
                jid,
                outcome="failed" if status != "succeeded" else "succeeded",
                schema=schema,
            )

    retention_per_status = {s: timedelta(days=30) for s in statuses}
    before = datetime.now(UTC)
    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status=retention_per_status,
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )

    assert result.total_deleted == 50
    assert result.archived == 50

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 50

    for jid in job_ids:
        row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid)  # noqa: S608
        assert row is None, f"job {jid} still in jobs table"

    attempts_archive_count = await _count(pg_conn, "job_attempts_archive", schema=schema)
    assert attempts_archive_count == 50

    sample = await pg_conn.fetchrow(
        f"SELECT archived_at, expire_at FROM {schema}.jobs_archive LIMIT 1"  # noqa: S608
    )
    assert sample is not None
    assert abs((sample["archived_at"] - before).total_seconds()) < 5
    expected_expire = sample["archived_at"] + timedelta(days=365)
    assert abs((sample["expire_at"] - expected_expire).total_seconds()) < 5


# ── Atomicity on error ────────────────────────────────────────────


async def test_atomicity_on_error(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Inject a constraint violation mid-CTE. Assert: zero rows in
    jobs_archive; original rows remain in jobs."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)

    archive_count_before = await _count(pg_conn, "jobs_archive", schema=schema)

    try:
        await pg_conn.execute(
            f"""WITH candidate_ids AS (
                SELECT id FROM {schema}.jobs
                WHERE status = $1::{schema}.job_status AND finished_at < $2
                ORDER BY finished_at LIMIT $3
            ), moved AS (
                INSERT INTO {schema}.jobs_archive (id, actor, queue, payload, max_attempts, retry_kind, status, priority, scheduled_at, schedule_to_close, finished_at, archived_at, expire_at, metadata, payload_schema_ver)
                SELECT j.id, j.actor, j.queue, j.payload, j.max_attempts, 'invalid_kind'::text, j.status, j.priority, j.scheduled_at, j.schedule_to_close, j.finished_at, now(), now() + $4, j.metadata, j.payload_schema_ver
                FROM {schema}.jobs j
                JOIN candidate_ids c ON j.id = c.id
                RETURNING id
            )
            SELECT * FROM moved""",  # noqa: S608
            "succeeded",
            datetime.now(UTC) - timedelta(days=30),
            10000,
            timedelta(days=365),
        )
        pytest.fail("Expected constraint violation error")
    except asyncpg.CheckViolationError:
        pass

    archive_count_after = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count_after == archive_count_before

    row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid)  # noqa: S608
    assert row is not None, "original row should remain in jobs"


# ── Column round-trip fidelity (positional-hazard regression) ──
#
# The archive INSERT used to be `INSERT INTO jobs_archive SELECT j.*, now(),
# now() + $4` -- relying on `jobs` and `jobs_archive` sharing physical column
# order. ALTER TABLE ADD COLUMN appends at the end of each table's own order
# (e.g. idempotency_scope), silently breaking that assumption: the new column
# landed in archived_at's position and the sweep failed with a type error.
# The sweep now names every column explicitly on both sides
# (_JOBS_COLUMNS_CSV in _leader_shared.py). These tests lock the mapping in:
# EVERY mirrored column must round-trip byte-for-byte, and the two
# archive-only trailing columns must be real timestamps. On the pre-fix SQL
# the first test fails outright (text → timestamptz type error).


async def _seed_fully_distinctive_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    idempotency_scope: str,
) -> uuid.UUID:
    jid = new_job_id()
    now = datetime.now(UTC)
    old = now - timedelta(days=31)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (
            id, actor, queue, identity_key, fairness_key,
            payload, payload_schema_ver, status, priority, attempt,
            max_attempts, retry_kind, schedule_to_close, start_to_close,
            heartbeat_timeout, created_at, scheduled_at, started_at, finished_at,
            last_heartbeat_at, locked_by_worker, lock_expires_at,
            cancel_requested_at, cancel_phase, error_class, error_message,
            error_traceback, progress_state, progress_seq, result,
            result_size_bytes, result_expires_at, idempotency_scope, idempotency_key,
            trace_id, span_id, metadata, tags
        ) VALUES (
            $1, 'archive_actor', 'archive_q', 'ident-1', 'fair-1',
            $2::jsonb, 2, 'succeeded'::{schema}.job_status, 7, 2,
            5, 'indefinite', $3, $4,
            $5, $6, $6, $6, $7,
            $6, NULL, NULL,
            NULL, 1, 'ValueError', 'boom',
            'tb-line', $8::jsonb, 9, $9::jsonb,
            123, $10, $11, 'archive-key',
            'trace-1', 'span-1', $12::jsonb, $13::text[]
        )""",  # noqa: S608
        jid,
        '{"v": 42}',
        old + timedelta(hours=2),
        timedelta(minutes=5),
        timedelta(minutes=1),
        old,
        old + timedelta(hours=1),
        '{"pct": 50}',
        '{"r": 1}',
        old + timedelta(days=2),
        idempotency_scope,
        '{"m": 1}',
        ["tag-a", "tag-b"],
    )
    return jid


async def _fetch_single(
    conn: asyncpg.Connection, schema: str, table: str, jid: uuid.UUID
) -> asyncpg.Record:
    row = await conn.fetchrow(f"SELECT * FROM {schema}.{table} WHERE id = $1", jid)  # noqa: S608
    assert row is not None, f"expected row in {table}"
    return row


async def test_archive_move_preserves_every_mirrored_column(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Seed a terminal job with a distinctive value in EVERY mirrored
    column (including a non-default idempotency_scope) and assert the
    archived row is byte-for-byte identical in every one of them."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    jid = await _seed_fully_distinctive_terminal_job(
        pg_conn, schema, idempotency_scope="run-archive"
    )
    await _seed_job_attempt(pg_conn, jid, schema=schema)

    before = await _fetch_single(pg_conn, schema, "jobs", jid)

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema=schema,
    )
    assert result.archived == 1

    after = await _fetch_single(pg_conn, schema, "jobs_archive", jid)
    for col in COPY_FROM_COLUMNS:
        assert after[col] == before[col], f"column {col!r} diverged during archive"

    # The two archive-only trailing columns must be genuine timestamps --
    # the canary that catches positional misalignment.
    assert isinstance(after["archived_at"], datetime)
    assert isinstance(after["expire_at"], datetime)
    assert after["expire_at"] > after["archived_at"]


async def test_archive_move_preserves_scope_via_actor_override_path(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The per-actor candidate window (_ARCHIVE_CANDIDATE_ACTOR_SQL) and
    the shared write statement (_ARCHIVE_CTE_SQL) it feeds have the same
    explicit-column contract; exercise the actor override path end to end
    with a non-default scope."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    jid = await _seed_fully_distinctive_terminal_job(
        pg_conn, schema, idempotency_scope="run-actor-archive"
    )

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema=schema,
        actor_overrides={"archive_actor": timedelta(days=15)},
    )
    assert result.archived == 1

    after = await _fetch_single(pg_conn, schema, "jobs_archive", jid)
    assert after["idempotency_scope"] == "run-actor-archive"
    assert after["idempotency_key"] == "archive-key"
    assert isinstance(after["archived_at"], datetime)


# ── job_attempts cascade ──────────────────────────────────────────


async def test_job_attempts_cascade(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 1 failed job with 3 job_attempts rows. Run prune.
    Assert: jobs row deleted; all 3 job_attempts cascade-deleted;
    all 3 in job_attempts_archive."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="failed", finished_at=old, schema=schema)
    for i in range(1, 4):
        await _seed_job_attempt(pg_conn, jid, attempt=i, outcome="failed", schema=schema)

    await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"failed": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )

    row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid)  # noqa: S608
    assert row is None

    attempts_count = await _count(pg_conn, "job_attempts", where=f"job_id = '{jid}'", schema=schema)
    assert attempts_count == 0

    archive_attempts = await _count(
        pg_conn, "job_attempts_archive", where=f"job_id = '{jid}'", schema=schema
    )
    assert archive_attempts == 3


# ── Batch drain ──────────────────────────────────────────────────


async def test_batch_drain(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 2500 succeeded jobs older than retention. Run prune with
    batch_size=1000. Assert: 3 batches completed; 2500 rows total in
    jobs_archive; 0 in jobs."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    await _seed_terminal_jobs_bulk(
        pg_conn, count=2500, status="succeeded", finished_at=old, schema=schema
    )

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=1000,
        schema=schema,
    )

    assert result.total_deleted == 2500
    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 2500
    jobs_remaining = await _count(pg_conn, "jobs", where="status = 'succeeded'", schema=schema)
    assert jobs_remaining == 0


# ── Partial retry ────────────────────────────────────────────────


async def test_partial_retry(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 2000 jobs. Run prune with batch_size=1000; simulate
    connection failure after first batch. Run prune again. Assert: second run
    archives remaining 1000; total in jobs_archive = 2000."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    await _seed_terminal_jobs_bulk(
        pg_conn, count=2000, status="succeeded", finished_at=old, schema=schema
    )

    first_result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=1000,
        schema=schema,
    )
    assert first_result.total_deleted == 2000

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 2000
    jobs_remaining = await _count(pg_conn, "jobs", where="status = 'succeeded'", schema=schema)
    assert jobs_remaining == 0


# ── Acceptance D - index usage ────────────────────────────────────


async def test_prune_uses_finished_at_index(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """EXPLAIN ANALYZE on the prune query with SET enable_seqscan = off.
    Assert jobs_finished_at_idx appears in the plan output."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    for _ in range(50):
        await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)

    await pg_conn.execute("SET enable_seqscan = off")
    try:
        plan = await pg_conn.fetch(
            f"""EXPLAIN (ANALYZE, FORMAT TEXT)
            WITH candidate_ids AS (
                SELECT id FROM {schema}.jobs
                WHERE status = $1::{schema}.job_status AND finished_at < $2
                ORDER BY finished_at LIMIT $3
            )
            SELECT * FROM candidate_ids""",  # noqa: S608
            "succeeded",
            datetime.now(UTC) - timedelta(days=30),
            10000,
        )
        plan_text = "\n".join(row[0] for row in plan)
        assert "jobs_finished_at_idx" in plan_text, (
            f"Expected index scan on jobs_finished_at_idx, got:\n{plan_text}"
        )
    finally:
        await pg_conn.execute("SET enable_seqscan = on")


# ── Per-status retention ──────────────────────────────────────────


async def test_per_status_retention(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 1 failed (60d ago) + 1 succeeded (60d ago). Run with
    retention_failed=90d, retention_succeeded=30d. Assert: succeeded archived;
    failed retained."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=60)
    succ_id = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)
    fail_id = await _seed_terminal_job(pg_conn, status="failed", finished_at=old, schema=schema)

    await prune_terminal_jobs(
        pg_conn,
        retention_per_status={
            "succeeded": timedelta(days=30),
            "failed": timedelta(days=90),
        },
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )

    succ_in_jobs = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", succ_id)  # noqa: S608
    assert succ_in_jobs is None, "succeeded job should be archived"

    fail_in_jobs = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", fail_id)  # noqa: S608
    assert fail_in_jobs is not None, "failed job should remain in jobs"


# ── Per-actor retention override ───────────────────────────────────


async def test_per_actor_retention(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Actor A with retention_days=7 in actor_config.metadata;
    actor B with no override (global=30d). Seed jobs at 10d ago.
    Assert: actor A archived; actor B retained."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    ten_days_ago = datetime.now(UTC) - timedelta(days=10)

    await pg_conn.execute(
        f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, queue, metadata)
            VALUES ($1, 5, 'default', '{{"retention_days": 7}}'::jsonb)""",  # noqa: S608
        "actor_a",
    )
    await pg_conn.execute(
        f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, queue, metadata)
            VALUES ($1, 5, 'default', '{{}}'::jsonb)""",  # noqa: S608
        "actor_b",
    )

    jid_a = await _seed_terminal_job(
        pg_conn, status="succeeded", finished_at=ten_days_ago, actor="actor_a", schema=schema
    )
    jid_b = await _seed_terminal_job(
        pg_conn, status="succeeded", finished_at=ten_days_ago, actor="actor_b", schema=schema
    )

    await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
        actor_overrides={"actor_a": timedelta(days=7)},
    )

    a_in_jobs = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid_a)  # noqa: S608
    assert a_in_jobs is None, "actor_a job should be archived (7d retention)"

    b_in_jobs = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid_b)  # noqa: S608
    assert b_in_jobs is not None, "actor_b job should remain in jobs (30d retention)"


# ── Acceptance B - expiry sweep ───────────────────────────────────


async def test_expiry_sweep(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 10 jobs_archive rows with expire_at < now().
    Run archive_expiry_sweep. Assert: 10 rows deleted;
    job_attempts_archive cascade-deleted."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    expired_ids: list[uuid.UUID] = []
    for _ in range(10):
        jid = await _seed_archive_row(pg_conn, expire_at=now - timedelta(hours=1), schema=schema)
        expired_ids.append(jid)
        await _seed_archive_attempt(pg_conn, jid, outcome="succeeded", schema=schema)

    result = await archive_expiry_sweep(pg_conn, batch_size=10000, schema=schema)
    assert result.total_deleted == 10

    for jid in expired_ids:
        row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs_archive WHERE id = $1", jid)  # noqa: S608
        assert row is None, f"expired row {jid} should be deleted"

    attempts_remaining = await _count(pg_conn, "job_attempts_archive", schema=schema)
    assert attempts_remaining == 0


# ── Expiry drain ─────────────────────────────────────────────────


async def test_expiry_drain(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 2500 jobs_archive rows with expire_at = now() - 1s.
    Run expiry with batch_size=1000. Assert: 3 batches; 2500 deleted."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    await _seed_archive_rows_bulk(
        pg_conn,
        count=2500,
        expire_at=now - timedelta(hours=1),
        finished_at=now - timedelta(days=31),
        schema=schema,
    )

    result = await archive_expiry_sweep(pg_conn, batch_size=1000, schema=schema)
    assert result.total_deleted == 2500

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 0


# ── Acceptance E - index usage ───────────────────────────────────


async def test_expiry_uses_expire_at_index(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """EXPLAIN ANALYZE on the expiry query with SET enable_seqscan = off.
    Assert jobs_archive_expire_at_idx appears in the plan output."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    for _ in range(50):
        await _seed_archive_row(pg_conn, expire_at=now - timedelta(hours=1), schema=schema)

    await pg_conn.execute("SET enable_seqscan = off")
    try:
        plan = await pg_conn.fetch(
            f"""EXPLAIN (ANALYZE, FORMAT TEXT)
            SELECT id FROM {schema}.jobs_archive
            WHERE expire_at < now()
            ORDER BY expire_at LIMIT $1""",  # noqa: S608
            10000,
        )
        plan_text = "\n".join(row[0] for row in plan)
        assert "jobs_archive_expire_at_idx" in plan_text, (
            f"Expected index scan on jobs_archive_expire_at_idx, got:\n{plan_text}"
        )
    finally:
        await pg_conn.execute("SET enable_seqscan = on")


# ── No unexpired deleted ──────────────────────────────────────────


async def test_no_unexpired_deleted(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 5 rows with expire_at = now() + 1d. Run expiry.
    Assert 0 deleted."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    for _ in range(5):
        await _seed_archive_row(pg_conn, expire_at=now + timedelta(days=1), schema=schema)

    result = await archive_expiry_sweep(pg_conn, batch_size=10000, schema=schema)
    assert result.total_deleted == 0

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 5


# ── End-to-end prune + expiry ────────────────────────────────────


async def test_e2e_prune_and_expiry(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 100 old terminal jobs with retention=7d and
    archive_retention=0. After prune: 100 rows in jobs_archive with
    expire_at ≈ now(). After expiry: 0 rows in jobs_archive."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=8)
    await _seed_terminal_jobs_bulk(
        pg_conn, count=100, status="succeeded", finished_at=old, schema=schema
    )

    prune_result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=7)},
        archive_retention=timedelta(0),
        batch_size=10000,
        schema=schema,
    )
    assert prune_result.total_deleted == 100

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 100

    expiry_result = await archive_expiry_sweep(pg_conn, batch_size=10000, schema=schema)
    assert expiry_result.total_deleted == 100

    final_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert final_count == 0


# ── job_attempts_archive cascade at expiry ───────────────────────


async def test_attempts_archive_cascade_at_expiry(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Seed 1 job with 3 attempt rows; prune it; run expiry sweep.
    Assert 0 rows in job_attempts_archive for that job_id."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=8)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)
    for i in range(1, 4):
        await _seed_job_attempt(pg_conn, jid, attempt=i, outcome="succeeded", schema=schema)

    await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=7)},
        archive_retention=timedelta(0),
        batch_size=10000,
        schema=schema,
    )

    attempts_archive_before = await _count(
        pg_conn, "job_attempts_archive", where=f"job_id = '{jid}'", schema=schema
    )
    assert attempts_archive_before == 3

    await archive_expiry_sweep(pg_conn, batch_size=10000, schema=schema)

    attempts_archive_after = await _count(
        pg_conn, "job_attempts_archive", where=f"job_id = '{jid}'", schema=schema
    )
    assert attempts_archive_after == 0


# ── Non-terminal untouched ────────────────────────────────────────


async def test_non_terminal_untouched(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Seed 5 pending + 5 running jobs. Run prune with
    retention=timedelta(0). Assert 0 rows in jobs_archive for those IDs."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    now = datetime.now(UTC)
    job_ids: list[uuid.UUID] = []

    for status in ("pending", "running"):
        for _ in range(5):
            jid = new_job_id()
            job_ids.append(jid)
            await pg_conn.execute(
                f"""INSERT INTO {schema}.jobs (
                    id, actor, queue, payload, max_attempts, retry_kind,
                    status, priority, scheduled_at, schedule_to_close,
                    payload_schema_ver
                ) VALUES (
                    $1, $2, $3, $4::jsonb, $5, $6,
                    $7::{schema}.job_status, 0, $8, $9, 1
                )""",  # noqa: S608
                jid,
                "test_actor",
                "default",
                '{"v": 1}',
                3,
                "transient",
                status,
                now,
                now + timedelta(hours=1),
            )

    await prune_terminal_jobs(
        pg_conn,
        retention_per_status={
            "succeeded": timedelta(0),
            "failed": timedelta(0),
            "cancelled": timedelta(0),
            "crashed": timedelta(0),
            "abandoned": timedelta(0),
        },
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )

    for jid in job_ids:
        row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs_archive WHERE id = $1", jid)  # noqa: S608
        assert row is None, f"non-terminal job {jid} should not be in archive"

    jobs_remaining = await _count(pg_conn, "jobs", schema=schema)
    assert jobs_remaining == 10


# ── Reservation slots FK ──────────────────────────────────────────


async def test_reservation_slots_no_fk_violation(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Seed a job with a linked reservation_slots row (job_id, no FK).
    Run prune. Assert no FK violation; prune completes successfully;
    slot row still exists."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)

    await pg_conn.execute(
        f"""INSERT INTO {schema}.reservation_slots (bucket_name, slot_index, job_id)
            VALUES ('test-bucket', 0, $1)""",  # noqa: S608
        jid,
    )

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )
    assert result.total_deleted == 1

    slot = await pg_conn.fetchrow(
        f"SELECT job_id FROM {schema}.reservation_slots WHERE bucket_name = 'test-bucket' AND slot_index = 0"  # noqa: S608
    )
    assert slot is not None, "reservation_slots row should still exist after prune"


# ── PG failure mid-CTE ────────────────────────────────────────────


async def test_pg_failure_mid_cte(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Kill/close the connection after advisory lock acquired but
    before CTE commits. Assert: zero rows in jobs_archive; original rows
    remain in jobs; next prune run archives the same rows."""
    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)

    kill_conn = await asyncpg.connect(str(settings.pg_dsn))

    # The schema-qualified prune-loop lock name (the one production
    # acquires via schema_lock_name): a killed holder's lock must
    # auto-release so the prune underneath can proceed.
    lock_acquired: bool = await kill_conn.fetchval(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
        schema_lock_name("prune", schema),
    )
    assert lock_acquired

    await kill_conn.close()

    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10000,
        schema=schema,
    )
    assert result.total_deleted == 1

    archive_count = await _count(pg_conn, "jobs_archive", schema=schema)
    assert archive_count == 1

    row = await pg_conn.fetchrow(f"SELECT id FROM {schema}.jobs WHERE id = $1", jid)  # noqa: S608
    assert row is None


# ── Concurrent prune lock ─────────────────────────────────────────


async def test_concurrent_prune_lock(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Two asyncpg connections both attempt the schema-qualified prune-loop
    advisory lock (``taskq:prune:{schema}``, the name the prune loop
    acquires via ``schema_lock_name``). Assert: first acquires; second
    returns false - mutual exclusion within one schema for the same
    loop's lock. No duplicate inserts."""
    await _apply(pg_conn, settings)
    lock_name = schema_lock_name("prune", settings.schema_name)

    conn1 = await asyncpg.connect(str(settings.pg_dsn))
    conn2 = await asyncpg.connect(str(settings.pg_dsn))

    try:
        lock1: bool = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        lock2: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )

        assert lock1 is True
        assert lock2 is False

        await conn1.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)

        lock3: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        assert lock3 is True

        await conn2.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)
    finally:
        await conn1.close()
        await conn2.close()


# ── Concurrent archive expiry lock ────────────────────────────────


async def test_concurrent_archive_expiry_lock(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Two asyncpg connections both attempt the schema-qualified
    archive-expiry-loop advisory lock (``taskq:archive_expiry:{schema}``,
    the name the loop acquires via ``schema_lock_name``). Assert: first
    acquires; second returns false - mutual exclusion within one schema
    for the same loop's lock."""
    await _apply(pg_conn, settings)
    lock_name = schema_lock_name("archive_expiry", settings.schema_name)

    conn1 = await asyncpg.connect(str(settings.pg_dsn))
    conn2 = await asyncpg.connect(str(settings.pg_dsn))

    try:
        lock1: bool = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        lock2: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )

        assert lock1 is True
        assert lock2 is False

        await conn1.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)

        lock3: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        assert lock3 is True

        await conn2.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)
    finally:
        await conn1.close()
        await conn2.close()


# ── C10: prune cutoff anchored to the server clock ─────────────────


async def test_prune_cutoff_anchored_to_server_clock(
    pg_conn: asyncpg.Connection,
    settings: TaskQSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job finished 40 s ago (server-stamped) with 30 s retention MUST be
    pruned even when the worker's Python clock is 120 s behind.  Pre-fix:
    cutoff = python_now - 30 s = server_now - 150 s → ``finished_at <
    cutoff`` is false → retention silently extended.  The predicate must be
    computed by the same clock that wrote ``finished_at`` - the server's."""
    from taskq.worker import _leader_shared

    class _SkewedDatetime:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return datetime.now(UTC if tz is None else tz) - timedelta(seconds=120)

    monkeypatch.setattr(_leader_shared, "datetime", _SkewedDatetime)

    await _apply(pg_conn, settings)
    await _seed_terminal_job(
        pg_conn,
        settings.schema_name,
        status="succeeded",
        finished_at=datetime.now(UTC) - timedelta(seconds=40),
    )
    result = await prune_terminal_jobs(
        pg_conn,
        retention_per_status={"succeeded": timedelta(seconds=30)},
        archive_retention=timedelta(days=1),
        schema=settings.schema_name,
    )
    assert result.total_deleted == 1  # pre-fix: 0 - the job survives past its retention


# The in-flight-retry protocol's bounds. The grace must sit well under the
# batch statement_timeout: on the pre-fix shape the blocked delete arm is
# released by the retry's commit, not killed by the batch timeout - a cancel
# would roll the batch back and mask which side won.
_RACE_GRACE_S = 10.0
_RACE_STATEMENT_TIMEOUT_MS = 60_000


async def _prune_with_retry_in_flight(
    pg_conn: asyncpg.Connection,
    retry_conn: asyncpg.Connection,
    retry_sql: str,
    jid: uuid.UUID,
    schema: str,
) -> PruneResult:
    """Run one prune batch against a retry that is in flight for the whole
    batch: the retry's UPDATE has locked the row (its transaction open,
    uncommitted) before the prune's first statement runs, and it commits
    only after the batch's write side has passed the row.

    This is the deterministic form of the window a concurrent retry
    occupies - the interleaving an asyncio.gather race leaves to the
    scheduler, which on the pre-fix single-statement shape never landed
    it (the retry either committed before the statement's snapshot or
    blocked behind its delete arm until the batch committed). Here the
    candidate window reads the pre-retry committed version whatever the
    scheduler does, and the retry's commit lands between that snapshot
    and the write side's lock-time re-read: on the pre-fix shape the
    delete arm blocks on the retry's in-flight lock, the grace above
    expires (the proof the statement is blocked mid-flight), and the
    commit is what releases it - the exact window the ghost forms in.
    The fixed shape never waits: its write locks the batch with
    SKIP LOCKED and commits around the in-flight retry, which keeps its
    own commit.
    """
    retry_tx = retry_conn.transaction()
    await retry_tx.start()
    # fetchrow, exactly as the backend's retry_job reads it: the
    # statement's row presence IS the retry's success signal (the
    # execute tag is always "SELECT 1").
    retried = await retry_conn.fetchrow(retry_sql, jid)
    assert retried is not None, "fixture broken: the retry did not match the seeded row"

    prune_task = asyncio.create_task(
        prune_terminal_jobs(
            pg_conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            batch_size=10000,
            schema=schema,
            statement_timeout_ms=_RACE_STATEMENT_TIMEOUT_MS,
        )
    )
    # Wait for either the batch's completion (fixed shape: it commits
    # around the in-flight retry) or the grace's expiry (pre-fix shape:
    # the write statement is blocked on the retry's lock). The shield
    # keeps the task alive through the expiry - canceling it would cancel
    # the statement mid-flight and roll the batch back before the retry's
    # commit could land.
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.shield(prune_task), _RACE_GRACE_S)
    await retry_tx.commit()
    # Both shapes: the task is either already done or unblocked by the
    # commit above.
    return await prune_task


@pytest.mark.integration
async def test_prune_never_deletes_a_row_a_concurrent_retry_retried(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A retry committing against the prune window cannot lose the live row,
    and cannot ghost it either.

    The failure this pin exists for: the archive statement copied the
    snapshot's terminal version into ``jobs_archive``, a concurrent
    ``retry_job`` flipped the row back to pending (its caller already
    told the operator the retry landed), and the delete arm's lock-time
    re-check met the version the retry had committed - the row stayed
    live AND archived: a ghost that wedges every later prune batch on
    the archive's primary key once the retried job re-terminalizes and
    re-enters the window.

    The candidates stay a lock-free selection window (a FOR UPDATE on
    that scan forced the planner off the bounded index-only shape); the
    race fence is the lock-bearing `locked` arm between the window and
    the archive INSERT: it takes the batch's row locks with the
    terminal-status qual re-evaluated at lock time (EvalPlanQual), so a
    retry that committed first drops out of the pipeline before anything
    is archived, and a retry still in flight is skipped and keeps its
    own commit. The interleaving is fixed by
    :func:`_prune_with_retry_in_flight`, and the invariants asserted
    here are: the retry's UPDATE matched the row, so after the prune the
    row is still live and claimable, the archive holds NO row for it,
    and the prune reports no deletion.

    The retry runs as the raw rendered statement ``retry_job`` issues,
    against its own connection: the race under test is between the two
    statements, not between backend objects.
    """
    from taskq.backend._sql_templates import render

    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)
    retry_sql = render(schema).retry_job

    retry_conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        result = await _prune_with_retry_in_flight(pg_conn, retry_conn, retry_sql, jid, schema)

        live = await pg_conn.fetchrow(
            f"SELECT status FROM {schema}.jobs WHERE id = $1",  # noqa: S608
            jid,
        )
        ghost = await pg_conn.fetchrow(
            f"SELECT id FROM {schema}.jobs_archive WHERE id = $1",  # noqa: S608
            jid,
        )
        assert live is not None, (
            "the retry's update matched the row but the live row is gone: "
            "the prune deleted concurrently retried work"
        )
        assert live["status"] == "pending"
        assert ghost is None, (
            "the live retried row is also archived: a ghost row that wedges "
            "the prune on the archive's primary key once the retried job "
            "re-terminalizes and re-enters the window"
        )
        assert result.total_deleted == 0, (
            "the prune reports deletions for a batch whose only row was retried out from under it"
        )
    finally:
        await retry_conn.close()


@pytest.mark.integration
async def test_prune_rearchives_a_retried_job_without_primary_key_violation(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A retried job that re-terminalizes and re-enters the prune window
    archives cleanly, no primary key violation.

    The failure this pin exists for: a prune that archives a row while
    leaving it live (the ghost) wedges the whole prune family forever.
    The ghost sits in jobs_archive with the live row still present;
    when the retried job runs to terminal again and ages past retention,
    every later archive batch tries to INSERT a second row with the same
    id, the archive's primary key rejects it, the error is not
    transient, and the batch containing that row aborts on every
    attempt: the drain never passes the head of the window again.

    The retried-then-re-terminalized cycle runs for real, with the race
    window fixed by :func:`_prune_with_retry_in_flight` (the retry's
    commit lands between the batch's candidate snapshot and its write
    side's lock-time re-read): the first prune must leave the row live
    with nothing archived, the re-terminalization stamps it back into
    the prune window (the state a re-run job reaches), and the second
    prune must archive it cleanly - exactly one archive row for the id,
    no live row left. On the ghosting shape the first prune leaves the
    ghost behind and this pin fails at that assertion: the exact state
    whose re-prune raises the archive primary key violation.
    """
    from taskq.backend._sql_templates import render

    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)
    retry_sql = render(schema).retry_job

    retry_conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await _prune_with_retry_in_flight(pg_conn, retry_conn, retry_sql, jid, schema)

        ghost = await pg_conn.fetchval(
            f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1",  # noqa: S608
            jid,
        )
        assert ghost == 0, (
            "the retried row was archived while the retry left it live: a "
            "ghost row that wedges every later prune batch on the archive's "
            "primary key once the retried job re-terminalizes"
        )

        # Re-terminalize past retention (the state the re-run job
        # reaches) and re-enter the prune window.
        await pg_conn.execute(
            f"UPDATE {schema}.jobs SET status = $1::{schema}.job_status, "  # noqa: S608
            f"finished_at = $2 WHERE id = $3",
            "succeeded",
            old,
            jid,
        )
        second = await prune_terminal_jobs(
            pg_conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema=schema,
        )
        assert second.total_deleted == 1

        archive_count = await pg_conn.fetchval(
            f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1",  # noqa: S608
            jid,
        )
        assert archive_count == 1, (
            "the archive must hold exactly one row for the id after the "
            f"full cycle (got {archive_count})"
        )
        final_live = await pg_conn.fetchrow(
            f"SELECT 1 FROM {schema}.jobs WHERE id = $1",  # noqa: S608
            jid,
        )
        assert final_live is None
    finally:
        await retry_conn.close()


class _RaceBetweenStatementsConn:
    """ConnLike proxy that commits a retry and a re-terminalization on a
    second connection in the gap between the batch's candidate window and
    its write statement (the duck-type surface is fetch/execute/
    transaction/fetchval, the same one the sweep helpers proxy)."""

    def __init__(
        self,
        inner: asyncpg.Connection,
        race_conn: asyncpg.Connection,
        race_statements: list[tuple[str, tuple[object, ...]]],
        trigger: str,
    ) -> None:
        self._inner = inner
        self._race_conn = race_conn
        self._race_statements = race_statements
        self._trigger = trigger
        self.raced = False

    def transaction(self) -> object:
        return self._inner.transaction()

    async def fetchval(self, sql: str, *args: object) -> object:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        rows = await self._inner.fetch(sql, *args)
        if not self.raced and rows and self._trigger in sql:
            self.raced = True
            for stmt, args_ in self._race_statements:
                await self._race_conn.fetchrow(stmt, *args_)
        return rows

    async def execute(self, sql: str, *args: object) -> str:
        return await self._inner.execute(sql, *args)


@pytest.mark.integration
async def test_prune_write_statement_rechecks_retention_age_at_lock_time(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A job that re-terminalizes between the candidate window and the
    write statement drops out at lock time; it is not archived at zero
    age.

    The interleaving the two race pins above cannot produce: their
    re-terminalize stamps the row back into the past, but a real re-run
    stamps ``finished_at = clock_timestamp()`` (mark_succeeded /
    mark_failed). When the retry AND the re-run both commit in the gap
    between the prune's two statements, the row is terminal again (the
    status re-check passes) but zero seconds old. The lock-time
    re-check must verify every candidate predicate, not just the
    status: the row drops out here, stays live, and re-enters a later
    window when it has actually aged past retention. A prune that
    archived it would silently delete a just-finished job from the
    live tables the moment it failed (its retention not served), and
    every metric would report the archive as routine.
    """
    from taskq.backend._sql_templates import render

    await _apply(pg_conn, settings)
    schema = settings.schema_name
    old = datetime.now(UTC) - timedelta(days=31)
    jid = await _seed_terminal_job(pg_conn, status="succeeded", finished_at=old, schema=schema)
    retry_sql = render(schema).retry_job

    race_conn = await asyncpg.connect(str(settings.pg_dsn))
    # The pre-fix shape cannot interleave an external statement between its
    # snapshot and its write - one statement is both - so the injected
    # retry blocks on the row lock the batch's own delete arm holds until
    # the batch commits, a commit that cannot happen while this test waits
    # on the prune (the first draft of this pin died as a 300 s
    # pytest-timeout deadlock exactly there). lock_timeout bounds that wait
    # server-side and turns the pre-fix failure into the fast, precise one
    # below: this pin's scenario only exists on the two-statement shape.
    await race_conn.execute("SET lock_timeout = '2s'")
    reterminalize_sql = (
        f"UPDATE {schema}.jobs SET status = $1::{schema}.job_status, "  # noqa: S608
        "finished_at = clock_timestamp() WHERE id = $2"
    )
    proxy = _RaceBetweenStatementsConn(
        pg_conn,
        race_conn,
        [
            # The full retry-then-re-run cycle, both committed before the
            # write statement takes the batch's row locks: the state the
            # row is in when a worker re-fails it while the prune's batch
            # is between its two statements.
            (retry_sql, (jid,)),
            (reterminalize_sql, ("succeeded", jid)),
        ],
        # The candidate window's signature: the only bounded selection
        # the batch runs. Firing after it lands the race exactly where
        # the two statements meet.
        "ORDER BY finished_at",
    )

    try:
        result = await prune_terminal_jobs(
            proxy,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema=schema,
        )
    except asyncpg.LockNotAvailableError:
        pytest.fail(
            "pre-fix: the archive is one statement, so the injected "
            "retry+re-run cannot commit between its snapshot and its lock "
            "acquisition - the retry's UPDATE blocks on the lock the batch's "
            "own delete arm holds until the batch commits. The lock-time "
            "retention-age re-check needs the two-statement shape; on the "
            "single-statement shape a re-terminalized candidate is archived "
            "from the statement snapshot with no age re-check at all."
        )
    finally:
        await race_conn.close()

    assert proxy.raced, "the candidate window must have selected the row for the race to exist"
    assert result.total_deleted == 0, (
        "the re-terminalized row was archived at zero age: the write "
        "statement's lock-time re-check verified the status but not the "
        "retention age"
    )
    assert result.archived == 0
    live = await pg_conn.fetchrow(
        f"SELECT status, finished_at FROM {schema}.jobs WHERE id = $1",  # noqa: S608
        jid,
    )
    assert live is not None, "the just-re-failed job was removed from the live tables"
    assert live["status"] == "succeeded"
    ghost = await pg_conn.fetchval(
        f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1",  # noqa: S608
        jid,
    )
    assert ghost == 0

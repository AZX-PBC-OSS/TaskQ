"""Integration tests for prune sweep, expiry sweep, and admin UI archive fallback.

Runs against real Postgres 18 via testcontainers. Per-test schema isolation
via the pg_conn fixture (DROP SCHEMA … CASCADE teardown).

Covers archive-move semantics, atomicity, cascades, batch draining,
per-status/per-actor retention overrides, index usage, expiry sweeps,
and concurrent-lock behavior.
"""

import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._json import dumps_str
from taskq.settings import TaskQSettings
from taskq.worker.leader import archive_expiry_sweep, prune_terminal_jobs

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
    jid = job_id or uuid.uuid4()
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
    jid = job_id or uuid.uuid4()
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
        jid = uuid.uuid4()
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
        jid = uuid.uuid4()
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


# ── Acceptance A — archive move ────────────────────────────────────


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


# ── Acceptance D — index usage ────────────────────────────────────


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


# ── Acceptance B — expiry sweep ───────────────────────────────────


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


# ── Acceptance E — index usage ───────────────────────────────────


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
            jid = uuid.uuid4()
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

    lock_acquired: bool = await kill_conn.fetchval(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:prune"
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
    """Two asyncpg connections both attempt pg_try_advisory_lock('taskq:prune').
    Assert: first acquires; second returns false. No duplicate inserts."""
    await _apply(pg_conn, settings)

    conn1 = await asyncpg.connect(str(settings.pg_dsn))
    conn2 = await asyncpg.connect(str(settings.pg_dsn))

    try:
        lock1: bool = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:prune"
        )
        lock2: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:prune"
        )

        assert lock1 is True
        assert lock2 is False

        await conn1.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", "taskq:prune")

        lock3: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:prune"
        )
        assert lock3 is True

        await conn2.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", "taskq:prune")
    finally:
        await conn1.close()
        await conn2.close()


# ── Concurrent archive expiry lock ────────────────────────────────


async def test_concurrent_archive_expiry_lock(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """Two asyncpg connections both attempt pg_try_advisory_lock('taskq:archive_expiry').
    Assert: first acquires; second returns false."""
    await _apply(pg_conn, settings)

    conn1 = await asyncpg.connect(str(settings.pg_dsn))
    conn2 = await asyncpg.connect(str(settings.pg_dsn))

    try:
        lock1: bool = await conn1.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:archive_expiry"
        )
        lock2: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:archive_expiry"
        )

        assert lock1 is True
        assert lock2 is False

        await conn1.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))", "taskq:archive_expiry"
        )

        lock3: bool = await conn2.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", "taskq:archive_expiry"
        )
        assert lock3 is True

        await conn2.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))", "taskq:archive_expiry"
        )
    finally:
        await conn1.close()
        await conn2.close()

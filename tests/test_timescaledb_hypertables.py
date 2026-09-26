# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Optional TimescaleDB hypertable support: opt-in, conversion, retention
policies, and differential agreement with vanilla Postgres.

Tiers, matching the suite's conventions:

* The flag-off zero-SQL gate is pure (no Docker, no server): one stub
  connection proves that ``TASKQ_TIMESCALEDB_HYPERTABLES=false`` setup
  issues ZERO statements.
* The loud-refusal leg runs against the shared vanilla Postgres container
  (the ``pg_dsn`` fixture): opting in where the extension does not exist
  must raise :class:`TimescaleDBUnavailableError` naming the setting,
  never silently degrade.
* The conversion, retention-policy, and differential legs run against a
  ``timescaledb`` container. The image is
  ``TASKQ_TEST_TIMESCALEDB_IMAGE`` when set, else the newest tagged
  release matching the suite's Postgres 18 pin
  (``timescale/timescaledb:2.30.1-pg18``). Every container fixture calls
  :func:`skip_test_without_docker` first, so a Docker-less machine sees
  skips with reasons, never errors.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.timescale import (
    HypertableReport,
    TimescaleDBUnavailableError,
    enable_hypertables,
)
from taskq.worker.leader import archive_expiry_sweep, prune_terminal_jobs

pytestmark = pytest.mark.integration

#: Chunk interval the short test retentions clamp to (retention/4 clamped
#: to a 1-day floor): every test leg below asserts against it.
_TEST_CHUNK_INTERVAL = timedelta(days=1)
_TEST_ARCHIVE_RETENTION = timedelta(days=2)
_TEST_EVENT_RETENTION = timedelta(days=1)

_TIMESCALE_IMAGE_DEFAULT = "timescale/timescaledb:2.30.1-pg18"
_AGED = datetime.now(UTC) - timedelta(days=31)


# ── The flag-off gate: pure, no server ───────────────────────────────────


class _RefusingConn:
    """A connection stub that fails the test if ANY statement reaches it."""

    def __getattr__(self, name: str) -> Any:
        def _no_sql(*args: object, **kwargs: object) -> None:
            raise AssertionError(
                f"conn.{name} was called with {args!r}: the flag-off setup "
                "path must issue ZERO statements against the server"
            )

        return _no_sql


async def test_flag_off_setup_issues_zero_statements() -> None:
    """``TASKQ_TIMESCALEDB_HYPERTABLES=false`` (the default) is a pure gate.

    A vanilla deployment's setup must not probe extensions, query
    catalogs, or run any DDL: the flag-off path is proven here by passing
    a connection that raises on first use.
    """
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_TIMESCALEDB_HYPERTABLES": "false",
        }
    )
    report = await enable_hypertables(
        _RefusingConn(),  # pyright: ignore[reportArgumentType]  # Why: the contract under test is that this value is never used.
        schema="taskq",
        settings=settings,
    )
    assert report == HypertableReport(converted=(), retention_policies=())


# ── Loud refusal on a vanilla server ─────────────────────────────────────


async def _migrate(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)


async def test_opt_in_on_vanilla_postgres_fails_loudly(pg_dsn: str) -> None:
    """Flag on, extension absent: a precise refusal, never a silent degrade.

    The error must name the setting that asked for the feature and what
    the server is missing, so an operator on a plain Postgres (or an
    Azure Flexible Server without the extension allow-listed) can act on
    the message alone.
    """
    schema = "ts_loud_vanilla"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _migrate(conn, schema)
        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            }
        )
        with pytest.raises(TimescaleDBUnavailableError, match="TASKQ_TIMESCALEDB_HYPERTABLES"):
            await enable_hypertables(conn, schema=schema, settings=settings)
        # The refusal converts nothing: the schema stays vanilla and
        # usable (migrations applied, tables intact).
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 ORDER BY table_name",
            schema,
        )
        assert {"jobs", "jobs_archive", "job_events"} <= {r["table_name"] for r in tables}
        pkey = await conn.fetchval(
            "SELECT count(*) FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = 'jobs_archive' AND con.contype = 'p'",
            schema,
        )
        assert pkey == 1, "the vanilla archive must keep its primary key"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── Timescale container fixtures ─────────────────────────────────────────

_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT


@pytest.fixture(scope="module")
def timescale_container() -> Iterator[Any]:
    """One timescaledb container per module; skips without Docker.

    The image is ``TASKQ_TEST_TIMESCALEDB_IMAGE`` when set (CI and local
    environments can point at their own tag), else the newest tagged
    release matching the suite's Postgres 18 pin. Labeled with the
    ownership labels so a crashed run's leftover is sweepable.
    """
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
def timescale_dsn(timescale_container: Any) -> str:
    """The module container's DSN in asyncpg form."""
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


@pytest.fixture
def ts_schema() -> str:
    """A unique schema name per test (the test body creates and drops it)."""
    return "ts_" + new_uuid().hex[:12]


@pytest.fixture
async def ts_conn(
    timescale_dsn: str,
    ts_schema: str,
) -> AsyncIterator[asyncpg.Connection]:
    """A connection to a freshly migrated AND converted schema.

    Retentions are short so the retention-policy legs can run in test
    time: the archive family at 2 days, events at 1 day, chunk interval
    clamped to its 1-day floor.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        settings = _ts_settings(timescale_dsn, ts_schema)
        report = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert set(report.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
        # add_retention_policy's default next_start is ~now, so a policy's
        # first background run can land at any moment - including between a
        # test's seeds and its pre-policy assertions, which would make
        # "chunks before the policy runs" a race (seen on CI: the aged
        # event chunk already dropped before the explicit force). Defer
        # every policy to the far future here; the legs that need a real
        # policy run pull next_start back to now themselves
        # (_force_policies_now), so what they assert stays the policy's
        # own run, triggered on the test's clock, not the scheduler's.
        await _schedule_policies(
            conn, ts_schema, next_start=datetime.now(UTC) + timedelta(days=3650)
        )
        yield conn
    finally:
        # Schema names are per-test unique and the module container is
        # removed at teardown, so a DROP here would only shorten a
        # container lifetime that is already bounded.
        await conn.close()


def _ts_settings(dsn: str, schema: str) -> WorkerSettings:
    """Worker settings for the timescale legs: flag on, short retentions."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(_TEST_ARCHIVE_RETENTION.total_seconds())}s",
            "TASKQ_EVENT_RETENTION_PERIOD": f"{int(_TEST_EVENT_RETENTION.total_seconds())}s",
        }
    )


# ── Conversion shape ──────────────────────────────────────────────────────


async def test_conversion_shape_and_retention_policies(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """Opt-in converts the three retention tables and registers the policies.

    Observable in the catalogs, not in any TaskQ private state: each table
    is a hypertable on its documented time column with the clamped chunk
    interval; uniqueness is widened to include the partition column (the
    hypertable rule); the ``jobs`` foreign key on job_events survives;
    the archive-attempts foreign key is gone (nothing may reference a
    hypertable) and is replaced by retention policies derived from the
    same archive retention setting.
    """
    ht = await ts_conn.fetch(
        "SELECT h.table_name, d.column_name, d.interval_length "
        "FROM _timescaledb_catalog.hypertable h "
        "JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id "
        "WHERE h.schema_name = $1 ORDER BY h.table_name",
        ts_schema,
    )
    got = {r["table_name"]: (r["column_name"], r["interval_length"]) for r in ht}
    assert got == {
        "job_events": ("occurred_at", _TEST_CHUNK_INTERVAL.total_seconds() * 1_000_000),
        "jobs_archive": ("finished_at", _TEST_CHUNK_INTERVAL.total_seconds() * 1_000_000),
        "job_attempts_archive": ("started_at", _TEST_CHUNK_INTERVAL.total_seconds() * 1_000_000),
    }, got

    cons = await ts_conn.fetch(
        "SELECT c.relname AS tbl, con.conname, con.contype "
        "FROM pg_constraint con "
        "JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = ANY($2) "
        "AND con.contype IN ('p', 'u', 'f')",
        ts_schema,
        ["job_events", "jobs_archive", "job_attempts_archive"],
    )
    by_table: dict[str, set[bytes]] = {}
    for r in cons:
        by_table.setdefault(r["tbl"], set()).add(r["contype"])
    # Uniqueness widened to include the partition column; no bare PK survives.
    names = {(r["tbl"], r["conname"]) for r in cons}
    assert ("job_events", "job_events_id_occurred_at_uniq") in names
    assert ("jobs_archive", "jobs_archive_id_finished_at_uniq") in names
    assert ("job_attempts_archive", "job_attempts_archive_job_attempt_started_at_uniq") in names
    assert all("p" not in types for types in by_table.values()), by_table
    # job_events keeps its FK to jobs (hypertable-to-regular is supported,
    # and ON DELETE CASCADE must keep reaching events).
    assert ("job_events", "job_events_job_id_fkey") in names
    # No table may reference the hypertable archive: the attempts FK drops.
    assert ("job_attempts_archive", "job_attempts_archive_job_id_fkey") not in names

    policies = await ts_conn.fetch(
        "SELECT hypertable_name, proc_name, config "
        "FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        ts_schema,
    )
    by_hypertable = {r["hypertable_name"]: r for r in policies}
    assert set(by_hypertable) == {"job_events", "jobs_archive", "job_attempts_archive"}
    assert all(r["proc_name"] == "policy_retention" for r in policies)
    drop_after = {name: r["config"] for name, r in by_hypertable.items()}
    assert '"drop_after": "2 days"' in drop_after["jobs_archive"]
    assert '"drop_after": "2 days"' in drop_after["job_attempts_archive"]
    assert '"drop_after": "1 day"' in drop_after["job_events"]


async def test_re_enable_converges(timescale_dsn: str, ts_schema: str) -> None:
    """A second conversion run changes nothing and duplicates no policy.

    ``taskq migrate up`` runs the conversion on every deploy; each run
    must converge (re-assert chunk intervals and policies from the
    current settings, skip already-hypertable tables) rather than
    accumulate.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        settings = _ts_settings(timescale_dsn, ts_schema)
        first = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert len(first.converted) == 3
        second = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert second.converted == (), "an already-converted schema must not re-convert its tables"
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert n_policies == 3, "re-enabling must not duplicate retention policies"
        # The schema stays fully operational after the re-run.
        n_ht = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert n_ht == 3
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


# ── Retention policies really drop chunks ─────────────────────────────────


async def _schedule_policies(
    conn: asyncpg.Connection, schema: str, *, next_start: datetime
) -> None:
    """Move every retention policy's next run to *next_start*.

    The policies run in TimescaleDB's background workers; tests do not
    wait out a schedule interval, they move the job's next_start (the
    supported alter_job knob) and let the worker execute the registered
    policy itself, so the chunk drop under test is the real policy run.
    """
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        schema,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            next_start,
        )


async def _force_policies_now(conn: asyncpg.Connection, schema: str) -> None:
    """Pull every retention policy's next run to now.

    The policies run in TimescaleDB's background workers; tests do not
    wait out a schedule interval, they move the job's next_start (the
    supported alter_job knob) and let the worker execute the registered
    policy itself, so the chunk drop under test is the real policy run.
    """
    await _schedule_policies(conn, schema, next_start=datetime.now(UTC))


async def _chunk_names(conn: asyncpg.Connection, schema: str, table: str) -> list[str]:
    rows = await conn.fetch(f'SELECT show_chunks(\'"{schema}"."{table}"\') AS ch')
    return sorted(r["ch"] for r in rows)


async def _seed_archive_row(conn: asyncpg.Connection, schema: str, *, finished_at: datetime) -> Any:
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)""",
        jid,
        now,
        now + timedelta(hours=1),
        finished_at,
        now + timedelta(days=365),
    )
    return jid


async def _seed_parent_with_event(
    conn: asyncpg.Connection, schema: str, *, occurred_at: datetime
) -> None:
    """One terminal parent job and one event at the given age.

    job_events carries a FK to jobs, so an event needs its parent row;
    this is the production shape too (a terminal job keeps events under
    the cascade-only regime until the event retention sweeps them).
    """
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close)
        VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3)""",
        jid,
        now,
        now + timedelta(hours=1),
    )
    await conn.execute(
        f"INSERT INTO {schema}.job_events (job_id, occurred_at, kind) "
        "VALUES ($1, $2, 'state_change')",
        jid,
        occurred_at,
    )


async def test_retention_policies_drop_old_chunks_keep_fresh_rows(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """The registered policies (not a test-callable shortcut) drop the aged
    chunks and preserve the fresh ones.

    Two archive rows three days apart (2-day retention, 1-day chunks): a
    real policy run must drop the old chunk outright, chunk storage, and
    the old row with it, while the fresh chunk survives. Same for
    job_events. This is the behavior that replaces the sweeps'
    long-window DELETEs on the hypertable mode.
    """
    now = datetime.now(UTC)
    old_id = await _seed_archive_row(ts_conn, schema=ts_schema, finished_at=now - timedelta(days=3))
    fresh_id = await _seed_archive_row(ts_conn, schema=ts_schema, finished_at=now)
    await _seed_parent_with_event(ts_conn, schema=ts_schema, occurred_at=now - timedelta(days=3))
    await _seed_parent_with_event(ts_conn, schema=ts_schema, occurred_at=now)

    assert len(await _chunk_names(ts_conn, ts_schema, "jobs_archive")) == 2
    assert len(await _chunk_names(ts_conn, ts_schema, "job_events")) == 2

    await _force_policies_now(ts_conn, ts_schema)
    archive_chunks: list[str] = []
    event_chunks: list[str] = []
    for _ in range(120):
        await asyncio.sleep(0.5)
        archive_chunks = await _chunk_names(ts_conn, ts_schema, "jobs_archive")
        event_chunks = await _chunk_names(ts_conn, ts_schema, "job_events")
        if len(archive_chunks) == 1 and len(event_chunks) == 1:
            break
    assert len(archive_chunks) == 1, "the aged archive chunk must be dropped by its policy"
    assert len(event_chunks) == 1, "the aged event chunk must be dropped by its policy"

    remaining_archive = await ts_conn.fetch(f"SELECT id FROM {ts_schema}.jobs_archive")
    assert {r["id"] for r in remaining_archive} == {fresh_id}, (
        "the policy must drop the aged row with its chunk and keep the fresh row"
    )
    remaining_events = await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.job_events")
    assert remaining_events == 1
    assert old_id not in {r["id"] for r in remaining_archive}


# ── The sweeps compose with the policies ─────────────────────────────────


async def test_event_sweep_still_deletes_inside_young_chunks(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """The row-level event TTL sweep keeps running on the hypertable.

    Chunk drops own the aged end of the timeline, but rows inside young
    chunks still age past the retention setting (retention < chunk
    interval is the ordinary case), and the sweep is what honors them.
    The reclaim-outbox carve-out must also survive the conversion: a
    lock_expired event younger than 100x retention stays for the
    watch_reclaims consumer while its ordinary sibling is deleted.
    """
    jid = new_uuid()
    now = datetime.now(UTC)
    await _seed_parent_job(ts_conn, ts_schema, jid)
    # Ordinary event, 30 minutes old: past the 20-minute retention below.
    await ts_conn.execute(
        f"INSERT INTO {ts_schema}.job_events (job_id, occurred_at, kind, detail) "
        "VALUES ($1, $2, 'state_change', '{}'::jsonb)",
        jid,
        now - timedelta(minutes=30),
    )
    # Outbox-slice event, same age: kept to 100x retention.
    await ts_conn.execute(
        f"INSERT INTO {ts_schema}.job_events (job_id, occurred_at, kind, detail) "
        "VALUES ($1, $2, 'state_change', '{\"reason\": \"lock_expired\"}'::jsonb)",
        jid,
        now - timedelta(minutes=30),
    )
    from taskq.backend._sweeps import sweep_expired_events

    deleted = await sweep_expired_events(
        ts_conn, schema=ts_schema, retention=timedelta(minutes=20), batch_size=100
    )
    assert deleted == 1, "the ordinary event is past retention and must be swept"
    rows = await ts_conn.fetch(f"SELECT detail FROM {ts_schema}.job_events")
    assert len(rows) == 1
    assert json.loads(rows[0]["detail"]).get("reason") == "lock_expired", (
        "the reclaim-outbox carve-out must survive the hypertable conversion"
    )


async def _seed_parent_job(conn: asyncpg.Connection, schema: str, jid: Any) -> None:
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close)
        VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3)""",
        jid,
        now,
        now + timedelta(hours=1),
    )


async def test_archive_expiry_sweep_still_honors_expire_at(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """A row whose expire_at passes inside a still-present chunk is deleted
    by the sweep.

    Chunks drop on the NEWEST finished_at they contain, so a row can
    pass its own expire_at while its chunk is still alive; the expiry
    sweep is the only mechanism that honors expire_at exactly, and the
    hypertable mode keeps it running (compose, not skip). No runtime
    code branches on whether hypertables exist.
    """
    now = datetime.now(UTC)
    jid = await _seed_archive_row(ts_conn, schema=ts_schema, finished_at=now)
    await ts_conn.execute(
        f"UPDATE {ts_schema}.jobs_archive SET expire_at = $2 WHERE id = $1",
        jid,
        now - timedelta(hours=1),
    )
    result = await archive_expiry_sweep(ts_conn, schema=ts_schema, batch_size=100)
    assert result.total_deleted == 1
    assert result.by_status == {"succeeded": 1}
    n = await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive WHERE id = $1", jid)
    assert n == 0


# ── Differential: prune agrees across vanilla and hypertable ──────────────


async def _prune_scenario(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    """One shared behavior script, run identically on both legs.

    Plants the pre-fix ghost state by hand (an archive row whose job is
    still live and aged) and runs the real prune: the outcome must be
    the fold in BOTH modes. Returns the observable facts.
    """
    now = datetime.now(UTC)
    for _ in range(2):
        jid = new_uuid()
        await conn.execute(
            f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, scheduled_at, schedule_to_close, finished_at)
            VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                'succeeded', $2, $3, $4)""",
            jid,
            _AGED,
            _AGED + timedelta(hours=1),
            _AGED,
        )
        await conn.execute(
            f"""INSERT INTO {schema}.job_attempts (job_id, attempt, started_at,
                finished_at, outcome, metadata)
            VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
            jid,
            _AGED,
            _AGED + timedelta(minutes=4),
        )
    # The hand-planted ghost: archived once (a pre-fix version), the job
    # retried and re-terminalized since (live, aged again).
    ghost = new_uuid()
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close, finished_at)
        VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4)""",
        ghost,
        _AGED,
        _AGED + timedelta(hours=1),
        _AGED,
    )
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close, finished_at,
            archived_at, expire_at)
        VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $5, $6)""",
        ghost,
        _AGED,
        _AGED + timedelta(hours=1),
        _AGED,
        now,
        now + timedelta(days=365),
    )
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        schema=schema,
        batch_size=100,
    )
    archive_ids = await conn.fetch(f"SELECT id FROM {schema}.jobs_archive")
    live_ids = await conn.fetch(f"SELECT id FROM {schema}.jobs WHERE status = 'succeeded'")
    attempts = await conn.fetchval(f"SELECT count(*) FROM {schema}.job_attempts_archive")
    return {
        "total_deleted": result.total_deleted,
        "archived_by_status": dict(result.by_status),
        "archive_rows": len(archive_ids),
        "ghost_archive_copies": sum(1 for r in archive_ids if r["id"] == ghost),
        "ghost_live_copies": sum(1 for r in live_ids if r["id"] == ghost),
        "archive_attempts": attempts,
        "live_terminal_rows": len(live_ids),
    }


async def test_prune_folds_ghosts_and_archives_identically_on_both_engines(
    timescale_dsn: str, pg_dsn: str
) -> None:
    """Differential: vanilla Postgres and the hypertable mode agree.

    The same behavior script (archive two aged terminal jobs with their
    attempts; meet a hand-planted pre-fix ghost) runs against both
    engines and the observable outcomes must match exactly: same prune
    counts, exactly one archive row per job id (the ghost is FOLDED, the
    retried job's live row removed, no duplicate archive rows), no live
    terminal rows left, attempts archived with their jobs. The hypertable
    mode's widened uniqueness (id, finished_at) plus the archive write's
    NOT EXISTS guard must deliver the re-archive semantics the vanilla
    primary key used to enforce, in both directions (no wedge, no
    duplicates).
    """
    outcomes: dict[str, dict[str, Any]] = {}
    for label, dsn in (("vanilla", pg_dsn), ("timescale", timescale_dsn)):
        schema = f"ts_diff_{label}"
        conn = await asyncpg.connect(dsn)
        try:
            await _migrate(conn, schema)
            if label == "timescale":
                await enable_hypertables(conn, schema=schema, settings=_ts_settings(dsn, schema))
                # Same deferral as the ts_conn fixture: the scenario's
                # assertions count rows a ~now-scheduled policy run could
                # drop mid-test (the seeded rows are 31 days old, the
                # retention is 2). The policy's behavior is not what this
                # leg tests - engine parity of the prune is - so the
                # policies must not fire while it runs.
                await _schedule_policies(
                    conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650)
                )
            outcomes[label] = await _prune_scenario(conn, schema)
        finally:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await conn.close()
    vanilla = outcomes["vanilla"]
    timescale = outcomes["timescale"]
    assert vanilla == timescale, f"engines disagree:\nvanilla={vanilla}\ntimescale={timescale}"
    assert vanilla["total_deleted"] == 3
    assert vanilla["archived_by_status"] == {"succeeded": 3}
    assert vanilla["archive_rows"] == 3
    assert vanilla["ghost_archive_copies"] == 1, "a ghost is folded into its archive row"
    assert vanilla["ghost_live_copies"] == 0, "the folded job's live row is removed"
    assert vanilla["archive_attempts"] == 2, "attempts archive with their jobs"
    assert vanilla["live_terminal_rows"] == 0


async def test_job_delete_still_cascades_to_attempts(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """job_attempts' FK to jobs is untouched by the conversion.

    The conversion drops job_attempts_archive's FK (it references the
    hypertable); job_attempts' FK to the REGULAR jobs table must keep
    its ON DELETE CASCADE working.
    """
    jid = new_uuid()
    now = datetime.now(UTC)
    await _seed_parent_job(ts_conn, ts_schema, jid)
    await ts_conn.execute(
        f"""INSERT INTO {ts_schema}.job_attempts (job_id, attempt, started_at,
            finished_at, outcome, metadata)
        VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
        jid,
        now,
        now + timedelta(minutes=4),
    )
    await ts_conn.execute(f"DELETE FROM {ts_schema}.jobs WHERE id = $1", jid)
    n = await ts_conn.fetchval(
        f"SELECT count(*) FROM {ts_schema}.job_attempts WHERE job_id = $1", jid
    )
    assert n == 0, "deleting a job must still cascade to its attempts"


# ── Metrics: windowed queries prune chunks ────────────────────────────────


async def test_windowed_metrics_query_prunes_chunks(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """A windowed job_events query plans against ONLY the chunks its window
    can touch.

    With the event log chunked, every admin/metrics query that bounds
    occurred_at on both sides prunes the chunks outside the window at
    plan time: the plan references a strict subset of the hypertable's
    chunks. Pinned here via EXPLAIN with the same bound shape the
    dashboards use (a timestamptz window), the plan shape the Timescale
    leg guarantees for every windowed consumer.
    """
    now = datetime.now(UTC)
    # Events across three distinct 1-day chunks.
    for age in (0.0, 1.5, 3.0):
        await _seed_parent_with_event(
            ts_conn, schema=ts_schema, occurred_at=now - timedelta(days=age)
        )
    all_chunks = await _chunk_names(ts_conn, ts_schema, "job_events")
    assert len(all_chunks) == 3

    rows = await ts_conn.fetch(
        f"""EXPLAIN (FORMAT TEXT)
        SELECT kind, count(*) FROM "{ts_schema}".job_events
        WHERE occurred_at >= $1::timestamptz AND occurred_at < $2::timestamptz
        GROUP BY kind""",
        now - timedelta(hours=2),
        now,
    )
    plan = "\n".join(r[0] for r in rows)
    referenced = sorted({line for line in plan.splitlines() if "_hyper_" in line})
    assert referenced, f"the plan must read chunks:\n{plan}"
    chunk_tokens = {token for r in all_chunks for token in [r.split(".")[-1]]}
    referenced_chunks = {tok for tok in chunk_tokens if tok in plan}
    assert referenced_chunks < chunk_tokens, (
        "a windowed metrics query must prune at least one chunk:\n"
        f"plan={plan}\nchunks={sorted(chunk_tokens)}"
    )


# ── Vanilla cross-check for the same EXPLAIN (both modes) ─────────────────


async def test_windowed_metrics_query_runs_on_vanilla(pg_dsn: str) -> None:
    """The same windowed query plans on vanilla Postgres (no chunk nodes,
    no errors), pinning that the metrics surface needs no mode-specific
    SQL."""
    schema = "ts_metrics_vanilla"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _migrate(conn, schema)
        now = datetime.now(UTC)
        await _seed_parent_with_event(conn, schema=schema, occurred_at=now)
        rows = await conn.fetch(
            f"""EXPLAIN (FORMAT TEXT)
            SELECT kind, count(*) FROM "{schema}".job_events
            WHERE occurred_at >= $1::timestamptz AND occurred_at < $2::timestamptz
            GROUP BY kind""",
            now - timedelta(hours=2),
            now,
        )
        plan = "\n".join(r[0] for r in rows)
        assert "_hyper_" not in plan
        assert "job_events" in plan
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── Red-team edges: re-run convergence, mid-population conversion, failure
#    atomicity, widened uniqueness, and the deliberately dropped FK ─────────


async def test_re_run_converges_chunks_and_honors_changed_retention(
    timescale_dsn: str, ts_schema: str
) -> None:
    """Re-run twice, then re-run with a CHANGED archive retention.

    ``taskq migrate up`` re-runs the conversion on every deploy, so the
    remove-then-add policy registration must converge under three runs:
    same settings (no duplicate chunks, no duplicate policies), then a
    changed ``archive_retention_period`` (the NEW interval must take
    force, not the first-registered one, and the chunk interval must
    re-assert from the changed setting for future chunks).
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        settings = _ts_settings(timescale_dsn, ts_schema)
        first = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert set(first.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
        # Same deferral as the ts_conn fixture: keep the scheduler out of
        # the assertions below; what runs here is the conversion path.
        await _schedule_policies(
            conn, ts_schema, next_start=datetime.now(UTC) + timedelta(days=3650)
        )
        # Real data so "no duplicate chunks" is observable: one row (one
        # chunk) per table.
        archive_jid = await _seed_archive_row(conn, schema=ts_schema, finished_at=datetime.now(UTC))
        await _seed_parent_with_event(conn, schema=ts_schema, occurred_at=datetime.now(UTC))
        await conn.execute(
            f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt,
                started_at, finished_at, outcome, metadata)
            VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
            archive_jid,
            datetime.now(UTC),
            datetime.now(UTC) + timedelta(minutes=4),
        )
        chunks_before = {
            t: await _chunk_names(conn, ts_schema, t)
            for t in ("job_events", "jobs_archive", "job_attempts_archive")
        }
        assert all(len(c) == 1 for c in chunks_before.values()), chunks_before

        same = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert same.converted == (), "an already-converted schema must not re-convert"
        assert same.retention_policies == (
            "jobs_archive:2 days",
            "job_attempts_archive:2 days",
            "job_events:1 days",
        )
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert n_policies == 3, "a same-settings re-run must not duplicate policies"
        for table, before in chunks_before.items():
            assert await _chunk_names(conn, ts_schema, table) == before, (
                f"a same-settings re-run must not create or drop chunks of {table}"
            )

        changed = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": timescale_dsn,
                "TASKQ_SCHEMA_NAME": ts_schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
                "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(timedelta(days=30).total_seconds())}s",
                "TASKQ_EVENT_RETENTION_PERIOD": f"{int(_TEST_EVENT_RETENTION.total_seconds())}s",
            }
        )
        third = await enable_hypertables(conn, schema=ts_schema, settings=changed)
        assert third.converted == ()
        assert third.retention_policies == (
            "jobs_archive:30 days",
            "job_attempts_archive:30 days",
            "job_events:1 days",
        ), "a changed archive retention must be re-registered, not kept stale"
        policies = await conn.fetch(
            "SELECT hypertable_name, config FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert len(policies) == 3, "a changed-settings re-run must not duplicate policies"
        drop_after = {r["hypertable_name"]: r["config"] for r in policies}
        assert '"drop_after": "30 days"' in drop_after["jobs_archive"]
        assert '"drop_after": "30 days"' in drop_after["job_attempts_archive"]
        assert '"drop_after": "1 day"' in drop_after["job_events"]
        # The chunk interval re-asserts from the changed setting for FUTURE
        # chunks (30d/4 = 7.5d, inside the clamp); job_events' is unchanged.
        dims = await conn.fetch(
            "SELECT h.table_name, d.interval_length "
            "FROM _timescaledb_catalog.hypertable h "
            "JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id "
            "WHERE h.schema_name = $1",
            ts_schema,
        )
        got = {r["table_name"]: r["interval_length"] for r in dims}
        new_archive_chunk = (timedelta(days=30) / 4).total_seconds() * 1_000_000
        assert got["jobs_archive"] == new_archive_chunk
        assert got["job_attempts_archive"] == new_archive_chunk
        assert got["job_events"] == _TEST_CHUNK_INTERVAL.total_seconds() * 1_000_000
        # And no re-run ever duplicated a chunk.
        for table, before in chunks_before.items():
            assert len(await _chunk_names(conn, ts_schema, table)) == len(before), table
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


async def test_mid_population_conversion_preserves_every_row_and_index(
    timescale_dsn: str, ts_schema: str
) -> None:
    """Converting a POPULATED table (the ``migrate_data`` path) keeps every
    row exactly and rebuilds usable chunk indexes.

    50k job_events across multiple chunks convert inside one
    ``enable_hypertables`` call: the row count and a content checksum over
    (id, occurred_at, kind, detail) must be bit-identical across the
    conversion, and a selective job_id query must plan through the carried
    index (a Seq Scan would mean the indexes did not come back).
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        # 100 parent jobs; the 50k events spread across them so a single
        # job_id lookup is genuinely selective (~1%).
        parent_ids = [new_uuid() for _ in range(100)]
        await conn.executemany(
            f"""INSERT INTO {ts_schema}.jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, scheduled_at)
            VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, 3, 'transient',
                'succeeded', now())""",
            [(pid,) for pid in parent_ids],
        )
        # 50k rows spread over 2000 distinct minutes (~34h => >= 2 chunks at
        # the 1-day clamp) and over the 100 parents: real volume for
        # migrate_data, still test-fast.
        await conn.execute(
            f"""WITH p AS (
                SELECT id, row_number() OVER (ORDER BY id) - 1 AS n
                FROM {ts_schema}.jobs
            )
            INSERT INTO {ts_schema}.job_events (job_id, occurred_at, kind, detail)
            SELECT p.id, now() - ((g % 2000) || ' minutes')::interval,
                'state_change', ('{{"g":' || g || '}}')::jsonb
            FROM generate_series(1, 50000) g
            JOIN p ON p.n = g % 100""",
        )
        # A small archive family converts in the same call.
        archive_jid = await _seed_archive_row(conn, schema=ts_schema, finished_at=datetime.now(UTC))
        await conn.execute(
            f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt,
                started_at, finished_at, outcome, metadata)
            VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
            archive_jid,
            datetime.now(UTC),
            datetime.now(UTC) + timedelta(minutes=4),
        )

        checksum_sql = f"""
            SELECT count(*) AS n,
                   md5(string_agg(e.job_id::text || ':' || e.id::text || ':' ||
                                  extract(epoch from e.occurred_at)::text || ':' ||
                                  e.kind || ':' || e.detail::text, ','
                                  ORDER BY e.id)) AS digest
            FROM {ts_schema}.job_events e
        """
        before = await conn.fetchrow(checksum_sql)
        assert before is not None

        report = await enable_hypertables(
            conn, schema=ts_schema, settings=_ts_settings(timescale_dsn, ts_schema)
        )
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }

        after = await conn.fetchrow(checksum_sql)
        assert after is not None
        assert after["n"] == before["n"] == 50000, (
            "the migrate_data conversion must preserve every row"
        )
        assert after["digest"] == before["digest"], (
            "the migrate_data conversion must preserve row CONTENT exactly, not just counts"
        )
        assert await conn.fetchval(f"SELECT count(*) FROM {ts_schema}.job_attempts_archive") == 1, (
            "the small archive family converts with its data too"
        )

        # Indexes must come back on every chunk: each chunk carries the same
        # index set as the parent (the widened unique plus the migrated
        # secondaries, partial predicate included).
        parent_indexes = await conn.fetch(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = 'job_events'",
            ts_schema,
        )
        chunks = await _chunk_names(conn, ts_schema, "job_events")
        assert len(chunks) >= 2, "the seed must span multiple chunks for this pin"
        bare_chunks = [c.split(".")[-1] for c in chunks]
        for chunk in bare_chunks:
            chunk_indexes = await conn.fetch(
                "SELECT indexdef FROM pg_indexes WHERE tablename = $1", chunk
            )
            assert len(chunk_indexes) == len(parent_indexes), (
                f"{chunk} must carry one index per parent index "
                f"({len(chunk_indexes)} vs {len(parent_indexes)})"
            )
        defs = "\n".join(
            r["indexdef"]
            for r in await conn.fetch(
                "SELECT indexdef FROM pg_indexes WHERE tablename = ANY($1)",
                bare_chunks,
            )
        )
        assert "(job_id, occurred_at)" in defs, (
            "the per-job event index must be carried onto chunks"
        )
        # The partial retention-sweep index carries its predicate onto
        # chunks in its logically-equivalent negated form (NOT (kind = ...
        # AND ...) becomes kind <> ... OR ... <> ...).
        assert "kind <> 'state_change'" in defs, (
            "the partial retention-sweep index must carry its predicate onto chunks"
        )

        # And the planner must actually USE one: a selective job_id lookup
        # (~1% of 50k rows) must not fall back to a Seq Scan.
        jid = parent_ids[0]
        await conn.execute(f"ANALYZE {ts_schema}.job_events")
        plan_rows = await conn.fetch(
            f"EXPLAIN (FORMAT TEXT) SELECT count(*) FROM {ts_schema}.job_events WHERE job_id = $1",
            jid,
        )
        plan = "\n".join(r[0] for r in plan_rows)
        assert "Seq Scan" not in plan, f"chunk indexes must serve a job_id lookup:\n{plan}"
        assert "job_id_idx" in plan, f"the carried job_id index must appear in the plan:\n{plan}"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


class _FailOnceConn:
    """A forwarding proxy that injects ONE deterministic failure.

    Every statement runs for real against the server; when the statement
    is the ``create_hypertable`` call for *victim_table* the proxy raises
    instead of forwarding - the same observable shape as the server dying
    (or the advisory-lock holder being killed) at that exact point, but
    deterministic and without any sleep.
    """

    def __init__(self, inner: asyncpg.Connection, victim_table: str) -> None:
        self._inner = inner
        self._victim = victim_table

    async def execute(self, sql: str, *args: Any) -> str:
        if "create_hypertable" in sql and any(self._victim in str(a) for a in args):
            raise RuntimeError(f"injected mid-conversion failure of {self._victim}")
        return await self._inner.execute(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        return await self._inner.fetch(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_failure_mid_conversion_leaves_schema_usable_and_rerun_completes(
    timescale_dsn: str, ts_schema: str
) -> None:
    """A failure BETWEEN table conversions leaves the schema usable and a
    re-run converges.

    The conversion is per-statement idempotent DDL, not one transaction,
    so a crash mid-flight can strand a PARTIALLY converted schema. Pinned
    here: the failure surfaces, the server keeps answering (ordinary DML
    works), exactly the already-converted tables are hypertables, the DO
    blocks that ran stay applied, and a clean re-run converts ONLY the
    stragglers and converges policies to 3.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        settings = _ts_settings(timescale_dsn, ts_schema)
        # Conversion order is job_events first, then the archive family;
        # the injected failure hits at jobs_archive's create_hypertable.
        with pytest.raises(RuntimeError, match="jobs_archive"):
            await enable_hypertables(
                _FailOnceConn(conn, "jobs_archive"),  # pyright: ignore[reportArgumentType]  # Why: the proxy IS the contract under test.
                schema=ts_schema,
                settings=settings,
            )

        # The server is left usable: ordinary DML and catalogs answer.
        await _seed_parent_job(conn, ts_schema, new_uuid())
        ht = await conn.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert {r["table_name"] for r in ht} == {"job_events"}, (
            "the failure must strand exactly the pre-failure conversion state, no more"
        )
        # The independently-runnable statements that DID run stay applied.
        widened = await conn.fetchval(
            "SELECT count(*) FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = 'jobs_archive' "
            "AND con.conname = 'jobs_archive_id_finished_at_uniq'",
            ts_schema,
        )
        assert widened == 1, "the DO block that ran before the failure must persist"

        report = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert set(report.converted) == {"jobs_archive", "job_attempts_archive"}, (
            "a re-run must convert ONLY the stragglers, not re-convert job_events"
        )
        n_ht = await conn.fetchval(
            "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert n_ht == 3
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            ts_schema,
        )
        assert n_policies == 3, "the converging re-run must register exactly one policy per table"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


async def test_widened_unique_constraints_still_reject_exact_duplicates(
    ts_conn: asyncpg.Connection, ts_schema: str
) -> None:
    """The widened UNIQUE constraints enforce the duplicates vanilla PG's
    primary keys rejected.

    A hypertable demands the partition column inside every unique
    constraint, so all three PKs widen. The narrowed claim - an EXACT
    duplicate row - must still be rejected on all three tables. The
    widening's cost (same id, different time value is now accepted) is the
    documented trade, compensated by the archive write's NOT EXISTS guard
    (see the differential prune test); pinned explicitly below.
    """
    # job_events: an exact duplicate (id, occurred_at) must be rejected.
    await _seed_parent_with_event(ts_conn, schema=ts_schema, occurred_at=datetime.now(UTC))
    ev = await ts_conn.fetchrow(f"SELECT id, job_id, occurred_at, kind FROM {ts_schema}.job_events")
    assert ev is not None
    with pytest.raises(asyncpg.UniqueViolationError):
        await ts_conn.execute(
            f"INSERT INTO {ts_schema}.job_events (id, job_id, occurred_at, kind) "
            "VALUES ($1, $2, $3, $4)",
            ev["id"],
            ev["job_id"],
            ev["occurred_at"],
            ev["kind"],
        )

    # jobs_archive: an exact duplicate row must be rejected.
    jid = await _seed_archive_row(ts_conn, schema=ts_schema, finished_at=datetime.now(UTC))
    with pytest.raises(asyncpg.UniqueViolationError):
        await ts_conn.execute(
            f"INSERT INTO {ts_schema}.jobs_archive SELECT * FROM {ts_schema}.jobs_archive "
            "WHERE id = $1",
            jid,
        )

    # job_attempts_archive: an exact duplicate (job_id, attempt, started_at)
    # must be rejected.
    await ts_conn.execute(
        f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt, started_at,
            finished_at, outcome, metadata)
        VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
        jid,
        datetime.now(UTC),
        datetime.now(UTC) + timedelta(minutes=4),
    )
    row = await ts_conn.fetchrow(
        f"SELECT job_id, attempt, started_at FROM {ts_schema}.job_attempts_archive "
        "WHERE job_id = $1",
        jid,
    )
    assert row is not None
    with pytest.raises(asyncpg.UniqueViolationError):
        await ts_conn.execute(
            f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt, started_at,
                finished_at, outcome, metadata)
            VALUES ($1, $2, $3, $4, 'succeeded', '{{}}'::jsonb)""",
            row["job_id"],
            row["attempt"],
            row["started_at"],
            datetime.now(UTC),
        )

    # The documented trade, pinned: jobs_archive's widened UNIQUE (id,
    # finished_at) accepts a second row with the same id at a different
    # finished_at - which the vanilla PRIMARY KEY (id) would have rejected.
    # This is safe ONLY because the archive write re-checks NOT EXISTS
    # (taskq.worker._leader_shared), which the differential prune test pins.
    await ts_conn.execute(
        f"""INSERT INTO {ts_schema}.jobs_archive (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close, finished_at,
            archived_at, expire_at)
        VALUES ($1, 'test_actor', 'default', '{{"v":2}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)""",
        jid,
        datetime.now(UTC),
        datetime.now(UTC) + timedelta(hours=1),
        datetime.now(UTC) + timedelta(days=1),
        datetime.now(UTC) + timedelta(days=365),
    )
    assert (
        await ts_conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive WHERE id = $1", jid)
        == 2
    ), "same id at a different finished_at is the accepted widening trade"


async def test_orphan_archive_attempt_accepted_and_danger_pinned(
    ts_conn: asyncpg.Connection,
    ts_schema: str,
    pg_dsn: str,
) -> None:
    """The dropped FK is GENUINELY gone: an orphan attempt row is accepted.

    ``job_attempts_archive``'s FK to ``jobs_archive(id) ON DELETE CASCADE``
    drops because nothing may reference a hypertable; chunk retention on
    the shared archive clock replaces it. Pinned in both directions: the
    orphan insert is REJECTED on vanilla Postgres (proving the drop, not a
    test artifact, is what changed) and ACCEPTED on the hypertable - the
    documented DANGER. The compensation is also pinned: both archive
    policies derive their drop_after from the SAME
    ``archive_retention_period`` setting, so an attempt row's chunk drops
    on the same clock its parent's chunk does.
    """
    orphan = new_uuid()
    await ts_conn.execute(
        f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt, started_at,
            finished_at, outcome, metadata)
        VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
        orphan,
        datetime.now(UTC),
        datetime.now(UTC) + timedelta(minutes=4),
    )
    assert (
        await ts_conn.fetchval(
            f"SELECT count(*) FROM {ts_schema}.job_attempts_archive WHERE job_id = $1", orphan
        )
        == 1
    ), "with the FK dropped, an orphan attempt must be accepted (the documented trade)"

    # Contrast on vanilla Postgres: the same insert violates the FK there,
    # proving the acceptance above is the conversion's doing.
    schema = "ts_orphan_vanilla"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _migrate(conn, schema)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                f"""INSERT INTO {schema}.job_attempts_archive (job_id, attempt, started_at,
                    finished_at, outcome, metadata)
                VALUES ($1, 1, $2, $3, 'succeeded', '{{}}'::jsonb)""",
                orphan,
                datetime.now(UTC),
                datetime.now(UTC) + timedelta(minutes=4),
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()

    # The compensation: one archive retention setting -> identical
    # drop_after on both archive hypertables (attempts drop on the same
    # clock as their parents, per-chunk).
    configs = await ts_conn.fetch(
        "SELECT hypertable_name, config FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention' "
        "AND hypertable_name IN ('jobs_archive', 'job_attempts_archive')",
        ts_schema,
    )
    assert {r["hypertable_name"] for r in configs} == {"jobs_archive", "job_attempts_archive"}
    drop_after = [str(r["config"]).split('"drop_after": ')[1].split(",")[0] for r in configs]
    assert len(set(drop_after)) == 1, (
        f"both archive policies must drop on the SAME clock, got {drop_after}"
    )

    # And the danger stays DOCUMENTED: the module contract names the FK
    # drop as the one deliberate structural loss.
    from taskq import timescale as timescale_module

    assert "may reference a hypertable" in (timescale_module.__doc__ or ""), (
        "the FK-drop danger must remain documented in the module contract"
    )

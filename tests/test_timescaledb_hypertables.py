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
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
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
    disable_hypertables,
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
    conn: asyncpg.Connection, schema: str, *, next_start: datetime, proc_name: str = "policy%"
) -> None:
    """Move every policy job's next run to *next_start*.

    The policies run in TimescaleDB's background workers; tests do not
    wait out a schedule interval, they move the job's next_start (the
    supported alter_job knob) and let the worker execute the registered
    policy itself, so the chunk drop under test is the real policy run.
    Defaults to ALL policy jobs — retention AND the compression policy
    the deploy step arms on the archive tables: a ~now-scheduled
    compression run would age chunks into the columnstore mid-test and
    put the sweep legs on compressed chunks (the measured
    ``ConfigurationLimitExceededError`` surface), so the fixture defers
    the whole family and a leg that needs compression asserts it
    directly.
    """
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name LIKE $2",
        schema,
        proc_name,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            next_start,
        )


async def _force_policies_now(conn: asyncpg.Connection, schema: str) -> None:
    """Pull every RETENTION policy's next run to now.

    The compression policy stays deferred: these legs assert chunk
    DROPS, not compression, and a mid-test compress would change the
    sweep surface they pin.
    """
    await _schedule_policies(
        conn, schema, next_start=datetime.now(UTC), proc_name="policy_retention"
    )


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


async def _seed_null_finished_archive_row(conn: asyncpg.Connection, schema: str) -> None:
    """The one row shape the hypertable mode cannot hold: a NULL finished_at.

    Direct-SQL written on purpose — that is the ONLY way the shape enters
    the archive (every production terminal arm stamps finished_at, and
    the archive COPY's candidate and lock-time predicates both require it
    non-NULL), which is exactly the debris verdict the refusal encodes.
    """
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                scheduled_at, finished_at, archived_at, expire_at)
            VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
                'succeeded', now(), NULL, now(), now() + interval '365 days')""",
        new_uuid(),
    )


async def test_null_finished_at_archive_tail_refuses_before_any_ddl(
    timescale_dsn: str, ts_schema: str
) -> None:
    """The archive's NULL finished_at tail: refused BEFORE any DDL, loudly.

    ``jobs_archive.finished_at`` is nullable (the archive mirrors every
    ``jobs`` column, and a job's ``finished_at`` is NULL until a terminal
    transition stamps it), so a legacy archive can hold NULL-finished_at
    rows: version skew carries the shape in (the reclaim sweep's
    pre-reorder shape terminalised budget-carrying cancelled rows
    unstamped — the un-draining NULL tail of the prune's cursor walk, the
    shape its finished_at CASE comment names), and direct SQL carries it
    in today. TimescaleDB cannot convert the table while such rows exist
    (the partition column must be NOT NULL), and the pre-fix behavior was
    the worst shape: the NotNullViolationError surfaced mid-migrate_data,
    AFTER the primary-key drops, the unique adds, and the foreign-key
    drop had already committed — a half-converted schema whose archive
    has no primary key and whose job_events is already a hypertable.
    Pinned here, three facts:

    * the refusal is LOUD: it names the row count and both remediations
      (complete via finished_at = archived_at, or discard);
    * the refusal is ATOMIC: it fires before ANY statement of the
      conversion, so the schema stays byte-identical to what the operator
      arrived with — every pkey and the attempts FK still in place, no
      hypertable, and the debris row untouched;
    * the remediation unblocks: the census counts remaining rows only,
      so the backfill the message names, followed by a re-run, converts
      all three tables cleanly.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        await _seed_null_finished_archive_row(conn, ts_schema)
        await _seed_null_finished_archive_row(conn, ts_schema)
        # A healthy row beside the debris: the conversion must not be
        # refused for a table that is merely POPULATED.
        await _seed_archive_row(
            conn, schema=ts_schema, finished_at=datetime.now(UTC) - timedelta(days=3)
        )
        settings = _ts_settings(timescale_dsn, ts_schema)

        with pytest.raises(
            TimescaleDBUnavailableError,
            match=r"2 row\(s\) whose finished_at is NULL",
        ) as exc_info:
            await enable_hypertables(conn, schema=ts_schema, settings=settings)
        # The remediation is in the message, both branches named.
        message = str(exc_info.value)
        assert "finished_at = archived_at" in message
        assert "DELETE FROM" in message

        # ATOMIC: nothing applied. Every vanilla constraint the
        # half-converted pre-fix state lost is still in place, no table
        # converted, and the debris row is exactly where it was.
        ht = await conn.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            ts_schema,
        )
        assert ht == [], "the refusal must precede every conversion statement"
        cons = await conn.fetch(
            "SELECT c.relname AS tbl, con.conname "
            "FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND con.contype IN ('p', 'f')",
            ts_schema,
        )
        names = {(r["tbl"], r["conname"]) for r in cons}
        assert ("jobs_archive", "jobs_archive_pkey") in names, (
            "the archive must keep its primary key: the pre-fix failure "
            "dropped it before surfacing, and a duplicate-id insert succeeded"
        )
        assert ("job_attempts_archive", "job_attempts_archive_pkey") in names
        assert ("job_attempts_archive", "job_attempts_archive_job_id_fkey") in names
        assert ("job_events", "job_events_pkey") in names
        n_rows = await conn.fetchval(f"SELECT count(*) FROM {ts_schema}.jobs_archive")
        assert n_rows == 3, "the debris rows must survive the refusal untouched"

        # The remediation unblocks: the census counts only rows that are
        # still NULL, so completing them makes the very next deploy
        # convert cleanly — the refusal is a gate, never a wedge.
        await conn.execute(
            f"UPDATE {ts_schema}.jobs_archive SET finished_at = archived_at "
            "WHERE finished_at IS NULL"
        )
        report = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
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


# ── The mirror: disable_hypertables ───────────────────────────────────────


def _vanilla_settings(dsn: str, schema: str) -> WorkerSettings:
    """The disable gate's settings: the flag flipped OFF first (the
    mirror of enable's flag-on gate — disable with the flag still true
    is a zero-statement no-op)."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "false",
            "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(_TEST_ARCHIVE_RETENTION.total_seconds())}s",
            "TASKQ_EVENT_RETENTION_PERIOD": f"{int(_TEST_EVENT_RETENTION.total_seconds())}s",
        }
    )


async def _hypertable_names(conn: asyncpg.Connection, schema: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT hypertable_name FROM timescaledb_information.hypertables "
        "WHERE hypertable_schema = $1",
        schema,
    )
    return {r["hypertable_name"] for r in rows}


async def _seed_history(
    conn: asyncpg.Connection, schema: str, *, n_events: int, n_archive: int, n_attempts: int
) -> dict[str, int]:
    """The deploy E2E's seed shapes: live parents (job_events' FK targets),
    events riding them, and the archive family with attempts. Returns the
    per-table counts seeded."""
    n_jobs = 10
    live_ids = [new_uuid() for _ in range(n_jobs)]
    await conn.executemany(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close)
        VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, 3, 'transient',
            'succeeded', now(), now() + interval '1 hour')""",
        [(jid,) for jid in live_ids],
    )
    await conn.executemany(
        f"INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail) "
        "VALUES ($1, clock_timestamp(), 'state_change', '{}'::jsonb)",
        [(live_ids[i % n_jobs],) for i in range(n_events)],
    )
    archive_ids = [uuid.UUID(int=i + 1) for i in range(n_archive)]
    await conn.executemany(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, status, attempt, max_attempts,
            retry_kind, expire_at, finished_at)
        VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', 0, 3, 'transient',
            clock_timestamp() + interval '365 days', clock_timestamp())""",
        [(jid,) for jid in archive_ids],
    )
    await conn.executemany(
        f"""INSERT INTO {schema}.job_attempts_archive (job_id, attempt, started_at)
        VALUES ($1, $2::smallint, clock_timestamp())""",
        [(archive_ids[i % n_archive], i // n_archive) for i in range(n_attempts)],
    )
    return {
        "job_events": n_events,
        "jobs_archive": n_archive,
        "job_attempts_archive": n_attempts,
    }


def _normalize_defs(rows: list[Any], schema: str, key: str) -> list[str]:
    """Index/constraint definitions with the schema token normalized away,
    so two schemas on the same server compare byte-equal."""
    return sorted(
        r[key].replace(f'"{schema}".', "SCHEMA.").replace(f"{schema}.", "SCHEMA.") for r in rows
    )


async def _shape_of(conn: asyncpg.Connection, schema: str, table: str) -> dict[str, list[str]]:
    """The shape a fresh vanilla migration mints for *table*: every index
    definition, every constraint definition (name + body), and the column
    order with defaults."""
    indexes = await conn.fetch(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
        schema,
        table,
    )
    constraints = await conn.fetch(
        "SELECT con.conname, pg_get_constraintdef(con.oid) AS def "
        "FROM pg_constraint con "
        "JOIN pg_class c ON c.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = $2",
        schema,
        table,
    )
    columns = await conn.fetch(
        "SELECT column_name, column_default FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
        schema,
        table,
    )
    return {
        "indexes": _normalize_defs(indexes, schema, "indexdef"),
        "constraints": _normalize_defs(constraints, schema, "def"),
        "columns": [
            f"{r['column_name']}::{(r['column_default'] or '').replace(f'"{schema}".', 'SCHEMA.').replace(f'{schema}.', 'SCHEMA.')}"
            for r in columns
        ],
    }


async def test_disable_restores_vanilla_shape_and_every_row(timescale_dsn: str) -> None:
    """enable -> seed (the deploy E2E's shapes) -> disable: zero
    hypertables, the vanilla SHAPE byte-equal to a fresh migration's
    (constraints, indexes, FK, column order and defaults — the shape is
    cloned from the migrations' own output, never re-typed), and EVERY
    row present, counted."""
    schema = "tsdis_" + new_uuid().hex[:12]
    fresh = "tsfresh_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        report = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=500, n_archive=200, n_attempts=300)

        disable_report = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert set(disable_report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, "the disable must restore all three tables to plain"
        # The mirror reading of the report: the policies REMOVED, at the
        # intervals the registered jobs themselves carried (the config's
        # own text — Postgres renders the singular).
        assert set(disable_report.retention_policies) == {
            "jobs_archive:2 days",
            "job_attempts_archive:2 days",
            "job_events:1 day",
        }, disable_report.retention_policies
        assert set(disable_report.compression_policies) == {
            "jobs_archive",
            "job_attempts_archive",
        }

        assert await _hypertable_names(conn, schema) == set(), (
            "the disable must leave zero hypertables"
        )
        jobs = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = $1",
            schema,
        )
        assert jobs == 0, "no retention or compression policy may survive the disable"

        # The shape, byte-equal to a fresh vanilla migration's.
        await _migrate(conn, fresh)
        for table in ("job_events", "jobs_archive", "job_attempts_archive"):
            disabled_shape = await _shape_of(conn, schema, table)
            fresh_shape = await _shape_of(conn, fresh, table)
            assert disabled_shape == fresh_shape, (
                f"the restored {table} must be byte-equal to a fresh vanilla "
                f"migration's shape:\nrestored={disabled_shape}\nfresh={fresh_shape}"
            )

        # Every row present, counted per table.
        for table, count in seeded.items():
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count, (
                f"every {table} row must survive the disable"
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'DROP SCHEMA IF EXISTS "{fresh}" CASCADE')
        await conn.close()


async def test_disable_restores_vanilla_behaviors(timescale_dsn: str) -> None:
    """The behaviors the conversion traded away come back with the shape —
    the exact inverses of the conversion attack's pins: the restored bare
    primary keys reject duplicates, the restored
    ``job_attempts_archive -> jobs_archive`` FK rejects orphan attempts,
    and the event id sequence continues from the restored maximum."""
    schema = "tsbeh_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        await _seed_history(conn, schema, n_events=5, n_archive=2, n_attempts=2)

        await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )

        # Duplicate insert rejected by the restored PRIMARY KEY (id) — the
        # hypertable's widened UNIQUE (id, finished_at) accepted the same
        # id at a different finished_at; vanilla never did.
        jid = await conn.fetchval(f'SELECT id FROM "{schema}".jobs_archive LIMIT 1')
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs_archive SELECT * FROM "{schema}".jobs_archive '
                "WHERE id = $1",
                jid,
            )
        # Orphan attempts rejected by the restored FK — accepted on the
        # hypertable (the documented danger), never on vanilla.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                f"""INSERT INTO "{schema}".job_attempts_archive (job_id, attempt, started_at)
                VALUES ($1, 1, clock_timestamp())""",
                new_uuid(),
            )
        # The event id sequence continues from the restored maximum: the
        # next insert omits id and gets max + 1, not a collision.
        max_before = await conn.fetchval(f'SELECT max(id) FROM "{schema}".job_events')
        parent = await conn.fetchval(f'SELECT id FROM "{schema}".jobs LIMIT 1')
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind) '
            "VALUES ($1, clock_timestamp(), 'state_change')",
            parent,
        )
        max_after = await conn.fetchval(f'SELECT max(id) FROM "{schema}".job_events')
        assert max_after == max_before + 1, (
            "the restored sequence must continue past the restored rows"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_disable_is_idempotent(timescale_dsn: str) -> None:
    """Running disable twice converges: the second run restores nothing,
    reports nothing, and leaves the schema fully operational."""
    schema = "tsdis2_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=10, n_archive=3, n_attempts=3)

        first = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert set(first.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }

        second = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert second.converted == (), "an already-plain schema must not re-restore"
        assert second.retention_policies == ()
        assert second.compression_policies == ()
        assert await _hypertable_names(conn, schema) == set()
        for table, count in seeded.items():
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count
        # The schema stays fully operational after both runs.
        parent = await conn.fetchval(f'SELECT id FROM "{schema}".jobs LIMIT 1')
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind) '
            "VALUES ($1, clock_timestamp(), 'state_change')",
            parent,
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_enable_disable_enable_round_trip(timescale_dsn: str) -> None:
    """The full round trip converges with rows preserved at every step:
    enable -> seed -> disable -> enable. The re-enable re-converts the
    restored vanilla tables (rows and all, ``migrate_data``) and
    re-registers every policy."""
    schema = "tsround_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        first = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert set(first.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=50, n_archive=20, n_attempts=30)

        disabled = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert set(disabled.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        assert await _hypertable_names(conn, schema) == set()
        for table, count in seeded.items():
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count

        again = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert set(again.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, "the re-enable must re-convert the restored vanilla tables"
        assert await _hypertable_names(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        n_policies = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            schema,
        )
        assert n_policies == 3, "the re-enable re-registers every retention policy"
        for table, count in seeded.items():
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count, (
                f"every {table} row must survive the full round trip"
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── The columnstore adoption ─────────────────────────────────────────────


async def test_compression_adopted_on_archives_only(
    ts_conn: asyncpg.Connection, ts_schema: str, timescale_dsn: str
) -> None:
    """After enable, the archive tables carry the measured columnstore
    settings and a registered compression policy; ``job_events`` stays
    rowstore (measured: nothing to gain); the hot page still reads; and
    a re-run converges without duplicating the policy."""
    # The per-column settings, via the information view: segmentby
    # (actor, queue) / orderby finished_at DESC on the archive;
    # segmentby job_id / orderby started_at DESC on the attempts. Never
    # the per-row-unique id (the compression-ratio anti-pattern).
    settings_rows = await ts_conn.fetch(
        "SELECT hypertable_name, attname, segmentby_column_index, "
        "orderby_column_index, orderby_asc, orderby_nullsfirst "
        "FROM timescaledb_information.compression_settings "
        "WHERE hypertable_schema = $1",
        ts_schema,
    )
    by_table: dict[str, dict[str, tuple[Any, ...]]] = {}
    for r in settings_rows:
        by_table.setdefault(r["hypertable_name"], {})[r["attname"]] = (
            r["segmentby_column_index"],
            r["orderby_column_index"],
            r["orderby_asc"],
            r["orderby_nullsfirst"],
        )
    assert by_table["jobs_archive"] == {
        "actor": (1, None, None, None),
        "queue": (2, None, None, None),
        "finished_at": (None, 1, False, True),
    }, by_table.get("jobs_archive")
    assert by_table["job_attempts_archive"] == {
        "job_id": (1, None, None, None),
        "started_at": (None, 1, False, True),
    }, by_table.get("job_attempts_archive")
    assert "job_events" not in by_table, "job_events must stay rowstore"

    # The policy is registered per archive hypertable, compress_after at
    # one chunk interval (the 2-day test retention clamps to 1-day
    # chunks — a chunk compresses once it has stopped receiving rows).
    policy_jobs = await ts_conn.fetch(
        "SELECT hypertable_name, proc_name, config FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name IN "
        "('policy_compression', 'policy_columnstore')",
        ts_schema,
    )
    by_hypertable = {r["hypertable_name"]: r for r in policy_jobs}
    assert set(by_hypertable) == {"jobs_archive", "job_attempts_archive"}
    assert all('"compress_after": "1 day"' in r["config"] for r in by_hypertable.values()), {
        k: str(v["config"]) for k, v in by_hypertable.items()
    }
    assert (
        await ts_conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND hypertable_name = 'job_events' "
            "AND proc_name IN ('policy_compression', 'policy_columnstore')",
            ts_schema,
        )
        == 0
    ), "job_events must carry no compression policy"

    # The hot page read still works: the youngest chunk is rowstore and
    # the newest-first read returns the row just seeded.
    jid = await _seed_archive_row(ts_conn, schema=ts_schema, finished_at=datetime.now(UTC))
    hot = await ts_conn.fetch(
        f'SELECT id FROM "{ts_schema}".jobs_archive ORDER BY finished_at DESC LIMIT 1'
    )
    assert hot and hot[0]["id"] == jid, "the hot page read must still work"

    # The re-run converges: same report entries, no duplicated policy.
    await _schedule_policies(
        ts_conn, ts_schema, next_start=datetime.now(UTC) + timedelta(days=3650)
    )
    again = await enable_hypertables(
        ts_conn, schema=ts_schema, settings=_ts_settings(timescale_dsn, ts_schema)
    )
    assert again.converted == ()
    assert again.compression_policies == (
        "jobs_archive:1 days",
        "job_attempts_archive:1 days",
    )
    n_jobs = await ts_conn.fetchval(
        "SELECT count(*) FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name IN "
        "('policy_compression', 'policy_columnstore')",
        ts_schema,
    )
    assert n_jobs == 2, "a re-run must not duplicate the compression policies"


async def test_decompression_guc_warning_fires_at_default_and_quiets_when_raised(
    timescale_dsn: str, ts_schema: str
) -> None:
    """The GUC prerequisite is loud, never a silent trap: on a server at
    the 100000 default the report carries the warning (and the log fires
    the same WARNING event); with the budget raised on the session, the
    same enable run reports no warning and converges without
    duplicating the compression policies."""
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)
        settings = _ts_settings(timescale_dsn, ts_schema)
        report = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        warning = report.decompression_guc_warning
        assert warning is not None, "a server at the 100000 default must carry the loud warning"
        assert "max_tuples_decompressed_per_dml_transaction" in warning
        assert "100000" in warning

        # Raise the budget to unlimited on this session (the GUC is
        # user-settable — the same knob the benchmark's server flags
        # pin): the identical run reports no warning.
        await conn.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        again = await enable_hypertables(conn, schema=ts_schema, settings=settings)
        assert again.converted == ()
        assert again.decompression_guc_warning is None, "a raised budget must not warn"
        # And the remove-then-add convergence held across both runs.
        n_jobs = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name IN "
            "('policy_compression', 'policy_columnstore')",
            ts_schema,
        )
        assert n_jobs == 2, "no run may duplicate the compression policies"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


# ── The disable crash matrix: every swap stage, converge on re-run ────────


class _CrashAtSwapStageConn:
    """A forwarding proxy that dies at ONE disable-swap stage.

    Every statement runs for real against the server until the stage's
    statement arrives; the proxy raises INSTEAD of forwarding it — the
    same observable shape as the process being SIGKILLed at that exact
    point of the swap, deterministic and without any sleep. The stages,
    matched on the swap's own statements:

    * ``after-copy``: the trash RENAME (the copy is verified, no name has
      moved) — dies before the rename.
    * ``after-trash-rename``: the staging table's SET SCHEMA move-in —
      dies after the trash rename, with the rows in two places.
    * ``after-set-schema``: the twin-guarded rows-return INSERT — dies
      after the move-in (and the retype and the sequence anchor), before
      any row returned.
    * ``after-insert``: the trash DROP — dies with the rows already live
      in the vanilla table, both orphan copies still on disk.
    """

    _STAGES = (
        "after-copy",
        "after-trash-rename",
        "after-set-schema",
        "after-insert",
    )

    def __init__(self, inner: asyncpg.Connection, stage: str, victim_table: str) -> None:
        assert stage in self._STAGES
        self._inner = inner
        self._stage = stage
        self._victim = victim_table
        self._fired = False

    def _hits(self, sql: str) -> bool:
        statement = sql.strip()
        if self._stage == "after-insert":
            # The trash drop names only the trash table; the plain victim
            # token never appears.
            return statement.startswith("DROP TABLE") and (
                f'"{self._victim}__hypertable_trash"' in statement
            )
        # The quoted token disambiguates: '"job_events"' matches the swap
        # statement about job_events itself, never the trash-table name
        # '"job_events__hypertable_trash"' another table's stage runs on.
        if f'"{self._victim}"' not in statement:
            return False
        if self._stage == "after-copy":
            return statement.startswith("ALTER TABLE") and "RENAME TO" in statement
        if self._stage == "after-trash-rename":
            return statement.startswith("ALTER TABLE") and "SET SCHEMA" in statement
        return statement.startswith("INSERT INTO") and "WHERE NOT EXISTS" in statement

    async def execute(self, sql: str, *args: Any) -> str:
        if not self._fired and self._hits(sql):
            self._fired = True
            raise RuntimeError(f"injected crash at the disable swap stage: {self._stage}")
        return await self._inner.execute(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        return await self._inner.fetch(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


_CRASH_MATRIX_SEED = (50, 20, 30)


async def _disable_crash_matrix_case(
    timescale_dsn: str,
    schema: str,
    stage: str,
    victim_table: str,
) -> None:
    """One matrix cell: crash the disable mid-swap on *victim_table* at
    *stage*, then re-run clean and demand the full vanilla shape with
    EVERY row — the structural convergence, never a stranded heap."""
    fresh = "tsfresh_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=50, n_archive=20, n_attempts=30)
        assert set(seeded.values()) == set(_CRASH_MATRIX_SEED)

        proxy = _CrashAtSwapStageConn(conn, stage, victim_table)  # pyright: ignore[reportArgumentType]  # Why: the proxy IS the contract under test.
        with pytest.raises(RuntimeError, match="injected crash"):
            await disable_hypertables(
                proxy, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
            )
        assert proxy._fired  # Why: the crash must actually have fired at the stage under test.

        # The re-run converges: the vanilla shape, every row, nothing left.
        report = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert await _hypertable_names(conn, schema) == set(), (
            f"stage {stage}: the converging re-run must leave zero hypertables"
        )
        # The crashed run's completed swaps stay done (the swap order is
        # jobs_archive, job_attempts_archive, job_events): the re-run
        # converts exactly the victim — by crash convergence or the normal
        # swap — and every table after it in the order.
        order = ("jobs_archive", "job_attempts_archive", "job_events")
        expected_converted = set(order[order.index(victim_table) :])
        assert set(report.converted) == expected_converted, (
            f"stage {stage}: the re-run must convert exactly the crashed "
            f"table and the stragglers after it, got {report.converted}"
        )
        for table, count in seeded.items():
            live = await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
            assert live == count, (
                f"stage {stage}: every {table} row must survive the crash and the "
                f"converging re-run ({live} of {count})"
            )
        for orphan in (
            "jobs_archive__restore",
            "job_attempts_archive__restore",
            "job_events__restore",
            "jobs_archive__hypertable_trash",
            "job_attempts_archive__hypertable_trash",
            "job_events__hypertable_trash",
        ):
            left = await conn.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{orphan}"'
            )
            assert not left, f"stage {stage}: {orphan} must be gone after the re-run"
        staging_left = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{schema}__vanilla".jobs'
        )
        assert not staging_left, f"stage {stage}: the staging schema must be gone"

        # The converged shape is byte-equal to a fresh vanilla migration's.
        await _migrate(conn, fresh)
        for table in ("job_events", "jobs_archive", "job_attempts_archive"):
            disabled_shape = await _shape_of(conn, schema, table)
            fresh_shape = await _shape_of(conn, fresh, table)
            assert disabled_shape == fresh_shape, (
                f"stage {stage}: the converged {table} must be byte-equal to a "
                f"fresh vanilla migration's shape:\nrestored={disabled_shape}\n"
                f"fresh={fresh_shape}"
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'DROP SCHEMA IF EXISTS "{fresh}" CASCADE')
        await conn.close()


@pytest.mark.parametrize(
    "stage,victim_table",
    [
        ("after-copy", "job_events"),
        ("after-trash-rename", "job_events"),
        ("after-set-schema", "job_events"),
        ("after-insert", "job_events"),
        ("after-trash-rename", "jobs_archive"),
    ],
)
async def test_disable_crash_at_every_swap_stage_converges(
    timescale_dsn: str, stage: str, victim_table: str
) -> None:
    """The crash matrix: a disable killed at EVERY swap stage converges on
    the re-run to the full vanilla shape with EVERY row.

    The swap is rename-first (the copy is count-verified, the hypertable
    RENAMES to ``{table}__hypertable_trash``, the migration-built table
    moves into the freed name, the rows return from the trash twin-
    verified, and only then is the trash dropped), so after the rename the
    rows exist in TWO places and no order of death loses them. The re-run's
    FIRST act — before any ``DROP SCHEMA CASCADE`` — finishes every crashed
    table's move under the same count/twin verification. Pinned here at
    every stage, on the last table of the swap order (job_events, so the
    earlier tables' completions ride along) and on the first (jobs_archive,
    whose retype dependency is the one the old ordering's CASCADE could
    destroy)."""
    await _disable_crash_matrix_case(
        timescale_dsn, f"tsmtx_{new_uuid().hex[:12]}", stage, victim_table
    )


async def test_disable_converges_legacy_drop_before_set_debris(timescale_dsn: str) -> None:
    """Hostile cell the matrix cannot reach by crashing the CURRENT swap:
    the pre-rename-first ordering's DROP-before-SET window (the fix commit's
    B4 state). A schema crashed under the OLD ordering holds NO live table,
    NO trash, the restore heap stranded with the rows, and the staging
    schema still holding the vanilla table. ``_converge_crashed_swaps``'s
    fourth state must move the staging table in, absorb the heap twin-
    first, and leave the vanilla shape with EVERY row — before any ``DROP
    SCHEMA CASCADE`` can eat the staging table the recovery needs."""
    schema = "tslegacy_" + new_uuid().hex[:12]
    fresh = "tsfresh_" + new_uuid().hex[:12]
    staging = schema + "__vanilla"
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=50, n_archive=20, n_attempts=30)

        # Hand-build the old ordering's debris: the heap copy exists, the
        # hypertable is GONE (the legacy DROP ran before its SET SCHEMA),
        # and the staging schema still holds the vanilla table — exactly
        # the state the rename-first swap makes unreachable but the
        # convergence still documents and must still finish.
        await conn.execute(
            f'CREATE TABLE "{schema}".jobs_archive__restore '
            f'AS SELECT * FROM "{schema}".jobs_archive'
        )
        await conn.execute(f'DROP TABLE "{schema}".jobs_archive')
        await apply_pending(conn, schema=staging)
        live_before = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{schema}".jobs_archive'
        )
        assert not live_before, "the hand-built debris must have no live jobs_archive"

        report = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert "jobs_archive" in report.converted, (
            f"the legacy debris must converge as a converted table, got {report.converted}"
        )
        assert await _hypertable_names(conn, schema) == set(), (
            "the converging run must leave zero hypertables"
        )
        for table, count in seeded.items():
            live = await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
            assert live == count, (
                f"every {table} row must survive the legacy debris and the "
                f"converging re-run ({live} of {count})"
            )
        heap_left = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{schema}".jobs_archive__restore'
        )
        assert not heap_left, "the absorbed heap must be gone"
        staging_left = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", f'"{staging}".jobs'
        )
        assert not staging_left, "the staging schema must be gone after the convergence"

        # The converged shape is byte-equal to a fresh vanilla migration's.
        await _migrate(conn, fresh)
        disabled_shape = await _shape_of(conn, schema, "jobs_archive")
        fresh_shape = await _shape_of(conn, fresh, "jobs_archive")
        assert disabled_shape == fresh_shape, (
            f"the converged jobs_archive must be byte-equal to a fresh vanilla "
            f"migration's shape:\nrestored={disabled_shape}\nfresh={fresh_shape}"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'DROP SCHEMA IF EXISTS "{fresh}" CASCADE')
        await conn.close()


async def test_disable_refuses_vanilla_key_collisions_loudly(timescale_dsn: str) -> None:
    """The one unrecoverable mismatch refuses loudly BEFORE any name
    moves: the hypertable's widened uniqueness admits rows (same id,
    different partition-column value) the restored vanilla table's primary
    key cannot hold. The swap refuses to choose which row survives; the
    hypertable is untouched after the refusal."""
    schema = "tsdup_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        # The collision: the SAME id twice at different finished_at — the
        # widened UNIQUE (id, finished_at) holds both, vanilla's bare
        # PRIMARY KEY (id) cannot hold either of them twice.
        jid = new_uuid()
        await conn.executemany(
            f"""INSERT INTO {schema}.jobs_archive (
                id, actor, queue, payload, status, attempt, max_attempts,
                retry_kind, expire_at, finished_at)
            VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', 0, 3, 'transient',
                clock_timestamp() + interval '365 days', clock_timestamp() + ($2 * interval '1 hour'))""",
            [(jid, i) for i in range(2)],
        )
        with pytest.raises(RuntimeError, match="vanilla-key collision"):
            await disable_hypertables(
                conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
            )
        # Nothing was mutated: the hypertable is still armed, rows intact.
        assert await _hypertable_names(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive') == 2
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


class _WindowWriterConn:
    """A forwarding proxy that opens the absorb's drop window on purpose.

    The serialization claim under test: ``_absorb_orphan_rows``' final
    twin check and its ``DROP TABLE`` share one transaction behind
    ``LOCK TABLE ... IN EXCLUSIVE MODE``, so the one writer that could
    otherwise destroy a row silently — a worker transaction that opened
    the orphan before the trash rename and commits after — cannot land a
    commit inside the window. This proxy proves the claim deterministically,
    without any real-world timing luck: when the ``LOCK TABLE`` statement
    for *victim_trash* arrives, it fires a REAL concurrent ``INSERT`` into
    that same table from a SECOND connection (the writer the lock must
    serialize against), racing it against the lock itself. Exactly two
    outcomes are possible, and both prove no row is destroyed silently:

    * the writer commits BEFORE the lock is granted → its row is in the
      orphan when the window opens and the twin-guard absorbs it into the
      live table;
    * the lock is granted first → the writer QUEUES behind it (measured
      on 2.30.1/pg18: the holder's in-transaction ``DROP TABLE`` proceeds
      past the queued writer, and the writer wakes at the commit to
      ``relation does not exist``) — its row never landed, and the writer
      KNOWS, loudly.

    Either way the window shut with every row accounted for.
    """

    def __init__(
        self,
        inner: asyncpg.Connection,
        writer_conn: asyncpg.Connection,
        victim_trash: str,
        insert_sql: str,
        insert_args: tuple[Any, ...],
    ) -> None:
        self._inner = inner
        self._writer_conn = writer_conn
        self._victim = victim_trash
        self._insert_sql = insert_sql
        self._insert_args = insert_args
        self._fired = False
        self.writer_task: asyncio.Task[str] | None = None

    async def _fire_writer(self) -> str:
        """The second connection's real INSERT — the row the lock must
        keep out of the drop window (or hand to the twin-guard)."""
        try:
            await self._writer_conn.execute(self._insert_sql, *self._insert_args)
            return "committed"
        except asyncpg.UndefinedTableError:
            # The blocked branch: the writer queued behind the lock, the
            # drop committed, the relation is gone. The row was never
            # landed and the writer holds the error — nothing silent.
            return "relation-gone"

    async def execute(self, sql: str, *args: Any) -> str:
        stmt = sql.lstrip()
        if not self._fired and stmt.startswith("LOCK TABLE") and f'"{self._victim}"' in stmt:
            self._fired = True
            self.writer_task = asyncio.create_task(self._fire_writer())
        return await self._inner.execute(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        return await self._inner.fetch(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_absorb_lock_holds_the_drop_window_shut_against_a_live_writer(
    timescale_dsn: str,
) -> None:
    """The absorb's ``LOCK TABLE ... IN EXCLUSIVE MODE`` serialization pin.

    enable -> seed -> disable through a proxy that, the instant the
    absorb's ``LOCK TABLE`` on the trash arrives, fires a real concurrent
    writer INSERT into that trash from a second connection. The window
    the lock owns is the twin-check → DROP pair; the race decides only
    WHICH proof runs (absorbed, or woken by the drop), never whether a
    row can vanish: the trash is gone at the end and every row is
    accounted — live count == seed + (1 iff the writer landed)."""
    schema = "tslock_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    writer_conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        await enable_hypertables(conn, schema=schema, settings=_ts_settings(timescale_dsn, schema))
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=10, n_archive=3, n_attempts=3)

        # The concurrent writer's row: a fresh id in the archive row's
        # own shape, aimed at the trash table the absorb is about to
        # lock and drop.
        writer_id = new_uuid()
        now = datetime.now(UTC)
        insert_sql = f"""INSERT INTO "{schema}".jobs_archive__hypertable_trash (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'test_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)"""
        insert_args = (
            writer_id,
            now,
            now + timedelta(hours=1),
            now,
            now + timedelta(days=365),
        )

        proxy = _WindowWriterConn(
            conn,  # pyright: ignore[reportArgumentType]  # Why: the proxy IS the contract under test.
            writer_conn,
            "jobs_archive__hypertable_trash",
            insert_sql,
            insert_args,
        )
        await disable_hypertables(
            proxy, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert proxy._fired, "the LOCK TABLE window must have been reached and opened"
        assert proxy.writer_task is not None
        outcome = await asyncio.wait_for(proxy.writer_task, 30)
        assert outcome in ("committed", "relation-gone"), (
            f"the concurrent writer must either commit before the lock or fail "
            f"with the relation gone, got: {outcome}"
        )
        landed = outcome == "committed"

        # The disable completed: zero hypertables, every orphan gone.
        assert await _hypertable_names(conn, schema) == set()
        for orphan in (
            "jobs_archive__hypertable_trash",
            "jobs_archive__restore",
            "job_attempts_archive__hypertable_trash",
            "job_attempts_archive__restore",
            "job_events__hypertable_trash",
            "job_events__restore",
        ):
            left = await conn.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{orphan}"'
            )
            assert not left, f"{orphan} must be gone after the disable"

        # Every row accounted, exactly: the seed survived, and the
        # writer's row is in the live table IFF its INSERT committed
        # (absorbed by the twin-guard) — never silently destroyed.
        for table, count in seeded.items():
            live = await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
            assert live == count, (
                f"every {table} row must survive the disable and the concurrent "
                f"writer ({live} of {count})"
            )
        present = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', writer_id
        )
        assert present == (1 if landed else 0), (
            f"the writer's row must be live iff the writer committed "
            f"(outcome={outcome}, found {present})"
        )
        total = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive')
        assert total == seeded["jobs_archive"] + (1 if landed else 0), (
            f"live count must be seed + absorbed ({outcome})"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
        await writer_conn.close()


# ── The census under attack: race window, skip premise, remediation arms ──

_DOCS = Path(__file__).resolve().parent.parent / "docs"


class _CensusRaceWriterConn:
    """A forwarding proxy that opens the census's race window on purpose.

    The census is a point-in-time autocommit SELECT; the conversion's DDL
    runs in separate transactions after it. This proxy proves the window
    deterministically, without timing luck: when ``jobs_archive``'s
    ``create_hypertable`` statement arrives — the census LONG committed,
    and the whole ``job_events`` conversion between — it fires a REAL
    concurrent ``INSERT`` of a NULL ``finished_at`` row into the still-
    vanilla archive from a SECOND connection. This is the honest posture,
    pinned, not endorsed: no TaskQ writer can produce that row (every
    production arm stamps ``finished_at``; the only archive INSERT is the
    prune's COPY, gated on non-NULL ``finished_at``; nothing updates the
    archive), so the only writer that can poison the window is direct SQL
    during the enabling deploy — exactly the writer the deploy window's
    documented maintenance-window exclusivity excludes.
    """

    def __init__(self, inner: asyncpg.Connection, writer: asyncpg.Connection, schema: str) -> None:
        self._inner = inner
        self._writer = writer
        self._schema = schema
        self.fired = False

    async def execute(self, sql: str, *args: Any) -> str:
        if (
            not self.fired
            and "create_hypertable" in sql
            and any("jobs_archive" in str(a) for a in args)
        ):
            self.fired = True
            await self._writer.execute(
                f"""INSERT INTO "{self._schema}".jobs_archive (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    scheduled_at, finished_at, archived_at, expire_at)
                VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                    now(), NULL, now(), now() + interval '365 days')""",
                new_uuid(),
            )
        return await self._inner.execute(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._inner.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        return await self._inner.fetch(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_census_race_window_reenters_the_mid_migrate_data_failure(
    timescale_dsn: str,
) -> None:
    """The census's race window: pinned as REAL, with the recovery.

    The census counts in its own autocommit transaction and every DDL
    statement commits separately, so a NULL ``finished_at`` row written
    between the census and ``jobs_archive``'s ``create_hypertable`` re-
    enters the pre-fix failure shape: ``NotNullViolationError`` mid-
    ``migrate_data``, stranding the half-converted schema (``job_events``
    converted, the archive's primary key already dropped). A lock cannot
    close the window — the conversion is deliberately per-statement
    autocommit DDL, so no lock taken at census time can span it — and no
    TaskQ writer can fire it (see the proxy's docstring), so the honest
    posture is the deploy window's documented exclusivity: no writers
    against the schema during the enabling deploy. Pinned here so the
    window's size and the stranded state it produces are FACTS, never
    assumptions — and so the recovery is too: the refusal's own
    remediation, applied to the stranded schema, converges it on the
    re-run (the census counts the table whenever it is still vanilla,
    wrecked or not).
    """
    schema = "tsrace_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    writer = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        # Real volume so the window is the deploy-shaped one (job_events
        # converts whole before the archive's create_hypertable), though
        # the proxy makes the interleaving deterministic at any size.
        parents = [new_uuid() for _ in range(20)]
        await conn.executemany(
            f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
                retry_kind, status, scheduled_at)
            VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded', now())""",
            [(p,) for p in parents],
        )
        await conn.execute(
            f"""INSERT INTO {schema}.job_events (job_id, occurred_at, kind)
            SELECT p.id, now() - (g || ' minutes')::interval, 'state_change'
            FROM generate_series(1, 2000) g
            JOIN (SELECT id, row_number() OVER (ORDER BY id) - 1 AS n
                  FROM {schema}.jobs) p ON p.n = g % 20"""
        )

        proxy = _CensusRaceWriterConn(conn, writer, schema)
        with pytest.raises(asyncpg.NotNullViolationError, match="finished_at"):
            await enable_hypertables(
                proxy,  # pyright: ignore[reportArgumentType]  # Why: the proxy IS the contract under test.
                schema=schema,
                settings=_ts_settings(timescale_dsn, schema),
            )
        assert proxy.fired, "the census-race write must have fired inside the window"

        # The pre-fix stranded state, exactly: job_events converted whole,
        # the archive's pkey dropped, the poison row inside the table.
        ht = await conn.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            schema,
        )
        assert {r["table_name"] for r in ht} == {"job_events"}
        pkey = await conn.fetchval(
            """SELECT count(*) FROM pg_constraint con
               JOIN pg_class c ON c.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = $1 AND c.relname = 'jobs_archive' AND con.contype = 'p'""",
            schema,
        )
        assert pkey == 0, "the mid-migrate_data failure must have dropped the archive pkey"
        n_null = await conn.fetchval(
            f"SELECT count(*) FROM {schema}.jobs_archive WHERE finished_at IS NULL"
        )
        assert n_null == 1, "the racing writer's poison row must be in the stranded table"

        # The recovery is the refusal's own remediation, applied to the
        # wreck: discard the debris, re-run, and the stragglers convert.
        await conn.execute(f"DELETE FROM {schema}.jobs_archive WHERE finished_at IS NULL")
        report = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert set(report.converted) == {"jobs_archive", "job_attempts_archive"}
        assert await _hypertable_names(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
        await writer.close()


async def test_converted_partition_columns_are_not_null_and_dml_rejects_null(
    ts_conn: asyncpg.Connection,
    ts_schema: str,
) -> None:
    """The already-hypertable skip's premise, probed in the catalogs.

    The census counts only while ``jobs_archive`` is still vanilla; on a
    converted schema it skips. That skip hides a live failure ONLY IF a
    converted schema could carry a NULL partition key. It cannot, twice
    over: ``create_hypertable`` leaves every partition column ``NOT NULL``
    in ``pg_attribute`` (the archive's nullable ``finished_at`` included —
    the conversion itself tightens the column), and DML into the converted
    table rejects a NULL partition key with ``NotNullViolationError``. The
    skip therefore skips only a schema where the NULL shape is structurally
    impossible.
    """
    nn = await ts_conn.fetch(
        """SELECT c.relname AS tbl, a.attname, a.attnotnull
           FROM pg_attribute a
           JOIN pg_class c ON c.oid = a.attrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1
             AND (c.relname, a.attname) IN (
                 ('jobs_archive', 'finished_at'),
                 ('job_attempts_archive', 'started_at'),
                 ('job_events', 'occurred_at'))
             AND a.attnum > 0 AND NOT a.attisdropped""",
        ts_schema,
    )
    got = {r["tbl"]: r["attnotnull"] for r in nn}
    assert got == {
        "jobs_archive": True,
        "job_attempts_archive": True,
        "job_events": True,
    }, f"every converted partition column must be NOT NULL, got {got}"

    # The enforcement backstop: the NULL insert vanilla accepts (the
    # archive mirrors jobs, where finished_at is NULL until terminal) is
    # rejected at DML time on the converted schema.
    with pytest.raises(asyncpg.NotNullViolationError):
        await ts_conn.execute(
            f"""INSERT INTO {ts_schema}.jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                scheduled_at, finished_at, archived_at, expire_at)
            VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                now(), NULL, now(), now() + interval '365 days')""",
            new_uuid(),
        )


async def test_census_absent_table_skip_fails_loudly_downstream(
    timescale_dsn: str,
) -> None:
    """The absent-table census skip never turns a missing schema into a
    silent success.

    In the product path the ordering question is settled by the deploy
    wiring: ``taskq migrate up`` applies pending migrations BEFORE
    ``enable_hypertables``, inside the same advisory lock, so the archive
    table always exists when the census runs (the deploy E2E's fresh-schema
    path pins that). The skip only guards a pathological direct caller on
    a schema nothing migrated — and for that caller the DDL path fails
    LOUDLY immediately after (``UndefinedTableError`` on the first
    conversion statement), never a quiet "nothing to do".
    """
    schema = "tsabsent_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        with pytest.raises(asyncpg.UndefinedTableError):
            await enable_hypertables(
                conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
            )
        ht = await conn.fetch(
            "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
            schema,
        )
        assert ht == [], "nothing may convert on a schema that was never migrated"
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_remediation_arms_survive_hostile_debris_and_the_delete_cascades(
    timescale_dsn: str,
    ts_schema: str,
) -> None:
    """Both documented remediations work on the DEBRIS shapes, verified.

    * The UPDATE arm's source can never be NULL: ``archived_at`` is
      ``timestamptz NOT NULL DEFAULT now()`` since the initial migration,
      so a hand-inserted debris row that omits it defaults it (an explicit
      NULL is a NOT NULL violation) — there is no archive row on the
      bundled schema whose completion stamps NULL back.
    * The DELETE arm is the vanilla schema's own cascade: at census time
      the attempts FK is still in place, so discarding a debris job takes
      its ``job_attempts_archive`` rows with it — the operator's "discard"
      discards the attempts history too, which is what the word means.
    Either arm leaves the census counting zero and the re-run converts.
    """
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, ts_schema)

        # Hostile debris: omit archived_at entirely (the shape a hand
        # INSERT without the column produces) — it defaults, non-NULL.
        await conn.execute(
            f"""INSERT INTO {ts_schema}.jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                scheduled_at, finished_at, expire_at)
            VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                now(), NULL, now() + interval '365 days')""",
            new_uuid(),
        )
        bad = await conn.fetchval(
            f"SELECT count(*) FROM {ts_schema}.jobs_archive "
            "WHERE finished_at IS NULL AND archived_at IS NULL"
        )
        assert bad == 0, (
            "archived_at NOT NULL DEFAULT now() must keep the UPDATE arm's source non-NULL"
        )
        # And an explicit NULL archived_at is refused outright.
        with pytest.raises(asyncpg.NotNullViolationError):
            await conn.execute(
                f"""INSERT INTO {ts_schema}.jobs_archive (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    scheduled_at, finished_at, archived_at, expire_at)
                VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                    now(), NULL, NULL, now() + interval '365 days')""",
                new_uuid(),
            )

        # The DELETE arm on debris that carries attempts history: the
        # vanilla FK is still in place at census time (the refusal fired
        # before any DDL), so discard cascades.
        debris_id = new_uuid()
        await conn.execute(
            f"""INSERT INTO {ts_schema}.jobs_archive (
                id, actor, queue, payload, max_attempts, retry_kind, status,
                scheduled_at, finished_at, archived_at, expire_at)
            VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                now(), NULL, now(), now() + interval '365 days')""",
            debris_id,
        )
        await conn.execute(
            f"""INSERT INTO {ts_schema}.job_attempts_archive (job_id, attempt, started_at)
            VALUES ($1, 1, now())""",
            debris_id,
        )
        attempts_before = await conn.fetchval(
            f"SELECT count(*) FROM {ts_schema}.job_attempts_archive WHERE job_id = $1", debris_id
        )
        assert attempts_before == 1
        refused = 0
        try:
            await enable_hypertables(
                conn, schema=ts_schema, settings=_ts_settings(timescale_dsn, ts_schema)
            )
        except TimescaleDBUnavailableError:
            refused = 1
        assert refused == 1, "the census must refuse while the debris (and its attempts) exist"
        await conn.execute(f"DELETE FROM {ts_schema}.jobs_archive WHERE finished_at IS NULL")
        attempts_after = await conn.fetchval(
            f"SELECT count(*) FROM {ts_schema}.job_attempts_archive WHERE job_id = $1", debris_id
        )
        assert attempts_after == 0, "discarding the debris job must cascade to its attempts history"

        # Either way the re-run converts: the census counts remaining
        # NULLs only.
        report = await enable_hypertables(
            conn, schema=ts_schema, settings=_ts_settings(timescale_dsn, ts_schema)
        )
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{ts_schema}" CASCADE')
        await conn.close()


async def test_disable_round_trip_after_refusal_path_enable(timescale_dsn: str) -> None:
    """The disable mirror on a schema that enabled VIA the refusal: the
    byte-equal convergence holds.

    The refusal-path enable is the same conversion once the census passes;
    the mirror must not be able to tell the difference. migrate -> debris
    -> refused -> the message's UPDATE arm -> enable -> seed -> disable:
    zero hypertables, the vanilla shape byte-equal to a fresh migration's,
    every row present."""
    schema = "tsdisr_" + new_uuid().hex[:12]
    fresh = "tsfresh_" + new_uuid().hex[:12]
    conn = await asyncpg.connect(timescale_dsn)
    try:
        await _migrate(conn, schema)
        # Two debris rows, refused loudly, then the documented cure.
        for _ in range(2):
            await conn.execute(
                f"""INSERT INTO {schema}.jobs_archive (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    scheduled_at, finished_at, archived_at, expire_at)
                VALUES ($1, 'a', 'q', '{{}}'::jsonb, 3, 'transient', 'succeeded',
                    now(), NULL, now(), now() + interval '365 days')""",
                new_uuid(),
            )
        with pytest.raises(
            TimescaleDBUnavailableError, match=r"2 row\(s\) whose finished_at is NULL"
        ):
            await enable_hypertables(
                conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
            )
        await conn.execute(
            f"UPDATE {schema}.jobs_archive SET finished_at = archived_at WHERE finished_at IS NULL"
        )

        report = await enable_hypertables(
            conn, schema=schema, settings=_ts_settings(timescale_dsn, schema)
        )
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await _schedule_policies(conn, schema, next_start=datetime.now(UTC) + timedelta(days=3650))
        seeded = await _seed_history(conn, schema, n_events=50, n_archive=20, n_attempts=30)

        disabled = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(timescale_dsn, schema)
        )
        assert set(disabled.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        assert await _hypertable_names(conn, schema) == set()
        await _migrate(conn, fresh)
        for table in ("job_events", "jobs_archive", "job_attempts_archive"):
            disabled_shape = await _shape_of(conn, schema, table)
            fresh_shape = await _shape_of(conn, fresh, table)
            assert disabled_shape == fresh_shape, (
                f"the refusal-path schema's restored {table} must be byte-equal to a "
                f"fresh vanilla migration's shape:\nrestored={disabled_shape}\nfresh={fresh_shape}"
            )
        for table, count in seeded.items():
            expected = count + (2 if table == "jobs_archive" else 0)
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == expected, (
                "every seeded row plus the two COMPLETED debris rows (the UPDATE "
                "arm keeps them, stamped) must survive the whole round trip"
            )
        # And the remediated debris rows carry no NULLs after all of it.
        assert (
            await conn.fetchval(
                f"SELECT count(*) FROM {schema}.jobs_archive WHERE finished_at IS NULL"
            )
            == 0
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'DROP SCHEMA IF EXISTS "{fresh}" CASCADE')
        await conn.close()


def test_census_point_in_time_posture_is_documented() -> None:
    """The docs state the census's honest posture: a point-in-time gate,
    not a lock — and what closes its window.

    The census counts in one transaction and the conversion commits
    statement by statement, so a NULL ``finished_at`` row written after
    the census (by the one writer that can produce it — direct SQL during
    the enabling deploy) re-enters the mid-``migrate_data`` failure. An
    operator reading the refusal section must not conclude the refusal is
    atomic against writers; the docs must name the window, the deploy
    window's exclusivity that covers it, and the recovery.
    """
    text = (_DOCS / "guides" / "timescaledb.md").read_text()
    section = text.split("## The NULL `finished_at` tail", 1)[1]
    section = section.split("## ", 1)[0]  # the census section only
    assert "point-in-time" in section, (
        "the census section must state that the census is a point-in-time gate, not a lock"
    )
    assert "autocommit" in section, (
        "the per-statement autocommit discipline is WHY no census-time lock can span the conversion"
    )
    assert "direct SQL" in section, (
        "the section must name the only writer that can poison the window"
    )
    assert "maintenance window" in section, (
        "the deploy window's exclusivity is what closes the window in practice"
    )
    assert "re-run" in section, "the recovery (remediate, re-run) must stay named"

# ruff: noqa: S608  # Why: schema is a fixed test identifier, every value is $-bound.
"""Upgrade-path attack: the hypertable flag cycle with WORK IN THE MIDDLE.

The pinned flag shapes (``tests/test_timescaledb_hypertables.py``) cover
conversion, re-enable convergence, and both engines' prune agreement.
The shape production actually walks when an operator flips the flag
mid-life has work landing between the flips:

    flag ON → jobs archived (real prunes into hypertable chunks)
    → flag OFF → more prunes (the vanilla statements now run against
      tables that are STILL hypertables, the conversion is forward-only)
    → a re-pended ghost whose archive row already exists (the fold guard
      under the off phase) → flag ON again.

Nothing may be orphaned across the whole cycle: every pruned live row
holds exactly one archive row with its attempts, the fold guard converges
the ghost instead of wedging, the migration ledger's checksums stay
intact (no hypertable DDL ever touches it), and the final re-enable
converges (no re-conversion, no duplicate policies).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq.migrate import (
    apply_pending,
    checksum_drifts,
    discover,
    list_applied,
    list_invalid_indexes,
)
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.pg import create_pending_job
from taskq.timescale import enable_hypertables
from taskq.worker._leader_shared import prune_terminal_jobs

pytestmark = pytest.mark.integration

_TIMESCALE_IMAGE = (
    os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or "timescale/timescaledb:2.30.1-pg18"
)
_AGED = datetime.now(UTC) - timedelta(hours=6)


@pytest.fixture(scope="module")
def flip_dsn() -> Iterator[str]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
async def flip_conn(flip_dsn: str) -> AsyncIterator[asyncpg.Connection]:
    conn = await asyncpg.connect(flip_dsn)
    try:
        yield conn
    finally:
        await conn.close()


def _settings(dsn: str, schema: str, *, hypertables: bool) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if hypertables else "false",
            "TASKQ_ARCHIVE_RETENTION_PERIOD": "172800s",  # 2 days
            "TASKQ_EVENT_RETENTION_PERIOD": "86400s",  # 1 day
        }
    )


async def _seed_terminal_job(
    conn: asyncpg.Connection, schema: str, *, with_attempt: bool = True
) -> object:
    jid = await create_pending_job(conn, schema, scheduled_at=_AGED)
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() - interval '6 hours' WHERE id = $1",
        jid,
    )
    if with_attempt:
        await conn.execute(
            f'INSERT INTO "{schema}".job_attempts (job_id, attempt, started_at, '
            "finished_at, outcome) VALUES ($1, 1, clock_timestamp() - interval '6 hours', "
            "clock_timestamp() - interval '6 hours', 'succeeded')",
            jid,
        )
    return jid


async def _counts(conn: asyncpg.Connection, schema: str, table: str) -> int:
    return int(await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"'))


async def test_flag_flip_cycle_with_work_in_the_middle(
    flip_dsn: str, flip_conn: asyncpg.Connection
) -> None:
    conn = flip_conn
    schema = "atk_flip_cycle"
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)

    # ── Phase 1: flag ON, convert, archive real jobs ──
    on = await enable_hypertables(
        conn, schema=schema, settings=_settings(flip_dsn, schema, hypertables=True)
    )
    assert set(on.converted) == {"job_events", "jobs_archive", "job_attempts_archive"}
    for _ in range(4):
        await _seed_terminal_job(conn, schema)
    pruned = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(hours=1)},
        archive_retention=timedelta(days=30),
        schema=schema,
    )
    assert pruned.total_deleted == 4
    assert await _counts(conn, schema, "jobs") == 0
    assert await _counts(conn, schema, "jobs_archive") == 4
    assert await _counts(conn, schema, "job_attempts_archive") == 4, (
        "each phase-1 job's attempt must ride into the hypertable archive"
    )

    # ── Phase 2: flag OFF. The conversion is forward-only: the tables are
    # still hypertables, and the vanilla prune statements must keep
    # landing on them.
    for _ in range(3):
        await _seed_terminal_job(conn, schema)
    pruned2 = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(hours=1)},
        archive_retention=timedelta(days=30),
        schema=schema,
    )
    assert pruned2.total_deleted == 3
    assert await _counts(conn, schema, "jobs") == 0
    assert await _counts(conn, schema, "jobs_archive") == 7, (
        "the off-phase prunes must archive into the still-hypertable tables"
    )
    assert await _counts(conn, schema, "job_attempts_archive") == 7

    # ── Phase 3 (still OFF): the fold guard against chunk residue. A
    # re-pended job whose id ALREADY holds an archive row re-enters the
    # prune; the guard must FOLD it (the standing archive row wins, the
    # live row is removed) instead of wedging on uniqueness.
    ghost = await _seed_terminal_job(conn, schema)
    await conn.execute(
        f'INSERT INTO "{schema}".jobs_archive '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, "
        "scheduled_at, schedule_to_close, finished_at, archived_at, expire_at) "
        f"SELECT id, actor, queue, payload, max_attempts, retry_kind, status, "
        "scheduled_at, schedule_to_close, finished_at, clock_timestamp(), "
        "clock_timestamp() + interval '30 days' "
        f'FROM "{schema}".jobs WHERE id = $1',
        ghost,
    )
    pruned3 = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(hours=1)},
        archive_retention=timedelta(days=30),
        schema=schema,
    )
    assert pruned3.total_deleted == 1, "the ghost must leave the live table either way"
    archive_dups = await conn.fetchval(
        f'SELECT count(*) FROM (SELECT id FROM "{schema}".jobs_archive '
        "GROUP BY id, finished_at HAVING count(*) > 1) d"
    )
    assert archive_dups == 0, "the fold must not write a second archive row"
    assert await _counts(conn, schema, "jobs") == 0
    ghost_attempt_archived = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts_archive a WHERE a.job_id = $1',
        ghost,
    )
    assert ghost_attempt_archived == 0, (
        "the fold's documented edge (the standing archive row is the "
        "pre-retry version; the folded job's post-retry attempts are "
        "lost with the live row's cascade) must hold identically on the "
        "still-hypertable shape"
    )
    orphan_attempts = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts_archive a '
        f'WHERE NOT EXISTS (SELECT 1 FROM "{schema}".jobs_archive j WHERE j.id = a.job_id)'
    )
    assert orphan_attempts == 0, (
        "every archived attempt's parent archive row must exist after the cycle"
    )

    # ── Phase 4: flag ON again. Already-hypertable tables must skip
    # conversion, policies must not duplicate, and the schema keeps
    # serving.
    again = await enable_hypertables(
        conn, schema=schema, settings=_settings(flip_dsn, schema, hypertables=True)
    )
    assert again.converted == (), "a converted schema must not re-convert"
    n_policies = await conn.fetchval(
        "SELECT count(*) FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        schema,
    )
    assert n_policies == 3, "the flip cycle must not duplicate retention policies"

    # ── Whole-cycle integrity ──
    assert await list_invalid_indexes(conn, schema) == []
    assert await checksum_drifts(conn, schema=schema) == {}
    assert await list_applied(conn, schema) == {m.key for m in discover()}
    # Every live-side artifact is gone; the archive holds exactly the
    # pruned population (4 + 3 prunes + the folded ghost's standing row).
    assert await _counts(conn, schema, "job_events") == 0, (
        "pruned jobs' events ride the cascade away; none may be orphaned"
    )
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_flip_cycle_checksum_ledger_survives_a_fresh_cycle(flip_dsn: str) -> None:
    """The ledger-level v2v check for the cycle: a schema that walked the
    flip cycle still verifies against the bundled files - a re-run of
    ``migrate up`` applies nothing and finds no drift."""
    conn = await asyncpg.connect(flip_dsn)
    schema = "atk_flip_ledger"
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await enable_hypertables(
            conn, schema=schema, settings=_settings(flip_dsn, schema, hypertables=True)
        )
        # Off phase: the deploy step issues zero SQL; the ledger is untouched.
        await enable_hypertables(
            conn, schema=schema, settings=_settings(flip_dsn, schema, hypertables=False)
        )
        await enable_hypertables(
            conn, schema=schema, settings=_settings(flip_dsn, schema, hypertables=True)
        )
        assert await checksum_drifts(conn, schema=schema) == {}
        applied = await apply_pending(conn, schema=schema)
        assert applied == [], "a cycled schema must have no pending migrations"
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()

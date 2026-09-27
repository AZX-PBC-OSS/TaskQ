"""The deploy-step E2E: the REAL ``taskq migrate up`` against a real
TimescaleDB, the flag on and off.

``tests/test_timescaledb_hypertables.py`` drives ``enable_hypertables``
directly (the conversion function's unit seam). This module drives the
OPERATOR's path instead: the CLI's deploy step (``migrate up``), which
owns the advisory lock, the migration application, the flag read, the
conversion call, and the capability-refusal exit - none of it mocked.
The pinned surface:

* the happy deploy: flag on, fresh schema - migrations AND conversion in
  one invocation, the hypertable shape exact;
* the mid-life enable: a populated vanilla database converts with every
  row preserved (the ``migrate_data`` promise, counted);
* the bounded-run interaction: ``--phase pre`` still converts (the
  current behavior, pinned so it cannot drift silently);
* the capability refusal: flag on, plain Postgres - exit 1, the setting
  NAMED, the schema and ledger left usable;
* flag-off after enable: zero down-conversion, policies persist;
* the disable mirror: the deployed-and-converted schema goes back to
  vanilla with every row counted and every traded-away behavior
  restored (the restored PK rejects duplicates, the restored FK rejects
  orphan attempts).
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels
from taskq.testing.pg import create_running_job
from taskq.timescale import disable_hypertables
from tests.test_timescaledb_hypertables import _TIMESCALE_IMAGE_DEFAULT

_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT


@pytest.fixture(scope="module")
def deploy_container() -> Iterator[Any]:
    """One timescaledb container per module; skips without Docker."""
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture(scope="module")
def deploy_dsn(deploy_container: Any) -> str:
    return deploy_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
def vanilla_container() -> Iterator[Any]:
    """A PLAIN postgres container (no timescale extension): the refusal E2E."""
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image="postgres:18",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture
def vanilla_dsn(vanilla_container: Any) -> str:
    return vanilla_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
def deploy_schema() -> str:
    return "tsdep_" + new_uuid().hex[:12]


def _invoke_migrate_up(
    dsn: str, schema: str, *, flag: bool = True, extra: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    """One REAL ``migrate up`` invocation - a subprocess, the operator's
    own path: the entrypoint's asyncio.run, the signal discipline, the
    env cascade, the exit code. Nothing of the CLI is imported into the
    test's process (its asyncio.run cannot coexist with pytest-asyncio's
    running loop, and it should not have to)."""
    return _invoke_migrate(dsn, schema, ["migrate", "up", *(extra or [])], flag=flag)


def _invoke_migrate_disable_hypertables(
    dsn: str, schema: str, *, flag: bool = False
) -> subprocess.CompletedProcess[str]:
    """One REAL ``migrate disable-hypertables`` invocation - the same
    subprocess discipline as ``_invoke_migrate_up``."""
    return _invoke_migrate(dsn, schema, ["migrate", "disable-hypertables"], flag=flag)


def _invoke_migrate(
    dsn: str, schema: str, argv: list[str], *, flag: bool
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "TASKQ_PG_DSN": dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if flag else "false",
    }
    return subprocess.run(  # noqa: S603  # Why: static argv from sys.executable; no shell.
        [sys.executable, "-m", "taskq", *argv],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


async def _hypertables(conn: asyncpg.Connection, schema: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT hypertable_name FROM timescaledb_information.hypertables "
        "WHERE hypertable_schema = $1",
        schema,
    )
    return {r["hypertable_name"] for r in rows}


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# ── The happy deploy ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_deploy_enables_hypertables_end_to_end(deploy_dsn: str, deploy_schema: str) -> None:
    """Flag on, fresh schema: ONE invocation applies every migration AND
    converts - the operator's entire opt-in is the env var."""
    schema = deploy_schema
    result = _invoke_migrate_up(deploy_dsn, schema, flag=True)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        # The widened uniques: present.
        uniques = {
            r["conname"]
            for r in await conn.fetch(
                "SELECT conname FROM pg_constraint con "
                "JOIN pg_class cl ON cl.oid = con.conrelid "
                "JOIN pg_namespace n ON n.oid = con.connamespace "
                "WHERE n.nspname = $1 AND con.contype = 'u'",
                schema,
            )
        }
        assert {
            "jobs_archive_id_finished_at_uniq",
            "job_events_id_occurred_at_uniq",
            "job_attempts_archive_job_attempt_started_at_uniq",
        } <= uniques
        # The vanilla archive FK is gone (no table may reference a hypertable).
        fks = await conn.fetchval(
            "SELECT count(*) FROM pg_constraint con "
            "JOIN pg_class cl ON cl.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = con.connamespace "
            "WHERE n.nspname = $1 AND cl.relname = 'job_attempts_archive' "
            "AND con.contype = 'f'",
            schema,
        )
        assert fks == 0, "the archive-attempts FK must be dropped under the flag"
        # The retention policies are registered for THIS schema's
        # hypertables (Timescale names the background jobs itself -
        # "Retention Policy [N]" - so scope by hypertable, not name).
        policy_jobs = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            schema,
        )
        assert {r["hypertable_name"] for r in policy_jobs} == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, f"the retention policies must cover all three hypertables: {policy_jobs}"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── The mid-life enable ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_midlife_enable_preserves_every_row(deploy_dsn: str) -> None:
    """A populated vanilla database converts with every row preserved:
    the ``migrate_data => TRUE`` promise, counted per table."""
    schema = "tsmid_" + new_uuid().hex[:12]

    # Phase 1: vanilla (flag off), then seed history through the real
    # tables' own shapes.
    result = _invoke_migrate_up(deploy_dsn, schema, flag=False)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        assert await _hypertables(conn, schema) == set(), "the flag-off deploy must convert nothing"
        n_events, n_archive, n_attempts = 500, 200, 300

        # Live jobs (job_events' FK target), then the events riding them.
        # with_events=False: this test writes its own event rows below and
        # counts them exactly - the helper's companion event would skew
        # the preservation count by one per job.
        live_job_ids = [
            await create_running_job(conn, schema, new_uuid(), new_uuid(), with_events=False)
            for _ in range(10)
        ]
        for _ in range(n_events):
            # job_events.id is sequence-backed bigint - the insert omits it
            # and the sequence owns it (the same shape the product's event
            # writer uses).
            await conn.execute(
                f'INSERT INTO "{schema}".job_events '
                f"(job_id, occurred_at, kind, detail) VALUES "
                f"($1, clock_timestamp(), 'state_change', '{{}}'::jsonb)",
                live_job_ids[_ % len(live_job_ids)],
            )
        # The archive family: terminal rows + their attempts.
        archive_ids = []
        for i in range(n_archive):
            archive_ids.append(
                await conn.fetchval(
                    f'INSERT INTO "{schema}".jobs_archive '
                    f"(id, actor, queue, payload, status, attempt, "
                    f"max_attempts, retry_kind, expire_at, finished_at) "
                    f"VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', 0, 3, "
                    f"'transient', clock_timestamp() + interval '365 days', "
                    f"clock_timestamp()) RETURNING id",
                    uuid.UUID(int=i + 1),
                )
            )
        for i in range(n_attempts):
            await conn.execute(
                f'INSERT INTO "{schema}".job_attempts_archive '
                f"(job_id, attempt, started_at, error_class) VALUES ($1, $2, "
                f"clock_timestamp(), NULL)",
                archive_ids[i % n_archive],
                i // n_archive,
            )

        # Phase 2: flip the flag; the SAME deploy path converts.
        result = _invoke_migrate_up(deploy_dsn, schema, flag=True)
        assert result.returncode == 0, f"stderr: {result.stderr}"

        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events') == n_events
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive') == n_archive
        assert (
            await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_attempts_archive')
            == n_attempts
        )
        # The rows are IN CHUNKS (the rewrite happened): at least one chunk
        # exists per populated table and the data reads through the hypertable.
        chunks = await conn.fetch(
            "SELECT hypertable_name, count(*) AS n "
            "FROM timescaledb_information.chunks WHERE hypertable_schema = $1 "
            "GROUP BY 1",
            schema,
        )
        by_table = {r["hypertable_name"]: r["n"] for r in chunks}
        assert all(
            by_table.get(t, 0) >= 1 for t in ("job_events", "jobs_archive", "job_attempts_archive")
        ), f"the converted tables must carry real chunks: {by_table}"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── The bounded-run interaction ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_bounded_run_still_converts(deploy_dsn: str) -> None:
    """``migrate up --phase pre`` with the flag on still converts - the
    current behavior, pinned so it cannot drift silently. If the deploy
    step ever grows an interaction exception for bounded runs, THIS pin
    is the one to consciously change."""
    schema = "tsbounded_" + new_uuid().hex[:12]
    result = _invoke_migrate_up(deploy_dsn, schema, flag=True, extra=["--phase", "pre"])
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, "the bounded run converts anyway (pinned current behavior)"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── The capability refusal, end to end ───────────────────────────────────


@pytest.mark.asyncio
async def test_capability_refusal_exits_one_names_the_setting(vanilla_dsn: str) -> None:
    """Flag on, plain Postgres: exit 1, the setting NAMED on stderr, and
    the schema left vanilla-and-usable (migrations applied, ledger rows
    present - a capability refusal is not schema damage)."""
    schema = "tsrefusal_" + new_uuid().hex[:12]
    result = _invoke_migrate_up(vanilla_dsn, schema, flag=True)
    assert result.returncode == 1, f"expected the refusal exit, got {result.returncode}"
    assert "TASKQ_TIMESCALEDB_HYPERTABLES" in result.stderr

    conn = await asyncpg.connect(vanilla_dsn)
    try:
        # The migrations APPLIED (the refusal is post-migration, and it
        # must not roll the schema back or report it damaged).
        tables = {
            r["table_name"]
            for r in await conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
                schema,
            )
        }
        assert {"jobs", "jobs_archive", "job_events", "schema_migrations"} <= tables
        applied = await conn.fetchval(f'SELECT count(*) FROM "{schema}".schema_migrations')
        assert applied > 0, "the migration ledger must be intact after the refusal"
        pkey = await conn.fetchval(
            "SELECT count(*) FROM pg_constraint con "
            "JOIN pg_class cl ON cl.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = con.connamespace "
            "WHERE n.nspname = $1 AND cl.relname = 'jobs_archive' AND con.contype = 'p'",
            schema,
        )
        assert pkey == 1, "the refusal must leave the vanilla archive's primary key standing"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Flag-off after enable: roll-forward only ─────────────────────────────


@pytest.mark.asyncio
async def test_flag_off_after_enable_changes_nothing(deploy_dsn: str) -> None:
    """There is no down-conversion: a flag-off deploy on an already-
    converted schema is a zero-statement no-op for the hypertable
    machinery - the tables stay hypertables and the retention policies
    persist at their last-registered intervals."""
    schema = "tsrollfwd_" + new_uuid().hex[:12]

    first = _invoke_migrate_up(deploy_dsn, schema, flag=True)
    assert first.returncode == 0, f"stderr: {first.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        # Identity, not a name proxy: the bgw job id is minted at
        # registration, so a re-registration (down- then up-converted
        # policies) would surface as NEW ids - and the interval rides
        # along so any drift from "last-registered" fails here too.
        policies_before = await conn.fetch(
            "SELECT job_id, hypertable_name, schedule_interval "
            "FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            schema,
        )
        assert policies_before

        second = _invoke_migrate_up(deploy_dsn, schema, flag=False)
        assert second.returncode == 0, f"stderr: {second.stderr}"
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, "the flag-off deploy must not down-convert"
        policies_after = await conn.fetch(
            "SELECT job_id, hypertable_name, schedule_interval "
            "FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            schema,
        )
        assert {
            (r["job_id"], r["hypertable_name"], r["schedule_interval"]) for r in policies_after
        } == {
            (r["job_id"], r["hypertable_name"], r["schedule_interval"]) for r in policies_before
        }, "the retention policies must persist at their last-registered intervals"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── The disable mirror, end to end ───────────────────────────────────────


def _vanilla_settings(dsn: str, schema: str) -> WorkerSettings:
    """The disable gate's settings: the flag flipped OFF first (the mirror
    of enable's flag-on gate)."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "false",
        }
    )


@pytest.mark.asyncio
async def test_disable_after_deploy_restores_vanilla(deploy_dsn: str, deploy_schema: str) -> None:
    """The deployed-and-converted schema (the REAL ``migrate up`` path)
    goes back to vanilla: zero hypertables, the deploy E2E's seed shapes
    preserved row for row, and the behaviors the conversion traded away
    restored - the restored bare PK rejects the duplicate the widened
    unique accepted, the restored FK rejects the orphan attempt the
    hypertable accepted."""
    schema = deploy_schema
    result = _invoke_migrate_up(deploy_dsn, schema, flag=True)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        # The deploy E2E's seed shapes (10 live FK targets, 500 events,
        # 200 archive rows, 300 attempts).
        live_job_ids = [
            await create_running_job(conn, schema, new_uuid(), new_uuid(), with_events=False)
            for _ in range(10)
        ]
        for _ in range(500):
            await conn.execute(
                f'INSERT INTO "{schema}".job_events '
                f"(job_id, occurred_at, kind, detail) VALUES "
                f"($1, clock_timestamp(), 'state_change', '{{}}'::jsonb)",
                live_job_ids[_ % len(live_job_ids)],
            )
        archive_ids = []
        for i in range(200):
            archive_ids.append(
                await conn.fetchval(
                    f'INSERT INTO "{schema}".jobs_archive '
                    f"(id, actor, queue, payload, status, attempt, "
                    f"max_attempts, retry_kind, expire_at, finished_at) "
                    f"VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', 0, 3, "
                    f"'transient', clock_timestamp() + interval '365 days', "
                    f"clock_timestamp()) RETURNING id",
                    uuid.UUID(int=i + 1),
                )
            )
        for i in range(300):
            await conn.execute(
                f'INSERT INTO "{schema}".job_attempts_archive '
                f"(job_id, attempt, started_at, error_class) VALUES ($1, $2, "
                f"clock_timestamp(), NULL)",
                archive_ids[i % 200],
                i // 200,
            )

        report = await disable_hypertables(
            conn, schema=schema, settings=_vanilla_settings(deploy_dsn, schema)
        )
        assert set(report.converted) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        assert await _hypertables(conn, schema) == set(), (
            "the disable must leave zero hypertables behind the deploy"
        )
        jobs = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = $1",
            schema,
        )
        assert jobs == 0, "no policy may survive the disable"
        for table, count in (
            ("job_events", 500),
            ("jobs_archive", 200),
            ("job_attempts_archive", 300),
        ):
            assert await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count, (
                f"every {table} row must survive the disable"
            )

        # The restored vanilla behaviors, the conversion pins' inverses.
        jid = await conn.fetchval(f'SELECT id FROM "{schema}".jobs_archive LIMIT 1')
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs_archive SELECT * FROM "{schema}".jobs_archive '
                "WHERE id = $1",
                jid,
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                f"""INSERT INTO "{schema}".job_attempts_archive (job_id, attempt, started_at)
                VALUES ($1, 1, clock_timestamp())""",
                new_uuid(),
            )
        # And the schema is fully operational: the event id sequence
        # continues past the restored rows.
        max_before = await conn.fetchval(f'SELECT max(id) FROM "{schema}".job_events')
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind) '
            "VALUES ($1, clock_timestamp(), 'state_change')",
            live_job_ids[0],
        )
        assert (
            await conn.fetchval(f'SELECT max(id) FROM "{schema}".job_events') == max_before + 1
        ), "the restored sequence must continue past the restored rows"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


@pytest.mark.asyncio
async def test_disable_hypertables_cli_subprocess_end_to_end(deploy_dsn: str) -> None:
    """The disable's operator path end to end: a REAL ``taskq migrate
    disable-hypertables`` subprocess (the flag flipped off first) converts
    the deployed-and-converted schema back to vanilla, and the flag-still-true
    invocation refuses loudly with exit 1 — the same deploy-step discipline
    the ``migrate up`` E2E pins on the enable side."""
    schema = "tsdcmd_" + new_uuid().hex[:12]
    first = _invoke_migrate_up(deploy_dsn, schema, flag=True)
    assert first.returncode == 0, f"stderr: {first.stderr}"

    conn = await asyncpg.connect(deploy_dsn)
    try:
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await conn.execute(
            f'INSERT INTO "{schema}".jobs_archive '
            f"(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
            f"expire_at, finished_at) VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', "
            f"0, 3, 'transient', clock_timestamp() + interval '365 days', clock_timestamp())",
            new_uuid(),
        )
        n_archive = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive')

        # The mistyped invocation — the flag still true — exits 1 loudly.
        refused = _invoke_migrate_disable_hypertables(deploy_dsn, schema, flag=True)
        assert refused.returncode == 1
        assert "TASKQ_TIMESCALEDB_HYPERTABLES" in refused.stderr
        assert await _hypertables(conn, schema) == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }, "the refused run must have issued zero statements"

        # The real one: flag off, one subprocess, exit 0, the report on stdout.
        result = _invoke_migrate_disable_hypertables(deploy_dsn, schema, flag=False)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "disabled hypertables on 3 table(s)" in result.stdout
        assert "restored to plain: job_events" in result.stdout
        assert await _hypertables(conn, schema) == set()
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive') == n_archive
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = $1",
                schema,
            )
            == 0
        ), "no policy may survive the CLI disable"

        # Idempotent on the operator's path too: a second run exits 0 and
        # reports there is nothing left to do.
        again = _invoke_migrate_disable_hypertables(deploy_dsn, schema, flag=False)
        assert again.returncode == 0, f"stderr: {again.stderr}"
        assert "already plain" in again.stdout
    finally:
        await _drop_schema(conn, schema)
        await conn.close()

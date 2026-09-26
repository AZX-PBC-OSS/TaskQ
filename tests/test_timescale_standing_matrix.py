# ruff: noqa: S608  # Why: schema is a fixed test identifier, never user input; every value is $-bound.
"""The STANDING-UP matrix: every way TaskQ gets deployed, with and without
hypertables, each cell proven against real containers through the
operator's own subprocess-CLI deploy step.

``tests/test_timescale_deploy_e2e.py`` owns the deploy-step E2E's core
cells (the happy deploy, the mid-life enable, the bounded run, the
capability refusal, the roll-forward, the disable mirror). This module
covers the holes THAT one does not - the cells an operator actually
stands up from, one per test, the cell named in the docstring:

* fresh + flag-on, the FULL bundled migration series with the conversion
  riding after it (the deploy E2E pins the shape; this pins the ledger's
  completeness);
* fresh + flag-off on a server that OFFERS the extension - the vanilla
  baseline, zero timescale artifacts left behind;
* re-migrate idempotence: the flag-on deploy run twice, then a third
  time - ledger, hypertables and policies converge, no duplicates;
* ``migrate status`` + a second ``migrate up`` against an
  already-converted schema - the day-2 operator path, not just day 1;
* the mixed state: a crash artifact that converted ONLY ``job_events`` -
  ``migrate up`` converges the rest (pinned after the convergence fix);
* cross-engine standing: the SAME env on plain Postgres. The flag-ON
  refusal cell is the deploy E2E's
  ``test_capability_refusal_exits_one_names_the_setting`` - referenced,
  not duplicated; the cell pinned here is the world's default
  deployment, flag OFF, engine-agnostic;
* the worker's first boot on a converted schema: a REAL worker
  subprocess (the ``tests/system_e2e`` harness's spawn shape) claims a
  job and completes it on hypertables - the runtime proof, not just the
  schema's.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.migrate import discover
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.pg import create_running_job
from taskq.timescale import _convert_job_events
from tests.system_e2e.actors import SysPayload, sys_fast
from tests.test_timescaledb_hypertables import _TIMESCALE_IMAGE_DEFAULT

pytestmark = pytest.mark.integration

_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT

#: The three tables the conversion turns into hypertables - the matrix's
#: every converted cell asserts this exact set.
_HYPERTABLES = frozenset({"job_events", "jobs_archive", "job_attempts_archive"})

#: The widened uniques the conversion substitutes for the bare pkeys.
_WIDENED_UNIQUES = frozenset(
    {
        "jobs_archive_id_finished_at_uniq",
        "job_events_id_occurred_at_uniq",
        "job_attempts_archive_job_attempt_started_at_uniq",
    }
)

#: The sys-effects ledger DDL, byte-identical to the system tier's
#: (``tests/system_e2e/conftest.py``): the worker subprocess's actors
#: record their body runs there, and the tier conftest that normally owns
#: it does not run in this module.
_SYS_EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".sys_effects (
    job_id  UUID NOT NULL,
    attempt INT NOT NULL,
    actor   TEXT NOT NULL,
    kind    TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""


# ── Containers, DSNs, schemas ────────────────────────────────────────────


@pytest.fixture(scope="module")
def matrix_container() -> Iterator[Any]:
    """One TimescaleDB container per module; skips without Docker."""
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
def matrix_dsn(matrix_container: Any) -> str:
    return matrix_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
def plain_container() -> Iterator[Any]:
    """A PLAIN postgres container (no timescale extension): the cross-
    engine cell."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image="postgres:18",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


@pytest.fixture
def plain_dsn(plain_container: Any) -> str:
    return plain_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture
def matrix_schema() -> str:
    return "tsmx_" + new_uuid().hex[:12]


# ── The operator's subprocess idiom (the deploy E2E's house pattern) ─────


def _invoke_migrate(
    dsn: str, schema: str, argv: list[str], *, flag: bool
) -> subprocess.CompletedProcess[str]:
    """One REAL ``taskq migrate`` invocation - a subprocess, the
    operator's own path. Nothing of the CLI is imported into the test's
    process (its asyncio.run cannot coexist with pytest-asyncio's loop)."""
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


def _migrate_up(
    dsn: str, schema: str, *, flag: bool, extra: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    return _invoke_migrate(dsn, schema, ["migrate", "up", *(extra or [])], flag=flag)


def _migrate_status(
    dsn: str, schema: str, *, flag: bool = True
) -> subprocess.CompletedProcess[str]:
    return _invoke_migrate(dsn, schema, ["migrate", "status"], flag=flag)


# ── Catalog readers ──────────────────────────────────────────────────────


async def _hypertables(conn: asyncpg.Connection, schema: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT hypertable_name FROM timescaledb_information.hypertables "
        "WHERE hypertable_schema = $1",
        schema,
    )
    return {r["hypertable_name"] for r in rows}


async def _policy_jobs(
    conn: asyncpg.Connection, schema: str, proc_names: tuple[str, ...]
) -> set[tuple[str, str]]:
    """The registered background policies as (hypertable, interval).

    The deploy step's remove-then-add registration mints a NEW bgw job id
    on every flag-on run (measured on the 2.30.1 image: ids 1000-1004,
    then 1005-1009), so identity-by-job-id would misread the by-design
    re-registration as drift. The convergent invariants are: exactly one
    policy per hypertable (checked by count, where duplicates cannot hide
    behind a set) at the interval the settings derive."""
    rows = await conn.fetch(
        "SELECT hypertable_name, schedule_interval FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = ANY($2)",
        schema,
        list(proc_names),
    )
    return {(r["hypertable_name"], str(r["schedule_interval"])) for r in rows}


async def _policy_counts(
    conn: asyncpg.Connection, schema: str, proc_names: tuple[str, ...]
) -> dict[str, int]:
    """Policies per hypertable, counted: a duplicate registration cannot
    hide behind a set."""
    rows = await conn.fetch(
        "SELECT hypertable_name, count(*) AS n FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = ANY($2) "
        "GROUP BY 1",
        schema,
        list(proc_names),
    )
    return {r["hypertable_name"]: int(r["n"]) for r in rows}


_RETENTION_PROCS = ("policy_retention",)
_COMPRESSION_PROCS = ("policy_compression", "policy_columnstore")


async def _ledger_keys(conn: asyncpg.Connection, schema: str) -> set[str]:
    rows = await conn.fetch(f'SELECT version FROM "{schema}".schema_migrations')
    return {r["version"] for r in rows}


async def _uniques(conn: asyncpg.Connection, schema: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT conname FROM pg_constraint con "
        "JOIN pg_class cl ON cl.oid = con.conrelid "
        "JOIN pg_namespace n ON n.oid = con.connamespace "
        "WHERE n.nspname = $1 AND con.contype = 'u'",
        schema,
    )
    return {r["conname"] for r in rows}


async def _constraint_count(conn: asyncpg.Connection, schema: str, table: str, contype: str) -> int:
    # contype is Postgres's one-byte "char" type: asyncpg binds it from
    # bytes, and the value is always this module's own fixed literal.
    return int(
        await conn.fetchval(
            "SELECT count(*) FROM pg_constraint con "
            "JOIN pg_class cl ON cl.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = con.connamespace "
            "WHERE n.nspname = $1 AND cl.relname = $2 AND con.contype = $3",
            schema,
            table,
            contype.encode("ascii"),
        )
    )


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _flag_on_settings(dsn: str, schema: str) -> WorkerSettings:
    """The flag-on settings for the in-process conversion legs (the same
    cascade the deploy subprocess re-reads)."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
        }
    )


# ── Cell: fresh + flag-on ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cell_fresh_flag_on_applies_the_full_migration_series(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: fresh + flag-on. The deploy E2E's happy deploy pins
    the resulting SHAPE; this cell pins the LEDGER's completeness: with
    the conversion running after, EVERY migration in the bundled series
    applies - not a subset - and the converted shape stands on top of the
    full series."""
    schema = matrix_schema
    result = _migrate_up(matrix_dsn, schema, flag=True)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(matrix_dsn)
    try:
        expected = {m.key for m in discover()}
        assert expected, "the bundled series must be non-empty for this pin to mean anything"
        assert await _ledger_keys(conn, schema) == expected, (
            "every bundled migration must be in the ledger after the flag-on deploy"
        )
        assert await _hypertables(conn, schema) == set(_HYPERTABLES)
        assert await _uniques(conn, schema) >= _WIDENED_UNIQUES
        assert await _constraint_count(conn, schema, "job_attempts_archive", "f") == 0, (
            "the archive-attempts FK must be dropped under the flag"
        )
        retention = await _policy_jobs(conn, schema, _RETENTION_PROCS)
        assert {name for name, _ in retention} == set(_HYPERTABLES), (
            f"the retention policies must cover all three hypertables: {retention}"
        )
        compression = await _policy_jobs(conn, schema, _COMPRESSION_PROCS)
        assert {name for name, _ in compression} == {"jobs_archive", "job_attempts_archive"}, (
            f"the compression policies must cover the two archives: {compression}"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: fresh + flag-off (the vanilla baseline) ────────────────────────


@pytest.mark.asyncio
async def test_cell_fresh_flag_off_is_the_vanilla_baseline(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: fresh + flag-off, on a server that OFFERS the
    extension. The zero-SQL gate must hold at the deploy step: the full
    series applies, and ZERO timescale artifacts are left - no
    hypertables, no policies, no extension created. The vanilla shape is
    pinned by its own constraints: the bare archive pkey and the
    attempts-archive FK the conversion would trade away."""
    schema = matrix_schema
    conn = await asyncpg.connect(matrix_dsn)
    try:
        ext_before = await conn.fetchval(
            "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'"
        )
        result = _migrate_up(matrix_dsn, schema, flag=False)
        assert result.returncode == 0, f"stderr: {result.stderr}"

        expected = {m.key for m in discover()}
        assert await _ledger_keys(conn, schema) == expected, (
            "the flag-off deploy applies the same full series"
        )
        assert await _hypertables(conn, schema) == set(), "the flag-off deploy converts nothing"
        jobs = await conn.fetchval(
            "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = $1",
            schema,
        )
        assert jobs == 0, "no policy may be registered with the flag off"
        ext_after = await conn.fetchval(
            "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'"
        )
        assert ext_after == ext_before, (
            "the flag-off deploy must not create the extension (the zero-SQL gate)"
        )
        # The vanilla shape, pinned by the constraints the conversion
        # trades away: the bare archive pkey stands, the archive FK stands,
        # no widened uniques exist anywhere in the schema.
        assert await _constraint_count(conn, schema, "jobs_archive", "p") == 1, (
            "the vanilla archive's bare primary key must stand with the flag off"
        )
        assert await _constraint_count(conn, schema, "job_attempts_archive", "f") == 1, (
            "the vanilla archive FK must stand with the flag off"
        )
        assert not (_WIDENED_UNIQUES & await _uniques(conn, schema)), (
            "no widened unique may exist with the flag off"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: re-migrate idempotence ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_cell_remigrate_idempotence_converges_without_duplicates(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: re-migrate idempotence, flag on. ``migrate up`` runs a
    SECOND and a THIRD time against the converted schema: every run exits
    0, the ledger stops growing, the hypertable set never changes, and the
    policies converge. Convergence is pinned where the duplicates would
    have to hide: the deploy step's registration is remove-then-add (it
    deliberately re-registers on every flag-on run, minting a fresh bgw
    job id each time - measured), so the stable invariants are ONE policy
    per hypertable (counted, never accumulated) at the interval the
    settings derive, identical after every run."""
    schema = matrix_schema
    conn = await asyncpg.connect(matrix_dsn)
    try:
        snapshots: list[tuple[int, set[str], set[Any], set[Any]]] = []
        for run in range(3):
            result = _migrate_up(matrix_dsn, schema, flag=True)
            assert result.returncode == 0, f"run {run + 1} stderr: {result.stderr}"
            if run > 0:
                assert "no pending migrations" in result.stdout, (
                    f"run {run + 1} must report nothing left to apply"
                )
            snapshots.append(
                (
                    len(await _ledger_keys(conn, schema)),
                    await _hypertables(conn, schema),
                    await _policy_jobs(conn, schema, _RETENTION_PROCS),
                    await _policy_jobs(conn, schema, _COMPRESSION_PROCS),
                )
            )
        _first_ledger, first_hypertables, first_retention, _first_compression = snapshots[0]
        assert first_hypertables == set(_HYPERTABLES)
        assert {name for name, _ in first_retention} == set(_HYPERTABLES)
        assert all(snap == snapshots[0] for snap in snapshots[1:]), (
            f"every re-run must converge on the identical state: {snapshots}"
        )
        # No duplicate policies of either kind, by direct count - the
        # count is where an accumulated registration would show.
        for proc_names, tables in (
            (_RETENTION_PROCS, _HYPERTABLES),
            (_COMPRESSION_PROCS, {"jobs_archive", "job_attempts_archive"}),
        ):
            by_table = await _policy_counts(conn, schema, proc_names)
            assert all(by_table.get(t, 0) == 1 for t in tables), (
                f"exactly one policy per hypertable, never duplicates: {by_table}"
            )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: day-2 operations on a converted schema ─────────────────────────


@pytest.mark.asyncio
async def test_cell_migrate_status_and_second_up_run_on_converted_schema(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: migrations AFTER conversion. A converted schema is not
    a special case for the migrate tooling: ``migrate status`` walks the
    whole bundled series against it without error (every migration shows
    applied, none pending), and a second ``migrate up`` applies nothing
    and leaves the policy surface identical - one policy per hypertable
    at the same intervals (the registration is remove-then-add, so the
    bgw job ids rotate by design; the POLICIES, not their ids, are the
    operator-visible state)."""
    schema = matrix_schema
    first = _migrate_up(matrix_dsn, schema, flag=True)
    assert first.returncode == 0, f"stderr: {first.stderr}"

    conn = await asyncpg.connect(matrix_dsn)
    try:
        policies_before = await _policy_jobs(conn, schema, _RETENTION_PROCS + _COMPRESSION_PROCS)

        status = _migrate_status(matrix_dsn, schema, flag=True)
        assert status.returncode == 0, f"status stderr: {status.stderr}"
        expected = len(discover())
        assert f"applied: {expected}" in status.stdout, (
            f"status must report the full series applied on the converted "
            f"schema ({expected}): {status.stdout}"
        )
        assert "[ ]" not in status.stdout, (
            f"no migration may read as pending on the converted schema: {status.stdout}"
        )

        second = _migrate_up(matrix_dsn, schema, flag=True)
        assert second.returncode == 0, f"second-up stderr: {second.stderr}"
        assert "no pending migrations" in second.stdout

        assert await _hypertables(conn, schema) == set(_HYPERTABLES), (
            "the no-op up must leave the hypertables standing"
        )
        assert (
            await _policy_jobs(conn, schema, _RETENTION_PROCS + _COMPRESSION_PROCS)
            == policies_before
        ), "the no-op up must leave the policy surface identical"
        for proc_names, tables in (
            (_RETENTION_PROCS, _HYPERTABLES),
            (_COMPRESSION_PROCS, {"jobs_archive", "job_attempts_archive"}),
        ):
            by_table = await _policy_counts(conn, schema, proc_names)
            assert all(by_table.get(t, 0) == 1 for t in tables), (
                f"the re-registration must never accumulate policies: {by_table}"
            )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: the mixed state (a crash artifact) ─────────────────────────────


@pytest.mark.asyncio
async def test_cell_mixed_partially_converted_schema_converges(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: the mixed state. A deploy crashed mid-conversion:
    ``job_events`` converted but the two archive tables are still vanilla.
    The artifact is built at a REAL statement boundary of the public entry
    point - ``enable_hypertables`` runs ``_convert_job_events`` first, the
    archive conversion second, and every statement on its own autocommit
    transaction, so a crash between the two leaves exactly this state
    (job_events a hypertable, the archives plain, no policies anywhere -
    registration comes after ALL conversions); invoking the first half
    directly reproduces it without mocking anything. The pin: the next
    ``migrate up`` CONVERGES the rest - exit 0, all three hypertables,
    policies on every one - and the events written before the crash
    survive it."""
    schema = matrix_schema
    vanilla = _migrate_up(matrix_dsn, schema, flag=False)
    assert vanilla.returncode == 0, f"stderr: {vanilla.stderr}"

    conn = await asyncpg.connect(matrix_dsn)
    try:
        # Seed history the crash artifact must preserve: one live job and
        # the events riding it.
        job_id = await create_running_job(conn, schema, new_uuid(), new_uuid(), with_events=False)
        n_events = 5
        for _ in range(n_events):
            await conn.execute(
                f'INSERT INTO "{schema}".job_events '
                f"(job_id, occurred_at, kind, detail) VALUES "
                f"($1, clock_timestamp(), 'state_change', '{{}}'::jsonb)",
                job_id,
            )

        # The crash artifact: only job_events converts - the boundary
        # between the conversion's own first and second halves. No
        # policies: registration comes after ALL conversions, so the
        # crashed run never reached it.
        settings = _flag_on_settings(matrix_dsn, schema)
        converted: list[str] = []
        await _convert_job_events(conn, schema, settings, converted)
        assert await _hypertables(conn, schema) == {"job_events"}, (
            "the artifact must be genuinely mixed: job_events converted, the archives not"
        )
        assert await _policy_jobs(conn, schema, _RETENTION_PROCS + _COMPRESSION_PROCS) == set(), (
            "the crashed run must have registered no policies yet"
        )

        # The next deploy, the operator's path: converges the rest.
        result = _migrate_up(matrix_dsn, schema, flag=True)
        assert result.returncode == 0, (
            f"the deploy over the mixed state must converge, not fail: {result.stderr}"
        )
        assert await _hypertables(conn, schema) == set(_HYPERTABLES), (
            "the converging deploy must convert the two archive tables the crash skipped"
        )
        retention = await _policy_jobs(conn, schema, _RETENTION_PROCS)
        assert {name for name, _ in retention} == set(_HYPERTABLES), (
            f"the retention policies must cover all three after convergence: {retention}"
        )
        compression = await _policy_jobs(conn, schema, _COMPRESSION_PROCS)
        assert {name for name, _ in compression} == {"jobs_archive", "job_attempts_archive"}
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".job_events') == n_events, (
            "the events written before the crash must survive the convergence"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: cross-engine standing ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_cell_plain_postgres_flag_off_deploys_clean(plain_dsn: str) -> None:
    """MATRIX CELL: cross-engine standing, flag off - the world's default
    deployment, and the cell the deploy E2E does not own (its
    ``test_capability_refusal_exits_one_names_the_setting`` already pins
    the flag-ON refusal on this engine; referenced, not duplicated).
    The SAME env, flag off, on plain Postgres: the full series applies,
    the vanilla shape stands, and zero timescale artifacts exist - the
    extension is not even probed for, let alone created."""
    schema = "tsxp_" + new_uuid().hex[:12]
    result = _migrate_up(plain_dsn, schema, flag=False)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = await asyncpg.connect(plain_dsn)
    try:
        expected = {m.key for m in discover()}
        assert await _ledger_keys(conn, schema) == expected, (
            "the flag-off deploy is engine-agnostic: the full series applies on plain Postgres"
        )
        assert await _constraint_count(conn, schema, "jobs_archive", "p") == 1, (
            "the vanilla archive's bare primary key stands on plain Postgres"
        )
        assert await _constraint_count(conn, schema, "job_attempts_archive", "f") == 1, (
            "the vanilla archive FK stands on plain Postgres"
        )
        assert not (_WIDENED_UNIQUES & await _uniques(conn, schema))
        ext = await conn.fetchval("SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'")
        assert ext == 0, "a flag-off deploy must leave no timescale extension behind"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── Cell: the worker's first boot on a converted schema ──────────────────


@pytest.mark.asyncio
async def test_cell_worker_first_boot_on_converted_schema_claims_and_completes(
    matrix_dsn: str, matrix_schema: str
) -> None:
    """MATRIX CELL: the worker's first boot on a converted schema. The
    schema is deployed with the flag ON through the operator's subprocess,
    then a REAL worker subprocess (the ``tests/system_e2e`` harness's
    spawn shape: the production bootstrap, env-configured like a pod,
    health-socket readiness, ``TASKQ_MIGRATE_ON_START=false``) boots
    against it, claims a job, and completes it - with the sweeps running
    on their interval against the hypertables. The runtime proof, not
    just the schema's: nothing in the worker's boot or its claim/complete
    path cares that the tables it reads and writes are hypertables."""
    schema = matrix_schema
    deploy = _migrate_up(matrix_dsn, schema, flag=True)
    assert deploy.returncode == 0, f"stderr: {deploy.stderr}"

    from taskq import TaskQ
    from tests.system_e2e._harness import (
        graceful_stop,
        reap,
        spawn_worker,
        wait_worker_ready,
    )

    conn = await asyncpg.connect(matrix_dsn)
    worker = spawn_worker(matrix_dsn, schema, tag="tsmx")
    try:
        # The system tier's actors record their body runs in this table; the
        # tier conftest that normally creates it does not run in this module.
        await conn.execute(_SYS_EFFECTS_DDL.format(schema=schema))
        wait_worker_ready(worker)
        assert worker.poll() is None, "the worker must still be alive at readiness"

        async with TaskQ(dsn=matrix_dsn, schema=schema) as client:
            await client.enqueue(sys_fast, SysPayload(sleep=0.05))

        deadline = time.monotonic() + 60.0
        succeeded = 0
        effects = 0
        while time.monotonic() < deadline:
            succeeded = int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs '
                    "WHERE actor = 'sys_fast' AND status = 'succeeded'"
                )
            )
            effects = int(
                await conn.fetchval(
                    f"SELECT count(*) FROM \"{schema}\".sys_effects WHERE kind = 'done'"
                )
            )
            if succeeded == 1 and effects == 1:
                break
            assert worker.poll() is None, "the worker died mid-claim"
            await asyncio.sleep(0.25)
        assert succeeded == 1, "the enqueued job must complete on the hypertable schema"
        assert effects == 1, "the actor body's effect row must land through the hypertable FK"
        assert worker.poll() is None, "the worker must still be alive after the completed job"

        code = graceful_stop(worker)
        assert code == 0, f"the worker must exit cleanly on SIGTERM, got {code}"
    finally:
        reap(worker)
        await _drop_schema(conn, schema)
        await conn.close()

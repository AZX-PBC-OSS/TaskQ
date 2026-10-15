# ruff: noqa: S608  # Why: schema is a fixed test identifier, never user input; every value is $-bound.
"""The THREE-MODE lifecycle: full TSL, Apache-license, and no-TimescaleDB
are three first-class storage modes, and each one's ENTIRE lifecycle is
pinned here against a real container of that mode — the same operator
path end to end (``taskq migrate up`` as a subprocess, operate, the
retention drain in the mode's own mechanism, the disable round-trip),
asserting each mode's documented matrix cell
(``docs/guides/timescaledb.md``'s support matrix, cited from
``taskq.timescale.detect_storage_mode``).

The three configurations:

* ``timescale-tsl`` — ``timescale/timescaledb:2.30.1-pg18``, the flag on:
  hypertables + columnstore + policy-driven chunk-drop retention;
* ``timescale-apache`` — the SAME image started with
  ``postgres -c timescaledb.license=apache`` (the license GUC cannot be
  changed inside a running session; the server command line is the
  documented way): hypertables convert (the conversion itself is
  Apache-licensed) but BOTH the retention policies and the columnstore
  are Timescale-license features the server refuses — measured
  ``FeatureNotSupportedError`` — so the deploy registers nothing and
  retention stays with the row-level sweeps;
* ``vanilla`` — plain ``postgres:18``, the flag off: plain tables, the
  row-level sweeps own all of retention. The repo's default mode, bound
  into the matrix explicitly by running the SAME lifecycle on it.

Per phase, the mode-documented behavior:

1. **Migrate up** (the real subprocess): exits 0 on all three; the
   migrated schema carries the mode's shape — three hypertables on both
   Timescale modes (the apache mode's conversion succeeds; refusing the
   policies is not refusing the feature), plain tables on vanilla, and
   the policy families registered ONLY on the TSL mode.
2. **Operate**: live claims and terminal folds through the REAL
   ``prune_terminal_jobs`` move — the prune→archive write path works on
   every mode's schema, and folds the same rows to the same counts.
3. **Retention drain** (in the converted state, the mode's own
   mechanism): on TSL the POLICY drops the fully-aged chunk — including
   a row whose ``expire_at`` is still far in the future (the documented
   partition-clock trade) — while the sweep still expires an in-window
   row whose ``expire_at`` passed (row-exact inside young chunks); on
   apache and vanilla there are no policies, and the SAME
   ``archive_expiry_sweep`` that TSL bounds below its policy floor owns
   the whole range and deletes the aged row exactly on ``expire_at``.
4. **Disable round-trip**: the converted schemas return to plain tables
   with every row preserved and the restored primary key enforcing
   again; vanilla's disable is a no-op that changes nothing. After it,
   all three modes are the same mode: one more expired row is drained by
   the one mechanism all three share, the row-level sweep.

The container legs skip with a reason when Docker is unreachable (the
house pattern of the sibling Timescale modules). Detection itself —
which container presents which mode — is asserted first, so a drift in
the images or the license mechanics fails as a DETECTION failure, not
as a confusing downstream shape mismatch.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.assertions import plain_cli_output
from taskq.testing.pg import create_running_job
from taskq.timescale import StorageMode, detect_storage_mode
from taskq.worker.leader import archive_expiry_sweep, prune_terminal_jobs
from tests.test_timescale_deploy_e2e import (
    _drop_schema,
    _hypertables,
    _invoke_migrate_disable_hypertables,
)

pytestmark = pytest.mark.integration

_TIMESCALE_IMAGE = (
    os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or "timescale/timescaledb:2.30.1-pg18"
)

#: The retention settings the lifecycle runs with: short enough that the
#: TSL policy's chunk interval clamps to the 1-day floor and the aged
#: seeds (3.5 days) sit a full chunk clear of the 2-day drop boundary —
#: the sibling retention-interplay module's arithmetic, reused.
_TEST_ARCHIVE_RETENTION = timedelta(days=2)
_TEST_EVENT_RETENTION = timedelta(days=1)
_AGED_FINISHED_AT = timedelta(days=3.5)

#: The prune's fold clocks (the attack-integration module's values): a
#: terminal row folds into the archive one hour past terminal, and the
#: archive stamp it leaves is far-future so nothing else expires it early.
_FOLD_RETENTION = timedelta(hours=1)
_FOLD_ARCHIVE_RETENTION = timedelta(days=365)


# ── Fixtures: one container per mode ──────────────────────────────────────


@pytest.fixture(scope="module")
def tsl_container() -> Iterator[Any]:
    """The full-TSL mode: the pinned image, default license."""
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
def apache_container() -> Iterator[Any]:
    """The apache-license mode: the SAME image, the license set at boot
    (the GUC cannot change inside a running session, so the server
    command line is the documented mechanism)."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container.with_command("postgres -c timescaledb.license=apache")


@pytest.fixture(scope="module")
def vanilla_container() -> Iterator[Any]:
    """The no-TimescaleDB mode: plain Postgres, no extension offered."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image="postgres:18",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container


def _dsn(container: Any) -> str:
    return container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
def timescale_tsl_dsn(tsl_container: Any) -> str:
    return _dsn(tsl_container)


@pytest.fixture(scope="module")
def timescale_apache_dsn(apache_container: Any) -> str:
    """The apache-license mode's DSN: the license GUC cannot change
    inside a running session, so the dedicated module container is
    configured down with ALTER SYSTEM + reload (measured: after the
    reload, NEW sessions see ``apache`` — and the server refuses the
    retention policies AND the compression APIs alike)."""
    import asyncio

    dsn = _dsn(apache_container)

    async def _downgrade_license() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute("ALTER SYSTEM SET timescaledb.license = 'apache'")
            await conn.execute("SELECT pg_reload_conf()")
        finally:
            await conn.close()

    asyncio.run(_downgrade_license())
    return dsn


@pytest.fixture(scope="module")
def vanilla_dsn(vanilla_container: Any) -> str:
    return _dsn(vanilla_container)


_PARAMS = [StorageMode.TIMESCALE_TSL, StorageMode.TIMESCALE_APACHE, StorageMode.VANILLA]


@pytest.fixture(params=_PARAMS, ids=[m.value for m in _PARAMS])
def mode(request: pytest.FixtureRequest) -> StorageMode:
    """The mode under test; the container is selected by the param."""
    return request.param


@pytest.fixture
def mode_dsn(mode: StorageMode, request: pytest.FixtureRequest) -> str:
    return request.getfixturevalue(mode.value.replace("-", "_") + "_dsn")


# ── Settings helpers ──────────────────────────────────────────────────────


def _up_env(dsn: str, schema: str, *, flag: bool) -> dict[str, str]:
    """The deploy env: the flag on for both Timescale modes, off for
    vanilla — and the retention settings the chunk intervals and the
    policies derive from, identical across all three legs."""
    return {
        **os.environ,
        "TASKQ_PG_DSN": dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if flag else "false",
        "TASKQ_ARCHIVE_RETENTION_PERIOD": f"{int(_TEST_ARCHIVE_RETENTION.total_seconds())}s",
        "TASKQ_EVENT_RETENTION_PERIOD": f"{int(_TEST_EVENT_RETENTION.total_seconds())}s",
    }


def _run(dsn: str, schema: str, argv: list[str], *, flag: bool) -> subprocess.CompletedProcess[str]:
    env = _up_env(dsn, schema, flag=flag)
    return subprocess.run(  # noqa: S603  # Why: static argv from sys.executable; no shell.
        [sys.executable, "-m", "taskq", *argv],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


# ── Per-mode phase assertions ─────────────────────────────────────────────


async def _policy_rows(conn: asyncpg.Connection, schema: str) -> set[tuple[str, str]]:
    rows = await conn.fetch(
        "SELECT hypertable_name, proc_name FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name LIKE 'policy%'",
        schema,
    )
    return {(r["hypertable_name"], r["proc_name"]) for r in rows}


async def _count(conn: asyncpg.Connection, schema: str, table: str) -> int:
    return int(await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"'))


# ── The lifecycle ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_lifecycle_on_each_of_the_three_modes(mode: StorageMode, mode_dsn: str) -> None:
    """Migrate up → operate → retention drain → disable round-trip, one
    full pass per mode, each phase asserting that mode's documented
    matrix cell (docstring: the module header)."""
    detected = await _detect(mode_dsn)
    assert detected is mode, (
        f"the {mode.value} container must detect as {mode.value}, got {detected.value}"
    )
    flag_on = mode is not StorageMode.VANILLA
    schema = "tslc_" + new_uuid().hex[:12]

    # ── Phase 1: migrate up (the operator's own subprocess) ──
    up = _run(mode_dsn, schema, ["migrate", "up"], flag=flag_on)
    assert up.returncode == 0, f"migrate up failed on {mode.value}: {up.stderr}"

    conn = await asyncpg.connect(mode_dsn)
    try:
        await _assert_phase_one_schema(conn, schema, mode)

        # The compression family's first run fires seconds after the TSL
        # deploy registered it; defer it before any more DML, so no phase
        # below races it for a chunk (see _defer_compression_policies).
        # THE RETENTION FAMILY JOINS THE DEFERRAL (the round-2 red, cured
        # 2026-10-08): the retention policy's first run fires seconds after
        # registration exactly like the compression one — and phase 3's
        # row-exactness window (the aged row must SURVIVE the row-level
        # sweep before the forced policy run takes it) closes the moment
        # an un-deferred retention job fires mid-test and drops the aged
        # seed's chunk ahead of its own script (count 0 != 1, the
        # partition clock beating the test's). Deferred here, pulled back
        # to now ONLY by _force_retention_policies_now at the phase's
        # own drop step — the drop belongs to the phase, never to the
        # background scheduler's arrival time.
        if mode is StorageMode.TIMESCALE_TSL:
            await _defer_retention_policies(conn, schema)
            await _defer_compression_policies(conn, schema)

        # ── Phase 2: operate — live claims + the real prune→archive fold ──
        for _ in range(2):
            await create_running_job(conn, schema, new_uuid(), new_uuid(), with_events=False)
        folded = [
            await _seed_terminal_job(conn, schema, status="succeeded", age=timedelta(hours=2))
            for _ in range(3)
        ]
        prune = await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": _FOLD_RETENTION},
            archive_retention=_FOLD_ARCHIVE_RETENTION,
            schema=schema,
        )
        assert prune.total_deleted == 3, (
            f"the prune must fold the same terminal rows on every mode: {prune.total_deleted}"
        )
        assert await _count(conn, schema, "jobs_archive") == 3

        # ── Phase 3: the retention drain, in the mode's own mechanism ──
        await _assert_phase_three_drain(conn, schema, mode, folded)

        # ── Phase 4: the disable round-trip ──
        down = _invoke_migrate_disable_hypertables(mode_dsn, schema)
        assert down.returncode == 0, f"disable-hypertables failed on {mode.value}: {down.stderr}"
        if flag_on:
            assert await _hypertables(conn, schema) == set(), (
                "the disable round-trip must leave zero hypertables on every Timescale mode"
            )
        for table in ("jobs", "job_events", "jobs_archive", "job_attempts_archive"):
            assert await _table_exists(conn, schema, table), (
                f"{table} must survive the round-trip on {mode.value}"
            )
        assert await _count(conn, schema, "jobs_archive") == 3, (
            "every folded row must survive the round-trip"
        )
        # The restored vanilla behavior: the bare primary key enforces
        # again (on vanilla it never left; the assertion is mode-uniform
        # and cheap).
        jid = await conn.fetchval(f'SELECT id FROM "{schema}".jobs_archive LIMIT 1')
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs_archive SELECT * FROM "{schema}".jobs_archive '
                "WHERE id = $1",
                jid,
            )

        # ── Post-round-trip: all three modes are now the SAME mode ──
        # One more expired row, drained by the one mechanism every mode
        # shares: the row-level sweep, exact on expire_at, watermark kept.
        await _seed_expired_archive_row(conn, schema)
        sweep = await archive_expiry_sweep(conn, schema=schema)
        assert sweep.total_deleted == 1, (
            f"after the round-trip the row-level sweep owns the drain on "
            f"{mode.value}: deleted {sweep.total_deleted}"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def _detect(dsn: str) -> StorageMode:
    """The mode the container actually presents — asserted BEFORE the
    lifecycle runs: a drift in the images or the license mechanics is a
    DETECTION failure, never a confusing downstream shape mismatch."""
    conn = await asyncpg.connect(dsn)
    try:
        return await detect_storage_mode(conn)
    finally:
        await conn.close()


async def _table_exists(conn: asyncpg.Connection, schema: str, table: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{table}"'))


async def _assert_phase_one_schema(
    conn: asyncpg.Connection, schema: str, mode: StorageMode
) -> None:
    """Phase 1's cell: what ``migrate up`` leaves behind in each mode."""
    for table in ("jobs", "job_events", "jobs_archive", "job_attempts_archive"):
        assert await _table_exists(conn, schema, table), f"{table} must exist on {mode.value}"

    if mode is StorageMode.VANILLA:
        # No extension created, nothing Timescale left behind: the
        # vanilla mode's deploy issues zero statements about any of it.
        ext = await conn.fetchval("SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'")
        assert ext == 0, "the vanilla mode's deploy must never create the extension"
        return

    # Both Timescale modes convert the three tables — the conversion is
    # Apache-licensed; refusing the POLICIES (apache mode) is not
    # refusing the feature.
    assert await _hypertables(conn, schema) == {
        "job_events",
        "jobs_archive",
        "job_attempts_archive",
    }, f"{mode.value} must convert all three retention tables"
    if mode is StorageMode.TIMESCALE_TSL:
        assert await _policy_rows(conn, schema) == {
            ("job_events", "policy_retention"),
            ("jobs_archive", "policy_retention"),
            ("job_attempts_archive", "policy_retention"),
            # The 2.30.1 image: the compression policy registers under
            # the LEGACY name (the deploy code's measured probe —
            # add_columnstore_policy errors, add_compression_policy
            # lands), so the proc_name the catalogs report is
            # policy_compression.
            ("jobs_archive", "policy_compression"),
            ("job_attempts_archive", "policy_compression"),
        }, "the TSL mode registers retention + compression policies"
    else:
        assert await _policy_rows(conn, schema) == set(), (
            "the apache mode registers NOTHING: the policies are "
            "Timescale-license features the server refuses"
        )


async def _seed_terminal_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    status: str,
    age: timedelta,
) -> uuid.UUID:
    """One terminal job, finished *age* ago: past the fold clock, inside
    every retention window the lifecycle arms."""
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs (id, actor, queue, payload, max_attempts,
            retry_kind, status, scheduled_at, schedule_to_close, finished_at)
        VALUES ($1, 'lifecycle_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            $2, $3, $4, $5)""",
        jid,
        status,
        now,
        now + timedelta(hours=1),
        now - age,
    )
    return jid


async def _seed_expired_archive_row(conn: asyncpg.Connection, schema: str) -> uuid.UUID:
    """One archive row whose ``expire_at`` passed: the row-level sweep's
    own prey on every mode."""
    jid = new_uuid()
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'lifecycle_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)""",
        jid,
        now,
        now + timedelta(hours=1),
        now - timedelta(hours=2),
        now - timedelta(minutes=1),
    )
    return jid


async def _assert_phase_three_drain(
    conn: asyncpg.Connection,
    schema: str,
    mode: StorageMode,
    folded: list[uuid.UUID],
) -> None:
    """Phase 3's cell: WHO drains aged rows in each mode, and what each
    mechanism honors.

    Two aged seeds, identical on every mode:

    * an expired row (``expire_at`` just passed, ``finished_at`` fresh):
      inside every window — the row-level sweep's prey wherever the
      sweeps own the range;
    * a fully-aged row (``finished_at`` 3.5 days back, ``expire_at`` far
      future): the POLICY's prey on TSL (dropped despite the future
      ``expire_at`` — the documented partition-clock trade), nobody's
      prey on the other two modes (their sweeps honor ``expire_at``
      exactly and this row has not expired).
    """
    expired_jid = await _seed_expired_archive_row(conn, schema)
    aged_jid = new_uuid()
    now = datetime.now(UTC)
    # The aged seed's expire_at is MODE-DELIBERATE. On TSL it is far
    # future: the row is 3.5 days past the policy's 2-day partition
    # boundary, so if it drains at all it is the POLICY that took it —
    # never the expire_at sweep. On apache/vanilla there is no policy;
    # the row-level sweep takes rows exactly on expire_at, so the aged
    # seed must actually BE expired to be drainable.
    aged_expire_at = (
        now + timedelta(days=365)
        if mode is StorageMode.TIMESCALE_TSL
        else now - timedelta(minutes=1)
    )
    await _seed_fully_aged_archive_row(conn, schema, aged_jid, expire_at=aged_expire_at)

    await archive_expiry_sweep(conn, schema=schema)

    if mode is StorageMode.TIMESCALE_TSL:
        # In-window: the sweep stays row-exact on expire_at — the expired
        # row goes, the fully-aged one stays (expire_at is far future;
        # only the policy's partition clock takes it).
        assert (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', expired_jid
                )
            )
            == 0
        ), "the sweep must expire the in-window row"
        assert (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', aged_jid
                )
            )
            == 1
        ), (
            "the fully-aged row's expire_at is far future: the sweep must NOT "
            "take it — on TSL only the policy's partition clock does"
        )
        # The policy drain: the aged row's chunk is a full interval older
        # than the 2-day boundary (3.5 days back, 1-day chunks). Force a
        # REAL retention-policy run (the alter_job next_start knob, the
        # sibling module's sanctioned pattern) and poll for the policy's
        # own committed effect — on the DERIVED budget (see the
        # constants' block above the helper): the recorded first-tick
        # artifact gives 3 executions, the sibling's 60s the per-dispatch
        # bound, the cancel-storm doctrine the 20x co-tenancy stretch.
        await _force_retention_policies_now(conn, schema)
        await _wait_for(
            _row_gone(conn, schema, aged_jid),
            what="the TSL retention policy to drop the fully-aged chunk",
            timeout_secs=_POLICY_DROP_BUDGET_SECS,
        )
    else:
        # apache + vanilla: no policies exist, the sweep owns the WHOLE
        # range — the aged row goes exactly on expire_at.
        if mode is StorageMode.TIMESCALE_APACHE:
            # The apache mode HAS the information views; the policy set
            # is provably empty. (Vanilla has no views at all — its
            # no-extension state was phase 1's pin.)
            assert await _policy_rows(conn, schema) == set(), f"no policy can exist on {mode.value}"
        assert (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', expired_jid
                )
            )
            == 0
        )
        assert (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', aged_jid
                )
            )
            == 0
        ), (
            f"on {mode.value} the row-level sweep owns the whole range: the "
            "expired row goes on expire_at exactly, no policy floor to stop it"
        )

    # The folded archive rows (fresh, far-future expire_at) survive the
    # drain on every mode.
    for jid in folded:
        assert (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', jid
                )
            )
            == 1
        ), "the drain must not touch rows inside their window"


async def _seed_fully_aged_archive_row(
    conn: asyncpg.Connection, schema: str, jid: uuid.UUID, *, expire_at: datetime
) -> None:
    """The fully-aged archive seed: ``finished_at`` 3.5 days back — a
    full chunk clear of the 2-day drop boundary — and a caller-chosen
    ``expire_at`` (the phase-3 caller's comment explains why the value is
    mode-deliberate). Shared by the lifecycle's phase 3 and the
    never-drops teeth leg, so both pin the IDENTICAL prey."""
    now = datetime.now(UTC)
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'lifecycle_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)""",
        jid,
        now,
        now + timedelta(hours=1),
        now - _AGED_FINISHED_AT,
        expire_at,
    )


def _row_gone(
    conn: asyncpg.Connection, schema: str, jid: uuid.UUID
) -> Callable[[], Awaitable[bool]]:
    """The poll condition: the row is gone (the policy's committed
    effect)."""

    async def _gone() -> bool:
        return (
            int(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', jid
                )
            )
            == 0
        )

    return _gone


async def _defer_retention_policies(conn: asyncpg.Connection, schema: str) -> None:
    """Push the RETENTION policy's next run a year out — the setup-side
    twin of :func:`_defer_compression_policies` (same measured first-run-
    seconds-after-registration mechanism, same race: an un-deferred
    retention job's chunk drop fires whenever the background scheduler
    arrives, and phase 3's row-exactness window needs the drop to land
    on the phase's own clock, via :func:`_force_retention_policies_now`)."""
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        schema,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            datetime.now(UTC) + timedelta(days=365),
        )


async def _defer_compression_policies(conn: asyncpg.Connection, schema: str) -> None:
    """Push the compression family's next run a year out — the sibling
    retention-interplay module's documented discipline (``_schedule_policies``
    defers ``policy%`` wholesale; the compression policy is not these
    phases' subject). Measured on this box: a fresh container's
    compression jobs make their FIRST run seconds after registration, and
    that run races the forced retention run for the SAME chunk (compress
    vs drop — the retention job's ``drop_chunks`` blocks on the
    compression job's transaction, or the conversion fails outright);
    the loser's retry clock (5 minutes for a retention job, 1 hour for a
    compression one) outlives any sane wait bound. Deferred, the forced
    retention runs own the chunk exclusively and the drain lands in
    seconds."""
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name IN "
        "('policy_compression', 'policy_columnstore')",
        schema,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            datetime.now(UTC) + timedelta(days=365),
        )


async def _force_retention_policies_now(conn: asyncpg.Connection, schema: str) -> None:
    """Pull every RETENTION policy's next run to now — the policy job
    itself (Timescale's background worker) does the draining, never a
    test shortcut. The compression policies are deferred first (see
    :func:`_defer_compression_policies`): they are not this lifecycle's
    subject, and an un-deferred compression run racing the forced
    retention run for the same chunk is the flake this module's gate
    runs measured."""
    await _defer_compression_policies(conn, schema)
    rows = await conn.fetch(
        "SELECT job_id FROM timescaledb_information.jobs "
        "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
        schema,
    )
    for r in rows:
        await conn.execute(
            "SELECT alter_job($1, next_start => $2::timestamptz)",
            r["job_id"],
            datetime.now(UTC),
        )


#: The TSL policy-drop wait budget, DERIVED from the repo's own measured
#: artifacts — never a guess:
#:
#: * The recorded first-tick artifact (``benchmarks/results/timescale-
#:   compression.json``'s ``first_tick_probe``, produced by
#:   ``benchmarks/timescale_compression.py`` Shape 7 and cited by
#:   ``docs/guides/ops.md``'s watch list): on a compressed-chunk
#:   hypertable the DML path trails the chunk's compression state by ONE
#:   EXECUTION — the recorded worst shapes deleted ``[12, 28, 0]`` (mixed:
#:   12 young + 28 compressed of the 40 qualifying) and ``[0, 40, 0]``
#:   (all-compressed: the first tick deletes 0 outright), with the bare
#:   ``DELETE`` contrast deleting all 40 on the first execution — the
#:   skip is statement-shape-dependent. So the policy's first EFFECTIVE
#:   run can be its SECOND dispatch, and convergence plus the confirming
#:   settled tick take THREE. The expired 60s budget baked in the
#:   single-dispatch assumption — that assumption is the red.
#: * One dispatch's bound: 60.0s — the sibling retention-interplay
#:   module's ``_wait_for`` default, the single-dispatch bound every
#:   policy leg there has run green under.
#: * The co-tenancy stretch: x20 — the stall band these runners produce
#:   under ``-n 2`` leg load (``tests/system_e2e/test_cancel_storm.py``'s
#:   ``_COTENANCY_STRETCH``, the derived-window doctrine
#:   ``tests/test_ratelimit_provider.py`` reuses).
#:
#: The full product (3 executions x 60s x 20 = 3600s) can never fire:
#: the harness's global per-test guillotine (``--timeout=300``,
#: ``pyproject.toml`` addopts) executes the TEST first — a wait budget
#: beyond it is decoration, and this module's own gate runs under
#: ``-n 2`` with siblings measured exactly that death (the poll still
#: sleeping at the 300s kill). The bound actually honored is therefore
#: the cap: 300 - 60 = 240s, where 60s is the leg's measured cost
#: OUTSIDE the wait (the migrate subprocess, the fold, the sweeps, the
#: disable round-trip — the solo leg runs 23s end to end). The budget
#: bounds FAILURE only: a healthy policy lands the drop in seconds and
#: the poll pays nothing, while a never-dropping policy must still RED
#: (the teeth leg below pins exactly that — it passes its own short
#: bound, because with the policy job disabled no dispatch can ever
#: land the drop, so any positive budget proves the red).
_POLICY_DROP_EXECUTIONS = 3  # the recorded artifact's convergence ticks
_SINGLE_DISPATCH_BUDGET_SECS = 60.0  # the sibling's single-dispatch bound
_COTENANCY_STRETCH = 20  # the cancel-storm doctrine's measured stall band
_DERIVED_DROP_BUDGET_SECS = (
    _POLICY_DROP_EXECUTIONS * _SINGLE_DISPATCH_BUDGET_SECS * _COTENANCY_STRETCH
)  # 3600s — honest, and unreachable (see the guillotine above)
_HARNESS_GUILLOTINE_SECS = 300.0  # --timeout=300, the global per-test bound
_LEG_OVERHEAD_SECS = 60.0  # the leg's measured cost outside the wait
_POLICY_DROP_BUDGET_SECS = min(
    _DERIVED_DROP_BUDGET_SECS, _HARNESS_GUILLOTINE_SECS - _LEG_OVERHEAD_SECS
)  # 240s

#: The teeth leg's bound (the mutation there makes ANY budget red).
_NEVER_DROPS_TEETH_BUDGET_SECS = 10.0


async def _wait_for(
    condition: Callable[[], Awaitable[bool]], *, what: str, timeout_secs: float = 60.0
) -> None:
    """Bounded poll for an effect of TimescaleDB's background worker (the
    sibling module's pattern; the worker is an external daemon no test
    clock can advance). The CONDITION is what has teeth — the real policy
    must do the drop; the bound only has to outlive the runner weather,
    which is why the policy leg passes the derived
    :data:`_POLICY_DROP_BUDGET_SECS`, not this generic default."""
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if await condition():
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.asyncio
async def test_tsl_policy_job_disabled_the_drop_wait_still_reds(
    timescale_tsl_dsn: str,
) -> None:
    """The policy-drop wait's TEETH: a policy that NEVER drops must red
    the wait, never green it — the lifecycle leg's green must always be
    the policy's own committed effect, not the wait's patience.

    The mutation: every ``policy_retention`` job in the schema is
    DISABLED (``alter_job(scheduled => FALSE)``) BEFORE the aged row even
    exists — so even the deploy's ~now-scheduled first policy run had
    nothing aged to drop, and after the disable no background-worker
    dispatch can ever take the chunk. The wait is then required to RAISE
    on its own bound, and the row must still be there. The teeth budget
    is short by construction (``_NEVER_DROPS_TEETH_BUDGET_SECS``): with
    the job disabled ANY positive budget proves the red — the full
    derived :data:`_POLICY_DROP_BUDGET_SECS` is the lifecycle leg's to
    pay, never this leg's.
    """
    schema = "tslt_" + new_uuid().hex[:12]
    up = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
    assert up.returncode == 0, up.stderr

    conn = await asyncpg.connect(timescale_tsl_dsn)
    try:
        # The mutation FIRST — the aged seed does not exist yet.
        jobs = await conn.fetch(
            "SELECT job_id, scheduled FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
            schema,
        )
        assert {r["scheduled"] for r in jobs} == {True}, (
            "setup: the registered retention policies start scheduled"
        )
        for r in jobs:
            await conn.execute("SELECT alter_job($1, scheduled => FALSE)", r["job_id"])
        still_scheduled = await conn.fetch(
            "SELECT job_id FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention' AND scheduled",
            schema,
        )
        assert still_scheduled == [], "the mutation must take: no scheduled policy remains"

        # The IDENTICAL prey the lifecycle's TSL leg drops: 3.5 days back,
        # a full chunk clear of the 2-day boundary, expire_at far future
        # (only a policy's partition clock can take it).
        aged_jid = new_uuid()
        await _seed_fully_aged_archive_row(
            conn, schema, aged_jid, expire_at=datetime.now(UTC) + timedelta(days=365)
        )
        # The force is deliberately STILL applied: a disabled job must
        # not run merely because its next_start says now.
        await _force_retention_policies_now(conn, schema)

        with pytest.raises(AssertionError, match="timed out waiting"):
            await _wait_for(
                _row_gone(conn, schema, aged_jid),
                what="the TSL retention policy to drop the fully-aged chunk",
                timeout_secs=_NEVER_DROPS_TEETH_BUDGET_SECS,
            )
        # And the prey SURVIVED the whole window: nothing but the policy
        # could have taken it, and the policy was disabled.
        assert not await _row_gone(conn, schema, aged_jid)(), (
            "the aged row must survive the never-drops window: the red above "
            "is the wait's own bound, not a missing seed"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── The doctor's real-rendering leg ───────────────────────────────────────


class _LifecyclePayload(BaseModel):
    value: int


@actor(name="lifecycle_probe", queue="default")
async def _lifecycle_probe(payload: _LifecyclePayload) -> None: ...


#: A registry the doctor subprocess can load (``--actors module:attr``):
#: one registered actor, so the report's other families have something to
#: read against the migrated schema.
_DOCTOR_REGISTRY: dict[str, Any] = {"lifecycle_probe": _lifecycle_probe}
_REGISTRY_PATH = "tests.test_timescale_three_mode_lifecycle:_DOCTOR_REGISTRY"


def _invoke_doctor(dsn: str, schema: str, *, flag: bool) -> subprocess.CompletedProcess[str]:
    """One REAL ``taskq doctor`` subprocess — the same sync-helper
    discipline as the sibling modules' ``_invoke_migrate_up`` (a blocking
    process run never sits inside an async body)."""
    env = {
        **os.environ,
        "TASKQ_PG_DSN": dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if flag else "false",
    }
    return subprocess.run(  # noqa: S603  # Why: static argv from sys.executable; no shell.
        [sys.executable, "-m", "taskq", "doctor", "--actors", _REGISTRY_PATH],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.asyncio
async def test_doctor_reports_the_detected_mode_on_real_containers(
    mode: StorageMode, mode_dsn: str
) -> None:
    """The doctor's first finding family, rendered by the REAL CLI
    subprocess against each REAL container: the line names the mode the
    server was detected in — and on the vanilla leg with the flag on
    (the family's red arm), the drift finding fires with the refusal it
    predicts."""
    schema = "tsld_" + new_uuid().hex[:12]
    flag_on = mode is not StorageMode.VANILLA
    up = _run(mode_dsn, schema, ["migrate", "up"], flag=flag_on)
    assert up.returncode == 0, f"migrate up failed on {mode.value}: {up.stderr}"

    result = _invoke_doctor(mode_dsn, schema, flag=flag_on)
    assert result.returncode == 0, f"doctor failed on {mode.value}: {result.stderr}"
    assert f"storage mode: {mode.value} - " in plain_cli_output(result.stdout), result.stdout

    # The vanilla leg's red arm: run it again with the flag ON (the env
    # contradicting the server) — the drift finding names the refusal.
    if mode is StorageMode.VANILLA:
        drifted = _invoke_doctor(mode_dsn, schema, flag=True)
        assert drifted.returncode == 0  # doctor never fails the shell
        assert (
            "storage mode drift: TASKQ_TIMESCALEDB_HYPERTABLES=true but this "
            "server detects vanilla" in plain_cli_output(drifted.stdout)
        ), drifted.stdout
        assert "TimescaleDBUnavailableError" in plain_cli_output(drifted.stdout)
    else:
        # The aligned arms stay green: no drift finding on either
        # Timescale mode with the flag on.
        assert "storage mode drift" not in plain_cli_output(result.stdout)


# ── The attack legs: mode flips, the TSL-call census, doctor-vs-reality ──


async def _set_license(dsn: str, license: str) -> None:
    """Flip the server's license GUC (ALTER SYSTEM + reload — the GUC
    cannot change inside a running session; NEW sessions see the new
    value). Every caller restores the prior value in its own finally."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f"ALTER SYSTEM SET timescaledb.license = '{license}'")
        await conn.execute("SELECT pg_reload_conf()")
    finally:
        await conn.close()


async def _live_license(dsn: str) -> str | None:
    """The license a NEW session sees (never an in-session cached one)."""
    conn = await asyncpg.connect(dsn)
    try:
        value = await conn.fetchval("SELECT current_setting('timescaledb.license', true)")
        return value if isinstance(value, str) else None
    finally:
        await conn.close()


async def _policy_rows_scoped(conn: asyncpg.Connection, schema: str) -> set[tuple[str, str]]:
    return await _policy_rows(conn, schema)


async def test_tsl_to_apache_downgrade_disable_refuses_with_the_license_remedy(
    tsl_container: Any, timescale_tsl_dsn: str
) -> None:
    """The operator's real downgrade path: convert under the full TSL
    license, THEN downgrade the license to apache (ALTER SYSTEM + reload).
    The conversion-era policies are still registered - and measured on
    2.30.1 they now fail on every background run - while every removal
    API refuses under the downgraded license. The disable must refuse
    loudly (never swap tables under a live policy) and its refusal must
    be HONEST: the license is why the policies survive, the removal APIs
    were never attempted, and the remedy names the license restore."""
    schema = "tslf_" + new_uuid().hex[:12]
    try:
        up = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
        assert up.returncode == 0, up.stderr
        await _set_license(timescale_tsl_dsn, "apache")
        assert await _live_license(timescale_tsl_dsn) == "apache"

        # The converging re-deploy under the downgraded license: exits 0
        # (the conversion is Apache-licensed, nothing to re-convert) - and
        # the five TSL-era policies STAY (nothing under apache can remove
        # them; measured: they fail on every background run).
        up2 = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
        assert up2.returncode == 0, up2.stderr
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            assert await _policy_rows_scoped(conn, schema) == {
                ("job_events", "policy_retention"),
                ("jobs_archive", "policy_retention"),
                ("job_attempts_archive", "policy_retention"),
                ("jobs_archive", "policy_compression"),
                ("job_attempts_archive", "policy_compression"),
            }, "the downgrade removes nothing: the TSL-era policies stay registered"

            # The disable: refuses loudly, with the license-honest message.
            down = _invoke_migrate_disable_hypertables(timescale_tsl_dsn, schema)
            assert down.returncode != 0, (
                "the disable must refuse a schema carrying live policies under "
                f"the apache license: {down.stdout}"
            )
            combined = down.stdout + down.stderr
            assert "timescaledb.license is 'apache'" in combined, combined
            assert "never attempted" in combined, combined
            assert "ALTER SYSTEM SET timescaledb.license = 'timescale'" in combined, combined
            assert "survived the removal APIs" not in combined, combined
            # The refusal is protective: nothing was swapped under the
            # live policies - the hypertables and their rows all survive.
            assert await _hypertables(conn, schema) == {
                "job_events",
                "jobs_archive",
                "job_attempts_archive",
            }
        finally:
            await conn.close()
    finally:
        await _set_license(timescale_tsl_dsn, "timescale")
        assert await _live_license(timescale_tsl_dsn) == "timescale"
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()


async def test_downgrade_strands_aged_rows_and_doctor_names_it(
    tsl_container: Any, timescale_tsl_dsn: str
) -> None:
    """The strand the downgrade causes, and the doctor's drift arm that
    names it. Under the downgraded license the TSL-era retention policies
    fail on every run (measured ``sqlerrcode 0A000``) - but the row-level
    expiry sweep DEFERS the aged end to them (the retention-policy floor
    reads the registered policy's own horizon). The compound effect,
    measured: an expired row older than the dead policies' horizon is
    deleted by NOBODY - the sweep defers, the policy cannot run - it
    strands silently. The doctor must surface exactly that state (the
    drift arm: apache license + live policy jobs), naming the strand and
    the license-restore remedy; a healthy-apache summary here is a LYING
    doctor ("retention is the row-level sweeps" is false on this server)."""
    schema = "tsls_" + new_uuid().hex[:12]
    try:
        up = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
        assert up.returncode == 0, up.stderr
        await _set_license(timescale_tsl_dsn, "apache")

        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            now = datetime.now(UTC)
            expired_jid = new_uuid()
            await conn.execute(
                f"""INSERT INTO {schema}.jobs_archive (
                    id, actor, queue, payload, max_attempts, retry_kind, status,
                    scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
                ) VALUES ($1, 'strand_actor', 'default', '{{"v":1}}'::jsonb, 3,
                    'transient', 'succeeded', $2, $3, $4, $2, $5)""",
                expired_jid,
                now,
                now + timedelta(hours=1),
                now - _AGED_FINISHED_AT,  # older than the 2-day policy horizon
                now - timedelta(minutes=1),  # expire_at HAS passed
            )
            sweep = await archive_expiry_sweep(conn, schema=schema)
            survivors = await conn.fetchval(
                f"SELECT count(*) FROM {schema}.jobs_archive WHERE id = $1", expired_jid
            )
            assert sweep.total_deleted == 0 and survivors == 1, (
                "the measured strand: the sweep defers the aged end to the dead "
                "policies, the policies fail under the license - the expired row "
                f"was deleted by nobody (sweep deleted {sweep.total_deleted})"
            )
        finally:
            await conn.close()

        # The doctor's drift arm names the stranded state and the remedy.
        result = _invoke_doctor(timescale_tsl_dsn, schema, flag=True)
        assert result.returncode == 0
        assert (
            "storage mode drift: this server's timescaledb.license is 'apache' but 5 "
            "TimescaleDB policy job(s) from an earlier timescale-license deployment are "
            "still registered" in plain_cli_output(result.stdout)
        ), result.stdout
        assert "strand" in plain_cli_output(result.stdout), result.stdout
        assert "ALTER SYSTEM SET timescaledb.license = 'timescale'" in plain_cli_output(
            result.stdout
        )
    finally:
        await _set_license(timescale_tsl_dsn, "timescale")
        assert await _live_license(timescale_tsl_dsn) == "timescale"
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()


async def test_apache_to_tsl_upgrade_adopts_policies_on_the_next_migrate(
    tsl_container: Any, timescale_tsl_dsn: str
) -> None:
    """The reverse flip - the operator UPGRADES apache to TSL mid-life:
    the apache-era hypertables converted bare (no policies), and the next
    ``migrate up`` under the restored license must ADOPT the full policy
    set (retention on all three tables, the columnstore on the two
    archives) - the schema is not stuck bare, and the disable still
    round-trips cleanly afterward."""
    schema = "tslu_" + new_uuid().hex[:12]
    try:
        await _set_license(timescale_tsl_dsn, "apache")
        up = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
        assert up.returncode == 0, up.stderr
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            assert await _hypertables(conn, schema) == {
                "job_events",
                "jobs_archive",
                "job_attempts_archive",
            }
            assert await _policy_rows_scoped(conn, schema) == set(), (
                "the apache-era conversion registers nothing"
            )
        finally:
            await conn.close()

        # The UPGRADE, then the next deploy: the policies are adopted.
        await _set_license(timescale_tsl_dsn, "timescale")
        assert await _live_license(timescale_tsl_dsn) == "timescale"
        up2 = _run(timescale_tsl_dsn, schema, ["migrate", "up"], flag=True)
        assert up2.returncode == 0, up2.stderr
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            assert await _policy_rows_scoped(conn, schema) == {
                ("job_events", "policy_retention"),
                ("jobs_archive", "policy_retention"),
                ("job_attempts_archive", "policy_retention"),
                ("jobs_archive", "policy_compression"),
                ("job_attempts_archive", "policy_compression"),
            }, "the TSL upgrade's next deploy adopts the full policy set"
        finally:
            await conn.close()

        # And the round-trip still lands: disable cleanly to plain tables.
        down = _invoke_migrate_disable_hypertables(timescale_tsl_dsn, schema)
        assert down.returncode == 0, down.stderr
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            assert await _hypertables(conn, schema) == set()
            assert await _policy_rows_scoped(conn, schema) == set()
        finally:
            await conn.close()
    finally:
        await _set_license(timescale_tsl_dsn, "timescale")
        conn = await asyncpg.connect(timescale_tsl_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()


_TSL_ONLY_STATEMENT = re.compile(
    r"(add_retention_policy|remove_retention_policy"
    r"|add_columnstore_policy|add_compression_policy"
    r"|remove_columnstore_policy|remove_compression_policy"
    r"|compress_chunk|decompress_chunk|recompress_chunk"
    r"|drop_chunks|alter_job"
    r"|timescaledb\.compress)"
)


async def test_apache_mode_issues_zero_tsl_only_statements(
    apache_container: Any, timescale_apache_dsn: str
) -> None:
    """The exhaustive TSL-call census, run on the apache-license server
    with the server's OWN statement log (``log_statement = all`` — the
    capture that records every ATTEMPTED statement, including ones an
    error aborts). One full lifecycle — migrate up (the enable), operate
    (the real prune→archive fold), the row-level drain, the disable
    round-trip — bracketed by a unique marker statement, then the log is
    swept end to end: ZERO TSL-only statements may be attempted anywhere
    in the window. The census is negative by absence: the enable path
    must branch BEFORE the policy APIs (not call-and-catch them), the
    disable path must skip the removals (not call-and-ignore), the
    decompression-budget probe must not run (no columnstore was adopted),
    and the sweeps' floor probe must stay on the Apache-legal information
    views. One missed call site — a code path the builder didn't visit —
    shows up here as the attempted statement itself. (``to_regproc`` is
    deliberately absent from the pattern: the detection's machinery probe
    is a catalog read the apache license executes fine.)"""
    assert await _live_license(timescale_apache_dsn) == "apache"
    schema = "tslcensus_" + new_uuid().hex[:10]
    marker = f"census-marker-{new_uuid().hex}"
    await _set_license(timescale_apache_dsn, "apache")  # idempotent; reload is harmless
    conn = await asyncpg.connect(timescale_apache_dsn)
    try:
        await conn.execute("ALTER SYSTEM SET log_statement = 'all'")
        await conn.execute("SELECT pg_reload_conf()")
    finally:
        await conn.close()
    try:
        # The marker, its own logged statement: everything AFTER it in the
        # server's statement log is the census window.
        conn = await asyncpg.connect(timescale_apache_dsn)
        try:
            await conn.execute(f"SELECT '{marker}'")
        finally:
            await conn.close()

        # The full lifecycle, every phase the builder claims is clean.
        up = _run(timescale_apache_dsn, schema, ["migrate", "up"], flag=True)
        assert up.returncode == 0, up.stderr
        conn = await asyncpg.connect(timescale_apache_dsn)
        try:
            for _ in range(2):
                await create_running_job(conn, schema, new_uuid(), new_uuid(), with_events=False)
            folded = [
                await _seed_terminal_job(conn, schema, status="succeeded", age=timedelta(hours=2))
                for _ in range(2)
            ]
            prune = await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": _FOLD_RETENTION},
                archive_retention=_FOLD_ARCHIVE_RETENTION,
                schema=schema,
            )
            assert prune.total_deleted == 2
            expired_jid = await _seed_expired_archive_row(conn, schema)
            sweep = await archive_expiry_sweep(conn, schema=schema)
            assert sweep.total_deleted == 1
            assert folded or expired_jid  # the seeds were the operate leg's subject
        finally:
            await conn.close()
        down = _invoke_migrate_disable_hypertables(timescale_apache_dsn, schema)
        assert down.returncode == 0, down.stderr
        conn = await asyncpg.connect(timescale_apache_dsn)
        try:
            assert await _hypertables(conn, schema) == set()
        finally:
            await conn.close()
    finally:
        # Restore the statement log, whatever the census found.
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(timescale_apache_dsn)
            try:
                await conn.execute("ALTER SYSTEM SET log_statement = 'none'")
                await conn.execute("SELECT pg_reload_conf()")
            finally:
                await conn.close()
        conn = await asyncpg.connect(timescale_apache_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()

    # The census read: the server's own statement log, after the marker.
    _, stderr = apache_container.get_logs()
    log_lines = stderr.decode("utf-8", errors="replace").splitlines()
    marker_at = max((i for i, line in enumerate(log_lines) if marker in line), default=None)
    assert marker_at is not None, "the marker statement never reached the server log"
    statements = [ln for ln in log_lines[marker_at + 1 :] if "statement:" in ln]
    assert statements, "the census window captured no statements at all"
    offenders = [
        ln for ln in statements if "to_regproc" not in ln and _TSL_ONLY_STATEMENT.search(ln)
    ]
    assert offenders == [], (
        "the apache mode attempted TSL-only calls the builder's paths were "
        f"supposed to skip: {offenders[:10]}"
    )


# ── The doctor's capability lines against the server's MEASURED truth ────


async def _measure_policy_capability(dsn: str) -> str:
    """The server's OWN verdict on the one capability the mode matrix
    turns on: can this license register a retention policy at all? A
    scratch hypertable (the conversion itself is Apache-licensed), one
    ``add_retention_policy`` attempt, everything dropped after. Vanilla
    refuses by absence (``create_hypertable`` is not a function there),
    apache by license - both measure "refused"."""
    scratch = "cap_" + new_uuid().hex[:10]
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'CREATE SCHEMA "{scratch}"')
        await conn.execute(f'CREATE TABLE "{scratch}".probe (a bigint, b timestamptz NOT NULL)')
        try:
            await conn.execute(f"SELECT create_hypertable('\"{scratch}\".probe', 'b')")
        except (asyncpg.FeatureNotSupportedError, asyncpg.UndefinedFunctionError):
            return "refused"
        try:
            await conn.execute(
                f"SELECT add_retention_policy('\"{scratch}\".probe', interval '7 days')"
            )
            return "accepted"
        except asyncpg.FeatureNotSupportedError:
            return "refused"
        finally:
            with contextlib.suppress(asyncpg.FeatureNotSupportedError):
                await conn.execute(
                    f"SELECT remove_retention_policy('\"{scratch}\".probe', if_exists => TRUE)"
                )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{scratch}" CASCADE')
        await conn.close()


async def _doctor_mode_line(dsn: str, schema: str, *, flag: bool) -> str:
    """The doctor subprocess's stdout on a REAL migrated schema."""
    result = _invoke_doctor(dsn, schema, flag=flag)
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.mark.asyncio
async def test_doctor_capability_lines_match_measured_reality(
    mode: StorageMode, mode_dsn: str
) -> None:
    """The worst outcome is a doctor that renders a capability the server
    does not have, so each mode's rendered line is checked against the
    server's MEASURED verdict, not against the enum: the mode summary the
    doctor prints must be the exact one the detection's
    ``STORAGE_MODE_SUMMARY`` pins, and the policy capability the summary
    implies must be what a real ``add_retention_policy`` attempt on a
    scratch hypertable just did."""
    schema = "tslr_" + new_uuid().hex[:12]
    flag_on = mode is not StorageMode.VANILLA
    up = _run(mode_dsn, schema, ["migrate", "up"], flag=flag_on)
    assert up.returncode == 0, up.stderr

    measured = await _measure_policy_capability(mode_dsn)
    output = await _doctor_mode_line(mode_dsn, schema, flag=flag_on)

    # The rendered line is the exact pinned summary - not a paraphrase.
    from taskq.timescale import STORAGE_MODE_SUMMARY

    assert f"storage mode: {mode.value} - {STORAGE_MODE_SUMMARY[mode]} " in output
    if mode is StorageMode.TIMESCALE_TSL:
        assert measured == "accepted", (
            "the doctor renders policy-driven retention on this server; the "
            "server just refused it - the doctor would be lying"
        )
        assert "columnstore on the archive tables" in output
    elif mode is StorageMode.TIMESCALE_APACHE:
        assert measured == "refused", (
            "the doctor renders 'the chunk-drop policies AND the columnstore "
            "are Timescale-license features this server's license disables'; "
            "the server just ACCEPTED a retention policy - the doctor would "
            "be lying in the other direction"
        )
        # The apache line must promise rowstore, and must NOT promise the
        # columnstore or the policies.
        assert "rowstore" in output
        assert "columnstore on the archive tables" not in output
        assert "policy-driven chunk-drop retention;" not in output
    else:
        assert measured == "refused"  # vanilla: no extension, nothing to accept
        assert "no hypertables, no columnstore" in output
    # The honest edge, every mode: detection reads the SERVER, never the
    # flag - with the flag OFF the mode line still renders the server's
    # capability (the flag-off deployment is supported on every mode), and
    # no drift arm fires for that alignment.
    quiet = _invoke_doctor(mode_dsn, schema, flag=False)
    assert quiet.returncode == 0
    assert f"storage mode: {mode.value} - " in quiet.stdout
    assert "storage mode drift" not in quiet.stdout
    conn = await asyncpg.connect(mode_dsn)
    try:
        await _drop_schema(conn, schema)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_detection_needs_no_privilege_and_never_lies_vanilla(
    tsl_container: Any, timescale_tsl_dsn: str
) -> None:
    """The detection edges, on the real server:

    * a RESTRICTED role (no superuser, no extension privileges - just
      CONNECT) detects the same mode. Measured on PG18/2.30.1: every
      probe read is PUBLIC - the catalogs, ``timescaledb.license`` (its
      value reads fine restricted), ``to_regproc`` - except ONE:
      examining ``shared_preload_libraries`` raises
      ``InsufficientPrivilegeError`` for a restricted role (the setting
      exists, so missing_ok does not apply). The probe reads that one
      unknown as its advisory default (the preload guard exists to name
      a confusing DDL error; the DDL is the loud backstop), so detection
      itself works for unprivileged operators and the doctor's first
      family renders for them;
    * a DETECTION PROBE failure of any OTHER shape is fail-LOUD, never
      fail-to-vanilla: a doctor that cannot read the catalogs must crash
      with the error, not render the honest-looking 'storage mode:
      vanilla - plain tables' line for a server it could not see
      (vanilla is a real mode with its own retention semantics - guessing
      it from a failed probe would be the one lie the family cannot
      tell)."""
    # The restricted role: CONNECT only.
    conn = await asyncpg.connect(timescale_tsl_dsn)
    try:
        await _drop_restricted_role(conn)
        await conn.execute("CREATE ROLE detc_ro LOGIN PASSWORD 'detc_ro'")
        await conn.execute("GRANT CONNECT ON DATABASE taskq TO detc_ro")
    finally:
        await conn.close()
    restricted_dsn = timescale_tsl_dsn.replace("taskq:taskq@", "detc_ro:detc_ro@")
    detected = await _detect(restricted_dsn)
    assert detected is StorageMode.TIMESCALE_TSL, (
        "detection must work for an unprivileged role: every mode-deciding "
        f"probe is a PUBLIC catalog read (got {detected})"
    )
    # ... and the license value itself reads fine restricted - the GUC the
    # mode DECIDES on is not among the PG18-examination-restricted ones.
    restricted = await asyncpg.connect(restricted_dsn)
    try:
        assert (
            await restricted.fetchval("SELECT current_setting('timescaledb.license', true)")
            == "timescale"
        )
    finally:
        await restricted.close()
    conn = await asyncpg.connect(timescale_tsl_dsn)
    try:
        await _drop_restricted_role(conn)
    finally:
        await conn.close()

    # The fail-loud edge: a probe failure outside the one known privilege
    # shape PROPAGATES - it must never classify as vanilla (the unit-tier
    # pin of the arm's honesty).
    class _BrokenConn:
        async def fetchval(self, query: str, *args: Any) -> Any:
            raise RuntimeError("the catalogs are unreachable from here")

    with pytest.raises(RuntimeError, match="unreachable"):
        await detect_storage_mode(_BrokenConn())


async def _drop_restricted_role(conn: asyncpg.Connection) -> None:
    """Drop the detection-edge role cleanly: the CONNECT grant on the
    database is a dependency DROP ROLE refuses to take silently."""
    with contextlib.suppress(Exception):
        await conn.execute("REVOKE CONNECT ON DATABASE taskq FROM detc_ro")
    await conn.execute("DROP ROLE IF EXISTS detc_ro")

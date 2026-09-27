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
import os
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
    await conn.execute(
        f"""INSERT INTO {schema}.jobs_archive (
            id, actor, queue, payload, max_attempts, retry_kind, status,
            scheduled_at, schedule_to_close, finished_at, archived_at, expire_at
        ) VALUES ($1, 'lifecycle_actor', 'default', '{{"v":1}}'::jsonb, 3, 'transient',
            'succeeded', $2, $3, $4, $2, $5)""",
        aged_jid,
        now,
        now + timedelta(hours=1),
        now - _AGED_FINISHED_AT,
        aged_expire_at,
    )

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
        # own committed effect.
        await _force_retention_policies_now(conn, schema)
        await _wait_for(
            _row_gone(conn, schema, aged_jid),
            what="the TSL retention policy to drop the fully-aged chunk",
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


async def _force_retention_policies_now(conn: asyncpg.Connection, schema: str) -> None:
    """Pull every RETENTION policy's next run to now — the policy job
    itself (Timescale's background worker) does the draining, never a
    test shortcut. The compression policies stay deferred: they are not
    this lifecycle's subject."""
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


async def _wait_for(
    condition: Callable[[], Awaitable[bool]], *, what: str, timeout_secs: float = 60.0
) -> None:
    """Bounded poll for an effect of TimescaleDB's background worker (the
    sibling module's pattern; the worker is an external daemon no test
    clock can advance)."""
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        if await condition():
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


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
    assert f"storage mode: {mode.value} - " in result.stdout, result.stdout

    # The vanilla leg's red arm: run it again with the flag ON (the env
    # contradicting the server) — the drift finding names the refusal.
    if mode is StorageMode.VANILLA:
        drifted = _invoke_doctor(mode_dsn, schema, flag=True)
        assert drifted.returncode == 0  # doctor never fails the shell
        assert (
            "storage mode drift: TASKQ_TIMESCALEDB_HYPERTABLES=true but this "
            "server detects vanilla" in drifted.stdout
        ), drifted.stdout
        assert "TimescaleDBUnavailableError" in drifted.stdout
    else:
        # The aligned arms stay green: no drift finding on either
        # Timescale mode with the flag on.
        assert "storage mode drift" not in result.stdout

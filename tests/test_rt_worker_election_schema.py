"""The #105 fix attacked at the LOOP level: two real elections, two
schemas, one database.

The lock-level property (two schema-qualified lock names both winnable)
is pinned in tests/test_advisory_lock_schema_isolation.py. These tests
drive the actual election code path — ``MaintenanceLeader._election_loop``
with real connections against migrated schemas — so a regression anywhere
between ``schema_lock_name`` and the loop's ``pg_try_advisory_lock`` call
(upsert, dedicated conns, is_leader) fails here, not just at the lock
name. The regression shape within ONE schema is pinned too: the loser
records lock contention and an election failure and keeps retrying — no
teardown — and a demoted leader's closed connections re-arm the same
schema's lock for the next worker.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.constants import schema_lock_name
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

_ATTEMPTS_METRIC = "taskq.leader.election_attempts"
_FAILURES_METRIC = "taskq.leader.election_failures"
_CONTENTION_METRIC = "taskq.leader.lock_contention"


@pytest_asyncio.fixture(scope="module")
async def module_pg_schema_b(
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[ModulePgSchema]:
    """A SECOND migrated schema in the SAME database as the primary.

    Same derivation as the isolation tests' second schema (hex suffix
    swapped for ``_b``) so the name stays inside the identifier budget
    and can never collide with the primary. Election needs the workers
    and maintenance_leader tables, which apply_pending creates.
    """
    schema_name = module_pg_schema.schema_name[:-2] + "_b"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await apply_pending(conn, schema=schema_name)
    finally:
        await conn.close()

    yield ModulePgSchema(schema_name=schema_name, pg_dsn=module_pg_schema.pg_dsn)

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
    finally:
        await conn.close()


class _FactoryLedger:
    """Tracks every connection a leader's factory opened, for teardown.

    Advisory session locks die with their connection, so a leaked
    factory conn would hold a schema's election lock open and poison
    later tests in this module.
    """

    def __init__(self, pg_dsn: str) -> None:
        self.pg_dsn = pg_dsn
        self.conns: list[asyncpg.Connection] = []

    async def open(self) -> asyncpg.Connection:
        conn = await asyncpg.connect(self.pg_dsn)
        self.conns.append(conn)
        return conn

    async def close_all(self) -> None:
        for conn in self.conns:
            if not conn.is_closed():
                with contextlib.suppress(Exception):
                    await conn.close()
        self.conns.clear()


async def _register_worker(pg_dsn: str, schema: str, worker_id: UUID) -> None:
    """Insert the workers row the election upsert's FK requires."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
            "VALUES ($1, 'election-test', 1, '{default}')",
            worker_id,
        )
    finally:
        await conn.close()


def _election_leader(
    pg_dsn: str,
    schema: str,
    ledger: _FactoryLedger,
    worker_id: UUID,
) -> MaintenanceLeader:
    """A real MaintenanceLeader whose election loop opens REAL connections
    through ``leader_conn_factory`` and runs the production election SQL
    path (lock attempt, upsert, dedicated conns, is_leader)."""
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema},
        validate=False,
    )
    settings.heartbeat_interval = (
        0.1  # bypasses the ge=0.5 constraint by hand: election cycles must repeat quickly in-test
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=None,  # pyright: ignore[reportArgumentType]  # Why: the election loop touches no pool — only the factory-built dedicated conns.
        heartbeat_pool=None,  # pyright: ignore[reportArgumentType]
        worker_pool=None,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
        leader_conn_factory=ledger.open,
        owns_leader_conn=True,
    )
    return MaintenanceLeader(
        deps,
        worker_id,
        cast("Backend", object()),
        # A clock the election loop never consults: elections are decided
        # by the server-side lock and upsert, not the client clock.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
    )


async def _await_election(
    leader: MaintenanceLeader,
    *,
    timeout_secs: float = 10.0,
) -> asyncio.Task[None]:
    """Run the leader's election loop until it wins (or fail the test)."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
    try:
        await asyncio.wait_for(leader._deps.is_leader.wait(), timeout=timeout_secs)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return task


async def _run_election_cycles(
    leader: MaintenanceLeader,
    *,
    cycles: int,
    timeout_secs: float = 10.0,
) -> asyncio.Task[None]:
    """Run the election loop and stop after the conn probe of the *cycles*
    th full cycle (a losing cycle ends with the loop sleeping toward its
     next attempt, alive)."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
    try:
        for _ in range(cycles):
            await asyncio.sleep(0.15)
        assert not task.done(), "the election loop died mid-retry — a teardown, not a retry"
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return task


# ── W1: two real elections, two schemas, one database ────────────────────


async def test_two_real_election_loops_both_win_their_own_schema(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """THE #105 regression at the loop level: two MaintenanceLeaders, two
    schemas of one database, elections running CONCURRENTLY — both must
    hold leadership at once, and each schema's singleton row must name
    its own worker. Under the unqualified lock the perpetual loser never
    ran its sweeps while its dispatch kept flowing: a healthy-looking
    fleet with stalled scheduled work."""
    pg_dsn = module_pg_schema.pg_dsn
    ledger_a = _FactoryLedger(pg_dsn)
    ledger_b = _FactoryLedger(pg_dsn)
    worker_a = new_uuid()
    worker_b = new_uuid()
    await _register_worker(pg_dsn, module_pg_schema.schema_name, worker_a)
    await _register_worker(pg_dsn, module_pg_schema_b.schema_name, worker_b)
    leader_a = _election_leader(pg_dsn, module_pg_schema.schema_name, ledger_a, worker_a)
    leader_b = _election_leader(pg_dsn, module_pg_schema_b.schema_name, ledger_b, worker_b)

    try:
        # Both elections in flight at the same time — the contention the
        # unqualified lock produced, now across schemas it must not.
        task_a = asyncio.create_task(_await_election(leader_a))
        task_b = asyncio.create_task(_await_election(leader_b))
        await asyncio.wait_for(asyncio.gather(task_a, task_b), timeout=15.0)

        assert leader_a._deps.is_leader.is_set() is True  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.
        assert leader_b._deps.is_leader.is_set() is True, (
            "schema 2's worker lost the election to schema 1's in the same "
            "database — the cross-schema serialization regression, at the "
            "election loop level"
        )

        for schema, worker_id in (
            (module_pg_schema.schema_name, worker_a),
            (module_pg_schema_b.schema_name, worker_b),
        ):
            conn = await asyncpg.connect(pg_dsn)
            try:
                row = await conn.fetchrow(
                    f'SELECT worker_id FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
                )
            finally:
                await conn.close()
            assert row is not None, f"no leader row upserted in {schema}"
            assert row["worker_id"] == worker_id, (
                f"the singleton row in {schema} names the wrong worker — the "
                "election wrote to (or read from) the wrong schema"
            )
    finally:
        await ledger_a.close_all()
        await ledger_b.close_all()


# ── W1 regression shape: the WITHIN-schema loser ─────────────────────────


@pytest.fixture
def election_metrics(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instruments for the election emitters (attempts, failures,
    lock contention) plus ``_otel_enabled`` forced on."""
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as the sweep-metrics fixture.
    )
    monkeypatch.setattr(
        otel_mod, "_leader_election_attempts", meter.create_counter(_ATTEMPTS_METRIC)
    )
    monkeypatch.setattr(
        otel_mod, "_leader_election_failures", meter.create_counter(_FAILURES_METRIC)
    )
    monkeypatch.setattr(otel_mod, "_lock_contention", meter.create_counter(_CONTENTION_METRIC))
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    return reader


async def test_election_loser_records_contention_and_failure_and_retries(
    module_pg_schema: ModulePgSchema,
    election_metrics: InMemoryMetricReader,
) -> None:
    """Within ONE schema the lock still serializes, and the loser is the
    detector: it records ``lock_contention`` with the schema-qualified
    lock name and an election failure per lost attempt, and it RETRIES —
    the loop stays alive with is_leader unset, and no leader row is
    written by the loser (no teardown, no phantom leadership)."""
    from taskq.testing.otel import counter_data_points, counter_value

    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name
    lock_name = schema_lock_name("maintenance_leader", schema)

    holder = await asyncpg.connect(pg_dsn)
    got = await holder.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name)
    assert got is True, "test setup: the holder must own the schema's election lock"
    # Clean singleton slate: an earlier test's winner may have left its row
    # in this module-shared schema; the assertion below must be about THIS
    # loser never writing, not about table state it inherited.
    await holder.execute(f'DELETE FROM "{schema}".maintenance_leader')  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.

    ledger = _FactoryLedger(pg_dsn)
    worker_id = new_uuid()
    await _register_worker(pg_dsn, schema, worker_id)
    leader = _election_leader(pg_dsn, schema, ledger, worker_id)
    try:
        await _run_election_cycles(leader, cycles=3)

        assert leader._deps.is_leader.is_set() is False  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.
        failures = counter_value(election_metrics, _FAILURES_METRIC)
        assert failures >= 3, (
            f"a lock held by another session must record election failures per "
            f"attempt; got {failures}"
        )
        contention = [
            dp
            for dp in counter_data_points(election_metrics, _CONTENTION_METRIC)
            if dp.attributes == {"lock": lock_name}
        ]
        assert contention and contention[0].value >= 3, (
            "the losing side must record lock_contention with the exact "
            f"schema-qualified lock name {lock_name!r}"
        )
        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await conn.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
            )
        finally:
            await conn.close()
        assert row is None, "the loser must not write a leader row — no phantom leadership"
    finally:
        await ledger.close_all()
        await holder.close()


# ── W10: demote, then the next worker wins the SAME schema ───────────────


async def test_demoted_leaders_conns_re_arm_the_same_schema(
    module_pg_schema: ModulePgSchema,
) -> None:
    """A full promote/demote cycle on ONE schema: after the demoted
    leader's connections close (the conn-close half of demotion), a
    second worker must be able to win the SAME schema's election — the
    schema-qualified session locks are held on the leader's conns and
    released by their close, not leaked past the demotion."""
    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name

    ledger_a = _FactoryLedger(pg_dsn)
    worker_a = new_uuid()
    await _register_worker(pg_dsn, schema, worker_a)
    leader_a = _election_leader(pg_dsn, schema, ledger_a, worker_a)
    ledger_b = _FactoryLedger(pg_dsn)
    worker_b = new_uuid()
    await _register_worker(pg_dsn, schema, worker_b)
    leader_b = _election_leader(pg_dsn, schema, ledger_b, worker_b)

    try:
        await _await_election(leader_a)
        assert leader_a._deps.is_leader.is_set() is True  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.

        # The demotion path: drop the leader conn (closing it releases the
        # election session lock), close the leader-owned dedicated conns.
        await leader_a._drop_leader_conn(reason="test_demote")  # pyright: ignore[reportPrivateUsage]  # Why: driving the demotion path directly is the point of the test.
        await leader_a._close_leader_owned_conns()  # pyright: ignore[reportPrivateUsage]  # Why: see above.
        assert leader_a._deps.is_leader.is_set() is False  # pyright: ignore[reportPrivateUsage]  # Why: see above.

        await _await_election(leader_b)
        assert leader_b._deps.is_leader.is_set() is True, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "the demoted leader's closed connections must re-arm the same "
            "schema's election lock for the next worker"
        )

        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await conn.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
            )
        finally:
            await conn.close()
        assert row is not None and row["worker_id"] == worker_b
    finally:
        await ledger_a.close_all()
        await ledger_b.close_all()

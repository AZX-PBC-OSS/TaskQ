"""Two real elections, two schemas, one database — the loop-level pin that
election is schema-scoped.

The lock-level property (two schema-qualified lock names both winnable)
is pinned in tests/test_advisory_lock_schema_isolation.py. These tests
drive the actual election code path — ``MaintenanceLeader._election_loop``
with real connections against migrated schemas — so a regression anywhere
between ``schema_lock_name`` and the lease statements (elect, dedicated
conns, is_leader) fails here, not just at the lock name. The regression
shape within ONE schema is pinned too: the loser records an election
failure per lost cycle and keeps retrying — no teardown, no phantom
leadership — and a demoted leader's row stops being renewed, so the next
worker takes the schema over once that lease lapses.
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
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader, build_leader_lease_sql

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
    factory conn would hold a schema's courtesy election lock open and
    confuse later tests in this module.
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


@pytest_asyncio.fixture(autouse=True)
async def _clean_leader_row(module_pg_schema: ModulePgSchema) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the runner; pyright does not track fixture usage.
    """Per-test clean slate on the module-shared singleton lease row.

    A won election leaves a row whose lease outlives the test; the next
    test's candidate would then follow a ghost holder until the lapse.
    """
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(
            f'DELETE FROM "{module_pg_schema.schema_name}".maintenance_leader'  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
        )
    finally:
        await conn.close()


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
    *,
    leader_lease: float | None = None,
) -> MaintenanceLeader:
    """A real MaintenanceLeader whose election loop opens REAL connections
    through ``leader_conn_factory`` and runs the production election SQL
    path (elect, dedicated conns, is_leader)."""
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema},
        validate=False,
    )
    settings.heartbeat_interval = (
        0.1  # bypasses the ge=0.5 constraint by hand: election cycles must repeat quickly in-test
    )
    if leader_lease is not None:
        # Same by-hand bypass as heartbeat_interval: a short lease lets a
        # test reach the lapse horizon in a second instead of the shipped
        # 40 s. resolved_leader_lease still floors it at 4 heartbeats.
        settings.leader_lease = leader_lease
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


# ── W1: two real elections, two schemas, one database ────────────────────


async def test_two_real_election_loops_both_win_their_own_schema(
    module_pg_schema: ModulePgSchema,
    module_pg_schema_b: ModulePgSchema,
) -> None:
    """The cross-schema regression at the loop level: two MaintenanceLeaders,
    two schemas of one database, elections running CONCURRENTLY — both must
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


async def test_election_loser_records_failure_and_retries(
    module_pg_schema: ModulePgSchema,
    election_metrics: InMemoryMetricReader,
) -> None:
    """Within ONE schema the lease row serializes: while a live holder holds
    it, the follower loses every cycle, records an election failure per lost
    attempt, and RETRIES — the loop stays alive with is_leader unset, and no
    leader row is written by the loser (no teardown, no phantom leadership).
    Contention is deliberately NOT recorded per lost cycle here: following a
    stable live holder is the healthy steady state, and the per-transition
    semantics are pinned by the stable-fleet test below."""
    from taskq.testing.otel import counter_value

    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name

    # A live lease holder: a real elected leader whose row stays fresh for
    # the whole window (its written lease is far longer than the follower's
    # three cycles).
    holder_ledger = _FactoryLedger(pg_dsn)
    holder_id = new_uuid()
    await _register_worker(pg_dsn, schema, holder_id)
    holder = _election_leader(pg_dsn, schema, holder_ledger, holder_id)
    follower_ledger = _FactoryLedger(pg_dsn)
    follower_id = new_uuid()
    await _register_worker(pg_dsn, schema, follower_id)
    follower = _election_leader(pg_dsn, schema, follower_ledger, follower_id)
    try:
        await _await_election(holder)

        shutdown = asyncio.Event()
        task = asyncio.create_task(follower._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
        try:
            # The failure counter IS the observable: waiting on it is a
            # deadline-bounded condition, not a pacing sleep.
            await wait_for_condition(
                lambda: counter_value(election_metrics, _FAILURES_METRIC) >= 3,
                description="the follower must record an election failure per lost cycle",
                timeout=10.0,
            )
            assert not task.done(), "the election loop died mid-retry — a teardown, not a retry"
        finally:
            shutdown.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert follower._deps.is_leader.is_set() is False  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.
        conn = await asyncpg.connect(pg_dsn)
        try:
            row = await conn.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
            )
        finally:
            await conn.close()
        assert row is not None and row["worker_id"] == holder_id, (
            "the loser must not write a leader row — no phantom leadership"
        )
    finally:
        await follower_ledger.close_all()
        await holder_ledger.close_all()


# ── A stable 2-worker fleet must not look like sustained contention ──────


async def test_stable_follower_does_not_sustain_contention_like_a_stuck_holder(
    module_pg_schema: ModulePgSchema,
    election_metrics: InMemoryMetricReader,
) -> None:
    """A stable follower's losses must not sustain the contention counter.

    docs/guides/runbooks.md#taskqleaderlockcontention reads
    ``taskq_leader_lock_contention_total`` as distinguishing "the
    legitimately-elected leader during handover (benign, and filtered out
    by the sustained-rate alert) or a stuck/dead one", relying on healthy
    contention being "brief and intermittent, which is why the alert
    requires a sustained rate."

    A follower in a healthy fleet loses every heartbeat for the life of
    the process; if each loss recorded contention, every fleet larger
    than one would hold the alert's rate condition forever, and the
    runbook's recovery check ("stops rising") could never be satisfied.
    Contention is therefore recorded per distinct observed holder — the
    transition — and this test drives a follower's loop through a
    settling window and an equally long steady-state window of lost
    cycles against an undisturbed leader, expecting the counter to move
    only across the first.
    """
    from taskq.testing.otel import counter_data_points, counter_value

    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name
    lock_name = schema_lock_name("maintenance_leader", schema)

    ledger_leader = _FactoryLedger(pg_dsn)
    worker_leader = new_uuid()
    await _register_worker(pg_dsn, schema, worker_leader)
    leader = _election_leader(pg_dsn, schema, ledger_leader, worker_leader)

    ledger_follower = _FactoryLedger(pg_dsn)
    worker_follower = new_uuid()
    await _register_worker(pg_dsn, schema, worker_follower)
    follower = _election_leader(pg_dsn, schema, ledger_follower, worker_follower)

    try:
        # Establish a genuinely stable fleet: worker_leader wins and its
        # written lease (resolved_leader_lease, tens of seconds at these
        # settings) far outlives this test's windows, so the row alone
        # keeps the follower losing — no renewal machinery is needed to
        # keep the fleet stable for the measurement.
        await _await_election(leader)
        assert leader._deps.is_leader.is_set() is True  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.

        def _contention_value() -> int:
            points = [
                dp
                for dp in counter_data_points(election_metrics, _CONTENTION_METRIC)
                if dp.attributes == {"lock": lock_name}
            ]
            return int(points[0].value) if points else 0

        def _failures() -> float:
            return counter_value(election_metrics, _FAILURES_METRIC)

        shutdown = asyncio.Event()
        task = asyncio.create_task(follower._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
        try:
            # Windows measured in lost cycles (the loop's own emitted
            # observable), not wall-clock sleeps: the first window is the
            # settling, the second the steady state the runbook describes.
            await wait_for_condition(
                lambda: _failures() >= 4,
                description="the follower completed its settling window of lost cycles",
                timeout=10.0,
            )
            first_window_value = _contention_value()
            assert first_window_value > 0, (
                "test setup: the follower must have lost (and recorded the "
                "holder transition) before the steady-state window is measured"
            )

            await wait_for_condition(
                lambda: _failures() >= 8,
                description="the follower completed its steady-state window of lost cycles",
                timeout=10.0,
            )
            second_window_value = _contention_value()
        finally:
            shutdown.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert leader._deps.is_leader.is_set() is True, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "test invariant broken: the leader must remain elected for the "
            "whole measurement window for this to be a 'stable fleet', not "
            "a handover"
        )

        assert second_window_value == first_window_value, (
            "a stable fleet (leader never demoted) must not keep "
            "accumulating lock_contention once past the initial election "
            "settling — contention is recorded per distinct observed "
            "holder, so a follower that keeps losing to the same live "
            f"holder is the steady state, not a stuck one; the counter "
            f"grew from {first_window_value} to {second_window_value} "
            "across a second full window with no handover in progress"
        )
    finally:
        await ledger_leader.close_all()
        await ledger_follower.close_all()


# ── W10: demote, then the next worker wins the SAME schema ───────────────


async def test_demoted_leaders_lease_lapses_for_the_next_worker(
    module_pg_schema: ModulePgSchema,
) -> None:
    """A full promote/demote cycle on ONE schema: the demoted leader's row
    stops being renewed, so once the lease it wrote has lapsed a second
    worker takes the SAME schema over — a handover bounded by the lease the
    demoted pod itself chose, never by anything holding a connection open.
    A short leader_lease brings that horizon inside the test."""
    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name

    ledger_a = _FactoryLedger(pg_dsn)
    worker_a = new_uuid()
    await _register_worker(pg_dsn, schema, worker_a)
    leader_a = _election_leader(pg_dsn, schema, ledger_a, worker_a, leader_lease=1.0)
    ledger_b = _FactoryLedger(pg_dsn)
    worker_b = new_uuid()
    await _register_worker(pg_dsn, schema, worker_b)
    leader_b = _election_leader(pg_dsn, schema, ledger_b, worker_b, leader_lease=1.0)

    try:
        await _await_election(leader_a)
        assert leader_a._deps.is_leader.is_set() is True  # pyright: ignore[reportPrivateUsage]  # Why: the deps the leader was constructed with.

        # The demotion path: drop the leader conn (closing it releases the
        # courtesy lock with the session), close the leader-owned dedicated
        # conns. The row still names the demoted pod; with nothing renewing
        # it, it lapses on its own expiry.
        await leader_a._drop_leader_conn(reason="test_demote")  # pyright: ignore[reportPrivateUsage]  # Why: driving the demotion path directly is the point of the test.
        await leader_a._close_leader_owned_conns()  # pyright: ignore[reportPrivateUsage]  # Why: see above.
        assert leader_a._deps.is_leader.is_set() is False  # pyright: ignore[reportPrivateUsage]  # Why: see above.

        await _await_election(leader_b)
        assert leader_b._deps.is_leader.is_set() is True, (  # pyright: ignore[reportPrivateUsage]  # Why: see above.
            "once the demoted leader's lease lapses, the next worker must "
            "win the same schema's election"
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


# ── W5: a holder from a release that does not lease is not displaced ─────


async def test_a_pinging_holder_without_a_lease_is_not_taken_over(
    module_pg_schema: ModulePgSchema,
) -> None:
    """A holder whose release predates the lease keeps the role while it pings.

    During a roll, a pod from the previous release takes the row with an
    upsert that names four columns and leaves whatever ``expires_at`` it
    found in place -- the previous holder's, already lapsed or about to be.
    Judging such a row on the expiry alone declares a pod that is pinging
    every heartbeat to be dead, and two processes run the maintenance loops
    at once. The row is takeable only once BOTH signals have stopped, so a
    live pinger keeps the role and a silent one still loses it.
    """
    pg_dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name
    leasing, pre_lease, taker = new_uuid(), new_uuid(), new_uuid()
    for worker_id in (leasing, pre_lease, taker):
        await _register_worker(pg_dsn, schema, worker_id)

    elect_sql, _renew_sql, _resign_sql = build_leader_lease_sql(schema)
    lease_secs = 1.0
    slack_secs = 1.0

    conn = await asyncpg.connect(pg_dsn)
    ping_conn = await asyncpg.connect(pg_dsn)
    try:
        # A leasing pod holds the role, stamping an expiry on the row.
        elected = await conn.fetchval(elect_sql, leasing, lease_secs, slack_secs)
        assert elected is not None, "the unclaimed role must be winnable"

        # A pod from the previous release takes over with its four-column
        # upsert, which leaves the leasing pod's expiry behind.
        await conn.execute(
            f'INSERT INTO "{schema}".maintenance_leader '  # noqa: S608  # Why: schema is the module fixture's validated identifier; no user input.
            "(singleton, worker_id, elected_at, last_seen_at) "
            "VALUES (true, $1, clock_timestamp(), clock_timestamp()) "
            "ON CONFLICT (singleton) DO UPDATE SET worker_id = EXCLUDED.worker_id, "
            "elected_at = EXCLUDED.elected_at, last_seen_at = EXCLUDED.last_seen_at",
            pre_lease,
        )
        stale_expiry: datetime | None = await conn.fetchval(
            f'SELECT expires_at FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: see above.
        )
        assert stale_expiry is not None, (
            "the previous release's upsert names four columns, so the expiry "
            "it inherits is exactly what makes this row ambiguous"
        )

        # Keep pinging as a live pod of that release does on every heartbeat,
        # until the server's own clock says the inherited expiry has lapsed.
        # (A second connection: asyncpg does not interleave queries on one.)
        ping_stop = asyncio.Event()

        async def _keep_pinging() -> None:
            while not ping_stop.is_set():
                await ping_conn.execute(
                    f'UPDATE "{schema}".maintenance_leader '  # noqa: S608  # Why: see above.
                    "SET last_seen_at = clock_timestamp() WHERE worker_id = $1",
                    pre_lease,
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(ping_stop.wait(), timeout=0.05)

        ping_task = asyncio.create_task(_keep_pinging())
        try:
            await wait_for_condition(
                lambda: conn.fetchval(
                    f'SELECT expires_at < clock_timestamp() FROM "{schema}".maintenance_leader',  # noqa: S608  # Why: see above.
                ),
                description="the inherited expiry must lapse while the holder keeps pinging",
                timeout=5 * lease_secs,
            )
        finally:
            ping_stop.set()
            await asyncio.wait_for(ping_task, timeout=5.0)

        took = await conn.fetchval(elect_sql, taker, lease_secs, slack_secs)
        assert took is None, (
            "a holder that is still pinging last_seen_at must keep the role — "
            "taking it here runs two leaders through the rest of the roll"
        )
        row = await conn.fetchrow(
            f'SELECT worker_id FROM "{schema}".maintenance_leader'  # noqa: S608  # Why: see above.
        )
        assert row is not None and row["worker_id"] == pre_lease

        # ...and once the ping stops, the role is recoverable on the slack,
        # so deferring above costs availability nothing. The server is the
        # arbiter of "stopped", so the wait polls its clock, not a sleep.
        await wait_for_condition(
            lambda: conn.fetchval(
                f"SELECT last_seen_at < clock_timestamp() - make_interval(secs => $1) "  # noqa: S608  # Why: see above.
                f'FROM "{schema}".maintenance_leader WHERE singleton = true',
                slack_secs,
            ),
            description="the holder's ping must go stale on the pre-lease slack",
            timeout=5 * slack_secs,
        )
        took = await conn.fetchval(elect_sql, taker, lease_secs, slack_secs)
        assert took is not None, "a holder that has stopped pinging must lose the role on the slack"
    finally:
        await conn.close()
        await ping_conn.close()

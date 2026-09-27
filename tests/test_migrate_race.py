# ruff: noqa: S608  # Why: schema is a fixed test identifier, every value is $-bound.
"""The migration concurrency campaign: migrator-vs-migrator and
migrator-vs-traffic races against the REAL ``taskq migrate`` subprocess.

``tests/test_timescaledb_hypertables.py`` pins the LOCK TABLE EXCLUSIVE
serialization for ONE concurrent writer; the migration advisory lock
(:func:`taskq.migrate.migration_advisory_lock`) claims to serialize the
deploy step. Nothing has PROVEN migrator-vs-migrator or
migrator-vs-traffic. This module attacks, every migration invocation a
subprocess (the deploy E2E's ``_invoke_migrate`` idiom — the real
entrypoint, env cascade, signal discipline and exit code, nothing of the
CLI imported into the test's process):

* two pods run ``migrate up`` simultaneously against one database —
  outcomes must be serialized-or-cleanly-refused: no half-applied ledger
  state, both pods agree on the final version, the loser either no-ops
  (exit 0, "no pending migrations") or exits 1 with the honest
  lock-contention error;
* the storm shape: five pods at once;
* the hardening shape: the second migrator joins MID-first-run, gated on
  the ledger's migration-row state, not merely concurrently-launched;
* migrate during live traffic: claim/commit loops running while the
  remaining migration chain applies (real ACCESS EXCLUSIVE DDL over
  ``jobs``), and while the hypertable enable/disable lifecycle runs —
  every job outcome honest (the loop knows its commit landed or not),
  no crash-loop, conservation (succeeded + pending == seeded);
* SIGKILL one migrator mid-run: a second pod converges the ledger —
  plain Postgres, not only the hypertable machinery (the crash-convergence
  matrix of ``test_atk_upgrade_kill_mid_migration.py`` extended from
  statement boundaries to the whole-runner shape);
* there is no ``migrate down`` — forward-only by design (the module
  docstring of ``taskq/migrate.py``), so the rollback-shaped operator
  path is ``migrate disable-hypertables``; it is raced against
  ``migrate up`` on the SAME advisory lock under the same honesty
  contract.

Residue discipline: every test drops its own schema in ``finally``;
container fixtures are module-scoped and labeled with
:func:`creator_labels`.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import os
import sys
import uuid
from collections.abc import Iterator

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.migrate import (
    checksum_drifts,
    discover,
    list_applied,
    list_invalid_indexes,
    migration_lock_name,
)
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.pg import create_pending_job
from tests.test_timescaledb_hypertables import _TIMESCALE_IMAGE_DEFAULT

pytestmark = pytest.mark.integration

_TIMESCALE_IMAGE = os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or _TIMESCALE_IMAGE_DEFAULT

# ── Timeout arithmetic (derived, not vibes) ──────────────────────────────
# DEFAULT_MIGRATION_LOCK_TIMEOUT = 120s: each pod's bounded WAIT for the
# advisory lock. A fresh-schema apply of the bundled chain measured single
# digits of seconds on CI containers; _APPLY_MARGIN_PER_POD = 60s is an
# order of magnitude above it. Pods queue on the lock, so a pod's total
# lifetime is bounded by its own lock wait + its own apply:
#   per-pod budget = 120 + 60 = 180s; n pods, n x 180s.
# The gate waits (mid-run join, SIGKILL) get the same 180s the gated pod
# is entitled to. Traffic tests add the hypertable conversions of small
# seeded tables (the mid-life enable E2E converts 1000 rows in seconds):
# +60s flat.
_MIGRATION_LOCK_WAIT_SECS: float = 120.0
_APPLY_MARGIN_PER_POD_SECS: float = 60.0
_GATE_BUDGET_SECS: float = _MIGRATION_LOCK_WAIT_SECS + _APPLY_MARGIN_PER_POD_SECS
_TRAFFIC_EXTRA_SECS: float = 60.0


def _pod_budget(pods: int) -> float:
    return pods * (_MIGRATION_LOCK_WAIT_SECS + _APPLY_MARGIN_PER_POD_SECS)


# The honest lock-contention refusal the CLI prints on SystemExit
# (taskq.migrate.migration_advisory_lock's message).
_LOCK_CONTENTION_MARKER = "migration advisory lock"


@dataclasses.dataclass
class PodOutcome:
    """One subprocess migrator's exit: code and captured streams."""

    returncode: int
    stdout: str
    stderr: str


async def _spawn_migrate(
    dsn: str, schema: str, argv: list[str], *, flag: bool
) -> asyncio.subprocess.Process:
    """One REAL ``taskq migrate`` subprocess — the deploy E2E's env
    cascade, asyncio-flavored so pods can be launched simultaneously."""
    env = {
        **os.environ,
        "TASKQ_PG_DSN": dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_TIMESCALEDB_HYPERTABLES": "true" if flag else "false",
    }
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "taskq",
        *argv,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _collect(proc: asyncio.subprocess.Process, budget: float) -> PodOutcome:
    stdout, stderr = await asyncio.wait_for(proc.communicate(), budget)
    assert proc.returncode is not None
    return PodOutcome(returncode=proc.returncode, stdout=stdout.decode(), stderr=stderr.decode())


async def _run_migrate(
    dsn: str, schema: str, argv: list[str], *, flag: bool, budget: float
) -> PodOutcome:
    proc = await _spawn_migrate(dsn, schema, argv, flag=flag)
    return await _collect(proc, budget)


async def _run_pods_simultaneously(
    dsn: str, schema: str, argv: list[str], *, flag: bool, pods: int
) -> list[PodOutcome]:
    """Launch ``pods`` migrators at once and wait for all of them."""
    procs = [await _spawn_migrate(dsn, schema, argv, flag=flag) for _ in range(pods)]
    budget = _pod_budget(pods)
    return list(await asyncio.gather(*(_collect(p, budget) for p in procs)))


def _assert_honest(outcomes: list[PodOutcome], context: str) -> None:
    """Every pod either exits 0 (applied or no-op) or exits 1 with the
    honest lock-contention error. A crash, a traceback, or a bare exit is
    dishonest."""
    for i, outcome in enumerate(outcomes):
        honest = outcome.returncode == 0 or (
            outcome.returncode == 1 and _LOCK_CONTENTION_MARKER in outcome.stderr
        )
        assert honest, (
            f"{context}: pod {i} exited {outcome.returncode} "
            f"stdout={outcome.stdout!r} stderr={outcome.stderr!r}"
        )


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _assert_ledger_converged(conn: asyncpg.Connection, schema: str) -> None:
    """No half-applied ledger state: every bundled migration recorded,
    every checksum intact, no INVALID-index debris."""
    expected = {m.key for m in discover()}
    applied = await list_applied(conn, schema)
    assert applied == expected, f"ledger {applied - expected} missing, {expected - applied} extra"
    assert await checksum_drifts(conn, schema=schema) == {}, "ledger checksums drifted"
    assert await list_invalid_indexes(conn, schema) == [], "INVALID index debris in the schema"


async def _gate_on_ledger_rows(dsn: str, schema: str, *, minimum: int) -> int:
    """Poll until the ledger records at least ``minimum`` rows (the
    mid-run gate: the first migrator is provably INSIDE the chain), and
    return the count seen. Bounded by the same budget a queued pod gets."""
    conn = await asyncpg.connect(dsn)
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _GATE_BUDGET_SECS
        while True:
            try:
                n: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".schema_migrations')
            except asyncpg.UndefinedTableError:
                n = 0
            if n >= minimum:
                return n
            if loop.time() > deadline:
                pytest.fail(f"the first migrator never recorded {minimum} ledger row(s)")
            await asyncio.sleep(0.05)
    finally:
        await conn.close()


async def _gate_on_queued_join(conn: asyncpg.Connection, schema: str) -> None:
    """Fail-closed proof that the second pod's join landed MID-first-run.

    Spawning pod B after a ledger gate proves pod A was mid-chain at
    GATE-FIRE time, not that B's own ledger read landed inside A's apply
    window: B's interpreter boot (~0.8s) is on the same order as the
    whole-chain apply (~0.8s), so a broken-lock mutant can finish A before
    B arrives and the join degenerates to a sequential no-op — silently.

    So the gate demands a POSITIVE observation, sampled in one instant,
    that the serialization actually engaged: the ledger already carries
    rows (pod A provably inside the chain) AND at least one session is
    BLOCKED on the migration advisory lock (``pg_locks``: locktype
    'advisory', NOT granted, keyed on the migration lock key's two 32-bit
    halves — B queueing
    behind A). Every ``migrate up`` acquires the lock before reading the
    ledger, even a no-op one, so on correct code B must queue whenever it
    arrives before A releases; the gate fails closed if that instant is
    never observed, with a lock mutant (barge / early-release / private
    key) B never queues at all and the test reds.
    """
    name = migration_lock_name(schema)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _GATE_BUDGET_SECS
    while True:
        try:
            rows: int = await conn.fetchval(f'SELECT count(*) FROM "{schema}".schema_migrations')
        except asyncpg.UndefinedTableError:
            rows = 0
        if rows >= 1:
            # A single-bigint advisory key is stored in pg_locks split
            # across classid (low 32 bits) and objid (high 32 bits);
            # comparing objid against the full 64-bit hash is an OID
            # range error. Reconstruct both halves.
            waiters = await conn.fetchval(
                "SELECT count(*) FROM pg_locks "
                "WHERE locktype = 'advisory' AND NOT granted "
                "AND classid::bigint = ((hashtextextended($1, 0) >> 32) & 4294967295) "
                "AND objid::bigint = (hashtextextended($1, 0) & 4294967295)",
                name,
            )
            if waiters >= 1:
                return
        if loop.time() > deadline:
            pytest.fail(
                f"the join was never proven mid-run: no instant showed the "
                f"ledger mid-chain ({rows} row(s)) AND a session blocked on "
                f"the migration advisory lock — either the second pod "
                f"arrived after the first finished (overlap window missed) "
                f"or the advisory-lock serialization did not engage"
            )
        await asyncio.sleep(0.025)


async def _gate_on_advisory_lock_free(conn: asyncpg.Connection, schema: str) -> None:
    """Poll until the killed pod's session-level advisory lock is gone
    (SIGKILL closes the socket, the backend notices and releases)."""
    name = migration_lock_name(schema)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _GATE_BUDGET_SECS
    while True:
        got = await conn.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", name)
        if got:
            await conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", name)
            return
        if loop.time() > deadline:
            pytest.fail("the SIGKILLed migrator's advisory lock never released")
        await asyncio.sleep(0.1)


async def _assert_no_pending_via_third_pod(dsn: str, schema: str) -> PodOutcome:
    """The convergence proof on the operator's own path: a fresh migrator
    reports ``no pending migrations`` and exits 0."""
    outcome = await _run_migrate(dsn, schema, ["migrate", "up"], flag=False, budget=_pod_budget(1))
    assert outcome.returncode == 0, f"stderr: {outcome.stderr}"
    assert "no pending migrations" in outcome.stdout, f"stdout: {outcome.stdout}"
    return outcome


# ── Live traffic: the claim/commit loop ──────────────────────────────────

_SEED_JOBS = 300


@dataclasses.dataclass
class TrafficCounters:
    """The traffic loop's own ledger of outcomes. Honesty contract: every
    claim either COMMITTED (the loop knows) or raised (the loop knows it
    did not land). ``claimed_without_commit`` is derived, never asserted
    to zero — a failed claim leaves the row pending, which conservation
    counts."""

    committed: int = 0
    empty_polls: int = 0
    errors: collections.Counter[str] = dataclasses.field(default_factory=collections.Counter)


async def _traffic_loop(
    dsn: str, schema: str, stop: asyncio.Event, counters: TrafficCounters
) -> None:
    """Claim-and-commit jobs one transaction at a time, until ``stop``.

    The worker's own shape: claim under FOR UPDATE SKIP LOCKED, commit,
    KNOW the commit landed (the transaction returned) or did not (the
    exception escaped the transaction block). Any driver/database error
    is classified and counted, never swallowed blindly: an error class
    outside asyncpg/OSError/ConnectionError propagates and fails the test
    — that would be a crash-loop, not an honest outcome."""
    conn = await asyncpg.connect(dsn)
    worker_id = new_uuid()
    try:
        while not stop.is_set():
            try:
                claimed: uuid.UUID | None = None
                async with conn.transaction():
                    row = await conn.fetchrow(
                        f"""
                        SELECT id FROM "{schema}".jobs
                        WHERE status = 'pending'
                        ORDER BY priority DESC, scheduled_at, id
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    )
                    if row is not None:
                        claimed = row["id"]
                        await conn.execute(
                            f"""
                            UPDATE "{schema}".jobs
                            SET status = 'succeeded', started_at = clock_timestamp(),
                                finished_at = clock_timestamp(), locked_by_worker = $1
                            WHERE id = $2
                            """,
                            worker_id,
                            claimed,
                        )
                if claimed is None:
                    counters.empty_polls += 1
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=0.05)
                else:
                    counters.committed += 1
            except (asyncpg.PostgresError, ConnectionError, OSError) as exc:
                counters.errors[type(exc).__name__] += 1
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=0.05)
    finally:
        await conn.close()


async def _seed_jobs(dsn: str, schema: str, n: int) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        for _ in range(n):
            await create_pending_job(conn, schema)
    finally:
        await conn.close()


async def _assert_traffic_conservation(
    conn: asyncpg.Connection, schema: str, counters: list[TrafficCounters]
) -> None:
    """Every job outcome honest: the loop's own commit ledger and the
    database agree, nothing vanished, nothing half-applied."""
    rows = await conn.fetch(f'SELECT status, count(*) AS n FROM "{schema}".jobs GROUP BY status')
    by_status = {r["status"]: r["n"] for r in rows}
    assert set(by_status) <= {"succeeded", "pending"}, (
        f"illegal intermediate states survived: {by_status} - a job whose "
        "commit did not land must read pending again"
    )
    assert sum(by_status.values()) == _SEED_JOBS, "jobs were created or lost"
    total_committed = sum(c.committed for c in counters)
    assert by_status.get("succeeded", 0) == total_committed, (
        f"the loops' commit ledger ({total_committed}) and the database "
        f"({by_status.get('succeeded', 0)}) disagree"
    )
    assert by_status.get("pending", 0) > 0 or total_committed == _SEED_JOBS
    for c in counters:
        assert sum(c.errors.values()) < _SEED_JOBS, (
            f"the traffic loop error-classified its whole run: {dict(c.errors)} - "
            "that is a crash-loop, not an honest degraded state"
        )


async def _assert_traffic_still_flows(
    dsn: str, schema: str, *, pending: int, settle_budget: float
) -> None:
    """After the migration lifecycle settles, the schema still serves
    claims: whatever is pending can actually be committed."""
    if pending == 0:
        return
    counters = TrafficCounters()
    stop = asyncio.Event()
    loop_task = asyncio.create_task(_traffic_loop(dsn, schema, stop, counters))
    try:
        await asyncio.wait_for(asyncio.sleep(2.0), timeout=settle_budget)
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(loop_task, timeout=10.0)
    conn = await asyncpg.connect(dsn)
    try:
        left = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"
        )
        assert left == 0, f"the settled schema must drain its {left} remaining job(s)"
    finally:
        await conn.close()


# ── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def race_schema() -> str:
    return "tsrace_" + new_uuid().hex[:12]


@pytest.fixture(scope="module")
def race_ts_dsn() -> Iterator[str]:
    """One timescaledb container per module (the hypertable races); skips
    without Docker."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


async def _traffic_race(
    dsn: str, schema: str, loops: int
) -> tuple[list[TrafficCounters], asyncio.Event, list[asyncio.Task[None]]]:
    stop = asyncio.Event()
    counters = [TrafficCounters() for _ in range(loops)]
    tasks = [
        asyncio.create_task(_traffic_loop(dsn, schema, stop, c), name=f"traffic-{i}")
        for i, c in enumerate(counters)
    ]
    return counters, stop, tasks


# ── 1. Two pods, one database ────────────────────────────────────────────


@pytest.mark.timeout(360)
async def test_two_pods_migrate_up_concurrently(pg_dsn: str, race_schema: str) -> None:
    """Two pods run ``migrate up`` at the same instant against a virgin
    schema: serialized-or-cleanly-refused, both agree on the final ledger,
    and a third pod proves convergence on the operator's own path."""
    schema = race_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        outcomes = await _run_pods_simultaneously(
            pg_dsn, schema, ["migrate", "up"], flag=False, pods=2
        )
        _assert_honest(outcomes, "two-pod race")
        applied_counts = [o.stdout.count(".sql") for o in outcomes]
        assert sorted(applied_counts) == [0, len(discover())], (
            f"exactly one pod may apply the chain; got {applied_counts}: "
            f"{[o.stdout for o in outcomes]}"
        )
        await _assert_ledger_converged(conn, schema)
        await _assert_no_pending_via_third_pod(pg_dsn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 2. The five-pod storm ────────────────────────────────────────────────


@pytest.mark.load_sensitive
@pytest.mark.timeout(5 * 180)
async def test_five_pods_migrate_up_storm(pg_dsn: str, race_schema: str) -> None:
    """Five pods launch at once: the chain applies exactly once, every
    pod exits honestly, the ledger converges."""
    schema = race_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        outcomes = await _run_pods_simultaneously(
            pg_dsn, schema, ["migrate", "up"], flag=False, pods=5
        )
        _assert_honest(outcomes, "five-pod storm")
        applied_counts = [o.stdout.count(".sql") for o in outcomes]
        assert sum(1 for n in applied_counts if n > 0) == 1, (
            f"exactly one pod may apply the chain; got {applied_counts}"
        )
        assert max(applied_counts) == len(discover())
        await _assert_ledger_converged(conn, schema)
        await _assert_no_pending_via_third_pod(pg_dsn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 3. The mid-run join (hardening) ─────────────────────────────────────


@pytest.mark.load_sensitive
@pytest.mark.timeout(2 * 180 + 180)
async def test_second_migrator_joins_mid_first_run(pg_dsn: str, race_schema: str) -> None:
    """Not concurrently-launched-and-hoped: the join must be PROVEN mid-run.
    Pod B launches alongside pod A and ``_gate_on_queued_join`` then demands
    one sampled instant showing the ledger mid-chain AND B blocked behind
    the advisory lock — B provably queued on the lock while the chain
    applied, never apply-over-A. Spawn order does not buy the lock: either
    pod may win, so the outcome contract is "exactly one applied the whole
    chain, the other no-oped", not a name. Both finish the story honestly."""
    schema = race_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        pod_a = await _spawn_migrate(pg_dsn, schema, ["migrate", "up"], flag=False)
        pod_b = await _spawn_migrate(pg_dsn, schema, ["migrate", "up"], flag=False)
        try:
            await _gate_on_queued_join(conn, schema)
            budget = _pod_budget(2)
            outcome_a, outcome_b = await asyncio.gather(
                _collect(pod_a, budget), _collect(pod_b, budget)
            )
        except BaseException:
            # The gate failed (or the collect did) with both pods possibly
            # still alive: reap them BEFORE the finally's DROP SCHEMA, which
            # otherwise deadlocks against a live migrator's schema locks.
            for pod in (pod_a, pod_b):
                pod.kill()
            await asyncio.gather(pod_a.wait(), pod_b.wait(), return_exceptions=True)
            raise
        _assert_honest([outcome_a, outcome_b], "mid-run join")
        applied_counts = [outcome_a.stdout.count(".sql"), outcome_b.stdout.count(".sql")]
        assert sorted(applied_counts) == [0, len(discover())], (
            f"exactly one pod may apply the chain; got {applied_counts}: "
            f"{[o.stdout for o in (outcome_a, outcome_b)]}"
        )
        await _assert_ledger_converged(conn, schema)
        await _assert_no_pending_via_third_pod(pg_dsn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 4. SIGKILL mid-run, second pod converges ────────────────────────────


@pytest.mark.timeout(3 * 180)
async def test_sigkilled_migrator_second_pod_converges(pg_dsn: str, race_schema: str) -> None:
    """SIGKILL pod A once it is provably inside the chain; pod B must be
    able to converge the ledger (plain PG, no hypertables involved)."""
    schema = race_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        pod_a = await _spawn_migrate(pg_dsn, schema, ["migrate", "up"], flag=False)
        await _gate_on_ledger_rows(pg_dsn, schema, minimum=1)
        pod_a.kill()
        await pod_a.wait()
        await _gate_on_advisory_lock_free(conn, schema)

        outcome_b = await _run_migrate(
            pg_dsn, schema, ["migrate", "up"], flag=False, budget=_pod_budget(2)
        )
        assert outcome_b.returncode == 0, (
            f"the second pod must converge the ledger; stderr: {outcome_b.stderr}"
        )
        await _assert_ledger_converged(conn, schema)
        await _assert_no_pending_via_third_pod(pg_dsn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 5. migrate up during live traffic (plain PG) ─────────────────────────


@pytest.mark.load_sensitive
@pytest.mark.timeout(2 * 180 + _TRAFFIC_EXTRA_SECS + 120)
async def test_migrate_up_during_live_traffic(pg_dsn: str, race_schema: str) -> None:
    """Workers claim/commit jobs while the REMAINING chain applies —
    real ACCESS EXCLUSIVE DDL over ``jobs`` (claim-epoch ALTER, reclaim
    indexes) queuing against the traffic loop's row locks. Every job
    outcome honest, no crash-loop, conservation holds, and the settled
    schema still drains."""
    schema = race_schema
    conn = await asyncpg.connect(pg_dsn)
    prefix_target = "01.00.16_01"
    try:
        await _drop_schema(conn, schema)
        first = await _run_migrate(
            pg_dsn,
            schema,
            ["migrate", "up", "--target", prefix_target],
            flag=False,
            budget=_pod_budget(1),
        )
        assert first.returncode == 0, f"stderr: {first.stderr}"
        await _seed_jobs(pg_dsn, schema, _SEED_JOBS)

        counters, stop, tasks = await _traffic_race(pg_dsn, schema, loops=2)
        try:
            outcome = await _run_migrate(
                pg_dsn, schema, ["migrate", "up"], flag=False, budget=_pod_budget(1)
            )
            assert outcome.returncode == 0, (
                f"the racing migrate must still succeed: {outcome.stderr}"
            )
        finally:
            stop.set()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                    await asyncio.wait_for(task, timeout=30.0)

        await _assert_ledger_converged(conn, schema)
        await _assert_traffic_conservation(conn, schema, counters)
        pending = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"
        )
        await _assert_traffic_still_flows(pg_dsn, schema, pending=pending, settle_budget=120.0)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 6. Two pods with the hypertable flag on ─────────────────────────────


@pytest.mark.timeout(360)
async def test_two_pods_flag_on_race_ends_converted(race_ts_dsn: str, race_schema: str) -> None:
    """The deploy shape with the flag on: both pods run ``migrate up``
    against a virgin timescale schema; the chain applies once, the
    conversion runs once (or converges twice), the final shape is the
    converted one."""
    schema = race_schema
    dsn = race_ts_dsn
    conn = await asyncpg.connect(dsn)
    try:
        await _drop_schema(conn, schema)
        outcomes = await _run_pods_simultaneously(dsn, schema, ["migrate", "up"], flag=True, pods=2)
        _assert_honest(outcomes, "two-pod flag-on race")
        applied_counts = [o.stdout.count(".sql") for o in outcomes]
        assert sorted(applied_counts) == [0, len(discover())], (
            f"exactly one pod may apply the chain; got {applied_counts}"
        )
        rows = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema = $1",
            schema,
        )
        assert {r["hypertable_name"] for r in rows} == {
            "job_events",
            "jobs_archive",
            "job_attempts_archive",
        }
        await _assert_ledger_converged(conn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 7. The rollback-shaped path: disable vs up on one lock ──────────────


@pytest.mark.timeout(2 * 180 + 180)
async def test_disable_hypertables_races_migrate_up(race_ts_dsn: str, race_schema: str) -> None:
    """There is no ``migrate down`` (forward-only by design), so the
    rollback-shaped operator path is ``migrate disable-hypertables``. It
    and ``migrate up`` claim the SAME advisory lock: run them
    concurrently — both must exit honestly and the final state must be
    internally consistent (whichever ran last owns the shape; nothing
    half-converted)."""
    schema = race_schema
    dsn = race_ts_dsn
    conn = await asyncpg.connect(dsn)
    try:
        await _drop_schema(conn, schema)
        deploy = await _run_migrate(
            dsn, schema, ["migrate", "up"], flag=True, budget=_pod_budget(1)
        )
        assert deploy.returncode == 0, f"stderr: {deploy.stderr}"

        up_proc = await _spawn_migrate(dsn, schema, ["migrate", "up"], flag=True)
        down_proc = await _spawn_migrate(
            dsn, schema, ["migrate", "disable-hypertables"], flag=False
        )
        up_outcome, down_outcome = await asyncio.gather(
            _collect(up_proc, _pod_budget(2)), _collect(down_proc, _pod_budget(2))
        )
        _assert_honest([up_outcome, down_outcome], "up vs disable race")

        rows = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema = $1",
            schema,
        )
        hypertables = {r["hypertable_name"] for r in rows}
        if hypertables:
            assert hypertables == {
                "job_events",
                "jobs_archive",
                "job_attempts_archive",
            }, f"half-converted shape: {hypertables}"
            policies = await conn.fetchval(
                "SELECT count(*) FROM timescaledb_information.jobs "
                "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
                schema,
            )
            assert policies == 3, f"converted shape must carry its policies: {policies}"
        else:
            pkey = await conn.fetchval(
                "SELECT count(*) FROM pg_constraint con "
                "JOIN pg_class cl ON cl.oid = con.conrelid "
                "JOIN pg_namespace n ON n.oid = con.connamespace "
                "WHERE n.nspname = $1 AND cl.relname = 'jobs_archive' AND con.contype = 'p'",
                schema,
            )
            assert pkey == 1, "the vanilla shape must be whole after the disable"
        await _assert_ledger_converged(conn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 8. The hypertable lifecycle during live traffic ─────────────────────


@pytest.mark.load_sensitive
@pytest.mark.timeout(2 * 180 + 2 * _TRAFFIC_EXTRA_SECS + 120)
async def test_hypertable_lifecycle_during_live_traffic(race_ts_dsn: str, race_schema: str) -> None:
    """Traffic claim/commit loops run through the whole hypertable
    lifecycle: the mid-life enable (``migrate_data`` rewrites the
    populated tables under ACCESS EXCLUSIVE) and the disable. Every job
    outcome honest, no crash-loop, conservation holds across both
    conversions."""
    schema = race_schema
    dsn = race_ts_dsn
    conn = await asyncpg.connect(dsn)
    try:
        await _drop_schema(conn, schema)
        deploy = await _run_migrate(
            dsn, schema, ["migrate", "up"], flag=False, budget=_pod_budget(1)
        )
        assert deploy.returncode == 0, f"stderr: {deploy.stderr}"
        await _seed_jobs(dsn, schema, _SEED_JOBS)
        # The enable converts job_events via its FK target's live rows;
        # the archive family needs terminal rows to rewrite.
        for _i in range(50):
            await conn.fetchval(
                f'INSERT INTO "{schema}".jobs_archive '
                f"(id, actor, queue, payload, status, attempt, max_attempts, retry_kind, "
                f"expire_at, finished_at) VALUES ($1, 'a', 'q', '{{}}'::jsonb, 'succeeded', "
                f"0, 3, 'transient', clock_timestamp() + interval '365 days', clock_timestamp()) "
                f"RETURNING id",
                new_uuid(),
            )

        counters, stop, tasks = await _traffic_race(dsn, schema, loops=2)
        try:
            enable = await _run_migrate(
                dsn, schema, ["migrate", "up"], flag=True, budget=_pod_budget(1)
            )
            assert enable.returncode == 0, f"the enable must succeed under traffic: {enable.stderr}"
            disable = await _run_migrate(
                dsn,
                schema,
                ["migrate", "disable-hypertables"],
                flag=False,
                budget=_pod_budget(1),
            )
            assert disable.returncode == 0, (
                f"the disable must succeed under traffic: {disable.stderr}"
            )
        finally:
            stop.set()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                    await asyncio.wait_for(task, timeout=30.0)

        await _assert_traffic_conservation(conn, schema, counters)
        pending = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"
        )
        await _assert_traffic_still_flows(dsn, schema, pending=pending, settle_budget=120.0)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()

"""Partition weather I: BLACKHOLE (packets accepted, never delivered) on worker↔PG.

The weather engine is a real toxiproxy between the worker subprocess and
the shared Postgres container (see :mod:`tests.system_e2e._toxiproxy`):
the worker's packets are accepted by the OS and never delivered, the TCP
connections stay ESTABLISHED, and the cut applies to connections opened
before it - the fault shape no container kill or ``CLIENT PAUSE`` stub
can produce, and the shape the heartbeat's disown-after-commit and
isolate classification (``src/taskq/worker/heartbeat.py``) were designed
for. These tests prove those contracts hold on the actual wire.

Cell 1 (heartbeat): a worker whose PG link blackholes mid-job must
self-isolate - count (F+1) consecutive failed ticks, give the isolate's
own bounded connect a chance, walk away, and leave the row for the
leader's sweep - while a healthy sibling re-runs the job exactly once.

Cell 2 (claim + commit): a fleet whose ONLY PG path is the proxy takes
weather aimed at the dispatcher's windows - claim and terminal-commit -
and the conservation counter must balance: no lost job, no resurrected
job, every body run backed by a claim, every claim that "committed but
never heard back" reconciled or honestly re-run under a fresh attempt.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import time
from typing import TYPE_CHECKING

import pytest

from taskq.worker._watchdog import EXIT_WATCHDOG
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e._toxiproxy import dsn_host_port, proxied_dsn
from tests.system_e2e.actors import SysPayload, sys_fast, sys_slow

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema
    from tests.system_e2e._toxiproxy import Toxiproxy

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-partition-pg"


#: The heartbeat cascade the isolate contract is held against, DERIVED
#: from the worker env below, never asserted from hope:
#:   worst_beat_gap = heartbeat_interval + heartbeat_command_timeout
#:                  = 0.5 + 0.25 = 0.75s
#:   isolate lands on the (F+1)-th consecutive failure, F = 2, and the
#:   documented floor (heartbeat._lease_renewal_threshold) is
#:   tail + (F+1) * worst_beat_gap = max(0.5, 0.25) + 3 * 0.75 = 2.75s
#: from the cut's start (the last good beat may sit a full interval back).
_CASCADE_S = max(0.5, 0.25) + (2 + 1) * (0.5 + 0.25)

#: The isolate's OWN walk-away, measured in a standalone probe: the
#: cascade (2.75s) fires isolate, the isolate's fresh connect gives up
#: at 5s, the shutdown closes every dedicated connection at its 5s close
#: bound under a full cut (~8 closers = 40s), and the SHUTDOWN WATCHDOG
#: (on by production default, ``watchdog_enabled=True``) force-exits the
#: wedged teardown at the 15s termination grace - measured exit 17s.
#: Mutation-measured (this suite's own teeth): the exit is a RACE between
#: two armed detectors - stale-loop-tick (the leader/sweep sibling loops
#: starve first, ~5s) can preempt the 15s deadline trip - and with every
#: trip neutered the bounded closers still finished the teardown inside
#: ~35s. The test pins the FAMILY (a trip fired, rc EXIT_WATCHDOG), never
#: which detector won; with the watchdog OFF (the harness's ``_BASE_ENV``
#: shape) the release hold switches to ``lock_lease`` and the exit loses
#: its guaranteed bound - the finding is reported, the pinned contract is
#: the production shape. Budget: cascade + grace + dump slack -> 40.
_ISOLATE_EXIT_BUDGET_S = 40.0

#: The reclaim pipeline that must hand the job back WITHOUT the cut
#: worker's help. The MEASURED shape (standalone probe, identical
#: topology): A's watchdog-bounded exit ~17s, then B's leadership
#: takeover (the leader advisory lock only frees with A's session; the
#: election cadence plus first sweep tick), then lease expiry (8s from
#: A's last renewal, already past) + sweep tick (1s) + reclaim delay
#: (retry curve base 1s) + claim (poll 0.05) + body (3s) - observed
#: reclaim-to-success at cut+48s. Budget: exit bound 40 + takeover 10 +
#: sweep+delay 3 + claim+body 5 + slack -> 70.
_RERUN_BUDGET_S = 70.0


@pytest.mark.timeout(240)
@pytest.mark.load_sensitive
async def test_blackhole_mid_heartbeat_isolates_worker_and_job_reruns_exactly_once(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    toxiproxy: Toxiproxy,
    tmp_path: pathlib.Path,
) -> None:
    """BLACKHOLE on worker↔PG during claim + body + heartbeat: the worker
    classifies itself (HeartbeatLost isolate), exits, and the job is
    re-run EXACTLY once by a sibling that never took the weather."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    # Worker A is the claimer (spawned alone, so the job cannot land on a
    # sibling) and takes the weather; the sibling spawned after the cut
    # is clean by construction. A's heartbeat knobs shrink the cascade
    # the isolate contract sizes (see the arithmetic above): interval
    # 0.5 (harness default), command timeout 0.25, F=2.
    proxy_a = toxiproxy.create_proxy_sync("pg_heartbeat_a", *dsn_host_port(pg_dsn))
    # The log sink: a partitioned worker logs every failed tick - a PIPE
    # nobody drains fills and BLOCKS the worker's logging write, a wedge
    # the harness would manufacture. The file carries the isolate's own
    # failure record for the diagnostics below.
    log_a = tmp_path / "part-a.log"
    worker_a = spawn_worker(
        pg_dsn,
        schema,
        tag="part-a",
        extra_env={
            "TASKQ_PG_DSN": proxied_dsn(pg_dsn, proxy_a),
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.25",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "2",
            # The production watchdog shape: the shipped default is ON;
            # the harness's _BASE_ENV disables it. Under a persistent cut
            # the isolate fires and the teardown's own closers are
            # unfinishable - the watchdog's grace force-exit is what
            # bounds the exit, the contract this cell pins.
            "TASKQ_WATCHDOG_ENABLED": "true",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "5.0",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "2.5",
        },
        log_sink=str(log_a),
    )
    worker_b: WorkerProc | None = None
    try:
        wait_worker_ready(worker_a)

        handle = await sys_client.enqueue(sys_slow, SysPayload(sleep=3.0), tags=[_TAG])
        job_id = handle.job_id

        # The cut lands DURING the body: the claim (and the attempt it
        # charged) is already durable, the heartbeats that must keep the
        # lease alive now starve. Deadline: the claim is visible within
        # poll+sweep slack; failure here means the worker never claimed
        # at all - a harness fault, not a partition result.
        deadline = time.monotonic() + 15.0
        claimed_at: float | None = None
        last_row: asyncpg.Record | None = None
        while time.monotonic() < deadline:
            last_row = await conn.fetchrow(
                f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            if last_row is not None and last_row["status"] == "running":
                claimed_at = time.monotonic()
                break
            await asyncio.sleep(0.05)
        assert claimed_at is not None, (
            f"worker A never claimed the job within 15s (poll 0.05 + slack): {last_row}"
        )

        await proxy_a.blackhole()
        cut_at = time.monotonic()

        # The healthy sibling, spawned under the cut: it must be the
        # recovery path, and its health is what makes the exactly-once
        # assertion below mean something (a re-run needs a live claimant).
        log_b = tmp_path / "part-b.log"
        worker_b = spawn_worker(pg_dsn, schema, tag="part-b", log_sink=str(log_b))

        # ── The sibling-health proof (the per-worker scoping's tooth) ──
        # The probe is enqueued NOW, mid-cut, before B's readiness gate:
        # A's PG link is blackholed (it cannot claim), so the probe can
        # only complete on B - B's bootstrap, claim, body and commit all
        # ride the DIRECT wire while the cut is live. Success here is the
        # CONCURRENT evidence the per-worker scoping stands on: the
        # sibling was healthy DURING the cut, not merely by the time the
        # re-run settled. (A re-run asserted only after A's exit would
        # prove nothing about the cut's window.)
        probe = await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG])
        probe_deadline = time.monotonic() + 20.0
        probe_row: asyncpg.Record | None = None
        while time.monotonic() < probe_deadline:
            probe_row = await conn.fetchrow(
                f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
                probe.job_id,
            )
            if probe_row is not None and probe_row["status"] == "succeeded":
                break
            await asyncio.sleep(0.05)
        assert probe_row is not None and probe_row["status"] == "succeeded", (
            f"the healthy sibling did not complete a probe job during the cut "
            f"(bootstrap + claim + body + commit, poll 0.05): {probe_row}"
        )
        assert worker_a.proc.poll() is None, (
            "the sibling's probe completed only after the partitioned worker had "
            "already exited - the sibling's health was not proven CONCURRENTLY "
            "with the cut"
        )
        wait_worker_ready(worker_b)

        # The isolate cascade (2.75s) + the isolate connect's own 5s
        # give-up + the termination grace (15s): A must EXIT, not linger
        # as a zombie holding in-memory state.
        exit_deadline = cut_at + _ISOLATE_EXIT_BUDGET_S
        while time.monotonic() < exit_deadline and worker_a.proc.poll() is None:  # noqa: ASYNC110  # Why: the wait is a deadline OR the process's death - an Event cannot express either arm.
            await asyncio.sleep(0.2)
        assert worker_a.proc.poll() is not None, (
            f"the partitioned worker did not self-isolate and exit within "
            f"{_ISOLATE_EXIT_BUDGET_S}s (cascade {_CASCADE_S}s + connect 5s + grace 15s)"
        )

        # The worker's OWN evidence, from its log sink: the failed ticks
        # the isolate ledger counted (F=2 - the isolate fires on the 3rd
        # consecutive failure) and the force-exit that bounded the exit.
        # A worker that exits for any OTHER reason (a crash, a supervisor
        # kill, a clean return) without walking the cascade is not the
        # contract here.
        log_text = log_a.read_text(errors="replace")
        tick_failures = log_text.count("heartbeat-tick-failure") + log_text.count(
            "heartbeat-tick-unexpected-error"
        )
        assert tick_failures >= 3, (
            f"the partitioned worker's log records {tick_failures} failed ticks - the "
            f"(F+1)-th consecutive failure the isolate decision needs (F=2) left no "
            f"trace in the worker's own log"
        )
        # The production-shape force-exit: the watchdog is armed (the
        # shipped default, re-armed explicitly in this cell's env) and the
        # wedged teardown is force-exited - WHICH detector wins the race
        # (shutdown-deadline at 15s vs stale-loop-tick, the leader/sweep
        # sibling loops starving first) is deliberately NOT pinned: both
        # are the armed watchdog's redundant teeth on the same contract.
        # What IS pinned: a trip fired, and the exit code is the watchdog's.
        assert "worker-watchdog-trip" in log_text, (
            "the partitioned worker's log never recorded a watchdog trip - the exit "
            "did not come from the armed watchdog"
        )
        assert worker_a.proc.returncode == EXIT_WATCHDOG, (
            f"the partitioned worker exited rc={worker_a.proc.returncode}, not the "
            f"watchdog force-exit ({EXIT_WATCHDOG}): the armed watchdog did not "
            f"bound this exit"
        )

        # The re-run: lease expiry (8s) + sweep (1s) + reclaim delay (1s)
        # + claim + body (3s) - the job must reach succeeded on attempt 2
        # with exactly ONE body run recorded (attempt 1 was interrupted
        # before its effect; the effects ledger is the evidence).
        settle_deadline = cut_at + _RERUN_BUDGET_S
        final: asyncpg.Record | None = None
        while time.monotonic() < settle_deadline:
            final = await conn.fetchrow(
                f"SELECT status::text AS status, attempt, "
                f'error_class FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            if final is not None and final["status"] == "succeeded":
                break
            await asyncio.sleep(0.2)
        if final is None or final["status"] != "succeeded":
            diag = await conn.fetchrow(
                f"SELECT status::text, attempt, locked_by_worker, lock_expires_at, "
                f'scheduled_at, started_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            workers = await conn.fetch(
                f'SELECT id::text, last_seen_at FROM "{schema}".workers ORDER BY last_seen_at DESC'
            )
            leader_row = await conn.fetchrow(
                f'SELECT worker_id::text FROM "{schema}".maintenance_leader'
            )
            b_alive = worker_b.proc.poll() is None
            b_tail = log_b.read_text(errors="replace")[-3000:] if log_b.exists() else "<no log>"
            pytest.fail(
                f"the partitioned job was not re-run to success within {_RERUN_BUDGET_S}s "
                f"of the cut (A's exit ~17s, B's takeover, lease expiry + sweep + reclaim "
                f"delay + claim + body, measured 48s in the probe): {final}; "
                f"row: {diag}; workers: {workers}; leader: {leader_row}; worker_b alive: {b_alive}; "
                f"worker B log tail: {b_tail!r}"
            )

        effects = await conn.fetch(
            f'SELECT attempt, kind FROM "{schema}".sys_effects '
            "WHERE job_id = $1 AND actor = 'sys_slow' ORDER BY attempt",
            job_id,
        )
        assert [(r["attempt"], r["kind"]) for r in effects] == [(2, "done")], (
            f"the re-run is not exactly-once (interrupted attempt must record nothing, "
            f"the re-run one 'done' row at attempt 2): {effects}"
        )
    finally:
        reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        with contextlib.suppress(Exception):
            await proxy_a.clear()
        await delete_tagged(conn, schema, _TAG)


@pytest.mark.timeout(240)
async def test_blackhole_windows_across_claims_and_commits_conserve(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    toxiproxy: Toxiproxy,
) -> None:
    """BLACKHOLE + hold-then-close windows across the WHOLE claim/commit
    surface: a fleet whose only PG path is the proxy runs through
    alternating cut windows aimed at the dispatcher's commit moments -
    a claim whose commit landed but whose reply died with the connection
    is the one producer-side loss the claim-loss reconcile exists for.

    System invariant: the conservation counter balances - no lost job,
    no resurrected job, no limbo row - and the effects ledger reconciles
    per attempt (a re-run after an unknown-commit is a NEW attempt, never
    a second run of the same one). The workers are held OUT of the
    isolate cascade on purpose (F=40): this cell attacks the dispatcher's
    windows, the heartbeat cascade has its own cell above.
    """
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    proxy = toxiproxy.create_proxy_sync("pg_soak", *dsn_host_port(pg_dsn))
    workers = [
        spawn_worker(
            pg_dsn,
            schema,
            tag=f"part-soak-{i}",
            extra_env={
                "TASKQ_PG_DSN": proxied_dsn(pg_dsn, proxy),
                "TASKQ_MAX_HEARTBEAT_FAILURES": "40",
                # The lease validator requires the lease to cover the
                # F=40 cascade (0.5 + 41 * (0.5 + 0.5) = 41.5s): the soak
                # workers must ride OUT the weather windows, never
                # isolate, so the lease carries the cascade they are
                # granted.
                "TASKQ_LOCK_LEASE": "45.0",
            },
        )
        for i in range(3)
    ]
    try:
        for worker in workers:
            wait_worker_ready(worker)

        # Enqueue BEFORE the weather, INTO it, and AFTER it: the cut must
        # not distinguish where a job's lifecycle began.
        before = [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG]) for _ in range(4)
        ]
        handles = list(before)

        # The weather: ~6s of alternating 0.4s full partitions (silent
        # blackhole) and 0.8s windows where in-flight commands complete
        # and new ones connect; then a hold-then-close pass whose finite
        # hold turns every silent hang into an honest connection error.
        for _cycle in range(4):
            await proxy.blackhole()
            await asyncio.sleep(0.4)
            await proxy.clear()
            await asyncio.sleep(0.8)
            handles.extend(
                [
                    await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG])
                    for _ in range(2)
                ]
            )
        await proxy.hold_then_close(400)
        await asyncio.sleep(0.5)
        handles.extend(
            [
                await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG])
                for _ in range(2)
            ]
        )
        await proxy.clear()

        # The last 4 jobs, enqueued into the recovery: the fleet must take
        # them on refilled pools WITHOUT any restart.
        handles.extend(
            [
                await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG])
                for _ in range(4)
            ]
        )

        # The settle is assert_balanced's own bounded wait (cap 120s) - a
        # population that cannot settle after the weather fails the
        # scenario by construction, no second timer to race it.
        assert (
            len(handles) == 4 + 4 * 2 + 2 + 4
        )  # 4 before + 2 per weather cycle + 2 into the hard-cut pass + 4 into the recovery
        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) == {"succeeded"}, (
            f"the partitioned fleet did not converge to all-succeeded: {counts}"
        )
        await assert_effects_balance(conn, schema, _TAG)

        # The workers are the SAME processes: recovery without restart.
        for i, worker in enumerate(workers):
            assert worker.proc.poll() is None, (
                f"soak worker {i} exited during the weather windows: recovery "
                f"must not require a restart"
            )
    finally:
        for worker in workers:
            reap(worker)
        with contextlib.suppress(Exception):
            await proxy.clear()
        await delete_tagged(conn, schema, _TAG)

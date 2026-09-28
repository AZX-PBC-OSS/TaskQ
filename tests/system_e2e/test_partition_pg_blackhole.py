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
from taskq.worker.heartbeat import (  # pyright: ignore[reportPrivateUsage]  # Why: the failed-tick retry constant is the cascade pace's own term, imported not re-copied.
    _FAILED_TICK_RETRY_FRACTION,
)
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
#: from the worker env below, never asserted from hope. A's knobs are
#: raised to their cascade-legal maximum so A's lifetime PROVABLY
#: outlives the sibling probe's budget (see _PROBE_BUDGET_S): the
#: concurrency assert below requires the probe to complete while A is
#: STILL ALIVE, and A's lifetime is governed by A's OWN watchdog
#: ladder - a small cascade (the original F=2 at interval 0.5: isolate
#: at ~2.75s, force-exit ~17s) makes that assert a RACE the loaded-box
#: pipeline straddles (the CI red: the probe landed post-exit).
#: Stretching the probe's budget cannot fix that - a longer budget
#: tolerates a LATER completion, further past A's exit - so the bound
#: moves on A's side. The lever is the heartbeat INTERVAL, because the
#: interval is the floor of EVERY failed cycle under the cut (measured
#: in this suite's own probes): a blackholed cycle can only end at a
#: LOCAL timeout, and there are exactly two shapes - the pool acquire
#: hanging a fresh connect to its own ``timeout=interval`` bound, or a
#: cached-connection command hanging the tick's whole command budget
#: (``heartbeat_command_timeout``) and paying the failed tick's
#: prompt-retry wait, ``min(interval - duration, 0.25 * interval)``.
#: With ``command_timeout >= 0.75 * interval`` the two shapes COINCIDE
#: at exactly one interval per cycle (16 >= 0.75 * 21; 16 + 5 = 21):
#:
#:   worst_beat_cycle = heartbeat_interval + heartbeat_command_timeout
#:                    = 21 + 16 = 37s   (a cycle's SLOWEST legal span)
#:   cycle floor       = min(interval, command_timeout + 0.25*interval)
#:                       = min(21, 16 + 5.25) = 21s
#:   F = max_heartbeat_failures = 6: the isolate decision lands on the
#:   (F+1)-th = 7th consecutive failure, and the documented settings
#:   floor (heartbeat._lease_renewal_threshold, enforced by settings
#:   post_load's lock_lease check) is
#:   tail + (F+1) * worst_beat_cycle = max(21, 16) + 7 * 37 = 280s, so
#:   the lease A claims with must carry it: lock_lease 285.0.
_A_HEARTBEAT_INTERVAL_S = 21.0
_A_HEARTBEAT_COMMAND_TIMEOUT_S = 16.0  # >= 0.75 * interval: the two failed-cycle shapes coincide
_A_MAX_HEARTBEAT_FAILURES = 6  # isolate on the 7th consecutive failure
_A_WORST_BEAT_CYCLE_S = _A_HEARTBEAT_INTERVAL_S + _A_HEARTBEAT_COMMAND_TIMEOUT_S  # 37
_A_LAST_BEAT_TAIL_S = max(_A_HEARTBEAT_INTERVAL_S, _A_HEARTBEAT_COMMAND_TIMEOUT_S)  # 21
_A_CASCADE_FLOOR_S = _A_LAST_BEAT_TAIL_S + (_A_MAX_HEARTBEAT_FAILURES + 1) * _A_WORST_BEAT_CYCLE_S
_A_LOCK_LEASE_S = 285.0  # >= the cascade floor 280 the settings' post_load enforces

#: A's LIFETIME FLOOR after the cut - the number the concurrency assert
#: stands on. Every failed cycle costs >= the interval (21s: the two
#: legal shapes above coincide there, and a blackholed cycle can end
#: at nothing FASTER than a local timeout), so (F+1) consecutive
#: failures take >= 7 * 21 = 147s, and A is alive through the cascade
#: AND the isolate's own bounded connect on top of it. That floor is
#: >= 2x the probe's 63s budget, so the probe - whatever the load -
#: lands strictly inside A's lifetime, and the concurrency assert
#: below is arithmetic, not a race. (Load only STRETCHES cycles: the
#: loop's sleeps oversleep, the timeouts fire late - the floor is the
#: one direction starvation cannot undercut.)
_A_MIN_FAILED_CYCLE_S = min(
    _A_HEARTBEAT_INTERVAL_S,
    _A_HEARTBEAT_COMMAND_TIMEOUT_S + _FAILED_TICK_RETRY_FRACTION * _A_HEARTBEAT_INTERVAL_S,
)
_A_LIFETIME_FLOOR_S = (_A_MAX_HEARTBEAT_FAILURES + 1) * _A_MIN_FAILED_CYCLE_S  # 147s
_CASCADE_S = _A_CASCADE_FLOOR_S

#: The isolate's OWN walk-away, measured in a standalone probe: the
#: cascade fires isolate, the isolate's fresh connect gives up at 5s,
#: the shutdown closes every dedicated connection at its 5s close
#: bound under a full cut (~8 closers = 40s), and the SHUTDOWN WATCHDOG
#: (on by production default, ``watchdog_enabled=True``) force-exits
#: the wedged teardown at the 15s termination grace - measured at this
#: cell's raised cascade: trip recorded, rc EXIT_WATCHDOG. The exit
#: bound is the cascade's WORST span (280s - the lease floor, the last
#: good beat a full interval back and every cycle at its slowest legal
#: pace) + the connect's 5s + the grace's 15s = 300s. The exit is a
#: FAMILY pin - the armed watchdog's shutdown-deadline is the detector
#: that bounds it (the stale-tick floor below is raised past the
#: cascade so detector 2 cannot preempt the isolate) - and where the
#: wedged teardown drains inside its closers' bounds the isolate's own
#: walk-away completes instead, the same contract's designed arm.
_A_ISOLATE_CONNECT_S = 5.0
_A_SHUTDOWN_GRACE_S = 15.0
_ISOLATE_EXIT_BUDGET_S = (
    _A_CASCADE_FLOOR_S + _A_ISOLATE_CONNECT_S + _A_SHUTDOWN_GRACE_S
) + 40.0  # 280 + 5 + 15 + loop-congestion slack == 340.0

#: Detector 2's stale-tick floor, raised for A above the cascade's
#: whole worst span (280s): under the cut the producer loop's rounds
#: starve (dispatch_batch is a multi-statement transaction, each
#: statement hanging its pool command budget) and the leader loops'
#: sweeps starve with them - at the shipped 10s floor the stale-tick
#: detector fires mid-cascade, A exits EARLY, and the concurrency
#: assert below is a race again (measured: the cascade's cycles
#: stretch to ~2s under the loop's own congestion). Sized past the
#: cascade, detector 2 cannot preempt the isolate decision it exists
#: to survive; the shutdown-deadline detector (the 15s grace) is what
#: bounds the exit and provides the pinned trip. Legal per the
#: settings' own checks: the floor must exceed
#: dispatcher_command_timeout + the 1s leader period (400 > 5 + 1),
#: and the terminal lag budget + interval stays inside the lease
#: (5.0 + 21 < 285.0).
_A_WATCHDOG_STALE_FLOOR_S = 400.0

#: The reclaim pipeline that must hand the job back WITHOUT the cut
#: worker's help. The MEASURED shape (standalone probe, identical
#: topology): A's watchdog-bounded exit, then B's leadership takeover
#: (the leader advisory lock only frees with A's session; the election
#: cadence plus first sweep tick), then the job's lease expiry (the
#: last renewal is A's last good beat, so expiry lands at most one
#: interval past the cut + lock_lease = ~cut + 306s, INSIDE A's exit
#: bound) + sweep tick (1s) + reclaim delay (retry curve base 1s) +
#: claim (poll 0.05) + body (3s). Budget: A's exit bound 340 + the
#: band's pipeline 63 + slack -> 460.
_RERUN_BUDGET_S = 460.0

#: The sibling-health probe's budget, DERIVED from the documented band
#: (the storm's ``_STORM_DEADLINE_SECS`` arithmetic, 74c5931b), never
#: bare. The original 20s was one pipeline's IDLE-box cost and flaked
#: exactly there (the CI red: ``<Record status='running'>`` - the
#: sibling's OWN bootstrap, pool creation + listen attach + the
#: schema's first-touch, alone outgrew it on a loaded -n 2 runner).
#: One pipeline's components, each a documented constant:
#:
#: * B's bootstrap - the readiness gate the shared harness itself
#:   grants one worker (``_harness.wait_for_socket``: 30s for the
#:   health socket to answer). The probe is enqueued BEFORE that gate
#:   is awaited, so the bootstrap rides INSIDE this budget, not on top
#:   of it - carried at full weight, unstretched: it is already the
#:   harness's loaded-box tolerance.
#: * the claim poll (0.05 - the worker's TASKQ_POLL_INTERVAL, the
#:   harness default, the same constant the storm's arithmetic names)
#:   + the probe's body (0.1, the SysPayload sleep) + the terminal
#:   commit's wire slack (0.5, one heartbeat interval) = 0.65s of
#:   tail, times the storm's 20x co-tenancy stretch (the stall band
#:   those runners produce between a seed and its observation)
#:   -> 13s;
#: * the original 20s kept as the floor (the storm keeps its original
#:   8s soak the same way).
#:
#:   30 + 0.65 * 20 + 20  =>  63.0s.
#:
#: Like the storm's, the budget bounds FAILURE only: a sibling that
#: genuinely cannot serve never completes the probe (the teeth below
#: pin that - pointed at the cut, it must red). The concurrency
#: assert's own posture is no longer the band's edge: A's raised
#: lifetime floor (above) is 2x this budget, so a completing probe is
#: always concurrent evidence.
_PROBE_READY_GATE_S = 30.0  # _harness.wait_for_socket's bound for one worker's bootstrap
_PROBE_CLAIM_POLL_S = 0.05  # the worker's TASKQ_POLL_INTERVAL (the harness default)
_PROBE_BODY_S = 0.1  # the probe's SysPayload sleep
_PROBE_COMMIT_S = 0.5  # the terminal commit's wire slack (one heartbeat interval)
_PROBE_TAIL_S = _PROBE_CLAIM_POLL_S + _PROBE_BODY_S + _PROBE_COMMIT_S
_COTENANCY_STRETCH = 20  # the storm's band (74c5931b) on loaded -n 2 runners
_PROBE_FLOOR_S = 20.0  # the original idle-box budget, kept as the floor
_PROBE_BUDGET_S = (
    _PROBE_READY_GATE_S + _PROBE_TAIL_S * _COTENANCY_STRETCH + _PROBE_FLOOR_S
)  # 30 + 0.65 * 20 + 20 == 63.0


@pytest.mark.timeout(600)
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
    # is clean by construction. A's heartbeat knobs are the
    # cascade-legal maximum (see the arithmetic at the constants):
    # interval 21 with command timeout 16 pins EVERY failed cycle at
    # >= 21s (the two blackhole shapes coincide), F=6 puts the isolate
    # decision at the 7th consecutive failure - a lifetime floor of
    # 147s, 2x the probe's 63s budget - and the lease carries the
    # cascade the settings' post_load check demands (280s floor <=
    # 285.0). The stale-tick floor rides above the cascade's whole
    # worst span so detector 2 cannot preempt the isolate mid-cascade
    # (the producer's blackhole-starved rounds would otherwise trip it
    # at the shipped 10s floor and re-introduce the exit race this
    # cell's concurrency assert died to); the shutdown-deadline
    # detector is what bounds the exit and provides the pinned trip.
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
            "TASKQ_HEARTBEAT_INTERVAL": str(_A_HEARTBEAT_INTERVAL_S),
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": str(_A_HEARTBEAT_COMMAND_TIMEOUT_S),
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(_A_MAX_HEARTBEAT_FAILURES),
            "TASKQ_LOCK_LEASE": str(_A_LOCK_LEASE_S),
            # The production watchdog shape: the shipped default is ON;
            # the harness's _BASE_ENV disables it. Under a persistent cut
            # the isolate fires and the teardown's own closers are
            # unfinishable - the watchdog's grace force-exit is what
            # bounds the exit, the contract this cell pins.
            "TASKQ_WATCHDOG_ENABLED": "true",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "5.0",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "2.5",
            "TASKQ_WATCHDOG_STALE_FLOOR": str(_A_WATCHDOG_STALE_FLOOR_S),
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
        probe_deadline = time.monotonic() + _PROBE_BUDGET_S
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
            f"(bootstrap gate {_PROBE_READY_GATE_S}s + claim {_PROBE_CLAIM_POLL_S}s + body "
            f"{_PROBE_BODY_S}s + commit {_PROBE_COMMIT_S}s, x{_COTENANCY_STRETCH} co-tenancy "
            f"stretch + {_PROBE_FLOOR_S}s floor = {_PROBE_BUDGET_S}s budget, poll 0.05): "
            f"{probe_row}"
        )
        # The DETERMINISTIC containment: the probe's budget bounds
        # COMPLETION at 63s, A's raised lifetime bounds EXIT at >= 147s
        # (the cascade floor: 7 failed beats x 21s minimum cycle, 2x the
        # budget), so the probe - whatever the load - lands strictly
        # inside A's lifetime. The assert is the arithmetic's witness,
        # not a race: any red here is a config regression, not a slow
        # runner.
        assert worker_a.proc.poll() is None, (
            "the sibling's probe completed only after the partitioned worker had "
            f"already exited - the sibling's health was not proven CONCURRENTLY "
            f"with the cut (probe budget {_PROBE_BUDGET_S}s vs A's lifetime floor "
            f"{_A_LIFETIME_FLOOR_S}s: the cascade-legal levers must keep A alive "
            f"past 2x the budget)"
        )
        wait_worker_ready(worker_b)

        # The isolate cascade (the raised floor: 7 failed beats, worst
        # span 280s) + the isolate connect's own 5s give-up + the
        # termination grace (15s): A must EXIT, not linger as a zombie
        # holding in-memory state. The bound is the cascade's WORST
        # legal span plus the walk-away - the exit is bounded, only
        # late.
        exit_deadline = cut_at + _ISOLATE_EXIT_BUDGET_S
        while time.monotonic() < exit_deadline and worker_a.proc.poll() is None:  # noqa: ASYNC110  # Why: the wait is a deadline OR the process's death - an Event cannot express either arm.
            await asyncio.sleep(0.2)
        assert worker_a.proc.poll() is not None, (
            f"the partitioned worker did not self-isolate and exit within "
            f"{_ISOLATE_EXIT_BUDGET_S}s (cascade floor {_CASCADE_S}s + connect "
            f"{_A_ISOLATE_CONNECT_S}s + grace {_A_SHUTDOWN_GRACE_S}s)"
        )

        # The worker's OWN evidence, from its log sink: the failed ticks
        # the isolate ledger counted (F=6 - the isolate fires on the 7th
        # consecutive failure) and the bounded exit above. A worker that
        # exits for any OTHER reason (a crash, a supervisor kill, a
        # clean return) without walking the cascade is not the contract
        # here.
        log_text = log_a.read_text(errors="replace")
        tick_failures = log_text.count("heartbeat-tick-failure") + log_text.count(
            "heartbeat-tick-unexpected-error"
        )
        assert tick_failures >= _A_MAX_HEARTBEAT_FAILURES + 1, (
            f"the partitioned worker's log records {tick_failures} failed ticks - the "
            f"({_A_MAX_HEARTBEAT_FAILURES + 1})-th consecutive failure the isolate "
            f"decision needs (F={_A_MAX_HEARTBEAT_FAILURES}) left no "
            f"trace in the worker's own log"
        )
        # The production-shape exit: the watchdog is armed (the shipped
        # default, re-armed explicitly in this cell's env) and the exit
        # is DELIBERATE - either the wedged teardown's force-exit at the
        # shutdown-deadline (the measured shape: trip recorded,
        # rc EXIT_WATCHDOG) or the isolate's own walk-away completing
        # inside its closers' bounds (rc 0 - the same contract's
        # designed arm, when the teardown drains before the grace).
        # A crash, a signal kill, or any foreign exit code is neither.
        assert worker_a.proc.returncode in (EXIT_WATCHDOG, 0), (
            f"the partitioned worker exited rc={worker_a.proc.returncode}, not the "
            f"isolate's deliberate exit (the watchdog force-exit {EXIT_WATCHDOG} or "
            f"the designed walk-away 0): the exit did not come from the armed "
            f"watchdog's isolate path"
        )
        if "worker-watchdog-trip" in log_text:
            assert worker_a.proc.returncode == EXIT_WATCHDOG, (
                f"the partitioned worker recorded a watchdog trip but exited "
                f"rc={worker_a.proc.returncode}: the trip's force-exit did not bound "
                f"the exit it fired for"
            )

        # The re-run: lease expiry (~cut + 306s: the last renewal is A's
        # last good beat, one interval past the cut, plus the 285s lease)
        # + sweep (1s) + reclaim delay (1s) + claim + body (3s) - the job
        # must reach succeeded on attempt 2
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
                f"of the cut (A's watchdog-bounded exit, B's takeover, lease expiry + "
                f"sweep + reclaim delay + claim + body, the pipeline the standalone "
                f"probe measured at cut+48s at the old cadence): {final}; "
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

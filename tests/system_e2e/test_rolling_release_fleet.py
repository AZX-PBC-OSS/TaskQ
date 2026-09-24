"""Lifecycle 5: the fleet under a rolling release.

Five real worker pods on one real Postgres, a deep mixed queue, and a
release that takes the fleet down a pod at a time: sequential SIGTERMs,
then overlapping pairs, then the whole-fleet storm, then the grace-edge
flap. The SURVIVORS are the subject: while one pod drains, the others
must keep dispatching, the distributed caps must stay honest (the
departing pod's capacity released and re-admissible, the keyed caps and
the queue caps alike), and no queue may be left owned by a corpse.

Every timing assertion is derived from the scenario's own knobs (the
termination grace, the lock lease, the sweep interval, the poll floor)
and the MEASURED value is printed next to the bound, so a failure names
both the bound and how far past it the system ran.

The churn matrix (one scenario per row):

1. sequential drains  - SIGTERM one pod at a time, each awaited; probes
   enqueued mid-drain must complete (no stall); the corpse owns nothing
   on exit (no running row, no live slot).
2. overlapping pairs  - two pods drain at once, a second pair overlaps
   the first, the last pod carries the load alone; the cap sampler
   watches every running row the whole time.
3. the churn x the caps - a pod holding queue-cap and keyed-cap slots
   is SIGTERMed (graceful release) and another SIGKILLed mid-job (the
   capacity-leak construct): the departing pod's slots must free within
   the derived bound (graceful: the drain itself; killed: the slot
   lease + the sweep interval) and the backlog must be re-admitted.
4. the storm - all five pods SIGTERM within one second; every drain
   concurrent; the last pod out leaves the fleet empty and the ledger
   whole; the next boot picks the requeues up and conserves.
5. the flapping release - a pod lingers at the grace edge, SIGKILLed at
   95% of its termination budget with the drain unfinished; a NEW pod
   with a NEW identity picks the row up inside the hold/lease bound and
   no path assumes the corpse's identity ever returns.

Shared system invariants (``_invariants.py``) close every scenario: the
conservation counter over jobs + archive, the attempt-ledger
reconciliation, and the exactly-once effects ledger.
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import TYPE_CHECKING

import asyncpg
import pytest
import pytest_asyncio

from tests.system_e2e._harness import (
    WorkerProc,
    graceful_stop,
    reap,
    spawn_worker,
    wait_worker_ready,
)
from tests.system_e2e._invariants import (
    assert_balanced,
    assert_effects_balance,
    conservation_violations,
    delete_tagged,
)
from tests.system_e2e.actors import (
    RollPayload,
    SysPayload,
    sys_capped,
    sys_fast,
    sys_keyed,
    sys_slow,
    sys_winc,
)

if TYPE_CHECKING:
    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s7"

#: The scenario fleet: five pods, every one consuming both queues.
_PODS = ["w0", "w1", "w2", "w3", "w4"]
_QUEUES = "system_e2e,roll_capped"

#: The queue-cap fleet pin: the capped queue admits 2 fleet-wide.
_QUEUE_CAP = 2

#: The per-worker dispatch cap (the harness's TASKQ_MAX_CONCURRENCY).
_WORKER_CAP = 4

#: The harness knobs the bounds below derive from (tests/system_e2e/
#: _harness.py _BASE_ENV): the SIGTERM budget per pod, the lock/slot
#: lease the heartbeat renews, the leader sweep cadence, the poll floor.
_GRACE = 15.0
_LOCK_LEASE = 8.0
_SWEEP_INTERVAL = 1.0
_POLL_FLOOR = 1.0

#: The keyed cap's slot lease (actors.py _TENANT_SLOT_LEASE).
_KEYED_LEASE = 8.0

#: The leader lease the scenario workers boot with (settings floor is
#: 4 heartbeats = 2 s here) and the killed-pod reclaim bounds. The
#: stale-row wrinkle the churn exposes (the heartbeat renews a slot row
#: BY JOB, so a reclaimed job's survivor keeps the corpse-named row
#: live-held until that attempt ends) stretches the corpse's slot
#: freedom to the re-claimed job's own recovery cycle: lease 8 + leader
#: failover 4 + sweep tick 1 + poll 1 + run 8 + lease 8 + deferral 5 +
#: margin 2 <= 37 s, bounded at 45 s; the sweep's nulling additionally
#: waits out a leader failover plus one tick past re-admission.
_LEADER_LEASE = 4.0
_CORPSE_SLOT_BOUND = 45.0
_SWEEP_BOUND = _LEADER_LEASE + _SWEEP_INTERVAL + _POLL_FLOOR


# ── Fixtures and fleet plumbing ──────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def roll_capped_queue(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> AsyncGenerator[None, None]:
    """The capped queue row, BEFORE any pod boots.

    The worker bootstrap reads ``queues.max_concurrent`` at startup and
    registers the queue-cap reservation from it, so the row must exist
    before the first pod of every scenario in this module spawns.
    """
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2) '
            "ON CONFLICT (name) DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent",
            "roll_capped",
            _QUEUE_CAP,
        )
        yield
    finally:
        await conn.close()


def _spawn_fleet(pg_dsn: str, schema: str, names: list[str]) -> dict[str, WorkerProc]:
    """Boot the named pods and gate each on its own health socket."""
    fleet: dict[str, WorkerProc] = {}
    for name in names:
        worker = spawn_worker(
            pg_dsn,
            schema,
            tag=f"roll-{name}",
            extra_env={
                "TASKQ_QUEUES": _QUEUES,
                # A short leader lease: the scenarios SIGKILL pods that
                # may be the maintenance leader, and the sweep-4
                # bookkeeping bound derives from the failover wait.
                "TASKQ_LEADER_LEASE": str(_LEADER_LEASE),
            },
        )
        wait_worker_ready(worker)
        fleet[name] = worker
    return fleet


async def _pid_to_worker_id(conn: asyncpg.Connection, schema: str) -> dict[int, str]:
    rows = await conn.fetch(f'SELECT pid, id::text AS id FROM "{schema}".workers')
    return {int(r["pid"]): str(r["id"]) for r in rows}


async def _worker_ids(
    conn: asyncpg.Connection, schema: str, fleet: dict[str, WorkerProc]
) -> dict[str, str]:
    """Map pod name -> worker row id, via the process's own pid.

    Registration lands after the pools open, which can be after the
    health socket answers, so this waits (briefly) for every pod's row.
    """
    deadline = time.monotonic() + 30.0
    out: dict[str, str] = {}
    while time.monotonic() < deadline:
        mapping = await _pid_to_worker_id(conn, schema)
        out = {name: mapping.get(worker.proc.pid, "") for name, worker in fleet.items()}
        if all(out.values()):
            return out
        await asyncio.sleep(0.2)
    missing = [name for name, wid in out.items() if not wid]
    raise AssertionError(f"pods {missing} never registered a worker row within 30s")


# ── The shared pins ──────────────────────────────────────────────────────


class CapSampler:
    """Continuously tally every running row against the distributed caps.

    Three pins, sampled every 100 ms for the WHOLE scenario:

    * the queue cap: running rows on the capped queue never exceed the
      queue's max_concurrent, no matter which pod holds them or which
      pod is mid-drain;
    * the keyed cap: running rows of the keyed actor never exceed one
      per tenant, across the whole fleet;
    * the worker cap: a pod never runs more than its max_concurrency.

    The sampler also records each cap's PEAK, so the pins are provably
    not vacuous (a cap never exercised proves nothing).
    """

    def __init__(self, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema
        self._conn: asyncpg.Connection | None = None
        self.stop = asyncio.Event()
        self.violations: list[str] = []
        self.peak: dict[str, int] = {}
        self.samples = 0
        self._task: asyncio.Task[None] | None = None

    async def _tick(self) -> None:
        if self._conn is None:
            self._conn = await asyncpg.connect(self._dsn)
        rows = await self._conn.fetch(
            f"SELECT actor::text AS actor, queue::text AS queue, "
            f"payload->>'tenant' AS tenant, locked_by_worker::text AS holder "
            f"FROM \"{self._schema}\".jobs WHERE status = 'running'"
        )
        slots = await self._conn.fetch(
            f"SELECT bucket_name::text AS bucket, count(*)::int AS held "
            f'FROM "{self._schema}".reservation_slots '
            f"WHERE job_id IS NOT NULL AND lease_expires_at >= statement_timestamp() "
            f"GROUP BY 1"
        )
        self.samples += 1
        # HARD: the slot surface. The queue-cap bucket's slot count is
        # the queues row's max_concurrent; a keyed tenant bucket's is
        # the ref's slots (1 here).
        for row in slots:
            bucket = row["bucket"]
            if bucket == "taskq:global:queue:roll_capped":
                self._record(f"slots:{bucket}", row["held"], _QUEUE_CAP)
            elif bucket.startswith("roll-tenant:"):
                self._record(f"slots:{bucket}", row["held"], 1)
        # DAMPED: the job surface, at the full fleet's derived bounds.
        capped = [r for r in rows if r["queue"] == "roll_capped"]
        self._record("capped_queue", len(capped), len(_PODS) * _QUEUE_CAP, rows)
        per_tenant: dict[str, int] = {}
        for row in rows:
            if row["actor"] == "sys_keyed":
                tenant = row["tenant"] or "<null>"
                per_tenant[tenant] = per_tenant.get(tenant, 0) + 1
        for tenant, n in per_tenant.items():
            self._record(f"tenant:{tenant}", n, len(_PODS), rows)
        per_worker: dict[str, int] = {}
        for row in rows:
            holder = row["holder"] or "<null>"
            per_worker[holder] = per_worker.get(holder, 0) + 1
        for holder, n in per_worker.items():
            self._record(f"worker:{holder}", n, 2 * _WORKER_CAP, rows)

    def _record(
        self, cap: str, observed: int, bound: int, rows: list[asyncpg.Record] | None = None
    ) -> None:
        key = f"peak.{cap}"
        if observed > self.peak.get(key, 0):
            self.peak[key] = observed
        if observed > bound:
            self.violations.append(
                f"cap {cap} observed {observed} > bound {bound} (sample {self.samples}): "
                f"{[dict(r) for r in (rows or [])]}"
            )

    async def _run(self) -> None:
        while not self.stop.is_set():
            try:
                await self._tick()
            except Exception as exc:  # Why: the sampler must never kill the scenario; a failed sample is a missing sample, the next one repairs the tally.
                self.violations.append(f"sampler error: {exc!r}")
            await asyncio.sleep(0.1)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        """Stop the sampler and make sure its task is always awaited."""
        self.stop.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task  # Why: teardown; the tally already recorded what it saw.

    async def stop_and_report(self, label: str) -> None:
        await self.close()
        assert not self.violations, (
            f"{label}: the distributed caps were breached under the churn:\n"
            + "\n".join(self.violations[:20])
        )


async def _assert_corpse_owns_nothing(
    conn: asyncpg.Connection, schema: str, worker_id: str, label: str
) -> None:
    """No queue left owned by a corpse: no running row locked by it, and
    no reservation slot (queue-cap or keyed) live-held by it."""
    rows = await conn.fetchval(
        f'SELECT count(*)::int FROM "{schema}".jobs '
        "WHERE status = 'running' AND locked_by_worker = $1::uuid",
        worker_id,
    )
    assert rows == 0, (
        f"{label}: the departed pod still owns {rows} running row(s) - "
        "a queue left owned by a corpse"
    )
    slots = await conn.fetchval(
        f'SELECT count(*)::int FROM "{schema}".reservation_slots '
        "WHERE held_by_worker_id = $1::uuid AND job_id IS NOT NULL "
        "AND lease_expires_at >= statement_timestamp()",
        worker_id,
    )
    assert slots == 0, (
        f"{label}: the departed pod still live-holds {slots} reservation slot(s) - "
        "its capacity never came back to the fleet"
    )


async def _db_now(conn: asyncpg.Connection) -> datetime:
    row = await conn.fetchrow("SELECT statement_timestamp() AS now")
    assert row is not None
    return row["now"]


async def _settle_probes(
    conn: asyncpg.Connection, schema: str, probe_ids: list[str], cap_secs: float
) -> None:
    """Wait until the probe jobs all succeeded (the survivors served them)."""
    deadline = time.monotonic() + cap_secs
    pending = list(probe_ids)
    while time.monotonic() < deadline:
        rows = await conn.fetch(
            f'SELECT id::text AS id, status::text AS status FROM "{schema}".jobs '
            "WHERE id = ANY($1::uuid[])",
            pending,
        )
        done = {r["id"] for r in rows if r["status"] == "succeeded"}
        pending = [i for i in pending if i not in done]
        if not pending:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"the survivors never served {len(pending)} probe job(s) within "
        f"{cap_secs}s - the fleet stalled while a pod drained; last states: "
        f"{await conn.fetch('SELECT id::text, status::text FROM "' + schema + '".jobs WHERE id = ANY($1::uuid[])', pending)}"
    )


async def _effect_times(
    conn: asyncpg.Connection, schema: str, job_ids: list[str]
) -> list[datetime]:
    rows = await conn.fetch(
        f'SELECT at FROM "{schema}".sys_effects WHERE job_id = ANY($1::uuid[])',
        job_ids,
    )
    return [r["at"] for r in rows]


async def _fill_fleet(
    conn: asyncpg.Connection, schema: str, sys_client: TaskQ, want_running: int
) -> None:
    """Enqueue the deep mixed queue and block until the fleet fills.

    Ten slow bodies spread one slow job over every pod pair of slots,
    one wind-down job per pod (every drain the release performs then has
    a measurable window), capped and keyed work spread the distributed
    caps fleet-wide.
    """
    for _ in range(10):
        await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG])
    for _ in range(5):
        # The wind-down body holds its pod's drain open for ~sleep
        # seconds: long enough that a loaded claim-to-effect cycle
        # (probes, below) lands inside the drain window it brackets.
        await sys_client.enqueue(sys_winc, SysPayload(sleep=6.0), tags=[_TAG])
    for _ in range(8):
        await sys_client.enqueue(sys_capped, SysPayload(sleep=2.0), tags=[_TAG])
    for i in range(8):
        await sys_client.enqueue(sys_keyed, RollPayload(sleep=2.0, tenant=f"t{i % 4}"), tags=[_TAG])
    deadline = time.monotonic() + 30.0
    running = 0
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            f"SELECT count(*)::int AS n FROM \"{schema}\".jobs WHERE status = 'running'"
        )
        assert row is not None
        running = row["n"]
        if running >= want_running:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"the fleet never filled its slots (peak {running} < {want_running}) - "
        "the scenario premise is void"
    )


# ── Scenario 1: sequential drains ────────────────────────────────────────


@pytest.mark.timeout(400)
async def test_rolling_release_sequential_drains_keep_the_fleet_serving(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    roll_capped_queue: None,
) -> None:
    """Pods SIGTERM one at a time, each awaited; survivors keep
    dispatching through every drain; each corpse's rows and capacity
    come back clean on exit."""
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    fleet = _spawn_fleet(pg_dsn, schema, _PODS)
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    measurements: list[tuple[str, float, float]] = []
    try:
        ids = await _worker_ids(conn, schema, fleet)
        await _fill_fleet(conn, schema, sys_client, want_running=10)

        for name in _PODS[:-1]:
            # The mid-drain probe wave (uncapped queue: the probe must
            # not queue behind the cap's own backlog). The window opens
            # BEFORE the probes are enqueued, so a probe effect landing
            # inside [open, close] means the fleet dispatched DURING the
            # drain, not merely after it.
            window_open = await _db_now(conn)
            probes = [
                await sys_client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
                for _ in range(4)
            ]
            probe_ids = [str(j.job_id) for j in probes]

            worker = fleet[name]
            t_sig = time.monotonic()
            worker.proc.terminate()
            rc = await asyncio.to_thread(graceful_stop, worker, timeout=_GRACE + 30.0)
            t_exit = time.monotonic()
            drained.add(name)
            drain_secs = t_exit - t_sig
            window_close = await _db_now(conn)

            assert rc == 0, (
                f"pod {name} exited rc={rc} - the rolling release's drain was not graceful"
            )
            assert drain_secs < _GRACE, (
                f"pod {name}'s drain ran {drain_secs:.2f}s, past its "
                f"{_GRACE:.0f}s termination grace"
            )
            measurements.append((f"sequential drain {name}", drain_secs, _GRACE))

            # No stall: the probes are served promptly, and when the
            # drain had a window to observe (the pod held a wind-down
            # body) at least one probe ran INSIDE it.
            await _settle_probes(conn, schema, probe_ids, cap_secs=30.0)
            times = await _effect_times(conn, schema, probe_ids)
            assert times, f"pod {name}: no probe effect recorded at all"
            if drain_secs >= 1.5:
                during = [t for t in times if window_open <= t <= window_close]
                assert during, (
                    f"pod {name}: no probe ran during its {drain_secs:.2f}s drain "
                    "window - the fleet stalled while the pod drained"
                )
            await _assert_corpse_owns_nothing(conn, schema, ids[name], f"sequential {name}")

        # The permanent survivor drains last, after the settle below
        # proves it carried the whole release's load.
        counts = await assert_balanced(conn, schema, _TAG)
        survivor = _PODS[-1]
        rc = await asyncio.to_thread(graceful_stop, fleet[survivor], timeout=_GRACE + 30.0)
        drained.add(survivor)
        assert rc == 0, f"the survivor {survivor} exited rc={rc}"
        await _assert_corpse_owns_nothing(conn, schema, ids[survivor], f"survivor {survivor}")
        # The counter balances over the whole cohort, the survivor's
        # own drain included.
        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) >= 10 + 8 + 8, (
            f"the fleet did not complete every job it admitted: {counts}"
        )
        await assert_effects_balance(conn, schema, _TAG)
        await sampler.stop_and_report("sequential drains")
        assert sampler.peak.get("peak.capped_queue", 0) >= _QUEUE_CAP, (
            "the queue cap was never exercised - the sampler pins are vacuous"
        )
        assert sampler.peak.get("peak.slots:taskq:global:queue:roll_capped", 0) >= _QUEUE_CAP, (
            "the capped bucket's slots were never all held - the hard pin is vacuous"
        )
        for label, measured, bound in measurements:
            print(f"[s7] {label}: measured {measured:.2f}s vs bound {bound:.0f}s")
    finally:
        await sampler.close()
        for name in _PODS:
            worker = fleet.get(name)
            if worker is not None and name not in drained:
                reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ── Scenario 2: overlapping pairs ────────────────────────────────────────


@pytest.mark.timeout(400)
async def test_rolling_release_overlapping_pairs_conserve_under_concurrent_churn(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    roll_capped_queue: None,
) -> None:
    """Two pods drain at once, a second pair overlaps the first, the
    last pod carries the load alone; the cap sampler watches every
    running row for the whole release."""
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    fleet = _spawn_fleet(pg_dsn, schema, _PODS)
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    try:
        ids = await _worker_ids(conn, schema, fleet)
        await _fill_fleet(conn, schema, sys_client, want_running=10)

        # Pair 1: both SIGTERMs land back to back; both drains run
        # concurrently.
        t_pair1 = time.monotonic()
        fleet["w0"].proc.terminate()
        fleet["w1"].proc.terminate()
        await asyncio.sleep(0.5)
        assert fleet["w0"].proc.poll() is None or fleet["w1"].proc.poll() is None, (
            "pair 1 exited before the overlap window opened - the churn "
            "was sequential, not concurrent"
        )

        # Pair 2 is signalled INSIDE pair 1's drain window: three pods
        # drain at once, two generations of the release overlap.
        fleet["w2"].proc.terminate()
        fleet["w3"].proc.terminate()
        await asyncio.sleep(0.5)
        assert fleet["w0"].proc.poll() is None or fleet["w3"].proc.poll() is None, (
            "pair 2's signal landed after pair 1 finished - the churn was not overlapping"
        )

        # Probes during the heaviest window: three pods draining, w4 -
        # still up - carrying them, then it settles before the LAST pod
        # drains (the storm scenario owns the full die-off; here the
        # probes must have a survivor to be served by).
        window_open = await _db_now(conn)
        probes = [
            await sys_client.enqueue(sys_slow, SysPayload(sleep=0.2), tags=[_TAG]) for _ in range(3)
        ]
        probe_ids = [str(j.job_id) for j in probes]
        await _settle_probes(conn, schema, probe_ids, cap_secs=60.0)
        probe_times = await _effect_times(conn, schema, probe_ids)
        assert any(window_open <= t for t in probe_times), (
            "no probe effect landed inside the churn window - the last "
            "survivor did not dispatch during the overlapping drains"
        )

        # The four drained pods are joined on their exits; the survivor
        # (w4) stays up and serves everything they handed back.
        for name in _PODS[:4]:
            rc = await asyncio.to_thread(graceful_stop, fleet[name], timeout=_GRACE + 60.0)
            drained.add(name)
            assert rc == 0, f"pod {name} exited rc={rc} under the overlapping churn"
        pair1_secs = time.monotonic() - t_pair1
        print(
            f"[s7] overlapping churn (5 pods, 3 draining concurrently): "
            f"measured {pair1_secs:.2f}s vs bound 5 x {_GRACE:.0f}s worst-case serial"
        )

        for name in _PODS[:4]:
            await _assert_corpse_owns_nothing(conn, schema, ids[name], f"overlap {name}")

        # The survivor settles the whole cohort (its re-spread of the
        # departed pods' queues is what the settle measures).
        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) >= 10 + 8 + 8 + 3, (
            f"the fleet did not complete every job it admitted: {counts}"
        )

        # The last pod out: drained only once it holds nothing, so its
        # drain interrupts nothing and the die-off leaves no stranded
        # requeue (the storm scenario owns the full die-off with a
        # restart; this scenario ends on a clean empty fleet).
        deadline = time.monotonic() + 90.0
        busy = -1
        while time.monotonic() < deadline:
            busy = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".jobs '
                "WHERE status = 'running' AND locked_by_worker = $1::uuid",
                ids["w4"],
            )
            if busy == 0:
                break
            await asyncio.sleep(0.2)
        assert busy == 0, "the survivor never drained its last row before the release ended"
        fleet["w4"].proc.terminate()
        rc = await asyncio.to_thread(graceful_stop, fleet["w4"], timeout=_GRACE + 30.0)
        drained.add("w4")
        assert rc == 0, f"the survivor w4 exited rc={rc}"
        await _assert_corpse_owns_nothing(conn, schema, ids["w4"], "survivor w4")

        await assert_effects_balance(conn, schema, _TAG)
        await sampler.stop_and_report("overlapping pairs")
        assert sampler.peak.get("peak.capped_queue", 0) >= _QUEUE_CAP, (
            "the queue cap was never exercised - the sampler pins are vacuous"
        )
        assert sampler.peak.get("peak.slots:taskq:global:queue:roll_capped", 0) >= _QUEUE_CAP, (
            "the capped bucket's slots were never all held - the hard pin is vacuous"
        )
    finally:
        await sampler.close()
        for name in _PODS:
            worker = fleet.get(name)
            if worker is not None and name not in drained:
                reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ── Scenario 3: the churn x the caps ────────────────────────────────────


async def _wait_corpse_slots_freed(
    conn: asyncpg.Connection, schema: str, worker_id: str
) -> tuple[float, float]:
    """Measure how long a departed pod's slot capacity takes to free.

    Two stages, each measured against the churn-recovery bound:

    * re-admissible: no slot row naming the CORPSE carries a LIVE
      lease. The wrinkle this measures (found by this scenario, kept
      because both backends' twins agree on it): a slot row's lease is
      renewed by the heartbeat BY JOB (``UPDATE_RESERVATION_LEASES_SQL_
      TEMPLATE`` keys on ``job_id IN (jobs locked by this worker)``,
      and the in-memory ``extend_leases_for_job`` renews the same
      way), so when a killed pod's job is reclaimed and re-claimed by
      a survivor, the SURVIVOR's beats keep the CORPSE-named row
      live-held until that attempt ends. The stale row's freedom is
      then bounded by the re-claimed job's own recovery cycle: the
      job-lock lapse + the reclaim sweep + the re-claim + the job's
      run time + one lease lapse of silence after it stops running.
      With this scenario's knobs (8 s jobs) that is
      lease 8 + leader failover 4 + sweep tick 1 + poll 1 (reclaim and
      re-claim) + run 8 + lease 8 (the lapse) + deferral 5 + margin
      2 <= 37 s, bounded here at 45 s;
    * swept: the freed rows are actually NULLED by the leader's slot
      sweep - the bookkeeping additionally waits out the leader
      failover (a SIGKILLed leader's lease must lapse before a
      survivor takes the tick) plus one sweep tick past re-admission.

    Returns (re-admission, swept) seconds; either bound exceeded raises.
    """
    t0 = time.monotonic()
    readmit: float | None = None
    swept: float | None = None
    live = -1
    held = -1
    while time.monotonic() - t0 < _CORPSE_SLOT_BOUND:
        row = await conn.fetchrow(
            f"SELECT count(*) FILTER (WHERE job_id IS NOT NULL AND lease_expires_at "
            f">= statement_timestamp())::int AS live, "
            f"count(*) FILTER (WHERE job_id IS NOT NULL)::int AS held "
            f'FROM "{schema}".reservation_slots WHERE held_by_worker_id = $1::uuid',
            worker_id,
        )
        assert row is not None
        live = row["live"]
        held = row["held"]
        elapsed = time.monotonic() - t0
        if readmit is None and live == 0:
            readmit = elapsed
        if readmit is not None and held == 0:
            swept = elapsed
            break
        await asyncio.sleep(0.1)
    if readmit is None:
        rows = await conn.fetch(
            f"SELECT bucket_name::text AS bucket, slot_index, job_id::text AS job_id, "
            f"lease_expires_at, statement_timestamp() AS now_ts "
            f'FROM "{schema}".reservation_slots WHERE held_by_worker_id = $1::uuid',
            worker_id,
        )
        raise AssertionError(
            f"the corpse's slot capacity did not re-admit within the derived bound "
            f"({_CORPSE_SLOT_BOUND:.1f}s): {live} slot(s) still live-held - a capacity "
            f"leak: the cap counts a claim the dead pod can never finish; rows: "
            f"{[dict(r) for r in rows]}"
        )
    if swept is None:
        held_now = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".reservation_slots '
            "WHERE held_by_worker_id = $1::uuid AND job_id IS NOT NULL",
            worker_id,
        )
        raise AssertionError(
            f"the corpse's freed slot rows were not swept within the derived bound "
            f"({_CORPSE_SLOT_BOUND:.1f}s, re-admission {readmit:.2f}s): {held_now} "
            "row(s) still name the dead pod - the slot sweep never caught up"
        )
    return readmit, swept


async def _settle_with_diagnostics(conn: asyncpg.Connection, schema: str, cap_secs: float) -> None:
    """Settle-or-diagnose: on a stuck population, name the stuck rows
    (status, attempt, holder, lock and schedule clocks) before failing -
    a livelock must print its own state, not just its timeout."""
    from taskq.backend.statemachine import TERMINAL_STATUSES

    terminal = [str(s) for s in TERMINAL_STATUSES]
    deadline = time.monotonic() + cap_secs
    while time.monotonic() < deadline:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status::text != ALL($2::text[])",
            _TAG,
            list(terminal),
        )
        if n == 0:
            return
        await asyncio.sleep(0.25)
    stuck = await conn.fetch(
        f"SELECT id::text, status::text AS status, attempt::int AS attempt, "
        f"locked_by_worker::text AS holder, lock_expires_at, scheduled_at, "
        f"interrupt_count::int AS interrupts, actor::text AS actor, queue::text AS queue "
        f'FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
        "AND status::text != ALL($2::text[])",
        _TAG,
        list(terminal),
    )
    raise AssertionError(
        f"NOT SETTLED within {cap_secs}s - a dropped or livelocked population; "
        f"stuck rows: {[dict(r) for r in stuck]}"
    )


@pytest.mark.timeout(400)
async def test_rolling_release_cap_churn_releases_departing_capacity_within_bounds(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    roll_capped_queue: None,
) -> None:
    """The departing pod's distributed capacity is released and
    re-admissible: graceful (the drain hands the slots back) and killed
    (the capacity-leak construct: lease expiry + the slot sweep free
    what the SIGKILL stranded) - each within its derived bound."""
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    fleet = _spawn_fleet(pg_dsn, schema, _PODS[:4])
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    try:
        ids = await _worker_ids(conn, schema, fleet)

        # Saturate BOTH cap families: the queue cap's two slots and one
        # keyed slot, all with a backlog behind them.
        for _ in range(6):
            await sys_client.enqueue(sys_capped, SysPayload(sleep=30.0), tags=[_TAG])
        for _ in range(4):
            await sys_client.enqueue(sys_keyed, RollPayload(sleep=30.0, tenant="t0"), tags=[_TAG])
        deadline = time.monotonic() + 30.0
        row = None
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                f"SELECT count(*) FILTER (WHERE queue = 'roll_capped')::int AS q, "
                f"count(*) FILTER (WHERE actor = 'sys_keyed')::int AS k "
                f"FROM \"{schema}\".jobs WHERE status = 'running'"
            )
            assert row is not None
            if row["q"] >= _QUEUE_CAP and row["k"] >= 1:
                break
            await asyncio.sleep(0.1)
        assert row is not None and row["q"] >= _QUEUE_CAP and row["k"] >= 1, (
            "the caps never saturated - the scenario premise is void"
        )

        # ── the graceful half: SIGTERM a pod holding cap slots ────────
        holder = await conn.fetchrow(
            f"SELECT locked_by_worker::text AS wid, count(*)::int AS n "
            f"FROM \"{schema}\".jobs WHERE status = 'running' "
            f"AND queue = 'roll_capped' GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
        )
        assert holder is not None and holder["wid"] is not None
        victim = next(name for name, wid in ids.items() if wid == holder["wid"])
        t_sig = time.monotonic()
        fleet[victim].proc.terminate()
        rc = await asyncio.to_thread(graceful_stop, fleet[victim], timeout=_GRACE + 30.0)
        graceful_secs = time.monotonic() - t_sig
        drained.add(victim)
        assert rc == 0, f"pod {victim} exited rc={rc}"
        await _assert_corpse_owns_nothing(conn, schema, ids[victim], f"graceful {victim}")
        assert graceful_secs < _GRACE, (
            f"graceful slot release took {graceful_secs:.2f}s, past the {_GRACE:.0f}s drain bound"
        )
        print(
            f"[s7] graceful cap release ({victim}, {holder['n']} capped rows): "
            f"measured {graceful_secs:.2f}s vs bound {_GRACE:.0f}s"
        )

        # ── the capacity-leak construct: SIGKILL a pod mid-job ────────
        # The churn re-spread the backlog after the graceful departure;
        # wait until one of the surviving pods live-holds cap capacity
        # again, so the kill provably strands a COUNTED slot (the leak
        # construct's premise), not an idle pod.
        victim2 = next(name for name in _PODS[:4] if name not in drained)
        corpse = ids[victim2]  # captured before the churn: the drained pod's row is gone by now
        deadline = time.monotonic() + 45.0
        held = 0
        while time.monotonic() < deadline:
            held = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".reservation_slots '
                "WHERE held_by_worker_id = $1::uuid AND job_id IS NOT NULL "
                "AND lease_expires_at >= statement_timestamp()",
                corpse,
            )
            if held >= 1:
                break
            await asyncio.sleep(0.2)
        assert held >= 1, "no survivor ever re-acquired cap capacity - nothing to strand"
        fleet[victim2].proc.kill()
        await asyncio.to_thread(fleet[victim2].proc.wait, 30)
        readmit_secs, swept_secs = await _wait_corpse_slots_freed(conn, schema, corpse)
        print(
            f"[s7] killed pod capacity reclaim ({victim2}): re-admission measured "
            f"{readmit_secs:.2f}s vs churn-recovery bound {_CORPSE_SLOT_BOUND:.0f}s "
            f"(terms in _wait_corpse_slots_freed); sweep-4 bookkeeping measured "
            f"{swept_secs:.2f}s after re-admission vs leader-failover bound "
            f"leader-lease {_LEADER_LEASE}+tick {_SWEEP_INTERVAL}+"
            f"{_POLL_FLOOR:.0f}={_SWEEP_BOUND:.0f}s"
        )

        # The release's replacement pod boots into the wounded fleet
        # (the rolling deploy's scale-back-up): the settle below runs on
        # three workers, not two.
        fleet["w1b"] = _spawn_fleet(pg_dsn, schema, ["w1b"])["w1b"]

        # Re-admission: with the backlog still deep, the survivors run
        # the capped queue back up to its cap AND re-fill the keyed
        # slot (the freed capacity is actually used again, not just
        # bookkept as free). The bound is generous because the reclaim
        # hands rows back on their own retry curves (the interrupted
        # rows re-pend on their policy's base) and the keyed stale row
        # costs a denial-snooze cycle before it frees, but it is finite.
        deadline = time.monotonic() + 60.0
        refilled = 0
        keyed_running = 0
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                f"SELECT count(*) FILTER (WHERE queue = 'roll_capped')::int AS q, "
                f"count(*) FILTER (WHERE actor = 'sys_keyed')::int AS k "
                f"FROM \"{schema}\".jobs WHERE status = 'running'"
            )
            assert row is not None
            refilled = row["q"]
            keyed_running = row["k"]
            if refilled >= _QUEUE_CAP and keyed_running >= 1:
                break
            await asyncio.sleep(0.1)
        assert refilled >= _QUEUE_CAP, (
            f"the freed queue-cap capacity was never re-admitted (peak {refilled}/"
            f"{_QUEUE_CAP} within 60s) - the departing pod's capacity leaked"
        )
        assert keyed_running >= 1, (
            "the freed keyed slot was never re-admitted within 60s - "
            "the departing pod's keyed capacity leaked"
        )
        print(
            f"[s7] re-admission: capped queue back to {refilled}/{_QUEUE_CAP} slots "
            f"and keyed slot re-filled after both departures"
        )

        # The stranded running rows of the corpse must still terminate:
        # the lease reclaim hands them back, the survivors finish them.
        await _settle_with_diagnostics(conn, schema, cap_secs=240.0)
        counts = await assert_balanced(conn, schema, _TAG)
        print(f"[s7] cap-churn settle counts: {counts}")
        await assert_effects_balance(conn, schema, _TAG)
        await sampler.stop_and_report("cap churn")
    finally:
        await sampler.close()
        for name in list(fleet):
            worker = fleet.get(name)
            if worker is not None and name not in drained:
                reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ── Scenario 4: the storm ────────────────────────────────────────────────


@pytest.mark.timeout(400)
async def test_rolling_release_fleet_wide_storm_leaves_a_clean_empty_fleet(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    roll_capped_queue: None,
) -> None:
    """ALL pods SIGTERM within one second: every drain runs
    concurrently, the last pod out leaves the fleet empty with the
    ledger whole, and the next boot picks the requeues up and conserves."""
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    fleet = _spawn_fleet(pg_dsn, schema, _PODS)
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    fleet2: dict[str, WorkerProc] = {}
    try:
        await _fill_fleet(conn, schema, sys_client, want_running=10)

        # The storm: every pod signalled inside one second.
        t_storm = time.monotonic()
        for name in _PODS:
            fleet[name].proc.terminate()
            await asyncio.sleep(0.15)
        signal_span = time.monotonic() - t_storm
        assert signal_span <= 1.0, f"the storm's signals took {signal_span:.2f}s"

        # Every drain runs concurrently; every pod exits rc=0 inside
        # its single shared grace (the drains overlap, the budget does
        # not multiply).
        for name in _PODS:
            rc = await asyncio.to_thread(graceful_stop, fleet[name], timeout=_GRACE + 30.0)
            drained.add(name)
            assert rc == 0, f"pod {name} exited rc={rc} under the storm"
        storm_secs = time.monotonic() - t_storm
        assert storm_secs < _GRACE + 5.0, (
            f"the whole fleet took {storm_secs:.2f}s to drain, past one shared "
            f"{_GRACE:.0f}s grace - the concurrent drains did not fit the budget"
        )
        print(
            f"[s7] storm: 5 concurrent drains measured {storm_secs:.2f}s vs "
            f"bound {_GRACE:.0f}s + 5s margin"
        )

        # The leaderless/empty-fleet end state: no running row, no live
        # slot, every requeue claimable, the ledger whole.
        limbo = await conn.fetchval(
            f"SELECT count(*)::int FROM \"{schema}\".jobs WHERE status = 'running'"
        )
        assert limbo == 0, f"{limbo} row(s) still running with the whole fleet dead"
        unclaimable = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE status NOT IN ('running') AND status::text NOT IN "
            "('succeeded','failed','cancelled','crashed') "
            "AND (scheduled_at IS NULL OR scheduled_at > statement_timestamp() + "
            f"interval '{_LOCK_LEASE + 10:.0f} seconds')"
        )
        assert unclaimable == 0, (
            f"{unclaimable} requeued row(s) are not claimable within the hold/lease "
            "bound - the die-off stranded them past every derived recovery bound"
        )
        violations = await conservation_violations(conn, schema, _TAG)
        assert not violations, "the empty-fleet ledger does not balance:\n" + "\n".join(
            violations[:20]
        )

        # The next boot: a fresh generation picks the requeues up and
        # the full die-off + restart cycle conserves.
        fleet2 = _spawn_fleet(pg_dsn, schema, ["n0", "n1"])
        counts = await assert_balanced(conn, schema, _TAG)
        await assert_effects_balance(conn, schema, _TAG)
        assert counts.get("succeeded", 0) >= 10 + 8 + 8, (
            f"the restarted fleet did not complete every job the storm requeued: {counts}"
        )
        await sampler.stop_and_report("the storm")
        for name in ("n0", "n1"):
            await asyncio.to_thread(graceful_stop, fleet2[name], timeout=_GRACE + 30.0)
    finally:
        await sampler.close()
        for name in _PODS:
            worker = fleet.get(name)
            if worker is not None and name not in drained:
                reap(worker)
        for worker in fleet2.values():
            reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ── Scenario 5: the flapping release ────────────────────────────────────


@pytest.mark.timeout(400)
async def test_rolling_release_grace_edge_flap_recovers_under_a_new_identity(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    roll_capped_queue: None,
) -> None:
    """A pod lingers at the grace edge (its body defies every cancel),
    is SIGKILLed at 95% of its termination budget with the drain
    unfinished; a NEW pod with a NEW identity picks the row up inside
    the hold/lease bound. No main path assumes identity persistence."""
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    from tests.system_e2e.actors import sys_defiant

    fleet = _spawn_fleet(pg_dsn, schema, _PODS[:3])
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    fresh: dict[str, WorkerProc] = {}
    try:
        ids = await _worker_ids(conn, schema, fleet)

        # One defiant job: it will still be running when the flap kills
        # its pod (the body outlasts the 95%-of-grace kill), plus
        # ordinary backlog for the survivors to carry.
        defiant_handle = await sys_client.enqueue(sys_defiant, SysPayload(sleep=20.0), tags=[_TAG])
        deadline = time.monotonic() + 30.0
        defiant = defiant_handle.job_id
        holder_id: str | None = None
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                f'SELECT locked_by_worker::text AS wid FROM "{schema}".jobs '
                "WHERE id = $1 AND status = 'running'",
                defiant,
            )
            if row is not None and row["wid"] is not None:
                holder_id = row["wid"]
                break
            await asyncio.sleep(0.1)
        assert holder_id is not None, "the defiant job never started - premise void"
        victim = next(name for name, wid in ids.items() if wid == holder_id)
        for _ in range(4):
            await sys_client.enqueue(sys_slow, SysPayload(sleep=0.3), tags=[_TAG])
        # The flap: SIGTERM, let the drain run to 95% of the budget
        # (the defiant body absorbs CANCELLING and FORCING, so the pod
        # is at the grace edge with the drain unfinished), then SIGKILL.
        fleet[victim].proc.terminate()
        await asyncio.sleep(0.95 * _GRACE)
        t_kill = time.monotonic()
        fleet[victim].proc.kill()
        await asyncio.to_thread(fleet[victim].proc.wait, 30)
        drained.add(victim)
        assert time.monotonic() - t_kill < 1.0

        # The rest of the OLD generation goes down WITH it (the deploy
        # rolled the whole generation): each survivor's drain runs in
        # the defiant body's teeth - the body defies those cancels too,
        # so the drain releases the row it holds with a fresh hold and
        # exits graceful while the body lingers behind it. Whatever the
        # flap released, the OLD generation's exits re-release with a
        # hold no old pod can outlive.
        for name in [n for n in ids if n != victim]:
            fleet[name].proc.terminate()
        for name in [n for n in ids if n != victim]:
            rc = await asyncio.to_thread(graceful_stop, fleet[name], timeout=_GRACE + 60.0)
            drained.add(name)
            assert rc == 0, f"survivor {name} exited rc={rc}"
            await _assert_corpse_owns_nothing(conn, schema, ids[name], f"survivor {name}")

        # A NEW pod with a NEW identity (kubernetes restart semantics):
        # nothing in the main path may assume the corpse returns. Two
        # pods boot into the empty fleet and own every row the old
        # generation's die-off left behind.
        fresh = _spawn_fleet(pg_dsn, schema, ["fresh0", "fresh1"])
        fresh_ids = await _worker_ids(conn, schema, fresh)
        assert all(fid != holder_id for fid in fresh_ids.values()), (
            "a new pod reused the corpse's identity"
        )

        # The defiant row: released with a hold (or stranded running
        # and locked if the kill beat the drain) - either way the
        # pickup bound is the hold/lease lapse + the drain tail + boot
        # + poll. The NEW identity must be the claimant: with the old
        # generation gone, only a fresh pod can hold it.
        pickup_bound = _LOCK_LEASE + _LEADER_LEASE + _GRACE + 20.0
        deadline = time.monotonic() + pickup_bound
        picked_by: str | None = None
        while time.monotonic() < deadline:
            row = await conn.fetchrow(
                f"SELECT locked_by_worker::text AS wid, attempt::int AS attempt "
                f"FROM \"{schema}\".jobs WHERE id = $1 AND status = 'running'",
                defiant,
            )
            if row is not None and row["wid"] in (fresh_ids["fresh0"], fresh_ids["fresh1"]):
                picked_by = row["wid"]
                break
            await asyncio.sleep(0.2)
        pickup_secs = time.monotonic() - t_kill
        state = await conn.fetchrow(
            f"SELECT status::text AS status, attempt::int AS attempt, scheduled_at, "
            f"locked_by_worker::text AS wid, lock_expires_at, interrupt_count::int AS interrupts "
            f'FROM "{schema}".jobs WHERE id = $1',
            defiant,
        )
        attempts = await conn.fetch(
            f"SELECT attempt, outcome, started_at, finished_at, worker_id::text AS wid "
            f'FROM "{schema}".job_attempts WHERE job_id = $1 ORDER BY attempt',
            defiant,
        )
        effects = await conn.fetch(
            f'SELECT attempt, kind, at FROM "{schema}".sys_effects WHERE job_id = $1 ORDER BY at',
            defiant,
        )
        assert picked_by is not None, (
            f"the new pod never picked the flapped row up within {pickup_bound:.0f}s "
            f"(measured {pickup_secs:.1f}s) - the grace-edge kill stranded it; "
            f"row state: {dict(state) if state else None}; attempts: "
            f"{[dict(r) for r in attempts]}; effects: {[dict(r) for r in effects]}"
        )
        print(
            f"[s7] grace-edge flap: the new identity picked the row up "
            f"{pickup_secs:.2f}s after the SIGKILL vs bound {pickup_bound:.0f}s"
        )

        # The corpse owns nothing anywhere: no running row, no live
        # slot, once the new attempt is under way the corpse's marks
        # are gone from every main surface.
        await _wait_corpse_slots_freed(conn, schema, holder_id)
        corpse_rows = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE status = 'running' AND locked_by_worker = $1::uuid",
            holder_id,
        )
        assert corpse_rows == 0, "the corpse still owns a running row"

        # The defied attempts: the body recorded what actually ran. The
        # effects ledger reconciles per (job, attempt): no attempt ran
        # twice, every run has a claim row behind it.
        counts = await assert_balanced(conn, schema, _TAG)
        await assert_effects_balance(conn, schema, _TAG)
        assert counts.get("succeeded", 0) >= 5, (
            f"the fleet did not complete every job it admitted: {counts}"
        )
        await sampler.stop_and_report("the grace-edge flap")
    finally:
        await sampler.close()
        for name in [*_PODS[:3]]:
            worker = fleet.get(name)
            if worker is not None and name not in drained:
                reap(worker)
        for worker in fresh.values():
            reap(worker)
        await delete_tagged(conn, schema, _TAG)

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
   after clean release, or nothing by the bounded lease/sweep reclaim
   after a failed release (no running row, no live slot).
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
import json
import logging
import signal
import sys
import time
import traceback
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from tests._wf_fixtures import seed_flow, seed_running_node
from tests.system_e2e._harness import (
    BOOT_READY_BOUND_S,
    LOCK_LEASE_S,
    SWEEP_INTERVAL_S,
    TERMINATION_GRACE_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    _tee_stderr,
    graceful_stop,
    reap,
    spawn_joined_worker,
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

_LOG = logging.getLogger(__name__)

_TAG = "sys-s7"

#: The scenario fleet: five pods, every one consuming both queues.
_PODS = ["w0", "w1", "w2", "w3", "w4"]
_QUEUES = "system_e2e,roll_capped"

#: The queue-cap fleet pin: the capped queue admits 1 fleet-wide.
#:
#: Why 1, not 2: the drain scenarios' exercise proof is the durable
#: evidence that the cap was actually admitted through, and a cap of 1
#: makes that evidence deterministic - saturation is any single capped
#: claim, so the proof cannot miss it by runner speed or sampler
#: cadence. At 2 the proof needed two capped claims to coincide inside
#: a 2s body, a throughput property the loaded CI runner voided (CI run
#: 37092146919 attempt 1: ``peak.capped_queue`` = 1 across a whole
#: 13-minute scenario - "the queue cap was never exercised - the
#: sampler pins are vacuous" - on a fleet that was serving fine). The
#: never-exceed pins keep (sharpen, in fact) their teeth at 1: any
#: over-admission above one concurrent capped job is a breach, sampled
#: from durable rows for the whole scenario.
_QUEUE_CAP = 1

#: The per-worker dispatch cap (the harness's TASKQ_MAX_CONCURRENCY).
_WORKER_CAP = 4

#: The harness knobs the bounds below derive from — imported, not
#: duplicated (tests/system_e2e/_harness.py's tier constants are the
#: single source _BASE_ENV itself derives from), so a cadence move
#: (the operational-loop family's 2s/24s shape) moves these bounds with
#: it instead of silently desyncing the file from its own fleet.
#: `_SWEEP_INTERVAL` is the leader sweep cadence, `_POLL_FLOOR` the poll
#: floor the scenarios budget; `_LEADER_LEASE` stays local (this module's
#: scenarios set it explicitly for the SIGKILL failover bounds).
_GRACE = TERMINATION_GRACE_S
_LOCK_LEASE = LOCK_LEASE_S
_SWEEP_INTERVAL = SWEEP_INTERVAL_S
_POLL_FLOOR = 1.0

#: The keyed cap's slot lease (actors.py _TENANT_SLOT_LEASE).
_KEYED_LEASE = 8.0

#: The leader lease the scenario workers boot with. sized for the loaded
#: host, not the unloaded one: the leader's renew check runs once per
#: leader-loop iteration, and a loaded iteration serially pays every
#: sweep's dispatcher budget (~8 x 0.5s) before it renews. A lease under
#: that bound churns leadership forever - observed at lease 4.0 under
#: co-tenancy: trust_expired every ~4.2s, 62 demotions in one scenario,
#: the leader-only scheduled->pending promotion starving the whole time,
#: rows due 264s never dispatched, the population never settling. 20s =
#: the loaded iteration bound (~4s) x the tier's load stretch (2) plus
#: failover headroom; the corpse bounds below derive from this name, so
#: the failover waits they budget follow it.
_LEADER_LEASE = 20.0
_CORPSE_SLOT_BOUND = (
    _LOCK_LEASE
    + _LEADER_LEASE
    + _SWEEP_INTERVAL
    + _POLL_FLOOR
    + _LOCK_LEASE
    + _LOCK_LEASE
    + 5.0
    + 2.0
) * TIER_LOAD_STRETCH
_SWEEP_BOUND = (_LEADER_LEASE + _SWEEP_INTERVAL + _POLL_FLOOR) * TIER_LOAD_STRETCH

#: The corpse-selection hang guard: one slot turnover (the capped body's
#: 30s sleep + the claim poll) plus the co-tenancy stretch. This bounds
#: the wait for a survivor to re-acquire cap capacity - a hang guard in
#: the dd4572ff doctrine, not a bet on the race firing; only a fleet
#: that never re-acquires (the genuine capacity-leak defect) reds here.
_CORPSE_PREMISE_BOUND = (
    30.0 + _POLL_FLOOR + _LOCK_LEASE + _LEADER_LEASE + _SWEEP_INTERVAL
) * TIER_LOAD_STRETCH

#: The mid-drain probe's co-tenancy margin: one claim cycle (the poll
#: floor) stretched by the 20x co-tenancy factor these runners measure
#: between a seed and its observation (dd4572ff's stall band; the same
#: 20x stretch test_cancel_storm's _STORM_DEADLINE_SECS derives from):
#: 1.0 s x 20 = 20 s. The probe assertion demands an effect inside the
#: measured drain window plus this margin, because the whole probe chain
#: crosses the same runner weather - the starved test process's enqueue
#: round trips, a survivor's claim poll, the 0.05s body, the effect
#: write - and a ~3 s window carries no probe whenever the weather eats
#: one link, even on a fleet that is serving fine (the CI red: w0's
#: 2.97 s drain, all four probes settled well inside their 30 s cap,
#: zero effects inside the raw window). It bounds failure only: probes
#: absent for window + margin red, and _settle_probes's 30 s cap reds a
#: fleet that never serves at all.
_PROBE_STRETCH = 20.0
_PROBE_STALL_MARGIN = _POLL_FLOOR * _PROBE_STRETCH

#: The CapSampler's close-join hang guard: one tick's two queries plus
#: the 0.1s cadence, stretched by the 20x co-tenancy factor these
#: runners actually measure (_PROBE_STRETCH, the same band this file's
#: other derived bounds price - not the tier's 2.0 progress factor,
#: which cannot tell a slow tick from a hung one at this file's own
#: weather). Say it plainly: under loaded weather a legitimate tick can
#: take multiples of its healthy-weather time, so this bound is priced
#: so a legitimate tick stays under it at 20x stretch and the fallback
#: below fires only on a genuine hang - a backend wedged past 20
#: seconds. The fallback cancel can land mid-query, and the fallback's
#: own task reap (CapSampler.close) is what keeps the cancel's asyncpg
#: Connection._cancel task off the module loop (the two shard reds'
#: leak class, see the close's docstring).
_SAMPLER_JOIN_BOUND_S = (_SWEEP_INTERVAL + _POLL_FLOOR) * _PROBE_STRETCH

#: The flap scenario's premise hang guard: one claim cycle (the poll
#: floor plus the leader's dispatch tick) stretched by the 20x co-tenancy
#: factor - the same shape the #651 fix gave this file's premise waits
#: (a hang guard only: the defiant job's first claim is near-instant on a
#: healthy fleet, and a fleet that never claims reds here instead of
#: hanging). Re-anchored from a bare 30.0 so the guard derives from the
#: file's own stall-band factors.
_FLAP_PREMISE_BOUND_S = (_POLL_FLOOR + _SWEEP_INTERVAL) * _PROBE_STRETCH


# ── Fixtures and fleet plumbing ──────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def roll_capped_queue(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> AsyncGenerator[None, None]:
    """The capped queue row, before any pod boots.

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


async def _spawn_joined_fleet(
    conn: asyncpg.Connection, pg_dsn: str, schema: str, names: list[str]
) -> dict[str, WorkerProc]:
    """Boot the named pods to the JOINED-fleet standard: every pod's row
    registered and its heartbeat advancing, ghosts reaped and respawned
    (``_harness.spawn_joined_worker`` - the operator's restart remedy).
    The plain ``_spawn_fleet`` stays for the call sites whose scenario
    owns the fleet's lifecycle afterward; the scenario-opening spawns go
    through here, so a co-tenancy reaping window (3s at the tier's own
    beat) can never strand a scenario on a pod that booted green but
    never joined."""
    fleet: dict[str, WorkerProc] = {}
    for name in names:
        fleet[name] = await spawn_joined_worker(
            conn,
            pg_dsn,
            schema,
            tag=f"roll-{name}",
            extra_env={
                "TASKQ_QUEUES": _QUEUES,
                "TASKQ_LEADER_LEASE": str(_LEADER_LEASE),
            },
        )
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
    The cap is the harness's own boot-readiness bound: registration is
    the boot's last DB step past the socket bind, so the same window
    that bounds readiness bounds the row.
    """
    deadline = time.monotonic() + BOOT_READY_BOUND_S
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


def _is_asyncpg_cancel_task(task: asyncio.Task[object]) -> bool:
    """Whether the task's coroutine is asyncpg's ``Connection._cancel``.

    The only fire-and-forget mint the sampler's paths can produce
    (``asyncpg/connection.py``'s ``_cancel_current_command``:
    ``self._cancellations.add(self._loop.create_task(self._cancel(waiter)))``,
    fired when a query waiter is cancelled mid-wire). The close's reap
    is scoped to exactly this class: anything else still pending after
    close() is not this reap's to swallow - the conftest loop-leak guard
    names it loudly instead (the fail-loudly doctrine), so a broad,
    globally-scoped reap here could mask a future mint class the guard
    exists to catch. Both the coroutine qualname and the code object's
    file are checked, so a coincidental ``_cancel`` method on some other
    module's Connection class cannot match.
    """
    coro = task.get_coro()
    code = getattr(coro, "cr_code", None)
    return (
        getattr(coro, "__qualname__", "") == "Connection._cancel"
        and code is not None
        and code.co_filename.endswith("asyncpg/connection.py")
    )


class CapSampler:
    """Continuously tally every running row against the distributed caps.

    Three pins, sampled every 100 ms for the WHOLE scenario:

    * the queue cap: running rows on the capped queue never exceed the
      queue's max_concurrent, no matter which pod holds them or which
      pod is mid-drain;
    * the keyed cap: running rows of the keyed actor never exceed one
      per tenant, across the whole fleet;
    * the worker cap: a pod never runs more than its max_concurrency.

    The sampler also records each cap's PEAK. The peaks are diagnostics
    only, on both surfaces - the exercise proof lives elsewhere, and
    neither surface's peak is pinned:

    * the JOBS surface is durable: a running row lives for its body's
      whole duration, but a PEAK over ticks still needs a tick to land
      inside a running window, and the starved test process's sparse
      ticks cannot promise one - at the retired cap of 2 the loaded CI
      runner read ``peak.capped_queue`` = 1 across a whole 13-minute
      scenario (observed on CI, attempt 1) while the fleet served
      fine. The exercise proof is therefore the RECEIPT this sampler
      records across the WHOLE scenario and the scenarios assert at
      their tail (``assert_cap_receipt``): a running row on the capped
      queue WITH a live-held slot on the cap's own bucket in the same
      pass - the cap is 1 fleet-wide, so the pair is the admission's
      receipt, and observed-at-least-once is a state no runner speed
      can hide. The retired front-loaded poll spent a bounded budget
      (one stretched claim cycle) on that observation inside the
      scenario's first seconds, and red a healthy fleet whose first
      admitted claim arrived past it; the tail record has the whole
      scenario - thousands of ticks - to see a state that persists for
      every admitted claim's body, so only a cap that never admits
      (the unregistered reservation, the dead admission path) stays
      unseen;
    * the SLOTS surface is transient: a slot flips free at every
      claim/release turnover and the lease predicate excludes the
      renewal gaps, so a 100 ms tick witnesses a LOWER BOUND of the
      true peak and, under runner load, the sparse ticks land in the
      turnover gaps - observed ``peak.slots:...:roll_capped`` = 1 >= 2
      on a loaded box while CI stayed green on the same sha: the
      sampler caught a gap, not a cap defect. Recorded, never pinned -
      the cap's enforcement teeth are the never-exceed checks, which a
      slot over-hold trips on any tick, and the cap-churn scenario's
      wait-until DB polls re-prove the exercise under churn and its
      corpse-slot checks query ``reservation_slots`` directly.

    The never-exceed checks keep their teeth: a sample ABOVE the bound
    is still a violation, and no weather can hide one.
    """

    def __init__(self, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema
        self._conn: asyncpg.Connection | None = None
        self.stop = asyncio.Event()
        self.violations: list[str] = []
        self.peak: dict[str, int] = {}
        self.samples = 0
        #: The cap-exercise receipt: the sample count at which a running
        #: row on the capped queue first coincided with a live-held slot
        #: on the cap's own bucket, in the same pass over the durable
        #: rows. None until seen; ``assert_cap_receipt`` owns the assert.
        self.cap_receipt_at: int | None = None
        self._task: asyncio.Task[None] | None = None
        #: Every task the close's reap reaped, as ``"<task name> coro=<coro>"``
        #: - the reap's count and its evidence in one surface (the reap is
        #: loud, never silent; the forced-fallback pin asserts on this).
        self.reaped_tasks: list[str] = []

    async def _tick(self) -> None:
        if self._conn is None:
            self._conn = await asyncpg.connect(self._dsn)
        # ONE statement = ONE observable state: the jobs' view and the
        # slots' view are two subqueries of a single SELECT, so they read
        # the same READ COMMITTED instant. The old pair of statements (an
        # await between them) could see post-completion slots after
        # pre-completion jobs and miss a real admission's receipt; a
        # single statement's snapshot cannot tear, and a failed tick
        # leaves no aborted transaction to poison the next one.
        snapshot_row = await self._conn.fetchrow(
            f"SELECT json_build_object("
            f"'jobs', (SELECT coalesce(json_agg(json_build_object("
            f"'actor', j.actor::text, 'queue', j.queue::text, "
            f"'tenant', j.payload->>'tenant', 'holder', j.locked_by_worker::text"
            f")), '[]'::json) FROM \"{self._schema}\".jobs j WHERE j.status = 'running'), "
            f"'slots', (SELECT coalesce(json_agg(x), '[]'::json) FROM ("
            f"SELECT bucket_name::text AS bucket, count(*)::int AS held "
            f'FROM "{self._schema}".reservation_slots '
            f"WHERE job_id IS NOT NULL AND lease_expires_at >= statement_timestamp() "
            f"GROUP BY 1) x)"
            f") AS snapshot"
        )
        assert snapshot_row is not None
        snapshot = json.loads(snapshot_row["snapshot"])
        rows = snapshot["jobs"]
        slots = snapshot["slots"]
        self.samples += 1
        # hard: the slot surface. The queue-cap bucket's slot count is
        # the queues row's max_concurrent; a keyed tenant bucket's is
        # the ref's slots (1 here).
        for row in slots:
            bucket = row["bucket"]
            if bucket == "taskq:global:queue:roll_capped":
                over_hold = row["held"] > _QUEUE_CAP
                details: list[dict[str, object]] | None = None
                if over_hold:
                    try:
                        details = await self._slot_details(bucket)
                    except Exception as exc:
                        # The violation is already observed: a failed
                        # diagnostic must never stop the cap record from
                        # landing.
                        details = [
                            {"bucket": bucket, "held": row["held"], "detail_error": repr(exc)}
                        ]
                self._record(f"slots:{bucket}", row["held"], _QUEUE_CAP, rows=details)
            elif bucket.startswith("roll-tenant:"):
                over_hold = row["held"] > 1
                details_tenant: list[dict[str, object]] | None = None
                if over_hold:
                    try:
                        details_tenant = await self._slot_details(bucket)
                    except Exception as exc:
                        details_tenant = [
                            {"bucket": bucket, "held": row["held"], "detail_error": repr(exc)}
                        ]
                self._record(f"slots:{bucket}", row["held"], 1, rows=details_tenant)
        # damped: the job surface, at the fleet's claim width. This
        # surface counts claimed rows, and a claim the cap then denies
        # stays 'running' while its denial-retry loop re-probes (up to
        # the ~0.8s local budget, per _acquire_for_actor_with_denial_
        # retry), so the surface legitimately reads above the queue
        # cap's admitted count - observed 6 running capped rows at a
        # cap of 1 (1 admitted + 5 denied re-probers, steady across a
        # 2s sample span). Its honest ceiling is therefore the claim
        # width - every consumer of every pod holding one queued claim
        # - and the cap's own teeth live on the slots surface above
        # (a denier holds no slot; an over-admission holds cap+1) plus
        # the exercise proof's slot receipt.
        capped = [r for r in rows if r["queue"] == "roll_capped"]
        # The cap-exercise receipt, same pass over the durable rows: a
        # running row on the capped queue and a live-held slot on the
        # cap's own bucket. The slot is the cap machinery's own write
        # (a denied re-prober holds none), so the pair proves the
        # admission went through the cap. Recorded once, asserted at
        # the scenario tail - a state, not a timing bet.
        cap_bucket_held = next(
            (r["held"] for r in slots if r["bucket"] == "taskq:global:queue:roll_capped"), 0
        )
        if (
            self.cap_receipt_at is None
            and len(capped) >= _QUEUE_CAP
            and cap_bucket_held >= _QUEUE_CAP
        ):
            self.cap_receipt_at = self.samples
        self._record("capped_queue", len(capped), len(_PODS) * _WORKER_CAP, rows)
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

    async def _slot_details(self, bucket: str) -> list[dict[str, object]]:
        """The over-hold's evidence: the exact bucket's slot rows in this
        schema, with the held job's state joined (a LEFT JOIN, so a slot
        whose job is gone names a null status - the shape a leaked test
        slot shows). Diagnostic only: the snapshot's count already made
        the violation decision, so this fetch cannot change it."""
        conn = self._conn
        assert conn is not None, "the tick's connection must exist to fetch details"
        rows = await conn.fetch(
            f"SELECT s.slot_index, s.job_id::text AS job_id, "
            f"s.held_by_worker_id::text AS held_by_worker_id, s.acquired_at, "
            f"s.lease_expires_at, "
            f"(s.lease_expires_at >= statement_timestamp()) AS lease_live, "
            f"j.status::text AS job_status, "
            f"j.locked_by_worker::text AS job_holder, "
            f"j.queue::text AS job_queue "
            f'FROM "{self._schema}".reservation_slots s '
            f'LEFT JOIN "{self._schema}".jobs j ON j.id = s.job_id '
            f"WHERE s.bucket_name = $1 AND s.job_id IS NOT NULL "
            f"ORDER BY s.slot_index",
            bucket,
        )
        return [dict(r) for r in rows]

    def _record(
        self,
        cap: str,
        observed: int,
        bound: int,
        rows: Sequence[Mapping[str, object]] | None = None,
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
        """Stop the sampler and leave the module loop clean.

        Three steps, each with a reason:

        * JOIN the NATURAL EXIT first (no cancel): the tick loop checks
          the stop flag between ticks, so the join costs at most one
          tick + the 0.1s cadence - and no cancel ever lands MID-QUERY.
          That is the leak class the two shard reds share (the PG16
          shard's overlapping-pairs error-at-teardown
          and the PG18 shard's grace-edge flap): a
          cancel meeting a query in flight on the wire makes asyncpg
          mint a fire-and-forget ``Connection._cancel`` task (a fresh
          server connection whose whole life is the PG cancel request),
          and a task left pending on the module loop at test end is the
          conftest loop-leak guard's error - on a fast box the task
          heals inside the teardown's own round trips (the rerun-green
          weather ruling), on a loaded runner the guard's snapshot
          catches it and the SHARD errors.
        * The join is bounded (a hang guard: a backend hung past it is
          the only thing that can hold a tick this long); the fallback
          cancel there CAN still land mid-query, so:
        * REAP what the close itself minted: the pending-task diff
          against the close's entry, SCOPED to asyncpg's
          ``Connection._cancel`` (the only fire-and-forget mint these
          paths produce - see ``_is_asyncpg_cancel_task``), is awaited
          off the loop (bounded, then cancelled - asyncpg's ``_cancel``
          suppresses its own cancellation), so even the fallback leaves
          no task behind on the module loop. The reap is LOUD: every
          reaped task is logged by name + coro and counted on the
          instance (``reaped_tasks``). Anything ELSE still pending after
          close() is deliberately left for the conftest loop-leak guard
          to name - the fail-loudly doctrine, not a mask. (On a fast
          box the mint often finishes inside the ``conn.close()`` round
          trips before the reap's diff runs - the reap is the
          GUARANTEE it is awaited, not a bet on the healing.)
        * The reap lives in a ``finally``: if close() ITSELF is
          cancelled mid-body (e.g. during the bounded ``conn.close()``
          await, whose ``contextlib.suppress(Exception)`` does not
          swallow ``CancelledError`` on 3.8+), the reap still runs and
          reaps the fallback's mint before the cancellation propagates.
          The one window that remains is a SECOND cancellation landing
          inside the reap's own awaits; nothing in the suite cancels
          close(), so that residue is latent and accepted.
        """
        minted_baseline = set(asyncio.all_tasks())
        self.stop.set()
        try:
            if self._task is not None and not self._task.done():
                done, _pending = await asyncio.wait({self._task}, timeout=_SAMPLER_JOIN_BOUND_S)
                if not done:
                    self._task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await self._task  # Why: teardown; the tally already recorded what it saw.
            if self._conn is not None and not self._conn.is_closed():
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._conn.close(), timeout=5.0)
                self._conn = None
        finally:
            # The reap: whatever the close's window minted (the
            # fallback's mid-query cancel - the shard-red leak class),
            # awaited off the loop even if close() itself is being
            # cancelled. Scoped, loud, counted - see the method.
            await self._reap_minted_cancels(minted_baseline)

    async def _reap_minted_cancels(self, minted_baseline: set[asyncio.Task[object]]) -> None:
        """Await off the loop every mint still pending since the baseline.

        SCOPED to asyncpg's ``Connection._cancel`` (see
        ``_is_asyncpg_cancel_task`` for why the scope stops there) and
        LOUD: every reaped task is logged by name + coro and appended to
        ``reaped_tasks`` - the count is the forced-fallback pin's
        deterministic evidence the reap ran on a mint, and the log is
        the operator's record when a runner's weather makes the mint
        outlive close's own round trips.
        """
        reaps = {
            task
            for task in set(asyncio.all_tasks()) - minted_baseline - {asyncio.current_task()}
            if _is_asyncpg_cancel_task(task)
        }
        for task in reaps:
            record = f"{task.get_name()} coro={task.get_coro()}"
            self.reaped_tasks.append(record)
            _LOG.warning("CapSampler.close reaped a minted task: %s", record)
        if reaps:
            _done, pending = await asyncio.wait(reaps, timeout=5.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=5.0)

    async def stop_and_report(self, label: str) -> None:
        await self.close()
        assert not self.violations, (
            f"{label}: the distributed caps were breached under the churn:\n"
            + "\n".join(self.violations[:20])
        )

    async def assert_cap_receipt(self, label: str) -> None:
        """The exercise ASSERT, anchored at the scenario tail.

        Requires the cap's admission receipt to have been observed at
        least once across the WHOLE scenario: a running row on the
        capped queue coinciding with a live-held slot on the cap's own
        bucket, in one pass over the durable rows (same-pass semantics
        as the retired front poll - the slot is the cap machinery's own
        write, so the pair cannot be produced by a claim the cap
        denied). This is state, not time: an admission that happened
        persists in the durable rows for its body's whole dwell and the
        sampler ticks every 100ms for the whole scenario, so no runner
        speed can hide a receipt that was there to see - while a cap
        that never admits (the reverted registration: no slot rows ever
        exist; the dead admission path) is unseen across every one of
        the scenario's ticks and reds here. That is the same
        state-not-time shape the scenario probes' settle asserts use,
        and it is what keeps this sampler's never-exceed checks
        non-vacuous.
        """
        if self.cap_receipt_at is not None:
            return
        try:
            conn = self._conn
            assert conn is not None
            jobs = await conn.fetch(
                f"SELECT id::text AS id, status::text AS status, attempt, "
                f"claim_epoch, locked_by_worker::text AS holder, started_at, "
                f'lock_expires_at, scheduled_at FROM "{self._schema}".jobs '
                "WHERE queue = 'roll_capped' ORDER BY created_at, id",
            )
            cap_bucket = "taskq:global:queue:roll_capped"
            slots = await self._slot_details(cap_bucket)
            report = f"capped jobs={jobs!r}; cap slots={slots!r}"
        except Exception as exc:
            report = f"failed to collect cap diagnostics: {exc!r}"
        assert self.cap_receipt_at is not None, (
            f"{label}: the queue cap's admission receipt never appeared in "
            f"{self.samples} samples across the WHOLE scenario - no live-held "
            f"slot on the cap's own bucket ever coincided with a running row "
            f"on the capped queue (peaks: {self.peak}): the capped queue's "
            f"admission machinery is dead or unregistered and the sampler's "
            f"never-exceed pins would be vacuous; {report}"
        )


_RELEASE_FALLBACK_MARKERS = (
    "release-pg-write-failed",
    "shutdown-release-failed",
    "terminal-write-failed",
)


def _log_stamp(line: str) -> datetime | None:
    """The console record's leading ISO stamp, parsed with
    ``datetime.fromisoformat``: the product's TimeStamper stamps ISO UTC
    (``2026-11-07T09:35:09.123456Z``), and the worker's log clock and
    the database's clock are the same host clock (the containers run
    UTC). A naive stamp, an unparseable one, or a missing one is never
    evidence."""
    token = line.split(" ", 1)[0]
    try:
        stamp = datetime.fromisoformat(token)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        return None
    return stamp


def _log_line_evidences(line: str, job_id: str, claim_floor: datetime) -> bool:
    """One log line is a row's release-fallback evidence when it names a
    fallback marker AND that row's job id AND carries a parseable ISO
    stamp from the current attempt's claim instant or later (the same
    host clock as the database's claim stamp). An older attempt's
    record is seconds or minutes older and rejected; an unparseable,
    naive, or missing stamp is never evidence (fail closed), and so is
    a row with no claim stamp at all."""
    if not any(marker in line for marker in _RELEASE_FALLBACK_MARKERS):
        return False
    if job_id not in line:
        return False
    stamp = _log_stamp(line)
    if stamp is None:
        return False
    return stamp >= claim_floor


async def _assert_corpse_owns_nothing(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: str,
    label: str,
    worker: WorkerProc | None = None,
    *,
    bound: float | None = None,
) -> None:
    """No queue left owned by a corpse: no running row locked by it, and
    no reservation slot (queue-cap or keyed) live-held by it.

    The postcondition has two shapes. A clean release clears both
    surfaces and the immediate fast path passes. The product also
    documents a release fallback: a shutdown release write can fail or
    be skipped, intentionally leaving running/locked rows for
    lease-expiry reclaim (the shutdown release's own error path), and
    probe settlement does not settle corpse rows. A running row is
    waited out ONLY with row-specific evidence in that worker's own
    log: a ``release-pg-write-failed``, ``shutdown-release-failed``, or
    ``terminal-write-failed`` record naming that job id. A residual
    running row without its evidence fails immediately - a wait here
    would mask a release miss - and every row in a mixed group needs
    its own evidence; one marker does not excuse siblings. The wait's
    bounds are the module's derived corpse arithmetic: a running row's
    own bound is its observed lock expiry plus one sweep tick and the
    poll floor, stretched by the tier's load factor (a missing
    lock_expires_at on a held row fails immediately); slot-only residue
    keeps the corpse slot bound (a stale slot row may be reused by a
    survivor, so the slot condition is a LIVE lease, never the row's
    existence). On any bound's expiry the offending rows and slots are
    reported and the assertion fails: the wait never masks a leak
    without evidence."""
    slot_deadline = time.monotonic() + (bound if bound is not None else _CORPSE_SLOT_BOUND)
    log_lines = _worker_log_lines(worker)
    row_deadline: float | None = None
    while True:
        rows, slots, stamp = await _corpse_ownership(conn, schema, worker_id)
        if rows == 0 and slots == 0:
            return
        if rows:
            residual = await conn.fetch(
                f"SELECT id::text AS id, actor, status::text AS status, attempt, "
                f"locked_by_worker::text AS holder, lock_expires_at, scheduled_at, "
                f'started_at, interrupt_count FROM "{schema}".jobs '
                "WHERE status = 'running' AND locked_by_worker = $1::uuid",
                worker_id,
            )
            unevidenced: list[dict[str, Any]] = []
            latest_expiry: datetime | None = None
            for residual_row in residual:
                if residual_row["lock_expires_at"] is None:
                    raise AssertionError(
                        f"{label}: the running row {residual_row['id']} holds no "
                        "lock expiry, so its reclaim bound cannot be derived - "
                        f"{await _corpse_ownership_report(conn, schema, worker_id)}"
                    )
                claim_stamp = residual_row["started_at"]
                evidenced = claim_stamp is not None and any(
                    _log_line_evidences(
                        line, residual_row["id"], claim_stamp.replace(microsecond=0)
                    )
                    for line in log_lines
                )
                if evidenced:
                    if latest_expiry is None or residual_row["lock_expires_at"] > latest_expiry:
                        latest_expiry = residual_row["lock_expires_at"]
                else:
                    unevidenced.append(dict(residual_row))
            if unevidenced:
                raise AssertionError(
                    f"{label}: {len(unevidenced)} running row(s) stayed locked by "
                    "the worker that exited rc=0 with no release-fallback evidence "
                    "in its log (release-pg-write-failed / shutdown-release-failed "
                    "/ terminal-write-failed naming the job id) - a release miss "
                    "is not waited out: "
                    f"{await _corpse_ownership_report(conn, schema, worker_id)}"
                )
            if row_deadline is None:
                assert latest_expiry is not None
                horizon = (
                    (latest_expiry - stamp).total_seconds() + _SWEEP_INTERVAL + _POLL_FLOOR
                ) * TIER_LOAD_STRETCH
                row_deadline = time.monotonic() + max(horizon, 1.0)
            if time.monotonic() >= row_deadline:
                raise AssertionError(
                    f"{label}: the evidenced release-fallback row(s) did not clear "
                    f"within their own lock-expiry-derived bound - "
                    f"{await _corpse_ownership_report(conn, schema, worker_id)}"
                )
        elif time.monotonic() >= slot_deadline:
            raise AssertionError(
                f"{label}: the departed pod's slots did not clear within "
                f"{bound if bound is not None else _CORPSE_SLOT_BOUND:.0f}s (the "
                f"slot lease and the reclaim cycle, x the tier's load stretch "
                f"{TIER_LOAD_STRETCH}) - "
                f"{await _corpse_ownership_report(conn, schema, worker_id)}"
            )
        await asyncio.sleep(0.2)


def _worker_log_lines(worker: WorkerProc | None) -> list[str]:
    """The worker's own flushed log lines, read-only: the explicit sink
    when one was given, else the harness's tee file next to the health
    socket. The process has already exited (rc observed at the call
    sites), so its stderr is closed and the file is final."""
    if worker is None:
        return []
    path = worker.log_path or f"{worker.sock_path}.worker.log"
    try:
        with open(path, errors="replace") as log:
            return log.read().splitlines()
    except OSError:
        return []


async def _corpse_ownership(
    conn: asyncpg.Connection, schema: str, worker_id: str
) -> tuple[int, int, datetime]:
    """One read of both ownership surfaces: the running rows the corpse
    still locks, the reservation slots it still live-holds, and the
    database clock the lease comparison ran on."""
    row = await conn.fetchrow(
        f'SELECT (SELECT count(*)::int FROM "{schema}".jobs '
        "WHERE status = 'running' AND locked_by_worker = $1::uuid) AS running_rows, "
        f'(SELECT count(*)::int FROM "{schema}".reservation_slots '
        "WHERE held_by_worker_id = $1::uuid AND job_id IS NOT NULL "
        "AND lease_expires_at >= statement_timestamp()) AS live_slots, "
        "statement_timestamp() AS stamp",
        worker_id,
    )
    assert row is not None
    return int(row["running_rows"]), int(row["live_slots"]), row["stamp"]


async def _corpse_ownership_report(conn: asyncpg.Connection, schema: str, worker_id: str) -> str:
    """The timeout's evidence: every offending row and slot, named."""
    rows = await conn.fetch(
        f"SELECT id::text AS id, actor, status::text AS status, attempt, "
        f"locked_by_worker::text AS holder, lock_expires_at, scheduled_at, "
        f'started_at, interrupt_count FROM "{schema}".jobs '
        "WHERE status = 'running' AND locked_by_worker = $1::uuid",
        worker_id,
    )
    slots = await conn.fetch(
        f"SELECT bucket_name, slot_index, job_id::text AS job_id, "
        f"held_by_worker_id::text AS holder, acquired_at, lease_expires_at, "
        f"lease_expires_at >= statement_timestamp() AS lease_live "
        f'FROM "{schema}".reservation_slots '
        "WHERE held_by_worker_id = $1::uuid AND job_id IS NOT NULL",
        worker_id,
    )
    now = await _db_now(conn)
    return (
        f"the running rows still locked: {[dict(r) for r in rows]}; "
        f"the reservation slots held (live or stale): {[dict(r) for r in slots]}; "
        f"the database clock: {now}"
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


#: The cap-exercise cohort: three capped bodies, each outlasting the
#: claim-observation window, so once a claim lands the durable running
#: row's dwell is the poll's to miss, never the fleet's to make.
_CAP_EXERCISE_COHORT = 3
_CAP_EXERCISE_BODY_S = 3.0

#: The front diagnostic probe's bound, arithmetic in-code: one claim
#: cycle (the poll floor) stretched by the runners' 20x stall-band
#: factor (the factor _PROBE_STALL_MARGIN derives) plus one cohort body.
#: This bound no longer carries the assert. The original shape spent it
#: on a single front-loaded claim cycle and claimed "a claim that
#: happened cannot be hidden by any runner speed at this bound" - that
#: claim was false: the repo measures end-to-end runner stalls at 2.5-5
#: minutes (dd4572ff, cited in the scenario docstrings below), so a
#: healthy fleet whose first admitted claim arrived past the 23s bound
#: red here while serving fine (the 1-in-10 sighting's exact
#: signature). The assert now lives at the scenario tail, on the
#: receipt the CapSampler records across the whole scenario (state,
#: not time - see ``assert_cap_receipt``); this probe only reports how
#: quickly the receipt first appeared, and a miss is a diagnostic
#: line, never a red.
_CAP_EXERCISE_BOUND = _POLL_FLOOR * _PROBE_STRETCH + _CAP_EXERCISE_BODY_S


async def _diagnose_queue_cap_exercise(
    conn: asyncpg.Connection, schema: str, sys_client: TaskQ
) -> None:
    """Seed the capped cohort, then report how fast the cap's admission
    receipt appears. DIAGNOSTIC only - this probe never reds.

    The cap is 1 fleet-wide, so any durable running row on the capped
    queue IS a saturated cap: the exercise needs one claim, not a
    throughput coincidence. The receipt is two durable rows observed in
    the same pass: a running row on the capped queue plus a live-held
    slot on the cap's own bucket. The running row alone cannot
    distinguish an admission from a claim the cap denied and is still
    re-probing (denied claims sit 'running' too); the slot row is the
    cap machinery's own write, so the pair proves the admission went
    THROUGH the cap. The retired shape pinned
    ``sampler.peak.capped_queue >= _QUEUE_CAP`` - a peak over 100ms
    ticks from this (starved) test process, which at cap 2 needed two
    capped claims to coincide inside a 2s body and read 1 on the loaded
    CI runner across a whole 13-minute scenario (observed on CI,
    attempt 1): the exercise is a throughput property there, voided by
    runner weather on a fleet that was serving fine. A dedicated cohort
    is enqueued here so the backlog is live at probe time regardless of
    what the fill's own capped work already finished.

    The ASSERT this probe carries must not sit in the scenario's critical
    path, front-loaded, budgeting one claim cycle against runner
    weather the repo measures at 2.5-5 minutes end to end (dd4572ff) -
    a healthy fleet starved past the bound before its first admitted
    claim red, the 1-in-10 sighting. The exercise assert therefore
    lives at the SCENARIO TAIL now, on the receipt the CapSampler
    records across the whole scenario (``assert_cap_receipt``): a
    receipt observed at least once is a state, and no runner speed
    hides a state - only a cap that never admits (the reverted
    registration, the dead admission path) stays unseen across
    thousands of ticks.
    """
    for _ in range(_CAP_EXERCISE_COHORT):
        await sys_client.enqueue(sys_capped, SysPayload(sleep=_CAP_EXERCISE_BODY_S), tags=[_TAG])
    deadline = time.monotonic() + _CAP_EXERCISE_BOUND
    t0 = time.monotonic()
    while time.monotonic() < deadline:
        row = await conn.fetchrow(
            f'SELECT (SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE status = 'running' AND queue = 'roll_capped') AS running, "
            f'(SELECT count(*)::int FROM "{schema}".reservation_slots '
            "WHERE bucket_name = 'taskq:global:queue:roll_capped' "
            "AND job_id IS NOT NULL "
            "AND lease_expires_at >= statement_timestamp()) AS held"
        )
        assert row is not None
        if row["running"] >= _QUEUE_CAP and row["held"] >= _QUEUE_CAP:
            print(
                f"[s7] cap-exercise receipt observed after {time.monotonic() - t0:.2f}s "
                "(diagnostic; the scenario tail owns the assert)"
            )
            return
        await asyncio.sleep(0.1)
    print(
        f"[s7] cap-exercise receipt NOT observed within {_CAP_EXERCISE_BOUND:.0f}s "
        "(diagnostic only - the scenario tail asserts on the sampler's "
        "whole-scenario record, a state no runner speed can hide)"
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
    come back clean on exit.

    The mid-drain probe's bound is DERIVED, not raw: the drain window
    the run itself measured plus a co-tenancy margin of one claim cycle
    stretched by the runners' 20x stall-band factor (the poll floor
    1.0s x 20 = 20s; the arithmetic is pinned on _PROBE_STALL_MARGIN
    above). Demanding an effect inside the raw ~3s window bets on the
    probe chain threading runner weather the repo has measured at 2.5-5
    minutes end to end (dd4572ff) - runner starvation misread as fleet
    stall. The teeth stay: a fleet whose probes are absent for window +
    margin reds, and one that never settles inside 30s reds first.

    The grace pin below is derived the same way: the drain's seconds
    are read on THIS starved test process's clock (the SIGTERM ->
    graceful-stop round trip), so the comparison budgets the operator
    grace PLUS the same co-tenancy margin. The comparison IS the bound:
    this tier's workers boot with the in-worker shutdown watchdog
    DISABLED (the harness's TASKQ_WATCHDOG_ENABLED=false - at this
    cadence it cannot arm: watchdog_loop_lag_budget 30 + heartbeat 0.5
    must be < lock_lease 8, a settings-validation error), so a drain
    that overruns the configured grace is not force-exited by the
    worker (exercised: a 25s wind-down body held w0's drain at 28.22s,
    188% of the 15s grace, and the pod exited rc=0). The harness's
    graceful_stop SIGKILLs only at its own timeout (_GRACE + 30), so
    the rc == 0 pin reds a grace-overrun only past THAT window; inside
    it, this comparison is the only bound the scenario has.
    """
    conn = sys_ledger
    schema = module_pg_schema.schema_name
    fleet = await _spawn_joined_fleet(conn, pg_dsn, schema, _PODS)
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    measurements: list[tuple[str, float, float]] = []
    try:
        ids = await _worker_ids(conn, schema, fleet)
        await _fill_fleet(conn, schema, sys_client, want_running=10)
        # The cap-exercise cohort, seeded while the fleet is at full
        # strength, with the bounded receipt probe run as a diagnostic
        # (it never reds): the exercise assert is the scenario tail's,
        # on the receipt the sampler records across the whole scenario
        # (sampler.assert_cap_receipt) - a state no runner speed can
        # hide. The retired front-loaded bounded poll spent its budget
        # on one claim cycle inside the scenario's first seconds and
        # red a healthy fleet whose first admitted claim arrived past
        # it (the 1-in-10 sighting).
        await _diagnose_queue_cap_exercise(conn, schema, sys_client)

        for name in _PODS[:-1]:
            # The mid-drain probe wave (uncapped queue: the probe must
            # not queue behind the cap's own backlog). The window opens
            # before the probes are enqueued, so a probe effect landing
            # inside [open, close + margin] means the fleet dispatched
            # within the drain window plus the co-tenancy margin the
            # _PROBE_STALL_MARGIN block derives - not that the race
            # threaded the raw window through runner weather.
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
            # The grace pin, derived: the semantic is the operator-facing
            # termination grace (_GRACE - the harness boots every pod
            # with it, TASKQ_TERMINATION_GRACE_PERIOD), but the drain's
            # seconds are read on this process's clock, across the same
            # starved round trips the _PROBE_STALL_MARGIN block derives
            # (measured 12.19s against the bare 15s grace under a mere
            # 4-hog load - 81% of a bound that never budgeted the
            # observer). So the comparison budgets the grace plus that
            # margin - and this comparison is the bound, not slack on a
            # state assert beside it: the tier's workers boot with the
            # shutdown watchdog disabled (the harness's cadence cannot
            # arm it, see the scenario docstring), so a drain overrunning
            # the grace exits rc=0, and only the harness's SIGKILL at
            # graceful_stop's own timeout (_GRACE + 30) turns an overrun
            # into a non-zero rc. Exercised: 25s wind-down body, drain
            # 28.22s, rc=0, only this pin could have red.
            assert drain_secs < _GRACE + _PROBE_STALL_MARGIN, (
                f"pod {name}'s drain ran {drain_secs:.2f}s, past its "
                f"{_GRACE:.0f}s termination grace plus the "
                f"{_PROBE_STALL_MARGIN:.0f}s co-tenancy margin this "
                f"process's own observation is budgeted (the worker does "
                f"not force-exit at the grace - the shutdown watchdog is "
                f"disabled at this tier's cadence - and the harness's "
                f"SIGKILL lands at the graceful_stop timeout, past this "
                f"bound: here, this comparison is the bound)"
            )
            measurements.append(
                (f"sequential drain {name}", drain_secs, _GRACE + _PROBE_STALL_MARGIN)
            )

            # No stall: the probes are served promptly, and within the
            # drain window plus the derived co-tenancy margin at least
            # one probe ran. The margin (one poll floor stretched 20x,
            # _PROBE_STALL_MARGIN above) absorbs the shared runner's
            # loop-stall weather: through it, any link of the probe
            # chain (the starved test process's enqueue round trips, a
            # survivor's claim poll, the effect write) can eat seconds
            # of a ~3s window while the fleet serves fine - settled
            # probes inside their 30s cap are the proof it did. A fleet
            # whose probes are absent for the derived bound, or that
            # never settles at all, is genuinely stalled and reds.
            await _settle_probes(conn, schema, probe_ids, cap_secs=30.0)
            times = await _effect_times(conn, schema, probe_ids)
            assert times, f"pod {name}: no probe effect recorded at all"
            if drain_secs >= 1.5:
                probe_bound = window_close + timedelta(seconds=_PROBE_STALL_MARGIN)
                during = [t for t in times if window_open <= t <= probe_bound]
                assert during, (
                    f"pod {name}: no probe ran within its measured "
                    f"{drain_secs:.2f}s drain window + the "
                    f"{_PROBE_STALL_MARGIN:.0f}s co-tenancy margin "
                    f"(poll floor {_POLL_FLOOR:.0f}s x the runners' "
                    f"{_PROBE_STRETCH:.0f}x stall-band stretch) - the "
                    "fleet stalled while the pod drained"
                )
            await _assert_corpse_owns_nothing(
                conn, schema, ids[name], f"sequential {name}", fleet[name]
            )

        # The permanent survivor drains last, after the settle below
        # proves it carried the whole release's load.
        counts = await assert_balanced(conn, schema, _TAG)
        survivor = _PODS[-1]
        rc = await asyncio.to_thread(graceful_stop, fleet[survivor], timeout=_GRACE + 30.0)
        drained.add(survivor)
        assert rc == 0, f"the survivor {survivor} exited rc={rc}"
        await _assert_corpse_owns_nothing(
            conn, schema, ids[survivor], f"survivor {survivor}", fleet[survivor]
        )
        # The counter balances over the whole cohort, the survivor's
        # own drain included.
        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) >= 10 + 8 + 8, (
            f"the fleet did not complete every job it admitted: {counts}"
        )
        await assert_effects_balance(conn, schema, _TAG)
        # The exercise assert, anchored at the tail on the sampler's
        # whole-scenario record of the admission receipt (state, not
        # time: observed-at-least-once cannot be hidden by any runner
        # speed, and a cap that never admits - the unregistered
        # reservation, the dead admission path - reds here). The peaks
        # stay for the record - neither surface's peak is pinned, a
        # 100ms tick's max cannot promise it landed inside a running
        # window, and the CapSampler docstring carries the doctrine.
        await sampler.assert_cap_receipt("sequential drains")
        await sampler.stop_and_report("sequential drains")
        print(f"[s7] cap sampler peaks (diagnostic): {sampler.peak}")
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
    fleet = await _spawn_joined_fleet(conn, pg_dsn, schema, _PODS)
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    try:
        ids = await _worker_ids(conn, schema, fleet)
        await _fill_fleet(conn, schema, sys_client, want_running=10)
        # The cap-exercise cohort, seeded while the fleet is at full
        # strength, with the bounded receipt probe run as a diagnostic
        # (it never reds; see the sequential scenario's call for the
        # doctrine). The exercise assert is this scenario's tail, on
        # the receipt the sampler records across the whole scenario
        # (sampler.assert_cap_receipt).
        await _diagnose_queue_cap_exercise(conn, schema, sys_client)

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

        # Pair 2 is signalled inside pair 1's drain window: three pods
        # drain at once, two generations of the release overlap.
        fleet["w2"].proc.terminate()
        fleet["w3"].proc.terminate()
        await asyncio.sleep(0.5)
        assert fleet["w0"].proc.poll() is None or fleet["w3"].proc.poll() is None, (
            "pair 2's signal landed after pair 1 finished - the churn was not overlapping"
        )

        # Probes during the heaviest window: three pods draining, w4 -
        # still up - carrying them, then it settles before the last pod
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
            await _assert_corpse_owns_nothing(
                conn, schema, ids[name], f"overlap {name}", fleet[name]
            )

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
        await _assert_corpse_owns_nothing(conn, schema, ids["w4"], "survivor w4", fleet["w4"])

        await assert_effects_balance(conn, schema, _TAG)
        # The exercise assert, anchored at the tail on the sampler's
        # whole-scenario record of the admission receipt (state, not
        # time; see the sequential scenario's tail for the doctrine).
        await sampler.assert_cap_receipt("overlapping pairs")
        await sampler.stop_and_report("overlapping pairs")
        print(f"[s7] cap sampler peaks (diagnostic): {sampler.peak}")
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
    fleet = await _spawn_joined_fleet(conn, pg_dsn, schema, _PODS[:4])
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    try:
        ids = await _worker_ids(conn, schema, fleet)

        # Saturate both cap families: the queue cap's single slot and
        # one keyed slot, all with a backlog behind them.
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
        # The victim's selection is a state wait, not a moment: the
        # saturation assert above proved a running capped row existed
        # at its fetch, but the selection re-reads the durable rows one
        # round trip later, and a body turnover in that gap (plus the
        # backlog's re-claim cycle) makes a bare single-shot fetch a
        # hope-timing red. The wait is a hang guard only - one claim
        # cycle stretched the runners' 20x stall-band factor (the
        # _PROBE_STALL_MARGIN arithmetic); a fleet whose capped queue
        # stops running anything at all reds at the bound.
        holder_deadline = time.monotonic() + _PROBE_STALL_MARGIN
        holder = None
        while time.monotonic() < holder_deadline:
            holder = await conn.fetchrow(
                f"SELECT locked_by_worker::text AS wid, count(*)::int AS n "
                f"FROM \"{schema}\".jobs WHERE status = 'running' "
                f"AND queue = 'roll_capped' GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            )
            if holder is not None and holder["wid"] is not None:
                break
            await asyncio.sleep(0.1)
        assert holder is not None and holder["wid"] is not None, (
            f"no pod ran a capped job within the {_PROBE_STALL_MARGIN:.0f}s "
            f"selection hang guard (one claim cycle stretched) right after "
            "the caps saturated - the capped queue stopped running"
        )
        victim = next(name for name, wid in ids.items() if wid == holder["wid"])
        t_sig = time.monotonic()
        fleet[victim].proc.terminate()
        rc = await asyncio.to_thread(graceful_stop, fleet[victim], timeout=_GRACE + 30.0)
        graceful_secs = time.monotonic() - t_sig
        drained.add(victim)
        assert rc == 0, f"pod {victim} exited rc={rc}"
        await _assert_corpse_owns_nothing(
            conn, schema, ids[victim], f"graceful {victim}", fleet[victim]
        )
        # The grace pin, the same derived shape as the sequential
        # drains' (see that scenario for the doctrine): the seconds are
        # read on this starved test process's clock, so the comparison
        # budgets the operator grace plus the co-tenancy margin - and
        # this comparison is the bound (the tier's workers boot with
        # the shutdown watchdog disabled; the harness's SIGKILL lands
        # only at the graceful_stop timeout, past it).
        assert graceful_secs < _GRACE + _PROBE_STALL_MARGIN, (
            f"graceful slot release took {graceful_secs:.2f}s, past the "
            f"{_GRACE:.0f}s termination grace plus the "
            f"{_PROBE_STALL_MARGIN:.0f}s co-tenancy margin this process's "
            "own observation is budgeted"
        )
        print(
            f"[s7] graceful cap release ({victim}, {holder['n']} capped rows): "
            f"measured {graceful_secs:.2f}s vs bound "
            f"{_GRACE + _PROBE_STALL_MARGIN:.0f}s"
        )

        # ── the capacity-leak construct: SIGKILL a pod mid-job ────────
        # The churn re-spread the backlog after the graceful departure;
        # the corpse is selected from observed slot holders - the mirror
        # of the graceful half's own selection above: wait until any
        # surviving pod live-holds cap capacity again, then make that
        # pod the corpse. Pinning a victim by name made the premise a
        # claim-race coin flip (three survivors race for the freed
        # slots; the named one can lose every turnover of a 30s body
        # cycle within any fixed window) and the pin red as "nothing to
        # strand" with the product healthy. The wait's budget is a hang
        # guard only, derived from the slot-turnover terms (the 30s
        # body sleep + the claim poll, one turnover plus stretch): if
        # no survivor ever re-acquires, that is the genuine
        # capacity-leak defect this pin exists to catch.
        corpse_deadline = time.monotonic() + _CORPSE_PREMISE_BOUND
        corpse = None
        held = 0
        while time.monotonic() < corpse_deadline:
            holder_row = await conn.fetchrow(
                f"SELECT locked_by_worker::text AS wid, count(*)::int AS n "
                f'FROM "{schema}".reservation_slots r JOIN "{schema}".jobs j ON j.id = r.job_id '
                "WHERE r.held_by_worker_id <> $1::uuid AND r.job_id IS NOT NULL "
                "AND r.lease_expires_at >= statement_timestamp() "
                "AND j.status = 'running' AND j.queue = 'roll_capped' "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
                ids[victim],
            )
            # No in-loop assert: None is the state this loop EXISTS to
            # wait through. At cap 1 the departed victim held the only
            # slot, so between its release and the next admitted claim
            # (a denial backoff + a claim poll) the probe legitimately
            # reads no holder - the CI red was the
            # first probe landing in that ordinary window, reding a
            # healthy fleet on a byte-identical assert that cap 2's
            # second slot had masked on base. The probe re-checks the
            # state each pass; the pin is the deadline plus the final
            # assert below - a fleet that never re-acquires (the
            # genuine capacity-leak defect) reds there, inside the
            # derived hang-guard bound.
            if holder_row is not None and holder_row["wid"] is not None:
                corpse = holder_row["wid"]
                held = holder_row["n"]
                break
            await asyncio.sleep(0.2)
        assert corpse is not None and held >= 1, (
            "no survivor ever re-acquired cap capacity - nothing to strand"
        )
        victim2 = next(name for name, wid in ids.items() if wid == corpse and name not in drained)
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
        fleet["w1b"] = (await _spawn_joined_fleet(conn, pg_dsn, schema, ["w1b"]))["w1b"]

        # Re-admission: with the backlog still deep, the survivors run
        # the capped queue back up to its cap and re-fill the keyed
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


async def _storm_limbo_report(
    conn: asyncpg.Connection, schema: str, fleet: Mapping[str, WorkerProc]
) -> str:
    """Describe residual running rows and their worker's shutdown records.

    This runs as diagnostic evidence only. The scenario's assertion does
    not read or depend on the report.
    """
    try:
        rows = await conn.fetch(
            f"SELECT j.id::text AS id, j.actor, j.status::text AS status, "
            f"j.attempt, j.claim_epoch, j.locked_by_worker::text AS holder, "
            f"j.started_at, j.lock_expires_at, j.scheduled_at, j.interrupt_count, "
            f"w.pid AS worker_pid, statement_timestamp() AS db_now "
            f'FROM "{schema}".jobs j '
            f'LEFT JOIN "{schema}".workers w ON w.id = j.locked_by_worker '
            "WHERE j.status = 'running' ORDER BY j.id"
        )
        evidence: list[dict[str, object]] = []
        for row in rows:
            item = dict(row)
            owner = next(
                (worker for worker in fleet.values() if worker.proc.pid == row["worker_pid"]),
                None,
            )
            related = [line for line in _worker_log_lines(owner) if str(row["id"]) in line]
            item["related_worker_log"] = related
            evidence.append(item)
        return repr(evidence)
    except Exception as exc:
        return f"failed to collect storm-row diagnostics: {exc!r}"


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
    fleet = await _spawn_joined_fleet(conn, pg_dsn, schema, _PODS)
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
        limbo_report = await _storm_limbo_report(conn, schema, fleet) if limbo else ""
        assert limbo == 0, (
            f"{limbo} row(s) still running with the whole fleet dead; "
            f"row and worker evidence: {limbo_report}"
        )
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
        fleet2 = await _spawn_joined_fleet(conn, pg_dsn, schema, ["n0", "n1"])
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

    fleet = await _spawn_joined_fleet(conn, pg_dsn, schema, _PODS[:3])
    sampler = CapSampler(pg_dsn, schema)
    sampler.start()
    drained: set[str] = set()
    fresh: dict[str, WorkerProc] = {}
    try:
        ids = await _worker_ids(conn, schema, fleet)

        # One defiant job: it will still be running when the flap kills
        # its pod (the body outlasts the 95%-of-grace kill), plus
        # ordinary backlog for the survivors to carry.
        # One defiant job: it will still be running when the flap kills
        # its pod (the body outlasts the 95%-of-grace kill), plus
        # ordinary backlog for the survivors to carry. The body's own
        # duration is derived from the scenario's arithmetic: the kill
        # lands by 0.95 of the grace plus the co-tenancy slack the
        # post-premise enqueues can eat (_PROBE_STALL_MARGIN), so the
        # body gets that bound plus two poll floors of margin - the
        # premise "the drain was unfinished at the kill" is then built
        # in, not raced.
        _defiant_sleep_s = 0.95 * _GRACE + _PROBE_STALL_MARGIN + _POLL_FLOOR * 2
        defiant_handle = await sys_client.enqueue(
            sys_defiant, SysPayload(sleep=_defiant_sleep_s), tags=[_TAG]
        )
        deadline = time.monotonic() + _FLAP_PREMISE_BOUND_S
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
        # (the defiant body absorbs cancelling and forcing, so the pod
        # is at the grace edge with the drain unfinished), then SIGKILL.
        fleet[victim].proc.terminate()
        await asyncio.sleep(0.95 * _GRACE)
        t_kill = time.monotonic()
        fleet[victim].proc.kill()
        kill_rc = await asyncio.to_thread(fleet[victim].proc.wait, 30)
        drained.add(victim)
        # The flap's receipt is durable state, not a clock (the #651
        # doctrine: a bare wall-clock assert here bet on the runner - the
        # SIGKILL's reap is the OS's, but the starved test process's
        # observation of it is not).
        #
        # 1. The process died by the kill (rc = -SIGKILL): a pod whose
        #    drain had finished would have exited graceful (rc = 0)
        #    before this kill - the grace-edge premise is exactly "the
        #    drain was unfinished when the budget's 95% came up".
        # 2. The defiant row is still non-terminal: the body the corpse
        #    held never completed - the kill caught real work mid-flight,
        #    which is what the new identity's pickup below has to
        #    recover.
        assert kill_rc == -int(signal.SIGKILL), (
            f"the grace-edge pod exited rc={kill_rc} before its SIGKILL - "
            "its drain finished inside the budget, so the flap premise "
            "(killed at 95% of the budget WITH THE DRAIN UNFINISHED) is void"
        )
        flap_state = await conn.fetchrow(
            f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
            defiant,
        )
        assert flap_state is not None and flap_state["status"] not in (
            "succeeded",
            "failed",
            "cancelled",
            "crashed",
            "abandoned",
        ), (
            f"the defiant row went terminal ({flap_state}) before/with the kill - "
            "the flap killed a pod that was no longer holding unfinished work"
        )

        # The rest of the old generation goes down with it (the deploy
        # rolled the whole generation): each survivor's drain runs in
        # the defiant body's teeth - the body defies those cancels too,
        # so the drain releases the row it holds with a fresh hold and
        # exits graceful while the body lingers behind it. Whatever the
        # flap released, the old generation's exits re-release with a
        # hold no old pod can outlive.
        for name in [n for n in ids if n != victim]:
            fleet[name].proc.terminate()
        for name in [n for n in ids if n != victim]:
            rc = await asyncio.to_thread(graceful_stop, fleet[name], timeout=_GRACE + 60.0)
            drained.add(name)
            assert rc == 0, f"survivor {name} exited rc={rc}"
            await _assert_corpse_owns_nothing(
                conn, schema, ids[name], f"survivor {name}", fleet[name]
            )

        # A new pod with a new identity (kubernetes restart semantics):
        # nothing in the main path may assume the corpse returns. Two
        # pods boot into the empty fleet and own every row the old
        # generation's die-off left behind.
        fresh = await _spawn_joined_fleet(conn, pg_dsn, schema, ["fresh0", "fresh1"])
        fresh_ids = await _worker_ids(conn, schema, fresh)
        assert all(fid != holder_id for fid in fresh_ids.values()), (
            "a new pod reused the corpse's identity"
        )

        # The defiant row: released with a hold (or stranded running
        # and locked if the kill beat the drain) - either way the
        # pickup bound is the hold/lease lapse + the drain tail + boot
        # + poll, stretched by the tier's load factor (the reclaim's
        # leader-failover hop and the re-claim both pay co-tenancy on
        # a shared runner). The new identity must be the claimant:
        # with the old generation gone, only a fresh pod can hold it.
        pickup_bound = (_LOCK_LEASE + _LEADER_LEASE + _GRACE + 20.0) * TIER_LOAD_STRETCH
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


# ── The sampler teardown's loop-hygiene pin ──────────────────────────────


@pytest.mark.timeout(60)
async def test_cap_sampler_close_leaves_no_task_pending_on_the_module_loop(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """The sampler's close must never leave a task pending on the module
    event loop - the leak class behind both shard reds (the PG16 shard's
    error-at-teardown of the overlapping-pairs scenario,
    and the PG18 shard's of the grace-edge flap: "test
    left asyncio task(s) still pending on the module event loop ...
    coroutine: Connection._cancel", 37/40 passed, 1 error each).

    The race, determinized here: ``close`` cancelled the sampler task
    MID-TICK, and when the cancel lands while a tick's query is genuinely
    IN FLIGHT on the wire, asyncpg answers it with a fire-and-forget
    ``Connection._cancel`` task (``_cancel_current_command``'s
    ``create_task`` - a fresh server connection whose whole life is the
    PG cancel request). That task outlives the test body: on a fast box
    it finishes inside the teardown's own awaits (rerun-green - the
    weather ruling), on a loaded runner the conftest loop-leak guard's
    snapshot catches it pending and the SHARD errors. The deterministic
    injection holds an ACCESS EXCLUSIVE lock on ``jobs``, so the next
    tick's SELECT is provably mid-wire when the cancel lands; the pin
    then mirrors the conftest guard's own baseline-diff the moment
    ``close`` returns.

    The fix this pin holds: the close joins the task's NATURAL exit
    (the tick loop checks the stop flag between ticks - no cancel ever
    lands mid-query), and the hang-guard fallback (a backend hung past
    the join bound) reaps what its own cancel mints before returning.
    """
    schema = module_pg_schema.schema_name
    baseline = set(asyncio.all_tasks())

    # Hold the table's exclusive lock in a side session so the sampler's
    # next SELECT is blocked in flight on the wire, not queued locally.
    locker = await asyncpg.connect(pg_dsn)
    await locker.execute("BEGIN")
    await locker.execute(
        f'LOCK TABLE "{schema}".jobs IN ACCESS EXCLUSIVE MODE'
    )  # Why: fixture-validated schema identifier.
    # The lock's bounded release: 1.0s covers close()'s entry plus the
    # sampler's one blocked tick on either shape (the retired bare close
    # and the fix), and is released long before the pin's own 60s bound.
    lock_open_until = time.monotonic() + 1.0

    async def _release_lock() -> None:
        remaining = lock_open_until - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)
        await locker.execute("ROLLBACK")
        await locker.close()

    releaser = asyncio.create_task(_release_lock())

    sampler = CapSampler(pg_dsn, schema)
    try:
        sampler.start()
        # The first tick is the one close() meets: its SELECT is already
        # blocked in-flight on the wire (the lock was taken before the
        # start), the state every scenario tail hands the close on the
        # red runs.
        await asyncio.sleep(0.3)

        await sampler.close()

        # The conftest guard's own shape, at the moment the guard sees
        # it: the teardown diff runs as soon as the test body hands
        # control back - every task minted during this body that is
        # still pending is the shard-red error. No await may sit between
        # the close and this snapshot: the leak heals inside a
        # teardown's own round trips (the weather ruling), which is
        # exactly why the CI red was rare.
        #
        # The pin's own helper is exempt: ``_release_lock`` was minted
        # after the baseline and is still pending when close() returns
        # under load (it sleeps to the 1.0s lock window and then pays
        # two round trips - a race close's return cannot win on a 20x
        # stretched runner). It is awaited in the finally below before
        # the test ends, so the exemption cannot hide a guard-visible
        # leak: the guard's own diff would never see it either. Every
        # other mint - the asyncpg Connection._cancel above all - must
        # be gone the moment close() returns.
        leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task(), releaser}
        assert not leaked, (
            "the sampler's close left task(s) pending on the module loop - "
            "asyncpg's fire-and-forget Connection._cancel, the shard-red "
            f"leak class: {[t.get_name() for t in leaked]}"
        )
    finally:
        # The lock's release is not part of the snapshot's story: the
        # leaked task (on the red shape) heals or not before this runs,
        # and the pin's own assertion is the red either way.
        await releaser


@pytest.mark.timeout(60)
async def test_cap_sampler_close_hang_guard_fallback_reaps_the_minted_cancel(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The close's HANG-GUARD FALLBACK path, forced and pinned.

    The sibling pin (``..._leaves_no_task_pending...``) exercises the
    NATURAL-exit join: its lock window (1.0s) always beats the join
    bound, so the fallback cancel and the scoped reap behind it were
    never exercised by any test - mutation-surviving code (deleting the
    reap block reddened nothing). THIS pin forces the fallback: the
    join bound is patched below the 1.0s lock window, so the join times
    out while the tick's SELECT is still blocked mid-wire and the
    fallback MUST cancel the task mid-query - the exact state that makes
    asyncpg mint its fire-and-forget ``Connection._cancel``.

    Two parts, because a single end-to-end no-leak diff cannot carry the
    red honestly: on a fast box the minted ``_cancel`` task usually
    finishes inside the close's own ``conn.close()`` round trips
    (measured: conn.close ~5ms, the mint's fresh server connection
    faster), so an end-to-end loop diff alone stays GREEN even with the
    reap block deleted - the same weather ruling that made the original
    leak rare.

    * Part 1 - the fallback RAN and the reap was INVOKED: a spy wraps
      ``_reap_minted_cancels`` around the real close(); if the reap
      block is deleted from close (mutation A), the spy never fires and
      this part reds deterministically. The loop diff after close must
      still be clean.
    * Part 2 - the reap's TEETH on a mint it provably sees: the pin
      hand-mints a ``Connection._cancel`` (cancel a blocked query's
      task, snapshot the diff synchronously - no await in between, so
      the mint cannot have completed), hands the diff to the reap, and
      asserts the mint came back DONE, counted in ``reaped_tasks`` by
      name + coro, and logged. Breaking the reap's diff, its await, or
      its log reds here.
    """
    monkeypatch.setattr(
        "tests.system_e2e.test_rolling_release_fleet._SAMPLER_JOIN_BOUND_S",
        0.3,  # Why: below the 1.0s lock window, so the join must time out.
    )
    schema = module_pg_schema.schema_name
    baseline = set(asyncio.all_tasks())

    # The lock window covers both parts (part 1's forced fallback and
    # part 2's blocked-query mint); released long before the pin's own
    # 60s bound either way.
    locker = await asyncpg.connect(pg_dsn)
    await locker.execute("BEGIN")
    await locker.execute(
        f'LOCK TABLE "{schema}".jobs IN ACCESS EXCLUSIVE MODE'
    )  # Why: fixture-validated schema identifier.
    lock_open_until = time.monotonic() + 2.5

    async def _release_lock() -> None:
        remaining = lock_open_until - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)
        await locker.execute("ROLLBACK")
        await locker.close()

    releaser = asyncio.create_task(_release_lock())

    sampler = CapSampler(pg_dsn, schema)
    try:
        # Part 1: the forced fallback through a real close(), with the
        # reap spied (the real method still runs underneath the spy).
        reap_calls: list[bool] = []
        real_reap = CapSampler._reap_minted_cancels

        async def _spy_reap(self: CapSampler, minted_baseline: set[asyncio.Task[object]]) -> None:
            reap_calls.append(True)
            await real_reap(self, minted_baseline)

        monkeypatch.setattr(CapSampler, "_reap_minted_cancels", _spy_reap)

        sampler.start()
        await asyncio.sleep(0.3)
        await sampler.close()

        # The fallback must have fired: the patched 0.3s join bound
        # expired while the lock still held the tick's SELECT mid-wire
        # (the lock opens at 2.5s). A cancelled tick task is the
        # fingerprint - a natural exit cannot leave one.
        assert sampler._task is not None and sampler._task.cancelled(), (
            "the forced hang-guard fallback did not fire - the join bound "
            "did not expire (patch failed?) or the close cancelled instead"
        )
        assert reap_calls == [True], (
            "the forced fallback never invoked the reap - the reap block "
            "is deleted from close() (mutation A) or bypassed"
        )

        # The sibling pin's no-leak diff, now over the forced fallback:
        # the scoped reap must leave the module loop exactly as clean as
        # the natural exit does.
        leaked = set(asyncio.all_tasks()) - baseline - {asyncio.current_task(), releaser}
        assert not leaked, (
            "the sampler's close FALLBACK path left task(s) pending on "
            "the module loop - the scoped reap must await what the "
            f"fallback's cancel mints: {[t.get_name() for t in leaked]}"
        )

        # Part 2: the reap's teeth on a mint it provably sees. Mint a
        # Connection._cancel by hand (cancel a blocked query's task),
        # snapshot the diff synchronously - no await between the mint's
        # delivery and this set, so the mint cannot have completed -
        # then hand it to the reap and demand it come back awaited,
        # counted, and logged.
        q_conn = await asyncpg.connect(pg_dsn)
        q_task = asyncio.create_task(
            q_conn.fetch(f'SELECT 1 FROM "{schema}".jobs')  # blocked on the lock
        )
        await asyncio.sleep(0.1)  # Why: let the SELECT reach the wire.
        reap_baseline = set(asyncio.all_tasks())
        q_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await q_task
        minted = set(asyncio.all_tasks()) - reap_baseline - {asyncio.current_task()}
        assert minted, "the repro did not mint the asyncpg Connection._cancel task"
        assert all(_is_asyncpg_cancel_task(t) for t in minted), (
            f"the minted task is not the asyncpg Connection._cancel class: "
            f"{[t.get_coro() for t in minted]}"
        )
        counted_before = len(sampler.reaped_tasks)
        with caplog.at_level("WARNING", logger="tests.system_e2e.test_rolling_release_fleet"):
            await sampler._reap_minted_cancels(reap_baseline)
        assert all(t.done() for t in minted), (
            "the reap returned with the minted cancel still pending - the "
            "reap's bounded await is broken (wrong diff, dropped wait)"
        )
        new_records = sampler.reaped_tasks[counted_before:]
        assert len(new_records) == len(minted), (
            f"the reap did not count every mint it reaped: {new_records} "
            f"for {[t.get_name() for t in minted]}"
        )
        assert all("Connection._cancel" in record for record in new_records), (
            f"the reap's count does not name the mint class: {new_records}"
        )
        assert "reaped a minted task" in caplog.text, (
            "the reap ran silently - every reaped task must be logged by "
            f"name + coro: {new_records}"
        )
    finally:
        await releaser


async def test_corpse_ownership_wait_staged_reads(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: Any, tmp_path: Any
) -> None:
    """The corpse-wait helper's staged reads: NOT a production
    reproduction. The real-world red is CI run 42359203585's rolling
    lane: worker 2 exited rc0, the probes settled, and one running row
    was still locked by it at the immediate assert - the product's
    shutdown release documents its own fallback (a failed or skipped
    release write leaves running/locked rows for lease-expiry reclaim),
    and probe settlement does not settle corpse rows. The stages drive
    the helper against REAL worker log files (the same tee/sink shape
    the harness writes), and the evidence rule is the current attempt's:
    a marker record naming the row's job id with a console stamp from
    the row's own claim instant or later. The cases: no log at all; a
    marker naming a different job id; the SAME job id with an OLDER
    attempt's stamp; a mixed group where one row's evidence does not
    excuse its sibling; the exact-row evidence whose reclaim lands
    during the wait (a stale slot row may survive the corpse - the LIVE
    lease is the condition); and a held row with no lock expiry."""
    schema = module_pg_schema.schema_name
    corpse = str(new_uuid())
    log_path = tmp_path / "corpse-worker.log"
    worker = WorkerProc(
        proc=None,  # type: ignore[arg-type]  # Why: the helper reads only the worker's log surface; no process is spawned for a staged read.
        sock_path=f"{tmp_path}/corpse.sock",
        log_path=str(log_path),
    )
    flow_id = await seed_flow(clean_pg_conn, schema)
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id)
    sibling_id = await seed_running_node(clean_pg_conn, schema, flow_id)
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET locked_by_worker = $1::uuid, '
        "lock_expires_at = statement_timestamp() + interval '60s', "
        "started_at = statement_timestamp() "
        "WHERE id = $2::uuid",
        corpse,
        node_id,
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".reservation_slots '
        "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
        "VALUES ('staged-cap', 0, $1::uuid, $2::uuid, statement_timestamp(), "
        "statement_timestamp() + interval '60s')",
        node_id,
        corpse,
    )

    # The clean fast path: a corpse with nothing staged passes at once.
    await _assert_corpse_owns_nothing(
        clean_pg_conn, schema, str(new_uuid()), "the clean fast path", worker
    )

    # A residual running row with no log at all: there is no
    # release-fallback evidence to wait on, so the release miss fails
    # immediately - the row's own bound is never granted to a missing
    # release.
    with pytest.raises(AssertionError) as missing_log:
        await _assert_corpse_owns_nothing(
            clean_pg_conn, schema, corpse, "no log", worker, bound=60.0
        )
    assert "no release-fallback evidence" in str(missing_log.value), str(missing_log.value)
    assert "did not clear within" not in str(missing_log.value), str(missing_log.value)

    # The claim stamp, from the row itself; the staged records use the
    # product TimeStamper's exact ISO UTC shape.
    staged_row = await clean_pg_conn.fetchrow(
        f'SELECT started_at FROM "{schema}".jobs WHERE id = $1::uuid', node_id
    )
    assert staged_row is not None and staged_row["started_at"] is not None
    claim_stamp = staged_row["started_at"]
    claim_line_stamp = claim_stamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    stale_line_stamp = (claim_stamp - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # A marker naming a DIFFERENT job id is not this row's evidence.
    log_path.write_text(
        "2026-11-07T09:35:09.123456Z level=warning event=release-pg-write-failed "
        "job_id=00000000-0000-0000-0000-000000000000 error=boom\n"
    )
    with pytest.raises(AssertionError) as wrong_row:
        await _assert_corpse_owns_nothing(
            clean_pg_conn, schema, corpse, "wrong row", worker, bound=60.0
        )
    assert "no release-fallback evidence" in str(wrong_row.value), str(wrong_row.value)

    # The SAME job id with an OLDER attempt's stamp: the marker record
    # predates the current claim, so it is not evidence for this row.
    log_path.write_text(
        f"{stale_line_stamp} level=warning event=release-pg-write-failed "
        f"job_id={node_id} error=boom\n"
    )
    with pytest.raises(AssertionError) as stale_attempt:
        await _assert_corpse_owns_nothing(
            clean_pg_conn, schema, corpse, "stale attempt", worker, bound=60.0
        )
    assert "no release-fallback evidence" in str(stale_attempt.value), str(stale_attempt.value)

    # A MIXED group: the sibling row's evidence does not excuse this
    # row's missing marker. The log names the node's row only, and the
    # stamp is the stale attempt's (the sibling's own current-attempt
    # marker is absent either way).
    log_path.write_text(
        stale_line_stamp
        + f" level=warning event=release-pg-write-failed job_id={node_id} error=boom\n"
    )
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET locked_by_worker = $1::uuid, '
        "lock_expires_at = statement_timestamp() + interval '60s', "
        "started_at = statement_timestamp() "
        "WHERE id = $2::uuid",
        corpse,
        sibling_id,
    )
    with pytest.raises(AssertionError) as mixed_group:
        await _assert_corpse_owns_nothing(
            clean_pg_conn, schema, corpse, "mixed group", worker, bound=60.0
        )
    assert "no release-fallback evidence" in str(mixed_group.value), str(mixed_group.value)

    # The EXACT-row evidence, current attempt, for EVERY row in the
    # group: the helper waits while the staged reclaim clears both
    # rows, then passes with the stale slot row still naming the corpse
    # (the LIVE lease is the condition).
    log_path.write_text(
        claim_line_stamp
        + f" level=warning event=release-pg-write-failed job_id={node_id} error=boom\n"
        + claim_line_stamp
        + f" level=warning event=shutdown-release-failed job_id={sibling_id} error=boom\n"
    )

    async def _staged_reclaim() -> None:
        await asyncio.sleep(1.0)
        await clean_pg_conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'pending' WHERE locked_by_worker = $1::uuid",
            corpse,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".reservation_slots '
            "SET lease_expires_at = statement_timestamp() - interval '1s' "
            "WHERE held_by_worker_id = $1::uuid",
            corpse,
        )

    reaper = asyncio.create_task(_staged_reclaim())
    try:
        await _assert_corpse_owns_nothing(clean_pg_conn, schema, corpse, "staged reclaim", worker)
    finally:
        if not reaper.done():
            reaper.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reaper

    # The held row with NO lock expiry: the reclaim bound cannot be
    # derived, so the fail is immediate.
    await clean_pg_conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'running', "
        "locked_by_worker = $1::uuid, lock_expires_at = NULL, "
        "started_at = statement_timestamp() WHERE id = $2::uuid",
        corpse,
        sibling_id,
    )
    with pytest.raises(AssertionError) as no_expiry:
        await _assert_corpse_owns_nothing(
            clean_pg_conn, schema, corpse, "no expiry", worker, bound=60.0
        )
    assert "holds no lock expiry" in str(no_expiry.value), str(no_expiry.value)


def test_worker_log_drain_joins_at_graceful_stop(tmp_path: Any) -> None:
    """The harness's tee reader is owned by the WorkerProc: after the
    graceful stop's process wait, the reader is joined at its own EOF,
    so the log file is final the moment the caller reads it - the last
    stderr record is present, and no reader thread lingers past the
    stop."""
    import subprocess as sp

    log_path = tmp_path / "short-worker.log"
    proc = sp.Popen(
        [
            sys.executable,
            "-c",
            "import signal, sys, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "print('early', file=sys.stderr, flush=True)\n"
            "time.sleep(0.3)\n"
            "print('final-line', file=sys.stderr, flush=True)\n",
        ],
        stdout=sp.PIPE,
        stderr=sp.PIPE,
    )
    worker = WorkerProc(
        proc=proc,
        sock_path=str(tmp_path / "short.sock"),
        log_path=str(log_path),
        log_drain=_tee_stderr(proc, str(log_path)),
    )
    # Wait for the child's own first record: poll the file's existence
    # then its contents (the tee's reader opens the file in its own
    # thread, so existence precedes contents); the child's death breaks
    # the wait loudly instead of polling forever. No blind sleeps. The
    # whole post-spawn flow rides a finally: a readiness or assertion
    # failure still kills and reaps the live child and joins the log
    # reader.
    ready_deadline = time.monotonic() + 10.0
    try:
        while not (log_path.exists() and "early" in log_path.read_text(errors="replace")):
            if proc.poll() is not None:
                raise AssertionError(
                    f"the probe subprocess died before its first record: rc={proc.returncode}"
                )
            if time.monotonic() > ready_deadline:
                raise AssertionError("the probe subprocess never wrote its first record")
            time.sleep(0.05)
        graceful_stop(worker, timeout=10.0)
        assert worker.log_drain is not None
        assert not worker.log_drain.is_alive(), "the tee reader outlived the graceful stop"
        assert "final-line" in log_path.read_text(), (
            "the log's last stderr record is missing after the graceful stop: "
            "the reader was not joined at its EOF"
        )
    finally:
        reap(worker)


class _FaultedDetailsSampler(CapSampler):
    """The declared fault injection: the diagnostic fetch fails. The
    snapshot's own over-hold decision is unaffected (the subclass
    changes nothing else); the pin is whether the cap record survives
    the diagnostic's failure."""

    async def _slot_details(self, bucket: str) -> list[dict[str, object]]:
        raise RuntimeError("the diagnostic fetch exploded")


async def _cleanup_staged_cap_slots(
    conn: asyncpg.Connection, schema: str, holder: str, *, primary: bool
) -> None:
    """The staged cap slots' cleanup with precise error precedence: with
    NO active test exception a cleanup failure raises loudly (the
    cleanup is the test's own guarantee); with a primary failure the
    cleanup's own failure is printed in full and the primary exception
    is preserved - the cleanup never masks the test's real red, and a
    green test never turns red before the guilty cleanup has run and
    named itself. The deletion is scoped to the exact bucket and the
    exact staged holder; the zero-leftover assertion holds on the
    normal path."""
    try:
        await conn.execute(
            f'DELETE FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND held_by_worker_id = $2::uuid",
            "taskq:global:queue:roll_capped",
            holder,
        )
        leftovers = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND held_by_worker_id = $2::uuid",
            "taskq:global:queue:roll_capped",
            holder,
        )
        if leftovers:
            raise AssertionError(
                f"the staged slot rows survived their own cleanup: {leftovers} row(s) remain"
            )
    except Exception:
        if primary:
            print(
                "the staged cap slots' cleanup failed (the primary test "
                "failure is preserved); the cleanup's own failure:"
            )
            traceback.print_exc()
        else:
            raise


async def test_cap_overhold_record_survives_a_failed_diagnostic_fetch(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: Any, pg_dsn: str
) -> None:
    """A cap violation is observed by the snapshot; a failed diagnostic
    fetch must never swallow it. With the faulted details, the tick
    still records the over-hold (the held count and the fetch's own
    error both named in the violation), and the tick does not raise a
    generic sampler error in the record's place."""
    schema = module_pg_schema.schema_name
    sampler = _FaultedDetailsSampler(pg_dsn, schema)
    holder = str(new_uuid())
    flow_id = await seed_flow(clean_pg_conn, schema)
    job_id = await seed_running_node(clean_pg_conn, schema, flow_id)
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent",
        "roll_capped",
        _QUEUE_CAP,
    )
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET queue = (SELECT name FROM "{schema}".queues WHERE name = $1) '
        "WHERE id = $2::uuid",
        "roll_capped",
        job_id,
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".reservation_slots '
        "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
        "VALUES ('taskq:global:queue:roll_capped', 0, $1::uuid, $2::uuid, "
        "statement_timestamp(), statement_timestamp() + interval '60s')",
        job_id,
        holder,
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".reservation_slots '
        "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
        "VALUES ('taskq:global:queue:roll_capped', 1, $1::uuid, $2::uuid, "
        "statement_timestamp(), statement_timestamp() + interval '60s')",
        job_id,
        holder,
    )
    try:
        await sampler._tick()  # must not raise: the violation is the record
        overhold = next(
            (v for v in sampler.violations if "slots:taskq:global:queue:roll_capped" in v), ""
        )
        assert overhold, (
            f"the cap record was swallowed by the diagnostic's failure: {sampler.violations}"
        )
        assert "2 > bound 1" in overhold, overhold
        assert "detail_error" in overhold and "the diagnostic fetch exploded" in overhold, overhold
        assert not any(v.startswith("sampler error") for v in sampler.violations), (
            f"the generic sampler error replaced the cap record: {sampler.violations}"
        )
    finally:
        primary_active = sys.exc_info()[0] is not None
        if sampler._conn is not None:
            with contextlib.suppress(Exception):
                await sampler._conn.close()
        await _cleanup_staged_cap_slots(
            clean_pg_conn,
            schema,
            holder,
            primary=primary_active,
        )


async def test_cap_sampler_tick_is_one_consistent_snapshot(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: Any, pg_dsn: str
) -> None:
    """One tick, one observable state: the cap receipt requires the
    capped running row and the live cap slot in the SAME snapshot, so a
    stale slot alone (the post-completion shape a torn two-statement
    read could pair with an empty job view) records no receipt and no
    violation, while the over-hold tooth still trips on any tick."""
    schema = module_pg_schema.schema_name
    sampler = CapSampler(pg_dsn, schema)
    holder = str(new_uuid())
    flow_id = await seed_flow(clean_pg_conn, schema)
    job_id = await seed_running_node(clean_pg_conn, schema, flow_id)
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent",
        "roll_capped",
        _QUEUE_CAP,
    )
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET queue = (SELECT name FROM "{schema}".queues WHERE name = $1) '
        "WHERE id = $2::uuid",
        "roll_capped",
        job_id,
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".reservation_slots '
        "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
        "VALUES ('taskq:global:queue:roll_capped', 0, $1::uuid, $2::uuid, "
        "statement_timestamp(), statement_timestamp() + interval '60s')",
        job_id,
        holder,
    )
    try:
        # The admitted pair, one snapshot: the receipt lands at sample 1.
        await sampler._tick()
        assert sampler.cap_receipt_at == 1, (
            f"the admitted pair did not mint the receipt: {sampler.peak}"
        )
        assert sampler.peak["peak.capped_queue"] == 1
        assert sampler.peak["peak.slots:taskq:global:queue:roll_capped"] == 1
        assert not sampler.violations

        # The torn shape's rejection: the job gone, the slot's lease
        # still live. One snapshot sees the truth: no admission, no
        # receipt, no violation.
        await clean_pg_conn.execute(f'DELETE FROM "{schema}".jobs WHERE id = $1::uuid', job_id)
        await sampler._tick()
        assert sampler.cap_receipt_at == 1, "a stale slot minted a receipt"
        assert sampler.samples == 2
        assert not sampler.violations

        # The tooth: a slot over-hold (held 2 at the cap of 1) trips on
        # any tick, whatever the job view says.
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".reservation_slots '
            "(bucket_name, slot_index, job_id, held_by_worker_id, acquired_at, lease_expires_at) "
            "VALUES ('taskq:global:queue:roll_capped', 1, $1::uuid, $2::uuid, "
            "statement_timestamp(), statement_timestamp() + interval '60s')",
            job_id,
            holder,
        )
        await sampler._tick()
        overhold = next(
            (v for v in sampler.violations if "slots:taskq:global:queue:roll_capped" in v), ""
        )
        assert overhold, f"the over-hold tooth never tripped: {sampler.violations}"
        assert "'slot_index': 0" in overhold and "'slot_index': 1" in overhold, overhold
        assert holder in overhold, overhold
        assert "'job_status': None" in overhold, (
            f"the joined job state must name itself (a deleted job's null "
            f"status is the leaked-slot shape): {overhold}"
        )
        assert sampler.cap_receipt_at == 1
    finally:
        # The active exception, read at finally entry BEFORE any await
        # (the first await can clear exc_info's view of the primary):
        # the cleanup's error behavior keys on it.
        primary_active = sys.exc_info()[0] is not None
        if sampler._conn is not None:
            with contextlib.suppress(Exception):
                await sampler._conn.close()
        # The staged slot rows are this test's own: deleted by the exact
        # bucket AND the exact staged holder on every path, so no
        # residue can turn a real scenario's first sample into a false
        # over-hold. Never a bare bucket deletion: a real scenario's
        # slots name real workers.
        await _cleanup_staged_cap_slots(clean_pg_conn, schema, holder, primary=primary_active)


async def test_staged_cleanup_error_precedence(
    clean_pg_conn: asyncpg.Connection, module_pg_schema: Any, pg_dsn: str, tmp_path: Any
) -> None:
    """The cleanup helper's error precedence, with the fault declared:
    the cleanup's connection is closed before each call. With no
    primary failure, the cleanup failure raises loudly (the green test
    turns red only when the guilty cleanup itself failed); with a
    primary failure, the cleanup's own failure is printed in full and
    the primary exception is preserved (the helper returns, the caller
    re-raises the primary)."""
    schema = module_pg_schema.schema_name
    holder = str(new_uuid())
    dead_conn = await asyncpg.connect(pg_dsn)
    await dead_conn.close()

    with pytest.raises(asyncpg.exceptions.InterfaceError, match="closed"):
        await _cleanup_staged_cap_slots(dead_conn, schema, holder, primary=False)

    # The primary path: the cleanup's failure is printed (named), the
    # helper returns, and the primary error is the caller's to raise.
    primary = AssertionError("the primary test failure stands")
    await _cleanup_staged_cap_slots(dead_conn, schema, holder, primary=True)
    with pytest.raises(AssertionError, match="the primary test failure stands"):
        raise primary

    # The healthy path on the live connection: nothing staged, nothing
    # raised.
    await _cleanup_staged_cap_slots(clean_pg_conn, schema, holder, primary=False)

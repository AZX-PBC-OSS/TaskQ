# ruff: noqa: S608  # Why: every interpolated identifier is this module's own generated schema name (a fresh per-test schema, or the module fixture's), never operator input; all values are $-bound.
"""Missed leader elections: the dormant gap and the split brain.

Leadership is the single point that runs every sweep (the reclaim, the
deadline, the reservation-slot release, the retention deletes). Two
failure shapes around the election itself are pinned here against a real
Postgres, because the lease statements' sequential contracts (pinned in
``test_leader_lease_contract.py``) do not by themselves settle them:

* the DORMANT GAP: the leader dies silently (no FIN, no resign) and the
  lease lapses while every surviving process reports healthy. The pins
  measure the sweep-silent window on a live fleet and bound it: the
  takeover is refused while the lease lives (never two leaders) and
  lands within ``leader_lease + heartbeat_interval`` once it lapses.
  The follower's election loop is free-running (it reads only the row
  and its own heartbeat clock, nothing the leader emits), which is what
  makes the bound a derived constant rather than "whenever a leader
  tick happens to wake a follower". The row's ``expires_at`` flip is
  the monitor-visible boundary, and it precedes the takeover.
* the SPLIT BRAIN: the old leader's fence renewal racing the new
  leader's takeover. The fences are proven under the race - an
  uncommitted renewal holding the row lock while the takeover's elect
  blocks behind it, and the mirror ordering - not just sequentially:
  exactly one of the two statements may ever win, the loser reads the
  winner's row and matches nothing, and the deposed holder's late
  resign deletes nothing.

The prune family's drain gate (``_batch_drain_gate``) is pinned to stop
at the demotion boundary the same way ``_drain_bounded`` does for the
every-tick sweeps: a leader that demotes mid-drain must not keep
running leader-only retention deletes (or holding the schema's prune
session-advisory lock) past the batch it already committed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_worker
from taskq.worker.deps import LeaderTerm
from taskq.worker.health import build_ready_body, compute_health
from taskq.worker.leader import MaintenanceLeader, build_leader_lease_sql
from tests._fleet import Fleet, Pod, open_fleet
from tests._leader_stub_deps import stub_deps

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_QUEUE = "attack_gap_q"
_ACTOR = "attack_gap_actor"

# The gap bound the pins measure against: a lease short enough to watch
# lapse, a heartbeat short enough that the follower's cadence is not the
# bottleneck, a sweep interval that makes the reclaim's resumption
# visible within the test's budget. The settings floors (heartbeat >=
# 0.5, sweep >= 1.0) are the honest minimums.
_LEASE_SECS = 2.0
_HB_SECS = 0.5
_SWEEP_SECS = 1.0
_GAP_SETTINGS = {
    "heartbeat_interval": str(_HB_SECS),
    "leader_lease": str(_LEASE_SECS),
    "sweep_interval": str(_SWEEP_SECS),
}

# The lease-contract shapes use generous horizons: liveness states are
# arranged by backdating, never by sleeping (the lease contract suite's
# doctrine).
_LONG_LEASE = 3600.0
_LONG_SLACK = 3600.0

_TERMINAL_STATUSES = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


def _start_leader(pod: Pod) -> tuple[MaintenanceLeader, asyncio.Event, asyncio.Task[None]]:
    leader = MaintenanceLeader(pod.deps, pod.worker_id, pod.backend, clock=SystemClock())
    stop = asyncio.Event()
    return leader, stop, asyncio.create_task(leader.run(stop))


async def _kill_silently(leader: MaintenanceLeader, task: asyncio.Task[None]) -> None:
    """Take the leader down the way a SIGKILL does: no resign, no FIN.

    ``run``'s finally would hand the lease back; a killed process never
    runs its finally. Stubbing ``resign`` is the honest simulation: the
    row survives with a future ``expires_at`` and must lapse on its own.
    """

    async def _no_resign() -> bool:
        return False

    leader.resign = _no_resign  # type: ignore[method-assign]
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _row_takeable(fleet: Fleet) -> bool:
    """Whether the leader row's liveness signals have both lapsed.

    The same predicate the takeover uses, readable by any monitor: this
    is the earliest instant leader-absence is visible in the database.
    """
    rows = await fleet.fetch(
        "SELECT expires_at < clock_timestamp() AS takeable, "
        "last_seen_at < clock_timestamp() - make_interval(secs => $1) AS ping_stopped "
        'FROM "{schema}".maintenance_leader WHERE singleton = true',
        4 * _HB_SECS,
    )
    if not rows:
        return False
    return bool(rows[0]["takeable"]) and bool(rows[0]["ping_stopped"])


async def _job_reclaimed(fleet: Fleet, job_id: UUID) -> bool:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    if not rows or rows[0]["status"] != "pending":
        return False
    events = await fleet.fetch(
        'SELECT count(*) AS n FROM "{schema}".job_events WHERE job_id = $1 '
        "AND kind = 'state_change' AND detail->>'reason' = 'lock_expired'",
        job_id,
    )
    return int(events[0]["n"]) == 1


async def _strand_one_job(fleet: Fleet, pod: Pod) -> UUID:
    """A real stranded row: claimed by *pod*, its lock backdated past the
    reclaim's lease arm."""
    (job_id,) = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE)
    claimed = await pod.claim([_QUEUE], 1)
    assert len(claimed) == 1
    await fleet.fetch(
        'UPDATE "{schema}".jobs SET lock_expires_at = clock_timestamp() - '
        "interval '1 second', last_heartbeat_at = clock_timestamp() - "
        "interval '1 second' WHERE id = $1",
        job_id,
    )
    return job_id


# ── the dormant gap ────────────────────────────────────────────────────


async def test_silent_leader_death_gaps_sweeps_for_at_most_lease_plus_heartbeat(
    pg_dsn: str,
) -> None:
    """A leader that dies without a FIN strands rows for a bounded window.

    The bound is derived, not aspirational: the lease must lapse on the
    server clock (nothing shortens it, the holder is dead), then the
    follower's own election cadence (one heartbeat interval) reaches the
    lapsed row. The pin asserts BOTH halves of the contract: no early
    takeover while the lease lives (that would be two leaders), and no
    late takeover past lease + heartbeat (that would be the dormant gap
    the fleet cannot see). The stranded row is a real running job with a
    lapsed lock, and the window ends when the NEW leader's reclaim
    actually lands on it.
    """
    schema = f"attack_gap_bound_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_GAP_SETTINGS,
    ) as fleet:
        leader1, stop1, task1 = _start_leader(fleet.pod("pod-1"))
        leader2, stop2, task2 = _start_leader(fleet.pod("pod-2"))
        try:
            # Either pod may win the race; the pin is about the ROLE, not
            # the identity.
            await wait_for_condition(
                lambda: (
                    fleet.pod("pod-1").deps.is_leader.is_set()
                    or fleet.pod("pod-2").deps.is_leader.is_set()
                ),
                description="a pod won the leader election",
                timeout=10.0,
            )
            if fleet.pod("pod-1").deps.is_leader.is_set():
                leader_pod, follower_pod = "pod-1", "pod-2"
                dying_leader, dying_task = leader1, task1
            else:
                leader_pod, follower_pod = "pod-2", "pod-1"
                dying_leader, dying_task = leader2, task2
            follower = fleet.pod(follower_pod)

            job_id = await _strand_one_job(fleet, fleet.pod(leader_pod))

            await _kill_silently(dying_leader, dying_task)
            t_kill = time.monotonic()

            # Phase 1: inside the lease, NOBODY may take the row. The old
            # leader is gone; the row is the only authority, and it still
            # says held.
            await asyncio.sleep(_LEASE_SECS * 0.5)
            assert not follower.deps.is_leader.is_set(), (
                "a follower took the role while the dead leader's lease was still "
                "live: two leaders were acting at once"
            )
            # The monitor boundary: the follower's ready body exposes its
            # non-leadership.
            report = await compute_health(follower.deps)
            body = json.loads(build_ready_body(report, follower.deps))
            assert body["is_leader"] is False

            # Phase 2: the takeability flip is bounded by lease + hb. The
            # flip is what a fleet-wide monitor (or an operator's probe)
            # reads; it must not depend on any leader-side tick. The
            # takeable state is TRANSITORY: the phase-locked healthy
            # follower legally re-elects within milliseconds of the flip
            # and renews the row, so a poller sampling only the row would
            # usually miss the very window it exists to prove. Either
            # signal settles the same bound: the row reads takeable, or
            # the follower HAS taken the role - which only a takeable row
            # permits, so the flip is implied. (The frozen-follower pin
            # below keeps the row-only shape honest for a fleet whose
            # election loop is broken: there the flip must be visible
            # with no follower election running at all.) The event is
            # tested FIRST: a ``_row_takeable(fleet) or X`` lambda
            # short-circuits on the coroutine object's truthiness and
            # would never reach the event.
            await wait_for_condition(
                lambda: follower.deps.is_leader.is_set() or _row_takeable(fleet),
                description=(
                    "the leader row's lease lapsed on the server clock, "
                    "or the follower has taken the role"
                ),
                timeout=_LEASE_SECS + _HB_SECS + 1.0,
            )

            # Phase 3: the follower's free-running election takes over
            # within lease + heartbeat + one round trip.
            await wait_for_condition(
                lambda: follower.deps.is_leader.is_set(),
                description="the follower took over after the lease lapsed",
                timeout=_LEASE_SECS + _HB_SECS + 1.5,
            )
            t_takeover = time.monotonic()
            assert t_takeover - t_kill <= _LEASE_SECS + _HB_SECS + 1.0, (
                "failover took longer than leader_lease + heartbeat_interval + slack: "
                "the fleet's sweeps were silent past the derived bound"
            )

            # Phase 4: the new leader's sweep lands on the stranded row -
            # the sweep-silent window closes where it matters.
            await wait_for_condition(
                lambda: _job_reclaimed(fleet, job_id),
                description="the stranded job was reclaimed by the new leader",
                timeout=_SWEEP_SECS + 3.0,
            )
            t_reclaimed = time.monotonic()
            assert t_reclaimed - t_kill <= _LEASE_SECS + _HB_SECS + _SWEEP_SECS + 2.0, (
                "the reclaim landed past lease + heartbeat + sweep_interval: rows "
                "strand longer than the fleet's own failover bound"
            )
        finally:
            stop1.set()
            stop2.set()
            await asyncio.gather(task1, task2, return_exceptions=True)


async def test_frozen_follower_election_strands_rows_in_plain_sight(
    pg_dsn: str,
) -> None:
    """With the follower's election frozen, the gap is unbounded - and the
    row says so.

    The misconfigured fleet (jitter/backoff wedged, a follower's election
    loop hung) has no in-band recovery: nothing in the system can elect a
    leader if no election loop runs. What the system owes an operator is
    the detectable boundary: the leader row's liveness signals lapse on
    the SERVER clock within lease + heartbeat of the death, independent
    of every follower, and the strand itself (a running job no sweep will
    reclaim) stays visible. This pins the boundary without a follower
    election loop running at all - the takeability flip must not depend
    on the thing that is broken.
    """
    schema = f"attack_gap_frozen_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_GAP_SETTINGS,
    ) as fleet:
        leader1, stop1, task1 = _start_leader(fleet.pod("pod-1"))
        try:
            await wait_for_condition(
                lambda: fleet.pod("pod-1").deps.is_leader.is_set(),
                description="pod-1 won the leader election",
                timeout=10.0,
            )
            job_id = await _strand_one_job(fleet, fleet.pod("pod-1"))
            await _kill_silently(leader1, task1)

            # The election on pod-2 is frozen: never started. Give the
            # fleet well past the bound.
            await asyncio.sleep(_LEASE_SECS + _HB_SECS + 1.0)

            assert not fleet.pod("pod-2").deps.is_leader.is_set()
            assert await _row_takeable(fleet), (
                "the leader row did not lapse within lease + heartbeat of the "
                "silent death: the monitor boundary depends on a follower tick"
            )
            rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
            assert rows[0]["status"] == "running", (
                "the stranded job's state changed with no leader anywhere: "
                "something is sweeping without the role"
            )
        finally:
            stop1.set()
            await asyncio.gather(task1, return_exceptions=True)


# ── the split brain: the fence under the race ──────────────────────────


async def _elect_holder(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> object | None:
    elect_sql, _, _ = build_leader_lease_sql(schema)
    return await conn.fetchval(elect_sql, worker_id, _LONG_LEASE, _LONG_SLACK)


async def test_renewal_racing_a_takeover_cannot_produce_two_leaders(
    module_pg_schema: ModulePgSchema,
) -> None:
    """The old leader's last renewal and the new leader's elect, both orders.

    Sequentially the fences are trivial. Under the race they must still
    hold, and the race is forced deterministically with an uncommitted
    transaction holding the singleton row's lock while the rival
    statement blocks behind it:

    * renewal first: the renewal commits, pushing ``expires_at`` out -
      the blocked elect must re-evaluate against the fresh row and win
      NOTHING (the takeover is refused; one leader).
    * elect first: the takeover commits - the blocked renewal must
      re-evaluate against the successor's row and match nothing (the old
      leader stands down), and the old leader's late resign, fenced on
      its dead term, must delete nothing.
    """
    schema = module_pg_schema.schema_name
    conn_a = await asyncpg.connect(module_pg_schema.pg_dsn)
    conn_b = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        a_id, b_id = new_uuid(), new_uuid()
        await create_worker(conn_a, schema, a_id)
        await create_worker(conn_a, schema, b_id)
        elect_sql, renew_sql, resign_sql = build_leader_lease_sql(schema)

        # ── order 1: the renewal holds the row lock, the elect blocks ──
        await conn_a.execute(f'DELETE FROM "{schema}".maintenance_leader')
        elected_at = await _elect_holder(conn_a, schema, a_id)
        assert elected_at is not None
        tx_a = conn_a.transaction()
        await tx_a.start()
        renewed = await conn_a.fetchval(renew_sql, a_id, elected_at, _LONG_LEASE)
        assert renewed is not None, "a live holder must be able to renew"
        elect_task = asyncio.create_task(conn_b.fetchval(elect_sql, b_id, _LONG_LEASE, _LONG_SLACK))
        await asyncio.sleep(0.3)  # the elect is now parked on the row lock
        await tx_a.commit()
        assert await asyncio.wait_for(elect_task, 5.0) is None, (
            "the blocked elect took the row a live renewal had just re-stamped: "
            "two leaders for one instant"
        )
        row = await conn_a.fetchrow(
            f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
        )
        assert row is not None and row["worker_id"] == a_id

        # ── order 2: the elect holds the row lock, the renewal blocks ──
        # The row must be takeable for the elect to win: backdate BOTH
        # liveness signals past their horizons.
        await conn_a.execute(
            f'UPDATE "{schema}".maintenance_leader SET '
            "expires_at = clock_timestamp() - interval '1 hour', "
            "last_seen_at = clock_timestamp() - interval '1 hour'"
        )
        tx_b = conn_b.transaction()
        await tx_b.start()
        taken = await conn_b.fetchval(elect_sql, b_id, _LONG_LEASE, _LONG_SLACK)
        assert taken is not None, "a fully lapsed row must be takeable"
        renew_task = asyncio.create_task(conn_a.fetchval(renew_sql, a_id, elected_at, _LONG_LEASE))
        await asyncio.sleep(0.3)  # the renewal is now parked on the row lock
        await tx_b.commit()
        assert await asyncio.wait_for(renew_task, 5.0) is None, (
            "the deposed holder's renewal landed on the successor's row: the "
            "term fence does not hold under the takeover race"
        )
        # The old leader's late resign, fenced on its dead term, deletes
        # nothing of the successor's.
        tag = await conn_a.execute(resign_sql, a_id, elected_at)
        assert tag.split()[-1] == "0", "a deposed leader's resign deleted the successor's row"
        row = await conn_a.fetchrow(
            f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
        )
        assert row is not None and row["worker_id"] == b_id

        # ── order 3: a lapsed lease is not renewable at all ── the row
        # still names A (its ping is fresh: no peer has taken it yet),
        # but the lease itself has run out; the liveness clause is what
        # stops the holder from silently extending a lease the fleet was
        # already entitled to take over.
        await conn_a.execute(f'DELETE FROM "{schema}".maintenance_leader')
        elected_at = await _elect_holder(conn_a, schema, a_id)
        assert elected_at is not None
        await conn_a.execute(
            f'UPDATE "{schema}".maintenance_leader SET '
            "expires_at = clock_timestamp() - interval '1 hour'"
        )
        assert await conn_a.fetchval(renew_sql, a_id, elected_at, _LONG_LEASE) is None, (
            "a lease lapsed at the server was renewed: the holder bypassed the "
            "takeover horizon every peer competes on"
        )
    finally:
        await conn_a.close()
        await conn_b.close()


async def test_takeover_racing_a_mid_sweep_reclaim_lands_exactly_once(
    pg_dsn: str,
) -> None:
    """The old leader's sweep transaction commits after the new leader's
    same sweep ran: no double reclaim, no double audit rows.

    The old leader's reclaim holds the job row's lock (uncommitted) when
    the takeover happens; the new leader's identical sweep runs
    concurrently. The two statements must arbitrate on the row locks:
    the second reads either a skipped (locked) or an already-reclaimed
    row, so the job is reclaimed once, one attempt row is written, one
    event is emitted - the double effect a split brain would produce is
    structurally absent.
    """
    schema = f"attack_gap_reclaim_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_GAP_SETTINGS,
    ) as fleet:
        job_id = await _strand_one_job(fleet, fleet.pod("pod-1"))

        conn_old = await asyncpg.connect(fleet.dsn)
        conn_new = await asyncpg.connect(fleet.dsn)
        try:
            from taskq.backend.postgres import PostgresBackend

            grace = timedelta(seconds=0)
            cap = timedelta(hours=1)

            # The OLD leader's sweep, uncommitted, holding the job's row
            # lock across the "takeover".
            tx_old = conn_old.transaction()
            await tx_old.start()
            old_rows = await PostgresBackend.sweep_expired_locks(
                conn_old, grace, grace, schema=schema, max_retry_backoff=cap
            )
            assert old_rows == 1, "the old leader's sweep must reclaim the expired row"

            # The NEW leader's same sweep, concurrent with the old one's
            # uncommitted transaction.
            new_rows = await PostgresBackend.sweep_expired_locks(
                conn_new, grace, grace, schema=schema, max_retry_backoff=cap
            )
            assert new_rows == 0, (
                "the concurrent sweep reclaimed rows the old leader's "
                "uncommitted transaction already held: a double reclaim was "
                "possible"
            )

            await tx_old.commit()

            # Exactly once, in the audit surfaces as well as the state.
            attempts = await fleet.fetch(
                'SELECT count(*) AS n FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            assert int(attempts[0]["n"]) == 1, (
                "two attempt rows for one reclaim: the race landed double"
            )
            events = await fleet.fetch(
                'SELECT count(*) AS n FROM "{schema}".job_events WHERE job_id = $1 '
                "AND kind = 'state_change' AND detail->>'reason' = 'lock_expired'",
                job_id,
            )
            assert int(events[0]["n"]) == 1, "two reclaim events for one reclaim"
            states = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
            assert states[0]["status"] == "pending"

            # And a THIRD sweep (the successor's next tick) reclaims
            # nothing: the effect is idempotent after the fact too.
            again = await PostgresBackend.sweep_expired_locks(
                conn_new, grace, grace, schema=schema, max_retry_backoff=cap
            )
            assert again == 0
        finally:
            await conn_old.close()
            await conn_new.close()


# ── the flapping leader: demotion fences the pod's own writes ──────────


async def test_flapping_renewal_demotes_but_never_yields_the_row_to_a_peer(
    pg_dsn: str,
) -> None:
    """Transient renewal failures demote at the trust boundary, and the
    row never becomes a peer's to take while the flapping holder can
    still reach the database.

    A renewal failing transiently must NOT demote instantly (the trust
    window exists so one blip does not churn leadership), must demote
    once the trust is spent (a holder that cannot renew must not keep
    the flag), and the demoted pod's own-row re-election is what keeps
    the row alive for its own recovery - which means a peer racing the
    flap can never win the row the flapping holder still reaches. The
    peer's elect is attempted on a real connection throughout the flap.
    """
    schema = f"attack_gap_flap_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_GAP_SETTINGS,
    ) as fleet:
        pod = fleet.pod("pod-1")
        leader = MaintenanceLeader(pod.deps, pod.worker_id, pod.backend, clock=SystemClock())
        stop = asyncio.Event()
        task = asyncio.create_task(leader.run(stop))
        peer_conn = await asyncpg.connect(fleet.dsn)
        try:
            await wait_for_condition(
                lambda: pod.deps.is_leader.is_set(),
                description="pod-1 won the leader election",
                timeout=10.0,
            )

            step_downs: list[str] = []
            original_step_down = leader._step_down
            original_renew = leader._renew_lease

            async def _spy_step_down(reason: str, *, error: str | None = None) -> None:
                step_downs.append(reason)
                await original_step_down(reason, error=error)

            flap_until = time.monotonic() + 2.5

            async def _flappy_renew(term: LeaderTerm, renew_sql: str) -> LeaderTerm | None:
                if time.monotonic() < flap_until:
                    raise asyncpg.PostgresConnectionError("flap")
                return await original_renew(term, renew_sql)

            leader._step_down = _spy_step_down  # type: ignore[method-assign]
            leader._renew_lease = _flappy_renew  # type: ignore[method-assign]

            # Race a peer's elect against the whole flap window.
            peer_won = False
            elect_sql, _, _ = build_leader_lease_sql(schema)
            peer_id = new_uuid()
            await create_worker(peer_conn, schema, peer_id)
            flap_end = time.monotonic() + 2.5
            while time.monotonic() < flap_end:
                got = await peer_conn.fetchval(elect_sql, peer_id, _LONG_LEASE, _LONG_SLACK)
                if got is not None:
                    peer_won = True
                    break
                await asyncio.sleep(0.1)

            assert not peer_won, (
                "a peer won the leader row while the holder was flapping on "
                "renewals it could still reach: leadership churned to a pod "
                "the row's live lease forbade"
            )
            assert step_downs, (
                "the flapping holder never demoted: the trust window became a "
                "license to keep the flag with a renewal that never lands"
            )
            assert any(reason in {"renew_failed", "trust_expired"} for reason in step_downs), (
                f"unexpected step-down reasons: {step_downs}"
            )

            # The flap ends: the same holder recovers on its own row - no
            # whole-lease stall, no peer in between.
            await wait_for_condition(
                lambda: pod.deps.is_leader.is_set(),
                description="the flapping holder re-assumed leadership after recovery",
                timeout=5.0,
            )
            row = await peer_conn.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
            )
            assert row is not None and row["worker_id"] == pod.worker_id
        finally:
            stop.set()
            await asyncio.gather(task, return_exceptions=True)
            await peer_conn.close()


# ── the demotion boundary for the prune-family drains ──────────────────


def _gate_ctx(is_leader: asyncio.Event) -> SimpleNamespace:
    stub = SimpleNamespace(
        is_leader=is_leader,
        liveness=SimpleNamespace(tick=lambda *args, **kwargs: None),
    )
    stub_deps(stub)
    return SimpleNamespace(deps=stub)


def test_prune_drain_gate_stops_on_demotion() -> None:
    """A demotion mid-drain stops the prune-family drain at the batch
    boundary, the same contract ``_drain_bounded`` pins for the
    every-tick sweeps: committed batches stay committed, nothing further
    starts under a pod that no longer leads.
    """
    from taskq.worker._leader_sweeps import _batch_drain_gate

    is_leader = asyncio.Event()
    is_leader.set()
    shutdown = asyncio.Event()
    ctx = _gate_ctx(is_leader)
    gate = _batch_drain_gate(
        cast("object", ctx), shutdown, loop_name="leader.prune", period_secs=1.0
    )
    assert gate() is True
    is_leader.clear()  # demoted mid-drain
    assert gate() is False, (
        "the prune drain kept running leader-only work after demotion: the "
        "retention deletes and the schema's prune advisory lock outlived the "
        "authority that started them"
    )


def test_prune_drain_gate_still_stops_on_shutdown() -> None:
    """The demotion check is additive: shutdown still gates the drain."""
    from taskq.worker._leader_sweeps import _batch_drain_gate

    is_leader = asyncio.Event()
    is_leader.set()
    shutdown = asyncio.Event()
    shutdown.set()
    ctx = _gate_ctx(is_leader)
    gate = _batch_drain_gate(
        cast("object", ctx), shutdown, loop_name="leader.prune", period_secs=1.0
    )
    assert gate() is False


async def test_demoted_leader_stops_pruning_mid_drain(pg_dsn: str) -> None:
    """Through the real prune drain: a demotion between batches leaves the
    remaining prunable rows in place, committed batches stay committed.

    The drain's gate is consulted before every batch; clearing the
    leadership flag mid-drain must stop the drain with rows remaining -
    the successor's prune resumes them, the deposed leader does not keep
    deleting.
    """
    from taskq.worker._leader_shared import prune_terminal_jobs
    from taskq.worker._leader_sweeps import _batch_drain_gate

    schema = f"attack_gap_prune_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_GAP_SETTINGS,
    ) as fleet:
        # One prunable row per terminal status (the drain visits statuses
        # in frozenset order), each far past retention; the batch size is
        # 1, so the demotion between batches has rows left to stop on.
        ids = await fleet.enqueue(len(_TERMINAL_STATUSES), actor=_ACTOR, queue=_QUEUE)
        for job_id, status in zip(ids, _TERMINAL_STATUSES, strict=True):
            await fleet.fetch(
                'UPDATE "{schema}".jobs SET status = $2::"{schema}".job_status, '
                "finished_at = clock_timestamp() - interval '1 hour', "
                "created_at = clock_timestamp() - interval '2 hours' WHERE id = $1",
                job_id,
                status,
            )

        is_leader = asyncio.Event()
        is_leader.set()
        shutdown = asyncio.Event()
        stub = SimpleNamespace(
            is_leader=is_leader,
            worker_id=fleet.pod("pod-1").worker_id,
            liveness=SimpleNamespace(tick=lambda *args, **kwargs: None),
            settings=fleet.pod("pod-1").deps.settings,
        )
        stub_deps(stub)
        gate_ctx = SimpleNamespace(deps=stub)
        gate = _batch_drain_gate(
            cast("object", gate_ctx), shutdown, loop_name="leader.prune", period_secs=30.0
        )

        # The drain outruns any external demotion, so the demotion is
        # delivered through the seam the drain itself consults: the first
        # gate call admits batch 1, the second IS the demotion instant,
        # and the underlying gate must then refuse everything further.
        gate_calls = {"n": 0}

        def demote_after_first_batch() -> bool:
            gate_calls["n"] += 1
            if gate_calls["n"] == 2:
                is_leader.clear()
            return gate()

        conn = await asyncpg.connect(fleet.dsn)
        try:
            # Batch 1 commits, then the demotion lands, then the gate
            # must refuse batch 2.
            result = await prune_terminal_jobs(
                conn,
                retention_per_status={
                    status: timedelta(seconds=1) for status in _TERMINAL_STATUSES
                },
                archive_retention=timedelta(hours=24),
                schema=schema,
                actor_overrides=None,
                drain_gate=demote_after_first_batch,
                batch_size=1,
            )
            pruned_before_demotion = sum(result.by_status.values())
            assert pruned_before_demotion == 1, (
                f"expected exactly one committed batch before the demotion, got "
                f"{pruned_before_demotion} ({result.by_status})"
            )
            assert gate_calls["n"] == len(_TERMINAL_STATUSES) + 1, (
                f"the drain consulted its gate {gate_calls['n']} times: expected "
                "two calls for the pruned status (admit, then the demotion "
                "refusal) plus one refused visit per remaining status - a "
                "drain that keeps admitting batches past the demotion lands "
                "elsewhere in this count"
            )
            remaining = await fleet.fetch(
                'SELECT count(*) AS n FROM "{schema}".jobs WHERE status::text = ANY('
                "ARRAY['succeeded','failed','cancelled','crashed','abandoned'])"
            )
            remaining_after = int(remaining[0]["n"])
            assert remaining_after == 4, (
                f"{remaining_after} prunable rows were deleted past the demotion "
                "boundary: the drain kept running leader-only work after the "
                "pod stopped leading"
            )
        finally:
            await conn.close()

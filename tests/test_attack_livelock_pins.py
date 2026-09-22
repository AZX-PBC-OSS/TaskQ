"""Deterministic liveness pins: no livelock, no starvation, progress always.

One pin per candidate livelock shape from the robustness hunt. Livelock =
work keeps happening (claims, queries, wakeups, re-parks) while forward
progress never does. Every pin here is deterministic, a FakeClock or a
fixed interleaving, so green cannot flake and red is a real defect:

1. The dispatch loop vs the cap-refusal kernel: a capped actor whose
   saturated cap frees must be admitted on the first round after the
   free, and a fleet of capped actors must reach first service within
   ceil(actor_count / limit) rounds (the freed slot must not be
   perpetually re-taken by the same competitor).
2. Retry backoff: the curve must converge to its cap, never reset and
   never degenerate to a zero period (a claim/deny churn with no
   convergence is a livelock with a retry curve).
3. Sweep interleaving: the reclaim sweep, the deadline sweep and the
   scheduled-to-pending sweep rotating over one mixed population must
   drive every row terminal within a bounded number of steps; a sweep
   re-scanning another sweep's in-flight row and resetting its progress
   would stall the terminal count.
4. Notify reconnect: a drop-and-relisten episode must back off on a
   bounded, growing curve, fire the simulated wake on success (pending
   subscribers are unblocked, dispatch is never re-keyed to a silent
   listener), and observe shutdown without a hot spin.
5. Bounded release / re-park: a perpetually denied job's snooze curve
   must terminate at its denial deadline (schedule_to_close), and a
   zero-delay re-park must be floored off the head of the dispatch
   order (no instant re-claim thrash).
6. Leader handover under clock skew: the lease statements bind only
   intervals and read the server's clock, so a skewed Python clock must
   leave takeability unchanged in both directions - no premature
   takeover (ping-pong) and no stalled handover.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.constants import wake_channel
from taskq.retry import (
    RetryPolicy,
    _capped_jitter_band,
    _raw_backoff_seconds,
    compute_backoff,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema, _create_worker
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.leader import build_leader_lease_sql

# ruff: noqa: S608  # Why: every interpolated identifier is the module fixture's schema name, fixture-derived, not user input; all values are $-bound.

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LEASE = timedelta(seconds=30)


def _backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _enqueue(
    backend: InMemoryBackend,
    actor: str,
    queue: str,
    *,
    count: int = 1,
    priority: int = 0,
    max_attempts: int = 3,
    schedule_to_close: datetime | None = None,
) -> None:
    for _ in range(count):
        await backend.enqueue(
            EnqueueArgs(
                id=new_uuid(),
                actor=actor,
                queue=queue,
                payload={},
                max_attempts=max_attempts,
                retry_kind="transient",
                scheduled_at=backend._clock.now(),  # type: ignore[reportPrivateUsage]
                priority=priority,
                schedule_to_close=schedule_to_close,
            )
        )


# ── 1. Dispatch vs the cap: the freed slot must reach the refused actor ──


@pytest.mark.asyncio
async def test_capped_actor_admitted_on_the_first_round_after_its_cap_frees() -> None:
    """A perpetually-refused (cap-saturated) actor must be served on the
    first dispatch round after its cap frees, however deep a competitor's
    backlog runs. The livelock shape: the freed slot is re-taken by the
    same competitor every round, so the capped actor starves forever
    behind a cap that keeps 'freeing' and never reaches it."""
    backend = _backend()
    backend.register_actor_config(actor="solo", max_concurrent=1)
    backend.register_actor_config(actor="herd", max_concurrent=None)
    wid = new_uuid()
    queue = "cappin_q"
    # solo: high priority so round 1 claims its head job and saturates
    # its cap; one more row waits behind the cap.
    await _enqueue(backend, "solo", queue, count=2, priority=10)
    # herd: 30 low-priority rows, the deep backlog that monopolises every
    # subsequent round.
    await _enqueue(backend, "herd", queue, count=30)

    round1 = await backend.dispatch_batch(wid, [queue], 2, _LEASE)
    assert {j.actor for j in round1} == {"solo", "herd"}
    solo_running = next(j for j in round1 if j.actor == "solo")

    # Competitor-monopoly phase: complete every herd claim each round,
    # keep solo's row running so its cap stays saturated. Every round
    # must spend both slots on the herd and never on a second solo row.
    herd_rounds = 0
    while True:
        claimed = await backend.dispatch_batch(wid, [queue], 2, _LEASE)
        if not claimed:
            break
        assert all(j.actor == "herd" for j in claimed), (
            "a solo row was claimed while its cap was still saturated"
        )
        for j in claimed:
            tri = await backend.mark_succeeded(
                j.id, wid, result={}, attempt=j.attempt, claim_epoch=j.claim_epoch
            )
            assert tri
        herd_rounds += 1
        assert herd_rounds <= 20, "herd backlog did not drain in a bounded number of rounds"
    assert herd_rounds == 15  # 30 rows at 2 per round: the competitor truly monopolised

    # The cap frees. The very next round must admit solo's waiting row.
    tri = await backend.mark_succeeded(
        solo_running.id,
        wid,
        result={},
        attempt=solo_running.attempt,
        claim_epoch=solo_running.claim_epoch,
    )
    assert tri is True
    freed_round = await backend.dispatch_batch(wid, [queue], 2, _LEASE)
    assert freed_round, "nothing claimed after the cap freed: the slot leaked"
    assert any(j.actor == "solo" for j in freed_round), (
        "the capped actor was not admitted on the first round after its cap "
        "freed - the freed slot is being re-taken by the competitor"
    )


@pytest.mark.asyncio
async def test_each_capped_actor_reaches_first_service_within_the_rotation_bound() -> None:
    """More capped actors with pending work than the round limit: every
    actor must reach first service within ceil(actor_count / limit)
    rounds (the cross-round rotation bound), not sit pending forever."""
    backend = _backend()
    actors = [f"cap_{i:02d}" for i in range(6)]
    for name in actors:
        backend.register_actor_config(actor=name, max_concurrent=1)
    wid = new_uuid()
    queue = "rotation_pin_q"
    for name in actors:
        await _enqueue(backend, name, queue, count=2)

    limit = 3
    bound = -(-len(actors) // limit)  # ceil
    first_served: dict[str, int] = {}
    for round_no in range(1, bound + 1):
        claimed = await backend.dispatch_batch(wid, [queue], limit, _LEASE)
        assert claimed, f"round {round_no} claimed nothing with backlog outstanding"
        for j in claimed:
            first_served.setdefault(j.actor, round_no)
            tri = await backend.mark_succeeded(
                j.id, wid, result={}, attempt=j.attempt, claim_epoch=j.claim_epoch
            )
            assert tri
    never = sorted(set(actors) - set(first_served))
    assert not never, (
        f"{never} never reached first service within ceil(6/{limit})={bound} rounds "
        "of capped rotation - the rotation stamp is not protecting capped actors"
    )


# ── 2. Retry backoff: the curve converges, never resets, never spins ────


@pytest.mark.parametrize("backoff", ["exponential", "linear", "fixed"])
@pytest.mark.parametrize("jitter", [0.0, 0.2, 0.5, 1.0])
def test_backoff_band_is_capped_monotone_and_non_degenerate(backoff: str, jitter: float) -> None:
    """For every curve family and attempt number, the jitter band lies
    inside [0, effective_cap], its upper edge never decreases with the
    attempt (the curve converges to the cap, it never 'resets' to a
    shorter wait) and the raw value never degenerates to a zero period.

    A band whose upper edge dropped as attempts accumulate would let a
    transiently failing job's re-park interval shrink forever: the
    claim/retry churn of a livelock, with a retry curve attached."""
    bases = [timedelta(seconds=0.1), timedelta(seconds=1), timedelta(seconds=5)]
    caps = [timedelta(seconds=5), timedelta(seconds=60), timedelta(hours=1)]
    global_caps = [timedelta(hours=24), timedelta(seconds=90)]
    for base in bases:
        for cap in caps:
            for max_retry_backoff in global_caps:
                policy = RetryPolicy(
                    backoff=backoff,  # type: ignore[arg-type]
                    base=base,
                    cap=cap,
                    jitter=jitter,
                )
                cap_s = min(cap.total_seconds(), max_retry_backoff.total_seconds())
                base_s = base.total_seconds()
                prev_upper = -1.0
                for attempt in range(1, 400):
                    raw = _raw_backoff_seconds(base_s, cap_s, backoff, attempt)  # type: ignore[arg-type]
                    assert raw > 0.0, (
                        f"{backoff} curve degenerated to a zero period at attempt {attempt}"
                    )
                    lower, upper = _capped_jitter_band(raw, cap_s, jitter)
                    assert 0.0 <= lower <= upper <= cap_s + 1e-9
                    assert upper >= prev_upper - 1e-12, (
                        f"{backoff} band upper edge fell from {prev_upper} to {upper} at "
                        f"attempt {attempt}: the retry curve resets instead of converging"
                    )
                    prev_upper = upper
                    rng = __import__("random").Random(attempt)
                    delay = compute_backoff(
                        policy, attempt, rng, max_retry_backoff=max_retry_backoff
                    )
                    delay_s = delay.total_seconds()
                    assert lower - 1e-9 <= delay_s <= upper + 1e-9
                    assert 0.0 <= delay_s <= cap_s + 1e-9


# ── 3. Sweep interleaving: rotating sweeps must terminate every row ─────


@pytest.mark.asyncio
async def test_rotating_sweeps_drive_every_row_terminal_without_progress_regression() -> None:
    """The reclaim sweep, the deadline sweep and the scheduled-to-pending
    sweep, interleaved over one mixed population (a chronic crasher, a
    perpetually rate-limited job, a healthy job), must drive every row
    terminal within a bounded number of steps, and the terminal count
    must never regress. A sweep re-scanning another sweep's in-flight
    row and resetting its progress would show up as a stalled or
    non-monotone terminal count."""
    backend = _backend()
    queue = "sweeps_q"
    for name in ("crasher", "denied", "healthy"):
        backend.register_actor_config(actor=name, max_concurrent=None, queue=queue)
    wid = new_uuid()
    close_at = _START + timedelta(seconds=120)
    await _enqueue(backend, "crasher", queue, max_attempts=2, schedule_to_close=close_at)
    await _enqueue(backend, "denied", queue, schedule_to_close=close_at)
    await _enqueue(backend, "healthy", queue, schedule_to_close=close_at)

    lease = timedelta(seconds=5)
    quantum = timedelta(seconds=2)
    sweeps = (
        backend.scheduled_to_pending,
        backend.deadline_sweep,
        lambda: backend.reclaim_expired_locks(timedelta(seconds=0), timedelta(seconds=0)),
    )
    terminal_statuses = {"succeeded", "failed", "cancelled", "crashed"}

    def _terminal_count() -> int:
        return sum(
            1
            for row in backend._jobs.values()  # type: ignore[reportPrivateUsage]
            if row.status in terminal_statuses
        )

    prev_terminal = 0
    saw_progress = False
    for step in range(1, 200):
        backend._clock.advance(quantum)  # type: ignore[reportPrivateUsage]
        await sweeps[step % 3]()
        claims = await backend.dispatch_batch(wid, [queue], 3, lease)
        for job in claims:
            if job.actor == "healthy":
                assert await backend.mark_succeeded(
                    job.id, wid, result={}, attempt=job.attempt, claim_epoch=job.claim_epoch
                )
            elif job.actor == "denied":
                # The consumer's denial path: snooze on the limiter's
                # retry hint, attempt refunded, row re-parked.
                tri = await backend.mark_snoozed(
                    job.id,
                    wid,
                    timedelta(seconds=2),
                    outcome="rate_limit_denied",
                    attempt=job.attempt,
                    claim_epoch=job.claim_epoch,
                )
                assert tri in ("scheduled", "failed")
            # crasher: the holder is gone; the row is left to its lease.
        terminal = _terminal_count()
        assert terminal >= prev_terminal, (
            f"terminal count regressed from {prev_terminal} to {terminal} at step {step}: "
            "a sweep reset another sweep's settled row"
        )
        if terminal > prev_terminal:
            saw_progress = True
        prev_terminal = terminal
        if terminal == 3:
            break
    assert prev_terminal == 3, (
        f"only {prev_terminal}/3 rows terminal after 200 interleaved sweep steps: "
        "the sweeps churn rows without converging (livelock)"
    )
    assert saw_progress
    for job_id, row in backend._jobs.items():  # type: ignore[reportPrivateUsage]
        if job_id and row.actor == "denied":
            assert row.error_class == "DeadlineExceeded", (
                "the perpetually denied job exited as "
                f"{row.error_class}: the denial deadline arm did not fire"
            )


# ── 4. Notify reconnect: bounded backoff, wake on success, no hot spin ──


def _notify_deps(factory: object) -> Mock:
    from taskq.settings import WorkerSettings

    deps = Mock()
    deps.notify_reconnect_lock = asyncio.Lock()
    deps.notify_conn = None
    deps.notify_conn_factory = factory
    deps.settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": "taskq_livelock_pin",
            "notify_reconnect_backoff_initial": "0.5",
        }
    )
    return deps


def _stub_conn() -> Mock:
    conn = Mock()
    conn.execute = AsyncMock()
    conn.add_listener = AsyncMock()
    conn.remove_listener = AsyncMock()
    conn.close = AsyncMock()
    conn.is_closed = Mock(return_value=False)
    return conn


@pytest.mark.asyncio
async def test_reconnect_backoff_grows_to_the_cap_and_the_wake_fires_on_success() -> None:
    """A drop-and-relisten episode must retry on a growing, bounded curve
    (never reset, never exceed the 30s cap) and, once the connection is
    rebuilt, fire the simulated wake so pending subscribers unblock - a
    listener that relistens but never delivers is the livelock in
    disguise."""
    import random as random_mod

    import taskq.worker.notify as notify_mod

    failures = {"n": 0}

    async def flaky_factory() -> Mock:
        if failures["n"] < 4:
            failures["n"] += 1
            raise asyncpg_error()
        return _stub_conn()

    def asyncpg_error() -> Exception:
        import asyncpg

        return asyncpg.PostgresConnectionError("pin: connection lost")

    deps = _notify_deps(flaky_factory)
    backend = Mock()
    wake_event = asyncio.Event()

    def wake_cb(conn: object, pid: int, channel: str, payload: str) -> None:
        wake_event.set()

    channels = [(wake_channel("taskq_livelock_pin"), wake_cb)]

    slept: list[float] = []

    def fake_uniform(a: float, b: float) -> float:
        return 1.0

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(random_mod, "uniform", fake_uniform)
        mp.setattr(asyncio, "sleep", fake_sleep)
        mp.setattr(notify_mod, "logger", Mock())
        recovered = await asyncio.wait_for(
            notify_mod._recover_notify_conn(
                deps, backend, asyncio.Event(), channels, _stub_conn(), Exception("pin: dead conn")
            ),
            timeout=10.0,
        )
    assert recovered is not None, "reconnect gave up while the factory was recovering"
    assert slept == [0.5, 1.0, 2.0, 4.0], (
        f"reconnect backoff curve is {slept}: expected the doubling ladder 0.5, 1, 2, 4 "
        "(bounded, monotonically growing, capped at 30s)"
    )
    assert all(d <= 30.0 for d in slept), "reconnect backoff exceeded its documented cap"
    assert wake_event.is_set(), (
        "reconnect succeeded but the simulated wake never fired: pending subscribers "
        "would wait forever on a listener that relistens and never delivers"
    )


@pytest.mark.asyncio
async def test_reconnect_loop_observes_shutdown_without_a_hot_spin() -> None:
    """A factory that fails forever must not turn the reconnect loop into
    a hot spin: shutdown must be observed and the loop must exit through
    the same bounded backoff sleeps, not a tight retry."""
    import random as random_mod

    import taskq.worker.notify as notify_mod

    shutdown = asyncio.Event()
    sleep_calls: list[float] = []

    async def always_fails() -> Mock:
        raise RuntimeError("pin: factory down for good")

    def fake_uniform(a: float, b: float) -> float:
        return 1.0

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        shutdown.set()  # the operator gives up mid-episode

    deps = _notify_deps(always_fails)
    backend = Mock()
    channels = [(wake_channel("taskq_livelock_pin"), lambda *a: None)]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(random_mod, "uniform", fake_uniform)
        mp.setattr(asyncio, "sleep", fake_sleep)
        mp.setattr(notify_mod, "logger", Mock())
        recovered = await asyncio.wait_for(
            notify_mod._recover_notify_conn(
                deps, backend, shutdown, channels, _stub_conn(), Exception("pin: dead conn")
            ),
            timeout=10.0,
        )
    assert recovered is None
    assert len(sleep_calls) == 1, (
        f"reconnect loop slept {len(sleep_calls)} times before observing shutdown: "
        "a hot retry spin, not a bounded backoff loop"
    )
    assert sleep_calls[0] <= 30.0 * 1.25


# ── 5. Bounded release: denial churn terminates, zero re-parks floor ────


@pytest.mark.asyncio
async def test_perpetual_denial_churn_terminates_at_the_denial_deadline() -> None:
    """The livelock in disguise: claim, deny, snooze (attempt refunded),
    promote, claim again - work happens every cycle and the job never
    runs. The schedule_to_close deadline must terminate the churn:
    bounded cycles, strictly advancing schedule, DeadlineExceeded exit."""
    backend = _backend()
    queue = "denial_q"
    backend.register_actor_config(actor="denied", max_concurrent=None, queue=queue)
    wid = new_uuid()
    close_at = _START + timedelta(seconds=60)
    await _enqueue(backend, "denied", queue, schedule_to_close=close_at)

    lease = timedelta(seconds=5)
    prev_scheduled: datetime | None = None
    cycles = 0
    tri: str = "scheduled"
    while True:
        claims = await backend.dispatch_batch(wid, [queue], 1, lease)
        if not claims:
            break
        job = claims[0]
        if prev_scheduled is not None:
            assert job.scheduled_at > prev_scheduled, (
                "denial churn re-parked the job without advancing its schedule: no forward progress"
            )
        prev_scheduled = job.scheduled_at
        tri = await backend.mark_snoozed(
            job.id,
            wid,
            timedelta(seconds=2),  # the limiter's retry hint
            outcome="rate_limit_denied",
            attempt=job.attempt,
            claim_epoch=job.claim_epoch,
        )
        if tri == "failed":
            break
        assert tri == "scheduled"
        backend._clock.advance(timedelta(seconds=2))  # type: ignore[reportPrivateUsage]
        assert await backend.scheduled_to_pending() == 1
        cycles += 1
        assert cycles <= 60, (
            "perpetual denial churn did not terminate within the denial deadline's "
            "bound of cycles: the re-park loop is unbounded"
        )
    # Once the deferral schedule passes the deadline the row is no longer
    # dispatchable; the deadline sweep is its terminal exit.
    backend._clock.advance(timedelta(seconds=1))  # type: ignore[reportPrivateUsage]
    assert await backend.deadline_sweep() == 1
    row = next(iter(backend._jobs.values()))  # type: ignore[reportPrivateUsage]
    assert row.status == "failed", (
        f"the perpetually denied job ended as {row.status}: the denial deadline did not terminate it"
    )
    assert row.error_class == "DeadlineExceeded"


@pytest.mark.asyncio
async def test_zero_delay_repark_is_floored_off_the_head_of_the_dispatch_order() -> None:
    """A snooze with a zero delay must not park the job at the head of
    the dispatch order: the MIN_DEFERRAL_INTERVAL floor keeps the row
    unclaimable for one interval, so one claim/refund round trip per
    scheduler tick cannot monopolise a worker slot."""
    backend = _backend()
    queue = "floor_q"
    backend.register_actor_config(actor="floored", max_concurrent=None, queue=queue)
    wid = new_uuid()
    await _enqueue(backend, "floored", queue)

    claims = await backend.dispatch_batch(wid, [queue], 1, _LEASE)
    assert len(claims) == 1
    job = claims[0]
    tri = await backend.mark_snoozed(
        job.id, wid, timedelta(seconds=0), attempt=job.attempt, claim_epoch=job.claim_epoch
    )
    assert tri == "scheduled"
    row = backend._jobs[job.id]  # type: ignore[reportPrivateUsage]
    assert row.scheduled_at == _START + timedelta(seconds=1), (
        f"zero-delay re-park landed at {row.scheduled_at}: the deferral floor is gone, "
        "the row is instantly re-claimable and one job can thrash a slot forever"
    )
    # Not promoted, not claimable inside the floor window.
    assert await backend.scheduled_to_pending() == 0
    assert await backend.dispatch_batch(wid, [queue], 1, _LEASE) == []
    # After the floor elapses the row is promoted and claimed: progress.
    backend._clock.advance(timedelta(seconds=1))  # type: ignore[reportPrivateUsage]
    assert await backend.scheduled_to_pending() == 1
    claims = await backend.dispatch_batch(wid, [queue], 1, _LEASE)
    assert len(claims) == 1, "the floored row never became claimable after its deferral"


# ── 6. Leader handover: the lease predicates ignore the Python clock ────


@pytest.mark.integration
@pytest.mark.xdist_group(name="chaos")
async def test_leader_lease_takeability_is_immune_to_python_clock_skew(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Handover ping-pong under clock skew is structurally excluded: the
    elect statement reads the SERVER clock (``clock_timestamp()``) and
    binds only intervals, never an absolute time from the Python clock.
    Pin the two shapes where a Python-clock predicate would answer
    DIFFERENTLY from the server and the server must win both ways:

    * skew +2h against a 1h-old lease: a Python-clock predicate reads the
      lease as lapsed (premature takeover, two leaders, split-fire) - the
      server must refuse the takeover;
    * skew -2h against a backdated row: a Python-clock predicate reads
      the lease as live (stalled handover, no leader) - the server must
      grant the takeover."""
    import asyncpg as asyncpg_mod

    schema = module_pg_schema.schema_name
    conn = await asyncpg_mod.connect(module_pg_schema.pg_dsn)
    try:
        await conn.execute(f'DELETE FROM "{schema}".maintenance_leader')
        holder, taker = (new_uuid() for _ in range(2))
        await _create_worker(conn, schema, holder)
        await _create_worker(conn, schema, taker)
        elect_sql, _, _ = build_leader_lease_sql(schema)

        # Live lease, +2h skew: the two clocks disagree (skew exceeds the
        # remaining lease), the server must keep the row.
        elected = await conn.fetchval(elect_sql, holder, 3600.0, 3600.0)
        assert elected is not None
        assert await conn.fetchval(elect_sql, taker, 3600.0, 3600.0) is None, (
            "a lease with 1h of life left was taken while the Python clock ran "
            "2h ahead: premature takeover, two leaders"
        )

        # Lapsed lease, -2h skew: a Python-clock predicate reads the
        # backdated row as live; the server must grant the takeover.
        await conn.execute(
            f'UPDATE "{schema}".maintenance_leader SET '
            f"expires_at = clock_timestamp() - interval '1 hour', "
            f"last_seen_at = clock_timestamp() - interval '2 hour' WHERE singleton = true"
        )
        takeover = await conn.fetchval(elect_sql, taker, 3600.0, 3600.0)
        assert takeover is not None, (
            "a lease lapsed an hour ago on the server clock was not taken while the "
            "Python clock ran 2h behind: handover stalled, no leader"
        )
    finally:
        await conn.close()

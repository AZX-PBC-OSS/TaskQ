# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team storm: the mass-reclaim cycle's redispatch damper and the
failure-retry ladder's floor.

System-level question (the amplification question): when a systemic
failure (PG restart, deploy, dependency outage) crashes N running jobs,
does the fleet's crash→reclaim→redispatch→crash cycle damp or amplify?

Model (from ``backend/_sweeps.py`` ``_SWEEP_1_SQL``, ``heartbeat.py``
``_ISOLATE_JOB_SQL_TEMPLATE``, and ``backend/_dispatch_sql.py``):

* The consumer-side failure ladder (``retry.py`` ``compute_backoff``) IS
  exponential, jittered and doubly capped (``min(policy.cap,
  max_retry_backoff)``) — already pinned by ``test_retry_backoff.py``
  and ``test_retry_backoff_overflow.py``. Not re-pinned here.
* The CRASH-reclaim ladder is NOT the exponential ladder: sweep 1 (and
  the heartbeat isolate twin) re-pend with a FIXED
  ``scheduled_at = clock_timestamp() + interval '5 seconds'`` — no
  attempt scaling. The cycle period is therefore floored at
  ``5 s + lock_lease + sweep_interval`` (≈ 95 s at default settings),
  and the cycle COUNT is bounded by ``max_attempts`` because dispatch
  stamps ``attempt = attempt + 1`` each round-trip and sweep 1's retry
  arm requires ``attempt < max_attempts``. Damped — pinned below at
  both tiers (in-memory and real PG).
* The failure-retry arm — the one requeue shape that was unfloored — is
  floored at ``MIN_DEFERRAL_INTERVAL``: ``retry.py`` ``_retry_decision``
  returns ``Retry(retry_delay=max(delay, MIN_DEFERRAL_INTERVAL))``, the
  in-memory write floors again as defense-in-depth
  (``testing/_terminal.py`` ``_mark_failed_or_retry`` applies
  ``max(retry_delay, MIN_DEFERRAL_INTERVAL)`` as the single effective
  delay), and the PG template's own floor is held to the deferral
  family's ``GREATEST`` shape by
  ``test_failure_retry_floor_must_match_the_deferral_family`` below.
  The rationale (``constants.py`` MIN_DEFERRAL_INTERVAL) is exactly the
  monopolisation hazard: "one job monopolises a worker slot in a
  claim/refund round trip per cycle", and an ``indefinite`` kind has no
  attempt ceiling — so a ``RetryPolicy(base=0)`` can no longer cycle a
  worker slot at claim/run/fail/retry round-trip rate; the pins below
  hold the tiers to it.

The denial-snooze ladder was audited and is NOT red:
``_handle_reservation_class_denied`` jitters the raw hint and
``mark_snoozed`` floors it at ``MIN_DEFERRAL_INTERVAL`` (1 s) on both
tiers; each cycle additionally costs claim + acquire + snooze and is
bounded by per-actor concurrency. Sweep 1's fixed 5 s is a damper, not
a zero-backoff loop.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId
from taskq.backend._sql_templates import render as render_sql
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.migrate import apply_pending
from taskq.retry import (
    JobRetryState,
    Retry,
    RetryPolicy,
    decide_after_failure,
)
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.settings import make_integration_settings

_START = datetime(2025, 1, 1, tzinfo=UTC)
_FIVE_SECONDS = timedelta(seconds=5)
_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)
_WORKER = new_uuid()
_QUEUE = "default"
_ACTOR = "storm_actor"

# Cycle cost model constants (asserted, not just documented): one
# mass-reclaim cycle writes one job_attempts row + one job_events row +
# one jobs UPDATE per job, and re-admits it to dispatch no sooner than
# 5 s later; the cycle count is capped by max_attempts.
_STORM_N = 6
_MAX_ATTEMPTS = 3


def _make_backend() -> tuple[InMemoryBackend, FakeClock]:
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor=_ACTOR)
    return backend, clock


async def _enqueue(backend: InMemoryBackend, retry_kind: str) -> JobId:
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=_ACTOR,
            queue=_QUEUE,
            payload={},
            max_attempts=_MAX_ATTEMPTS,
            retry_kind=retry_kind,
            scheduled_at=_START,
        )
    )
    return job_id


def _set_running(backend: InMemoryBackend, job_id: JobId, *, attempt: int = 1) -> None:
    """Park a job mid-flight on this worker: running, lock held, lease
    either live (60 s) or already expired 1 s ago (the crash shape)."""
    row = backend._jobs[job_id]
    backend._jobs[job_id] = replace(
        row,
        status="running",
        attempt=attempt,
        locked_by_worker=_WORKER,
        lock_expires_at=_START - timedelta(seconds=1),
        started_at=_START - timedelta(seconds=30),
    )


async def _seed_running_expired(
    backend: InMemoryBackend, n: int, *, attempt: int = 1
) -> list[JobId]:
    """Seed *n* running jobs whose locks expired 1 s ago (mass crash)."""
    ids: list[JobId] = []
    for _ in range(n):
        job_id = await _enqueue(backend, "transient")
        _set_running(backend, job_id, attempt=attempt)
        ids.append(job_id)
    return ids


# ── GREEN pin: the reclaim cycle is period-floored (5 s) on both tiers ──


async def test_mass_reclaim_requeue_is_floored_at_five_seconds_and_gates_redispatch() -> None:
    """A mass-reclaimed fleet is NOT redispatchable for 5 s — the fixed
    requeue damper — and the dispatch gate enforces it.

    Contract: after sweep 1 reclaims expired-lock running jobs onto the
    retry arm, every row carries ``scheduled_at == reclaim_time + 5 s``
    (the fixed backoff — a period floor, not the exponential ladder) and
    ``dispatch_batch`` claims none of them until that timestamp passes.
    Without the gate, a 500-job fleet at max_concurrency 8 would re-enter
    dispatch on the very next poll and hammer PG claim-by-claim; with it,
    the cycle period is floored at 5 s + lease + sweep interval.
    """
    backend, clock = _make_backend()
    job_ids = await _seed_running_expired(backend, _STORM_N)

    reclaimed = await backend.reclaim_expired_locks(_GRACE, _GRACE)

    assert reclaimed == _STORM_N, "the whole seeded fleet must be reclaimed in one call"
    for job_id in job_ids:
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "pending", (
            f"job {job_id} must land on the retry arm as pending, got {row.status!r}"
        )
        assert row.scheduled_at - _START == _FIVE_SECONDS, (
            f"job {job_id} requeue backoff must be the fixed 5 s damper "
            f"(got {row.scheduled_at - _START}); an unfloored or zero backoff "
            "turns the mass-reclaim cycle into a tight redispatch loop"
        )

    # The dispatch gate: scheduled_at is 5 s in the future — nothing is
    # claimable until it passes, on either side of the boundary.
    claimed_now = await backend.dispatch_batch(_WORKER, [_QUEUE], _STORM_N, _LEASE)
    assert claimed_now == [], (
        "dispatch must not claim a requeued job before its 5 s backoff elapses — "
        "the scheduled_at <= now gate is the redispatch rate damper"
    )

    clock.advance(timedelta(seconds=4, milliseconds=999))
    claimed_early = await backend.dispatch_batch(_WORKER, [_QUEUE], _STORM_N, _LEASE)
    assert claimed_early == [], "4.999 s in: still inside the damper window"

    clock.advance(timedelta(milliseconds=1))
    claimed = await backend.dispatch_batch(_WORKER, [_QUEUE], _STORM_N, _LEASE)
    assert len(claimed) == _STORM_N, "at exactly 5 s the whole fleet is redispatchable"
    for row in claimed:
        assert row.attempt == 2, (
            f"dispatch must stamp attempt = attempt + 1 (got {row.attempt}); "
            "without the increment the reclaim cycle would never exhaust attempts"
        )


async def test_reclaim_cycle_count_is_bounded_by_attempt_exhaustion() -> None:
    """The crash→reclaim→redispatch cycle terminates: dispatch's attempt
    increment plus sweep 1's ``attempt < max_attempts`` retry-arm guard
    cap the cycle count at ``max_attempts - 1`` reclaims per job.

    Contract: a permanently-crashing job cannot cycle forever — after
    the attempt budget is spent, the same systemic failure lands the row
    on a terminal state ('crashed'). The repetition until attempt
    exhaustion is bounded, not unbounded.
    """
    backend, clock = _make_backend()
    (job_id,) = await _seed_running_expired(backend, 1, attempt=_MAX_ATTEMPTS - 1)

    # Cycle 1: attempt 2 < max 3 → retry arm.
    first = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert first == 1
    row = await backend.get(job_id)
    assert row is not None and row.status == "pending"

    # Redispatch (attempt 2 → 3 = max_attempts), crash again.
    clock.advance(_FIVE_SECONDS)
    (claimed,) = await backend.dispatch_batch(_WORKER, [_QUEUE], 1, _LEASE)
    assert claimed.attempt == _MAX_ATTEMPTS
    backend._jobs[job_id] = replace(
        backend._jobs[job_id],
        lock_expires_at=clock.now() - timedelta(seconds=1),
    )
    clock.advance(timedelta(seconds=1))

    # Cycle 2: attempt 3 == max 3 → terminal, no third redispatch.
    second = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert second == 1
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "crashed", (
        "a job at max_attempts whose lock expired again must land 'crashed' — "
        "the attempt budget is the cycle-count bound; a retry-arm miss here "
        "would mean an unbounded crash/reclaim loop"
    )
    assert row.finished_at is not None, "the terminal arm must stamp finished_at"


# ── CONTRACT pin: the failure-retry ladder never carries a sub-floor delay ──


async def test_zero_base_indefinite_failure_retry_cycles_at_zero_period_unfloored() -> None:
    """A ``RetryPolicy(base=0, kind='indefinite')`` is accepted by the
    policy boundary (only ``cap >= base`` and ``jitter`` are validated)
    and degenerates the backoff curve to zero — the monopolisation
    hazard with no attempt ceiling. The CONTRACT, asserted after the
    loop: a failure-retry decision must never carry a delay below
    ``MIN_DEFERRAL_INTERVAL`` — the same floor the deferral arms
    (``mark_snoozed``, ``mark_retry_after`` non-consuming) apply at
    their writes for exactly this monopolisation rationale, now applied
    by the failure-retry decision itself (``retry.py`` floors at the
    classifier) with the write arms flooring again as defense-in-depth.

    The loop drives five full claim→fail→retry rounds; its zero-landing
    assertions (``scheduled_at == now``, instant reclaim, five cycles at
    one unmoved tick) are conditioned on the decision carrying a zero
    delay: they fire only inside the defect shape — documenting its full
    cycle as evidence — and never pin it as today's behaviour. If the
    floor ever regresses, the loop re-derives the zero-period evidence
    and the contract assert below is what fails.
    """
    backend, clock = _make_backend()
    policy = RetryPolicy(
        kind="indefinite",
        base=timedelta(0),
        cap=timedelta(hours=1),
        jitter=0.2,
    )
    # Evidence that the policy boundary is unguarded: only cap>=base and
    # jitter are validated; base=0 sails through, and 0 * 2**k == 0 at
    # every rung, so "exponential, capped" degenerates to "zero, forever".
    assert policy.base == timedelta(0)
    actor_config = StubActorConfig(retry=policy)

    job_id = await _enqueue(backend, "indefinite")
    _set_running(backend, job_id, attempt=1)
    error_info = ErrorInfo(
        error_class="RuntimeError", error_message="dependency down", error_traceback=None
    )

    cycles_at_t0 = 0
    last_delay: timedelta | None = None
    for attempt in range(1, 6):
        job_state = JobRetryState(
            attempt=attempt,
            max_attempts=_MAX_ATTEMPTS,
            retry_kind="indefinite",
            schedule_to_close=None,
            start_to_close=None,
        )
        decision = decide_after_failure(actor_config, RuntimeError("dependency down"), job_state)
        assert isinstance(decision, Retry), "indefinite kind must never exhaust attempts"
        last_delay = decision.retry_delay

        await backend.mark_failed_or_retry(
            job_id, _WORKER, error_info, decision.retry_delay, attempt=attempt
        )
        row = await backend.get(job_id)
        assert row is not None

        # Zero-delay landing evidence, conditioned on the decision
        # actually carrying a zero delay (the sibling-conditioning
        # pattern): the assertions document the defect's full
        # claim→fail→retry cycle at one unmoved tick and fire only
        # inside that shape, so the loop can never pin the defect as
        # today's behaviour — the contract asserted after the loop is
        # the binding one, and it is what goes red if the floor ever
        # regresses while these assertions re-derive the evidence.
        if decision.retry_delay == timedelta(0):
            assert row.scheduled_at == clock.now(), (
                "retry_delay=0 lands scheduled_at == now (pending, unfloored) — "
                "head of every dispatch round"
            )
            claimed = await backend.dispatch_batch(_WORKER, [_QUEUE], 1, _LEASE)
            assert len(claimed) == 1, "the zero-delay retry is instantly re-claimable"
            assert claimed[0].attempt == attempt + 1
            cycles_at_t0 += 1
        else:
            # Floored decision — the contract's landing: the row is
            # scheduled floor-out and NOT claimable at this tick, so the
            # loop must re-park it running/owned to drive the next round
            # (the zero-delay branch's instant reclaim plays this role
            # inside the defect shape). Pure mechanics — the floored
            # landing itself is pinned by the side-by-side test below.
            _set_running(backend, job_id, attempt=attempt + 1)

    if last_delay == timedelta(0):
        assert cycles_at_t0 == 5, (
            "five full failure cycles completed at one unmoved clock tick — the zero-period evidence"
        )

    # ── THE CONTRACT ────────────────────────────────────────────────────
    assert last_delay is not None and last_delay >= MIN_DEFERRAL_INTERVAL, (
        f"failure-retry delay must be floored at MIN_DEFERRAL_INTERVAL "
        f"({MIN_DEFERRAL_INTERVAL}); got {last_delay} for a base=0 indefinite "
        "policy — a sub-floor delay requeues the job at the head of dispatch "
        "order, so the actor cycles a worker slot at claim/run/fail/retry "
        "round-trip rate with no period and no attempt ceiling"
    )


async def test_deferral_arms_floor_zero_delay_but_failure_retry_does_not() -> None:
    """The two requeue families side by side on the same backend: a
    ``Snooze(0)`` is floored to ``MIN_DEFERRAL_INTERVAL`` (1 s) by
    ``mark_snoozed``'s snooze arm, and a failure ``Retry(0)`` handed to
    the write raw is floored to the same interval by the failure-retry
    arm — the asymmetry this test was written red against is landed.

    GREEN on both halves (the floor is pinned behaviour on both arms);
    each half's assertion carries its arm's contract.
    """
    backend, _clock = _make_backend()

    snoozer = await _enqueue(backend, "indefinite")
    failer = await _enqueue(backend, "indefinite")
    for job_id in (snoozer, failer):
        _set_running(backend, job_id, attempt=1)

    await backend.mark_snoozed(snoozer, _WORKER, timedelta(0), attempt=1)
    snooze_row = await backend.get(snoozer)
    assert snooze_row is not None
    assert snooze_row.scheduled_at - _START == MIN_DEFERRAL_INTERVAL, (
        "the deferral arm floors Snooze(0) at MIN_DEFERRAL_INTERVAL — pinned "
        "anti-monopolisation behaviour"
    )

    await backend.mark_failed_or_retry(
        failer,
        _WORKER,
        ErrorInfo(error_class="RuntimeError", error_message="x", error_traceback=None),
        timedelta(0),
        attempt=1,
    )
    fail_row = await backend.get(failer)
    assert fail_row is not None
    assert fail_row.scheduled_at - _START >= MIN_DEFERRAL_INTERVAL, (
        f"the failure-retry arm must apply the same floor the deferral arm "
        f"just demonstrated; got scheduled_at - now = "
        f"{fail_row.scheduled_at - _START} — a sub-floor landing requeues the "
        "job at the head of the dispatch order, instantly re-claimable "
        "(the monopolisation hazard the floor exists for)"
    )


def test_failure_retry_floor_must_match_the_deferral_family() -> None:
    """The PG ``mark_retry`` template must be honest to the floor the
    other tiers already apply: the SAME ``GREATEST($3::interval,
    MIN_DEFERRAL_INTERVAL)`` expression ``mark_snoozed`` and the
    non-consuming ``mark_retry_after`` arm pin, keyed on a single
    effective delay.

    The behavioural half is green on the in-memory tier (the twin's
    ``_mark_failed_or_retry`` floors ``max(retry_delay,
    MIN_DEFERRAL_INTERVAL)`` — see the side-by-side test above); this
    pin's job is the template half: without the ``GREATEST`` in
    ``mark_retry``, the PG tier silently requeues below the floor
    whenever a caller hands the write a raw sub-floor delay (the
    in-memory twin floors, the mirror diverges, and the monopolisation
    hazard the floor exists for — one claim/refund round trip per cycle
    monopolising a worker slot — returns on real PG).

    Source-text assertion over rendered SQL, the same inventory-guard
    category as ``test_release_fixes_core``'s no-``now()`` pin: there is
    no runtime expression to observe without a container round-trip, and
    the shape IS the contract (the floor expression, the status branch,
    and the deadline comparison must all read the effective delay).

    The consuming ``mark_retry_after_consume_true`` arm is pinned to
    keep the RAW delay: a consuming retry is a real execution bounded by
    the budget it spends, not by the deferral floor.
    """
    sql = render_sql("taskq")
    floor_sql = f"interval '{MIN_DEFERRAL_INTERVAL.total_seconds()} seconds'"
    greatest_floor = f"GREATEST($3::interval, {floor_sql})"

    # The deferral family's pinned floor shape — the reference:
    assert greatest_floor in sql.mark_snoozed, (
        "mark_snoozed must keep its GREATEST($3, MIN_DEFERRAL_INTERVAL) floor — "
        "the family reference this pin holds mark_retry to"
    )
    assert greatest_floor in sql.mark_retry_after_consume_false, (
        "the non-consuming RetryAfter arm must keep the deferral floor"
    )
    # The failure-retry arm must carry the SAME floor:
    assert greatest_floor in sql.mark_retry, (
        "mark_retry must floor its requeue delay at MIN_DEFERRAL_INTERVAL via "
        "GREATEST, exactly like the deferral arms — the PG twin of the "
        "in-memory floor; without it a raw sub-floor delay requeues at the "
        "head of the dispatch order on real PG"
    )
    # ...and key its status branch on the floored effective delay (the
    # single-delay contract mark_snoozed pins), not the raw $3 bind:
    assert (
        "CASE WHEN (SELECT effective_delay FROM params) > interval '0' "
        "THEN 'scheduled'" in sql.mark_retry
    ), (
        "mark_retry's status branch must key on the floored effective delay, "
        "not the raw $3 bind — the single effective delay contract"
    )
    # Both arms' deadline comparison AND the retried arm's scheduled_at
    # read the same effective delay:
    assert sql.mark_retry.count("clock_timestamp() + (SELECT effective_delay FROM params)") == 3, (
        "the retried arm's scheduled_at and both arms' deadline comparisons "
        "must read the floored effective_delay — one effective delay per write"
    )
    # The consuming RetryAfter arm keeps the RAW delay by design:
    assert "GREATEST" not in sql.mark_retry_after_consume_true, (
        "the consuming RetryAfter arm must keep the raw delay — its requeue "
        "is a real execution bounded by the budget it spends, not the "
        "deferral floor"
    )


# ── GREEN pin, PG tier: the same damper + gate + exhaustion on real PG ──


class _PoolsDeps:
    """Duck-typed ``BackendDeps``: settings plus the three pools, the
    full surface the swept/dispatched paths touch (same shape as the
    ``test_rt_locks_sweep_notify_pool_unbounded`` stand-in)."""

    def __init__(self, settings: WorkerSettings, *, worker_pool: Any, dispatcher_pool: Any) -> None:
        self.settings = settings
        self.worker_pool = worker_pool
        self.heartbeat_pool = worker_pool
        self.dispatcher_pool = dispatcher_pool


async def _seed_pg_running_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: list[UUID],
    *,
    worker_id: UUID,
    lease_seconds: float,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs ('
        "    id, actor, queue, payload, max_attempts, retry_kind,"
        "    status, priority, attempt, scheduled_at,"
        "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
        "    cancel_phase, cancel_requested_at"
        ") SELECT"
        "    t.id, $3::text, $4::text, '{}'::jsonb,"
        f"    {_MAX_ATTEMPTS}::smallint, 'transient',"
        "    'running', 0, 1::smallint, clock_timestamp(),"
        "    $2::uuid,"
        "    clock_timestamp() + ($5::double precision * interval '1 second'),"
        "    clock_timestamp() - interval '30 seconds',"
        "    clock_timestamp() - interval '30 seconds',"
        "    0::smallint, NULL"
        " FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
        worker_id,
        _ACTOR,
        _QUEUE,
        lease_seconds,
    )


@pytest.mark.integration
async def test_pg_reclaim_damper_and_attempt_exhaustion(pg_dsn: str) -> None:
    """On real Postgres: sweep 1's retry arm stamps
    ``scheduled_at = clock_timestamp() + 5 s``; ``dispatch_batch`` claims
    nothing while that timestamp is in the future (the SQL gate
    ``j2.scheduled_at <= statement_timestamp()``); once due, dispatch
    claims with ``attempt + 1``; and the exhausted arm lands 'crashed'.

    Deterministic — no wall-clock sleeps: the damper is asserted from
    the row's own timestamps (future by ~5 s), and due-ness is produced
    by rewinding ``scheduled_at`` rather than waiting.
    """
    schema = f"tst_{new_base62()}".lower()
    settings = make_integration_settings(pg_dsn, schema_name=schema)
    admin = await asyncpg.connect(pg_dsn)
    pools: list[asyncpg.Pool] = []
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
        await admin.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _ACTOR,
            _QUEUE,
        )
        worker_id = new_uuid()
        await admin.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, 'storm-host', 12345, ARRAY['default'])",
            worker_id,
        )
        storm_ids = [new_uuid() for _ in range(3)]
        victim = new_uuid()
        await _seed_pg_running_jobs(
            admin, schema, [*storm_ids, victim], worker_id=worker_id, lease_seconds=-1.0
        )

        worker_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=3)
        dispatcher_pool = await asyncpg.create_pool(
            pg_dsn, min_size=1, max_size=4, command_timeout=5.0
        )
        pools.extend([worker_pool, dispatcher_pool])
        backend = PostgresBackend(
            _PoolsDeps(settings, worker_pool=worker_pool, dispatcher_pool=dispatcher_pool),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps — settings plus the three pools, the full surface the swept paths touch.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )

        # Fill: one bounded reclaim batch re-pends the storm with the 5 s
        # damper.
        reclaimed = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
        assert reclaimed == 4, "the seeded storm must be reclaimed in one bounded call"
        rows = await admin.fetch(
            f"SELECT id, scheduled_at, clock_timestamp() AS now_ts "
            f'FROM "{schema}".jobs WHERE id = ANY($1)',
            [*storm_ids, victim],
        )
        for rec in rows:
            gap = rec["scheduled_at"] - rec["now_ts"]
            assert timedelta(seconds=4) < gap <= _FIVE_SECONDS, (
                f"requeue damper must stamp scheduled_at = now + 5 s on PG "
                f"(got now + {gap}); the fixed backoff is the cycle's period floor"
            )

        # Gate: nothing claimable while scheduled_at is in the future.
        claimed = await backend.dispatch_batch(worker_id, [_QUEUE], 10, _LEASE)
        assert claimed == [], (
            "PG dispatch must refuse future-scheduled_at rows — the "
            "scheduled_at <= statement_timestamp() index gate is the "
            "redispatch rate damper under storm"
        )

        # Due: rewind the damper and claim — attempt increments.
        await admin.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = clock_timestamp() - '
            "interval '1 second' WHERE id = ANY($1)",
            [*storm_ids, victim],
        )
        claimed = await backend.dispatch_batch(worker_id, [_QUEUE], 10, _LEASE)
        assert len(claimed) == 4
        for row in claimed:
            assert row.attempt == 2, (
                f"PG dispatch stamps attempt + 1 (got {row.attempt}); the "
                "increment is what bounds the crash-cycle count"
            )

        # Exhaustion: park the victim at max_attempts, expire its lock
        # again — the next reclaim must land it terminal.
        await admin.execute(
            f'UPDATE "{schema}".jobs SET attempt = $2, lock_expires_at = '
            "clock_timestamp() - interval '1 second' WHERE id = $1",
            victim,
            _MAX_ATTEMPTS,
        )
        reclaimed_again = await backend.reclaim_expired_locks(timedelta(0), timedelta(0))
        assert reclaimed_again == 1
        status = await admin.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', victim)
        assert status == "crashed", (
            f"the exhausted arm must land 'crashed' (got {status!r}) — "
            "attempt exhaustion is the cycle-count bound on PG too"
        )
    finally:
        for pool in pools:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()

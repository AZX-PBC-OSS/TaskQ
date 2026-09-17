"""Differential composition: shutdown-interrupted, re-claimed, then crash-reclaimed.

Two release paths hand a running row back to the fleet, and each keeps its
own books:

* ``mark_interrupted`` (the shutdown release) — a NON-consuming release of
  a started attempt: the claim's increment is refunded, no attempt row is
  written (an interruption is not an execution outcome), one
  ``reason='interrupted'`` event records it, and ``interrupt_count`` bumps.
* the crash-reclaim sweep — the holding worker died mid-attempt, so the
  attempt IS spent (left as claimed), a ``'crashed'`` attempt row and a
  ``reason='lock_expired'`` event are written, and the row reschedules on
  the retry curve stamped on it at enqueue time
  (``retry_base_seconds``/``retry_cap_seconds``/``retry_backoff``/
  ``retry_jitter``), with the delay's jitter derived — md5 of
  ``'<job id>:<attempt>'`` — never drawn.

The composition is what a rolling deploy actually produces: pod A's SIGTERM
interrupts the job mid-flight, pod B claims the refunded row and is then
killed outright, and the leader's sweep hands it back a second time. The
pins here hold the two release paths' bookkeeping disjoint through the
chain — the interrupt's refund leaves the re-claim at the SAME attempt
number, so the reclaim reads (and hashes) exactly the attempt a
never-interrupted job would show — and the cancel-wins fence
(``cancel_phase = 0``) still declines the shutdown release when an
operator's cancel lands on the re-claimed row between the release and the
reclaim.

The cross-backend comparison rides the shared differential harness
(tests/test_rt_diff_harness.py): identical scenario, identical normalized
observables — statuses, attempt rows, event trails, and the row counters —
or the in-memory twin certifies a recovery Postgres does not perform.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import JobId
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF
from taskq.retry import (
    RetryPolicy,
    # The pin asserts the sweep stamped exactly the twin's value for this
    # row — the same cross-module consumption taskq.testing._sweeps performs.
    _compute_reclaim_backoff,
)
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.pg import create_worker

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)

#: A protective curve whose attempt-1 and attempt-2 delays sit in disjoint
#: bands even after jitter (attempt 1: [240, 360); attempt 2: [480, 720)),
#: so a reclaim that read the WRONG attempt cannot hide inside the band.
_POLICY = RetryPolicy(
    backoff="exponential",
    base=timedelta(seconds=300),
    cap=timedelta(hours=1),
    jitter=0.2,
    max_attempts=5,
)

#: Slack for the gap between the test's server-clock reads and the sweep's
#: own clock_timestamp() — orders of magnitude below the 120 s miss a
#: shifted curve produces, so the bracket stays decisive.
_CLOCK_GAP_SLACK = timedelta(seconds=30)


def _curve_args(side: DiffSide, *, jitter: float) -> EnqueueArgs:
    """One due batch-free job carrying the protective retry curve on the row
    — the scalars a real enqueue stamps from the actor's live ActorRef."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=_POLICY.max_attempts,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        retry_base=_POLICY.base,
        retry_cap=_POLICY.cap,
        retry_backoff=_POLICY.backoff,
        retry_jitter=jitter,
    )
    side.register_job_id("j1", args.id)
    return args


async def _interrupt_reclaim_chain(
    side: DiffSide, *, jitter: float
) -> tuple[JobId, UUID, datetime, datetime]:
    """Deploy-interrupt, re-claim, crash-reclaim one job; return its id, the
    claiming worker, and the server-clock bracket around the sweep."""
    args = _curve_args(side, jitter=jitter)
    await side.backend.enqueue(args)

    wid = await side.worker("w1")
    first_claim = await side.dispatch("w1", ["default"], limit=5)
    assert first_claim == ["j1"], "the scenario requires the first pod's claim"
    claimed_row = await side.backend.get(args.id)
    assert claimed_row is not None and claimed_row.attempt == 1

    # Pod A takes SIGTERM mid-attempt: the shutdown release hands the row
    # back with the claim's increment refunded — the interrupt is not an
    # execution and must not advance the row's retry curve.
    released = await side.backend.mark_interrupted(
        args.id, wid, attempt=claimed_row.attempt, hold=timedelta(0)
    )
    assert released == "pending", (
        f"the shutdown release of a cleanly-owned row must land pending; got {released!r}"
    )

    # Pod B claims the refunded row: the same attempt number again, because
    # the refund returned the increment the first claim borrowed.
    second_claim = await side.dispatch("w1", ["default"], limit=5)
    assert second_claim == ["j1"], "the refunded row must be re-claimable at once (zero hold)"
    reclaimed_row = await side.backend.get(args.id)
    assert reclaimed_row is not None
    assert reclaimed_row.attempt == 1, (
        "the re-claim after a shutdown release must stamp the SAME attempt "
        f"number the interrupt refunded — got attempt={reclaimed_row.attempt}, "
        "so the interrupt silently spent budget the retry curve reads"
    )

    # Pod B is killed outright: the lease expires with no terminal write,
    # and the leader's sweep is the only way back.
    await side.mutate("j1", lock_expired_ago_s=10.0)
    before = await side.now()
    reclaimed = await side.sweep_reclaim()
    after = await side.now()
    assert reclaimed == 1, "the scenario requires the crashed claim to be reclaimed"
    return args.id, wid, before, after


async def test_diff_interrupted_then_crash_reclaimed_keeps_both_paths_books(
    pg_dsn: str,
) -> None:
    """Interrupt → re-claim → crash-reclaim: the interrupt refunds and counts
    itself, the reclaim spends the attempt and reschedules on the row's own
    curve, and neither path's records leak into the other's — identical on
    both backends."""

    async def scenario(side: DiffSide) -> None:
        # jitter pinned off: the harness compares scheduled_at across
        # backends at second resolution, which cannot resolve a jitter
        # band (the exact derived-jitter parity is pinned bit-for-bit by
        # tests/test_reclaim_backoff_policy_parity.py; the jitter-ON
        # composition is pinned per backend below).
        await _interrupt_reclaim_chain(side, jitter=0.0)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn)
    assert_mirror(
        "a shutdown-interrupted, re-claimed, then crash-reclaimed job keeps "
        "the two release paths' books disjoint: interrupt_count=1 with no "
        "attempt row for the interruption, one 'crashed' attempt row at the "
        "re-claimed epoch, the interrupted and lock_expired events in order, "
        "and the row back in the claimable pool on its own retry curve",
        mem,
        pg,
    )
    j1 = pg["jobs"]["j1"]
    assert j1["present"] is True
    assert j1["status"] == "pending", (
        "a transient job at attempt 1 of 5 has reclaim budget: the sweep "
        f"hands it back, got {j1['status']!r}"
    )
    assert j1["attempt"] == 1, (
        "the reclaim leaves the crashed attempt as claimed — the interrupt's "
        "refund and the re-claim already netted out"
    )
    assert j1["interrupt_count"] == 1, (
        "exactly the shutdown release counted itself; a crash reclaim is not an interruption"
    )
    assert j1["snooze_count"] == 0 and j1["rate_limit_blocked_count"] == 0
    # The jitter-free curve: base * 2**(attempt-1) = 300 s exactly.
    assert j1["scheduled_at"] == 300, (
        f"the reclaim must reschedule on the row's own curve (300 s at "
        f"attempt 1, jitter off) — the bucketed offset is "
        f"{j1['scheduled_at']!r}; a flat reclaim constant or a curve read at "
        "the wrong attempt lands outside it"
    )
    assert len(j1["attempts"]) == 1, (
        "exactly one attempt row: the interrupt wrote none (not an execution "
        f"outcome), the reclaim wrote the crash; got {j1['attempts']!r}"
    )
    attempt_row = j1["attempts"][0]
    assert (
        attempt_row["attempt"],
        attempt_row["outcome"],
        attempt_row["error_class"],
        attempt_row["error_message"],
        attempt_row["worker"],
        attempt_row["finished"],
    ) == (
        1,
        "crashed",
        "WorkerCrashed",
        "lock expired before worker reported terminal state",
        "w1",
        True,
    ), f"attempt trail diverges from the one-crash record: {attempt_row!r}"
    assert attempt_row["duration_s"] is not None, "a finished attempt row records its duration"
    event_kinds = [(e["kind"], e["detail"].get("reason")) for e in j1["events"]]
    assert event_kinds == [("state_change", "interrupted"), ("state_change", "lock_expired")], (
        f"the timeline must carry exactly the interrupt's event and the "
        f"reclaim's event, in that order; got {event_kinds!r}"
    )


async def test_diff_operator_cancel_between_release_and_reclaim_keeps_the_fence(
    pg_dsn: str,
) -> None:
    """The cancel-wins fence holds on the re-claimed epoch: an operator
    cancel landing after the interrupt's release makes the next shutdown
    release read back ``noop`` — no second refund, no second interruption
    counted, no interrupted event — and the operator's terminal write owns
    the outcome."""

    async def scenario(side: DiffSide) -> None:
        args = _curve_args(side, jitter=0.0)
        await side.backend.enqueue(args)

        wid = await side.worker("w1")
        assert await side.dispatch("w1", ["default"], limit=5) == ["j1"]
        row = await side.backend.get(args.id)
        assert row is not None

        released = await side.backend.mark_interrupted(
            args.id, wid, attempt=row.attempt, hold=timedelta(0)
        )
        side.record("first_release", released)

        assert await side.dispatch("w1", ["default"], limit=5) == ["j1"]

        # The operator's cancel lands while the re-claimed attempt is live.
        side.record("cancel_request", await side.write_cancel_request("j1", "operator stop"))

        # The same deploy's release reaches the row a beat later: the fence
        # must decline it — the operator's request owns the outcome, never
        # the deploy's.
        row2 = await side.backend.get(args.id)
        assert row2 is not None
        side.record(
            "second_release",
            await side.backend.mark_interrupted(
                args.id, wid, attempt=row2.attempt, hold=timedelta(0)
            ),
        )

        side.record("cancelled", await side.mark_cancelled("j1", "w1"))
        # Nothing terminal is reclaimable: a later sweep touches nothing.
        side.record("reclaim_after", await side.sweep_reclaim())

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn)
    assert_mirror(
        "an operator cancel landing between a shutdown release and the next "
        "claim keeps the cancel-wins fence: the second release is declined, "
        "the operator's cancel terminalises the row, and a later reclaim "
        "sweep finds nothing",
        mem,
        pg,
    )
    assert pg["records"]["first_release"] == "pending"
    assert pg["records"]["cancel_request"] is True
    assert pg["records"]["second_release"] == "noop", (
        "the fence must decline the deploy's release once the operator's cancel owns the row"
    )
    assert pg["records"]["cancelled"] is True
    assert pg["records"]["reclaim_after"] == 0

    j1 = pg["jobs"]["j1"]
    assert j1["status"] == "cancelled"
    assert j1["attempt"] == 1, (
        "the declined release refunded nothing — the operator's terminal "
        "write closed the re-claimed attempt as it stood"
    )
    assert j1["interrupt_count"] == 1, (
        "only the first release counted an interruption; the declined one must not"
    )
    interrupted_events = [e for e in j1["events"] if e["detail"].get("reason") == "interrupted"]
    assert len(interrupted_events) == 1, (
        "the declined release wrote no second interrupted event; the "
        f"timeline is {[(e['kind'], e['detail']) for e in j1['events']]!r}"
    )


# ── The jitter-ON composition, per backend at full precision ─────────────
#
# The harness buckets scheduled_at at second resolution and the two sides
# draw different job ids (hence different derived jitter fractions), so the
# exact delay cannot ride the mirror comparison. What the composition must
# hold is per-backend exact: the reclaimed row's delay IS the twin formula's
# value for THIS row at the re-claimed attempt — the interrupt's refund is
# the only reason that attempt is 1 and not 2, and the two attempts' jitter
# bands are disjoint by construction (see _POLICY).


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_reclaim_backoff_policy_parity.py
    from taskq.backend.postgres import PostgresBackend

    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_reclaim_backoff_policy_parity.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await create_worker(conn, schema, worker_id)
    return worker_id


async def test_interrupt_does_not_advance_the_reclaim_curve_with_jitter_on(
    backend_pair: Backend,
) -> None:
    """With jitter armed, the interrupted-then-crash-reclaimed job's delay
    is exactly the derived curve value for the re-claimed attempt — the
    interrupt did not advance the curve input, and the jitter is the row's
    deterministic md5 fraction, not a fresh draw."""
    from taskq.backend.postgres import PostgresBackend

    backend = backend_pair
    worker_id = await _worker_of(backend)

    if isinstance(backend, InMemoryBackend):
        # The twin's frozen clock anchors the chain's time domain.
        now0 = backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: the twin's clock is the arbiter of its own scheduling; mirrors tests/test_reclaim_backoff_policy_parity.py
        scheduled_at = now0 - timedelta(seconds=1)
    else:
        assert isinstance(backend, PostgresBackend)
        scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
    job_id = JobId(new_job_id())
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=_POLICY.max_attempts,
            retry_kind="transient",
            scheduled_at=scheduled_at,
            retry_base=_POLICY.base,
            retry_cap=_POLICY.cap,
            retry_backoff=_POLICY.backoff,
            retry_jitter=_POLICY.jitter,
        )
    )

    claimed = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=5, lock_lease=_LOCK_LEASE
    )
    assert [row.id for row in claimed] == [job_id]
    assert claimed[0].attempt == 1

    released = await backend.mark_interrupted(
        job_id, worker_id, attempt=claimed[0].attempt, hold=timedelta(0)
    )
    assert released == "pending"

    reclaimed_claim = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=5, lock_lease=_LOCK_LEASE
    )
    assert [row.id for row in reclaimed_claim] == [job_id]
    assert reclaimed_claim[0].attempt == 1, (
        "the refund must return the re-claim to the interrupted attempt number — the curve's input"
    )

    # The crash: the lease expires with no terminal write.
    if isinstance(backend, InMemoryBackend):
        from dataclasses import replace

        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing the crashed-holder state the public API cannot reach; mirrors tests/test_reclaim_backoff_policy_parity.py
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            lock_expires_at=backend._clock.now() - timedelta(seconds=10),  # pyright: ignore[reportPrivateUsage]  # Why: same
        )
        before = backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: same
        assert await backend.reclaim_expired_locks(_GRACE, _GRACE) == 1
        after = before  # the frozen clock did not move
    else:
        assert isinstance(backend, PostgresBackend)
        schema = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above
        pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
        async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
            await conn.execute(
                f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is the fixture's _IDENT_RE-validated identifier; the id is $-bound.
                "SET lock_expires_at = clock_timestamp() - interval '10 seconds' "
                "WHERE id = $1",
                job_id,
            )
            before = await conn.fetchval("SELECT clock_timestamp()")
            assert isinstance(before, datetime)
            assert await backend.reclaim_expired_locks(_GRACE, _GRACE) == 1
            after = await conn.fetchval("SELECT clock_timestamp()")
            assert isinstance(after, datetime)

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "pending"
    assert row.interrupt_count == 1

    expected = _compute_reclaim_backoff(
        _POLICY,
        reclaimed_claim[0].attempt,
        job_id=job_id,
        max_retry_backoff=DEFAULT_MAX_RETRY_BACKOFF,
    )
    assert before + expected <= row.scheduled_at <= after + expected + _CLOCK_GAP_SLACK, (
        f"{type(backend).__name__}: the interrupted-then-reclaimed row was "
        f"rescheduled to {row.scheduled_at!r}, outside "
        f"[before + {expected}, after + {expected} + slack] with "
        f"before={before!r}, after={after!r}. The sweep must stamp the row's "
        "own derived delay for the re-claimed attempt (attempt=1 — the "
        "interrupt refunded, it did not spend): the attempt-2 curve starts "
        f"at {timedelta(seconds=480)} and the bands cannot overlap, so any "
        "shift of the curve input lands outside this bracket"
    )

"""The crash-reclaim re-pend delay is floored at ``MIN_DEFERRAL_INTERVAL``.

The failure-retry path has floored the degenerate zero-base backoff curve
for a while: the decision clamps at ``MIN_DEFERRAL_INTERVAL``
(``taskq.retry.RetryClassifier._retry_decision``) and the deferral write
arms repeat the floor server-side. The reclaim arm had no floor: sweep 1's
re-pend and the heartbeat isolate rescheduled the row with
``clock_timestamp() + _RECLAIM_DELAY_SQL``, and a row stamped with a zero
or negative ``retry_base`` draws exactly zero (or a negative value) from
its own curve. The re-pend then lands at or before the instant the sweep
hands the row back: every worker's claim expires, the next sweep re-pends
at zero again, and the job cycles the fleet at claim/lease-expiry/reclaim
rate with no backoff and, for an ``indefinite`` kind, no attempt ceiling.

How a degenerate base reaches a row despite the policy boundary (which now
refuses ``base <= 0``): rows stamped by an earlier release that accepted
the shape, and a producer building ``EnqueueArgs`` directly (the
documented direct-backend path). Zero stays inside the struct's domain and
is floored at the write; a negative base is refused by the struct's column
domain check, so that shape is forced onto the row directly here.

These pins hold all three implementations to the same floor: the SQL
fragment (through the real sweep on Postgres), the Python twin
(``taskq.retry._compute_reclaim_backoff``), and the in-memory mirror.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import JobId
from taskq.backend.postgres import PostgresBackend
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.retry import RetryPolicy, _compute_reclaim_backoff
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)

#: The degenerate curve a pre-fix release stamps: a zero base (with cap = 0,
#: the only shape the old cap >= base rule admitted alongside it) draws zero
#: from its own curve at every attempt.
_ZERO_BASE = timedelta(0)

#: The other degenerate shape: a negative base draws a negative raw value,
#: which without the floor lands ``scheduled_at`` in the past.
_NEGATIVE_BASE = timedelta(seconds=-5)


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_reclaim_backoff_policy_parity.py
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_reclaim_backoff_policy_parity.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; every value is $N-bound
            worker_id,
            "test-host",
            12345,
            ["default"],
        )
    return worker_id


async def _enqueue_zero_base(backend: Backend, job_id: JobId | None = None) -> JobId:
    """Enqueue through the public backend API with a zero base and cap.

    Zero stays inside ``EnqueueArgs``' domain by doctrine (the boundary that
    knows the semantics, ``RetryPolicy``, refuses non-positive; the struct
    refuses negative), so this is the shape a direct-backend producer can
    legitimately write and the reclaim write must neutralise.
    """
    job_id = job_id if job_id is not None else new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=5,
            retry_kind="indefinite",
            scheduled_at=_START,
            retry_base=_ZERO_BASE,
            retry_cap=_ZERO_BASE,
        )
    )
    return job_id


async def _force_negative_base(backend: Backend, job_id: JobId) -> None:
    """Stamp the row with a negative base, the shape no enqueue boundary
    admits anymore but an earlier release's rows still carry."""
    if isinstance(backend, InMemoryBackend):
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: forcing the legacy-row state no enqueue path writes anymore
            backend._jobs[job_id],  # pyright: ignore[reportPrivateUsage]  # Why: same
            retry_base=_NEGATIVE_BASE,
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: as above
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        await conn.execute(
            f'UPDATE "{schema}".jobs SET retry_base_seconds = $2 WHERE id = $1',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; values are $N-bound
            job_id,
            _NEGATIVE_BASE.total_seconds(),
        )


async def _expire_lease(backend: Backend, job_id: JobId) -> None:
    """Age the job's lease into the past: the state a crashed worker leaves
    behind, with no terminal write ever arriving."""
    if isinstance(backend, InMemoryBackend):
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: forcing the crashed-holder state the public API cannot reach
            backend._jobs[job_id],  # pyright: ignore[reportPrivateUsage]  # Why: same
            lock_expires_at=backend._clock.now() - timedelta(seconds=10),  # pyright: ignore[reportPrivateUsage]  # Why: the reclaim predicate is arbitrated by the backend's own clock
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: as above
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the id is $-bound
            "SET lock_expires_at = clock_timestamp() - interval '10 seconds' WHERE id = $1",
            job_id,
        )


async def _now_of(backend: Backend) -> datetime:
    """The backend's own clock, the arbiter that stamped ``scheduled_at``."""
    if isinstance(backend, InMemoryBackend):
        return backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: the twin's clock is the arbiter of its own scheduling
    assert isinstance(backend, PostgresBackend)
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: as above
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        value = await conn.fetchval("SELECT clock_timestamp()")
    assert isinstance(value, datetime)
    return value


async def _claim_then_reclaim(backend: Backend, job_id: JobId) -> timedelta:
    """Run a real claim round, expire the lease, reclaim through the public
    sweep API, and return how far out the re-pend landed."""
    worker_id = await _worker_of(backend)
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert job_id in {row.id for row in dispatched}, (
        "the scenario requires the job to have been claimed for an attempt"
    )
    await _expire_lease(backend, job_id)
    before = await _now_of(backend)
    assert await backend.reclaim_expired_locks(_GRACE, _GRACE) == 1, (
        "the scenario requires the expired-lease job to have been reclaimed"
    )
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "pending", (
        f"the scenario requires the indefinite job to be handed back, got {row.status!r}"
    )
    return row.scheduled_at - before


async def test_zero_base_reclaim_re_pend_is_floored(backend_pair: Backend) -> None:
    """A zero-base row reclaimed by the real sweep re-pends at least one
    deferral interval out, in the future on both backends."""
    job_id = await _enqueue_zero_base(backend_pair)

    delay = await _claim_then_reclaim(backend_pair, job_id)

    assert delay >= MIN_DEFERRAL_INTERVAL, (
        f"a crash-reclaimed job with retry_base={_ZERO_BASE!r} was rescheduled "
        f"{delay!r} out: a sub-floor re-pend lands the row at or before the "
        f"instant the sweep handed it back, so the claim/lease-expiry/reclaim "
        f"cycle repeats with no period and, for an indefinite kind, no attempt "
        f"ceiling. The re-pend delay must be floored at {MIN_DEFERRAL_INTERVAL!r}"
    )
    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.scheduled_at > await _now_of(backend_pair) - MIN_DEFERRAL_INTERVAL, (
        f"the re-pend stamped scheduled_at={row.scheduled_at!r}, at or behind "
        "the backend clock: the reclaim must land the row in the future"
    )


async def test_negative_base_reclaim_re_pend_is_floored(backend_pair: Backend) -> None:
    """A negative-base row (the legacy-row shape) re-pends in the future.

    The negative base draws a negative raw value from the curve; without
    the floor the re-pend anchors ``scheduled_at`` in the past, which makes
    the row instantly claimable every sweep.
    """
    job_id = await _enqueue_zero_base(backend_pair)
    await _force_negative_base(backend_pair, job_id)

    delay = await _claim_then_reclaim(backend_pair, job_id)

    assert delay >= MIN_DEFERRAL_INTERVAL, (
        f"a crash-reclaimed job with retry_base={_NEGATIVE_BASE!r} was "
        f"rescheduled {delay!r} out: a negative base draws a negative raw "
        "value, and an unfloored re-pend anchors scheduled_at in the past. "
        f"The re-pend delay must be floored at {MIN_DEFERRAL_INTERVAL!r}"
    )


async def test_zero_base_reclaim_twin_stamps_the_floor_exactly() -> None:
    """Twin parity on the degenerate curve: the Python twin and the
    in-memory mirror stamp exactly ``MIN_DEFERRAL_INTERVAL``.

    The mirror's frozen clock makes its measurement exact, and the twin
    shares ``_compute_reclaim_backoff`` with the SQL fragment's parity pin
    (``test_reclaim_backoff_policy_parity.py`` pins the degenerate row
    bit for bit against the database), so one exact value here ties all
    three implementations to the same floor.
    """
    policy = RetryPolicy.model_construct(
        backoff="exponential",
        base=_ZERO_BASE,
        cap=_ZERO_BASE,
        jitter=0.2,
    )
    job_id = JobId(UUID("01906e5a-0000-7000-8000-00000000d001"))
    expected = _compute_reclaim_backoff(
        policy,
        1,
        job_id=job_id,
    )
    assert expected == MIN_DEFERRAL_INTERVAL, (
        f"the Python twin must lift the degenerate curve's zero draw to "
        f"{MIN_DEFERRAL_INTERVAL!r}, got {expected!r}"
    )

    memory = InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    memory.register_actor_config(actor="actor_a")
    await _enqueue_zero_base(memory, job_id=job_id)

    delay = await _claim_then_reclaim(memory, job_id)

    assert delay == MIN_DEFERRAL_INTERVAL, (
        f"the in-memory mirror must stamp exactly {MIN_DEFERRAL_INTERVAL!r} "
        f"for the degenerate row (its clock is frozen), got {delay!r}"
    )

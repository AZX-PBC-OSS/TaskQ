"""A crash reclaim must retry on the actor's configured backoff, not a flat 5s.

Every other path that re-schedules a job for another attempt derives the delay
from the actor's ``RetryPolicy`` — base, cap, backoff kind and jitter — through
``taskq.retry.compute_backoff``. The consumer's failure path does, the
``Retry-After`` override does, the snooze arms do. Crash reclaim does not: a
running job whose worker missed its lease or its heartbeat is rescheduled a
hardcoded, unjittered five seconds out, whatever the actor asked for.

Two consequences, both operational:

**The policy is silently void.** An actor configured ``base=5min, cap=1h``
because its downstream is fragile — a rate-limited third party, a database that
needs room to recover — gets retried five seconds after its worker crashes.
That configuration exists precisely to protect the thing the job talks to, and
the crash path is where the downstream is least likely to be healthy.

**No jitter means a thundering herd.** A fleet-wide event — a node drain, an
availability-zone loss, an OOM sweep across a deployment — expires many leases
at once. Every reclaimed job in that cohort is stamped with the same
``clock_timestamp() + 5 seconds``, so the whole backlog becomes due in the same
instant and lands on the recovering fleet as one synchronized wave. Jitter on
the retry curve is the mechanism that spreads exactly this, and reclaim is the
one path that skips it.

These tests hold reclaim to the delay the job's own policy produces, on both
backends — a divergence here would mean the in-memory twin certifies a backoff
Postgres does not apply.

A note on what the contract requires of the schema. The reclaim statements run
entirely inside Postgres, on a leader worker that need not host the crashed
job's actor at all, so ``RetryPolicy`` living only in the worker process (as
it did before this fix) left reclaim with no source for the curve. The
``jobs`` row now stamps it: ``retry_base_seconds`` / ``retry_cap_seconds`` /
``retry_backoff`` / ``retry_jitter`` (migration
``01.00.12_03_pre_reclaim_retry_policy.sql``), populated from the enqueuing
client's live ``ActorRef.retry`` exactly as ``max_attempts`` and
``retry_kind`` already were (``taskq.client._args``), and exposed on
``EnqueueArgs`` for direct-backend callers such as this test. The tests
deliberately assert the observable outcome (the delay, and its spread across
a cohort) rather than the storage mechanism, so a future change of source
keeps them holding as long as the row is still where reclaim reads it.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import JobId
from taskq.backend._sweeps import _RECLAIM_DELAY_SQL, _RECLAIM_JITTER_FRACTION_SQL
from taskq.backend.postgres import PostgresBackend
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF
from taskq.retry import (
    RetryPolicy,
    _compute_reclaim_backoff,
    _reclaim_jitter_fraction,
    compute_backoff,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)

#: The shape of an actor protecting a fragile downstream: a floor far above
#: any flat reclaim constant, so a reclaim honouring the policy and a reclaim
#: ignoring it cannot be confused for one another.
_PROTECTIVE_POLICY = RetryPolicy(
    backoff="exponential",
    base=timedelta(minutes=5),
    cap=timedelta(hours=1),
    jitter=0.2,
    max_attempts=5,
)

#: The flat interval the reclaim statements stamp today. Named so the failure
#: message can tell an operator what they are actually getting.
_FLAT_RECLAIM_DELAY = timedelta(seconds=5)

#: A policy whose cap sits above the operator's global backoff ceiling. With
#: ``backoff='fixed'`` the curve is flat at base, far above the ceiling, so
#: the ceiling — not the curve — decides the stamped delay: the jitter band
#: is fitted under it, and the delay lands in ``[ceiling·(1-j), ceiling]``.
_WIDE_POLICY = RetryPolicy(
    backoff="fixed",
    base=timedelta(days=3),
    cap=timedelta(days=7),
    jitter=0.2,
    max_attempts=5,
)

#: Slack for the gap between the test's clock read and the sweep's own
#: clock_timestamp() — orders of magnitude below the multi-day miss a
#: missing clamp produces, so the band stays decisive.
_CLOCK_GAP_SLACK = timedelta(seconds=30)


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_reclaim_retry_budget_parity.py
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_reclaim_retry_budget_parity.py
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


async def _enqueue(
    backend: Backend,
    *,
    job_id: JobId | None = None,
    max_attempts: int = 5,
    policy: RetryPolicy = _PROTECTIVE_POLICY,
) -> JobId:
    job_id = job_id if job_id is not None else new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=_START,
            # The retry-curve scalars a real enqueue stamps from the
            # actor's live ActorRef (taskq.client._args) — see this
            # module's docstring on why the reclaim contract requires a
            # source for the curve on the row itself.
            retry_base=policy.base,
            retry_cap=policy.cap,
            retry_backoff=policy.backoff,
            retry_jitter=policy.jitter,
        )
    )
    return job_id


async def _claim(backend: Backend, job_id: JobId, worker_id: UUID) -> int:
    """Run a production claim round and return the attempt it stamped."""
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert job_id in {row.id for row in dispatched}, (
        "the scenario requires the job to have been claimed for an attempt"
    )
    row = await backend.get(job_id)
    assert row is not None
    return row.attempt


async def _expire_lease(backend: Backend, job_id: JobId) -> None:
    """Age the job's lease into the past — the state a crashed worker leaves
    behind, with no terminal write ever arriving.

    The row's own column is moved rather than waiting out a real lease, so the
    reclaim predicate is reached deterministically.
    """
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing the crashed-holder state the public API cannot reach; mirrors tests/test_reclaim_retry_budget_parity.py
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            lock_expires_at=backend._clock.now() - timedelta(seconds=10),  # pyright: ignore[reportPrivateUsage]  # Why: the reclaim predicate is arbitrated by the backend's own clock
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the id is $-bound
            "SET lock_expires_at = clock_timestamp() - interval '10 seconds' WHERE id = $1",
            job_id,
        )


async def _now_of(backend: Backend) -> datetime:
    """The backend's own clock — the arbiter that stamped ``scheduled_at``.

    Reclaim schedules relative to the database's clock, so the delay it
    applied must be measured in that same domain; this process's clock would
    fold app-to-database skew into the measurement.
    """
    if isinstance(backend, InMemoryBackend):
        return backend._clock.now()  # pyright: ignore[reportPrivateUsage]  # Why: the twin's clock is the arbiter of its own scheduling, as above
    assert isinstance(backend, PostgresBackend)
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        value = await conn.fetchval("SELECT clock_timestamp()")
    assert isinstance(value, datetime)
    return value


async def _reclaim_delay(backend: Backend, job_id: JobId) -> timedelta:
    """Crash-reclaim the job and return how far out it was rescheduled."""
    await _expire_lease(backend, job_id)
    before = await _now_of(backend)
    assert await backend.reclaim_expired_locks(_GRACE, _GRACE) == 1, (
        "the scenario requires the expired-lease job to have been reclaimed"
    )
    row = await backend.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), (
        f"the scenario requires a retryable job to be handed back, got {row.status!r}"
    )
    return row.scheduled_at - before


async def test_reclaim_applies_the_actors_configured_backoff(
    backend_pair: Backend,
) -> None:
    """A reclaimed job comes back on its actor's retry curve.

    An actor whose policy sets a five-minute base did so to keep pressure off
    something fragile. A worker crash is not a reason to override that — if
    anything it is a reason to respect it, since the downstream is least
    likely to be healthy at that moment.
    """
    worker_id = await _worker_of(backend_pair)
    job_id = await _enqueue(backend_pair)
    attempt = await _claim(backend_pair, job_id, worker_id)

    delay = await _reclaim_delay(backend_pair, job_id)

    lower = compute_backoff(_PROTECTIVE_POLICY, attempt, max_retry_backoff=timedelta(days=1))
    # The policy's own jitter band is what makes a single expected value
    # wrong to assert; the floor below is the band's lower edge, which a flat
    # constant cannot reach.
    floor = _PROTECTIVE_POLICY.base * (1 - _PROTECTIVE_POLICY.jitter)
    assert delay >= floor, (
        f"a crash-reclaimed job was rescheduled {delay} out, below the "
        f"{floor} floor its actor's retry policy produces "
        f"(base={_PROTECTIVE_POLICY.base}, cap={_PROTECTIVE_POLICY.cap}, "
        f"backoff={_PROTECTIVE_POLICY.backoff!r}). Reclaim is stamping a flat "
        f"{_FLAT_RECLAIM_DELAY} instead of the actor's curve, so an actor "
        f"configured to protect a fragile downstream hammers it seconds after "
        f"every worker crash — the moment that downstream is least likely to "
        f"be healthy. Every other retry path derives its delay from the same "
        f"policy (reference delay for this attempt: {lower})"
    )
    assert delay <= _PROTECTIVE_POLICY.cap, (
        f"the reclaim delay {delay} exceeded the policy cap "
        f"{_PROTECTIVE_POLICY.cap}; the cap bounds every retry path"
    )


async def test_reclaim_jitters_a_cohort_reclaimed_together(
    backend_pair: Backend,
) -> None:
    """Jobs reclaimed by one sweep do not all become due at the same instant.

    This is the fleet-wide-event case: a node drain or zone loss expires many
    leases at once, and one sweep hands the whole cohort back. Stamping them
    all with the same delay turns recovery into a synchronized wave against a
    fleet that is still coming up. Jitter is the mechanism that spreads it,
    and it is the actor's policy that configures it.
    """
    worker_id = await _worker_of(backend_pair)
    job_ids = [await _enqueue(backend_pair) for _ in range(8)]

    # One claim round takes the whole cohort, as a single worker's dispatch
    # would, and one crash then strands all of them together.
    claimed = await backend_pair.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=len(job_ids),
        lock_lease=_LOCK_LEASE,
    )
    assert {row.id for row in claimed} == set(job_ids), (
        "the scenario requires one worker to be holding the whole cohort when it crashes"
    )
    for job_id in job_ids:
        await _expire_lease(backend_pair, job_id)

    reclaimed = await backend_pair.reclaim_expired_locks(_GRACE, _GRACE)
    assert reclaimed == len(job_ids), (
        f"the scenario requires the whole crashed cohort to be reclaimed by one "
        f"sweep, got {reclaimed} of {len(job_ids)}"
    )

    due: list[datetime] = []
    for job_id in job_ids:
        row = await backend_pair.get(job_id)
        assert row is not None
        due.append(row.scheduled_at)

    spread = max(due) - min(due)
    # The jitter band the policy configures for this attempt. The bar is a
    # fraction of it rather than the whole band because eight draws need not
    # reach both edges — but it is far above the sub-millisecond drift a
    # per-row ``clock_timestamp()`` produces while stamping a flat constant,
    # which spreads nothing and protects nothing.
    band = _PROTECTIVE_POLICY.base * _PROTECTIVE_POLICY.jitter
    required = band / 4
    assert spread >= required, (
        f"{len(job_ids)} jobs reclaimed by one sweep became due within {spread} "
        f"of each other, effectively the same instant (first due {min(due)}). A "
        f"node drain or zone loss expires many leases together, so this is the "
        f"shape of a real fleet event: the entire cohort lands on the recovering "
        f"fleet as one synchronized wave. The actor's retry policy configures "
        f"jitter (±{band}) precisely to spread this, and the reclaim path is the "
        f"one path that ignores it — it stamps a flat {_FLAT_RECLAIM_DELAY} on "
        f"every row"
    )


async def test_reclaim_delay_is_capped_by_the_global_backoff_ceiling(
    backend_pair: Backend,
) -> None:
    """A policy cap above the operator ceiling does not carry a reclaim past it.

    The failure path clamps every retry delay at ``min(policy.cap,
    max_retry_backoff)`` — 24 hours at the default
    (``WorkerSettings.max_retry_backoff``). Crash/heartbeat reclaim is the
    same rescheduling decision and must clamp at the same value on both
    backends: a row stamped with a multi-day cap otherwise comes back days
    later on Postgres while the in-memory twin says hours — the twin
    certifying a delay Postgres never applies.
    """
    worker_id = await _worker_of(backend_pair)
    job_id = await _enqueue(backend_pair, policy=_WIDE_POLICY)
    await _claim(backend_pair, job_id, worker_id)

    delay = await _reclaim_delay(backend_pair, job_id)

    assert delay >= DEFAULT_MAX_RETRY_BACKOFF * (1 - _WIDE_POLICY.jitter), (
        f"a crash-reclaimed job was rescheduled {delay} out, below the jitter "
        f"band under the {DEFAULT_MAX_RETRY_BACKOFF} ceiling: with a policy whose "
        "curve sits above the ceiling, the band fitted under the ceiling — not "
        "a smaller value — must decide the delay"
    )
    assert delay <= DEFAULT_MAX_RETRY_BACKOFF + _CLOCK_GAP_SLACK, (
        f"a crash-reclaimed job with retry_cap={_WIDE_POLICY.cap} was "
        f"rescheduled {delay} out, past the {DEFAULT_MAX_RETRY_BACKOFF} "
        "operator ceiling. The failure path clamps at min(policy.cap, "
        "max_retry_backoff); reclaim must apply the same effective cap or an "
        "actor's multi-day cap strands reclaimed jobs for days"
    )


async def test_reclaim_delay_honours_a_non_default_backoff_ceiling(
    backend_pair: Backend,
) -> None:
    """The operator's configured ``max_retry_backoff`` — not a hardcoded
    constant — is the ceiling reclaim applies.

    A smaller operator ceiling tightens the clamp on both backends; this is
    what stops a misconfigured per-actor cap from stranding reclaimed jobs
    past the fleet's own bound.
    """
    ceiling = timedelta(hours=2)
    if isinstance(backend_pair, InMemoryBackend):
        # Why: the twin's WorkerSettings-knob seam is its constructor, which
        # the shared fixture owns; the sweep reads the value per call, so
        # setting it before the call is the same wiring a configured twin
        # has.
        backend_pair._max_retry_backoff = ceiling  # pyright: ignore[reportPrivateUsage]
    else:
        assert isinstance(backend_pair, PostgresBackend)
        # Why: the operator knob lives on WorkerSettings inside the backend's
        # deps, and the sweep reads it per call — the same seam the suite's
        # interval-shrinking tests use.
        backend_pair._deps.settings.max_retry_backoff = ceiling  # pyright: ignore[reportPrivateUsage]
    worker_id = await _worker_of(backend_pair)
    job_id = await _enqueue(backend_pair, policy=_WIDE_POLICY)
    await _claim(backend_pair, job_id, worker_id)

    delay = await _reclaim_delay(backend_pair, job_id)

    assert delay >= ceiling * (1 - _WIDE_POLICY.jitter), (
        f"a crash-reclaimed job was rescheduled {delay} out, below the jitter "
        f"band under the operator's configured {ceiling} ceiling: with a policy "
        "whose curve sits above the ceiling, the band fitted under the ceiling — "
        "not a smaller value — must decide the delay"
    )
    assert delay <= ceiling + _CLOCK_GAP_SLACK, (
        f"a crash-reclaimed job was rescheduled {delay} out under an "
        f"operator-configured max_retry_backoff of {ceiling} — the reclaim "
        "path kept the 24 h default (or the policy cap) instead of the "
        "configured ceiling, so the knob does not reach the sweep"
    )


# ── Deterministic jitter: the fraction and its cross-backend parity ────────
#
# The reclaim delay's jitter fraction is DERIVED from the row, never drawn:
# ``md5(j.id::text || ':' || j.attempt::text)`` — first 8 hex digits as a
# uint32 over 2**32 — computed byte-identically by ``_RECLAIM_DELAY_SQL`` in
# the database and by ``taskq.retry._compute_reclaim_backoff`` in Python.
# One row's reclaim delay is computed by more than one statement (the
# leader's sweep and a partitioned worker's isolate_self can each transition
# the same row within one outage window) and replayed by later sweeps; a
# per-statement ``random()`` makes those paths disagree about one row's
# hand-back instant, while the derived fraction makes every path stamp the
# same delay — replay-idempotent per row — and keeps the fleet spread
# (distinct ids hash to distinct fractions).  These pins assert that parity
# exactly: no buckets, no bands.

#: Fixed (job id, attempt) inputs for the formula pin — the pin draws
#: nothing, so its inputs are fixed.  attempt=0 pins the
#: direct-construction corner (the SQL floors the exponent with
#: GREATEST(attempt - 1, 0) and hashes the raw attempt); attempt=1000
#: exercises the deep-exponent arm.  attempt=1024 pins the float8-overflow
#: corner: with the exponent clamped only at power()'s domain ceiling,
#: ``base * power(2.0, attempt - 1)`` overflowed float8 for base > ~2 and
#: RAISED SQLSTATE 22003 where the Python twin saturated to the cap — the
#: clamp must keep the multiply itself in range (the curve has long
#: saturated against the cap there).  attempt=32767 is the smallint
#: ceiling, the deepest attempt the column can stamp.
_FORMULA_ROWS: tuple[tuple[UUID, int], ...] = (
    (UUID("01906e5a-0000-7000-8000-000000000001"), 0),
    (UUID("01906e5a-0000-7000-8000-000000000002"), 1),
    (UUID("01906e5a-0000-7000-8000-000000000003"), 2),
    (UUID("01906e5a-0000-7000-8000-000000000004"), 7),
    (UUID("01906e5a-0000-7000-8000-000000000005"), 1000),
    (UUID("01906e5a-0000-7000-8000-000000000006"), 1024),
    (UUID("01906e5a-0000-7000-8000-000000000007"), 32767),
)

#: The curve shapes the reclaim formula branches on: all three backoff
#: kinds, zero / mid / full jitter, and a policy whose cap sits above the
#: operator ceiling (exercising LEAST(retry_cap_seconds, max_retry_backoff)).
#: The trailing zero policy pins the degenerate-cap contract: cap = 0 (with
#: base = 0, the only shape RetryPolicy's cap >= base admits) means a zero
#: delay on both evaluators — preserved deliberately across the
#: overflow-clamp change, which introduced no division that a zero cap
#: could fault.
_FORMULA_POLICIES: tuple[RetryPolicy, ...] = (
    RetryPolicy(
        backoff="exponential", base=timedelta(seconds=5), cap=timedelta(hours=1), jitter=0.2
    ),
    RetryPolicy(backoff="linear", base=timedelta(seconds=3), cap=timedelta(minutes=10), jitter=0.5),
    RetryPolicy(backoff="fixed", base=timedelta(seconds=30), cap=timedelta(hours=2), jitter=1.0),
    RetryPolicy(
        backoff="exponential", base=timedelta(seconds=5), cap=timedelta(hours=1), jitter=0.0
    ),
    RetryPolicy(backoff="fixed", base=timedelta(days=3), cap=timedelta(days=7), jitter=0.2),
    RetryPolicy(backoff="exponential", base=timedelta(0), cap=timedelta(0), jitter=0.2),
)

# The shipped fragments, evaluated against a one-row VALUES table aliased
# like the sweep's update target so the ``j.``-scoped references resolve
# with the real column types (attempt is smallint on ``jobs``).
_FRACTION_PROBE_SQL = (
    f"SELECT ({_RECLAIM_JITTER_FRACTION_SQL}) AS fraction "  # noqa: S608  # Why: the interpolated text is the shipped module constant under test, never user input; every runtime value is $N-bound.
    "FROM (VALUES ($1::uuid, $2::smallint)) AS j(id, attempt)"
)
_DELAY_PROBE_SQL = (
    f"SELECT ({_RECLAIM_DELAY_SQL.replace('{max_backoff_seconds}', '$7')}) AS delay "  # noqa: S608  # Why: same — the interpolated text is the shipped fragment under test; every runtime value is $N-bound.
    "FROM (VALUES ($1::uuid, $2::smallint, $3::float8, $4::float8, $5::text, $6::float8)) "
    "AS j(id, attempt, retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter)"
)


@pytest.mark.integration
async def test_reclaim_delay_formula_matches_the_sql_fragment_bit_for_bit(
    pg_dsn: str,
) -> None:
    """A given (job id, attempt, policy) yields the identical reclaim delay
    in SQL and Python — bit for bit.

    The SQL fragment and the Python twin run the same correctly-rounded
    IEEE-754 operations in the same order over the same hash-derived
    fraction, so exact equality — not a band — is the assertable contract.
    A per-statement ``random()`` (the old shape) makes virtually every row
    here mismatch, and a formula drift on either side (a different hash
    slice, a reordered operand, a numeric-typed intermediate) fails this
    pin just as loudly.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        for job_id, attempt in _FORMULA_ROWS:
            sql_fraction = await conn.fetchval(_FRACTION_PROBE_SQL, job_id, attempt)
            py_fraction = _reclaim_jitter_fraction(job_id, attempt)
            assert sql_fraction == py_fraction, (
                f"jitter fraction diverged for ({job_id}, {attempt}): "
                f"SQL {sql_fraction!r} vs Python {py_fraction!r} — the two sides "
                "must derive the identical fraction from md5('<id>:<attempt>') "
                "(first 8 hex digits as uint32, over 2**32, in float8)"
            )
            for policy in _FORMULA_POLICIES:
                sql_delay = await conn.fetchval(
                    _DELAY_PROBE_SQL,
                    job_id,
                    attempt,
                    policy.base.total_seconds(),
                    policy.cap.total_seconds(),
                    policy.backoff,
                    policy.jitter,
                    DEFAULT_MAX_RETRY_BACKOFF.total_seconds(),
                )
                py_delay = _compute_reclaim_backoff(
                    policy,
                    attempt,
                    job_id=job_id,
                    max_retry_backoff=DEFAULT_MAX_RETRY_BACKOFF,
                )
                assert sql_delay == py_delay, (
                    f"reclaim delay diverged for ({job_id}, attempt={attempt}, "
                    f"backoff={policy.backoff!r}, base={policy.base}, "
                    f"cap={policy.cap}, jitter={policy.jitter}): "
                    f"SQL {sql_delay!r} vs Python {py_delay!r} — the sweep, the "
                    "isolate, and the in-memory mirror must all stamp the same "
                    "delay for the same row"
                )
    finally:
        await conn.close()


#: The fixed row identity the cross-backend sweep pin reclaims on both
#: backends — fixed so the pin draws nothing.
_SWEEP_PARITY_JOB_ID = JobId(UUID("01906e5a-0000-7000-8000-00000000b001"))


@pytest.mark.integration
async def test_reclaim_delay_exponential_arm_saturates_at_cap_past_the_overflow_corner(
    pg_dsn: str,
) -> None:
    """attempt >= 1024 must saturate at the cap, never raise out of the driver.

    Postgres RAISES ``value out of range: overflow`` (SQLSTATE 22003,
    surfaced by asyncpg as ``NumericValueOutOfRangeError``) when a float8
    multiply exceeds ~1.8e308 — it does not saturate to Infinity the way
    Python's float arithmetic does.  With the exponent clamped only at
    ``power()``'s own domain ceiling, ``base * power(2.0, attempt - 1)``
    overflowed for any base > ~2 once the attempt passed ~1021: a
    non-transient data error escaping into the leader sweep's failure
    path (tearing down the reclaim loop on a deliberately non-retryable
    class), on a corner the Python twin answered with the cap.  The
    shared fragment bounds the exponent so the multiply stays inside
    float8 for every timedelta-representable base while the LEAST
    against the effective cap still decides the value — so a jitter=0
    policy at a saturated attempt stamps EXACTLY the cap, bit-identical
    to the twin.
    """
    policy = RetryPolicy(
        backoff="exponential", base=timedelta(seconds=5), cap=timedelta(hours=1), jitter=0.0
    )
    job_id = UUID("01906e5a-0000-7000-8000-00000000c001")
    conn = await asyncpg.connect(pg_dsn)
    try:
        # 1024 is past the old raise point; 32767 is the smallint ceiling,
        # the deepest attempt the column can stamp.
        for attempt in (1024, 32767):
            sql_delay = await conn.fetchval(
                _DELAY_PROBE_SQL,
                job_id,
                attempt,
                policy.base.total_seconds(),
                policy.cap.total_seconds(),
                policy.backoff,
                policy.jitter,
                DEFAULT_MAX_RETRY_BACKOFF.total_seconds(),
            )
            py_delay = _compute_reclaim_backoff(
                policy,
                attempt,
                job_id=job_id,
                max_retry_backoff=DEFAULT_MAX_RETRY_BACKOFF,
            )
            assert sql_delay == policy.cap, (
                f"attempt={attempt}: the saturated exponential arm must stamp exactly "
                f"the cap, got {sql_delay!r} — the curve reaches the cap long before "
                "the exponent clamp, and the multiply must not raise"
            )
            assert py_delay == policy.cap, (
                f"attempt={attempt}: the Python twin must stamp exactly the cap, got {py_delay!r}"
            )
            assert sql_delay == py_delay, (
                f"attempt={attempt}: SQL {sql_delay!r} vs Python {py_delay!r} — the "
                "overflow corner must not become a parity break"
            )
    finally:
        await conn.close()


@pytest.mark.integration
async def test_reclaim_sweep_stamps_the_identical_delay_on_both_backends(
    pg_dsn: str,
) -> None:
    """End to end: one fixed (job id, attempt) row, reclaimed through the real
    sweep on each backend, is rescheduled by the same delay.

    The mirror's FakeClock is frozen, so its measurement is exact. PG's
    stamp is bracketed between server-clock reads taken around the sweep —
    the sweep's own ``clock_timestamp()`` is unobservable from outside — and
    the bracket is milliseconds wide while the policy's jitter band here is
    [240 s, 360 s), so a freshly-drawn jitter (the old per-statement
    ``random()``) falls outside it on virtually every run. Both measurements
    are compared against the Python twin's value, which the formula pin
    above ties to the SQL fragment bit for bit.
    """
    policy = _PROTECTIVE_POLICY
    expected = _compute_reclaim_backoff(
        policy,
        1,
        job_id=_SWEEP_PARITY_JOB_ID,
        max_retry_backoff=DEFAULT_MAX_RETRY_BACKOFF,
    )
    # Both backends apply the same effective ceiling here: the mirror's
    # constructor default and the PG settings default are both
    # DEFAULT_MAX_RETRY_BACKOFF (24 h), and the policy's own cap (1 h)
    # binds first — so the expected value reads the row's curve, not the
    # ceiling.
    memory = InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    memory.register_actor_config(actor="actor_a")
    stack, _deps, pg = await _open_pg_backend(pg_dsn, schema_name=f"trbp_{new_base62()}".lower())
    try:
        for backend in (memory, pg):
            worker_id = await _worker_of(backend)
            await _enqueue(backend, job_id=_SWEEP_PARITY_JOB_ID, policy=policy)
            attempt = await _claim(backend, _SWEEP_PARITY_JOB_ID, worker_id)
            assert attempt == 1, "the pin's delay is keyed on the row's first attempt"
            await _expire_lease(backend, _SWEEP_PARITY_JOB_ID)
            before = await _now_of(backend)
            assert await backend.reclaim_expired_locks(_GRACE, _GRACE) == 1, (
                "the scenario requires the expired-lease job to have been reclaimed"
            )
            after = await _now_of(backend)
            row = await backend.get(_SWEEP_PARITY_JOB_ID)
            assert row is not None and row.status == "pending"
            assert before + expected <= row.scheduled_at <= after + expected, (
                f"{type(backend).__name__} stamped scheduled_at={row.scheduled_at!r}, "
                f"outside [before + expected, after + expected] with "
                f"before={before!r}, after={after!r}, expected delay={expected!r} "
                f"for (job_id, attempt)=({_SWEEP_PARITY_JOB_ID}, 1) — every "
                "reclaim path must stamp the row's derived delay, not a fresh draw"
            )
        mem_row = await memory.get(_SWEEP_PARITY_JOB_ID)
        assert mem_row is not None
        assert mem_row.scheduled_at - _START == expected, (
            f"the mirror's frozen clock makes the measurement exact: "
            f"got {mem_row.scheduled_at - _START!r}, expected {expected!r}"
        )
    finally:
        await stack.aclose()

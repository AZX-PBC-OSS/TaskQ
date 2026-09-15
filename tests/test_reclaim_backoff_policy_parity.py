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

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import JobId
from taskq.backend.postgres import PostgresBackend
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF
from taskq.retry import RetryPolicy, compute_backoff
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
#: ``backoff='fixed'`` the curve is flat at base, so every jitter draw lands
#: above the ceiling and the clamp — not the draw — decides the stamped
#: delay, deterministically.
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
    backend: Backend, *, max_attempts: int = 5, policy: RetryPolicy = _PROTECTIVE_POLICY
) -> JobId:
    job_id = new_job_id()
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

    assert delay >= DEFAULT_MAX_RETRY_BACKOFF, (
        f"a crash-reclaimed job was rescheduled {delay} out, under the "
        f"{DEFAULT_MAX_RETRY_BACKOFF} ceiling: with a policy whose every "
        "jitter draw exceeds the ceiling, the clamp — not a smaller value — "
        "must decide the delay"
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

    assert delay >= ceiling, (
        f"a crash-reclaimed job was rescheduled {delay} out, under the "
        f"operator's configured {ceiling} ceiling: with a policy whose every "
        "jitter draw exceeds the ceiling, the clamp — not a smaller value — "
        "must decide the delay"
    )
    assert delay <= ceiling + _CLOCK_GAP_SLACK, (
        f"a crash-reclaimed job was rescheduled {delay} out under an "
        f"operator-configured max_retry_backoff of {ceiling} — the reclaim "
        "path kept the 24 h default (or the policy cap) instead of the "
        "configured ceiling, so the knob does not reach the sweep"
    )

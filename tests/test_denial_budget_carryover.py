"""Retry budget across the deny / reschedule / run cycle, driven through the
production dispatch and consumer path.

An admission denial is HTTP-429 semantics: the limiter said "come back
later", the actor body never ran, and nothing about the job failed.  The
retry budget exists to bound *failures* - how many times a worker will
re-run an actor whose execution went wrong.  These two facts have a
consequence that only shows up one cycle later, once capacity frees:

    the budget a job has left for its real work must not depend on how
    long it waited for a slot.

An operator configures ``max_attempts=3`` meaning "run this up to three
times before giving up".  If a saturated bucket denies the job first, the
job must still get its three runs.  It must not arrive at its first real
execution with the budget already spent by waiting, because the operator
never authorised that trade and cannot see it coming: bucket saturation is
a property of the fleet's load at that moment, so the same job with the
same configuration gets a different number of real retries depending on how
busy the queue happened to be.  That is an unreproducible, load-dependent
retry policy.

The tests here drive the whole cycle - real ``dispatch_batch`` claim, real
``RateLimitRegistry`` acquire against a real exhausted ``TokenBucket``, the
production consumer's denial handling and terminal write, the
scheduled-to-pending promotion - and then assert what an operator can read
off the job row and the attempt history at the end.  Both backends are
exercised through ``backend_pair``, because a divergence here would mean
production and the in-memory test double disagree about how much retry
budget a job has.

The control in the other direction is pinned too: a genuine execution
failure MUST consume budget.  The distinction between "the system declined
to run you" and "you ran and broke" is the whole point, and a fix that
refunds denials must not also refund failures.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend import Backend, JobRow
from taskq.backend._protocol import JobId
from taskq.backend.clock import Clock, SystemClock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration

_ACTOR = "test_actor"
_QUEUE = "default"
_LEASE = timedelta(seconds=60)
_MAX_ATTEMPTS = 3
_DENIAL_ROUNDS = 5


class _EmptyPayload(BaseModel):
    """The actor's payload model - the jobs here carry no meaningful input."""


# ── Backend-agnostic drivers ───────────────────────────────────────────


def _now_for(backend: Backend) -> datetime:
    """The backend's own notion of now.

    The in-memory backend is driven by an injected ``FakeClock``; the PG
    backend is arbitrated by the server clock, which the client clock
    approximates closely enough for seeding.
    """
    if isinstance(backend, InMemoryBackend):
        return backend._clock.now()  # type: ignore[reportPrivateUsage]  # Why: the injected clock is this backend's canonical time source; there is no public reader.
    return datetime.now(UTC)


async def _register_actor(backend: Backend) -> None:
    """Make the actor dispatchable: dispatch draws candidates from the
    actor-config registry on both backends, so an unregistered actor has
    zero capacity and claims nothing."""
    if isinstance(backend, InMemoryBackend):
        backend.register_actor_config(actor=_ACTOR)
        return
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: PG-path test helper, the pattern the equivalence harness uses.
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same.
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs.
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '  # noqa: S608
            "ON CONFLICT (actor) DO NOTHING",
            _ACTOR,
            _QUEUE,
        )


async def _register_worker(backend: Backend) -> UUID:
    """Return a worker id the backend's terminal writes will accept."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: the backend's own worker identity; no public reader.
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: PG-path test helper.
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same.
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs.
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608
            worker_id,
            "test-host",
            12345,
            [_QUEUE],
        )
    return worker_id


async def _let_the_backoff_elapse(backend: Backend) -> None:
    """Move the world past the reschedule point and run the promotion sweep.

    A denied job is rescheduled with a backoff; the leader's
    scheduled-to-pending sweep is what makes it claimable again.  Time
    moves deterministically: the in-memory backend's injected clock is
    advanced, and the PG job's ``scheduled_at`` is pushed into the server
    clock's past.  No wall-clock waiting is involved in either case.
    """
    if isinstance(backend, InMemoryBackend):
        backend.advance_clock_to(_now_for(backend) + timedelta(minutes=5))
        await backend.scheduled_to_pending()
        return
    import asyncpg

    schema: str = backend._schema_name  # type: ignore[reportPrivateUsage]  # Why: PG-path test helper.
    pool: asyncpg.Pool = backend._worker_pool  # type: ignore[reportPrivateUsage]  # Why: same.
    async with pool.acquire() as conn:  # type: ignore[reportUnknownVariableType]  # Why: asyncpg stubs.
        # A generous margin into the server clock's past: the client and
        # server keep separate clocks that can disagree by whole seconds.
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608
            "SET scheduled_at = clock_timestamp() - interval '30 seconds' "
            "WHERE status = 'scheduled'"
        )
    await backend.scheduled_to_pending()


def _saturated_bucket_registry(name: str) -> RateLimitRegistry:
    """A registry whose single bucket holds no tokens and never refills -
    a saturated limiter, so every acquire is a real denial raised by the
    real acquire path rather than an injected exception."""
    registry = RateLimitRegistry()
    registry.register(TokenBucket(name, capacity=1.0, refill_per_second=0.0, backend="memory"))
    return registry


def _open_bucket_registry(name: str) -> RateLimitRegistry:
    """A registry with ample capacity - the limiter after the operator
    scaled the bucket, or after the burst passed."""
    registry = RateLimitRegistry()
    registry.register(
        TokenBucket(name, capacity=1000.0, refill_per_second=1000.0, backend="memory")
    )
    return registry


async def _drain(registry: RateLimitRegistry, name: str, clock: Clock) -> None:
    """Consume the bucket's only token so the next acquire denies."""
    bucket = registry._rate_limits[name]  # type: ignore[reportPrivateUsage]  # Why: the registry has no public reader for a registered primitive; draining it is setup, not the behaviour under test.
    assert isinstance(bucket, TokenBucket)
    decision = await bucket.acquire(1.0, clock=clock)
    assert decision.allowed, "setup: the fresh bucket must hand out its one token"


async def _claim_one(backend: Backend, worker_id: UUID) -> JobRow:
    """Claim exactly one job through the production dispatch path."""
    claimed = await backend.dispatch_batch(worker_id, [_QUEUE], 5, _LEASE)
    assert len(claimed) == 1, (
        f"expected exactly one claimable job, got {len(claimed)} - the scenario "
        "seeds a single job and promotes it before each round"
    )
    return claimed[0]


# ── The scenario ───────────────────────────────────────────────────────


async def test_denials_do_not_spend_the_budget_the_first_real_run_needs(
    backend_pair: Backend,
) -> None:
    """A job denied while waiting for capacity still gets its configured
    number of real executions once capacity frees.

    The operator's contract is ``max_attempts=3``: run the actor up to
    three times before giving up.  Here the job is denied five times by a
    saturated bucket - the actor body never runs, nothing fails - and then
    capacity frees and the actor starts failing for real.

    The job must get three real executions.  If the waiting spent the
    budget, the first real execution is also the last: the job is written
    ``failed`` with the actor's error and exactly one attempt row, having
    been retried zero times.  An operator reading that row sees a job
    configured for three tries that died on its first, and nothing in the
    row explains why - the denials left no attempt rows and no events, so
    the missing retries are invisible.  Worse, the number of real retries
    the job actually gets is a function of how saturated the bucket was,
    which makes the retry policy unreproducible and load-dependent.

    ``schedule_to_close`` is set deliberately far in the future: it keeps
    the denial loop from terminalising the job on its own, so what this
    test measures is purely how much budget survives the wait.
    """
    backend = backend_pair
    clock: Clock = (
        backend._clock  # type: ignore[reportPrivateUsage]  # Why: the acquire path needs the backend's own clock so both agree on time.
        if isinstance(backend, InMemoryBackend)
        else SystemClock()
    )
    await _register_actor(backend)
    worker_id = await _register_worker(backend)

    now = _now_for(backend)
    enqueued = await backend.enqueue(
        make_enqueue_args(
            actor=_ACTOR,
            queue=_QUEUE,
            max_attempts=_MAX_ATTEMPTS,
            retry_kind="transient",
            scheduled_at=now - timedelta(seconds=5),
            # Far enough out that the deadline arm can never fire during
            # the scenario: the only thing that could end this job is the
            # retry budget, which is exactly what is under measurement.
            schedule_to_close=now + timedelta(days=365),
        )
    )
    job_id: JobId = enqueued.id

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=_MAX_ATTEMPTS, jitter=0.0)
    )
    executions = 0

    async def failing_actor(_payload: object, _ctx: object) -> object:
        nonlocal executions
        executions += 1
        raise RuntimeError("the downstream this actor calls is broken")

    # ── Phase 1: the bucket is saturated; every claim ends in a denial ──
    denied_registry = _saturated_bucket_registry("saturated")
    await _drain(denied_registry, "saturated", clock)

    for _ in range(_DENIAL_ROUNDS):
        await _let_the_backoff_elapse(backend)
        job = await _claim_one(backend, worker_id)
        outcome = await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=failing_actor,
            actor_config=actor_config,
            payload_type=_EmptyPayload,
            clock=clock,
            rate_limit_registry=denied_registry,
            rate_limits=["saturated"],
        )
        assert outcome == "scheduled", (
            f"a denial must reschedule the job, got outcome {outcome!r} - "
            "backpressure delays work, it never ends it"
        )

    assert executions == 0, (
        f"the actor body ran {executions} times during the denial phase; a denied "
        "job is one the limiter declined to admit, so no handler may have run"
    )

    denied_row = await backend.get(job_id)
    assert denied_row is not None
    assert denied_row.rate_limit_blocked_count == _DENIAL_ROUNDS, (
        f"the aggregated denial counter reads {denied_row.rate_limit_blocked_count} after "
        f"{_DENIAL_ROUNDS} denials; with no per-denial rows written it is the only way "
        "contention stays visible to an operator"
    )
    assert denied_row.max_attempts == _MAX_ATTEMPTS, (
        f"max_attempts drifted to {denied_row.max_attempts}; the configured ceiling is a "
        "bound the operator set, never a counter the system adjusts"
    )

    # ── Phase 2: capacity frees; the actor runs and fails for real ──────
    open_registry = _open_bucket_registry("scaled_up")

    outcomes: list[str] = []
    for _ in range(_MAX_ATTEMPTS + 1):
        await _let_the_backoff_elapse(backend)
        current = await backend.get(job_id)
        assert current is not None
        if current.status in ("failed", "succeeded", "cancelled"):
            break
        job = await _claim_one(backend, worker_id)
        outcomes.append(
            await consume_one_job(
                backend,
                job,
                worker_id,
                run_actor=failing_actor,
                actor_config=actor_config,
                payload_type=_EmptyPayload,
                clock=clock,
                rate_limit_registry=open_registry,
                rate_limits=["scaled_up"],
            )
        )

    final = await backend.get(job_id)
    assert final is not None
    attempts = await backend.get_attempts(job_id)

    assert executions == _MAX_ATTEMPTS, (
        f"the actor body ran {executions} time(s) before the job was written "
        f"{final.status!r}, but max_attempts is {_MAX_ATTEMPTS}. The "
        f"{_DENIAL_ROUNDS} denials that preceded it spent the budget the real work "
        "needed: the operator asked for three runs and the job got "
        f"{executions}. How many real retries a job gets must not depend on how "
        "saturated the bucket was while it waited - that makes the retry policy "
        "load-dependent and unreproducible, and nothing on the row explains the "
        "shortfall because denials write no attempt rows and no events."
    )
    assert len(attempts) == _MAX_ATTEMPTS, (
        f"the attempt history holds {len(attempts)} row(s) for a job configured with "
        f"max_attempts={_MAX_ATTEMPTS} that failed every real execution; the history an "
        "operator debugs from must show every run the budget paid for"
    )
    assert final.status == "failed", (
        f"the job ended {final.status!r}; after exhausting its budget on genuine "
        "execution failures it belongs in 'failed'"
    )


async def test_a_genuine_execution_failure_still_spends_the_budget(
    backend_pair: Backend,
) -> None:
    """The distinction that makes the refund meaningful: an actor that ran
    and raised consumes an attempt.

    A denial is the system declining to run the job; a failure is the job
    running and going wrong.  Only the second is what ``max_attempts``
    bounds.  This is the control for the test above - a change that stops
    denials from spending budget must not also stop failures from spending
    it, or ``max_attempts`` stops bounding anything and a permanently
    broken actor retries forever.

    Here no limiter is involved at all: the actor fails every time,
    and the job must reach a terminal ``failed`` after exactly
    ``max_attempts`` executions, with one attempt row per execution.
    """
    backend = backend_pair
    clock: Clock = (
        backend._clock  # type: ignore[reportPrivateUsage]  # Why: both backends must read the same clock the terminal writes use.
        if isinstance(backend, InMemoryBackend)
        else SystemClock()
    )
    await _register_actor(backend)
    worker_id = await _register_worker(backend)

    now = _now_for(backend)
    enqueued = await backend.enqueue(
        make_enqueue_args(
            actor=_ACTOR,
            queue=_QUEUE,
            max_attempts=_MAX_ATTEMPTS,
            retry_kind="transient",
            scheduled_at=now - timedelta(seconds=5),
            schedule_to_close=now + timedelta(days=365),
        )
    )
    job_id: JobId = enqueued.id

    actor_config = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=_MAX_ATTEMPTS, jitter=0.0)
    )
    executions = 0

    async def failing_actor(_payload: object, _ctx: object) -> object:
        nonlocal executions
        executions += 1
        raise RuntimeError("the downstream this actor calls is broken")

    for _ in range(_MAX_ATTEMPTS + 2):
        await _let_the_backoff_elapse(backend)
        current = await backend.get(job_id)
        assert current is not None
        if current.status in ("failed", "succeeded", "cancelled"):
            break
        job = await _claim_one(backend, worker_id)
        await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=failing_actor,
            actor_config=actor_config,
            payload_type=_EmptyPayload,
            clock=clock,
        )

    final = await backend.get(job_id)
    assert final is not None
    attempts = await backend.get_attempts(job_id)

    assert executions == _MAX_ATTEMPTS, (
        f"the actor ran {executions} time(s) for max_attempts={_MAX_ATTEMPTS}; a real "
        "execution that raised must spend an attempt, or a permanently broken actor "
        "retries forever and max_attempts bounds nothing"
    )
    assert final.status == "failed", (
        f"the job ended {final.status!r} after exhausting its budget on genuine failures; "
        "it belongs in 'failed'"
    )
    assert len(attempts) == _MAX_ATTEMPTS, (
        f"the attempt history holds {len(attempts)} row(s) for {_MAX_ATTEMPTS} real "
        "executions; every execution the budget paid for must leave its record"
    )

"""Admission denials are HTTP-429 semantics: come back later, never fail.

A rate-limited job that is denied admission — because the limiter's store
cannot answer (Redis unreachable, the PG fallback disabled) OR because the
bucket is simply saturated — has not executed. Its actor never ran, no
handler raised, nothing about the job itself went wrong. The denial is the
system saying "no capacity right now, come back later", exactly as an HTTP
429 with a ``Retry-After`` header does.

That makes every admission denial NON-CONSUMING, regardless of its cause:

* A denial never spends a unit of the job's retry budget. The claim's
  attempt increment is refunded by the denial write, so ``attempt`` does
  not creep and the configured ``max_attempts`` ceiling is not inflated to
  compensate.
* A denial never by itself terminalises a job. A denied job is rescheduled
  indefinitely with backoff until capacity frees or its
  ``schedule_to_close`` deadline expires; expiry fails it terminally
  through the ordinary deadline path as ``DeadlineExceeded``. A job whose
  only fault is that it never got a slot must never land in the terminal
  ``MaxAttemptsExceeded`` exit — that label asserts the actor ran and
  failed ``max_attempts`` times, and an operator reading it must be able
  to trust that.
* A denial writes no per-denial ``job_events`` or ``job_attempts`` rows.
  Contention stays observable through the aggregated
  ``rate_limit_blocked_count`` counter on the job row.

Why it matters: a queue or rate-limit misconfiguration must not be able to
kill work. A bucket sized too small, or a limiter store left down over a
weekend, is an operational problem to be seen in telemetry and fixed — not
a reason to destroy jobs that were never given a chance to run. Bounding
denial pressure is the deadline's job, not the retry budget's.

Store-outage denials stay DISTINGUISHABLE from saturation denials even
though both are non-consuming: the outage denial carries the
``rate_limit:unavailable:<error_type>`` awaiting annotation naming the
unavailability and its cause, so an operator scaling a bucket on denial
counts alone does not chase an outage with capacity.

The seam driven is the consumer level: a real ``dispatch_batch`` claim
(the production attempt increment), a real ``dispatch_one_job`` →
``consume_one_job`` acquire boundary → the production denial synthesis →
``_run_terminal_path`` → ``_handle_reservation_class_denied`` → the real
Postgres ``mark_snoozed`` arms. Budget consumption is a property of the
claim and the denial arm composing, and no narrower seam composes them.
"""

from datetime import timedelta
from typing import Any

import asyncpg
import pytest
import redis
import redis.asyncio as redis_async
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, JobId
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import dispatch_one_job
from tests._di_scopes import bootstrap_scopes, make_scopes

pytestmark = pytest.mark.integration

_WORKER_ID = new_uuid()

_MAX_ATTEMPTS = 3
"""The stock transient budget from the claim: attempt would reach the
ceiling on the third claim→denial cycle if denials consumed budget."""

_SUSTAINED_OUTAGE_CYCLES = _MAX_ATTEMPTS + 2
"""Cycle count past the budget boundary: the contract is that SUSTAINED
denial pressure keeps the job retryable, not merely pressure that ends
before the boundary is reached."""

_LOCK_LEASE = timedelta(seconds=60)

_ACTOR_RUNS: list[int] = [0]


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1


class _RaisingScript:
    """AsyncScript double: every invocation raises the injected error."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def __call__(self, **kwargs: object) -> object:
        raise self._error


def _dead_redis_client(error: Exception) -> redis_async.Redis:
    """A REAL ``redis.asyncio.Redis`` instance (dispatch resolves the client
    via ``isinstance(raw_redis, Redis)``) whose Lua script call fails with
    *error* — no container, no socket: the command surface is duck-typed."""
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    client.register_script = lambda script: _RaisingScript(error)  # type: ignore[method-assign]  # Why: injecting the failure at the script-call seam redis-py would use; no connection exists
    return client


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        scopes = make_scopes(self.registry)
        self.process_scope, self.thread_scope, self.loop_scope = scopes
        await bootstrap_scopes(self.registry, *scopes)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


def _di_registry_with_redis(
    rl_registry: RateLimitRegistry, client: redis_async.Redis
) -> ProviderRegistry:
    from taskq.ratelimit._provider import register_rate_limit_registry

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    di_registry.register_value(redis_async.Redis, Scope.LOOP, client)
    return di_registry


def _rate_limited_actor_ref(bucket: TokenBucket) -> Any:
    return ActorRef(
        name="test_actor",
        queue="default",
        fn=_noop_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not exercised on the denial path
        retry=RetryPolicy(jitter=0.0),
        result_ttl=None,
        rate_limits=[bucket],
    )


async def _requeue_due(deps: WorkerDeps, schema: str, job_id: JobId) -> None:
    """Put the snoozed job back in the claimable set.

    Applies exactly the due transition ``sweep_scheduled_to_pending``
    performs (a scheduled row whose ``scheduled_at`` has passed becomes
    ``pending``) without waiting out the denial backoff's wall-clock
    delay: the sweep's own behaviour is pinned in
    tests/test_sweep_scheduled_to_pending_batching.py and is not the seam
    under attack here.
    """
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'pending', scheduled_at = clock_timestamp() - interval '1 second' WHERE id = $1",  # noqa: S608  # Why: schema is a test-fixture identifier; the job id is $-bound
            job_id,
        )


async def _fetch_row(deps: WorkerDeps, schema: str, job_id: JobId) -> asyncpg.Record:
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, error_class, attempt, max_attempts, metadata, rate_limit_blocked_count FROM "{schema}".jobs WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the job id is $-bound
            job_id,
        )
    assert row is not None, "the job row vanished mid-outage — test wiring is wrong"
    return row


def _awaiting_annotation(row: asyncpg.Record) -> str | None:
    metadata: object = row["metadata"]
    if isinstance(metadata, str):
        from taskq._json import loads

        metadata = loads(metadata)
    if isinstance(metadata, dict):
        value: object = metadata.get("awaiting")
        return value if isinstance(value, str) else None
    return None


async def _claim_and_dispatch(
    backend: PostgresBackend,
    deps: WorkerDeps,
    scopes: _ScopeStack,
    actor_ref: Any,
    job_id: JobId,
) -> str:
    """One production dispatch round: real claim, real consume.

    The claim is the real ``dispatch_batch`` statement — the production
    attempt increment — and the consume is the real ``dispatch_one_job``
    composition, so whatever the acquire boundary does with the dead store
    is production behaviour, not a hand-built denial.
    """
    claimed = await backend.dispatch_batch(_WORKER_ID, ["default"], 1, _LOCK_LEASE)
    assert [row.id for row in claimed] == [job_id], (
        f"the claim must pick up exactly the job under test; got {[row.id for row in claimed]}"
    )
    return await dispatch_one_job(
        backend=backend,
        deps=deps,
        job=claimed[0],
        worker_id=_WORKER_ID,
        registry=scopes.registry,
        process_scope=scopes.process_scope,
        thread_scope=scopes.thread_scope,
        loop_scope=scopes.loop_scope,
        actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening seam as the guardrail harness in test_rt_depfail_ratelimit_acquire.py
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        clock=SystemClock(),
        enqueuer=SubJobEnqueuer(backend=backend, loop_scope_resolved=None, worker_pool=None),
    )


async def _drive_outage_cycles(
    backend: PostgresBackend,
    deps: WorkerDeps,
    scopes: _ScopeStack,
    actor_ref: Any,
    schema: str,
    job_id: JobId,
    *,
    cycles: int,
    expect_awaiting: str | None = "rate_limit:unavailable:ConnectionError",
) -> asyncpg.Record:
    """Drive *cycles* real claim→admission-denial rounds, asserting the
    429 contract at every boundary; returns the final row.

    *expect_awaiting* pins the awaiting annotation the denial must leave
    when the denial cause is meant to be distinguishable on the row;
    ``None`` skips that assertion for causes with no annotation contract.
    """
    row: asyncpg.Record | None = None
    for cycle in range(1, cycles + 1):
        if cycle > 1:
            await _requeue_due(deps, schema, job_id)
        outcome = await _claim_and_dispatch(backend, deps, scopes, actor_ref, job_id)
        row = await _fetch_row(deps, schema, job_id)
        assert outcome == "scheduled", (
            "429 DENIAL CONTRACT (an admission denial is 'come back later'): the "
            f"consumer's outcome for denial cycle {cycle} is {outcome!r} — the job "
            "is leaving the retryable set over a slot it never got. The actor "
            "never executed; the denial must reschedule (stay scheduled), never "
            "terminalise."
        )
        assert row["status"] == "scheduled", (
            "429 DENIAL CONTRACT (the job stays retryable under sustained "
            f"contention): after denial cycle {cycle} the row is "
            f"{row['status']!r} with error_class={row['error_class']!r}. The "
            "actor never ran, so no budget-consuming event occurred that could "
            "justify a terminal exit."
        )
        if expect_awaiting is not None:
            assert _awaiting_annotation(row) == expect_awaiting, (
                "429 DENIAL CONTRACT (denial causes stay distinguishable): the "
                f"awaiting annotation after cycle {cycle} is "
                f"{_awaiting_annotation(row)!r}, expected {expect_awaiting!r}. A "
                "store-outage denial must stay distinguishable from a saturation "
                "denial by naming the unavailability and its cause, so an "
                "operator does not answer an outage with more capacity."
            )
    assert row is not None, "at least one denial cycle must have run"
    return row


async def _count_rows(deps: WorkerDeps, schema: str, table: str, job_id: JobId) -> int:
    """Durable bookkeeping rows a denial loop left behind for this job."""
    async with deps.worker_pool.acquire() as conn:
        count: int | None = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".{table} WHERE job_id = $1',  # noqa: S608  # Why: schema and table are test-fixture identifiers; the job id is $-bound
            job_id,
        )
    return count or 0


async def _seed_outage_scenario(
    clean_jobs_app: JobsApp,
) -> tuple[PostgresBackend, WorkerDeps, str, JobId]:
    """A stock transient job (max_attempts=3, no schedule_to_close) plus the
    worker row the real attempt-history writes reference, with the PG
    fallback disabled so the dead Redis store raises the dependency-failure
    family straight out of the acquire."""
    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    deps.settings.rate_limit_pg_fallback_enabled = False
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, _WORKER_ID)
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={"value": 42},
            max_attempts=_MAX_ATTEMPTS,
            retry_kind="transient",
            scheduled_at=None,
        )
    )
    return backend, deps, schema, job_id


async def test_sustained_store_outage_does_not_terminally_fail_the_job(
    clean_jobs_app: JobsApp,
) -> None:
    """A stock transient job rides out a sustained limiter-store outage.

    An admission denial is "come back later", so none of the denials a
    store outage produces spend retry budget: even after max_attempts + 2
    denial cycles the job is still scheduled/retryable and carries no
    MaxAttemptsExceeded mislabel. An operator whose limiter store is down
    must find their work waiting when it comes back, not destroyed.
    """
    _ACTOR_RUNS[0] = 0
    backend, deps, schema, job_id = await _seed_outage_scenario(clean_jobs_app)

    bucket = TokenBucket(
        name="rt_outage_budget_bucket", capacity=5, refill_per_second=1.0, backend="redis"
    )
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)
    actor_ref = _rate_limited_actor_ref(bucket)

    dead_client = _dead_redis_client(redis.ConnectionError("redis unreachable (store outage)"))
    try:
        async with _ScopeStack(_di_registry_with_redis(rl_registry, dead_client)) as scopes:
            final = await _drive_outage_cycles(
                backend,
                deps,
                scopes,
                actor_ref,
                schema,
                job_id,
                cycles=_SUSTAINED_OUTAGE_CYCLES,
            )
    finally:
        await dead_client.aclose()

    assert final["error_class"] != "MaxAttemptsExceeded", (
        "429 DENIAL CONTRACT (no mislabel): the job terminally failed "
        f"as {final['error_class']!r} — retry budget exhausted — while its ONLY "
        "fault was the limiter's store being unreachable. The actor never "
        "executed; MaxAttemptsExceeded asserts it ran and failed "
        "max_attempts times, which is false."
    )
    assert final["max_attempts"] == _MAX_ATTEMPTS, (
        "429 DENIAL CONTRACT (the ceiling is immutable): the configured "
        f"max_attempts is {final['max_attempts']} after "
        f"{_SUSTAINED_OUTAGE_CYCLES} denials, not the configured "
        f"{_MAX_ATTEMPTS}. Denials must be excluded from the budget outright, "
        "not compensated for by inflating the operator's declared ceiling — an "
        "inflated ceiling silently buys the job extra real execution attempts "
        "the operator never asked for."
    )
    assert _ACTOR_RUNS[0] == 0, (
        "the actor must never run while its limiter's store cannot answer — "
        "every dispatch in the outage window was fail-closed admission, not "
        "an execution"
    )


async def test_outage_denials_preserve_budget_job_completes_when_store_returns(
    clean_jobs_app: JobsApp,
    module_redis_url: str,
) -> None:
    """The budget a denial never consumed is still there to spend.

    After a full window of denial cycles at the budget boundary, capacity
    returns (the same limiter, a live store) and the very next dispatch
    runs the actor for real and completes the job — impossible if the
    denials had spent the budget. This is what "come back later" has to
    mean: the job's real allowance of execution attempts is untouched by
    however long it waited for a slot.
    """
    _ACTOR_RUNS[0] = 0
    backend, deps, schema, job_id = await _seed_outage_scenario(clean_jobs_app)

    bucket = TokenBucket(
        name="rt_outage_heal_bucket", capacity=5, refill_per_second=1.0, backend="redis"
    )
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)
    actor_ref = _rate_limited_actor_ref(bucket)

    dead_client = _dead_redis_client(redis.ConnectionError("redis unreachable (store outage)"))
    live_client = redis_async.from_url(module_redis_url, decode_responses=False)
    try:
        async with _ScopeStack(_di_registry_with_redis(rl_registry, dead_client)) as scopes:
            await _drive_outage_cycles(
                backend,
                deps,
                scopes,
                actor_ref,
                schema,
                job_id,
                cycles=_MAX_ATTEMPTS,
            )

        await _requeue_due(deps, schema, job_id)
        async with _ScopeStack(_di_registry_with_redis(rl_registry, live_client)) as healed_scopes:
            outcome = await _claim_and_dispatch(backend, deps, healed_scopes, actor_ref, job_id)
    finally:
        await dead_client.aclose()
        await live_client.aclose()

    assert outcome == "succeeded", (
        "BUDGET-PRESERVED CONTRACT: with the store back, the first real "
        f"dispatch of the job came back {outcome!r} instead of succeeding — "
        "the outage's denials consumed budget the outage had no right to "
        "spend (the actor never executed during it)."
    )
    row = await _fetch_row(deps, schema, job_id)
    assert row["status"] == "succeeded", (
        f"the job's durable state after the healed dispatch is {row['status']!r}; "
        "a job that never executed during the outage must complete on its "
        "first real execution once the store answers."
    )
    assert _ACTOR_RUNS[0] == 1, (
        "the healed dispatch must run the actor exactly once — the outage "
        "window executed nothing, and this dispatch is the job's first real "
        "execution"
    )


def _saturated_bucket(name: str) -> TokenBucket:
    """A bucket that denies every acquire: one token, never refilled.

    The in-process memory backend keeps the exhausted state on the bucket
    object the actor ref holds, so every dispatch of the job under test
    meets a genuinely saturated limiter — ordinary contention, no store
    failure anywhere in the picture.
    """
    return TokenBucket(name=name, capacity=1, refill_per_second=0.0, backend="memory")


async def test_saturated_bucket_denials_never_exhaust_the_retry_budget(
    clean_jobs_app: JobsApp,
) -> None:
    """Ordinary contention must not kill work, exactly like a store outage.

    A saturated bucket is the commonest admission denial there is, and the
    least deserving of a terminal failure: the actor never ran, so nothing
    about the job failed. Sustained contention past the retry ceiling
    leaves the job scheduled and retryable with its declared max_attempts
    intact — a bucket an operator sized too small is a capacity problem to
    be seen in telemetry and fixed, never a reason to destroy jobs that
    were merely waiting their turn. Only the schedule-to-close deadline
    bounds a denied job, and it fails it as DeadlineExceeded, not as
    MaxAttemptsExceeded.
    """
    _ACTOR_RUNS[0] = 0
    backend, deps, schema, job_id = await _seed_outage_scenario(clean_jobs_app)

    bucket = _saturated_bucket("rt_saturation_budget_bucket")
    # Spend the single token so the job's own dispatches all meet a full
    # bucket: the denial under test is saturation, not a cold-start race.
    first = await bucket.acquire(1.0, clock=SystemClock())
    assert first.allowed, "fixture wiring: the bucket's first token must be available"

    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)
    actor_ref = _rate_limited_actor_ref(bucket)

    di_registry = ProviderRegistry()
    from taskq.ratelimit._provider import register_rate_limit_registry

    register_rate_limit_registry(di_registry, rl_registry)

    async with _ScopeStack(di_registry) as scopes:
        final = await _drive_outage_cycles(
            backend,
            deps,
            scopes,
            actor_ref,
            schema,
            job_id,
            cycles=_SUSTAINED_OUTAGE_CYCLES,
            expect_awaiting=None,
        )

    assert final["error_class"] != "MaxAttemptsExceeded", (
        "429 DENIAL CONTRACT (contention is not failure): the job terminally "
        f"failed as {final['error_class']!r} after {_SUSTAINED_OUTAGE_CYCLES} "
        "saturation denials. Its actor never executed once — a full bucket says "
        "'come back later', and a rate-limit misconfiguration must not be able "
        "to kill work with a retry count of three."
    )
    assert final["attempt"] < _MAX_ATTEMPTS, (
        "429 DENIAL CONTRACT (denials are non-consuming): attempt reached "
        f"{final['attempt']} of {_MAX_ATTEMPTS} through denials alone. Each "
        "denial must refund the claim's attempt increment, so a job that never "
        "ran keeps its whole allowance of real execution attempts."
    )
    assert final["max_attempts"] == _MAX_ATTEMPTS, (
        "429 DENIAL CONTRACT (the ceiling is immutable): max_attempts is "
        f"{final['max_attempts']}, not the declared {_MAX_ATTEMPTS}. Denials are "
        "excluded from the budget outright; inflating the operator's declared "
        "ceiling to stay ahead of them silently grants extra real execution "
        "attempts nobody asked for."
    )
    assert _ACTOR_RUNS[0] == 0, (
        "the actor must never run while its bucket is empty — every dispatch in "
        "the contention window was admission control, not an execution"
    )


async def test_saturation_denials_are_visible_as_a_counter_not_as_rows(
    clean_jobs_app: JobsApp,
) -> None:
    """Contention stays observable without unbounded bookkeeping growth.

    Because a denial never fails a job, telemetry is the only way an
    operator sees a bottleneck — so the signal has to exist. It is an
    aggregated counter on the job row (``rate_limit_blocked_count``),
    which rises once per denial and is O(1) in storage. The per-denial
    ``job_events`` and ``job_attempts`` rows are deliberately absent: a
    denial records that nothing happened, and a job that may be denied
    indefinitely until its deadline would otherwise accrue bookkeeping
    without bound.
    """
    _ACTOR_RUNS[0] = 0
    backend, deps, schema, job_id = await _seed_outage_scenario(clean_jobs_app)

    attempts_before = await _count_rows(deps, schema, "job_attempts", job_id)
    events_before = await _count_rows(deps, schema, "job_events", job_id)

    bucket = _saturated_bucket("rt_saturation_counter_bucket")
    first = await bucket.acquire(1.0, clock=SystemClock())
    assert first.allowed, "fixture wiring: the bucket's first token must be available"

    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)
    actor_ref = _rate_limited_actor_ref(bucket)

    di_registry = ProviderRegistry()
    from taskq.ratelimit._provider import register_rate_limit_registry

    register_rate_limit_registry(di_registry, rl_registry)

    async with _ScopeStack(di_registry) as scopes:
        final = await _drive_outage_cycles(
            backend,
            deps,
            scopes,
            actor_ref,
            schema,
            job_id,
            cycles=_SUSTAINED_OUTAGE_CYCLES,
            expect_awaiting=None,
        )

    assert final["rate_limit_blocked_count"] == _SUSTAINED_OUTAGE_CYCLES, (
        "429 DENIAL OBSERVABILITY: the aggregated denial counter on the job row "
        f"reads {final['rate_limit_blocked_count']} after "
        f"{_SUSTAINED_OUTAGE_CYCLES} denials. With denials no longer failing "
        "jobs and no longer writing per-denial rows, this counter is the only "
        "way an operator can see that a job is starving for capacity — it must "
        "count every denial."
    )
    attempts_added = await _count_rows(deps, schema, "job_attempts", job_id) - attempts_before
    events_added = await _count_rows(deps, schema, "job_events", job_id) - events_before
    assert attempts_added == 0, (
        f"{_SUSTAINED_OUTAGE_CYCLES} denials wrote {attempts_added} job_attempts "
        "rows. A denial is not an attempt — no handler ran — and a job denied "
        "until its deadline would grow this table without bound."
    )
    assert events_added == 0, (
        f"{_SUSTAINED_OUTAGE_CYCLES} denials wrote {events_added} job_events "
        "rows. Denial contention belongs on the aggregated counter, not as one "
        "durable row per poll."
    )

"""RED-team: a rate-limiter STORE outage must not terminally fail jobs.

The contract under attack (issue #139's ship-blocker edge): a rate-limited
job whose limiter store cannot answer — Redis unreachable, the PG fallback
disabled — is failed closed by the consumer's acquire boundary, which
synthesises the limiter's own denial (``ReservationUnavailable(bucket_name=
"unavailable:<error_type>", source="rate_limit")``) and snoozes the job
through ``mark_snoozed``. That denial is infrastructure backpressure about
a job whose actor never executed, so it is non-consuming: across a
sustained outage the job stays scheduled/retryable, and when the store
returns it still completes within its original retry budget. A job whose
only fault is its limiter's store being down never lands in the terminal
``MaxAttemptsExceeded`` exit — that label asserts the actor ran and failed
``max_attempts`` times, and an operator reading it must be able to trust
that.

Distinguishability is the operator's stated contract: a SATURATION denial
(a full bucket) legitimately consumes budget and terminalises at the
boundary — pinned deliberately in tests/test_denial_and_retention_bounds.py
and tests/test_denial_observability.py — while the store-outage denial
carries the ``rate_limit:unavailable:<error_type>`` awaiting annotation
and must not burn budget. The seam driven is the consumer level: a real
``dispatch_batch`` claim (the production attempt increment), a real
``dispatch_one_job`` → ``consume_one_job`` acquire boundary → the
production denial synthesis → ``_run_terminal_path`` →
``_handle_reservation_class_denied`` → the real Postgres ``mark_snoozed``
arms. The burn is a property of the claim and the denial arm composing,
and no narrower seam composes them; the pre-existing pins of this
dependency-failure family (tests/test_rt_depfail_ratelimit_acquire.py,
tests/test_rt_depfail_ratelimit_acquire_pg_fallback_death.py) drive one
cycle against a recording stub backend, so the budget arms never run.
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
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
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
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.dispatch import dispatch_one_job

pytestmark = pytest.mark.integration

_WORKER_ID = new_uuid()

_MAX_ATTEMPTS = 3
"""The stock transient budget from the claim: attempt reaches the ceiling
on the third claim→denial cycle when denials do not refund."""

_SUSTAINED_OUTAGE_CYCLES = _MAX_ATTEMPTS + 2
"""Cycle count past the budget boundary: the contract is that a SUSTAINED
outage keeps the job retryable, not merely one that ends before the
boundary is reached."""

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


def _make_scopes(
    registry: ProviderRegistry,
) -> tuple[ProcessScope, ThreadScope, LoopScope]:
    scope_containers: dict[Scope, Any] = {}

    def _resolver(func: object) -> Any:
        async def _resolve() -> dict[str, object]:
            from taskq._di.solver import solve_dependencies

            return await solve_dependencies(
                func=func,
                registry=registry,
                scope_containers=scope_containers,
            )

        return _resolve()

    process_scope = ProcessScope(resolver=_resolver)
    thread_scope = ThreadScope(resolver=_resolver)
    loop_scope = LoopScope(resolver=_resolver)
    scope_containers = {
        Scope.PROCESS: process_scope,
        Scope.THREAD: thread_scope,
        Scope.LOOP: loop_scope,
    }
    return process_scope, thread_scope, loop_scope


async def _bootstrap_scopes(
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
) -> None:
    settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        },
    )
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> "_ScopeStack":
        self.registry.validate()
        scopes = _make_scopes(self.registry)
        self.process_scope, self.thread_scope, self.loop_scope = scopes
        await _bootstrap_scopes(self.registry, *scopes)
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
) -> asyncpg.Record:
    """Drive *cycles* real claim→store-outage-denial rounds, asserting the
    non-consuming contract at every boundary; returns the final row."""
    row: asyncpg.Record | None = None
    for cycle in range(1, cycles + 1):
        if cycle > 1:
            await _requeue_due(deps, schema, job_id)
        outcome = await _claim_and_dispatch(backend, deps, scopes, actor_ref, job_id)
        row = await _fetch_row(deps, schema, job_id)
        assert outcome == "scheduled", (
            "STORE-OUTAGE DENIAL CONTRACT (non-consuming infra backpressure): the "
            f"consumer's outcome for denial cycle {cycle} of a store outage is "
            f"{outcome!r} — the job is leaving the retryable set for an outage it "
            "cannot control. A store-unreachable denial is about a job that never "
            "executed; it must snooze (stay scheduled), never terminalise."
        )
        assert row["status"] == "scheduled", (
            "STORE-OUTAGE DENIAL CONTRACT (the job stays retryable across a "
            f"sustained outage): after denial cycle {cycle} the row is "
            f"{row['status']!r} with error_class={row['error_class']!r}. The "
            "actor never ran — the limiter's store was down — so no "
            "budget-consuming event occurred that could justify a terminal exit."
        )
        assert _awaiting_annotation(row) == "rate_limit:unavailable:ConnectionError", (
            "STORE-OUTAGE DENIAL CONTRACT (distinguishable degradation): the "
            f"awaiting annotation after cycle {cycle} is "
            f"{_awaiting_annotation(row)!r}; the store-outage denial must stay "
            "distinguishable from a saturation denial by naming the "
            "unavailability and its cause."
        )
    assert row is not None, "at least one denial cycle must have run"
    return row


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

    Verdict asserted: NON-CONSUMING. Every dispatch during the outage is
    denied by the synthesized store-failure denial; the contract is that
    none of those denials spend retry budget, so even after
    max_attempts + 2 denial cycles the job is still scheduled/retryable
    and carries no MaxAttemptsExceeded mislabel.
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
        "STORE-OUTAGE DENIAL CONTRACT (no mislabel): the job terminally failed "
        f"as {final['error_class']!r} — retry budget exhausted — while its ONLY "
        "fault was the limiter's store being unreachable. The actor never "
        "executed; MaxAttemptsExceeded asserts it ran and failed "
        "max_attempts times, which is false."
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
    """The budget an outage never consumed is still there to spend.

    Verdict asserted: BUDGET PRESERVED. After a full outage window of
    denial cycles at the budget boundary, the store returns (the same
    limiter, a live store) and the very next dispatch runs the actor for
    real and completes the job — impossible if the outage denials had
    spent the budget.
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

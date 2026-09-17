"""RED-team pin: the PG fallback's own death during rate-limit acquire.

The Redis-outage instances of the dependency-failure contract are pinned
in tests/test_rt_depfail_ratelimit_acquire.py: a limiter whose store
cannot answer must fail CLOSED — the job snoozes, the actor never runs,
the job's retry budget and error_class are untouched. The PG fallback is
the same class's second store: with ``rate_limit_pg_fallback_enabled``
on (the default) and a Redis outage funneling admission into Postgres, a
PG connection death mid-fallback must take the same fail-closed path —
not escape into ``dispatch_one_job``'s generic handler, which would burn
the job's retry budget for an infrastructure outage and persist the
Postgres error as the job's own ``error_class``.

The degraded outcome is distinguishable from an ordinary saturation
denial: the job's ``awaiting`` annotation names the limiter's
unavailability and its cause (``rate_limit:unavailable:<error_type>``),
and the fallback attempt itself stays visible through the
``rate-limit-redis-fallback`` warning.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg
import redis
import redis.asyncio as redis_async
import structlog.testing
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker.dispatch import dispatch_one_job
from tests._di_scopes import bootstrap_scopes, make_scopes

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

_ACTOR_RUNS = [0]


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _FakeWorkerDeps:
    def __init__(self) -> None:
        self.active_jobs = None
        self.worker_pool: Any = None
        self.slot_pool: Any = None
        self.settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        self.settings.worker_group = "default"
        self.redis_client: Any = None
        self.progress_buffers: dict[Any, Any] = {}
        self.disowned_jobs: set[UUID] = set()


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


class _DeadPgAcquireCtx:
    """Pool-acquire context whose connection handover fails like a dead PG."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def __aenter__(self) -> object:
        raise self._error

    async def __aexit__(self, *exc: object) -> None:
        return None


class _DeadPgPool:
    """Duck-typed asyncpg.Pool: every acquire raises the injected error —
    the PG fallback store dying mid-outage, one store failure deep."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    def acquire(self) -> _DeadPgAcquireCtx:
        return _DeadPgAcquireCtx(self._error)


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1


async def _dispatch_with_both_stores_dead(
    *,
    redis_error: Exception,
    pg_pool: Any,
) -> FakeBackend:
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    bucket = TokenBucket(
        name="rt_dep_pg_bucket", capacity=5, refill_per_second=1.0, backend="redis"
    )
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    client = _dead_redis_client(redis_error)
    di_registry.register_value(redis_async.Redis, Scope.LOOP, client)

    actor_ref = ActorRef(
        name="test_actor",
        queue="default",
        fn=_noop_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_Payload,
        result_adapter=None,  # type: ignore[arg-type]  # Why: test-only; result_adapter not used in dispatch_one_job
        retry=RetryPolicy(),
        result_ttl=None,
        rate_limits=[bucket],
    )

    fake_backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()
    fake_deps.settings.rate_limit_pg_fallback_enabled = True
    fake_deps.worker_pool = pg_pool
    clock = FakeClock(_NOW)

    try:
        async with _ScopeStack(di_registry) as scopes:
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=fake_deps,  # type: ignore[arg-type]  # Why: same Any-cast seam as test_rt_depfail_ratelimit_acquire's harness
                job=make_job_row(payload={"value": 42}),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as the deliverable's harness
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=clock,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )
    finally:
        await client.aclose()
    return fake_backend


async def test_pg_fallback_death_acquire_does_not_burn_retry_budget() -> None:
    """A PG connection death inside the fallback acquire (Redis already
    dead, fallback enabled) is the limiter's dependency failing, not the
    job's: fail closed with a snooze whose ``awaiting`` annotation names
    the unavailability, and leave the job's failure accounting untouched.
    """
    _ACTOR_RUNS[0] = 0
    pg_error = asyncpg.PostgresConnectionError("pg dead mid-fallback")
    with structlog.testing.capture_logs() as captured:
        fake_backend = await _dispatch_with_both_stores_dead(
            redis_error=redis.ConnectionError("redis unreachable"),
            pg_pool=_DeadPgPool(pg_error),
        )

    assert _ACTOR_RUNS[0] == 0, "the actor must never run when neither store can answer"

    assert fake_backend.mark_failed_or_retry_calls == [], (
        "DEPENDENCY-FAILURE contract (fail-closed, no misattribution): the PG "
        "fallback's death during acquire must NOT be written as the job's own "
        "failure — mark_failed_or_retry burns a retry attempt and persists "
        "error_class=PostgresConnectionError on the job row for an "
        "infrastructure outage."
    )
    job_exception_logs = [e for e in captured if e.get("event") == "job_exception"]
    assert job_exception_logs == [], (
        "DEPENDENCY-FAILURE contract (distinguishable degradation): a PG death "
        "during the fallback acquire must not be logged as job_exception "
        "blaming the actor."
    )

    snoozes = fake_backend.mark_snoozed_calls
    assert len(snoozes) == 1, (
        "the fail-closed outcome is the limiter's denial channel: exactly one "
        f"snooze write; got {len(snoozes)}"
    )
    assert snoozes[0]["outcome"] == "rate_limit_denied"
    assert snoozes[0]["denial_reason"] == "unavailable", (
        "the store-failure denial must route to the NON-consuming snooze arm "
        "(denial_reason='unavailable'): the PG fallback's death is "
        "infrastructure backpressure about a job whose actor never ran, so the "
        "claim's attempt increment is refunded and no terminal arm can fire — "
        "a 'capacity' reason would burn the job's retry budget for the outage."
    )
    assert snoozes[0]["metadata_update"] == {
        "awaiting": "rate_limit:unavailable:PostgresConnectionError"
    }, (
        "the degraded denial must be distinguishable from saturation: the "
        "awaiting annotation names the limiter's unavailability and its cause"
    )

    fallback_warnings = [e for e in captured if e.get("event") == "rate-limit-redis-fallback"]
    assert len(fallback_warnings) == 1, (
        "the fallback attempt itself stays visible — the operator sees the "
        "Redis outage AND the failed degradation through the existing "
        "rate-limit-redis-fallback warning"
    )


async def test_no_pool_fallback_acquire_does_not_burn_retry_budget() -> None:
    """The fallback entered with no pool injected at all — the limiter's
    PG store never wired — is the same dependency-unavailable class as a
    pool death mid-fallback, not a job failure: the no-pool branch raises
    the typed ``RateLimitDependencyUnavailable``, the consumer's
    dependency-failure family recognises it, and the dispatch fails
    closed with the distinguishable snooze while the job's failure
    accounting stays untouched.
    """
    _ACTOR_RUNS[0] = 0
    with structlog.testing.capture_logs() as captured:
        fake_backend = await _dispatch_with_both_stores_dead(
            redis_error=redis.ConnectionError("redis unreachable"),
            pg_pool=None,
        )

    assert _ACTOR_RUNS[0] == 0, "the actor must never run when neither store can answer"

    assert fake_backend.mark_failed_or_retry_calls == [], (
        "DEPENDENCY-FAILURE contract (fail-closed, no misattribution): an "
        "unwired PG fallback store must NOT be written as the job's own "
        "failure — mark_failed_or_retry burns a retry attempt and persists "
        "a store-wiring error as the job's error_class."
    )
    job_exception_logs = [e for e in captured if e.get("event") == "job_exception"]
    assert job_exception_logs == [], (
        "DEPENDENCY-FAILURE contract (distinguishable degradation): a "
        "no-pool fallback must not be logged as job_exception blaming the "
        "actor."
    )

    snoozes = fake_backend.mark_snoozed_calls
    assert len(snoozes) == 1, (
        "the fail-closed outcome is the limiter's denial channel: exactly one "
        f"snooze write; got {len(snoozes)}"
    )
    assert snoozes[0]["outcome"] == "rate_limit_denied"
    assert snoozes[0]["denial_reason"] == "unavailable", (
        "the store-failure denial must route to the NON-consuming snooze arm "
        "(denial_reason='unavailable'): an unwired fallback store is "
        "infrastructure backpressure about a job whose actor never ran, so the "
        "claim's attempt increment is refunded and no terminal arm can fire — "
        "a 'capacity' reason would burn the job's retry budget for the outage."
    )
    assert snoozes[0]["metadata_update"] == {
        "awaiting": "rate_limit:unavailable:RateLimitDependencyUnavailable"
    }, (
        "the degraded denial must be distinguishable from saturation: the "
        "awaiting annotation names the limiter's unavailability and its "
        "cause — the typed no-pool error, not a bare RuntimeError the "
        "family cannot recognise"
    )

    fallback_warnings = [e for e in captured if e.get("event") == "rate-limit-redis-fallback"]
    assert len(fallback_warnings) == 1, (
        "the fallback attempt itself stays visible through the existing "
        "rate-limit-redis-fallback warning"
    )

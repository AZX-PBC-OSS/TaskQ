"""RED-team: dependency-failure posture of the Redis rate-limit acquire path.

Constitution: fail closed where a check cannot complete; a degraded outcome
must be distinguishable, never a silent wrong answer. An infrastructure
outage (Redis unreachable) during rate-limit acquisition is NOT a job
outcome — burning the job's retry budget and persisting the Redis error as
the job's ``error_message`` is the misattribution class.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
import redis
import redis.asyncio as redis_async
import structlog.testing
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope
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


class _FakeTx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePgConn:
    """Duck-typed asyncpg connection satisfying TokenBucket._acquire_pg's
    surface: execute / fetchrow / nested transaction context managers."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object]:
        return {"state": {"tokens": 5.0, "ts": 0.0}, "now_s": 100.0}

    def transaction(self) -> "_FakeTx":
        return _FakeTx()


class _FakePoolAcquireCtx:
    def __init__(self, conn: _FakePgConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakePgConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePgPool:
    """Duck-typed asyncpg.Pool: yields a fresh recording connection."""

    def __init__(self) -> None:
        self.conns: list[_FakePgConn] = []

    def acquire(self) -> _FakePoolAcquireCtx:
        conn = _FakePgConn()
        self.conns.append(conn)
        return _FakePoolAcquireCtx(conn)


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1


async def _dispatch_with_dead_redis(
    *,
    error: Exception,
    fallback_enabled: bool,
    pg_pool: Any = None,
) -> tuple[FakeBackend, int]:
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    bucket = TokenBucket(name="rt_dep_bucket", capacity=5, refill_per_second=1.0, backend="redis")
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    client = _dead_redis_client(error)
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
    fake_deps.settings.rate_limit_pg_fallback_enabled = fallback_enabled
    fake_deps.worker_pool = pg_pool
    clock = FakeClock(_NOW)

    try:
        async with _ScopeStack(di_registry) as scopes:
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=fake_deps,  # type: ignore[arg-type]  # Why: same Any-cast seam as test_dispatch_one_job._as_deps
                job=make_job_row(payload={"value": 42}),
                worker_id=_WORKER_ID,
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as test_dispatch_one_job
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=clock,
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )
    finally:
        await client.aclose()
    return fake_backend, _ACTOR_RUNS[0]


@pytest.mark.parametrize(
    ("error", "fallback_enabled"),
    [
        pytest.param(
            redis.ConnectionError("redis unreachable"), False, id="conn-error-no-fallback"
        ),
        pytest.param(redis.ResponseError("redis degraded"), True, id="response-error-fallback-on"),
    ],
)
async def test_redis_outage_acquire_does_not_burn_retry_budget(
    error: Exception, fallback_enabled: bool
) -> None:
    """A Redis outage during rate-limit acquire must be fail-closed and
    NON-misattributed: the job must not land in the failure-accounting path
    (``mark_failed_or_retry`` burns an attempt and persists the Redis error
    as the job's own ``error_class``), and the actor must not run.

    Verdict asserted: FAIL-CLOSED. Current posture is fail-open-RED
    (misattribution): the outage exception escapes ``consume_one_job``'s
    ReservationUnavailable-only catch, lands in ``dispatch_one_job``'s
    generic handler, and burns the job's retry budget for an infra outage.
    """
    _ACTOR_RUNS[0] = 0
    with structlog.testing.capture_logs() as captured:
        fake_backend, actor_runs = await _dispatch_with_dead_redis(
            error=error, fallback_enabled=fallback_enabled
        )

    assert actor_runs == 0, "the actor must never run when its limiter cannot answer"

    assert fake_backend.mark_failed_or_retry_calls == [], (
        "DEPENDENCY-FAILURE contract (fail-closed, no misattribution): a Redis outage "
        "during rate-limit acquire must NOT be written as the job's own failure — "
        "mark_failed_or_retry burns a retry attempt and persists error_class="
        f"{type(error).__name__} on the job row for an infrastructure outage. "
        "Verdict: FAIL-OPEN-RED — the limiter's dependency failure is misattributed "
        "to the job and consumes its retry budget."
    )
    job_exception_logs = [e for e in captured if e.get("event") == "job_exception"]
    assert job_exception_logs == [], (
        "DEPENDENCY-FAILURE contract (distinguishable degradation): a Redis outage "
        "during acquire must not be logged as job_exception blaming the actor. "
        "Verdict: FAIL-OPEN-RED — the actor is blamed for its limiter's outage."
    )


async def test_redis_outage_fallback_composition_runs_actor_via_pg() -> None:
    """With the PG fallback enabled (default) and a live PG pool, a dead
    Redis during acquire must degrade-and-report CLEANLY through the full
    dispatch composition: the acquire falls back to Postgres, the actor
    runs, and no failure accounting happens.

    Verdict asserted: DEGRADE-AND-REPORT (fail-closed fallback, no retry
    budget burn, distinguishable via the fallback warning).
    """
    _ACTOR_RUNS[0] = 0
    pool = _FakePgPool()
    with structlog.testing.capture_logs() as captured:
        fake_backend, actor_runs = await _dispatch_with_dead_redis(
            error=redis.ConnectionError("redis unreachable"), fallback_enabled=True, pg_pool=pool
        )

    assert actor_runs == 1, (
        "the PG fallback must be entered cleanly: the actor runs on the "
        "fallback store's admission, exactly as it would on Redis's"
    )
    assert len(fake_backend.mark_succeeded_calls) == 1
    assert fake_backend.mark_failed_or_retry_calls == [], (
        "a cleanly-entered fallback must not touch the job's failure accounting"
    )
    fallback_warnings = [e for e in captured if e.get("event") == "rate-limit-redis-fallback"]
    assert len(fallback_warnings) == 1, (
        "DEGRADE-AND-REPORT contract: the degraded acquire must be distinguishable — "
        "exactly one rate-limit-redis-fallback warning naming the redis backend and "
        f"the postgres fallback; got {[e.get('event') for e in captured]}"
    )
    assert fallback_warnings[0].get("backend") == "redis"
    assert fallback_warnings[0].get("fallback") == "postgres"
    assert pool.conns, "the fallback must actually have gone to Postgres"
    assert any("rate_limit_buckets" in sql for sql in pool.conns[0].executed), (
        "the fallback acquire must have run the token-bucket PG statements"
    )

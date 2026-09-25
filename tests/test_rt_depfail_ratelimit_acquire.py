"""RED-team: dependency-failure posture of the Redis rate-limit acquire path.

Constitution: fail closed where a check cannot complete; a degraded outcome
must be distinguishable, never a silent wrong answer. An infrastructure
outage (Redis unreachable) during rate-limit acquisition is NOT a job
outcome - burning the job's retry budget and persisting the Redis error as
the job's ``error_message`` is the misattribution class.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
import redis
import redis.asyncio as redis_async
import structlog.testing
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.actor import ActorRef
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS
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
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        raise self._error


def _dead_redis_client(error: Exception) -> tuple[redis_async.Redis, _RaisingScript]:
    """A REAL ``redis.asyncio.Redis`` instance (dispatch resolves the client
    via ``isinstance(raw_redis, Redis)``) whose Lua script call fails with
    *error* - no container, no socket: the command surface is duck-typed.
    The registered double is handed back so tests can count invocations."""
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    script = _RaisingScript(error)
    client.register_script = lambda script_arg: script  # type: ignore[method-assign]  # Why: injecting the failure at the script-call seam redis-py would use; no connection exists
    return client, script


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
        self.fetched: list[str] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object]:
        self.fetched.append(sql)
        # The fused acquire's RETURNING row: the final token count
        # and the decision bit. 5.0 capacity, 1.0 spent -> 4.0 remaining,
        # granted.
        return {"tokens_after": 4.0, "granted": True}

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
) -> tuple[FakeBackend, int, _RaisingScript]:
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    bucket = TokenBucket(name="rt_dep_bucket", capacity=5, refill_per_second=1.0, backend="redis")
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    client, script = _dead_redis_client(error)
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
    return fake_backend, _ACTOR_RUNS[0], script


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
        fake_backend, actor_runs, _script = await _dispatch_with_dead_redis(
            error=error, fallback_enabled=fallback_enabled
        )

    assert actor_runs == 0, "the actor must never run when its limiter cannot answer"

    assert fake_backend.mark_failed_or_retry_calls == [], (
        "DEPENDENCY-FAILURE contract (fail-closed, no misattribution): a Redis outage "
        "during rate-limit acquire must NOT be written as the job's own failure - "
        "mark_failed_or_retry burns a retry attempt and persists error_class="
        f"{type(error).__name__} on the job row for an infrastructure outage. "
        "Verdict: FAIL-OPEN-RED - the limiter's dependency failure is misattributed "
        "to the job and consumes its retry budget."
    )
    job_exception_logs = [e for e in captured if e.get("event") == "job_exception"]
    assert job_exception_logs == [], (
        "DEPENDENCY-FAILURE contract (distinguishable degradation): a Redis outage "
        "during acquire must not be logged as job_exception blaming the actor. "
        "Verdict: FAIL-OPEN-RED - the actor is blamed for its limiter's outage."
    )

    snoozes = fake_backend.mark_snoozed_calls
    assert len(snoozes) == 1, (
        "the fail-closed outcome is the limiter's denial channel: exactly one "
        f"snooze write; got {len(snoozes)}"
    )
    assert snoozes[0]["outcome"] == "rate_limit_denied"
    assert snoozes[0]["denial_reason"] == "unavailable", (
        "DEPENDENCY-FAILURE contract (non-consuming denial): the store-outage "
        "denial must route to the NON-consuming snooze arm - denial_reason="
        "'unavailable' is what makes mark_snoozed refund the claim's attempt "
        "increment and never take a terminal arm, so an outage the job cannot "
        "control spends none of its retry budget. A 'capacity' reason here "
        "means the outage denial is being accounted as saturation backpressure "
        "and the budget burns."
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
        fake_backend, actor_runs, script = await _dispatch_with_dead_redis(
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
        "DEGRADE-AND-REPORT contract: the degraded acquire must be distinguishable - "
        "exactly one rate-limit-redis-fallback warning naming the redis backend and "
        f"the postgres fallback; got {[e.get('event') for e in captured]}"
    )
    assert fallback_warnings[0].get("backend") == "redis"
    assert fallback_warnings[0].get("fallback") == "postgres"
    assert pool.conns, "the fallback must actually have gone to Postgres"
    assert any(
        "rate_limit_buckets" in sql for sql in pool.conns[0].executed + pool.conns[0].fetched
    ), (
        "the fallback acquire must have run the token-bucket PG statements, "
        "the fused acquire is one fetchrow"
    )
    assert script.calls == RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS, (
        f"the Redis acquire ran {script.calls} times on a persistent outage: "
        "the connection family gets EXACTLY the bounded transient-retry budget "
        "(RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS attempts, never an unbounded "
        "replay), and only AFTER the budget is spent does the fallback run as "
        "an INDEPENDENT Postgres decision - the fallback must never be a replay "
        "of the Redis acquire, a replay would spend the bucket twice for one "
        "admission (and a half-applied script state would be spent a third time)"
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            redis.ReadOnlyError("READONLY You can't write against a read only replica."),
            id="readonly-error-replica-promotion",
        ),
        pytest.param(
            redis.OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'."),
            id="oom-error-maxmemory-breach",
        ),
    ],
)
async def test_redis_store_rejection_fallback_composition_runs_actor_via_pg(
    error: Exception,
) -> None:
    """Redis ``ResponseError`` siblings that mean "this server cannot serve
    right now" must degrade through the PG fallback exactly like a connection
    failure: a replica promoted mid-flight answers writes with
    ``ReadOnlyError``, and a maxmemory breach answers commands with
    ``OutOfMemoryError``. Both escape a catch that names only
    ``ConnectionError``/``TimeoutError`` - the worker then snoozes in a ~5s
    loop for the whole promotion/maxmemory window the PG fallback was
    designed to absorb. ``NoScriptError`` (the other ``ResponseError``
    sibling) stays excluded on purpose: redis-py 8.x handles it client-side
    in ``Script.__call__`` (re-EVAL after re-SCRIPT LOAD), so it never
    signals a store outage.

    Verdict asserted: DEGRADE-AND-REPORT, identical to the
    ``ConnectionError`` composition test above.
    """
    _ACTOR_RUNS[0] = 0
    pool = _FakePgPool()
    with structlog.testing.capture_logs() as captured:
        fake_backend, actor_runs, script = await _dispatch_with_dead_redis(
            error=error, fallback_enabled=True, pg_pool=pool
        )

    assert actor_runs == 1, (
        f"a {type(error).__name__} from the store substrate must enter the "
        "PG fallback cleanly: the actor runs on the fallback store's "
        "admission, exactly as it does for ConnectionError"
    )
    assert fake_backend.mark_failed_or_retry_calls == [], (
        "a cleanly-entered fallback must not touch the job's failure accounting"
    )
    fallback_warnings = [e for e in captured if e.get("event") == "rate-limit-redis-fallback"]
    assert len(fallback_warnings) == 1, (
        "the degraded acquire must be distinguishable - exactly one "
        f"rate-limit-redis-fallback warning, same as ConnectionError; "
        f"got {[e.get('event') for e in captured]}"
    )
    assert fallback_warnings[0].get("backend") == "redis"
    assert fallback_warnings[0].get("fallback") == "postgres"
    assert pool.conns, "the fallback must actually have gone to Postgres"
    assert any(
        "rate_limit_buckets" in sql for sql in pool.conns[0].executed + pool.conns[0].fetched
    ), (
        "the fallback acquire must have run the token-bucket PG statements, "
        "the fused acquire is one fetchrow"
    )
    assert script.calls == 1, (
        f"the Redis acquire ran {script.calls} times on a "
        f"{type(error).__name__}: the fallback must be an INDEPENDENT "
        "Postgres decision, never a replay of the Redis acquire - a replay "
        "would spend the bucket twice for one admission (and a half-applied "
        "script state would be spent a third time)"
    )

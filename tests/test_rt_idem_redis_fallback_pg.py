# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""RED-TEAM pins: the redis fallback is one admission, never a replay (S6).

#421's widening: the rate-limit acquire's PG fallback absorbs the
``ResponseError`` siblings that mean "this store cannot serve right now"
(``ReadOnlyError``, a replica promoted mid-flight; ``OutOfMemoryError``, a
maxmemory breach) beside the ConnectionError/TimeoutError pair. The unit
pins (``tests/test_rt_depfail_ratelimit_acquire.py``) hold the composition
against fakes; these pins hold it against REAL Postgres, the same
idempotency question the fallback's own review stated: the fallback must be
an INDEPENDENT Postgres decision, never a replay of the redis acquire - a
replay would spend the bucket twice for one admission, and a half-applied
script state a third time.

Pinned, per rejection class, through the full dispatch composition with a
dead-redis double (a real ``redis.asyncio.Redis`` whose script seam raises)
and the real worker pool:

* the actor runs exactly once per job (the fallback admission is clean),
* exactly ONE redis acquire per dispatch (never a replay),
* the job terminalizes ``succeeded`` exactly once, no failure accounting,
* the bucket ledger lives in the REAL ``rate_limit_buckets`` table and
  reflects exactly the admissions (two dispatches spend two tokens).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis
import redis.asyncio as redis_async
from pydantic import BaseModel, ConfigDict

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_base62, new_uuid
from taskq.actor import ActorRef
from taskq.backend.clock import SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import _open_pg_backend
from taskq.testing.jobs import make_enqueue_args
from taskq.worker.dispatch import dispatch_one_job
from tests._di_scopes import bootstrap_scopes, make_scopes

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_BUCKET = "rt_idem_fallback_bucket"
_ACTOR_RUNS = [0]


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


class _RaisingScript:
    """AsyncScript double: every invocation raises the injected error."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        raise self._error


def _dead_redis_client(error: Exception) -> tuple[redis_async.Redis, _RaisingScript]:
    """A REAL ``redis.asyncio.Redis`` whose script seam raises *error*: the
    command surface is duck-typed, no socket exists."""
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    script = _RaisingScript(error)
    client.register_script = lambda script_arg: script  # type: ignore[method-assign]  # Why: injecting the failure at the script-call seam redis-py would use; no connection exists
    return client, script


async def _noop_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    _ACTOR_RUNS[0] += 1


async def _dispatch_with_dead_redis(
    pg_dsn: str,
    *,
    error: Exception,
    count: int,
) -> tuple[Any, Any, Any, _RaisingScript, str]:
    """Dispatch *count* jobs through the full composition: real backend, real
    pool, real ``rate_limit_buckets`` writes; the redis acquire always fails
    with *error* and the PG fallback must absorb it.

    Returns ``(stack, deps, client, script, schema)``: the caller asserts against the
    live schema and then closes the stack and drops the schema.
    """
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket

    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    worker_id = new_uuid()
    bucket = TokenBucket(name=_BUCKET, capacity=5, refill_per_second=1.0, backend="redis")
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

    try:
        async with _ScopeStack(di_registry) as scopes:
            for i in range(count):
                args = make_enqueue_args(payload={"value": i}, idempotency_key=f"rt-fb-{i}")
                row = await backend.enqueue(args)
                claimed = await backend.dispatch_batch(
                    worker_id, ["default"], 10, timedelta(seconds=30)
                )
                assert [j.id for j in claimed] == [row.id], "the claim must take the job"
                await dispatch_one_job(
                    backend=backend,
                    deps=deps,  # type: ignore[arg-type]  # Why: the same Any-cast seam as test_dispatch_one_job._as_deps
                    job=claimed[0],
                    worker_id=worker_id,
                    registry=scopes.registry,
                    process_scope=scopes.process_scope,
                    thread_scope=scopes.thread_scope,
                    loop_scope=scopes.loop_scope,
                    actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: same ActorRef generic-widening pattern as test_dispatch_one_job
                    actor_config=StubActorConfig(retry=RetryPolicy()),
                    clock=SystemClock(),
                    enqueuer=SubJobEnqueuer(
                        backend=backend, loop_scope_resolved=None, worker_pool=None
                    ),
                )
        return stack, deps, client, script, schema
    except BaseException:
        await stack.aclose()
        await client.aclose()
        raise


class _ScopeStack:
    def __init__(self, registry: ProviderRegistry) -> None:
        self.registry = registry

    async def __aenter__(self) -> _ScopeStack:
        self.registry.validate()
        scopes = make_scopes(self.registry)
        self.process_scope, self.thread_scope, self.loop_scope = scopes
        await bootstrap_scopes(self.registry, *scopes)
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.loop_scope.shutdown()
        await self.thread_scope.shutdown()
        await self.process_scope.shutdown()


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            redis.ReadOnlyError("READONLY You can't write against a read only replica"),
            id="readonly",
        ),
        pytest.param(
            redis.OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'"),
            id="oom",
        ),
    ],
)
async def test_fallback_admission_is_one_independent_pg_decision(
    pg_dsn: str, error: Exception
) -> None:
    """A readonly/oom rejection mid-dispatch: the fallback makes ONE
    independent PG decision per admission. Two jobs through the same bucket
    mean exactly two redis acquires (one each, never a replay), two clean
    admissions, two succeeded rows, and a bucket ledger in the REAL table
    that spent exactly two tokens."""
    _ACTOR_RUNS[0] = 0
    stack, _deps, client, script, schema = await _dispatch_with_dead_redis(
        pg_dsn, error=error, count=2
    )
    try:
        assert _ACTOR_RUNS[0] == 2, (
            f"CONTRACT: the fallback admission is clean - the actor runs exactly "
            f"once per job; ran {_ACTOR_RUNS[0]} times for 2 jobs."
        )
        assert script.calls == 2, (
            f"CONTRACT: exactly one redis acquire per dispatch, never a replay - "
            f"a replay spends the bucket twice for one admission; got {script.calls} "
            f"acquires for 2 dispatches."
        )
        import asyncpg

        conn = await asyncpg.connect(pg_dsn)
        try:
            succeeded: int = await conn.fetchval(
                f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'succeeded'"
            )
            assert succeeded == 2, (
                f"CONTRACT: both fallback admissions terminalize exactly once; "
                f"got {succeeded} succeeded rows for 2 jobs."
            )
            failed: int = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE status NOT IN '
                "('succeeded', 'pending', 'running')"
            )
            assert failed == 0, "no job may land in the failure-accounting path for an infra outage"
            state: Any = await conn.fetchval(
                f'SELECT state FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
                _BUCKET,
            )
            assert state is not None, "the fallback must have written the bucket ledger to PG"
            # A raw asyncpg connection has no jsonb codec: the document
            # arrives as its text form.
            import json

            doc: Any = json.loads(state) if isinstance(state, str) else state
            tokens = float(doc.get("tokens", -1.0)) if isinstance(doc, dict) else -1.0
            assert tokens == pytest.approx(3.0, abs=0.5), (
                f"CONTRACT: the ledger reflects exactly the admissions - capacity 5 "
                f"minus 2 spends (plus sub-second refill drift); got {tokens}. A "
                f"replay would leave 2.0 or less."
            )
        finally:
            await conn.close()
    finally:
        await stack.aclose()
        await client.aclose()
        await _drop(pg_dsn, schema)


async def _drop(pg_dsn: str, schema: str) -> None:
    import asyncpg

    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()

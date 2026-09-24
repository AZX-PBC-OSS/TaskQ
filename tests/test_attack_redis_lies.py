"""Redis-lies pins: the store answers, but the answer is a LIE.

The attack class: not an outage (the fallback handles down), a proxy
between TaskQ and Redis returning semantically wrong replies:

* stale values (the bucket state from 10 minutes ago),
* wrong types (a string where an int belongs),
* huge values (a token count of 10^18, a seq past every future event),
* negative values (tokens: -50, the permanent-denial lie),
* truncated or non-sequential replies (2 elements, ``None``, a bare
  string),
* values for the WRONG key (a crossed wire).

The fail-closed contract pinned here: a lie routes to the SAME path as an
outage, the PG fallback on acquire (admission re-run against the durable
row), the worker's synthetic non-consuming denial when no fallback is
wired, and message discard on the SSE stream, never a crash, never the
lie trusted into a decision.

Trust boundaries and the lie each pin kills:

* the token-bucket Lua decode (``_decode_lua_result``): shape, verdict in
  {0, 1}, tokens in [0, capacity], retry hint >= 0. The negative-tokens
  lie was TRUSTED into the decision before the boundary; the wrong-type,
  truncated and None lies crashed with ``ValueError`` /
  ``IndexError`` / ``TypeError`` no handler recognised (the mutant that
  drops the shape check reintroduces the IndexError; the mutant that
  drops the range check reintroduces the trusted negative count).
* the sliding-window Lua decode (``_validate_script_reply``): the huge
  ``retry_after_ms`` lie was a ``timedelta`` OverflowError crash on the
  denial path; the out-of-range window count was trusted.
* the peek parsers (token-bucket hash read, GCRA TAT read, log peek's
  ZCARD and score, the ``TIME`` read): garbage was a ``ValueError``
  crash with no provenance; now the store-corrupt sentinel, the same
  class a store that cannot answer raises.
* the SSE envelope validator: the huge-seq lie advanced the dedup cursor
  past every future event (the progress blackhole, pinned by the
  subsequent honest event arriving); the crossed-wire lie (another job's
  envelope on this job's channel) was forwarded verbatim.
* the dispatch composition: a lying acquire must not burn the job's
  retry budget (the synthetic denial) and must not blame the actor
  (no mark_failed_or_retry, no job_exception).

All pins are unit-level (duck-typed clients, no container); the
dispatch-level pins reuse the rt_depfail harness shape.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from uuid import UUID

import pytest
import redis.asyncio as redis_async

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.exceptions import RateLimitDependencyUnavailable, RateLimitStoreCorrupt
from taskq.progress._events import ProgressEvent
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit._sliding_window_redis import _peek_redis_gcra, _peek_redis_log
from taskq.settings import WorkerSettings
from taskq.web.progress import _event_generator

pytestmark = [pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")]

_START = datetime(2026, 1, 1, tzinfo=UTC)
_JOB_ID = new_uuid()
_ACTOR = "lie_probe"
_UNSET = object()
"""Distinguishes "no lie injected" from "the lie IS ``None``"."""


# ── lying-proxy doubles ──────────────────────────────────────────────


class _LyingScript:
    """AsyncScript double that answers with the injected hostile reply."""

    def __init__(self, reply: object) -> None:
        self._reply = reply
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        return self._reply


def _lying_redis_client(reply: object) -> tuple[redis_async.Redis, _LyingScript]:
    """A REAL ``redis.asyncio.Redis`` (dispatch resolves the client via
    ``isinstance``) whose Lua script seam answers *reply* - no socket."""
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    script = _LyingScript(reply)
    client.register_script = lambda _script: script  # type: ignore[method-assign]  # Why: injecting the lie at the script-call seam redis-py would use; no connection exists
    return client, script


def _settings(**overrides: object) -> WorkerSettings:
    base: dict[str, object] = {
        "pg_dsn": "postgresql://u:p@h/d",
        "schema_name": "taskq_test",
        "redis_url": "redis://lie-proxy:6379/0",
    }
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)  # type: ignore[arg-type]  # Why: WorkerSettings.load_from_dict accepts the dict at runtime


class _FakePgConn:
    """Duck-typed asyncpg connection for the PG fallback arms.

    Dispatches ``fetchrow`` on the SQL text: each fallback's fused
    statement names its own result columns, so the SQL is the discriminator.
    """

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    async def fetchval(self, sql: str, *args: object) -> object:
        # The advisory try-lock: truthy = held.
        return True

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object]:
        if "tokens_after" in sql:
            # Token-bucket fused acquire's RETURNING row.
            return {"tokens_after": 4.0, "granted": True}
        if "inserted" in sql:
            # Sliding-window log fused acquire's SELECT row.
            return {
                "inserted": True,
                "count_in_window": 1,
                "oldest_ts": None,
                "server_now": _START,
            }
        if "new_tat" in sql:
            # GCRA fused acquire's RETURNING row.
            return {"new_tat": 1767225601.0, "now_s": 1767225600.0}
        if "kind" in sql:
            # GCRA denial follow-up read.
            return {"kind": "gcra", "tat": None, "now_s": 1767225600.0}
        # The log-style peek's count row.
        return {"count": 0}

    def transaction(self) -> "_FakePgConn":
        return self

    async def __aenter__(self) -> "_FakePgConn":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePgPool:
    def acquire(self) -> _FakePgConn:
        return _FakePgConn()


# ── token-bucket acquire: every lie routes to the fallback ───────────


_LIES: list[Any] = [
    pytest.param([1, b"-50.0", b"0"], id="negative-tokens-with-allow"),
    pytest.param([1, b"999999", b"0"], id="huge-tokens"),
    pytest.param([1, b"nan", b"0"], id="nan-tokens"),
    pytest.param([1, b"inf", b"0"], id="inf-tokens"),
    pytest.param([b"garbage", b"1", b"1"], id="wrong-type-verdict"),
    pytest.param([1, b"not-a-float", b"0"], id="wrong-type-tokens"),
    pytest.param([1, b"1.0"], id="truncated"),
    pytest.param([], id="empty"),
    pytest.param(None, id="none-reply"),
    pytest.param("OK", id="bare-ok-string"),
    pytest.param([2, b"1.0", b"0"], id="verdict-2"),
    pytest.param([-1, b"1.0", b"0"], id="verdict-negative"),
    pytest.param([0, b"4.0", b"-3"], id="negative-retry-hint"),
]


@pytest.mark.parametrize("reply", _LIES)
async def test_token_bucket_lie_routes_to_pg_fallback(reply: object) -> None:
    """Every semantically-wrong reply is failed closed to the SAME path an
    outage takes: the PG fallback re-runs admission against the durable
    row. No lie reaches the decision as ``backend="redis"``; none crashes
    with a bare ValueError/IndexError/TypeError.

    The mutant that drops the shape check dies on the truncated/None
    params (IndexError/TypeError escape instead of the fallback); the
    mutant that drops the range check dies on the negative/huge params
    (the lie is trusted as backend="redis").
    """
    tb = TokenBucket(name="lie", capacity=5, refill_per_second=1.0, backend="redis")
    client, _script = _lying_redis_client(reply)
    pool = _FakePgPool()
    decision = await tb.acquire(
        redis_client=client, pg_pool=pool, clock=SystemClock(), settings=_settings()
    )
    assert decision.backend == "postgres", (
        "a lying reply must route to the PG fallback (the outage path), never be "
        f"trusted as a redis answer and never crash: reply was {reply!r}"
    )
    await client.aclose()


async def test_token_bucket_lie_without_fallback_raises_the_store_corrupt_sentinel() -> None:
    """With the PG fallback disabled, a lie raises
    :class:`RateLimitStoreCorrupt`, a RuntimeError subclass that the
    worker's dependency-failure family already recognises, so the acquire
    boundary synthesizes the same non-consuming denial an outage gets.
    """
    from taskq.worker._consumer import _RATE_LIMIT_DEPENDENCY_EXCEPTIONS

    tb = TokenBucket(name="lie", capacity=5, refill_per_second=1.0, backend="redis")
    client, _script = _lying_redis_client([1, b"-50.0", b"0"])
    with pytest.raises(RuntimeError) as exc_info:
        await tb.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(rate_limit_pg_fallback_enabled=False),
        )
    assert isinstance(exc_info.value, RateLimitStoreCorrupt)
    assert isinstance(exc_info.value, RateLimitDependencyUnavailable)
    assert isinstance(exc_info.value, _RATE_LIMIT_DEPENDENCY_EXCEPTIONS), (
        "the lie sentinel must be in the worker's dependency-failure family: "
        "the acquire boundary fails it closed as the limiter's denial, the "
        "same channel an outage takes"
    )
    await client.aclose()


async def test_token_bucket_honest_reply_still_admits() -> None:
    """Control: the boundary passes the honest replies through unchanged
    (the allow verdict, the deny verdict with its hint) - validation must
    not fail closed the healthy store too.
    """
    for reply, allowed, remaining in [
        ([1, b"4.0", b"0"], True, 4.0),
        ([0, b"0.0", b"1.5"], False, 0.0),
        ([1, "4.0", "0"], True, 4.0),  # decode_responses=True shape
    ]:
        tb = TokenBucket(name="ok", capacity=5, refill_per_second=1.0, backend="redis")
        client, _script = _lying_redis_client(reply)
        decision = await tb.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(rate_limit_pg_fallback_enabled=False),
        )
        assert decision.backend == "redis"
        assert decision.allowed is allowed
        assert decision.remaining == remaining
        await client.aclose()


# ── token-bucket peek: lies are the store-corrupt sentinel ────────────


class _PeekRedis:
    """Duck-typed client for the peek paths: controllable TIME and hash."""

    def __init__(
        self,
        *,
        time_reply: object = _UNSET,
        hmget_reply: object = _UNSET,
        get_reply: object = _UNSET,
        zcount_reply: object = _UNSET,
        zrangebyscore_reply: object = _UNSET,
    ) -> None:
        self._time = [1767225600, 0] if time_reply is _UNSET else time_reply
        self._hmget = hmget_reply
        self._get = get_reply
        self._zcount = zcount_reply
        self._zrangebyscore = zrangebyscore_reply

    async def time(self) -> object:
        return self._time

    async def hmget(self, key: str, fields: list[str]) -> object:
        return self._hmget

    async def get(self, key: str) -> object:
        return self._get

    async def zcount(self, key: str, min: str, max: str) -> object:
        return self._zcount

    async def zrangebyscore(
        self, key: str, min: str, max: str, start: int = 0, num: int = 1, withscores: bool = False
    ) -> object:
        return self._zrangebyscore


async def test_token_bucket_peek_lies_raise_the_sentinel() -> None:
    """The peek path's hash read is the same trust boundary: a non-numeric
    token value (the wrong-type lie), a negative count (the
    permanent-denial lie) and a huge one (the phantom-balance lie) raise
    the sentinel, not a bare ValueError the operator cannot attribute.
    """
    for hmget, _label in [
        ([b"garbage", b"1767225600"], "non-numeric tokens"),
        ([b"-50.0", b"1767225600"], "negative tokens"),
        ([b"999999", b"1767225600"], "huge tokens"),
        ([b"nan", b"1767225600"], "nan tokens"),
        (["garbage", 1767225600], "wrong-type element"),
    ]:
        tb = TokenBucket(name="lie", capacity=5, refill_per_second=1.0, backend="redis")
        with pytest.raises(RateLimitStoreCorrupt, match="peek"):
            await tb.peek(redis_client=_PeekRedis(hmget_reply=hmget), settings=_settings())
        # mutation anchor: the dropped-validation mutant lets the lie
        # through as a RateLimitState with is_exhausted driven by the lie
        # (negative -> exhausted=False... the State is built either way);
        # the sentinel here is the fail-closed verdict.


async def test_token_bucket_peek_honest_reads_pass() -> None:
    """Control: honest hash state (including a full and an empty bucket)
    peeks normally.
    """
    for hmget, exhausted in [
        ([b"4.0", b"1767225600"], False),
        ([b"0.0", b"1767225600"], True),
        (None, False),  # empty hash: the full-bucket default
    ]:
        tb = TokenBucket(name="ok", capacity=5, refill_per_second=1.0, backend="redis")
        state = await tb.peek(redis_client=_PeekRedis(hmget_reply=hmget), settings=_settings())
        assert state.backend == "redis"
        assert state.is_exhausted is exhausted


async def test_redis_time_lie_raises_the_sentinel() -> None:
    """The store-clock read (``TIME``) is a trust boundary of its own: the
    peek paths' elapsed math runs on it. A malformed tuple (wrong arity,
    non-numeric, nan) raises the sentinel, not IndexError/ValueError.
    """
    for reply in ([1], [b"x", b"y"], "12", None, [1767225600, b"nan"]):
        with pytest.raises(RateLimitStoreCorrupt, match="TIME"):
            from taskq.ratelimit._redis_utils import redis_time_seconds

            await redis_time_seconds(_PeekRedis(time_reply=reply))  # type: ignore[arg-type]  # Why: the duck-typed client is the point


# ── sliding-window acquire: the OverflowError lie fails closed ────────


async def test_sliding_window_huge_retry_hint_routes_to_fallback() -> None:
    """The denial-path lie of a huge ``retry_after_ms`` was a hard
    ``OverflowError`` crash (``timedelta`` cannot hold it) before the
    boundary existed; now it routes to the PG fallback like an outage.
    Both styles' field orders are pinned (log: count then hint; GCRA:
    hint then count).
    """
    for style, reply in [
        ("log", [0, b"5", b"99999999999999999999"]),
        ("gcra", [0, b"99999999999999999999", b"1"]),
        ("log", [b"junk", b"1", b"1"]),
        ("gcra", [0, b"1", b"bogus"]),
        ("log", [0, b"-999", b"-5"]),  # the negative-count lie
        ("log", [0, b"5", b"-1"]),  # the negative-hint lie
    ]:
        sw = SlidingWindow(
            name="lie", limit=5, window=timedelta(seconds=60), backend="redis", style=style
        )
        client, _script = _lying_redis_client(reply)
        pool = _FakePgPool()
        decision = await sw.acquire(
            redis_client=client, pg_pool=pool, clock=SystemClock(), settings=_settings()
        )
        assert decision.backend == "postgres", f"style={style} reply={reply!r}"
        await client.aclose()


async def test_sliding_window_honest_replies_pass() -> None:
    """Control: honest allow and deny replies of both styles decode
    unchanged.
    """
    for style, reply, allowed, retry_s in [
        ("log", [1, b"2", b"0"], True, 0),
        ("log", [0, b"5", b"30000"], False, 30.0),
        ("gcra", [1, b"0", b"3", b"1.0", b"2.0"], True, 0),
        ("gcra", [0, b"500", b"0"], False, 0.5),
    ]:
        sw = SlidingWindow(
            name="ok", limit=5, window=timedelta(seconds=60), backend="redis", style=style
        )
        client, _script = _lying_redis_client(reply)
        decision = await sw.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(rate_limit_pg_fallback_enabled=False),
        )
        assert decision.allowed is allowed
        assert decision.retry_after == timedelta(seconds=retry_s)
        await client.aclose()


async def test_sliding_window_peek_lies_raise_the_sentinel() -> None:
    """The log peek's ZCARD and oldest-score reads and the GCRA peek's TAT
    read are trust boundaries: garbage raises the sentinel, never a bare
    ValueError/IndexError, and a huge score can no longer overflow the
    timedelta it feeds.
    """
    with pytest.raises(RateLimitStoreCorrupt, match="ZCARD"):
        await _peek_redis_log(
            SlidingWindow("l", limit=5, window=timedelta(seconds=10), style="log"),
            redis_client=_PeekRedis(time_reply=[2000, 0], zcount_reply=b"garbage"),
            settings=_settings(),
        )
    with pytest.raises(RateLimitStoreCorrupt, match="window"):
        # The score lie: the reply names a member OUTSIDE the window the
        # query itself filtered by; a huge one overflowed the timedelta
        # before the boundary existed.
        await _peek_redis_log(
            SlidingWindow("l", limit=5, window=timedelta(seconds=10), style="log"),
            redis_client=_PeekRedis(
                time_reply=[2000, 0],
                zcount_reply=10,
                zrangebyscore_reply=[(b"req1", 999999999999999.0)],
            ),
            settings=_settings(),
        )
    with pytest.raises(RateLimitStoreCorrupt, match="TAT"):
        await _peek_redis_gcra(
            SlidingWindow("g", limit=5, window=timedelta(seconds=10), style="gcra"),
            redis_client=_PeekRedis(time_reply=[2000, 0], get_reply=b"garbage"),
            settings=_settings(),
        )


# ── SSE envelope: the cursor blackhole and the crossed wire ───────────


def _envelope_bytes(
    *,
    seq: int,
    job_id: UUID = _JOB_ID,
    kind: str = "progress",
    status: str = "running",
    terminal: bool = False,
) -> bytes:
    event = ProgressEvent(
        kind=kind,  # type: ignore[arg-type]  # Why: test-only construction with known-valid values
        job_id=job_id,
        actor=_ACTOR,
        ts=_START,
        seq=seq,
        status=status,
        step=None,
        percent=None,
        detail=None,
        data=None,
        terminal=terminal,
    )
    return event.model_dump_json(exclude_none=True).encode("utf-8")


def _crossed_wire_bytes(seq: int) -> bytes:
    """A foreign envelope: a real ProgressEvent shape for ANOTHER job,
    the crossed-wire lie, valid by every other check."""
    return _envelope_bytes(seq=seq, job_id=new_uuid())


class _PubSubMock:
    """Duck-typed pubsub serving a fixed message list, then parking."""

    def __init__(self, messages: list[dict[str, object]]) -> None:
        self._messages = list(messages)

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature, the seam the generator calls through.
    ) -> dict[str, object] | None:
        if self._messages:
            return self._messages.pop(0)
        await asyncio.sleep(3600)  # the loop's park; the test cancels

    async def unsubscribe(self, channel: str) -> None:  # the generator's finally
        return None

    async def aclose(self) -> None:
        return None


async def _collect(pubsub: Any, n: int) -> list[Any]:
    gen = _event_generator(
        pubsub=pubsub,
        channel="taskq:s:progress:job",
        job_id=_JOB_ID,
        is_terminal=False,
        progress_seq=0,
        progress_data="{}",
        resolved_last_event_id=None,
        heartbeat_secs=15.0,
    )
    out: list[Any] = []
    try:
        for _ in range(n):
            out.append(await gen.__anext__())
    finally:
        # Close the generator so its finally releases the slot and the
        # subscription; no task of this test outlives the test.
        await gen.aclose()
    return out


async def test_sse_huge_seq_lie_cannot_blackhole_the_cursor() -> None:
    """The huge-seq lie: an envelope carrying a seq no honest writer can
    issue (the durable cursor is the int4 ``progress_seq`` column) must
    be discarded WITHOUT advancing the dedup cursor, so the honest event
    behind it still arrives. The mutant that drops the range check
    yields the lie, sets ``last_emitted_seq`` to 2^31, and the honest
    event is filtered as a duplicate: the progress blackhole.
    """
    pubsub = _PubSubMock(
        [
            {"type": "message", "data": _envelope_bytes(seq=2**31)},
            {"type": "message", "data": _envelope_bytes(seq=5)},
        ]
    )
    events = await _collect(pubsub, 2)
    assert str(2**31) not in [e.id for e in events], "the lie must be discarded"
    assert events[1].id == "5", (
        "the honest event AFTER the lie must still arrive: a discarded lie "
        "must not advance the dedup cursor into a range no future event "
        "can reach (the progress blackhole)"
    )


async def test_sse_crossed_wire_envelope_is_discarded() -> None:
    """The crossed-wire lie: another job's envelope on this job's channel
    must be discarded, never forwarded onto this stream. The mutant that
    drops the job_id compare forwards the foreign payload (and its seq
    gates this job's honest events).
    """
    pubsub = _PubSubMock(
        [
            {"type": "message", "data": _crossed_wire_bytes(seq=3)},
            {"type": "message", "data": _envelope_bytes(seq=3)},
        ]
    )
    events = await _collect(pubsub, 2)
    forwarded = [json.loads(e.data) for e in events if getattr(e, "data", None)]
    assert len(forwarded) == 2  # the snapshot plus the honest event
    assert all(str(_JOB_ID) == str(event["job_id"]) for event in [forwarded[-1]]), (
        "the forwarded envelope must be this stream's job, the foreign one must have been discarded"
    )


async def test_sse_negative_seq_lie_is_discarded() -> None:
    """The negative-seq lie (the permanent-denial shape on the cursor):
    out of the int4 cursor's domain, discarded without advancing the
    cursor; the honest event behind it arrives.
    """
    pubsub = _PubSubMock(
        [
            {"type": "message", "data": _envelope_bytes(seq=-7)},
            {"type": "message", "data": _envelope_bytes(seq=2)},
        ]
    )
    events = await _collect(pubsub, 2)
    assert events[1].id == "2"


# ── the dispatch composition: a lie is a denial, not a job failure ────


def _payload_model() -> "type[Any]":
    from pydantic import BaseModel, ConfigDict

    class _Payload(BaseModel):
        value: int = 0

        model_config = ConfigDict(extra="forbid")

    return _Payload


async def test_dispatch_lying_acquire_is_the_synthetic_denial() -> None:
    """The end-to-end pin: a limiter whose store LIES must land the job in
    the synthetic non-consuming denial (the outage channel), never in the
    failure accounting. Reuses the rt_depfail harness shape with a lying
    script instead of a raising one.
    """
    from taskq._di.registry import ProviderRegistry
    from taskq._di.scope import Scope
    from taskq._ids import new_uuid
    from taskq.actor import ActorRef
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.ratelimit._provider import register_rate_limit_registry
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker.dispatch import dispatch_one_job
    from tests._di_scopes import bootstrap_scopes, make_scopes

    runs = 0

    async def _actor(payload: Any, ctx: Any) -> None:
        nonlocal runs
        runs += 1

    bucket = TokenBucket(name="lie_bucket", capacity=5, refill_per_second=1.0, backend="redis")
    rl_registry = RateLimitRegistry()
    rl_registry.register(bucket)

    di_registry = ProviderRegistry()
    register_rate_limit_registry(di_registry, rl_registry)
    client, _script = _lying_redis_client([1, b"-50.0", b"0"])
    di_registry.register_value(redis_async.Redis, Scope.LOOP, client)

    class _FakeWorkerDeps:
        active_jobs = None
        worker_pool: Any = None
        slot_pool: Any = None
        settings = WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq"}
        )
        redis_client: Any = None
        progress_buffers: ClassVar[dict[Any, Any]] = {}
        disowned_jobs: ClassVar[set[UUID]] = set()

    class _ScopeStack:
        def __init__(self, registry: ProviderRegistry) -> None:
            self.registry = registry

        async def __aenter__(self) -> "_ScopeStack":
            self.registry.validate()
            scopes = make_scopes(self.registry)
            self.process_scope, self.thread_scope, self.loop_scope = scopes
            await bootstrap_scopes(self.registry, *scopes)
            return self

        async def __aexit__(self, *exc: object) -> None:
            await self.loop_scope.shutdown()
            await self.thread_scope.shutdown()
            await self.process_scope.shutdown()

    fake_deps = _FakeWorkerDeps()
    fake_deps.settings.rate_limit_pg_fallback_enabled = False
    actor_ref = ActorRef(
        name="lie_actor",
        queue="default",
        fn=_actor,
        wants_ctx=True,
        dependencies={},
        payload_type=_payload_model(),  # type: ignore[arg-type]  # Why: the harness's stub shape
        result_adapter=None,  # type: ignore[arg-type]
        retry=RetryPolicy(),
        result_ttl=None,
        rate_limits=[bucket],
    )
    fake_backend = FakeBackend()
    try:
        async with _ScopeStack(di_registry) as scopes:
            await dispatch_one_job(
                backend=as_backend(fake_backend),
                deps=fake_deps,  # type: ignore[arg-type]  # Why: the harness's Any-cast seam
                job=make_job_row(payload={"value": 42}),
                worker_id=new_uuid(),
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=actor_ref,  # type: ignore[arg-type]  # Why: the harness's ActorRef generic-widening seam
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(_START),
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )
    finally:
        await client.aclose()

    assert runs == 0, "the actor must never run when its limiter's store lies"
    assert fake_backend.mark_failed_or_retry_calls == [], (
        "the lie must not be misattributed to the job: no mark_failed_or_retry "
        "write, the retry budget does not burn"
    )
    snoozes = fake_backend.mark_snoozed_calls
    assert len(snoozes) == 1, "the fail-closed outcome is the synthetic denial's snooze"
    assert snoozes[0]["outcome"] == "rate_limit_denied"
    assert snoozes[0]["denial_reason"] == "unavailable", (
        "the lying store's denial must ride the NON-consuming 'unavailable' "
        "snooze arm (attempt refunded), never the saturation 'capacity' arm"
    )

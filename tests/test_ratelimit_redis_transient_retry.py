"""Redis transient-retry pins: weather gets a bounded retry, lies get the
sentinel.

The gap this pins: a TRANSIENT connection failure mid-burst (a blip of
100ms to 1s, the container co-tenancy shape) took the SAME path as a
persistent outage, straight to the fail-closed decision (the PG fallback
arm, then :class:`RateLimitDependencyUnavailable` when no pool is
wired), so a redis-only deployment denied a legitimate request the
store would have served a quarter-second later.

The contract pinned here, on BOTH redis acquire surfaces (the token
bucket and the sliding window, log and GCRA styles):

* a CONNECTION-family error (``redis.ConnectionError``/``redis.TimeoutError``,
  the error reaching the SERVER) is weathered with a bounded retry
  (:data:`~taskq.constants.RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS`
  attempts, the short backoffs of
  :data:`~taskq.constants.RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S`);
  a one-shot blip heals inside the budget and the honest answer is used;
* a PERSISTENT outage still fails closed: after the budget is spent the
  decision is exactly what it always was (the PG fallback when wired,
  the dependency-unavailable sentinel when not), and the attempt count
  is EXACTLY the budget, so a replay cannot loop;
* the store-lie shapes (:class:`~taskq.exceptions.RateLimitStoreCorrupt`,
  the container/type lies the redis-lies lineage pins) are NEVER
  retried: the reply arrived and is wrong, re-asking the liar buys
  nothing, so they fail closed on FIRST SIGHT, one script call, the
  sentinel.

All pins are unit-level (duck-typed script seams on a real
``redis.asyncio.Redis`` client, no socket), the same shape the
redis-lies pins use.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis.asyncio as redis_async

from taskq.backend.clock import SystemClock
from taskq.constants import (
    RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS,
    RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S,
)
from taskq.exceptions import RateLimitDependencyUnavailable, RateLimitStoreCorrupt
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.settings import WorkerSettings

_START = datetime(2026, 1, 1, tzinfo=UTC)

_HONEST_TB_REPLY: list[object] = [1, b"4.0", b"0"]
_HONEST_LOG_REPLY: list[object] = [1, b"2", b"0"]
_HONEST_GCRA_REPLY: list[object] = [1, b"0", b"3", b"1.0", b"2.0"]

_THE_STORE_LIE: list[object] = [1, b"-50.0", b"0"]
"""The negative-tokens lie: decode raises ``RateLimitStoreCorrupt`` on sight."""


class _BlipThenHonestScript:
    """AsyncScript double: refuses once (ConnectionError, then optionally a
    TimeoutError), then answers honestly. A one-shot blip."""

    def __init__(self, reply: object, errors: list[type[BaseException]]) -> None:
        self._reply = reply
        self._errors = list(errors)
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        if self._errors:
            raise self._errors.pop(0)(  # type: ignore[misc]  # Why: the family is (ConnectionError, TimeoutError), both exception types
                "blip: connection refused once"
            )
        return self._reply


class _DeadScript:
    """AsyncScript double: a PERSISTENT outage, raises on every attempt."""

    def __init__(self, error: type[BaseException]) -> None:
        self._error = error
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        raise self._error("persistent outage: every attempt refuses")  # type: ignore[misc]  # Why: see _BlipThenHonestScript

    @property
    def attempts(self) -> int:
        return self.calls


class _LyingScript:
    """AsyncScript double that answers with the injected hostile reply."""

    def __init__(self, reply: object) -> None:
        self._reply = reply
        self.calls = 0

    async def __call__(self, **kwargs: object) -> object:
        self.calls += 1
        return self._reply


def _scripted_client(script: Any) -> redis_async.Redis:
    """A REAL ``redis.asyncio.Redis`` (dispatch resolves the client via
    ``isinstance``) whose Lua script seam is the injected double, no socket."""
    client = redis_async.Redis(host="127.0.0.1", port=1, decode_responses=False)
    client.register_script = lambda _script: script  # type: ignore[method-assign]  # Why: injecting at the script-call seam redis-py would use; no connection exists
    return client


def _settings(fallback_enabled: bool = True) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",  # the deliberate redis-only shape
            "schema_name": "transient_retry_pin",
            "redis_url": "redis://blip-proxy:6379/0",
            "rate_limit_pg_fallback_enabled": fallback_enabled,
        }
    )  # type: ignore[arg-type]  # Why: WorkerSettings.load_from_dict accepts the dict at runtime


class _DuckPgConn:
    """Duck-typed asyncpg connection answering the token-bucket fused
    acquire's RETURNING row (the discriminator is the SQL text)."""

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object]:
        if "tokens_after" in sql:
            return {"tokens_after": 4.0, "granted": True}
        raise AssertionError(f"unexpected statement: {sql[:80]}")

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    def transaction(self) -> "_DuckPgConn":
        return self

    async def __aenter__(self) -> "_DuckPgConn":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class _DuckPgPool:
    def acquire(self) -> _DuckPgConn:
        return _DuckPgConn()


# ── the sliding-window lie pins: same rule, second surface ────────────


# ── the one-shot blip pin: weathered, the honest answer is used ───────


async def test_token_bucket_one_shot_connection_blip_is_weathered() -> None:
    """A one-shot ConnectionError blip on the token-bucket acquire is
    retried inside the bounded budget and the HONEST answer is used: the
    decision is ``backend="redis"``, never the fallback, never the
    dependency-unavailable sentinel. Red under the pre-retry code: the
    blip went straight to the fail-closed decision on ONE script call.
    """
    script = _BlipThenHonestScript(_HONEST_TB_REPLY, errors=[redis_async.ConnectionError])
    client = _scripted_client(script)
    tb = TokenBucket(name="blip_tb", capacity=5, refill_per_second=1.0, backend="redis")

    decision = await tb.acquire(
        redis_client=client, pg_pool=None, clock=SystemClock(), settings=_settings()
    )

    assert decision.backend == "redis", (
        "a one-shot connection blip must be weathered and answered from the "
        f"store, not fail closed: got backend={decision.backend}"
    )
    assert decision.allowed is True
    assert script.calls == 2, f"the blip costs exactly one retry: 2 attempts, got {script.calls}"
    await client.aclose()


@pytest.mark.parametrize(
    ("style", "honest_reply"),
    [("log", _HONEST_LOG_REPLY), ("gcra", _HONEST_GCRA_REPLY)],
)
async def test_sliding_window_one_shot_connection_blip_is_weathered(
    style: str, honest_reply: list[object]
) -> None:
    """The same one-shot blip on the sliding-window acquire (BOTH styles)
    is weathered: the honest answer is used, the decision is
    ``backend="redis"``. Red under the pre-retry code on both surfaces.
    """
    script = _BlipThenHonestScript(honest_reply, errors=[redis_async.ConnectionError])
    client = _scripted_client(script)
    sw = SlidingWindow(
        name=f"blip_sw_{style}", limit=5, window=timedelta(seconds=60), backend="redis", style=style
    )

    decision = await sw.acquire(
        redis_client=client, pg_pool=None, clock=SystemClock(), settings=_settings()
    )

    assert decision.backend == "redis", (
        f"style={style}: a one-shot connection blip must be weathered and "
        f"answered from the store, got backend={decision.backend}"
    )
    assert decision.allowed is True
    assert script.calls == 2, (
        f"style={style}: the blip costs exactly one retry: 2 attempts, got {script.calls}"
    )
    await client.aclose()


async def test_timeout_error_blip_is_weathered_too() -> None:
    """``redis.TimeoutError`` is the other CONNECTION-family error: a
    one-shot timeout blip is weathered exactly like a refused connection.
    """
    script = _BlipThenHonestScript(_HONEST_TB_REPLY, errors=[redis_async.TimeoutError])
    client = _scripted_client(script)
    tb = TokenBucket(name="blip_timeout", capacity=5, refill_per_second=1.0, backend="redis")

    decision = await tb.acquire(
        redis_client=client, pg_pool=None, clock=SystemClock(), settings=_settings()
    )

    assert decision.backend == "redis"
    assert decision.allowed is True
    assert script.calls == 2
    await client.aclose()


async def test_blip_is_weathered_even_with_the_fallback_disabled() -> None:
    """The retry arm sits BEFORE the fail-closed decision, both arms of
    it: with the PG fallback disabled, a blip is still weathered and the
    honest answer still wins (the pre-retry code raised here)."""
    script = _BlipThenHonestScript(_HONEST_TB_REPLY, errors=[redis_async.ConnectionError])
    client = _scripted_client(script)
    tb = TokenBucket(name="blip_nofb", capacity=5, refill_per_second=1.0, backend="redis")

    decision = await tb.acquire(
        redis_client=client,
        pg_pool=None,
        clock=SystemClock(),
        settings=_settings(fallback_enabled=False),
    )

    assert decision.backend == "redis"
    assert decision.allowed is True
    await client.aclose()


# ── the persistent-outage pin: the fail-closed contract's tooth ───────


async def test_persistent_outage_still_fails_closed_after_the_budget() -> None:
    """A persistent outage (every attempt refuses) still fails closed:
    after EXACTLY the bounded budget, the decision is what it always
    was. Redis-only shape (no pool): the dependency-unavailable sentinel,
    the loud error the fail-closed contract raises. The attempt count is
    EXACTLY :data:`RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS`: the retry
    is a budget, not a loop.
    """
    script = _DeadScript(redis_async.ConnectionError)
    client = _scripted_client(script)
    tb = TokenBucket(name="dead_tb", capacity=5, refill_per_second=1.0, backend="redis")

    with pytest.raises(RateLimitDependencyUnavailable, match="pg_pool not injected"):
        await tb.acquire(
            redis_client=client, pg_pool=None, clock=SystemClock(), settings=_settings()
        )

    assert script.calls == RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS, (
        f"the outage must be attempted exactly {RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS} "
        f"times (the bounded budget), got {script.calls}: an off-by-one or an "
        "unbounded replay loop breaks the fail-closed budget"
    )
    await client.aclose()


async def test_persistent_outage_fallback_disabled_raises_the_connection_error() -> None:
    """With the fallback disabled, the budget is spent and the RAW
    connection error propagates: the retry never swallows or reclassifies
    it, the fail-closed raise keeps its type."""
    script = _DeadScript(redis_async.ConnectionError)
    client = _scripted_client(script)
    tb = TokenBucket(name="dead_nofb", capacity=5, refill_per_second=1.0, backend="redis")

    with pytest.raises(redis_async.ConnectionError, match="persistent outage"):
        await tb.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(fallback_enabled=False),
        )

    assert script.calls == RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS
    await client.aclose()


async def test_persistent_outage_fails_closed_on_the_sliding_window_too() -> None:
    """The bounded budget + fail-closed decision on the sliding-window
    surface (log style pinned; GCRA shares the same wrapped seam)."""
    script = _DeadScript(redis_async.ConnectionError)
    client = _scripted_client(script)
    sw = SlidingWindow(
        name="dead_sw", limit=5, window=timedelta(seconds=60), backend="redis", style="log"
    )

    with pytest.raises(RateLimitDependencyUnavailable, match="pg_pool not injected"):
        await sw.acquire(
            redis_client=client, pg_pool=None, clock=SystemClock(), settings=_settings()
        )

    assert script.calls == RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS
    await client.aclose()


async def test_retry_budget_backoffs_sum_to_the_blip_window() -> None:
    """The pacing contract: one backoff per retry, the sequence summing
    to 1.0s, the ceiling of the observed blip window, so a persistent
    outage pays at most one extra second before the decision."""
    assert len(RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S) == (
        RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS - 1
    ), "one backoff per retry"
    assert all(b > 0 for b in RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S), "no zero waits"
    assert sum(RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S) <= 1.0, (
        "the budget's backoffs must stay inside the 1s blip window"
    )


# ── the store-lie pin: NEVER retried, fails closed on first sight ─────


async def test_store_lie_is_never_retried_fails_closed_on_first_sight() -> None:
    """A store-lie reply (the negative-tokens lie, decode raises
    ``RateLimitStoreCorrupt``) is NEVER retried: exactly ONE script call,
    then the fail-closed verdict. The retry arm exists for weather, a
    reply that arrived and is wrong is not weather, re-asking the liar
    buys nothing."""
    script = _LyingScript(_THE_STORE_LIE)
    client = _scripted_client(script)
    tb = TokenBucket(name="lie_tb", capacity=5, refill_per_second=1.0, backend="redis")

    with pytest.raises(RateLimitDependencyUnavailable) as exc_info:
        await tb.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(fallback_enabled=False),
        )

    assert isinstance(exc_info.value, RateLimitStoreCorrupt)
    assert script.calls == 1, (
        f"the lie must fail closed on FIRST SIGHT: 1 script call, got {script.calls} "
        "- a retried lie is a re-asked liar"
    )
    await client.aclose()


async def test_store_lie_routes_to_the_fallback_on_first_sight() -> None:
    """The lie's fail-closed verdict WITH the fallback wired: the PG arm
    runs on the FIRST sight (one script call, no weathering), the
    admission is re-run against the durable row exactly as the
    redis-lies pins contract."""
    script = _LyingScript(_THE_STORE_LIE)
    client = _scripted_client(script)
    tb = TokenBucket(name="lie_fb", capacity=5, refill_per_second=1.0, backend="redis")

    decision = await tb.acquire(
        redis_client=client,
        pg_pool=_DuckPgPool(),  # type: ignore[arg-type]  # Why: the duck-typed pool is the pin's seam, the same shape the redis-lies pins use
        clock=SystemClock(),
        settings=_settings(),
    )

    assert decision.backend == "postgres", (
        "the lie must route to the PG fallback, the outage path, on first sight"
    )
    assert script.calls == 1, (
        f"the lie must NOT be retried into the fallback: 1 script call, got {script.calls}"
    )
    await client.aclose()


@pytest.mark.parametrize(
    ("style", "lie_reply"),
    [
        ("log", [0, b"5", b"99999999999999999999"]),  # the huge-hint lie
        ("gcra", [0, b"99999999999999999999", b"1"]),  # the huge-hint lie
    ],
)
async def test_sliding_window_store_lie_is_never_retried(
    style: str, lie_reply: list[object]
) -> None:
    """The sliding-window surface's lie verdict (BOTH styles): one script
    call, the sentinel, never a retry."""
    script = _LyingScript(lie_reply)
    client = _scripted_client(script)
    sw = SlidingWindow(
        name=f"lie_sw_{style}", limit=5, window=timedelta(seconds=60), backend="redis", style=style
    )

    with pytest.raises(RateLimitDependencyUnavailable) as exc_info:
        await sw.acquire(
            redis_client=client,
            pg_pool=None,
            clock=SystemClock(),
            settings=_settings(fallback_enabled=False),
        )

    assert isinstance(exc_info.value, RateLimitStoreCorrupt)
    assert script.calls == 1, (
        f"style={style}: the lie must fail closed on FIRST SIGHT, got {script.calls} calls"
    )
    await client.aclose()

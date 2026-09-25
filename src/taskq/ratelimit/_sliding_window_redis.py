"""Redis implementations for sliding-window rate limiter.

All Redis-path methods (acquire, peek, reset, refund for both log and
GCRA styles, plus Lua-script caching helpers) live here as module-level
functions taking ``self: SlidingWindow`` as the first parameter.

Time domain: the acquire scripts derive now from ``redis.call('TIME')``
and the peek paths read the store clock via ``redis_time_seconds``, the
shared sorted-set scores and TATs are store-domain, so callers on nodes
with divergent Python clocks are all measured against the same window.
"""

import math
from datetime import timedelta
from typing import TYPE_CHECKING, cast
from uuid import UUID

from taskq.exceptions import RateLimitStoreCorrupt
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._redis_utils import ensure_redis_script, redis_time_seconds, with_pg_fallback
from taskq.ratelimit._scripts import (
    GCRA_REFUND_SCRIPT,
    SLIDING_WINDOW_GCRA_SCRIPT,
    SLIDING_WINDOW_LOG_SCRIPT,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState
from taskq.ratelimit.token_bucket import _retry_after

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.settings import WorkerSettings

__all__ = [
    "_acquire_redis_gcra",
    "_acquire_redis_gcra_wrapped",
    "_acquire_redis_log",
    "_acquire_redis_log_wrapped",
    "_peek_redis_gcra",
    "_peek_redis_log",
    "_refund_redis_gcra",
    "_refund_redis_log",
    "_reset_redis_gcra",
    "_reset_redis_log",
]


def _validate_script_reply(
    raw: object,
    *,
    limit: int,
    ttl_ms: int,
    gcra: bool,
) -> tuple[bool, int, int]:
    """Decode and validate a sliding-window script reply at the trust
    boundary; return ``(allowed, count_or_remaining, retry_after_ms)``.

    The reply contract per the script docs, a 3-element list where
    ``allowed`` is exactly 0 or 1:

    * log style: ``{allowed, count, retry_after_ms}``, the count after
      the operation inside ``[0, limit]`` (the ZADD is gated on
      ``count < limit``);
    * GCRA style: ``{allowed, retry_after_ms, remaining_estimate}``, the
      estimate floored at 0 by the script and bounded above by
      ``limit - 1``.

    ``retry_after_ms`` must be non-negative and bounded by the key's own
    TTL: no honest denial can outlive the key it came from. A proxy
    answering outside the contract (a truncated reply, a string where an
    int belongs, a count outside its range, a huge ``retry_after_ms``)
    raises :class:`RateLimitStoreCorrupt`, the fail-closed verdict: the
    caller's wrapped path re-runs admission against the durable PG row,
    exactly as a connection failure would, and a ``timedelta`` overflow
    (the crash a huge ``retry_after_ms`` produced before validation)
    cannot happen.

    The GCRA reply may carry two extra elements (the pre/post TAT strings
    for the compare-and-set refund); they are decoded by
    :func:`_gcra_tat_strings` and validated there only for type, a
    corrupt TAT echo fails the refund's CAS compare, which is already a
    no-op refund (the fail-closed direction). A type-valid echo holding
    bytes that are not UTF-8 is the one decode the type check cannot
    cover, so :func:`_gcra_tat_strings` raises the sentinel for it: the
    wrapped acquire takes the PG fallback instead of leaking a
    ``UnicodeDecodeError`` (a ``ValueError`` sibling the fallback does
    not name) out of the boundary.
    """
    if not isinstance(raw, (list, tuple)):
        raise RateLimitStoreCorrupt(
            f"sliding-window script reply violates the 3-element contract: {raw!r}"
        )
    elements = cast("list[object]", raw)
    if len(elements) < 3:
        raise RateLimitStoreCorrupt(
            f"sliding-window script reply violates the 3-element contract: {raw!r}"
        )
    try:
        allowed = int(elements[0])  # pyright: ignore[reportArgumentType]  # Why: the element is object after the shape check; int() accepts int | str | bytes at runtime
        second = int(elements[1])  # pyright: ignore[reportArgumentType]  # Why: same object element boundary
        third = int(elements[2])  # pyright: ignore[reportArgumentType]  # Why: same object element boundary
    except (TypeError, ValueError, OverflowError) as exc:
        # OverflowError joins the family: int(float("inf")) and
        # float(10**400) raise it, a sibling of neither TypeError nor
        # ValueError, and an uncaught one is the original crash class
        # the boundary exists to kill.
        raise RateLimitStoreCorrupt(f"sliding-window script reply is not numeric: {raw!r}") from exc
    if allowed not in (0, 1):
        raise RateLimitStoreCorrupt(
            f"sliding-window script reply has an impossible verdict: {raw!r}"
        )
    if gcra:
        count_or_remaining, retry_after_ms = third, second
    else:
        count_or_remaining, retry_after_ms = second, third
    if not (0 <= count_or_remaining <= limit):
        raise RateLimitStoreCorrupt(
            f"sliding-window script reply reports a count outside [0, limit={limit}]: {raw!r}"
        )
    if not (0 <= retry_after_ms <= ttl_ms):
        # The denial hint cannot ask for a wait longer than the key's
        # own lifetime (the key expires and the next acquire starts
        # fresh), and a negative wait is not a wait. Both are lies; the
        # huge case was a timedelta OverflowError crash before this
        # boundary existed.
        raise RateLimitStoreCorrupt(
            f"sliding-window script reply has a retry hint outside [0, ttl_ms={ttl_ms}]: {raw!r}"
        )
    return allowed == 1, count_or_remaining, retry_after_ms


def _gcra_tat_strings(raw: list[object] | tuple[object, ...]) -> tuple[str, str]:
    """Decode the GCRA reply's optional CAS echo elements (indexes 3 and 4).

    The decode is guarded: a type-valid element holding invalid-UTF-8
    bytes (``b"\\xff\\xfe"``) is a lie no honest script wrote (Lua
    ``tostring()`` of a number is ASCII), and a bare ``pre.decode()``
    raises ``UnicodeDecodeError`` - a ``ValueError`` sibling
    :func:`with_pg_fallback` does not name, so the lie escaped the
    wrapped acquire as a crash. Raising the store-corrupt sentinel here
    routes the lie to the same fail-closed fallback as every other
    verdict in this module.
    """
    pre, post = raw[3], raw[4]
    try:
        pre_str = pre.decode() if isinstance(pre, bytes) else str(pre)
        post_str = post.decode() if isinstance(post, bytes) else str(post)
    except UnicodeDecodeError as exc:
        raise RateLimitStoreCorrupt(
            f"sliding-window GCRA reply has a non-UTF-8 TAT echo: {raw!r}"
        ) from exc
    return pre_str, post_str


async def _ensure_log_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    def get() -> "AsyncScript | None":
        if self._redis_log_script_client is not redis_client:
            return None
        return self._redis_log_script

    def bind(script: "AsyncScript") -> None:
        self._redis_log_script_client = redis_client
        self._redis_log_script = script

    return await ensure_redis_script(
        get,
        bind,
        lambda: redis_client.register_script(SLIDING_WINDOW_LOG_SCRIPT),
        self._script_lock,
    )


async def _ensure_gcra_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    def get() -> "AsyncScript | None":
        if self._redis_gcra_script_client is not redis_client:
            return None
        return self._redis_gcra_script

    def bind(script: "AsyncScript") -> None:
        self._redis_gcra_script_client = redis_client
        self._redis_gcra_script = script

    return await ensure_redis_script(
        get,
        bind,
        lambda: redis_client.register_script(SLIDING_WINDOW_GCRA_SCRIPT),
        self._script_lock,
    )


async def _ensure_gcra_refund_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    def get() -> "AsyncScript | None":
        if self._redis_gcra_refund_script_client is not redis_client:
            return None
        return self._redis_gcra_refund_script

    def bind(script: "AsyncScript") -> None:
        self._redis_gcra_refund_script_client = redis_client
        self._redis_gcra_refund_script = script

    return await ensure_redis_script(
        get,
        bind,
        lambda: redis_client.register_script(GCRA_REFUND_SCRIPT),
        self._script_lock,
    )


async def _acquire_redis_log(
    self: "SlidingWindow",
    request_id: UUID | None,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis log-style acquire, the script derives now from
    ``redis.call('TIME')`` (store-domain), so no Python clock participates."""
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    if request_id is None:
        raise RuntimeError("request_id required for log-style acquire")

    script = await _ensure_log_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"

    window_ms = int(self._window.total_seconds() * 1000)
    ttl_ms = int(self._ttl.total_seconds() * 1000)

    argv: list[int | str] = [
        window_ms,
        self._limit,
        str(request_id),
        ttl_ms,
    ]

    raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation, pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

    allowed, count, retry_after_ms = _validate_script_reply(
        raw, limit=self._limit, ttl_ms=ttl_ms, gcra=False
    )

    result = RateLimitDecision(
        allowed=allowed,
        remaining=float(self._limit - count) if allowed else 0.0,
        retry_after=timedelta(0) if allowed else timedelta(milliseconds=retry_after_ms),
        bucket_name=self._name,
        backend="redis",
        request_id=str(request_id),
    )

    log_decision(result, style=self._style)
    return result


async def _acquire_redis_log_wrapped(
    self: "SlidingWindow",
    request_id: UUID | None,
    redis_client: "redis_async.Redis | None",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis log-style path with optional PG fallback on ConnectionError/TimeoutError."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_log

    return await with_pg_fallback(
        lambda: _acquire_redis_log(self, request_id, redis_client, settings),
        lambda: _acquire_pg_log(self, pg_pool, settings, request_id),
        bucket_name=self._name,
        settings=settings,
        style="log",
    )


async def _acquire_redis_gcra(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis GCRA acquire, the script derives now from ``redis.call('TIME')``
    (store-domain), so no Python clock participates."""
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    script = await _ensure_gcra_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"

    window_ms = int(self._window.total_seconds() * 1000)
    emission_interval_ms = window_ms / self._limit
    delay_tolerance_ms = window_ms
    quantity_ms = emission_interval_ms
    ttl_ms = int(self._ttl.total_seconds() * 1000)

    argv: list[float | int] = [
        emission_interval_ms,
        delay_tolerance_ms,
        quantity_ms,
        ttl_ms,
    ]

    raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation, pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

    allowed, remaining_estimate, retry_after_ms = _validate_script_reply(
        raw, limit=self._limit, ttl_ms=ttl_ms, gcra=True
    )

    previous_state: dict[str, object] | None = None
    if allowed and len(raw) >= 5:
        pre_str, post_str = _gcra_tat_strings(raw)
        previous_state = {
            "pre_acquire_tat_str": pre_str,
            "post_acquire_tat_str": post_str,
            "ttl_ms": ttl_ms,
        }

    result = RateLimitDecision(
        allowed=allowed,
        remaining=float(remaining_estimate) if allowed else 0.0,
        retry_after=timedelta(0) if allowed else timedelta(milliseconds=retry_after_ms),
        bucket_name=self._name,
        backend="redis",
        previous_state=previous_state,
    )

    log_decision(result, style=self._style)
    return result


async def _acquire_redis_gcra_wrapped(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis GCRA path with optional PG fallback on ConnectionError/TimeoutError."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_gcra

    return await with_pg_fallback(
        lambda: _acquire_redis_gcra(self, redis_client, settings),
        lambda: _acquire_pg_gcra(self, pg_pool, settings),
        bucket_name=self._name,
        settings=settings,
        style="gcra",
    )


async def _peek_redis_log(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    """Read-only log-style snapshot, the retry estimate runs on the
    store's clock (``TIME``), the same domain the acquire script's ZADD
    scores live in."""
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"
    window_ms = int(self._window.total_seconds() * 1000)

    # Read-only window filter: eviction only happens in acquire, so the
    # sorted set can still hold aged-out entries after the window empties
    # with no intervening acquire. Count only members whose score is
    # inside the window, exclusive lower bound at ``now - window``, the
    # exact boundary the acquire script's ZREMRANGEBYSCORE evicts up to ,
    # measured against the store's clock (``TIME``), the domain the
    # scores live in. ZCARD would count the whole key and overstate
    # exhaustion while the next acquire is allowed.
    now_ms = await redis_time_seconds(redis_client) * 1000
    if not math.isfinite(now_ms):
        # Why the DERIVED value gets its own guard: the seconds fields
        # can each pass the clock read's finiteness check yet overflow
        # the millisecond derivation (1e306 * 1000 = inf, silently), and
        # every window comparison below runs in that domain. A clock no
        # honest store can report is the sentinel, the same verdict as a
        # malformed TIME reply.
        raise RateLimitStoreCorrupt(
            f"sliding-window log peek read a non-finite store clock (TIME): {now_ms!r}"
        )
    cutoff_ms = now_ms - window_ms
    zcount_raw = await redis_client.zcount(key, f"({cutoff_ms}", "+inf")  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # Why: redis-py zcount return type is untyped in the stub
    try:
        count = int(zcount_raw)  # pyright: ignore[reportUnknownArgumentType]  # Why: untyped stub boundary; validated by the conversion and the range check below
    except (TypeError, ValueError, OverflowError) as exc:
        # OverflowError joins the family: int(float("inf")) and
        # float(10**400) raise it, a sibling of neither TypeError nor
        # ValueError, and an uncaught one is the original crash class
        # the boundary exists to kill.
        raise RateLimitStoreCorrupt(
            f"sliding-window log peek read a non-numeric ZCARD: {zcount_raw!r}"
        ) from exc
    if count < 0:
        # A negative window count is a lie (a ZCARD reply is a cardinality);
        # a count above limit is honest and only overstates exhaustion,
        # the fail-closed direction, so it is not a lie verdict.
        raise RateLimitStoreCorrupt(f"sliding-window log peek read a negative count: {count}")
    is_exhausted = count >= self._limit
    retry_after: timedelta | None = None

    if is_exhausted and count > 0:
        oldest = await redis_client.zrangebyscore(  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py zrangebyscore return type is untyped in the stub
            key, f"({cutoff_ms}", "+inf", start=0, num=1, withscores=True
        )
        if not isinstance(oldest, (list, tuple)):  # pyright: ignore[reportUnnecessaryIsInstance]  # Why: the stub types zrangebyscore as ZSetRangeResponse (list/tuple), but the declared type is exactly what a lying reply violates at runtime - the RESP3 map and the set both arrive through this untyped boundary, so the runtime shape check is the defense, not a redundancy.
            # Why the CONTAINER gets its own shape check before the
            # element access: the pair-arity check below validates the
            # ELEMENT's shape, but ``oldest[0]`` indexes the raw
            # zrangebyscore reply itself - a RESP3 map (a dict) raises
            # bare KeyError on the integer index and a set raises
            # TypeError, and neither is in any guarded conversion
            # family, so the lie escapes the peek as a bare crash. An
            # honest withscores reply is a list of pairs, so a
            # non-sequence container is the same verdict as every
            # other reply lie: the store-corrupt sentinel, the family
            # the truncated-pair check below already speaks. The EMPTY
            # list stays accepted: it is the honest count-vs-read race
            # (the member aged out between ZCOUNT and ZRANGEBYSCORE,
            # pinned by ``test_peek_log_oldest_empty_race``), not a
            # container lie.
            raise RateLimitStoreCorrupt(
                f"sliding-window log peek read a non-list withscores reply: {oldest!r}"
            )
        if oldest:
            oldest_entry = oldest[0]
            if isinstance(oldest_entry, (list, tuple)) and len(oldest_entry) != 2:
                # Why the pair arity is checked BEFORE the conversion: a
                # truncated withscores "pair" ([(b"req1",)] or [()])
                # raises bare IndexError on the element access, and
                # IndexError is in no guarded conversion family - the
                # reply contract (``_redis_utils.redis_time_seconds``:
                # "must not crash the caller with a
                # ValueError/TypeError/IndexError") explicitly names it
                # as a crash class this boundary must kill. Same verdict
                # as every other reply lie: the store-corrupt sentinel.
                raise RateLimitStoreCorrupt(
                    f"sliding-window log peek read a truncated withscores pair: {oldest!r}"
                )
            try:
                oldest_score = (
                    float(oldest_entry[1])
                    if isinstance(oldest_entry, (list, tuple))
                    else float(oldest_entry)
                )  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py zrangebyscore return type is untyped in the stub; isinstance narrowing is sufficient at runtime.
            except (TypeError, ValueError, OverflowError) as exc:
                # Why the conversion has its own family: the score element
                # of a lying withscores pair is converted BEFORE the finite
                # and window checks below can speak - a big int past
                # float's range raises OverflowError and a nested element
                # raises TypeError, and neither is a ValueError, so an
                # unguarded conversion lets the lie out of the peek as a
                # bare crash. Same verdict as every other reply boundary:
                # the store-corrupt sentinel.
                raise RateLimitStoreCorrupt(
                    f"sliding-window log peek read a non-numeric oldest score: {oldest!r}"
                ) from exc
            if not math.isfinite(oldest_score) or not (cutoff_ms < oldest_score <= now_ms):
                # The member was selected inside the window by the same
                # store clock this function just read, so its score is
                # within (cutoff, now]; a score outside is a lie (and a
                # huge one was a timedelta OverflowError crash before
                # this boundary existed).
                raise RateLimitStoreCorrupt(
                    f"sliding-window log peek read a score outside its own window: {oldest!r}"
                )
            retry_ms = int(oldest_score) + window_ms - now_ms
            retry_after = timedelta(milliseconds=max(1, retry_ms))

    return RateLimitState(
        bucket_name=self._name,
        backend="redis",
        is_exhausted=is_exhausted,
        remaining=float(max(0, self._limit - count)),
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="log",
    )


async def _peek_redis_gcra(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    """Read-only GCRA snapshot, measured against the store's clock
    (``TIME``), the same domain the acquire script advances the TAT in."""
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"
    window_ms = int(self._window.total_seconds() * 1000)
    emission_interval_ms = window_ms / self._limit
    delay_tolerance_ms = window_ms

    now_ms = await redis_time_seconds(redis_client) * 1000
    if not math.isfinite(now_ms):
        # Why the DERIVED value gets its own guard: the seconds fields
        # can each pass the clock read's finiteness check yet overflow
        # the millisecond derivation (1e306 * 1000 = inf, silently).
        # With an honest-shaped TAT that inf makes ``tat = max(tat, inf)
        # = inf`` and ``tat - now_ms = nan`` (``inf - inf``), so the
        # remaining estimate's ``int(nan)`` crashed with ValueError (and
        # ``int(-inf)`` with OverflowError) - crash classes this
        # boundary exists to kill. A clock no honest store can report is
        # the sentinel, the same verdict as a malformed TIME reply.
        raise RateLimitStoreCorrupt(
            f"sliding-window GCRA peek read a non-finite store clock (TIME): {now_ms!r}"
        )
    tat_raw = await redis_client.get(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py get return type is untyped in the stub
    try:
        tat = float(tat_raw) if tat_raw else now_ms
    except (TypeError, ValueError, OverflowError) as exc:
        # A TAT value no honest SET could have written (the script writes
        # Lua tostring() of a number) is a store lie, failed closed as
        # the outage it is indistinguishable from, never a ValueError
        # crash with no provenance. OverflowError joins the family:
        # float(10**400) raises it, a sibling of neither TypeError nor
        # ValueError, and an uncaught one is the original crash class
        # the boundary exists to kill.
        raise RateLimitStoreCorrupt(
            f"sliding-window GCRA peek read a non-numeric TAT: {tat_raw!r}"
        ) from exc
    if not math.isfinite(tat):
        raise RateLimitStoreCorrupt(f"sliding-window GCRA peek read a non-finite TAT: {tat_raw!r}")
    tat = max(tat, now_ms)

    try:
        remaining = float(max(0, int((delay_tolerance_ms - (tat - now_ms)) / emission_interval_ms)))
    except (TypeError, ValueError, OverflowError) as exc:
        # Why the arithmetic conversion joins the sentinel family: the
        # clock and TAT guards bound each input to finite, but a finite
        # TAT against a finite extreme clock can still overflow the
        # difference to +-inf (and inf - inf is nan), and int(nan) is a
        # ValueError while int(+-inf) is an OverflowError - siblings of
        # neither TypeError nor ValueError, the crash classes this
        # boundary exists to kill. The clock guard above kills the
        # reachable lie; this catch is the residual-arm defence.
        raise RateLimitStoreCorrupt(
            f"sliding-window GCRA peek derived a non-numeric remaining estimate: "
            f"tat={tat!r}, now_ms={now_ms!r}"
        ) from exc
    is_exhausted = remaining <= 0
    retry_after: timedelta | None = None
    if is_exhausted:
        new_tat = tat + emission_interval_ms
        allow_at = new_tat - delay_tolerance_ms
        # Why the hint goes through token_bucket._retry_after: the TAT
        # validation above admits any finite float, and a lying GET
        # answering b"1e300" passes every numeric check here yet
        # overflows timedelta(milliseconds=...) at this arm (an
        # OverflowError out of the peek). The hint is advisory, so a
        # value no honest store can ask for is clamped to the same
        # bounded "not any time soon" hint the token bucket uses; the
        # denial itself stands (is_exhausted), only the wait is bounded.
        retry_after = _retry_after((allow_at - now_ms) / 1000.0)

    return RateLimitState(
        bucket_name=self._name,
        backend="redis",
        is_exhausted=is_exhausted,
        remaining=remaining,
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="gcra",
    )


async def _reset_redis_log(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"
    await redis_client.delete(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py delete return type is untyped in the stub


async def _reset_redis_gcra(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"
    await redis_client.delete(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py delete return type is untyped in the stub


async def _refund_redis_log(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.request_id is None:
        raise ValueError(
            "log-style refund requires decision.request_id for ZREM; "
            "got None, was this decision from a non-log acquire path?"
        )
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend refund")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend refund")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"

    await redis_client.zrem(key, decision.request_id)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py zrem() return type is untyped in the stub


async def _refund_redis_gcra(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.previous_state is None:
        return
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis gcra refund")
    if settings is None:
        raise RuntimeError("settings not injected for redis gcra refund")

    script = await _ensure_gcra_refund_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"

    pre_acquire_tat_str = str(decision.previous_state["pre_acquire_tat_str"])  # type: ignore[arg-type]  # Why: dict[str, object] value is str at runtime; type narrowing not possible from generic dict
    post_acquire_tat_str = str(decision.previous_state["post_acquire_tat_str"])  # type: ignore[arg-type]  # Why: dict[str, object] value is str at runtime; type narrowing not possible from generic dict
    ttl_ms = int(decision.previous_state["ttl_ms"])  # type: ignore[arg-type]  # Why: dict[str, object] value is int at runtime; type narrowing not possible from generic dict

    argv: list[int | str] = [pre_acquire_tat_str, post_acquire_tat_str, ttl_ms]
    await script(keys=[key], args=argv)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation; refund return value is not consumed

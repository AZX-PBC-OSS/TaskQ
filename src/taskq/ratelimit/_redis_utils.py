"""Shared helpers for Redis rate-limit primitives: PG fallback, script caching,
and the store-clock read."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

import structlog

from taskq.constants import (
    RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS,
    RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S,
)
from taskq.exceptions import RateLimitStoreCorrupt
from taskq.ratelimit.decision import RateLimitDecision

if TYPE_CHECKING:
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit._redis_utils")

__all__ = [
    "ensure_redis_script",
    "redis_time_seconds",
    "with_pg_fallback",
]


async def redis_time_seconds(redis_client: "redis_async.Redis") -> float:
    """Read the store's clock via ``TIME``, the domain the Lua scripts stamp.

    Used by the non-script peek paths so their elapsed/refill estimates run
    in the same clock domain as the admission state they read.

    The reply is validated at this trust boundary: a lying proxy answering
    a malformed tuple (wrong arity, non-numeric fields) must not crash the
    caller with a ``ValueError``/``TypeError``/``IndexError`` that no
    fallback or dependency-failure handler recognises. A corrupt reply
    raises :class:`RateLimitStoreCorrupt`, the same class as a store that
    cannot answer, so every downstream path treats it as the outage it
    indistinguishable from.
    """
    t_raw = cast("object", await redis_client.time())  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py time() return type is untyped in the stub; returns (seconds, microseconds). The cast to object is the trust boundary itself: the stub's tuple[int, int] describes what an HONEST store sends, and a lying proxy's reply must reach the shape check un-narrowed.
    if not isinstance(t_raw, (list, tuple)):
        raise RateLimitStoreCorrupt(
            f"redis TIME reply violates the (seconds, microseconds) contract: {t_raw!r}"
        )
    t_seq = cast("list[object]", t_raw)
    if len(t_seq) != 2:
        raise RateLimitStoreCorrupt(
            f"redis TIME reply violates the (seconds, microseconds) contract: {t_raw!r}"
        )
    t_seq = cast("list[object]", t_raw)
    try:
        seconds = float(t_seq[0])  # pyright: ignore[reportArgumentType]  # Why: the element is object after the shape check; float() accepts int | str | bytes at runtime
        micros = float(t_seq[1])  # pyright: ignore[reportArgumentType]  # Why: same object element boundary
    except (TypeError, ValueError, OverflowError) as exc:
        # OverflowError joins the family: int(float("inf")) and
        # float(10**400) raise it, a sibling of neither TypeError nor
        # ValueError, and an uncaught one is the original crash class
        # the boundary exists to kill.
        raise RateLimitStoreCorrupt(f"redis TIME reply is not numeric: {t_raw!r}") from exc
    if not (math.isfinite(seconds) and math.isfinite(micros)):
        # nan/inf: a real clock is a finite number, a proxy answering
        # either is lying.
        raise RateLimitStoreCorrupt(f"redis TIME reply is not finite: {t_raw!r}")
    derived = seconds + micros / 1_000_000
    if not math.isfinite(derived):
        # Why the DERIVED sum gets its own check: both fields finite does
        # not bound the sum - seconds at 1.797e308 plus a micros field
        # that survives the division overflow it to inf SILENTLY (float
        # addition never raises), and the consumers' millisecond
        # arithmetic runs on the lie. A clock no honest store can report
        # is the same verdict as a malformed one.
        raise RateLimitStoreCorrupt(f"redis TIME reply is not finite: {t_raw!r}")
    return derived


async def with_pg_fallback(
    redis_call: Callable[[], Awaitable[RateLimitDecision]],
    pg_call: Callable[[], Awaitable[RateLimitDecision]],
    *,
    bucket_name: str,
    settings: "WorkerSettings | None",
    style: str | None = None,
) -> RateLimitDecision:
    """Try a Redis acquire; on ConnectionError/TimeoutError and the
    ResponseError siblings that mean "this server cannot serve right now",
    fall back to PG.

    The connection family (``redis.ConnectionError``/``redis.TimeoutError``,
    the error reaching the SERVER) first gets a BOUNDED transient retry,
    :data:`RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS` attempts with the
    short backoffs of :data:`RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S`,
    because a sub-second connection blip (container co-tenancy, a proxy
    restart) is weather: without the retry the same blip took the SAME
    path as a persistent outage, straight to the fail-closed decision,
    and a redis-only deployment denied a legitimate request the store
    would have served a quarter-second later. THE LINE: weather gets a
    bounded retry, lies get the sentinel.

    The extra siblings are pinned exactly (checked against redis-py 8.1.0's
    exception hierarchy): :class:`redis.ReadOnlyError` (a replica promoted
    mid-flight answers writes with READONLY) and
    :class:`redis.OutOfMemoryError` (a maxmemory breach) are both direct
    ``ResponseError`` subclasses and both mean the store cannot serve - the
    same outage class as a connection failure, but NOT blips (a promoted
    replica does not demote back in a second, a maxmemory breach does not
    clear itself), so they skip the retry arm entirely. They must be named
    individually rather than catching the parent ``ResponseError``:
    :class:`redis.exceptions.NoScriptError` is also a ``ResponseError``
    sibling, and redis-py handles it client-side (``Script.__call__``
    re-EVALs after a fresh SCRIPT LOAD), so it never signals a store outage.

    :class:`RateLimitStoreCorrupt` is named beside them: it is TaskQ's own
    trust-boundary verdict that the reply the store DID send is a lie
    (out of the reply's type or range domain), which is indistinguishable
    from a store that cannot serve and gets the same treatment, admission
    re-run against the durable PG row. It subclasses
    :class:`RateLimitDependencyUnavailable` (so the acquire boundary's
    dependency-failure family recognises it when no fallback is wired)
    and is deliberately NOT a redis ``ResponseError`` sibling: it marks
    TaskQ's verdict, not redis's. A lie is NEVER retried: the reply
    arrived and is wrong, re-asking the liar buys nothing, so the
    store-lie shapes fail closed on FIRST SIGHT while the connection
    family weathers its bounded budget.

    The WARNING log is emitted **before** delegating to the PG path so that
    if the PG path also emits an INFO denial log, the WARNING precedes the
    INFO in the captured stream. Each transient retry logs its own
    ``rate-limit-redis-transient-retry`` WARNING before the backoff sleep.

    Raises :class:`ImportError` if the ``[redis]`` extra is not installed
    and the caller somehow reaches this path (should not happen when
    ``register_redis_pool`` is used, which silently skips when redis is
    absent).
    """
    try:
        import redis as _redis_mod
        from redis.exceptions import OutOfMemoryError as _RedisOutOfMemoryError
        from redis.exceptions import ReadOnlyError as _RedisReadOnlyError
    except ImportError as exc:
        raise ImportError(
            "taskq[redis] is required to use a Redis-backed rate limiter. "
            "Install it with: pip install 'taskq[redis]'"
        ) from exc

    async def _fail_closed(exc: Exception) -> RateLimitDecision:
        """The dependency-unavailable decision, the shape the retry and
        the no-retry families both funnel into: fall back to PG when the
        fallback is wired, re-raise when it is not."""
        if settings is None or not settings.rate_limit_pg_fallback_enabled:
            raise
        log_kwargs: dict[str, object] = {
            "bucket_name": bucket_name,
            "backend": "redis",
            "fallback": "postgres",
            "error": str(exc),
        }
        if style is not None:
            log_kwargs["style"] = style
        logger.warning("rate-limit-redis-fallback", **log_kwargs)
        return await pg_call()

    # *redis_call* is a FACTORY (not an awaited coroutine) exactly so the
    # transient-retry arm below can re-invoke it; the acquire it wraps is
    # idempotent to re-enter (the script cache is, see ensure_redis_script).
    transient_retries_left = RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS - 1
    while True:
        try:
            return await redis_call()
        except (
            _redis_mod.ConnectionError,
            _redis_mod.TimeoutError,
        ) as exc:
            # A CANCELLATION is never weather. redis's read wraps its wait
            # in async_timeout, which converts a task cancellation arriving
            # mid-read into this same TimeoutError (the CancelledError at
            # the stream reader becomes the timeout at the context's exit);
            # without this guard the retry loop re-arms against a task the
            # caller already gave up on, and the cancellation surfaces
            # later, mid-backoff, mislabelled. cancelling() is nonzero
            # exactly when a cancellation was DELIVERED to this task; a
            # genuine socket read timeout never sets it.
            current_task = asyncio.current_task()
            if current_task is None or current_task.cancelling():
                raise
            # The WEATHER family: the connection to the server failed. A
            # blip is allowed one bounded weathering before the decision.
            if transient_retries_left > 0:
                backoff_s = RATE_LIMIT_REDIS_TRANSIENT_RETRY_BACKOFFS_S[
                    RATE_LIMIT_REDIS_TRANSIENT_RETRY_ATTEMPTS - 1 - transient_retries_left
                ]
                transient_retries_left -= 1
                logger.warning(
                    "rate-limit-redis-transient-retry",
                    bucket_name=bucket_name,
                    backend="redis",
                    error=str(exc),
                    backoff_s=backoff_s,
                    **({"style": style} if style is not None else {}),
                )
                await asyncio.sleep(backoff_s)
                continue
            # The budget is spent: the blip was an outage, the decision.
            return await _fail_closed(exc)
        except (
            _RedisReadOnlyError,
            _RedisOutOfMemoryError,
            RateLimitStoreCorrupt,
        ) as exc:
            # The CANNOT-SERVE and LIE families: fail closed on first
            # sight, no retry (see the docstring for why neither is a
            # blip).
            return await _fail_closed(exc)


async def ensure_redis_script(
    get: Callable[[], "AsyncScript | None"],
    set: Callable[["AsyncScript"], None],
    register: Callable[[], "AsyncScript"],
    lock: asyncio.Lock,
) -> "AsyncScript":
    """Lazily register a Lua script exactly once per client, using
    double-checked locking.

    A registered ``AsyncScript`` is bound to the client instance it was
    registered on, and a client is replaceable (the worker reload path
    rebuilds ``deps.redis_client``; a loop restart re-resolves the DI
    value).  The cache the caller's *get*/*set* closures manage MUST
    therefore be keyed by client identity: *get* returns the cached
    script only when it was registered on the client in use now, and
    *set* records that binding.  A cache keyed on nothing reuses a dead
    client's script against a live client forever, so a store that
    healed through a client swap never heals.
    """
    existing = get()
    if existing is not None:
        return existing
    async with lock:
        existing = get()
        if existing is not None:
            return existing
        script = register()
        set(script)
        return script

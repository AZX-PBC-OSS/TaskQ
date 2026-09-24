"""Shared helpers for Redis rate-limit primitives: PG fallback, script caching,
and the store-clock read."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

import structlog

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
    redis_call: Awaitable[RateLimitDecision],
    pg_call: Callable[[], Awaitable[RateLimitDecision]],
    *,
    bucket_name: str,
    settings: "WorkerSettings | None",
    style: str | None = None,
) -> RateLimitDecision:
    """Try a Redis acquire; on ConnectionError/TimeoutError and the
    ResponseError siblings that mean "this server cannot serve right now",
    fall back to PG.

    The extra siblings are pinned exactly (checked against redis-py 8.1.0's
    exception hierarchy): :class:`redis.ReadOnlyError` (a replica promoted
    mid-flight answers writes with READONLY) and
    :class:`redis.OutOfMemoryError` (a maxmemory breach) are both direct
    ``ResponseError`` subclasses and both mean the store cannot serve - the
    same outage class as a connection failure. They must be named
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
    TaskQ's verdict, not redis's.

    The WARNING log is emitted **before** delegating to the PG path so that
    if the PG path also emits an INFO denial log, the WARNING precedes the
    INFO in the captured stream.

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

    try:
        return await redis_call
    except (
        _redis_mod.ConnectionError,
        _redis_mod.TimeoutError,
        _RedisReadOnlyError,
        _RedisOutOfMemoryError,
        RateLimitStoreCorrupt,
    ) as exc:
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

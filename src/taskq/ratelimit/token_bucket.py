"""Token-bucket rate limiter with pluggable backends.

The in-memory backend is the reference implementation and arithmetic oracle
for the Redis Lua script and the PG fallback.

Design deviation — always invoke the Lua script instead of a Python
pre-check ``if self.refill_per_second == 0 and tokens < count: return ...``
before invoking the script. We deviate by always invoking the Lua script
and post-processing the result.
- Why: the pre-check requires local knowledge of ``tokens``.
  The in-memory backend has that knowledge in ``_InMemoryBucket._tokens``
  and implements the pre-check. The Redis backend does NOT
  have local knowledge of ``tokens`` — that state lives in the Redis
  hash and is computed inside the Lua script using ``elapsed * refill``.
  The guard cannot be applied verbatim on the Redis path
  without first issuing an HMGET to read ``tokens``, which would change
  the protocol from "one Lua call" to "HMGET then Lua call." Rather
  than introduce that round-trip, we let the Lua script run
  unconditionally and substitute ``None`` in Python when the denial
  branch produces a ``nan``/``inf`` ``retry_after_seconds``.
- What we do instead: invoke the Lua script unconditionally; in the
  denial branch with ``refill = 0`` the script's ``retry_after_seconds``
  is ``nan``/``inf`` (division by zero), but the script's ``tokens_remaining``
  (result index 1) IS still valid because the denial branch reports
  the current token count without modification. We discard the Lua
  ``retry_after_seconds`` and substitute ``None`` when
  ``allowed_int == 0 and self.refill_per_second == 0``.
- Reversibility: fully reversible. Switching to the
  pre-check is a one-method change (add ``_pre_check_redis()`` issuing
  HMGET, branch before ``register_script`` call). No persistent state
  or external contract relies on the deviation.

This file exceeds the 500-line soft ceiling (file-size
decomposition). It co-locates three concern-clusters — (a) the
``_InMemoryBucket`` state machine, (b) Lua-result decoding and the Redis
acquire path, and (c) the PG acquire path — all of which serve the single
token-bucket primitive. Splitting would move the shared ``RateLimitDecision``
return contract, the ``capacity``/``refill_per_second`` constructor validation,
and the ``acquire`` dispatch logic into a fourth module, creating an inner
platform where every backend module re-imports from a thin orchestrator that
exists only to satisfy a line-count rule. The three paths share the same
arithmetic, the same time-domain rule (the store owns the clock: Redis ``TIME``
in the script, ``EXTRACT(EPOCH FROM clock_timestamp())`` on PG; the injected
``Clock`` drives only the memory backend, its single domain), and the same
logging discipline; co-location keeps the arithmetic consistent and the
dispatch logic visible end-to-end without indirection.
"""

import asyncio
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Final

import structlog

from taskq.backend._protocol import RateLimitBackend
from taskq.backend._records import jsonb_param, jsonb_to_dict
from taskq.backend.clock import Clock
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._redis_utils import ensure_redis_script, redis_time_seconds, with_pg_fallback
from taskq.ratelimit._scripts import REFUND_SCRIPT, TOKEN_BUCKET_SCRIPT
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit.token_bucket")

_DEFAULT_FIXED_QUOTA_TTL: Final[timedelta] = timedelta(seconds=86400)

#: Ceiling for the *derived* default TTL.
#:
#: The default is "twice the time to refill from empty, plus a minute", which
#: is unbounded as ``refill_per_second`` approaches zero: at 1e-15 tokens/s it
#: is 2e16 seconds, past what ``timedelta`` accepts, and a denormal rate makes
#: ``capacity / refill`` overflow to ``inf`` before ``timedelta`` is even
#: reached. Either raised ``OverflowError`` straight out of the constructor,
#: from a rate the constructor's own contract (``refill_per_second >= 0``)
#: accepts and the admission arithmetic handles exactly.
#:
#: Clamping rather than rejecting: the TTL is a storage-eviction hint that the
#: token math never reads, so bounding it changes nothing observable about
#: rate limiting, whereas rejecting a legal positive rate would break an
#: operator configuring a genuinely slow trickle (a monthly quota, a value
#: from config). A year is far more conservative than the policy this module
#: already applies to the extreme case: a bucket with ``refill == 0`` can
#: NEVER recover its state and is nonetheless given 24 h. A bucket that takes
#: longer than a year to refill is a fixed quota over any operational horizon.
_MAX_TTL: Final[timedelta] = timedelta(days=365)


def _retry_after(seconds: float) -> timedelta:
    """Bound an advisory retry hint into a representable ``timedelta``.

    Same unbounded quantity as the default TTL, on the hot path: the wait for
    a deficit of tokens is ``deficit / refill_per_second``, so a very slow
    refill produces a number ``timedelta`` cannot hold (and a denormal rate
    produces ``inf`` outright). Every denial went through this conversion, so
    an out-of-range rate crashed the acquire itself, not just construction.
    The Redis path can additionally receive ``inf``/``nan`` from the Lua
    script's own division.

    The value is advisory — a hint for how long to back off — so clamping it
    to :data:`_MAX_TTL` loses nothing: no caller sleeps for a year, and every
    such caller is being told the same thing either way ("not any time soon").
    Negative and non-finite inputs collapse to a valid bound rather than
    propagating out of the primitive.
    """
    if not math.isfinite(seconds):
        return _MAX_TTL
    if seconds <= 0.0:
        return timedelta(0)
    # Why the comparison happens in seconds: timedelta(seconds=...) raises on
    # an out-of-range float, so clamping the timedelta afterwards is too late.
    if seconds >= _MAX_TTL.total_seconds():
        return _MAX_TTL
    return timedelta(seconds=seconds)


def _default_ttl(capacity: float, refill_per_second: float) -> timedelta:
    """Derive the default key TTL, bounded by :data:`_MAX_TTL`."""
    if refill_per_second == 0.0:
        return _DEFAULT_FIXED_QUOTA_TTL
    seconds = capacity / refill_per_second * 2 + 60
    if not math.isfinite(seconds) or seconds >= _MAX_TTL.total_seconds():
        return _MAX_TTL
    return timedelta(seconds=math.ceil(seconds))


@dataclass(frozen=True, slots=True)
class _LuaResult:
    """Typed decoding of the three-element list returned by the token-bucket Lua script.

    The Lua script returns ``{allowed, tokens_remaining,
    retry_after_seconds}`` where ``allowed`` is 0 or 1 (Lua integer) and
    ``tokens_remaining`` / ``retry_after_seconds`` are Lua number strings
    (``tostring()``). Redis truncates Lua numbers to integers on return;
    returning floats as strings preserves the fractional part (see Redis
    EVAL docs: "Lua number → RESP2 integer reply — removing the decimal
    part of the number, if any"). This helper normalises them to Python
    types in one place so the rest of the Redis path stays fully typed.
    """

    allowed: bool
    tokens_remaining: float
    retry_after_seconds: float


def _decode_lua_result(raw: list[object]) -> _LuaResult:
    """Decode the raw Redis response from the token-bucket Lua script.

    This function is the **only** place where redis-py's untyped
    ``AsyncScript.__call__`` return touches our code. ``raw[0]`` is an
    integer (allowed: 0 or 1). ``raw[1]`` and ``raw[2]`` are strings
    (bytes or str depending on ``decode_responses``) produced by Lua's
    ``tostring()`` — this is required because Redis truncates Lua number
    returns to integers, losing fractional parts. ``int()`` / ``float()``
    accept bytes, int, and str at runtime.
    """
    allowed_int = int(raw[0])  # pyright: ignore[reportArgumentType]  # Why: raw[0] is int | bytes from Redis; int() accepts both at runtime but pyright cannot model AsyncScript's untyped return
    tokens_remaining = float(raw[1])  # pyright: ignore[reportArgumentType]  # Why: raw[1] is bytes | str from Redis (Lua tostring); float() accepts both
    retry_after_seconds = float(raw[2])  # pyright: ignore[reportArgumentType]  # Why: raw[2] is bytes | str from Redis (Lua tostring); float() accepts both
    return _LuaResult(
        allowed=allowed_int == 1,
        tokens_remaining=tokens_remaining,
        retry_after_seconds=retry_after_seconds,
    )


class _InMemoryBucket:
    """Per-bucket state for the in-memory token-bucket algorithm."""

    __slots__ = ("_capacity", "_lock", "_name", "_refill", "_tokens", "_ts")

    def __init__(self, name: str, capacity: float, refill_per_second: float) -> None:
        self._name = name
        self._capacity = capacity
        self._refill = refill_per_second
        self._tokens: float = capacity
        self._ts: float | None = None
        self._lock = asyncio.Lock()

    async def acquire(self, count: float, now_ts: float) -> RateLimitDecision:
        async with self._lock:
            if self._ts is None:
                self._ts = now_ts

            elapsed = max(0.0, now_ts - self._ts)
            tokens = min(self._capacity, self._tokens + elapsed * self._refill)

            if tokens >= count:
                tokens -= count
                self._tokens = tokens
                self._ts = now_ts
                return RateLimitDecision(
                    allowed=True,
                    remaining=tokens,
                    retry_after=timedelta(0),
                    bucket_name=self._name,
                    backend="memory",
                )

            self._tokens = tokens
            self._ts = now_ts

            if self._refill == 0.0:
                return RateLimitDecision(
                    allowed=False,
                    remaining=tokens,
                    retry_after=None,
                    bucket_name=self._name,
                    backend="memory",
                )

            return RateLimitDecision(
                allowed=False,
                remaining=tokens,
                retry_after=_retry_after((count - tokens) / self._refill),
                bucket_name=self._name,
                backend="memory",
            )

    async def refund(self, count: float) -> None:
        async with self._lock:
            self._tokens = min(self._capacity, self._tokens + count)

    async def peek(self, now_ts: float) -> RateLimitState:
        async with self._lock:
            if self._ts is None:
                tokens = self._capacity
            else:
                elapsed = max(0.0, now_ts - self._ts)
                tokens = min(self._capacity, self._tokens + elapsed * self._refill)

            is_exhausted = tokens <= 0.0
            retry_after: timedelta | None = None
            if is_exhausted and self._refill > 0.0:
                retry_after = _retry_after((1.0 - tokens) / self._refill)

            return RateLimitState(
                bucket_name=self._name,
                backend="memory",
                is_exhausted=is_exhausted,
                tokens_remaining=tokens,
                retry_after=retry_after,
                capacity=self._capacity,
                refill_per_second=self._refill,
            )

    async def reset(self, now_ts: float) -> None:
        async with self._lock:
            self._tokens = self._capacity
            self._ts = now_ts


class TokenBucket:
    """Token-bucket rate limiter with pluggable backends.

    Raises :class:`ValueError` if ``capacity <= 0`` or
    ``refill_per_second < 0``.
    """

    __slots__ = (
        "_backend",
        "_capacity",
        "_mem_bucket",
        "_name",
        "_redis_refund_script",
        "_redis_script",
        "_refill",
        "_script_lock",
        "_ttl",
    )

    def __init__(
        self,
        name: str,
        capacity: float,
        refill_per_second: float,
        backend: RateLimitBackend = "redis",
        ttl: timedelta | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {capacity}")
        if refill_per_second < 0:
            raise ValueError(f"refill_per_second must be >= 0, got {refill_per_second}")

        self._name = name
        self._capacity = capacity
        self._refill = refill_per_second
        self._backend: RateLimitBackend = backend

        self._ttl = ttl if ttl is not None else _default_ttl(capacity, refill_per_second)

        self._mem_bucket: _InMemoryBucket | None = None
        if backend == "memory":
            self._mem_bucket = _InMemoryBucket(name, capacity, refill_per_second)

        self._redis_script: AsyncScript | None = None
        self._redis_refund_script: AsyncScript | None = None
        self._script_lock: asyncio.Lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def refill_per_second(self) -> float:
        return self._refill

    @property
    def backend(self) -> RateLimitBackend:
        return self._backend

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    def holds_consumed_memory_quota(self) -> bool:
        """True if idle-evicting this bucket's registry entry would silently reset a consumed fixed quota.

        Only the memory backend stores token state on the instance
        (``_mem_bucket``); Redis and PG keep state in the store — Redis
        deliberately for 24 h for fixed-quota buckets (see
        ``_compute_ttl_seconds``) — so a re-materialized bucket seamlessly
        resumes prior state there, and eviction of those backends' registry
        entries is always state-safe.

        For a memory fixed-quota (``refill_per_second == 0``) bucket that
        has consumed any of its quota, eviction destroys that state
        permanently: the next acquire materializes a fresh instance at FULL
        capacity, silently resetting a quota designed to never refill.
        (Refilling buckets are not exempt: their state converges back
        toward full on its own, so eviction loses at most one refill
        window's worth of tokens — an accepted, bounded divergence.)

        Reads ``_tokens`` without the bucket's async lock; safe because the
        only caller (the registry's idle-eviction sweep) runs synchronously
        in the event loop with no await between this read and the dict pop,
        so the value is consistent at the sweep instant.
        """
        return (
            self._backend == "memory"
            and self._refill == 0.0
            and self._mem_bucket is not None
            and self._mem_bucket._tokens < self._capacity
        )

    async def acquire(
        self,
        count: float = 1.0,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> RateLimitDecision:
        if self._backend == "memory":
            return await self._acquire_memory(count, clock)
        if self._backend == "redis":
            return await self._acquire_redis_wrapped(count, redis_client, pg_pool, settings)
        if self._backend == "postgres":
            return await self._acquire_pg(count, pg_pool, settings)

        raise RuntimeError(f"unknown backend: {self._backend!r}")

    async def refund(
        self,
        decision: RateLimitDecision,
        *,
        count: float = 1.0,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> None:
        # Why decision and clock are unused: a token-bucket refund returns
        # *count* tokens to the bucket, so it needs neither the original
        # decision (SlidingWindow.refund does — it removes the log entry
        # named by that decision's request id) nor a timestamp. Both stay
        # in the signature because
        # RateLimitRegistry dispatches refund/peek/reset polymorphically
        # over TokenBucket and SlidingWindow with one fixed keyword block
        # (redis_client, pg_pool, clock, settings) — see
        # registry.reset_limit's call sites. Dropping it here would raise
        # TypeError there, not merely break symmetry.
        if self._backend == "memory":
            await self._refund_memory(count)
        elif self._backend == "redis":
            await self._refund_redis(count, redis_client, settings)
        elif self._backend == "postgres":
            await self._refund_pg(count, pg_pool, settings)

    async def peek(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> RateLimitState:
        if self._backend == "memory":
            return await self._peek_memory(clock)
        if self._backend == "redis":
            return await self._peek_redis(redis_client, settings)
        if self._backend == "postgres":
            return await self._peek_pg(pg_pool, settings)

        raise RuntimeError(f"unknown backend: {self._backend!r}")

    async def reset(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> None:
        if self._backend == "memory":
            await self._reset_memory(clock)
        elif self._backend == "redis":
            await self._reset_redis(redis_client, settings)
        elif self._backend == "postgres":
            await self._reset_pg(pg_pool, settings)
        else:
            raise RuntimeError(f"unknown backend: {self._backend!r}")

        logger.warning(
            "ratelimit-reset",
            bucket_name=self._name,
            backend=self._backend,
        )

    async def _peek_memory(self, clock: Clock | None) -> RateLimitState:
        if clock is None:
            raise RuntimeError("clock not injected for memory backend")
        if self._mem_bucket is None:
            raise RuntimeError("memory bucket not initialised")
        now_ts = clock.now().timestamp()
        return await self._mem_bucket.peek(now_ts)

    async def _reset_memory(self, clock: Clock | None) -> None:
        if clock is None:
            raise RuntimeError("clock not injected for memory backend")
        if self._mem_bucket is None:
            raise RuntimeError("memory bucket not initialised")
        now_ts = clock.now().timestamp()
        await self._mem_bucket.reset(now_ts)

    async def _peek_redis(
        self,
        redis_client: "redis_async.Redis | None",
        settings: "WorkerSettings | None",
    ) -> RateLimitState:
        """Read-only Redis state snapshot — the elapsed-refill estimate runs
        on the store's clock (``TIME``), the same domain the acquire script
        stamps ``ts`` in."""
        if redis_client is None:
            raise RuntimeError("redis_client not injected for redis backend")
        if settings is None:
            raise RuntimeError("settings not injected for redis backend")

        schema_name = settings.schema_name
        key = f"taskq:{schema_name}:rl:tb:{{{self._name}}}"
        now_seconds = await redis_time_seconds(redis_client)

        raw = await redis_client.hmget(key, ["tokens", "ts"])  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportGeneralTypeIssues]  # Why: redis-py hmget return type is untyped in the stub; all operations reflect correct runtime behavior.

        tokens_raw = raw[0] if raw else None  # pyright: ignore[reportUnknownVariableType]  # Why: raw is untyped from redis-py hmget stub; validated at runtime.
        ts_raw = raw[1] if raw else None  # pyright: ignore[reportUnknownVariableType]  # Why: raw is untyped from redis-py hmget stub; validated at runtime.

        tokens = self._capacity if tokens_raw is None else float(tokens_raw)  # pyright: ignore[reportUnknownArgumentType]  # Why: tokens_raw type is unknown due to untyped redis-py stub; validated at runtime.
        ts = now_seconds if ts_raw is None else float(ts_raw)  # pyright: ignore[reportUnknownArgumentType]  # Why: ts_raw type is unknown due to untyped redis-py stub; validated at runtime.

        elapsed = max(0.0, now_seconds - ts)
        tokens = min(self._capacity, tokens + elapsed * self._refill)

        is_exhausted = tokens <= 0.0
        retry_after: timedelta | None = None
        if is_exhausted and self._refill > 0.0:
            retry_after = _retry_after((1.0 - tokens) / self._refill)

        return RateLimitState(
            bucket_name=self._name,
            backend="redis",
            is_exhausted=is_exhausted,
            tokens_remaining=tokens,
            retry_after=retry_after,
            capacity=self._capacity,
            refill_per_second=self._refill,
        )

    async def _reset_redis(
        self,
        redis_client: "redis_async.Redis | None",
        settings: "WorkerSettings | None",
    ) -> None:
        if redis_client is None:
            raise RuntimeError("redis_client not injected for redis backend")
        if settings is None:
            raise RuntimeError("settings not injected for redis backend")

        schema_name = settings.schema_name
        key = f"taskq:{schema_name}:rl:tb:{{{self._name}}}"
        await redis_client.delete(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py delete return type is untyped in the stub

    async def _peek_pg(
        self,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> RateLimitState:
        """Read-only PG state snapshot — the elapsed-refill estimate runs on
        the server epoch returned alongside the state (same domain the
        acquire path stamps)."""
        if pg_pool is None:
            raise RuntimeError("pg_pool not injected for postgres backend")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend")

        schema = settings.schema_name

        select_sql = (
            f"SELECT state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
            f'FROM "{schema}".rate_limit_buckets WHERE bucket_name=$1'
        )

        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(select_sql, self._name)

        if row is None:
            tokens = self._capacity
        else:
            now = float(row["now_s"])
            state = jsonb_to_dict(row["state"])
            tokens = float(state.get("tokens", self._capacity))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback for rows missing keys (e.g. from schema migrations or interop writes)
            ts = float(state.get("ts", now))  # type: ignore[index]  # Why: same — state is non-None; fallback to now for rows missing "ts"
            elapsed = max(0.0, now - ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill)

        is_exhausted = tokens <= 0.0
        retry_after: timedelta | None = None
        if is_exhausted and self._refill > 0.0:
            retry_after = _retry_after((1.0 - tokens) / self._refill)

        return RateLimitState(
            bucket_name=self._name,
            backend="postgres",
            is_exhausted=is_exhausted,
            tokens_remaining=tokens,
            retry_after=retry_after,
            capacity=self._capacity,
            refill_per_second=self._refill,
        )

    async def _reset_pg(
        self,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> None:
        if pg_pool is None:
            raise RuntimeError("pg_pool not injected for postgres backend")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend")

        schema = settings.schema_name
        delete_sql = f'DELETE FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1'  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        await pg_pool.execute(delete_sql, self._name)

    async def _refund_memory(self, count: float) -> None:
        if self._mem_bucket is None:
            raise RuntimeError("memory bucket not initialised")
        await self._mem_bucket.refund(count)

    async def _refund_redis(
        self,
        count: float,
        redis_client: "redis_async.Redis | None",
        settings: "WorkerSettings | None",
    ) -> None:
        if redis_client is None:
            raise RuntimeError("redis_client not injected for redis backend refund")
        if settings is None:
            raise RuntimeError("settings not injected for redis backend refund")

        script = await self._ensure_refund_script(redis_client)

        schema_name = settings.schema_name
        key = f"taskq:{schema_name}:rl:tb:{{{self._name}}}"

        argv: list[float] = [count, self._capacity, self._refill]
        await script(keys=[key], args=argv)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation; refund return value is not consumed

    async def _ensure_refund_script(self, redis_client: "redis_async.Redis") -> "AsyncScript":
        return await ensure_redis_script(
            lambda: self._redis_refund_script,
            lambda s: setattr(self, "_redis_refund_script", s),
            lambda: redis_client.register_script(REFUND_SCRIPT),
            self._script_lock,
        )

    async def _refund_pg(
        self,
        count: float,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> None:
        """Refund tokens on the PG backend using FOR UPDATE on rate_limit_buckets.

        Mirrors the Redis refund script: apply the elapsed-refill step so a
        refund landing after idle time does not lose accrued tokens, then add
        ``count`` capped at ``capacity``. If the bucket row does not exist
        (never created or already reset), this is a no-op. The elapsed math
        and the stored ``ts`` are server-domain (``EXTRACT(EPOCH FROM
        clock_timestamp())`` read in the same locked transaction), matching
        the acquire path's stamps.
        """
        if pg_pool is None:
            raise RuntimeError("pg_pool not injected for postgres backend refund")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend refund")

        schema = settings.schema_name

        select_sql = (
            f"SELECT state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
            f'FROM "{schema}".rate_limit_buckets WHERE bucket_name=$1 FOR UPDATE'
        )
        update_sql = f'UPDATE "{schema}".rate_limit_buckets SET state=$1::jsonb, updated_at=clock_timestamp() WHERE bucket_name=$2'  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; values are $1/$2-bound

        async with pg_pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(select_sql, self._name)

            if row is None:
                return

            now = float(row["now_s"])
            state = jsonb_to_dict(row["state"])
            tokens = float(state.get("tokens", self._capacity))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback for rows missing keys (e.g. from schema migrations or interop writes)
            ts = float(state.get("ts", now))  # type: ignore[index]  # Why: same — state is non-None; fallback to now for rows missing "ts"

            elapsed = max(0.0, now - ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill)
            tokens = min(self._capacity, tokens + count)

            state_param = jsonb_param({"tokens": tokens, "ts": now})
            await conn.execute(update_sql, state_param, self._name)

    async def _acquire_memory(self, count: float, clock: Clock | None) -> RateLimitDecision:
        if clock is None:
            raise RuntimeError("clock not injected for memory backend")
        if self._mem_bucket is None:
            raise RuntimeError("memory bucket not initialised")

        now_ts = clock.now().timestamp()
        result = await self._mem_bucket.acquire(count, now_ts)
        log_decision(result)
        return result

    async def _acquire_redis(
        self,
        count: float,
        redis_client: "redis_async.Redis | None",
        settings: "WorkerSettings | None",
    ) -> RateLimitDecision:
        """Redis acquire — the script derives now from ``redis.call('TIME')``
        (store-domain), so no Python clock participates."""
        if redis_client is None:
            raise RuntimeError("redis_client not injected for redis backend")
        if settings is None:
            raise RuntimeError("settings not injected for redis backend")

        script = await self._ensure_script(redis_client)

        schema_name = settings.schema_name
        key = f"taskq:{schema_name}:rl:tb:{{{self._name}}}"

        ttl_seconds = self._compute_ttl_seconds()

        argv: list[float | int] = [
            self._capacity,
            self._refill,
            count,
            ttl_seconds,
        ]

        raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation — pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

        lua = _decode_lua_result(raw)

        retry_after: timedelta | None
        if lua.allowed:
            retry_after = timedelta(0)
        elif self._refill == 0.0:
            retry_after = None
        else:
            retry_after = _retry_after(lua.retry_after_seconds)

        result = RateLimitDecision(
            allowed=lua.allowed,
            remaining=lua.tokens_remaining,
            retry_after=retry_after,
            bucket_name=self._name,
            backend="redis",
        )

        log_decision(result)
        return result

    async def _ensure_script(self, redis_client: "redis_async.Redis") -> "AsyncScript":
        return await ensure_redis_script(
            lambda: self._redis_script,
            lambda s: setattr(self, "_redis_script", s),
            lambda: redis_client.register_script(TOKEN_BUCKET_SCRIPT),
            self._script_lock,
        )

    async def _acquire_redis_wrapped(
        self,
        count: float,
        redis_client: "redis_async.Redis | None",
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> RateLimitDecision:
        """Redis path with optional PG fallback on ConnectionError/TimeoutError."""
        return await with_pg_fallback(
            self._acquire_redis(count, redis_client, settings),
            lambda: self._acquire_pg(count, pg_pool, settings),
            bucket_name=self._name,
            settings=settings,
        )

    async def _acquire_pg(
        self,
        count: float,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> RateLimitDecision:
        """PG fallback path using FOR UPDATE on rate_limit_buckets.

        Runs in a single transaction: SELECT … FOR UPDATE (blocking, NOT SKIP
        LOCKED), compute token arithmetic in Python, upsert the new state.
        The epoch math runs on ``EXTRACT(EPOCH FROM clock_timestamp())``
        read in the same transaction, so the stored ``ts`` is server-domain
        by construction — a node with a skewed Python clock cannot mint
        phantom refill.
        """
        if pg_pool is None:
            raise RuntimeError("pg_pool not injected for postgres backend")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend")

        schema = settings.schema_name

        # Schema-name interpolation ; schema_name is
        # pre-validated against _IDENT_RE at WorkerSettings load time.
        # The preseed stamps ts server-side via jsonb_build_object so the
        # first acquire's elapsed math is server-domain even before the
        # SELECT below folds the epoch in.
        preseed_sql = (
            f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$2-bound
            f"VALUES ($1, 'token_bucket', "
            f"jsonb_build_object('tokens', $2::float8, "
            f"'ts', EXTRACT(EPOCH FROM clock_timestamp())), clock_timestamp()) "
            f"ON CONFLICT (bucket_name) DO NOTHING"
        )
        select_sql = (
            f"SELECT state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
            f'FROM "{schema}".rate_limit_buckets WHERE bucket_name=$1 FOR UPDATE'
        )
        upsert_sql = (
            f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$2-bound
            f"VALUES ($1, 'token_bucket', $2::jsonb, clock_timestamp()) "
            f"ON CONFLICT (bucket_name) DO UPDATE SET state=EXCLUDED.state, updated_at=clock_timestamp()"
        )

        async with pg_pool.acquire() as conn, conn.transaction():
            # Cold-start guard: SELECT ... FOR UPDATE cannot lock a row that
            # does not exist yet, so concurrent first acquires would each
            # read `row is None` and independently admit up to `capacity`
            # tokens. Pre-seed a full-capacity row (idempotent — DO NOTHING
            # on conflict) so the very first acquire also serializes on the
            # row lock below.
            await conn.execute(preseed_sql, self._name, self._capacity)
            row = await conn.fetchrow(select_sql, self._name)

            if row is None:
                # Unreachable in the normal path — the preseed above guarantees
                # the row exists before the SELECT. Kept as a defensive fallback
                # (e.g. a concurrent DELETE between preseed and select).
                now = float(await conn.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))
                tokens = self._capacity
                ts = now
            else:
                now = float(row["now_s"])
                state = jsonb_to_dict(row["state"])
                tokens = float(state.get("tokens", self._capacity))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback for rows missing keys (e.g. from schema migrations or interop writes)
                ts = float(state.get("ts", now))  # type: ignore[index]  # Why: same — state is non-None; fallback to now for rows missing "ts"

            elapsed = max(0.0, now - ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill)

            retry_after: timedelta | None
            allowed: bool

            if tokens >= count:
                tokens -= count
                allowed = True
                retry_after = timedelta(0)
            else:
                allowed = False
                if self._refill == 0.0:
                    retry_after = None
                else:
                    retry_after = _retry_after((count - tokens) / self._refill)

            # _jsonb_param serializes via orjson — passing a dict directly
            # to conn.execute fails because asyncpg does not auto-encode
            # Python dicts as jsonb.
            state_param = jsonb_param({"tokens": tokens, "ts": now})
            # updated_at and the state ts are both server-domain now
            await conn.execute(upsert_sql, self._name, state_param)

        result = RateLimitDecision(
            allowed=allowed,
            remaining=tokens,
            retry_after=retry_after,
            bucket_name=self._name,
            backend="postgres",
        )
        log_decision(result)
        return result

    def _compute_ttl_seconds(self) -> int:
        """The Redis key's EXPIRE, in whole seconds.

        Reads ``self._ttl`` rather than re-deriving from capacity/refill:
        recomputing here silently discarded an explicit ``ttl=`` on the Redis
        path (the memory and PG paths honoured it) and carried its own copy of
        the overflow. ``SlidingWindow`` already derives its Redis TTL from
        ``self._ttl``; the two primitives now agree. Floored at one second
        because EXPIRE 0 deletes the key outright.
        """
        return max(1, int(self._ttl.total_seconds()))

"""Sliding-window rate limiter with pluggable backends.

The log-style and GCRA in-memory backends are reference implementations
and arithmetic oracles for the Redis Lua scripts and the PG fallbacks.

Redis implementations (:mod:`taskq.ratelimit._sliding_window_redis`) and
PG fallback implementations (:mod:`taskq.ratelimit._sliding_window_pg`)
live in companion submodules; this module holds the ``SlidingWindow``
class, the in-memory backends, and the public dispatch surface.
"""

import asyncio
import dataclasses
from collections import deque
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, assert_never
from uuid import UUID

import structlog

from taskq._ids import new_uuid
from taskq.backend._protocol import RateLimitBackend
from taskq.backend.clock import Clock
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._sliding_window_pg import (
    _acquire_pg_gcra,
    _acquire_pg_log,
    _peek_pg_gcra,
    _peek_pg_log,
    _refund_pg_gcra,
    _refund_pg_log,
    _reset_pg_gcra,
    _reset_pg_log,
)
from taskq.ratelimit._sliding_window_redis import (
    _acquire_redis_gcra_wrapped,
    _acquire_redis_log_wrapped,
    _peek_redis_gcra,
    _peek_redis_log,
    _refund_redis_gcra,
    _refund_redis_log,
    _reset_redis_gcra,
    _reset_redis_log,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit.sliding_window")

type SlidingWindowStyle = Literal["log", "gcra"]

_VALID_STYLES: frozenset[str] = frozenset({"log", "gcra"})


class _InMemorySlidingWindowLog:
    """Per-bucket state for the log-style in-memory sliding-window algorithm.

    Mirrors the ``_InMemoryBucket`` shape from ``token_bucket.py``:
    ``collections.deque[int]`` of ``now_ms`` timestamps guarded by an
    ``asyncio.Lock``.  A parallel ``deque[str]`` of ``request_id`` values
    (kept in lockstep with the timestamp deque) enables ``refund()`` to
    remove a specific entry by ``request_id``, mirroring the Redis
    ``ZREM``-by-member semantics.
    """

    __slots__ = ("_deque", "_ids", "_limit", "_lock", "_name", "_window_ms")

    def __init__(self, name: str, limit: int, window_ms: int) -> None:
        self._name = name
        self._limit = limit
        self._window_ms = window_ms
        self._deque: deque[int] = deque()
        self._ids: deque[str] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, now_ms: int, request_id: str) -> RateLimitDecision:
        async with self._lock:
            cutoff = now_ms - self._window_ms
            while self._deque and self._deque[0] <= cutoff:
                self._deque.popleft()
                self._ids.popleft()

            if len(self._deque) >= self._limit:
                oldest = self._deque[0]
                retry_ms = oldest + self._window_ms - now_ms
                return RateLimitDecision(
                    allowed=False,
                    remaining=0.0,
                    retry_after=timedelta(milliseconds=retry_ms),
                    bucket_name=self._name,
                    backend="memory",
                )

            self._deque.append(now_ms)
            self._ids.append(request_id)
            return RateLimitDecision(
                allowed=True,
                remaining=float(self._limit - len(self._deque)),
                retry_after=timedelta(0),
                bucket_name=self._name,
                backend="memory",
            )

    async def refund(self, request_id: str) -> None:
        async with self._lock:
            for i, rid in enumerate(self._ids):
                if rid == request_id:
                    del self._deque[i]
                    del self._ids[i]
                    return

    async def peek(self, now_ms: int) -> RateLimitState:
        async with self._lock:
            cutoff = now_ms - self._window_ms
            count = sum(1 for ts in self._deque if ts > cutoff)
            is_exhausted = count >= self._limit
            retry_after: timedelta | None = None
            if is_exhausted and self._deque:
                oldest_in_window: int | None = None
                for ts in self._deque:
                    if ts > cutoff:
                        oldest_in_window = ts
                        break
                if oldest_in_window is not None:
                    retry_ms = oldest_in_window + self._window_ms - now_ms
                    retry_after = timedelta(milliseconds=max(1, retry_ms))
            return RateLimitState(
                bucket_name=self._name,
                backend="memory",
                is_exhausted=is_exhausted,
                remaining=float(max(0, self._limit - count)),
                retry_after=retry_after,
                limit=self._limit,
                window=timedelta(milliseconds=self._window_ms),
                style="log",
            )

    async def reset(self) -> None:
        async with self._lock:
            self._deque.clear()
            self._ids.clear()


class _InMemorySlidingWindowGCRA:
    """Per-bucket state for the GCRA in-memory sliding-window algorithm.

    Implements the Generic Cell Rate Algorithm (GCRA) with a single TAT
    (theoretical arrival time) value instead of a log of timestamps.
    The arithmetic is canonical GCRA from Brandur Leach's published
    reference and mirrors the GCRA Lua script installed by the Redis
    backend.

    The in-memory backend augments the TAT with an exact timestamp log to
    enforce a strict per-window count bound.  Standard GCRA with
    ``delay_tolerance = window_ms`` allows up to ``limit + 1`` real
    timestamps in any window of ``window_ms`` at boundary conditions (e.g.
    a burst of ``limit`` cells at t=0 followed by one more at
    t=emission_interval).  The log guard closes this gap without altering
    the TAT arithmetic used to compute ``retry_after``.
    """

    __slots__ = (
        "_delay_tolerance_ms",
        "_emission_interval_ms",
        "_limit",
        "_lock",
        "_log",
        "_name",
        "_tat",
        "_window_ms",
    )

    def __init__(self, name: str, limit: int, window_ms: int) -> None:
        self._name = name
        self._limit = limit
        self._window_ms = window_ms
        self._emission_interval_ms = window_ms / limit
        self._delay_tolerance_ms = window_ms
        self._lock = asyncio.Lock()
        self._tat: float | None = None
        self._log: deque[int] = deque()

    async def acquire(self, now_ms: int) -> RateLimitDecision:
        async with self._lock:
            cutoff = now_ms - self._window_ms
            while self._log and self._log[0] <= cutoff:
                self._log.popleft()

            previous_tat = self._tat
            tat = self._tat if self._tat is not None else float(now_ms)
            tat = max(tat, float(now_ms))
            new_tat = tat + self._emission_interval_ms
            allow_at = new_tat - self._delay_tolerance_ms

            count_full = len(self._log) >= self._limit

            if float(now_ms) < allow_at or count_full:
                if count_full and float(now_ms) >= allow_at:
                    oldest = self._log[0]
                    retry_ms = oldest + self._window_ms - now_ms
                    retry_after = timedelta(milliseconds=max(1, round(retry_ms)))
                else:
                    retry_after = timedelta(milliseconds=round(allow_at - now_ms))
                return RateLimitDecision(
                    allowed=False,
                    remaining=0.0,
                    retry_after=retry_after,
                    bucket_name=self._name,
                    backend="memory",
                )

            self._tat = new_tat
            self._log.append(now_ms)
            remaining = max(
                0,
                int((self._delay_tolerance_ms - (new_tat - now_ms)) / self._emission_interval_ms),
            )
            remaining = min(remaining, self._limit - len(self._log))
            return RateLimitDecision(
                allowed=True,
                remaining=float(remaining),
                retry_after=timedelta(0),
                bucket_name=self._name,
                backend="memory",
                previous_state={
                    "previous_tat_ms": previous_tat,
                    "new_tat_ms": new_tat,
                    "acquire_now_ms": now_ms,
                },
            )

    async def refund(self, previous_state: dict[str, object]) -> None:
        import contextlib

        async with self._lock:
            new_tat = float(previous_state["new_tat_ms"])  # type: ignore[arg-type]  # Why: new_tat_ms is float stored as object in dict; runtime type is correct
            if self._tat != new_tat:
                return
            prev = previous_state["previous_tat_ms"]
            self._tat = float(prev) if prev is not None else None  # type: ignore[arg-type]  # Why: previous_tat_ms_ms is float | None stored as object in dict; runtime type is correct
            acquire_now_ms = int(previous_state["acquire_now_ms"])  # type: ignore[arg-type]  # Why: acquire_now_ms is int stored as object in dict; runtime type is correct
            with contextlib.suppress(ValueError):
                self._log.remove(acquire_now_ms)

    async def peek(self, now_ms: int) -> RateLimitState:
        async with self._lock:
            cutoff = now_ms - self._window_ms
            log_count = sum(1 for ts in self._log if ts > cutoff)

            tat = self._tat if self._tat is not None else float(now_ms)
            tat = max(tat, float(now_ms))

            remaining = float(
                max(
                    0,
                    int((self._delay_tolerance_ms - (tat - now_ms)) / self._emission_interval_ms),
                )
            )
            remaining = min(remaining, self._limit - log_count)

            is_exhausted = remaining <= 0 or log_count >= self._limit
            retry_after: timedelta | None = None
            if is_exhausted:
                if log_count >= self._limit:
                    oldest: int | None = None
                    for ts in self._log:
                        if ts > cutoff:
                            oldest = ts
                            break
                    if oldest is not None:
                        retry_ms = oldest + self._window_ms - now_ms
                        retry_after = timedelta(milliseconds=max(1, retry_ms))
                if retry_after is None:
                    new_tat = tat + self._emission_interval_ms
                    allow_at = new_tat - self._delay_tolerance_ms
                    retry_after = timedelta(milliseconds=max(1, round(allow_at - now_ms)))

            return RateLimitState(
                bucket_name=self._name,
                backend="memory",
                is_exhausted=is_exhausted,
                remaining=remaining,
                retry_after=retry_after,
                limit=self._limit,
                window=timedelta(milliseconds=self._window_ms),
                style="gcra",
            )

    async def reset(self) -> None:
        async with self._lock:
            self._tat = None
            self._log.clear()


class SlidingWindow:
    """Sliding-window rate limiter with pluggable backends.

    Raises :class:`ValueError` if ``limit < 1``, ``window <= timedelta(0)``,
    or ``style`` is not ``"log"`` or ``"gcra"``.
    """

    __slots__ = (
        "_backend",
        "_limit",
        "_mem_gcra",
        "_mem_log",
        "_name",
        "_redis_gcra_refund_script",
        "_redis_gcra_script",
        "_redis_log_script",
        "_script_lock",
        "_style",
        "_ttl",
        "_window",
    )

    def __init__(
        self,
        name: str,
        limit: int,
        window: timedelta,
        backend: Literal["redis", "postgres", "memory"] = "redis",
        style: SlidingWindowStyle = "log",
        ttl: timedelta | None = None,
    ) -> None:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if window <= timedelta(0):
            raise ValueError(f"window must be > timedelta(0), got {window}")
        if style not in _VALID_STYLES:
            raise ValueError(f"style must be 'log' or 'gcra', got {style!r}")

        self._name = name
        self._limit = limit
        self._window = window
        self._backend: RateLimitBackend = backend
        self._style: SlidingWindowStyle = style

        if ttl is not None:
            self._ttl = ttl
        elif style == "gcra":
            self._ttl = window + timedelta(milliseconds=60_000)
        else:
            self._ttl = 2 * window + timedelta(milliseconds=60_000)

        self._mem_log: _InMemorySlidingWindowLog | None = None
        self._mem_gcra: _InMemorySlidingWindowGCRA | None = None
        if backend == "memory":
            window_ms = int(window.total_seconds() * 1000)
            if style == "log":
                self._mem_log = _InMemorySlidingWindowLog(name, limit, window_ms)
            else:
                self._mem_gcra = _InMemorySlidingWindowGCRA(name, limit, window_ms)

        self._redis_log_script: AsyncScript | None = None
        self._redis_gcra_script: AsyncScript | None = None
        self._redis_gcra_refund_script: AsyncScript | None = None
        self._script_lock: asyncio.Lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window(self) -> timedelta:
        return self._window

    @property
    def backend(self) -> Literal["redis", "postgres", "memory"]:
        return self._backend

    @property
    def style(self) -> SlidingWindowStyle:
        return self._style

    @property
    def ttl(self) -> timedelta | None:
        return self._ttl

    async def acquire(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> RateLimitDecision:
        if clock is None:
            raise RuntimeError("clock not injected for sliding window acquire")

        now_ms = int(clock.now().timestamp() * 1000)
        request_id: UUID | None = new_uuid() if self._style == "log" else None

        match (self._backend, self._style):
            case ("memory", "log"):
                return await self._acquire_memory_log(now_ms, request_id)
            case ("memory", "gcra"):
                return await self._acquire_memory_gcra(now_ms)
            case ("redis", "log"):
                return await _acquire_redis_log_wrapped(
                    self, now_ms, request_id, redis_client, pg_pool, clock, settings
                )
            case ("redis", "gcra"):
                return await _acquire_redis_gcra_wrapped(
                    self, now_ms, redis_client, pg_pool, clock, settings
                )
            case ("postgres", "log"):
                return await _acquire_pg_log(self, pg_pool, clock, settings, request_id)
            case ("postgres", "gcra"):
                return await _acquire_pg_gcra(self, pg_pool, clock, settings)
            case _:
                assert_never((self._backend, self._style))

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
        match (self._backend, self._style):
            case ("redis", "log"):
                await _refund_redis_log(self, decision, redis_client, settings)
            case ("redis", "gcra"):
                await _refund_redis_gcra(self, decision, redis_client, settings)
            case ("memory", "log"):
                await self._refund_memory_log(decision)
            case ("memory", "gcra"):
                await self._refund_memory_gcra(decision)
            case ("postgres", "log"):
                await _refund_pg_log(self, decision, pg_pool, settings)
            case ("postgres", "gcra"):
                await _refund_pg_gcra(self, decision, pg_pool, settings)
            case _:
                assert_never((self._backend, self._style))

    async def peek(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> RateLimitState:
        if clock is None:
            raise RuntimeError("clock not injected for sliding window peek")

        now_ms = int(clock.now().timestamp() * 1000)

        match (self._backend, self._style):
            case ("memory", "log"):
                return await self._peek_memory_log(now_ms)
            case ("memory", "gcra"):
                return await self._peek_memory_gcra(now_ms)
            case ("redis", "log"):
                return await _peek_redis_log(self, now_ms, redis_client, settings)
            case ("redis", "gcra"):
                return await _peek_redis_gcra(self, now_ms, redis_client, settings)
            case ("postgres", "log"):
                return await _peek_pg_log(self, now_ms, pg_pool, clock, settings)
            case ("postgres", "gcra"):
                return await _peek_pg_gcra(self, now_ms, pg_pool, clock, settings)
            case _:
                assert_never((self._backend, self._style))

    async def reset(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: Clock | None = None,
        settings: "WorkerSettings | None" = None,
    ) -> None:
        match (self._backend, self._style):
            case ("memory", "log"):
                await self._reset_memory_log()
            case ("memory", "gcra"):
                await self._reset_memory_gcra()
            case ("redis", "log"):
                await _reset_redis_log(self, redis_client, settings)
            case ("redis", "gcra"):
                await _reset_redis_gcra(self, redis_client, settings)
            case ("postgres", "log"):
                await _reset_pg_log(self, pg_pool, settings)
            case ("postgres", "gcra"):
                await _reset_pg_gcra(self, pg_pool, settings)
            case _:
                assert_never((self._backend, self._style))

        logger.warning(
            "ratelimit-reset",
            bucket_name=self._name,
            backend=self._backend,
            style=self._style,
        )

    # ── Memory backend helpers ──────────────────────────────────────────

    async def _peek_memory_log(self, now_ms: int) -> RateLimitState:
        if self._mem_log is None:
            raise RuntimeError("memory log not initialised")
        return await self._mem_log.peek(now_ms)

    async def _peek_memory_gcra(self, now_ms: int) -> RateLimitState:
        if self._mem_gcra is None:
            raise RuntimeError("memory GCRA not initialised")
        return await self._mem_gcra.peek(now_ms)

    async def _reset_memory_log(self) -> None:
        if self._mem_log is None:
            raise RuntimeError("memory log not initialised")
        await self._mem_log.reset()

    async def _reset_memory_gcra(self) -> None:
        if self._mem_gcra is None:
            raise RuntimeError("memory GCRA not initialised")
        await self._mem_gcra.reset()

    async def _acquire_memory_log(self, now_ms: int, request_id: UUID | None) -> RateLimitDecision:
        if self._mem_log is None:
            raise RuntimeError("memory log not initialised")

        rid = str(request_id) if request_id is not None else ""
        result = await self._mem_log.acquire(now_ms, rid)
        log_decision(result, style=self._style)
        return dataclasses.replace(
            result, request_id=str(request_id) if request_id is not None else None
        )

    async def _acquire_memory_gcra(self, now_ms: int) -> RateLimitDecision:
        if self._mem_gcra is None:
            raise RuntimeError("memory GCRA not initialised")

        result = await self._mem_gcra.acquire(now_ms)
        log_decision(result, style=self._style)
        return result

    async def _refund_memory_gcra(self, decision: RateLimitDecision) -> None:
        if self._mem_gcra is None:
            raise RuntimeError("memory GCRA not initialised")
        if decision.previous_state is None:
            return
        await self._mem_gcra.refund(decision.previous_state)

    async def _refund_memory_log(self, decision: RateLimitDecision) -> None:
        if self._mem_log is None:
            raise RuntimeError("memory log not initialised")
        if decision.request_id is None:
            return
        await self._mem_log.refund(decision.request_id)
        logger.debug(
            "ratelimit-refund",
            bucket_name=self._name,
            backend="memory",
            style="log",
            request_id=decision.request_id,
        )

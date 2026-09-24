"""Token-bucket rate limiter with pluggable backends.

The in-memory backend is the reference implementation and arithmetic oracle
for the Redis Lua script and the PG fallback.

Design deviation, always invoke the Lua script instead of a Python
pre-check ``if self.refill_per_second == 0 and tokens < count: return ...``
before invoking the script. We deviate by always invoking the Lua script
and post-processing the result.
- Why: the pre-check requires local knowledge of ``tokens``.
  The in-memory backend has that knowledge in ``_InMemoryBucket._tokens``
  and implements the pre-check. The Redis backend does NOT
  have local knowledge of ``tokens``, that state lives in the Redis
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
decomposition). It co-locates three concern-clusters, (a) the
``_InMemoryBucket`` state machine, (b) Lua-result decoding and the Redis
acquire path, and (c) the PG acquire path, all of which serve the single
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
from typing import TYPE_CHECKING, Final, cast

import structlog

from taskq._advisory import (
    _LOCK_TIMEOUT_SET_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the one implementation of the set_config statement shared with the enqueue and sliding-window bounded locks, a local copy would drift from the machinery it mirrors.
    DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
)
from taskq.backend._protocol import RateLimitBackend
from taskq.backend._records import jsonb_param, jsonb_to_dict
from taskq.backend.clock import Clock
from taskq.exceptions import RateLimitDependencyUnavailable, RateLimitStoreCorrupt
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._lock_budget import resolve_token_bucket_lock_timeout_ms
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

#: Bounded wait (milliseconds) for the PG fallback's ``rate_limit_buckets``
#: row lock. Same value and rationale as the log-style sliding window's
#: :data:`~taskq.ratelimit._sliding_window_pg.DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`
#:, the PG fallback is the Redis-outage funnel
#: (``rate_limit_pg_fallback_enabled`` defaults on), so every admission
#: waits on this lock exactly when the fleet is already degraded; the
#: bound converts a black-holed holder (dead TCP, no FIN, the server
#: reaps it only via keepalives) from a bucket-wide admission hang into
#: the limiter's fail-closed denial. ``0`` (or less) waits indefinitely,
#: the ``lock_timeout`` GUC convention shared with migrate.py and
#: ``taskq._advisory``.
DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS: Final[float] = 5000.0

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

    The value is advisory, a hint for how long to back off, so clamping it
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
    EVAL docs: "Lua number → RESP2 integer reply, removing the decimal
    part of the number, if any"). This helper normalises them to Python
    types in one place so the rest of the Redis path stays fully typed.
    """

    allowed: bool
    tokens_remaining: float
    retry_after_seconds: float


def _decode_lua_result(raw: object, *, capacity: float) -> _LuaResult:
    """Decode AND validate the raw Redis response from the token-bucket
    Lua script at the trust boundary.

    This function is the **only** place where redis-py's untyped
    ``AsyncScript.__call__`` return touches our code. ``raw[0]`` is an
    integer (allowed: 0 or 1). ``raw[1]`` and ``raw[2]`` are strings
    (bytes or str depending on ``decode_responses``) produced by Lua's
    ``tostring()``, this is required because Redis truncates Lua number
    returns to integers, losing fractional parts. ``int()`` / ``float()``
    accept bytes, int, and str at runtime.

    The reply contract is enforced here, not assumed: a proxy between
    TaskQ and the store can answer with semantically wrong values, and a
    reply no honest script could produce must fail closed as a store
    outage (:class:`RateLimitStoreCorrupt`), never crash the caller with
    a ``ValueError``/``IndexError``/``TypeError`` no fallback handler
    recognises and never be trusted into a decision:

    * shape: exactly the three elements the script returns; a truncated
      or extended reply is a lie;
    * ``allowed``: exactly the integers 0 or 1 (any other decode, ``2``,
      ``-1``, ``True``-as-int noise aside, is a lie, there is no
      honest third verdict);
    * ``tokens_remaining``: finite and inside ``[0, capacity]``, the
      script's own arithmetic bounds the count both ways (the spend
      floors at 0, the refill's ``min`` caps at capacity), so a negative
      count (permanent-denial lie) or a huge one (phantom-balance lie)
      cannot be trusted into the decision;
    * ``retry_after_seconds``: never negative. The honest hint is
      ``(req - tokens) / refill`` with ``req > tokens`` and
      ``refill > 0``, strictly positive; a negative hint clamps to
      ``timedelta(0)`` downstream, which is the ALLOWED decision's
      sentinel value, so trusting it turns a denial into a zero-wait
      retry (a spin). Non-finite hints stay tolerated: the module's
      documented deviation (see the module docstring) substitutes
      ``None``/``_MAX_TTL`` for them, the advisory hint is clamped by
      :func:`_retry_after` and never corrupts admission state.
    """
    if not isinstance(raw, (list, tuple)):
        raise RateLimitStoreCorrupt(
            f"token-bucket script reply violates the 3-element contract: {raw!r}"
        )
    elements = cast("list[object]", raw)
    if len(elements) != 3:
        raise RateLimitStoreCorrupt(
            f"token-bucket script reply violates the 3-element contract: {raw!r}"
        )
    try:
        allowed_int = int(elements[0])  # pyright: ignore[reportArgumentType]  # Why: the element is object after the shape check; int() accepts int | str | bytes at runtime
        tokens_remaining = float(elements[1])  # pyright: ignore[reportArgumentType]  # Why: same object element boundary
        retry_after_seconds = float(elements[2])  # pyright: ignore[reportArgumentType]  # Why: same object element boundary
    except (TypeError, ValueError, OverflowError) as exc:
        # OverflowError joins the family: int(float("inf")) and
        # float(10**400) raise it, a sibling of neither TypeError nor
        # ValueError, and an uncaught one is the original crash class
        # the boundary exists to kill.
        raise RateLimitStoreCorrupt(f"token-bucket script reply is not numeric: {raw!r}") from exc
    if allowed_int not in (0, 1):
        raise RateLimitStoreCorrupt(f"token-bucket script reply has an impossible verdict: {raw!r}")
    if not math.isfinite(tokens_remaining) or not (0.0 <= tokens_remaining <= capacity):
        raise RateLimitStoreCorrupt(
            f"token-bucket script reply reports tokens outside [0, capacity={capacity}]: {raw!r}"
        )
    if retry_after_seconds < 0.0:
        # A negative hint is a spin lie: it clamps to the zero-wait
        # sentinel downstream, which is the ALLOWED decision's value,
        # so trusting it turns a denial into a zero-wait retry. nan/inf
        # stay tolerated (the documented deviation: nan never reaches
        # the hint on the fixed-quota arm, inf clamps to _MAX_TTL).
        raise RateLimitStoreCorrupt(f"token-bucket script reply has a negative retry hint: {raw!r}")
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

    ``keyed`` (default ``False``) marks a bucket materialised from a
    :class:`~taskq.ratelimit.refs.KeyedRateLimitRef` whose
    ``rate_limit_buckets`` row is FLEET-reclaimable: keyed-materialised
    AND PG-state-backed. The restriction to ``backend="postgres"`` is
    the point, not an accident: only a PG-state-backed bucket's acquire
    path touches its PG row, so only there does the row's
    ``last_used_at`` stamp (refreshed by the preseed/upsert/refund
    statements below) truthfully track use, a redis-backend keyed
    bucket's healthy acquire never touches PG, and its PG row is
    outage-fallback state plus admin metadata that the fleet sweep must
    never delete (the stamp would be a false staleness signal for an
    actively-used bucket). The registry's materialisation arm is the
    single call site that knows both facts and passes the flag
    accordingly; every other constructor call keeps the static default.
    """

    __slots__ = (
        "_backend",
        "_capacity",
        "_keyed",
        "_mem_bucket",
        "_name",
        "_redis_refund_script",
        "_redis_refund_script_client",
        "_redis_script",
        "_redis_script_client",
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
        *,
        keyed: bool = False,
    ) -> None:
        if capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {capacity}")
        if refill_per_second < 0:
            raise ValueError(f"refill_per_second must be >= 0, got {refill_per_second}")

        self._name = name
        self._capacity = capacity
        self._refill = refill_per_second
        self._backend: RateLimitBackend = backend
        self._keyed = keyed

        self._ttl = ttl if ttl is not None else _default_ttl(capacity, refill_per_second)

        self._mem_bucket: _InMemoryBucket | None = None
        if backend == "memory":
            self._mem_bucket = _InMemoryBucket(name, capacity, refill_per_second)

        self._redis_script: AsyncScript | None = None
        self._redis_script_client: redis_async.Redis | None = None
        self._redis_refund_script: AsyncScript | None = None
        self._redis_refund_script_client: redis_async.Redis | None = None
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

    @property
    def keyed(self) -> bool:
        """Whether this bucket's ``rate_limit_buckets`` row is
        fleet-reclaimable (see the class docstring for the exact
        marking rule)."""
        return self._keyed

    def _state_payload(self, tokens: float, now: float) -> dict[str, object]:
        """The ``rate_limit_buckets.state`` document for this bucket.

        ``capacity`` and ``refill`` ride along with the accumulator so the
        row is self-describing: the leader's fleet sweep decides whether
        deleting a row would destroy live state, and it has only the row ,
        no instance, no actor config. Without them a fixed quota
        (``refill == 0``) that has been partly spent is indistinguishable
        from an ordinary idle bucket, and deleting it lets the next
        acquire re-preseed at full capacity, over-admitting against a
        budget that was already spent. Rows written before these keys
        existed carry neither, and the sweep's veto treats a row it
        cannot prove safe to delete as one it must keep.

        THE CANONICAL KEY SET is exactly ``{tokens, ts, capacity,
        refill}``, this method is its definition, and every reader of the
        document reads NAMED keys only (``_peek_pg`` reads ``tokens``;
        ``_refund_pg`` reads ``tokens``/``ts``; the reclaim sweeps read
        ``tokens``/``refill``/``capacity``), so an unknown key is inert.
        Exactly one writer adds a key beyond it: the fused PG acquire
        (``_acquire_pg``) stamps a transient ``granted`` boolean so the
        decision can ride the row home through RETURNING (RETURNING sees
        only the final row), it describes the acquire that last wrote
        the state and is replaced by the next write (a refund rewrites
        the document through this method, dropping it; the next acquire
        re-adds it). The key-set pin in
        tests/test_ratelimit_token_bucket_pg.py asserts both shapes so a
        future key-iterating reader trips a loud test instead of a
        silent assumption.
        """
        return {
            "tokens": tokens,
            "ts": now,
            "capacity": self._capacity,
            "refill": self._refill,
        }

    def holds_consumed_quota(self) -> bool:
        """True if idle-evicting this bucket would silently reset a consumed fixed quota.

        Idle eviction exists to bound registry growth, not to hand back
        capacity a tenant already spent. A fixed quota
        (``refill_per_second == 0``) is drained forever by design, so
        nothing about it recovers on its own and idleness is never
        evidence that it is safe to discard. Refilling buckets are not
        exempt: their state converges back toward full anyway, so eviction
        loses at most one refill window's worth of tokens, an accepted,
        bounded divergence.

        What eviction destroys differs by where the state lives, and only
        ONE backend actually loses the budget:

        * **memory**: token state lives on the instance and nowhere else,
          so eviction discards it and the next acquire materializes at
          FULL capacity. The instance is right here, so the exemption is
          exact: only a bucket that has actually spent some quota is held.
        * **postgres**, the ``rate_limit_buckets`` row IS the state, and
          evicting the REGISTRY entry does not delete it: both row-delete
          paths (the maintenance leader's fleet sweep and the per-worker
          pending-reclaim drain, sharing ``_no_consumed_quota_sql``) veto
          deleting a row whose fixed quota is partly spent, and
          re-materialization resumes from the surviving row (the acquire
          preseeds ``ON CONFLICT DO NOTHING`` and reads the existing state
          under the row lock). The registry entry is pure bookkeeping,
          dropping it loses nothing, so no PG fixed-quota bucket is held.
          Holding them anyway (the pre-fix shape) was worse than a
          wasted entry: every PG fixed-quota key ever seen counted against
          ``max_keyed_rate_limits`` forever, so once the cap filled, every
          NEW key was refused with ``ReservationUnavailable`` and its jobs
          snooze-looped until process restart.
        * **redis**: the backend keeps fixed-quota state for 24 h of its
          own accord (see ``_compute_ttl_seconds``), so a re-materialized
          bucket resumes prior state there and eviction is state-safe.

        Reads ``_tokens`` without the bucket's async lock; safe because the
        only caller (the registry's idle-eviction sweep) runs synchronously
        in the event loop with no await between this read and the dict pop,
        so the value is consistent at the sweep instant.
        """
        if self._refill != 0.0:
            return False
        if self._backend != "memory":
            # postgres: the row-delete vetoes carry the state-safety
            # guarantee (see the docstring); redis: the 24 h TTL does.
            # Neither's quota can be reset by a REGISTRY eviction.
            return False
        # Why the protected read: _InMemoryBucket._tokens is this module's
        # own accumulator, and the registry's idle-eviction sweep (the
        # only caller) runs synchronously with no await between this read
        # and the dict pop, the docstring above documents the
        # consistency argument; a public accessor would widen the surface
        # for one internal read.
        tokens: float | None = (
            self._mem_bucket._tokens  # pyright: ignore[reportPrivateUsage]  # Why: same-module internal accumulator; see the comment above.
            if self._mem_bucket is not None
            else None
        )
        return tokens is not None and tokens < self._capacity

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
        # Why decision.backend and not self._backend: with backend="redis" and
        # rate_limit_pg_fallback_enabled (the default), an acquire during a
        # Redis outage falls through to Postgres and consumes the token THERE.
        # The decision records which store actually paid; the primitive's own
        # configuration only records where it prefers to go. Dispatching on the
        # latter refunded Redis for a token Postgres spent, inflating one
        # store's quota and destroying the other's, and for a fixed-quota
        # bucket (refill_per_second == 0) nothing ever puts the Postgres token
        # back, so that loss is permanent.
        #
        # Why clock is unused: a token-bucket refund returns *count* tokens to
        # the bucket and needs no timestamp. It stays in the signature because
        # RateLimitRegistry dispatches refund/peek/reset polymorphically over
        # TokenBucket and SlidingWindow with one fixed keyword block
        # (redis_client, pg_pool, clock, settings), see registry.reset_limit's
        # call sites. Dropping it would raise TypeError there, not merely break
        # symmetry.
        if decision.backend == "memory":
            await self._refund_memory(count)
        elif decision.backend == "redis":
            await self._refund_redis(count, redis_client, settings)
        elif decision.backend == "postgres":
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
        """Read-only Redis state snapshot, the elapsed-refill estimate runs
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

        try:
            tokens = self._capacity if tokens_raw is None else float(tokens_raw)  # pyright: ignore[reportUnknownArgumentType]  # Why: tokens_raw type is unknown due to untyped redis-py stub; validated at runtime.
            ts = now_seconds if ts_raw is None else float(ts_raw)  # pyright: ignore[reportUnknownArgumentType]  # Why: ts_raw type is unknown due to untyped redis-py stub; validated at runtime.
        except (TypeError, ValueError, OverflowError) as exc:
            # A peek is read-only, but the trust boundary is the same as
            # the acquire's: a reply no honest hash could hold is a store
            # lie, failed closed as the outage it is indistinguishable
            # from, never a ValueError crash with no provenance.
            # OverflowError joins the family: int(float("inf")) and
            # float(10**400) raise it, a sibling of neither TypeError nor
            # ValueError, and an uncaught one is the original crash class
            # the boundary exists to kill.
            raise RateLimitStoreCorrupt(
                f"token-bucket peek read a non-numeric hash value: {raw!r}"
            ) from exc
        # The acquire script bounds the stored count to [0, capacity]
        # (spend floors at 0, refill's min caps at capacity); anything
        # else in the hash is a lie (a negative count would report a
        # permanent denial, a huge one a phantom balance).
        if not math.isfinite(tokens) or not (0.0 <= tokens <= self._capacity):
            raise RateLimitStoreCorrupt(
                f"token-bucket peek read tokens outside [0, capacity={self._capacity}]: {tokens!r}"
            )

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
        """Read-only PG state snapshot: the STORED token count, exactly as
        the row holds it.

        No elapsed-refill projection. Peek is the audit view of the
        store, the bounded-lock contract's fail-closed verification
        reads it to prove a timed-out racer wrote nothing ("the seeded
        tokens are intact"), and a projection would make that audit
        drift with the read's timing: the same unchanged row would
        report a different count at T and T+1s. What an acquire WOULD
        see is the acquire's own arithmetic, applied under the row lock
        and re-stamped atomically; projecting it here would only
        duplicate it without its guarantees. ``is_exhausted`` and the
        exhausted retry hint derive from the reported count, so an
        exhausted bucket still tells the operator how long one more
        token takes.
        """
        if pg_pool is None:
            raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend")

        schema = settings.schema_name

        select_sql = f'SELECT state FROM "{schema}".rate_limit_buckets WHERE bucket_name=$1'  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound

        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(select_sql, self._name)

        if row is None:
            tokens = self._capacity
        else:
            state = jsonb_to_dict(row["state"])
            tokens = float(state.get("tokens", self._capacity))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback for rows missing keys (e.g. from schema migrations or interop writes)

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
            raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
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
        def get() -> "AsyncScript | None":
            if self._redis_refund_script_client is not redis_client:
                return None
            return self._redis_refund_script

        def bind(script: "AsyncScript") -> None:
            self._redis_refund_script_client = redis_client
            self._redis_refund_script = script

        return await ensure_redis_script(
            get,
            bind,
            lambda: redis_client.register_script(REFUND_SCRIPT),
            self._script_lock,
        )

    async def _refund_pg(
        self,
        count: float,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
        *,
        lock_timeout_ms: float | None = None,
    ) -> None:
        """Refund tokens on the PG backend using FOR UPDATE on rate_limit_buckets.

        Mirrors the Redis refund script: apply the elapsed-refill step so a
        refund landing after idle time does not lose accrued tokens, then add
        ``count`` capped at ``capacity``. If the bucket row does not exist
        (never created or already reset), this is a no-op. The elapsed math
        and the stored ``ts`` are server-domain (``EXTRACT(EPOCH FROM
        clock_timestamp())`` read in the same locked transaction), matching
        the acquire path's stamps.

        The row-lock WAIT is bounded by the operator's
        ``token_bucket_lock_timeout_ms`` budget (defaulting to
        :data:`DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS`), the same budget and
        the same discipline the acquire path in this file applies, so a
        deployment that shortens the wait shortens both arms: with
        ``rate_limit_pg_fallback_enabled`` on, a Redis outage funnels the
        fleet's refunds through this lock exactly when it is already
        degraded, and an unbounded wait would let one black-holed holder
        (dead TCP, no FIN) pin the refund until the server's keepalives
        reap it. On budget exhaustion the refund RAISES, the opposite of
        the acquire's fail-closed denial, because ``_refund_pg`` returns
        ``None`` on success and a quiet no-op return would make a lost
        refund look like a completed one (the tokens stay spent; for a
        fixed-quota bucket nothing ever puts them back). The raise surfaces
        one level up as the release path's rollback-failure ERROR and
        ``ratelimit.refund_failures`` counter. ``lock_timeout_ms <= 0``
        waits indefinitely, the ``lock_timeout`` GUC convention shared
        with migrate.py and ``taskq._advisory``.
        """
        if pg_pool is None:
            raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend refund")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend refund")
        lock_timeout_ms = resolve_token_bucket_lock_timeout_ms(
            lock_timeout_ms, settings, DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
        )

        schema = settings.schema_name

        select_sql = (
            f"SELECT state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
            f'FROM "{schema}".rate_limit_buckets WHERE bucket_name=$1 FOR UPDATE'
        )
        # The refund is a release-path USE of the row (the token goes
        # back), so it rides the same last_used_at refresh the acquire
        # does: a bucket whose token was just refunded is mid-workflow,
        # and an unstamped refund could let the fleet sweep catch the
        # row idle past the horizon in the window between the acquiring
        # worker's last acquire and its next one.
        update_sql = f'UPDATE "{schema}".rate_limit_buckets SET state=$1::jsonb, updated_at=clock_timestamp(), last_used_at=clock_timestamp() WHERE bucket_name=$2'  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; values are $1/$2-bound

        async with pg_pool.acquire() as conn, conn.transaction():
            row: asyncpg.Record | None = None

            if lock_timeout_ms > 0:
                # Bounded row-lock wait, mechanics mirrored from the
                # acquire path above (taskq._advisory's contended tier):
                # set_config(..., true) is SET LOCAL semantics, so the
                # bound covers every lock wait this transaction can take
                # and dies with the transaction's own commit; the
                # savepoint wraps the lock-taking read as one unit; the
                # client-side backstop bounds the network black hole the
                # server-side timeout cannot see. Why the function-level
                # import: same boundary reason as the acquire above ,
                # this module stays importable without the asyncpg
                # driver installed, and the refund only ever runs
                # against a real connection.
                from asyncpg.exceptions import LockNotAvailableError

                await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(lock_timeout_ms)}ms")

                async def _locked_state_read() -> None:
                    nonlocal row
                    async with conn.transaction():
                        row = await conn.fetchrow(select_sql, self._name)

                try:
                    await asyncio.wait_for(
                        _locked_state_read(),
                        timeout=lock_timeout_ms / 1000.0
                        + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
                    )
                except (LockNotAvailableError, TimeoutError):
                    # The acquire converts exhaustion into the limiter's
                    # denial outcome; the refund cannot, its success
                    # shape IS the silent ``None`` return, so the only
                    # honest exhausted outcome is the RAISE. The warning
                    # is the same contended-or-sick-bucket signal every
                    # bounded limiter lock emits (one event name, one
                    # condition), with ``phase`` marking the different
                    # consequence: a lost refund, not a denied admission.
                    logger.warning(
                        "ratelimit-lock-timeout",
                        bucket_name=self._name,
                        backend="postgres",
                        lock_timeout_ms=lock_timeout_ms,
                        phase="refund",
                    )
                    raise
            else:
                # lock_timeout_ms <= 0: the indefinite mode, the GUC
                # convention's opt-out, and the pre-bound behavior.
                row = await conn.fetchrow(select_sql, self._name)

            if row is None:
                return

            now = float(row["now_s"])
            state = jsonb_to_dict(row["state"])
            tokens = float(state.get("tokens", self._capacity))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback for rows missing keys (e.g. from schema migrations or interop writes)
            ts = float(state.get("ts", now))  # type: ignore[index]  # Why: same, state is non-None; fallback to now for rows missing "ts"

            elapsed = max(0.0, now - ts)
            tokens = min(self._capacity, tokens + elapsed * self._refill)
            tokens = min(self._capacity, tokens + count)

            state_param = jsonb_param(self._state_payload(tokens, now))
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
        """Redis acquire, the script derives now from ``redis.call('TIME')``
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

        raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation, pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

        lua = _decode_lua_result(raw, capacity=self._capacity)

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
        def get() -> "AsyncScript | None":
            if self._redis_script_client is not redis_client:
                return None
            return self._redis_script

        def bind(script: "AsyncScript") -> None:
            self._redis_script_client = redis_client
            self._redis_script = script

        return await ensure_redis_script(
            get,
            bind,
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
        *,
        lock_timeout_ms: float | None = None,
    ) -> RateLimitDecision:
        """PG path: ONE upsert statement whose conflict arm does the arithmetic.

        ``INSERT … ON CONFLICT (bucket_name) DO UPDATE … RETURNING``, the
        preseed, the locked state read, and the upsert of the pre-fused
        shape (BEGIN + set_config + SAVEPOINT + preseed + SELECT FOR
        UPDATE + RELEASE + upsert + COMMIT, 8 round trips in bounded
        mode) collapse into a single statement. The token
        arithmetic runs server-side under the row lock the conflict arm
        itself takes:

        * cold start (no row): the INSERT arm admits from full
          capacity, the same spend decision the preseed-then-read shape
          computed after preseeding a full row;
        * existing row: the conflict arm re-fetches the row's latest
          committed version (the documented ON CONFLICT DO UPDATE
          semantics under READ COMMITTED, the atomic-counter upsert
          idiom), applies elapsed refill and the spend, and writes the
          new state, so concurrent first acquires and concurrent spends
          serialize exactly as the preseed + FOR UPDATE pair did.

        The time domain for the arithmetic is ``statement_timestamp()``
        (STABLE, one value for the whole statement), deliberately: the
        spend decision and the ``granted`` flag are separate evaluations
        of the same expression, and a VOLATILE ``clock_timestamp()``
        could let them straddle the spend boundary between evaluations
        (grant recorded, spend not taken, or the reverse). A stable
        statement clock makes every evaluation identical, and the
        stored ``ts`` is the same value the elapsed math used. The WRITE
        stamps (``updated_at`` / ``last_used_at``) stay
        ``clock_timestamp()``: they must stay co-monotonic with every
        other row's stamps, and they feed no arithmetic.

        The decision rides the row home as a transient ``granted`` key
        in the state document: RETURNING sees only the FINAL row, and
        ``allowed`` is not derivable from the token count alone (a
        denial stores the post-refill count; an allowance stores
        post-refill-minus-count, the same final count is reachable
        both ways). Every existing reader ignores unknown keys (peek
        reads ``tokens``; the refund reads ``tokens``/``ts``; the
        reclaim sweeps read ``tokens``/``refill``/``capacity``), and the
        next write, refund or a later acquire, replaces the whole
        document, so the key is inert bookkeeping between acquires.

        The row-lock WAIT is bounded by the operator's
        ``token_bucket_lock_timeout_ms`` budget (defaulting to
        :data:`DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS`), preserving the
        pre-fused semantics: with ``rate_limit_pg_fallback_enabled`` on,
        a Redis outage funnels all admission through this lock, so an
        unbounded wait would let one black-holed holder (dead TCP, no
        FIN) stall its bucket's admission until the server's keepalives
        reap it. The bound rides the same ``set_config(..., true)`` SET
        LOCAL the enqueue path's bounded idempotency wait uses, in the
        acquire's own transaction, a refusal aborts the transaction
        outright and the bound dies with it, so the savepoint (and its
        RELEASE) the savepoint-wrapped read needed is pure cost here
        (see test_round_trip_budgets' keyed-enqueue pin for the shape).
        On budget exhaustion the acquire FAILS CLOSED, the limiter's
        denial outcome, ``allowed=False`` with a retry hint of one more
        budget, never an exception, never an admission: a racer that
        could not write the bucket spent and admitted nothing, because
        the whole spend was one statement that either landed or raised.
        ``lock_timeout_ms <= 0`` waits indefinitely (one autocommit
        statement, no transaction and no GUC), the ``lock_timeout`` GUC
        convention shared with migrate.py and ``taskq._advisory``.
        """
        if pg_pool is None:
            raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
        if settings is None:
            raise RuntimeError("settings not injected for postgres backend")
        lock_timeout_ms = resolve_token_bucket_lock_timeout_ms(
            lock_timeout_ms, settings, DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS
        )

        schema = settings.schema_name

        # Schema-name interpolation ; schema_name is
        # pre-validated against _IDENT_RE at WorkerSettings load time.
        # $1 name, $2 capacity, $3 refill, $4 keyed mark, $5 count.
        #
        # The conflict arm's SET expressions reference the EXISTING row
        # as ``rate_limit_buckets.<col>`` (the ON CONFLICT DO UPDATE
        # convention); the COALESCE fallbacks mirror the pre-fused
        # Python fallbacks for rows written before the state document
        # carried a key (tokens -> capacity, ts -> now, so elapsed is
        # zero). The fleet-reclaim marking rides the write the conflict
        # arm already makes: last_used_at refreshes on every acquire,
        # including denials, whose state write must not read as idle,
        # and keyed takes the CURRENT owner's mark (EXCLUDED.keyed), so
        # a keyed bucket acquiring over a stale static-marked row claims
        # it and a static bucket acquiring over a former keyed row
        # retires it.
        _now_epoch = "EXTRACT(EPOCH FROM statement_timestamp())"
        _old_tokens = "COALESCE((rate_limit_buckets.state->>'tokens')::float8, $2::float8)"
        _old_ts = f"COALESCE((rate_limit_buckets.state->>'ts')::float8, {_now_epoch})"
        # Post-refill token count: the spend decision's left side.
        # Every evaluation is identical (statement_timestamp() is
        # stable), so the CASE below and the granted flag cannot
        # disagree at the spend boundary.
        _refilled = (
            f"LEAST($2::float8, {_old_tokens} + GREATEST({_now_epoch} - {_old_ts}, 0) * $3::float8)"
        )
        # Cold start: the bucket begins full, the same spend decision
        # against capacity the preseed-then-read shape computed.
        _cold_tokens = (
            "CASE WHEN $2::float8 >= $5::float8 THEN $2::float8 - $5::float8 ELSE $2::float8 END"
        )
        fused_sql = (
            f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at, keyed, last_used_at) '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; every value is $-bound.
            f"VALUES ($1, 'token_bucket', "
            f"jsonb_build_object('tokens', {_cold_tokens}, "
            f"'ts', {_now_epoch}, "
            f"'capacity', $2::float8, 'refill', $3::float8, "
            f"'granted', $2::float8 >= $5::float8), "
            f"clock_timestamp(), $4, clock_timestamp()) "
            f"ON CONFLICT (bucket_name) DO UPDATE SET "
            f"state = jsonb_build_object( "
            f"'tokens', CASE WHEN {_refilled} >= $5::float8 "
            f"THEN {_refilled} - $5::float8 ELSE {_refilled} END, "
            f"'ts', {_now_epoch}, "
            f"'capacity', $2::float8, 'refill', $3::float8, "
            f"'granted', {_refilled} >= $5::float8), "
            f"updated_at = clock_timestamp(), last_used_at = clock_timestamp(), "
            f"keyed = EXCLUDED.keyed "
            f"RETURNING (state->>'tokens')::float8 AS tokens_after, "
            f"(state->>'granted')::bool AS granted"
        )

        async def _fused_acquire(
            conn: "asyncpg.Connection | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]",
        ) -> "asyncpg.Record | None":
            return await conn.fetchrow(
                fused_sql, self._name, self._capacity, self._refill, self._keyed, count
            )

        row: asyncpg.Record | None = None

        if lock_timeout_ms > 0:
            # Why a function-level import: this module is imported by
            # taskq.ratelimit, which taskq.testing imports transitively, and
            # that boundary must stay importable without the asyncpg
            # driver installed. The acquire only ever runs against a
            # real connection, where asyncpg is guaranteed present.
            from asyncpg.exceptions import LockNotAvailableError

            async with pg_pool.acquire() as conn:
                try:
                    async with conn.transaction():
                        await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(lock_timeout_ms)}ms")
                        row = await asyncio.wait_for(
                            _fused_acquire(conn),
                            timeout=lock_timeout_ms / 1000.0
                            + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
                        )
                except (LockNotAvailableError, TimeoutError):
                    # Fail closed: the limiter's denial outcome with a
                    # retry hint of one more budget: the fused statement
                    # is atomic, so the timed-out racer wrote nothing
                    # (no preseed to roll back, no upsert that could have
                    # landed half-spent). The warning is the operator
                    # signal that the bucket (or its holder) is contended
                    # or sick rather than merely busy: the same event
                    # name the log-style path emits for the same
                    # condition.
                    logger.warning(
                        "ratelimit-lock-timeout",
                        bucket_name=self._name,
                        backend="postgres",
                        lock_timeout_ms=lock_timeout_ms,
                    )
                    result = RateLimitDecision(
                        allowed=False,
                        remaining=0.0,
                        retry_after=timedelta(milliseconds=lock_timeout_ms),
                        bucket_name=self._name,
                        backend="postgres",
                    )
                    log_decision(result)
                    return result
        else:
            # lock_timeout_ms <= 0: the indefinite mode, the GUC
            # convention's opt-out. One autocommit statement; the
            # conflict arm's row lock waits as long as the holder holds.
            async with pg_pool.acquire() as conn:
                row = await _fused_acquire(conn)

        if row is None:
            # Unreachable in the normal path: RETURNING always yields
            # the written row (insert arm or conflict arm). Kept as a
            # defensive denial (e.g. a trigger swallowing RETURNING):
            # never an admission from a write we did not observe.
            logger.error(
                "ratelimit-pg-acquire-no-returning-row",
                bucket_name=self._name,
            )
            result = RateLimitDecision(
                allowed=False,
                remaining=0.0,
                retry_after=None if self._refill == 0.0 else _retry_after(count / self._refill),
                bucket_name=self._name,
                backend="postgres",
            )
            log_decision(result)
            return result

        granted = bool(row["granted"])
        tokens_after = float(row["tokens_after"])
        # remaining is the final token count either way: post-spend on
        # allowance, post-refill on denial, exactly the pre-fused
        # arithmetic's two arms. retry_after: the deficit against the
        # post-refill count, None for a fixed quota (no automatic
        # recovery), the same channel the memory/Redis backends take.
        retry_after: timedelta | None
        if granted:
            retry_after = timedelta(0)
        elif self._refill == 0.0:
            retry_after = None
        else:
            retry_after = _retry_after((count - tokens_after) / self._refill)

        result = RateLimitDecision(
            allowed=granted,
            remaining=tokens_after,
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

"""Unified rate-limit registry and AND-composition.

``RateLimitRegistry`` holds all registered ``TokenBucket``, ``SlidingWindow``,
and ``ConcurrencyReservation`` instances in two separate dicts.  It
provides ``register()`` with duplicate detection, lookup methods, and
the ``acquire()`` async context manager for non-job code.

AND-composition (``acquire_for_actor`` / ``release_for_actor``) implements
reservations first in declaration order, then
rate limits in declaration order; rollback on failure in reverse acquisition
order; best-effort release with per-handle error catching; post-actor release
where reservation slots are released but rate-limit tokens are consumed
permanently.

Over-acquisition window on rollback failure:

- TokenBucket (Redis): ``ceil(capacity / refill_per_second * 2) + 60`` seconds
  (the ``EXPIRE`` TTL).
- SlidingWindow (Redis): ``2 * window_ms + 60_000`` ms (the ``PEXPIRE`` TTL
  ).
- ConcurrencyReservation (PG): ``lease_duration`` — reclaimed by sweep 4
  within 30 seconds at most.
"""

import re
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from time import monotonic
from typing import TYPE_CHECKING

import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_RESERVATION_BACKOFF,
)
from taskq.exceptions import ReservationUnavailable
from taskq.obs import record_ratelimit_refund_failure
from taskq.ratelimit.composition import (
    AcquiredResource,
    RateLimitHandle,
    ReservationHandle,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket

if TYPE_CHECKING:
    from datetime import timedelta
    from uuid import UUID

    import asyncpg
    import redis.asyncio as redis_async

    from taskq.backend.clock import Clock
    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit.registry")

__all__ = ["RateLimitRegistry", "registry", "sync_rate_limit_buckets"]

_MAX_KEYED_RESERVATION_KEY_LEN = 255
_KEYED_RESERVATION_KEY_RE = re.compile(r"^[A-Za-z0-9_\-:.]+$")


def _same_config(
    a: TokenBucket | SlidingWindow | ConcurrencyReservation,
    b: TokenBucket | SlidingWindow | ConcurrencyReservation,
) -> bool:
    """Structural config comparison for ``register()`` idempotency.

    ``TokenBucket`` / ``SlidingWindow`` / ``ConcurrencyReservation`` are
    plain ``__slots__`` classes without ``__eq__`` (default identity
    comparison), so two distinct instances built from the same config
    (e.g. a module re-imported under ``importlib.reload``, or a config
    reconstructed on worker restart) would never compare equal via
    ``==``.  Compares only the public, immutable config surface —  not
    internal state such as cached Lua scripts or the in-memory bucket.
    """
    if isinstance(a, TokenBucket) and isinstance(b, TokenBucket):
        return (
            a.name == b.name
            and a.capacity == b.capacity
            and a.refill_per_second == b.refill_per_second
            and a.backend == b.backend
            and a.ttl == b.ttl
        )
    if isinstance(a, SlidingWindow) and isinstance(b, SlidingWindow):
        return (
            a.name == b.name
            and a.limit == b.limit
            and a.window == b.window
            and a.backend == b.backend
            and a.style == b.style
            and a.ttl == b.ttl
        )
    if isinstance(a, ConcurrencyReservation) and isinstance(b, ConcurrencyReservation):
        return a.name == b.name and a.slots == b.slots and a.lease == b.lease
    return False


class RateLimitRegistry:
    """Unified registry for rate-limit and reservation primitives.

    Stores two separate dicts: ``_rate_limits`` for ``TokenBucket`` /
    ``SlidingWindow`` and ``_reservations`` for ``ConcurrencyReservation``.
    Cross-dict name collision is allowed — they live in separate namespaces.
    """

    def __init__(self) -> None:
        self._rate_limits: dict[str, TokenBucket | SlidingWindow] = {}
        self._reservations: dict[str, ConcurrencyReservation] = {}
        # Names of reservations materialized from a KeyedReservationRef
        # (as opposed to a static @actor(reservations=["name"]) entry),
        # and the monotonic time each was last acquired — used only by
        # evict_idle_keyed_reservations() to bound registry growth under
        # high key cardinality. Never consulted by acquire_for_actor.
        self._keyed_reservation_last_used: dict[str, float] = {}

    @property
    def rate_limits(self) -> dict[str, TokenBucket | SlidingWindow]:
        return dict(self._rate_limits)

    @property
    def reservations(self) -> dict[str, ConcurrencyReservation]:
        return dict(self._reservations)

    @property
    def has_keyed_reservations(self) -> bool:
        return bool(self._keyed_reservation_last_used)

    def register(
        self,
        primitive: TokenBucket | SlidingWindow | ConcurrencyReservation,
    ) -> None:
        if isinstance(primitive, ConcurrencyReservation):
            name = primitive.name
            existing_reservation = self._reservations.get(name)
            if existing_reservation is not None:
                if _same_config(existing_reservation, primitive):
                    logger.debug(
                        "registry-register-idempotent-noop",
                        kind="reservation",
                        name=name,
                    )
                    return
                raise ValueError(
                    f"reservation name already registered with a different config: "
                    f"{name!r} — existing={existing_reservation!r}, new={primitive!r}"
                )
            self._reservations[name] = primitive
            logger.debug(
                "registry-registered",
                kind="reservation",
                name=name,
            )
            return

        name = primitive.name
        existing = self._rate_limits.get(name)
        if existing is not None:
            if _same_config(existing, primitive):
                logger.debug(
                    "registry-register-idempotent-noop",
                    kind="rate_limit",
                    name=name,
                )
                return
            raise ValueError(
                f"rate-limit name already registered with a different config: "
                f"{name!r} — existing={existing!r}, new={primitive!r}"
            )
        self._rate_limits[name] = primitive
        logger.debug(
            "registry-registered",
            kind="rate_limit",
            name=name,
        )

    def get_rate_limit(self, name: str) -> TokenBucket | SlidingWindow:
        try:
            return self._rate_limits[name]
        except KeyError:
            raise KeyError(name) from None

    def get_reservation(self, name: str) -> ConcurrencyReservation:
        try:
            return self._reservations[name]
        except KeyError:
            raise KeyError(name) from None

    @asynccontextmanager
    async def acquire(
        self,
        name: str,
        count: float = 1.0,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> AsyncGenerator[RateLimitDecision, None]:
        if name in self._reservations:
            raise TypeError(
                f"name {name!r} is a ConcurrencyReservation — "
                f"registry.acquire() is only for rate limits; "
                f"reservation acquisition requires a job_id"
            )
        if name not in self._rate_limits:
            raise KeyError(name)

        primitive = self._rate_limits[name]
        if isinstance(primitive, TokenBucket):
            decision = await primitive.acquire(
                count,
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )
        else:
            decision = await primitive.acquire(
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )
        yield decision

    async def _resolve_reservation_name(
        self,
        ref: "str | KeyedReservationRef",
        payload: dict[str, object] | None,
        *,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> str:
        """Return the concrete registry name for *ref*.

        A plain ``str`` is returned as-is (must already be registered via
        :meth:`register`). A :class:`KeyedReservationRef` derives
        ``f"{ref.base_name}:{key}"`` by calling ``ref.key_fn(payload)`` and
        lazily registers a matching :class:`ConcurrencyReservation` on
        first use — subsequent calls for the same key reuse it (``register``
        is idempotent for identical config, which every call for a given
        ref always produces since ``slots``/``lease`` are fixed on the ref).

        The ``key_fn`` return value is validated: it must be non-empty, at
        most ``_MAX_KEYED_RESERVATION_KEY_LEN`` characters, and match
        ``_KEYED_RESERVATION_KEY_RE`` (alphanumeric plus ``_ - : .``) — this
        prevents control characters in PG text columns and bounds storage
        growth from attacker-controlled keys. When ``settings`` is provided
        and the number of tracked keyed reservations reaches
        ``settings.max_keyed_reservations``, a new key raises
        :class:`~taskq.exceptions.ReservationUnavailable`.

        The reservation is built with ``schema=settings.schema_name`` (not
        the ``ConcurrencyReservation`` default) so it targets the same
        schema as every other primitive on this worker. Static reservations
        get their backing ``reservation_slots`` rows pre-allocated once at
        worker startup (see ``ensure_slots`` in worker/_bootstrap.py); a
        freshly-registered keyed reservation has no such startup hook, so
        :meth:`~ConcurrencyReservation.ensure_slots` is called here,
        immediately after registration, before the name is ever handed to
        ``acquire()`` — otherwise every acquisition would fail with
        ``ReservationUnavailable`` against an empty slot table. The
        ``_keyed_reservation_last_used`` entry is stamped *before* the
        ``ensure_slots`` await so that a concurrent
        :meth:`evict_idle_keyed_reservations` cannot evict the in-flight
        key; after the await, the reservation is re-registered if eviction
        did remove it (belt-and-suspenders for very aggressive eviction
        windows).
        """
        if isinstance(ref, str):
            return ref

        if payload is None:
            raise ValueError(
                f"reservation {ref.base_name!r} is a KeyedReservationRef but no "
                "payload was provided to derive its key from"
            )
        key = ref.key_fn(payload)
        if not key:
            raise ValueError(
                f"KeyedReservationRef(base_name={ref.base_name!r}).key_fn returned "
                f"an empty key for payload {payload!r}"
            )
        if len(key) > _MAX_KEYED_RESERVATION_KEY_LEN:
            raise ValueError(
                f"KeyedReservationRef(base_name={ref.base_name!r}).key_fn returned "
                f"a key of length {len(key)} which exceeds the maximum of "
                f"{_MAX_KEYED_RESERVATION_KEY_LEN} characters"
            )
        if not _KEYED_RESERVATION_KEY_RE.match(key):
            raise ValueError(
                f"KeyedReservationRef(base_name={ref.base_name!r}).key_fn returned "
                f"key {key!r} which contains characters outside the allowed set "
                f"[A-Za-z0-9_\\-:.]"
            )
        concrete_name = f"{ref.base_name}:{key}"
        if (
            concrete_name not in self._keyed_reservation_last_used
            and settings is not None
            and len(self._keyed_reservation_last_used) >= settings.max_keyed_reservations
        ):
            logger.warning(
                "registry-keyed-reservation-limit-exceeded",
                base_name=ref.base_name,
                current_count=len(self._keyed_reservation_last_used),
                limit=settings.max_keyed_reservations,
            )
            raise ReservationUnavailable(
                bucket_name=ref.base_name,
                retry_after=DEFAULT_RESERVATION_BACKOFF,
                source="reservation",
            )
        if concrete_name not in self._reservations:
            schema = settings.schema_name if settings is not None else "taskq"
            new_reservation = ConcurrencyReservation(
                name=concrete_name, slots=ref.slots, lease=ref.lease, schema=schema
            )
            self.register(new_reservation)
            self._keyed_reservation_last_used[concrete_name] = monotonic()
            if pg_pool is not None:
                await new_reservation.ensure_slots(pg_pool)
                if concrete_name not in self._reservations:
                    self.register(new_reservation)
        self._keyed_reservation_last_used[concrete_name] = monotonic()
        return concrete_name

    async def acquire_for_actor(
        self,
        rate_limits: list[str],
        reservations: Sequence["str | KeyedReservationRef"],
        *,
        job_id: "UUID",
        worker_id: "UUID",
        payload: dict[str, object] | None = None,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> list[AcquiredResource]:
        """AND-composition: acquire reservations first, then rate limits.

        ``reservations`` entries may be plain names (resolved against
        statically pre-registered primitives) or :class:`KeyedReservationRef`
        instances (resolved dynamically per job from ``payload`` — see
        :meth:`_resolve_reservation_name`). ``payload`` is required if any
        entry is a ``KeyedReservationRef``.

        Returns the list of ``AcquiredResource`` handles on full success.
        Raises ``ReservationUnavailable`` on any denial — rollback is performed
        internally before re-raising (already-acquired resources released in
        reverse order, each failure logged at ERROR).
        """
        acquired: list[AcquiredResource] = []
        try:
            for res_ref in reservations:
                res_name = await self._resolve_reservation_name(
                    res_ref, payload, pg_pool=pg_pool, settings=settings
                )
                reservation = self._reservations[res_name]
                slot_index = await reservation.acquire(
                    job_id,
                    worker_id,
                    pg_pool,
                )
                acquired.append(
                    ReservationHandle(
                        name=res_name,
                        reservation=reservation,
                        slot_index=slot_index,
                        job_id=job_id,
                        worker_id=worker_id,
                        pool=pg_pool,
                    )
                )

            for rl_name in rate_limits:
                rl = self._rate_limits[rl_name]
                if isinstance(rl, TokenBucket):
                    result = await rl.acquire(
                        1.0,
                        redis_client=redis_client,
                        pg_pool=pg_pool,
                        clock=clock,
                        settings=settings,
                    )
                else:
                    result = await rl.acquire(
                        redis_client=redis_client,
                        pg_pool=pg_pool,
                        clock=clock,
                        settings=settings,
                    )
                if not result.allowed:
                    retry_td = (
                        result.retry_after
                        if result.retry_after is not None
                        else DEFAULT_RESERVATION_BACKOFF
                    )
                    logger.info(
                        "composition-denied",
                        job_id=str(job_id),
                        rate_limits=rate_limits,
                        reservations=reservations,
                        allowed=False,
                        retry_after=retry_td,
                        failed_bucket=rl_name,
                    )
                    raise ReservationUnavailable(
                        bucket_name=rl_name,
                        retry_after=retry_td,
                        source="rate_limit",
                    )
                acquired.append(
                    RateLimitHandle(
                        name=rl_name,
                        primitive=rl,
                        decision=result,
                        redis_client=redis_client,
                        pg_pool=pg_pool,
                        clock=clock,
                        settings=settings,
                        count=1.0,
                        refund_on_release=True,
                    )
                )

            logger.debug(
                "composition-acquired",
                job_id=str(job_id),
                rate_limits=rate_limits,
                reservations=reservations,
                allowed=True,
                retry_after=None,
                handle_count=len(acquired),
            )
            return acquired
        except ReservationUnavailable:
            for handle in reversed(acquired):
                try:
                    await handle.release()
                except Exception as exc:
                    backend = (
                        handle.decision.backend
                        if isinstance(handle, RateLimitHandle)
                        else "postgres"
                    )
                    logger.error(
                        "ratelimit-rollback-failure",
                        handle_name=handle.name,
                        operation="release",
                        error=str(exc),
                        acquired_count=len(acquired),
                    )
                    record_ratelimit_refund_failure(handle.name, backend)
            raise

    async def peek(
        self,
        name: str,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> RateLimitState:
        """Look up a rate-limit primitive by name and return its current state."""
        if name in self._reservations:
            raise TypeError(
                f"name {name!r} is a ConcurrencyReservation — "
                f"peek() on reservations is not supported via this method"
            )
        if name not in self._rate_limits:
            raise KeyError(name)

        primitive = self._rate_limits[name]
        if isinstance(primitive, TokenBucket):
            return await primitive.peek(
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )
        else:
            return await primitive.peek(
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )

    async def peek_all(
        self,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> dict[str, RateLimitState]:
        """Peek all registered rate limits. Returns {name: RateLimitState}."""
        results: dict[str, RateLimitState] = {}
        for name, prim in list(self._rate_limits.items()):
            try:
                if isinstance(prim, TokenBucket):
                    results[name] = await prim.peek(
                        redis_client=redis_client,
                        pg_pool=pg_pool,
                        clock=clock,
                        settings=settings,
                    )
                else:
                    results[name] = await prim.peek(
                        redis_client=redis_client,
                        pg_pool=pg_pool,
                        clock=clock,
                        settings=settings,
                    )
            except Exception as exc:
                logger.warning(
                    "ratelimit-peek-failed",
                    bucket_name=name,
                    error=str(exc),
                )
        return results

    async def reset(
        self,
        name: str,
        *,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> None:
        """Reset a rate-limit bucket to full capacity."""
        if name in self._reservations:
            raise TypeError(
                f"name {name!r} is a ConcurrencyReservation — "
                f"reset() on reservations is not supported"
            )
        if name not in self._rate_limits:
            raise KeyError(name)

        primitive = self._rate_limits[name]
        if isinstance(primitive, TokenBucket):
            await primitive.reset(
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )
        else:
            await primitive.reset(
                redis_client=redis_client,
                pg_pool=pg_pool,
                clock=clock,
                settings=settings,
            )

    async def release_for_actor(
        self,
        acquired: list[AcquiredResource],
        *,
        pg_pool: "asyncpg.Pool | None" = None,
    ) -> None:
        """Release acquired resources after actor completion.

        Sets ``refund_on_release=False`` on all ``RateLimitHandle`` instances
        before iterating (token consumption is permanent after actor ran).
        Releases in reverse acquisition order.  Each release failure is caught,
        logged at ERROR, and loop continues (same pattern as rollback).
        """
        for handle in acquired:
            if isinstance(handle, RateLimitHandle):
                handle.refund_on_release = False

        for handle in reversed(acquired):
            try:
                await handle.release()
            except Exception as exc:
                backend = (
                    handle.decision.backend if isinstance(handle, RateLimitHandle) else "postgres"
                )
                logger.error(
                    "ratelimit-rollback-failure",
                    handle_name=handle.name,
                    operation="release",
                    error=str(exc),
                    acquired_count=len(acquired),
                )
                record_ratelimit_refund_failure(handle.name, backend)

    def evict_idle_keyed_reservations(self, idle_for: "timedelta") -> int:
        """Drop registry entries for keyed reservations idle at least ``idle_for``.

        Reservations derived from a :class:`KeyedReservationRef` are
        registered lazily and never removed automatically — under high key
        cardinality (e.g. one reservation per import session over a long
        worker lifetime) this dict grows without bound. The leader sweep
        calls this automatically with a 1-hour idle threshold; call directly
        for custom eviction windows.

        Only removes the in-memory registry entry and its
        acquire-recency tracking — it does NOT touch the underlying
        Postgres ``reservation_slots`` rows for that name; those are
        already reclaimed independently by the existing lock-expiry sweep.
        A key that is acquired again after eviction is simply
        re-registered on next use (idempotent — see
        :meth:`_resolve_reservation_name`), so eviction is always safe to
        call, including concurrently with in-flight acquisitions for
        other keys.

        Returns the number of entries evicted.
        """
        cutoff = monotonic() - idle_for.total_seconds()
        stale = [
            name
            for name, last_used in self._keyed_reservation_last_used.items()
            if last_used < cutoff
        ]
        for name in stale:
            self._reservations.pop(name, None)
            del self._keyed_reservation_last_used[name]
        if stale:
            logger.debug("registry-evicted-idle-keyed-reservations", count=len(stale))
        return len(stale)


async def sync_rate_limit_buckets(
    rl_registry: RateLimitRegistry,
    pool: "asyncpg.Pool",
    *,
    schema: str = "taskq",
) -> None:
    """Publish every registered rate limit to ``rate_limit_buckets``.

    Each worker calls this at startup so the admin UI can discover
    configured buckets from PG without depending on the in-memory
    singleton being populated in the admin process.

    Uses ``ON CONFLICT DO NOTHING`` so concurrent workers and restarts
    are idempotent.  Only PG-backed primitives are written; memory-only
    and log-style sliding windows (which have no PG backend) are skipped.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    upsert_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608
        f"VALUES ($1, $2, '{{}}'::jsonb, now()) "
        f"ON CONFLICT (bucket_name) DO NOTHING"
    )

    for name, prim in rl_registry.rate_limits.items():
        if isinstance(prim, TokenBucket):
            kind = "token_bucket"
        else:
            if prim.style == "gcra":
                kind = "gcra"
            else:
                continue

        async with pool.acquire() as conn:
            await conn.execute(upsert_sql, name, kind)

        logger.debug(
            "rl-bucket-synced",
            bucket_name=name,
            kind=kind,
        )


registry = RateLimitRegistry()

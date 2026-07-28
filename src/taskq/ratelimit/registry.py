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
permanently. Both ``reservations`` and ``rate_limits`` entries may be plain
names (statically pre-registered) or :class:`KeyedReservationRef` /
:class:`KeyedRateLimitRef` instances that lazily materialize a per-key
primitive from the job payload on first acquisition; registry growth from
high key cardinality is bounded by the leader-sweep eviction methods.

Every worker process unconditionally participates in leader election
(see ``src/taskq/worker/_bootstrap.py`` — ``MaintenanceLeader`` is
started inside the main ``TaskGroup`` without any settings flag), so the
30-second sweep that calls ``evict_idle_keyed_reservations`` /
``evict_idle_keyed_rate_limits`` always eventually runs in any topology
that is capable of materializing keyed primitives in the first place
(keyed materialisation only happens from a worker's job-dispatch path,
and that worker — or a peer — always wins leader election within the
documented failover SLA).  This is not a silent-forever-leak bug.

As a defence-in-depth measure, the acquisition path
(``_resolve_reservation_name`` / ``_resolve_rate_limit_name``) also
performs an *opportunistic* eviction when the keyed-entry cap would
otherwise be hit — so reclaiming idle capacity never depends solely on
sweep timing.  The opportunistic scan is amortized to at most one per
``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` (30 s), so a registry at cap under
sustained denials stays O(1) per request instead of rescanning the whole
tracking dict on every denied acquisition; idle capacity is still
reclaimed within the sweep's own 30-second SLA.  A cap hit after
opportunistic eviction is a genuine sustained-high-cardinality denial,
not an artefact of when the sweep last ran.

Over-acquisition window on rollback failure:

- TokenBucket (Redis): ``ceil(capacity / refill_per_second * 2) + 60`` seconds
  (the ``EXPIRE`` TTL).
- SlidingWindow (Redis): ``2 * window_ms + 60_000`` ms (the ``PEXPIRE`` TTL
  ).
- ConcurrencyReservation (PG): ``lease_duration`` — reclaimed by sweep 4
  within 30 seconds at most.
"""

from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, TypeVar

import structlog

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    _KEYED_KEY_RE,  # pyright: ignore[reportPrivateUsage]
    _MAX_KEYED_KEY_LEN,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_RESERVATION_BACKOFF,
    QUEUE_CONCURRENCY_PREFIX,
)
from taskq.exceptions import ReservationUnavailable
from taskq.obs import record_ratelimit_refund_failure
from taskq.ratelimit.composition import (
    AcquiredResource,
    RateLimitHandle,
    ReservationHandle,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket

if TYPE_CHECKING:
    from uuid import UUID

    import asyncpg
    import redis.asyncio as redis_async

    from taskq.backend.clock import Clock
    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit.registry")

# QUEUE_CONCURRENCY_PREFIX is defined in taskq.constants (imported above)
# and re-exported here for backwards compatibility with existing imports
# from this module.

__all__ = [
    "QUEUE_CONCURRENCY_PREFIX",
    "RateLimitRegistry",
    "queue_concurrency_reservation_name",
    "registry",
    "sync_rate_limit_buckets",
]

_P = TypeVar("_P")

_KEYED_IDLE_THRESHOLD = timedelta(hours=1)
"""Idle duration before a keyed entry is eligible for eviction.

Used both by the leader sweep (``_leader_sweeps.py``) and by the
opportunistic eviction on the acquisition path in
:meth:`~RateLimitRegistry._resolve_reservation_name` /
:meth:`~RateLimitRegistry._resolve_rate_limit_name`. Centralised here so
the two paths never drift apart.
"""

_OPPORTUNISTIC_EVICT_MIN_INTERVAL = timedelta(seconds=30)
"""Minimum interval between opportunistic eviction scans on the acquire path.

The scan itself is O(number of tracked keyed entries). Without a gate, a
registry sitting at its keyed-entry cap under sustained denials would pay
that O(n) scan on EVERY denied acquisition — at the default 10k-entry cap
and 1k denials/sec that is ~10M dict entries scanned per second on the
hottest path in the system, reclaiming nothing. Entries only become
evictable as wall-clock time passes (idle ≥ ``_KEYED_IDLE_THRESHOLD``),
so rescanning more often than the leader sweep's own 30-second cadence
buys nothing: a gated scan reclaims idle capacity at most 30 s after it
became reclaimable, identical to the sweep's documented SLA. The gate
makes the denied-cap-hit path O(1) amortized while preserving the
defence-in-depth guarantee that reclaiming idle capacity never depends
solely on sweep timing.
"""


def queue_concurrency_reservation_name(queue: str) -> str:
    """Return the registry name for the fleet-wide concurrency cap of *queue*.

    The ``taskq:global:queue:`` prefix namespaces these internally-generated
    reservations apart from user-declared ones.  A reservation only exists
    in the registry for a queue if that queue's ``max_concurrent`` column
    was set in the ``queues`` table (read from Postgres at worker startup);
    there is no other config source. All workers sharing the schema
    register and acquire against the same PG ``reservation_slots`` rows,
    giving a true fleet-wide cap per queue.
    """
    return f"{QUEUE_CONCURRENCY_PREFIX}{queue}"


def _ref_display(ref: "str | KeyedRateLimitRef | KeyedReservationRef") -> str:
    """Log-safe display string for a rate-limit / reservation ref.

    Refs are pydantic ``BaseModel``s, which orjson (the structlog JSON
    serializer used in production) cannot serialize — passing a ref
    instance as a log kwarg raises ``TypeError`` inside the logging
    handler and the event is silently dropped. Plain names pass through
    unchanged; refs render as ``ClassName(base_name)``.
    """
    if isinstance(ref, str):
        return ref
    return f"{type(ref).__name__}({ref.base_name})"


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
        return (
            a.name == b.name and a.slots == b.slots and a.lease == b.lease and a.schema == b.schema
        )
    return False


def _preserves_memory_fixed_quota_state(prim: TokenBucket | SlidingWindow) -> bool:
    """Eviction-exemption predicate for keyed rate limits.

    Exempts a memory-backed fixed-quota TokenBucket holding consumed quota
    state (see :meth:`TokenBucket.holds_consumed_memory_quota`) from idle
    eviction — evicting it would silently reset the drained quota.
    """
    return isinstance(prim, TokenBucket) and prim.holds_consumed_memory_quota()


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
        # Names of rate limits materialized from a KeyedRateLimitRef
        # (as opposed to a static @actor(rate_limits=["name"]) entry),
        # and the monotonic time each was last acquired — used only by
        # evict_idle_keyed_rate_limits() to bound registry growth under
        # high key cardinality. Never consulted by acquire_for_actor.
        self._keyed_rate_limit_last_used: dict[str, float] = {}
        # Monotonic timestamps of the last opportunistic eviction scan on
        # each acquisition path, used to amortize the O(n) scan to at most
        # once per _OPPORTUNISTIC_EVICT_MIN_INTERVAL under sustained cap-hit
        # denials (see the constant's docstring). -inf so the first cap-hit
        # after startup always scans. evict_idle_keyed_*() are synchronous
        # with no await points, so check-and-stamp is atomic within the
        # event loop — concurrent scanners cannot pile up.
        self._keyed_reservation_last_eviction_scan: float = float("-inf")
        self._keyed_rate_limit_last_eviction_scan: float = float("-inf")

    @property
    def rate_limits(self) -> dict[str, TokenBucket | SlidingWindow]:
        return dict(self._rate_limits)

    @property
    def reservations(self) -> dict[str, ConcurrencyReservation]:
        return dict(self._reservations)

    @property
    def has_keyed_reservations(self) -> bool:
        return bool(self._keyed_reservation_last_used)

    @property
    def has_keyed_rate_limits(self) -> bool:
        return bool(self._keyed_rate_limit_last_used)

    def has_reservation(self, name: str) -> bool:
        """O(1) membership test against the live reservations dict.

        Unlike the :attr:`reservations` property this does NOT defensively
        copy the dict — use it on per-job hot paths (e.g. the dispatch
        queue-cap check), where copying the whole registry per call is
        prohibitive at high keyed-entry cardinality.
        """
        return name in self._reservations

    def has_rate_limit(self, name: str) -> bool:
        """O(1) membership test against the live rate-limits dict.

        See :meth:`has_reservation` — the same no-copy guarantee applies.
        """
        return name in self._rate_limits

    def register(
        self,
        primitive: TokenBucket | SlidingWindow | ConcurrencyReservation,
    ) -> None:
        if primitive.name.startswith(QUEUE_CONCURRENCY_PREFIX):
            raise ValueError(
                f"name {primitive.name!r} starts with the reserved prefix "
                f"{QUEUE_CONCURRENCY_PREFIX!r} — internal queue-cap reservations "
                f"must be registered via register_queue_cap_reservation()"
            )
        if isinstance(primitive, ConcurrencyReservation):
            self._register_reservation_unchecked(primitive)
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

    def _register_reservation_unchecked(
        self,
        primitive: ConcurrencyReservation,
    ) -> None:
        """Idempotent registration of a ConcurrencyReservation.

        Called by both :meth:`register` (after the reserved-prefix rejection
        check) and :meth:`register_queue_cap_reservation` (after the
        reserved-prefix assertion). Duplicate-name-with-different-config →
        ``ValueError``; duplicate-name-with-same-config → idempotent no-op.
        """
        name = primitive.name
        existing = self._reservations.get(name)
        if existing is not None:
            if _same_config(existing, primitive):
                logger.debug(
                    "registry-register-idempotent-noop",
                    kind="reservation",
                    name=name,
                )
                return
            raise ValueError(
                f"reservation name already registered with a different config: "
                f"{name!r} — existing={existing!r}, new={primitive!r}"
            )
        self._reservations[name] = primitive
        logger.debug(
            "registry-registered",
            kind="reservation",
            name=name,
        )

    def register_queue_cap_reservation(
        self,
        reservation: ConcurrencyReservation,
    ) -> None:
        """Register a fleet-wide queue-cap reservation in the reserved namespace.

        This is the ONLY way to register a reservation whose name starts with
        :data:`QUEUE_CONCURRENCY_PREFIX`. The public :meth:`register` rejects
        such names to prevent users from accidentally shadowing internal
        queue caps. Idempotency and conflict detection are identical to
        :meth:`register` (duplicate-name-with-different-config →
        ``ValueError``; duplicate-name-with-same-config → idempotent no-op).
        """
        if not reservation.name.startswith(QUEUE_CONCURRENCY_PREFIX):
            raise ValueError(
                f"register_queue_cap_reservation() requires a name starting with "
                f"{QUEUE_CONCURRENCY_PREFIX!r}, got {reservation.name!r}"
            )
        self._register_reservation_unchecked(reservation)

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

    def _validate_keyed_key(
        self,
        key: object,
        ref_repr: str,
        payload: dict[str, object] | None,
        *,
        empty_key_msg: str = "an empty or non-string key",
    ) -> str:
        """Validate a ``key_fn`` return value and return it as a ``str``.

        Raises ``ValueError`` if *key* is not a non-empty ``str``, exceeds
        ``_MAX_KEYED_KEY_LEN`` characters, or contains characters outside
        ``_KEYED_KEY_RE``.  *ref_repr* is included in error messages to
        identify which ref type produced the invalid key.  *empty_key_msg*
        controls the wording of the empty/non-string error (the two
        call-sites historically used slightly different phrasing).

        ``isinstance`` (not an exact-type check) accepts ``str``
        subclasses — a ``class Tenant(str, Enum)`` member or a domain
        wrapper deriving from ``str`` is a natural ``key_fn`` return value
        and behaves identically to a plain ``str`` for namespacing, Redis
        keys, and dict lookups.
        """
        if not isinstance(key, str) or not key:
            raise ValueError(f"{ref_repr}.key_fn returned {empty_key_msg} for payload {payload!r}")
        if len(key) > _MAX_KEYED_KEY_LEN:
            raise ValueError(
                f"{ref_repr}.key_fn returned "
                f"a key of length {len(key)} which exceeds the maximum of "
                f"{_MAX_KEYED_KEY_LEN} characters"
            )
        if not _KEYED_KEY_RE.match(key):
            raise ValueError(
                f"{ref_repr}.key_fn returned "
                f"key {key!r} which contains characters outside the allowed set "
                f"[A-Za-z0-9_\\-:.]"
            )
        # Normalize to a base ``str``: ``f"{base_name}:{key}"`` at the
        # call sites would render a str-Enum member via ``Enum.__format__``
        # (``'Tenant.ACME'``) instead of its value (``'acme'``), and the
        # registry should key on the canonical plain-string form. A full
        # slice returns a base ``str`` with identical content.
        return key[:]

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
        first use — subsequent calls for the same key reuse it. Two reuse
        cases are distinguished:

        - **Keyed-materialized entry** (tracked in
          ``_keyed_reservation_last_used``): recency is refreshed, and the
          existing entry's ``slots``/``lease`` are checked against the
          ref's — a mismatch means two refs collided on the same concrete
          name with different configs (one ref's ``base_name`` is a prefix
          of the other's concrete name, since ``:`` is an allowed key
          character), which raises ``ValueError`` rather than silently
          over- or under-admitting relative to one ref's declared config.
          The guard covers live tracked entries only: if the colliding
          entry was idle-evicted in between, the second ref re-materializes
          its own config without error — eviction resets the guard.
        - **Statically pre-registered entry** (not tracked): reused as-is,
          and deliberately NOT stamped into ``_keyed_reservation_last_used``
          so the idle-eviction sweep can never evict a user's static entry.

        The ``key_fn`` return value is validated: it must be non-empty, at
        most ``_MAX_KEYED_KEY_LEN`` characters, and match
        ``_KEYED_KEY_RE`` (alphanumeric plus ``_ - : .``) — this
        prevents control characters in PG text columns and bounds storage
        growth from attacker-controlled keys. When ``settings`` is provided
        and the number of tracked keyed reservations reaches
        ``settings.max_keyed_reservations``, a new key raises
        :class:`~taskq.exceptions.ReservationUnavailable`.

        Capacity is normally reclaimed by the leader's 30-second sweep
        (``evict_idle_keyed_reservations``).  However, an acquisition that
        would otherwise be denied purely because idle entries haven't been
        swept yet gets one *opportunistic* eviction attempt first — so
        hitting the cap is never purely an artefact of sweep timing, only a
        genuine sustained-high-cardinality condition.  Only if the cap is
        still exceeded after the opportunistic eviction does the method
        raise :class:`~taskq.exceptions.ReservationUnavailable`.  The
        opportunistic scan is amortized to at most one per
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` so sustained cap-hit denials
        stay O(1) on this hot path (see
        :meth:`_opportunistic_evict_reservations`).

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
        key = self._validate_keyed_key(
            ref.key_fn(payload),
            f"KeyedReservationRef(base_name={ref.base_name!r})",
            payload,
            empty_key_msg="an empty key or non-string value",
        )
        concrete_name = f"{ref.base_name}:{key}"
        # The cap bounds keyed-materialized GROWTH. It must not fire when
        # the concrete name already exists — neither for a tracked keyed
        # entry (recency refresh grows nothing) nor for a statically
        # pre-registered entry (reused as-is, never tracked, grows nothing).
        if (
            concrete_name not in self._reservations
            and concrete_name not in self._keyed_reservation_last_used
            and settings is not None
            and len(self._keyed_reservation_last_used) >= settings.max_keyed_reservations
        ):
            self._opportunistic_evict_reservations()
            if len(self._keyed_reservation_last_used) >= settings.max_keyed_reservations:
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
            # Stamp BEFORE the ensure_slots await so that a concurrent
            # evict_idle_keyed_reservations cannot evict the in-flight
            # key; re-stamp after the await in case an aggressive eviction
            # removed both entries anyway (belt-and-suspenders).
            self._keyed_reservation_last_used[concrete_name] = monotonic()
            if pg_pool is not None:
                try:
                    await new_reservation.ensure_slots(pg_pool)
                except Exception:
                    # Unwind the materialization: leaving the entry
                    # registered would poison this key permanently — the
                    # reuse branch below would skip ensure_slots forever,
                    # acquire() would find no slot rows and keep denying,
                    # and each attempt would re-stamp recency so the entry
                    # is never idle-evicted either. ensure_slots is
                    # idempotent (ON CONFLICT DO NOTHING), so the next
                    # attempt simply re-materializes and retries.
                    self._reservations.pop(concrete_name, None)
                    self._keyed_reservation_last_used.pop(concrete_name, None)
                    raise
                if concrete_name not in self._reservations:
                    self.register(new_reservation)
                self._keyed_reservation_last_used[concrete_name] = monotonic()
        elif concrete_name in self._keyed_reservation_last_used:
            # Keyed-materialized entry reused for the same concrete name:
            # refresh recency, and guard against a concrete-name COLLISION
            # between two refs with different configs (one ref's base_name
            # can be a prefix of another's concrete name since ':' is an
            # allowed key character). Silently reusing the existing entry
            # would over- or under-admit relative to the colliding ref's
            # declared config — fail loudly instead.
            existing = self._reservations[concrete_name]
            if existing.slots != ref.slots or existing.lease != ref.lease:
                raise ValueError(
                    f"KeyedReservationRef(base_name={ref.base_name!r}) resolved to "
                    f"{concrete_name!r}, which is already materialized with a different "
                    f"config (existing slots={existing.slots}, lease={existing.lease}; "
                    f"ref declares slots={ref.slots}, lease={ref.lease}) — concrete-name "
                    f"collision between keyed refs; choose distinct base_names "
                    f"(':' in keys can make one ref's base_name a prefix of another's "
                    f"concrete name)"
                )
            self._keyed_reservation_last_used[concrete_name] = monotonic()
        # else: the concrete name was STATICALLY pre-registered (not keyed-
        # materialized) — reuse it as-is and never stamp the tracking dict,
        # so the sweep can never evict a user's static entry.
        return concrete_name

    async def _resolve_rate_limit_name(
        self,
        ref: "str | KeyedRateLimitRef",
        payload: dict[str, object] | None,
        *,
        settings: "WorkerSettings | None",
        pg_pool: "asyncpg.Pool | None" = None,
    ) -> str:
        """Return the concrete registry name for *ref*.

        Mirrors :meth:`_resolve_reservation_name` for rate limits. A plain
        ``str`` is returned as-is (must already be registered via
        :meth:`register`). A :class:`KeyedRateLimitRef` derives
        ``f"{ref.base_name}:{key}"`` by calling ``ref.key_fn(payload)`` and
        lazily registers a matching :class:`TokenBucket` on first use —
        subsequent calls for the same key reuse it. As in
        :meth:`_resolve_reservation_name`, two reuse cases are
        distinguished: a keyed-materialized (tracked) entry has its
        recency refreshed and its config checked against the ref's — a
        ``capacity``/``refill_per_second``/``backend`` mismatch means a
        concrete-name collision between refs and raises ``ValueError``;
        a statically pre-registered (untracked) entry is reused as-is and
        never stamped, so the idle-eviction sweep can never evict a user's
        static entry.

        The ``key_fn`` return value is validated with the same rules as
        keyed reservations: it must be a ``str``, non-empty, at most
        ``_MAX_KEYED_KEY_LEN`` characters, and match
        ``_KEYED_KEY_RE`` (alphanumeric plus ``_ - : .``). A
        ``key_fn`` that returns ``None`` or any non-``str`` value is treated
        as an invalid key and raises ``ValueError`` — a broken ``key_fn``
        can never silently resolve to a shared/global bucket. When
        ``settings`` is provided and the number of tracked keyed rate
        limits reaches ``settings.max_keyed_rate_limits``, a new key
        raises :class:`~taskq.exceptions.ReservationUnavailable`.

        Capacity is normally reclaimed by the leader's 30-second sweep
        (``evict_idle_keyed_rate_limits``).  However, an acquisition that
        would otherwise be denied purely because idle entries haven't been
        swept yet gets one *opportunistic* eviction attempt first — so
        hitting the cap is never purely an artefact of sweep timing, only a
        genuine sustained-high-cardinality condition.  Only if the cap is
        still exceeded after the opportunistic eviction does the method
        raise :class:`~taskq.exceptions.ReservationUnavailable`.  The
        opportunistic scan is amortized to at most one per
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` so sustained cap-hit denials
        stay O(1) on this hot path (see
        :meth:`_opportunistic_evict_rate_limits`).

        Unlike reservations there is no PG slot pre-allocation step — a
        :class:`TokenBucket` is immediately usable after ``register()``
        (there is no ``ensure_slots`` equivalent). Note that when the
        underlying ``TokenBucket`` uses the Redis backend, per-key Redis
        memory is already self-bounding via the Lua script's ``EXPIRE`` TTL
        on the bucket's hash; :meth:`evict_idle_keyed_rate_limits` only
        bounds this Python-process-local registry dict, not Redis itself.
        These are two independent growth bounds.

        On materialization with a ``pg_pool`` available, the new bucket is
        also published to the ``rate_limit_buckets`` table (best-effort,
        idempotent) so the admin UI can surface keyed buckets discovered
        after worker startup — see the inline note at the publish site.
        """
        if isinstance(ref, str):
            return ref

        if payload is None:
            raise ValueError(
                f"rate limit {ref.base_name!r} is a KeyedRateLimitRef but no "
                "payload was provided to derive its key from"
            )
        key = self._validate_keyed_key(
            ref.key_fn(payload),
            f"KeyedRateLimitRef(base_name={ref.base_name!r})",
            payload,
        )
        concrete_name = f"{ref.base_name}:{key}"
        # The cap bounds keyed-materialized GROWTH. It must not fire when
        # the concrete name already exists — neither for a tracked keyed
        # entry (recency refresh grows nothing) nor for a statically
        # pre-registered entry (reused as-is, never tracked, grows nothing).
        if (
            concrete_name not in self._rate_limits
            and concrete_name not in self._keyed_rate_limit_last_used
            and settings is not None
            and len(self._keyed_rate_limit_last_used) >= settings.max_keyed_rate_limits
        ):
            self._opportunistic_evict_rate_limits()
            if len(self._keyed_rate_limit_last_used) >= settings.max_keyed_rate_limits:
                logger.warning(
                    "registry-keyed-rate-limit-limit-exceeded",
                    base_name=ref.base_name,
                    current_count=len(self._keyed_rate_limit_last_used),
                    limit=settings.max_keyed_rate_limits,
                )
                raise ReservationUnavailable(
                    bucket_name=ref.base_name,
                    retry_after=DEFAULT_RESERVATION_BACKOFF,
                    source="rate_limit",
                )
        if concrete_name not in self._rate_limits:
            schema = settings.schema_name if settings is not None else "taskq"
            new_bucket = TokenBucket(
                name=concrete_name,
                capacity=ref.capacity,
                refill_per_second=ref.refill_per_second,
                backend=ref.backend,
            )
            self.register(new_bucket)
            self._keyed_rate_limit_last_used[concrete_name] = monotonic()
            # Publish the freshly-materialized bucket to PG (best-effort)
            # so the admin UI's rate-limits page surfaces it in ANY
            # topology: statically registered buckets are published at
            # worker startup by sync_rate_limit_buckets, but a keyed bucket
            # materialized long after startup would otherwise be invisible
            # to a standalone admin process — whose registry singleton
            # never dispatches jobs — making an active per-tenant throttle
            # look like "no limiter configured". Unlike ensure_slots for
            # keyed reservations (a correctness precondition for acquire),
            # this row is observability metadata, so a publish failure
            # must NOT fail the acquisition — warn and continue.
            if pg_pool is not None:
                try:
                    await _upsert_rate_limit_bucket_row(
                        pg_pool, schema, concrete_name, "token_bucket"
                    )
                except Exception:
                    logger.warning(
                        "keyed-rate-limit-bucket-publish-failed",
                        bucket_name=concrete_name,
                        exc_info=True,
                    )
        elif concrete_name in self._keyed_rate_limit_last_used:
            # Keyed-materialized entry reused for the same concrete name:
            # refresh recency, and guard against a concrete-name COLLISION
            # between two refs with different configs (see the reservation
            # twin in _resolve_reservation_name for the full rationale).
            existing = self._rate_limits[concrete_name]
            if (
                not isinstance(existing, TokenBucket)
                or existing.capacity != ref.capacity
                or existing.refill_per_second != ref.refill_per_second
                or existing.backend != ref.backend
            ):
                existing_config = (
                    f"capacity={existing.capacity}, refill_per_second="
                    f"{existing.refill_per_second}, backend={existing.backend}"
                    if isinstance(existing, TokenBucket)
                    else f"SlidingWindow(limit={existing.limit}, window={existing.window})"
                )
                raise ValueError(
                    f"KeyedRateLimitRef(base_name={ref.base_name!r}) resolved to "
                    f"{concrete_name!r}, which is already materialized with a different "
                    f"config (existing {existing_config}; ref declares "
                    f"capacity={ref.capacity}, refill_per_second={ref.refill_per_second}, "
                    f"backend={ref.backend}) — concrete-name collision between keyed "
                    f"refs; choose distinct base_names "
                    f"(':' in keys can make one ref's base_name a prefix of another's "
                    f"concrete name)"
                )
            self._keyed_rate_limit_last_used[concrete_name] = monotonic()
        # else: the concrete name was STATICALLY pre-registered (not keyed-
        # materialized) — reuse it as-is and never stamp the tracking dict,
        # so the sweep can never evict a user's static entry.
        return concrete_name

    async def acquire_for_actor(
        self,
        rate_limits: Sequence["str | KeyedRateLimitRef | TokenBucket | SlidingWindow"],
        reservations: Sequence["str | KeyedReservationRef | ConcurrencyReservation"],
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
        statically pre-registered primitives), :class:`KeyedReservationRef`
        instances (resolved dynamically per job from ``payload`` — see
        :meth:`_resolve_reservation_name`), or
        :class:`ConcurrencyReservation` instances (normalized to their
        ``.name`` up front — the instance must already be registered,
        e.g. by the worker bootstrap's actor-declaration collection
        pass; an unregistered instance raises ``KeyError`` exactly like
        an unknown name). ``rate_limits`` entries may likewise be plain
        names, :class:`KeyedRateLimitRef` instances, or
        :class:`TokenBucket` / :class:`SlidingWindow` instances.
        ``payload`` is required if any entry is a ``KeyedReservationRef``
        or ``KeyedRateLimitRef``.

        Returns the list of ``AcquiredResource`` handles on full success.
        Raises ``ReservationUnavailable`` on any denial — rollback is performed
        internally before re-raising (already-acquired resources released in
        reverse order, each failure logged at ERROR).
        """
        # Normalize primitive instances to their names BEFORE any use —
        # _ref_display (below) only handles str | keyed refs and would
        # AttributeError on a primitive instance, and every dict lookup
        # and handle construction sees names only. No acquisition-time
        # auto-registration: bootstrap is the fail-fast point; an
        # unregistered instance raises KeyError from the dict lookups below.
        rl_seq: list[str | KeyedRateLimitRef] = [
            rl.name if isinstance(rl, TokenBucket | SlidingWindow) else rl for rl in rate_limits
        ]
        res_seq: list[str | KeyedReservationRef] = [
            res.name if isinstance(res, ConcurrencyReservation) else res for res in reservations
        ]
        acquired: list[AcquiredResource] = []
        try:
            for res_ref in res_seq:
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

            for rl_ref in rl_seq:
                rl_name = await self._resolve_rate_limit_name(
                    rl_ref, payload, settings=settings, pg_pool=pg_pool
                )
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
                        rate_limits=[_ref_display(r) for r in rl_seq],
                        reservations=[_ref_display(r) for r in res_seq],
                        allowed=False,
                        retry_after_seconds=retry_td.total_seconds(),
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
                rate_limits=[_ref_display(r) for r in rl_seq],
                reservations=[_ref_display(r) for r in res_seq],
                allowed=True,
                retry_after=None,
                handle_count=len(acquired),
            )
            return acquired
        except Exception:
            # CancelledError deliberately bypasses this rollback:
            # asyncio.CancelledError derives from BaseException, not
            # Exception, so a cancellation landing mid-composition leaves
            # any already-acquired handles in place. That is an accepted,
            # bounded, self-healing leak — NOT an oversight: leaked
            # reservation slots are reclaimed by lease expiry (the
            # lock-expiry sweep, within ~30s), and consumed rate-limit
            # tokens are bounded by the bucket's Redis EXPIRE TTL. Rolling
            # back here would mean network I/O (handle.release()) while the
            # task is being torn down — delaying cancellation, with a
            # second cancel able to interrupt the release itself — which is
            # worse than a leak with an existing reclaim path.
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

    def _opportunistic_evict_reservations(self) -> None:
        """Idle-reservation scan, amortized to one per min-interval.

        Called on the acquisition path when the keyed-reservation cap is
        hit. The scan is O(tracked entries); without amortization a
        registry at cap under sustained denials would pay O(n) per denied
        request and reclaim nothing (see
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL``). Idle capacity is still
        reclaimed within max(sweep cadence, min-interval) of becoming
        reclaimable — the scan just can't be stampeded.
        """
        now = monotonic()
        if (
            now - self._keyed_reservation_last_eviction_scan
            >= _OPPORTUNISTIC_EVICT_MIN_INTERVAL.total_seconds()
        ):
            self._keyed_reservation_last_eviction_scan = now
            self.evict_idle_keyed_reservations(idle_for=_KEYED_IDLE_THRESHOLD)

    def _opportunistic_evict_rate_limits(self) -> None:
        """Idle-rate-limit scan, amortized to one per min-interval.

        Rate-limit twin of :meth:`_opportunistic_evict_reservations`.
        """
        now = monotonic()
        if (
            now - self._keyed_rate_limit_last_eviction_scan
            >= _OPPORTUNISTIC_EVICT_MIN_INTERVAL.total_seconds()
        ):
            self._keyed_rate_limit_last_eviction_scan = now
            self.evict_idle_keyed_rate_limits(idle_for=_KEYED_IDLE_THRESHOLD)

    def _evict_idle_keyed(
        self,
        tracking_dict: dict[str, float],
        primitive_dict: dict[str, _P],
        idle_for: timedelta,
        event_name: str,
        *,
        preserve: Callable[[_P], bool] | None = None,
    ) -> int:
        """Evict stale entries from *tracking_dict* and *primitive_dict*.

        Removes entries whose ``last_used`` timestamp is older than
        ``monotonic() - idle_for`` from both dicts, logs *event_name*
        with the evicted count, and returns that count.

        *preserve*, when given, exempts an entry from eviction when the
        predicate returns True for its primitive — the entry keeps its
        tracking timestamp and is re-scanned on the next sweep. Use for
        primitives whose in-instance state eviction would destroy
        irrecoverably (see :meth:`evict_idle_keyed_rate_limits`).
        """
        cutoff = monotonic() - idle_for.total_seconds()
        stale: list[str] = []
        for name, last_used in tracking_dict.items():
            if last_used >= cutoff:
                continue
            prim = primitive_dict.get(name)
            if preserve is not None and prim is not None and preserve(prim):
                continue
            stale.append(name)
        for name in stale:
            primitive_dict.pop(name, None)
            del tracking_dict[name]
        if stale:
            logger.debug(event_name, count=len(stale))
        return len(stale)

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
        return self._evict_idle_keyed(
            self._keyed_reservation_last_used,
            self._reservations,
            idle_for,
            "registry-evicted-idle-keyed-reservations",
        )

    def evict_idle_keyed_rate_limits(self, idle_for: "timedelta") -> int:
        """Drop registry entries for keyed rate limits idle at least ``idle_for``.

        Rate limits derived from a :class:`KeyedRateLimitRef` are registered
        lazily and never removed automatically — under high key cardinality
        (e.g. one token bucket per tenant over a long worker lifetime) this
        dict grows without bound. The leader sweep calls this automatically
        with a 1-hour idle threshold; call directly for custom eviction
        windows.

        Only removes the in-memory registry entry and its acquire-recency
        tracking — it does NOT touch the underlying Redis hash for that
        bucket; per-key Redis memory is already self-bounding via the Lua
        script's ``EXPIRE`` TTL on the bucket's hash (see
        :meth:`_resolve_rate_limit_name`). A key that is acquired again
        after eviction is simply re-registered on next use (idempotent — see
        :meth:`_resolve_rate_limit_name`), so eviction is always safe to
        call, including concurrently with in-flight acquisitions for other
        keys. Token buckets are not automatically removed from Redis; their
        independent TTL handles that side.

        **Exemption — memory fixed-quota buckets.** A ``backend="memory"``
        bucket with ``refill_per_second == 0`` that has consumed any of its
        quota is NOT evicted (see
        :meth:`TokenBucket.holds_consumed_memory_quota`): its token state
        lives on the instance, so eviction would silently reset the drained
        quota to full on next acquire — whereas Redis deliberately retains
        that same state for 24 h. The exemption applies to both callers of
        this method (the leader sweep and the cap-pressure opportunistic
        eviction). Trade-off, deliberately chosen: an exempt bucket counts
        against ``settings.max_keyed_rate_limits`` until its quota returns
        to full (refund/reset) or the process restarts, so under sustained
        high-cardinality fixed-quota keys the cardinality cap can
        permanently fill and deny NEW keys — the cap fails CLOSED with a
        warning rather than silently resetting quotas, which is the correct
        failure direction for a limiter.

        Returns the number of entries evicted.
        """
        return self._evict_idle_keyed(
            self._keyed_rate_limit_last_used,
            self._rate_limits,
            idle_for,
            "registry-evicted-idle-keyed-rate-limits",
            preserve=_preserves_memory_fixed_quota_state,
        )

    def clear(self) -> None:
        """Reset ALL mutable registry state — a test aid, NOT safe while running.

        Clears the four dicts (``_rate_limits``, ``_reservations``,
        ``_keyed_reservation_last_used``, ``_keyed_rate_limit_last_used``)
        AND resets the two opportunistic-eviction scan timestamps
        (``_keyed_reservation_last_eviction_scan`` /
        ``_keyed_rate_limit_last_eviction_scan``) to ``float("-inf")``.
        Omitting the timestamps would leave the opportunistic-eviction
        throttle stamped, silently suppressing scans for up to
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` (30 s) in the next test.

        **Not safe to call while a worker is running** — concurrent
        dispatch / sweep iteration over the dicts would observe
        inconsistent state. Use for per-test isolation only.
        """
        self._rate_limits.clear()
        self._reservations.clear()
        self._keyed_reservation_last_used.clear()
        self._keyed_rate_limit_last_used.clear()
        self._keyed_reservation_last_eviction_scan = float("-inf")
        self._keyed_rate_limit_last_eviction_scan = float("-inf")


async def _upsert_rate_limit_bucket_row(
    pool: "asyncpg.Pool",
    schema: str,
    name: str,
    kind: str,
) -> None:
    """Insert one ``rate_limit_buckets`` row (idempotent).

    Shared by :func:`sync_rate_limit_buckets` (startup bulk publish of
    statically registered primitives) and the keyed-materialization path
    in :meth:`RateLimitRegistry._resolve_rate_limit_name` (publish on
    first acquisition), so both write identical rows.

    Uses ``ON CONFLICT DO NOTHING`` so concurrent workers and restarts
    are idempotent.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    upsert_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608
        f"VALUES ($1, $2, '{{}}'::jsonb, now()) "
        f"ON CONFLICT (bucket_name) DO NOTHING"
    )
    async with pool.acquire() as conn:
        await conn.execute(upsert_sql, name, kind)


async def sync_rate_limit_buckets(
    rl_registry: RateLimitRegistry,
    pool: "asyncpg.Pool",
    *,
    schema: str = "taskq",
) -> None:
    """Publish every registered rate limit to ``rate_limit_buckets``.

    Each worker calls this at startup so the admin UI can discover
    configured buckets from PG without depending on the in-memory
    singleton being populated in the admin process.  Keyed buckets
    materialized lazily AFTER startup are published individually by the
    acquisition path — see
    :meth:`RateLimitRegistry._resolve_rate_limit_name`.

    Uses ``ON CONFLICT DO NOTHING`` so concurrent workers and restarts
    are idempotent.  Only PG-backed primitives are written; memory-only
    and log-style sliding windows (which have no PG backend) are skipped.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    for name, prim in rl_registry.rate_limits.items():
        if isinstance(prim, TokenBucket):
            kind = "token_bucket"
        else:
            if prim.style == "gcra":
                kind = "gcra"
            else:
                continue

        await _upsert_rate_limit_bucket_row(pool, schema, name, kind)

        logger.debug(
            "rl-bucket-synced",
            bucket_name=name,
            kind=kind,
        )


registry = RateLimitRegistry()

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
high key cardinality is bounded by the per-worker sweep eviction methods.

Every worker's 30-second sweep calls ``evict_idle_keyed_reservations`` /
``evict_idle_keyed_rate_limits`` against that worker's OWN registry ,
eviction is process-local bookkeeping and deliberately NOT leader-gated
(a non-leader's registry would otherwise receive no periodic eviction),
so it always runs in any topology that is capable of materializing keyed
primitives in the first place (keyed materialisation only happens from a
worker's job-dispatch path, and that worker sweeps its own registry
every 30 seconds).  This is not a silent-forever-leak bug.

As a defence-in-depth measure, the acquisition path
(``_resolve_reservation_name`` / ``_resolve_rate_limit_name``) also
performs an *opportunistic* eviction when the keyed-entry cap would
otherwise be hit, so reclaiming idle capacity never depends solely on
sweep timing.  The opportunistic scan is amortized to at most one per
``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` (30 s), so a registry at cap under
sustained denials stays O(1) per request instead of rescanning the whole
tracking dict on every denied acquisition; idle capacity is still
reclaimed within the sweep's own 30-second SLA.  A cap hit after
opportunistic eviction is a genuine sustained-high-cardinality denial,
not an artefact of when the sweep last ran.  Both eviction call sites
(the per-worker sweep and the opportunistic path) record evicted buckets
for row reclamation, reservation buckets' ``reservation_slots`` rows
under ``WorkerSettings.max_keyed_reservations``, rate-limit buckets'
published ``rate_limit_buckets`` rows (the schema captured at publish
time) under ``WorkerSettings.max_keyed_rate_limits`` on the
opportunistic path and the shared constant ceiling on the sweep path ,
so each pending-reclaim set is bounded by a configured ceiling
whichever path evicts.

Over-acquisition window on rollback failure:

- TokenBucket (Redis): ``ceil(capacity / refill_per_second * 2) + 60`` seconds
  (the ``EXPIRE`` TTL).
- SlidingWindow (Redis): ``2 * window_ms + 60_000`` ms (the ``PEXPIRE`` TTL
  ).
- ConcurrencyReservation (PG): ``lease_duration``, reclaimed by sweep 4
  within 30 seconds at most.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, TypeVar

import structlog
from pydantic import BaseModel, ValidationError

from taskq._validation import CURRENT_PAYLOAD_SCHEMA_VER
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the eviction drain and the fleet sweep must agree exactly on which rows still hold consumed quota, one predicate, no second hand-maintained copy.
    _no_consumed_quota_sql,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    _KEYED_KEY_RE,  # pyright: ignore[reportPrivateUsage]
    _MAX_KEYED_KEY_LEN,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_MAX_KEYED_RESERVATIONS,
    DEFAULT_RESERVATION_BACKOFF,
    QUEUE_CONCURRENCY_PREFIX,
)
from taskq.exceptions import PayloadValidationError, ReservationUnavailable
from taskq.obs import (
    record_ratelimit_refund_failure,
    record_reservation_reclaim_drain_duration,
    record_reservation_reclaim_drain_failure,
    record_reservation_reclaim_drain_rows,
    record_reservation_reclaim_heal_failure,
    update_keyed_reclaim_pending,
)
from taskq.ratelimit.composition import (
    AcquiredResource,
    RateLimitHandle,
    ReservationHandle,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.reservation import (
    _RECLAIM_SLICE_DELETE_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: the reclaim drain's statements live beside the slot-row templates they extend
    _RECLAIM_SLICE_EXISTING_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]
    ConcurrencyReservation,
    SlotLease,
)
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

Used both by the per-worker sweep (``_leader_sweeps.py``) and by the
opportunistic eviction on the acquisition path in
:meth:`~RateLimitRegistry._resolve_reservation_name` /
:meth:`~RateLimitRegistry._resolve_rate_limit_name`. Centralised here so
the two paths never drift apart.
"""

_OPPORTUNISTIC_EVICT_MIN_INTERVAL = timedelta(seconds=30)
"""Minimum interval between opportunistic eviction scans on the acquire path.

The scan itself is O(number of tracked keyed entries). Without a gate, a
registry sitting at its keyed-entry cap under sustained denials would pay
that O(n) scan on EVERY denied acquisition, at the default 10k-entry cap
and 1k denials/sec that is ~10M dict entries scanned per second on the
hottest path in the system, reclaiming nothing. Entries only become
evictable as wall-clock time passes (idle ≥ ``_KEYED_IDLE_THRESHOLD``),
so rescanning more often than the per-worker sweep's own 30-second cadence
buys nothing: a gated scan reclaims idle capacity at most 30 s after it
became reclaimable, identical to the sweep's documented SLA. The gate
makes the denied-cap-hit path O(1) amortized while preserving the
defence-in-depth guarantee that reclaiming idle capacity never depends
solely on sweep timing.
"""

_KEYED_RECLAIM_HEAL_WINDOW = timedelta(seconds=60)
"""Minimum spacing between acquire-path heal attempts for one keyed bucket.

A keyed bucket whose ``reservation_slots`` rows were deleted by a
sibling worker's pending-reclaim drain keeps denying acquisitions until
it is re-materialised (the registered-bucket-with-zero-rows trap). The
heal on the denial path re-materialises it, but a genuinely BUSY bucket
denies constantly, and probing it on every denial would add one PG round
trip per denied acquisition on the hottest path. The window bounds that
cost: at most one heal probe per contended keyed bucket per window per
worker; denials inside the window pay only an in-process stamp check.
"""

_DEFAULT_RECLAIM_BATCH_NAMES = 256
"""Bucket names per pending-reclaim drain statement.

The drain runs on the sweep cadence; each statement's write set is
bounded by this slice x each bucket's configured slot count, keeping one
drain tick a constant-size statement against any evicted-key backlog. At
the default 256-name slice and 30 s sweep interval this drains 512
names/min, if the eviction rate ever exceeds that, pending fills to its
cap and evictions are vetoed (the fail-closed bound) until the drain
catches up. The lever for a faster drain is this batch size, not the
sweep cadence.
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
    serializer used in production) cannot serialize, passing a ref
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
    ``==``.  Compares only the public, immutable config surface,  not
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


def _preserves_fixed_quota_state(prim: TokenBucket | SlidingWindow) -> bool:
    """Eviction-exemption predicate for keyed rate limits.

    Exempts a fixed-quota TokenBucket whose state an eviction cycle would
    discard (see :meth:`TokenBucket.holds_consumed_quota`), whether that
    state lives on the instance or in the row the reclaim drain deletes,
    losing it hands back a budget the tenant already spent.
    """
    return isinstance(prim, TokenBucket) and prim.holds_consumed_quota()


class RateLimitRegistry:
    """Unified registry for rate-limit and reservation primitives.

    Stores two separate dicts: ``_rate_limits`` for ``TokenBucket`` /
    ``SlidingWindow`` and ``_reservations`` for ``ConcurrencyReservation``.
    Cross-dict name collision is allowed, they live in separate namespaces.

    **Ownership.** A registry is an ordinary ownable object: construct one
    per process and pass it to ``worker_main(..., rate_limit_registry=...)``
    / ``create_router(..., rate_limit_registry=...)``, or rely on the
    module-level ``registry`` singleton (the default at every entry point).
    Actor-declared primitive instances are registered by the worker
    bootstrap's collection pass; use explicit ``.register()`` for
    primitives shared outside actor dispatch. :meth:`clear` resets all
    state and is a test aid only, NOT safe while a worker is running.
    """

    def __init__(self) -> None:
        self._rate_limits: dict[str, TokenBucket | SlidingWindow] = {}
        self._reservations: dict[str, ConcurrencyReservation] = {}
        # Names of reservations materialized from a KeyedReservationRef
        # (as opposed to a static @actor(reservations=["name"]) entry),
        # and the monotonic time each was last acquired, used only by
        # evict_idle_keyed_reservations() to bound registry growth under
        # high key cardinality. Never consulted by acquire_for_actor.
        self._keyed_reservation_last_used: dict[str, float] = {}
        # Names of rate limits materialized from a KeyedRateLimitRef
        # (as opposed to a static @actor(rate_limits=["name"]) entry),
        # and the monotonic time each was last acquired, used only by
        # evict_idle_keyed_rate_limits() to bound registry growth under
        # high key cardinality. Never consulted by acquire_for_actor.
        self._keyed_rate_limit_last_used: dict[str, float] = {}
        # Schema of the rate_limit_buckets row each keyed rate limit
        # published, captured at publish time, a TokenBucket carries no
        # schema of its own (unlike ConcurrencyReservation), so this is
        # the only record of where the row landed. Read by
        # evict_idle_keyed_rate_limits() to record the bucket's row for
        # reclamation; rides the keyed lifecycle like the heal stamps
        # (dropped on eviction, re-captured on re-materialization's
        # publish).
        self._keyed_rate_limit_row_schemas: dict[str, str] = {}
        # Evicted keyed-reservation bucket names whose reservation_slots
        # rows are still to be deleted, keyed by the schema those rows
        # live in. Each schema's names live in a dict[str, None] used as
        # an insertion-ordered set: the drain takes the FRONT batch and
        # re-appends that tick's survivors (names whose rows were all
        # still held) to the BACK, so a held bucket waits at most one
        # full rotation and never starves the buckets behind it. Bounded:
        # eviction refuses to record past the pending cap (the
        # settings-derived max_keyed_reservations at both production call
        # sites, the constant default for direct callers), and the drain
        # removes names whose rows are gone, drops names that
        # re-registered, and pops schema keys whose set is empty.
        # evict_idle_keyed_reservations() records; the sweep loop's
        # drain_pending_reservation_reclaims() deletes.
        self._pending_reservation_reclaims: dict[str, dict[str, None]] = {}
        # Evicted keyed rate-limit bucket names whose published
        # rate_limit_buckets rows are still to be deleted, the
        # rate-limit twin of _pending_reservation_reclaims: same
        # insertion-ordered per-schema sets, same cap-vetoed recording,
        # same drain (which deletes the front slice per schema; a bucket
        # row has no holder, so nothing survives the DELETE and no
        # survivor rotation is needed). evict_idle_keyed_rate_limits()
        # records; drain_pending_reservation_reclaims() deletes.
        self._pending_rate_limit_reclaims: dict[str, dict[str, None]] = {}
        # Monotonic time of the last acquire-path heal attempt per keyed
        # reservation name, gates the existence probe in the
        # ReservationUnavailable heal to one attempt per
        # _KEYED_RECLAIM_HEAL_WINDOW per bucket. Rides the keyed
        # registration's lifecycle: discarded on re-registration, pruned
        # by the same eviction pass that drops the tracking entry.
        self._keyed_reservation_heal_attempted: dict[str, float] = {}
        # Monotonic time of the last heal-failure WARNING per keyed
        # reservation name. The heal itself retries on every denial (a
        # failed attempt rolls its own stamp back), so the log needs a
        # separate gate: one warning per bucket per heal window, while
        # the failure counter records every attempt (metrics aggregate;
        # a per-denial warning line is what floods). Rides the same
        # lifecycle as _keyed_reservation_heal_attempted.
        self._keyed_reservation_heal_failure_logged: dict[str, float] = {}
        # Monotonic timestamps of the last opportunistic eviction scan on
        # each acquisition path, used to amortize the O(n) scan to at most
        # once per _OPPORTUNISTIC_EVICT_MIN_INTERVAL under sustained cap-hit
        # denials (see the constant's docstring). -inf so the first cap-hit
        # after startup always scans. evict_idle_keyed_*() are synchronous
        # with no await points, so check-and-stamp is atomic within the
        # event loop, concurrent scanners cannot pile up.
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

    @property
    def has_pending_reservation_reclaims(self) -> bool:
        """Whether any evicted keyed bucket still awaits its row deletion.

        Covers both pending sets, reservation slot rows and published
        rate-limit bucket rows. The sweep loop uses this single gate to
        skip the drain entirely (no connection acquired) when there is
        nothing to reclaim, so a pending set the gate cannot see would
        never be drained.
        """
        return any(self._pending_reservation_reclaims.values()) or any(
            self._pending_rate_limit_reclaims.values()
        )

    def _pending_reclaim_total(self) -> int:
        """Total bucket names across schemas awaiting row reclamation."""
        return sum(len(names) for names in self._pending_reservation_reclaims.values()) + sum(
            len(names) for names in self._pending_rate_limit_reclaims.values()
        )

    def has_reservation(self, name: str) -> bool:
        """O(1) membership test against the live reservations dict.

        Unlike the :attr:`reservations` property this does NOT defensively
        copy the dict, use it on per-job hot paths (e.g. the dispatch
        queue-cap check), where copying the whole registry per call is
        prohibitive at high keyed-entry cardinality.
        """
        return name in self._reservations

    def has_rate_limit(self, name: str) -> bool:
        """O(1) membership test against the live rate-limits dict.

        See :meth:`has_reservation`, the same no-copy guarantee applies.
        """
        return name in self._rate_limits

    def register(
        self,
        primitive: TokenBucket | SlidingWindow | ConcurrencyReservation,
    ) -> None:
        if primitive.name.startswith(QUEUE_CONCURRENCY_PREFIX):
            raise ValueError(
                f"name {primitive.name!r} starts with the reserved prefix "
                f"{QUEUE_CONCURRENCY_PREFIX!r}, internal queue-cap reservations "
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
                f"{name!r}, existing={existing!r}, new={primitive!r}"
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
                f"{name!r}, existing={existing!r}, new={primitive!r}"
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
                f"name {name!r} is a ConcurrencyReservation, "
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

        The error deliberately does NOT embed the payload (or its
        ``model_dump()``): this ``ValueError`` propagates into the
        persisted ``error_message`` (job row / web admin) via generic
        exception handling, and payload values are attacker-controlled ,
        the same sanitization contract ``PayloadValidationError`` follows
        in :mod:`taskq._validation`.

        ``isinstance`` (not an exact-type check) accepts ``str``
        subclasses, a ``class Tenant(str, Enum)`` member or a domain
        wrapper deriving from ``str`` is a natural ``key_fn`` return value
        and behaves identically to a plain ``str`` for namespacing, Redis
        keys, and dict lookups.
        """
        if not isinstance(key, str) or not key:
            raise ValueError(f"{ref_repr}.key_fn returned {empty_key_msg}")
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

    def _derive_keyed_key(
        self,
        ref: KeyedRateLimitRef | KeyedReservationRef,
        payload: dict[str, object] | BaseModel,
        *,
        empty_key_msg: str = "an empty or non-string key",
    ) -> str:
        """Resolve key_fn arg, call key_fn, and validate the returned key."""
        key_fn_arg = self._resolve_key_fn_arg(ref, payload)
        return self._validate_keyed_key(
            ref.key_fn(key_fn_arg),
            f"{type(ref).__name__}(base_name={ref.base_name!r})",
            empty_key_msg=empty_key_msg,
        )

    def _resolve_key_fn_arg(
        self,
        ref: KeyedRateLimitRef | KeyedReservationRef,
        payload: dict[str, object] | BaseModel,
    ) -> BaseModel:
        """Convert payload to a validated BaseModel for key_fn.

         Three cases:
         - Same type (isinstance check): zero-cost pass-through
         - Different BaseModel type: re-validate via
           ``ref.payload_type.model_validate(payload.model_dump(by_alias=True))``
         - Raw dict: validate via ``ref.payload_type.model_validate(dict)``

         Why the different-type case is a dump→validate round-trip rather
         than a cheap copy: this is type CONVERSION, not duplication ,
         ``model_copy`` cannot change the model type, so one full validation
         against ``ref.payload_type`` is essential here (pinned by
         ``test_resolve_keyed_ref_wrong_model_type_raises_validation_error``).
         ``by_alias=True`` keeps the dumped keys matching what the source
         model publishes; ``from_attributes`` would read attribute names
         instead and silently diverge for alias-carrying payload models.
         The common per-acquire case pays none of this cost: the worker
         hands ``acquire_for_actor`` an already-validated model of the
         actor's payload type, and when that matches ``ref.payload_type``
         the isinstance guard above passes the object through by identity ,
         no dump, no validate. No per-call ``TypeAdapter`` is constructed on
         any path: ``BaseModel.model_validate`` reuses the core validator
         cached on the model class.

         A ValidationError from conversion is re-raised as
         :class:`~taskq.exceptions.PayloadValidationError` (non-retryable)
        , it is a payload error, not a limiter fault.
        """
        if isinstance(payload, ref.payload_type):
            return payload
        try:
            if isinstance(payload, BaseModel):
                return ref.payload_type.model_validate(payload.model_dump(by_alias=True))
            return ref.payload_type.model_validate(payload)
        except ValidationError as exc:
            errs: list[dict[str, object]] = exc.errors(include_url=False, include_input=False)  # type: ignore[assignment]  # Why: pydantic v2 ErrorDetails is a TypedDict (subtype of dict[str, Any]); assignment to list[dict[str,object]] is safe at runtime but pyright cannot prove covariance
            raise PayloadValidationError(
                f"Payload validation failed for {type(ref).__name__}(base_name={ref.base_name!r}): "
                f"payload_type={ref.payload_type.__name__}, received={type(payload).__name__}. {exc.title}",
                # No job row exists on this path, the version in scope is
                # the one ``ref.payload_type`` is being validated against,
                # i.e. the schema version the system writes today.
                payload_schema_ver=str(CURRENT_PAYLOAD_SCHEMA_VER),
                validation_errors=errs,
            ) from exc

    async def _resolve_reservation_name(
        self,
        ref: "str | KeyedReservationRef",
        payload: dict[str, object] | BaseModel | None,
        *,
        pg_pool: "asyncpg.Pool | None",
        settings: "WorkerSettings | None",
    ) -> str:
        """Return the concrete registry name for *ref*.

        A plain ``str`` is returned as-is (must already be registered via
        :meth:`register`). A :class:`KeyedReservationRef` derives
        ``f"{ref.base_name}:{key}"`` by calling ``ref.key_fn(validated_model)``
        and lazily registers a matching :class:`ConcurrencyReservation` on
        first use, subsequent calls for the same key reuse it. Two reuse
        cases are distinguished:

        - **Keyed-materialized entry** (tracked in
          ``_keyed_reservation_last_used``): recency is refreshed, and the
          existing entry's ``slots``/``lease`` are checked against the
          ref's, a mismatch means two refs collided on the same concrete
          name with different configs (one ref's ``base_name`` is a prefix
          of the other's concrete name, since ``:`` is an allowed key
          character), which raises ``ValueError`` rather than silently
          over- or under-admitting relative to one ref's declared config.
          The guard covers live tracked entries only: if the colliding
          entry was idle-evicted in between, the second ref re-materializes
          its own config without error, eviction resets the guard.
        - **Statically pre-registered entry** (not tracked): reused as-is,
          and deliberately NOT stamped into ``_keyed_reservation_last_used``
          so the idle-eviction sweep can never evict a user's static entry.

        The ``key_fn`` return value is validated: it must be non-empty, at
        most ``_MAX_KEYED_KEY_LEN`` characters, and match
        ``_KEYED_KEY_RE`` (alphanumeric plus ``_ - : .``), this
        prevents control characters in PG text columns and bounds storage
        growth from attacker-controlled keys. When ``settings`` is provided
        and the number of tracked keyed reservations reaches
        ``settings.max_keyed_reservations``, a new key raises
        :class:`~taskq.exceptions.ReservationUnavailable`.

        Capacity is normally reclaimed by each worker's own 30-second
        sweep (``evict_idle_keyed_reservations``, per-worker, not
        leader-gated).  However, an acquisition that
        would otherwise be denied purely because idle entries haven't been
        swept yet gets one *opportunistic* eviction attempt first, so
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
        ``acquire()``, otherwise every acquisition would fail with
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
        key = self._derive_keyed_key(ref, payload, empty_key_msg="an empty key or non-string value")
        concrete_name = f"{ref.base_name}:{key}"
        # The cap bounds keyed-materialized GROWTH. It must not fire when
        # the concrete name already exists, neither for a tracked keyed
        # entry (recency refresh grows nothing) nor for a statically
        # pre-registered entry (reused as-is, never tracked, grows nothing).
        if (
            concrete_name not in self._reservations
            and concrete_name not in self._keyed_reservation_last_used
            and settings is not None
            and len(self._keyed_reservation_last_used) >= settings.max_keyed_reservations
        ):
            self._opportunistic_evict_reservations(settings)
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
            # A PG-backed materialization needs the schema the slot rows
            # live in, and settings is its only source. Falling back to the
            # ConcurrencyReservation default ("taskq") here, as this path
            # once did, silently targets a schema the caller never
            # configured: slot rows land in whatever "taskq".reservation_
            # slots exists in that database. The in-memory path (no pool)
            # stays settings-free: its slot table is process-local and the
            # schema is never read.
            if settings is None and pg_pool is not None:
                raise RuntimeError(
                    "KeyedReservationRef materialization against Postgres requires "
                    "settings (the schema source for reservation_slots): pass the "
                    "worker's TaskQSettings to acquire_for_actor(settings=...)"
                )
            schema = settings.schema_name if settings is not None else "taskq"
            new_reservation = ConcurrencyReservation(
                name=concrete_name,
                slots=ref.slots,
                lease=ref.lease,
                schema=schema,
                # The fleet-reclaimable mark: a keyed-materialised
                # bucket's rows carry their own staleness (keyed +
                # last_used_at, stamped by the reservation's own
                # acquire/release/ensure statements), so the maintenance
                # leader can reclaim them after this process dies, the
                # in-process bookkeeping below cannot survive that.
                keyed=True,
            )
            self.register(new_reservation)
            # A fresh registration is a fresh lifecycle: any heal-window
            # stamp (probe gate or failure-log gate) from the previous
            # incarnation of this concrete name (evicted, then
            # re-materialised) must not survive.
            self._keyed_reservation_heal_attempted.pop(concrete_name, None)
            self._keyed_reservation_heal_failure_logged.pop(concrete_name, None)
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
                    # registered would poison this key permanently, the
                    # reuse branch below would skip ensure_slots forever,
                    # acquire() would find no slot rows and keep denying,
                    # and each attempt would re-stamp recency so the entry
                    # is never idle-evicted either. ensure_slots is
                    # idempotent (ON CONFLICT DO NOTHING), so the next
                    # attempt re-materializes and retries.
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
            # declared config, fail loudly instead.
            existing = self._reservations[concrete_name]
            if existing.slots != ref.slots or existing.lease != ref.lease:
                raise ValueError(
                    f"KeyedReservationRef(base_name={ref.base_name!r}) resolved to "
                    f"{concrete_name!r}, which is already materialized with a different "
                    f"config (existing slots={existing.slots}, lease={existing.lease}; "
                    f"ref declares slots={ref.slots}, lease={ref.lease}), concrete-name "
                    f"collision between keyed refs; choose distinct base_names "
                    f"(':' in keys can make one ref's base_name a prefix of another's "
                    f"concrete name)"
                )
            self._keyed_reservation_last_used[concrete_name] = monotonic()
        # else: the concrete name was STATICALLY pre-registered (not keyed-
        # materialized), reuse it as-is and never stamp the tracking dict,
        # so the sweep can never evict a user's static entry.
        return concrete_name

    async def _resolve_rate_limit_name(
        self,
        ref: "str | KeyedRateLimitRef",
        payload: dict[str, object] | BaseModel | None,
        *,
        settings: "WorkerSettings | None",
        pg_pool: "asyncpg.Pool | None" = None,
    ) -> str:
        """Return the concrete registry name for *ref*.

        Mirrors :meth:`_resolve_reservation_name` for rate limits. A plain
        ``str`` is returned as-is (must already be registered via
        :meth:`register`). A :class:`KeyedRateLimitRef` derives
        ``f"{ref.base_name}:{key}"`` by calling ``ref.key_fn(validated_model)``
        and lazily registers a matching :class:`TokenBucket` on first use ,
        subsequent calls for the same key reuse it. As in
        :meth:`_resolve_reservation_name`, two reuse cases are
        distinguished: a keyed-materialized (tracked) entry has its
        recency refreshed and its config checked against the ref's, a
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
        as an invalid key and raises ``ValueError``, a broken ``key_fn``
        can never silently resolve to a shared/global bucket. When
        ``settings`` is provided and the number of tracked keyed rate
        limits reaches ``settings.max_keyed_rate_limits``, a new key
        raises :class:`~taskq.exceptions.ReservationUnavailable`.

        Capacity is normally reclaimed by each worker's own 30-second
        sweep (``evict_idle_keyed_rate_limits``, per-worker, not
        leader-gated).  However, an acquisition that
        would otherwise be denied purely because idle entries haven't been
        swept yet gets one *opportunistic* eviction attempt first, so
        hitting the cap is never purely an artefact of sweep timing, only a
        genuine sustained-high-cardinality condition.  Only if the cap is
        still exceeded after the opportunistic eviction does the method
        raise :class:`~taskq.exceptions.ReservationUnavailable`.  The
        opportunistic scan is amortized to at most one per
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` so sustained cap-hit denials
        stay O(1) on this hot path (see
        :meth:`_opportunistic_evict_rate_limits`).

        Unlike reservations there is no PG slot pre-allocation step, a
        :class:`TokenBucket` is immediately usable after ``register()``
        (there is no ``ensure_slots`` equivalent). When the
        underlying ``TokenBucket`` uses the Redis backend, per-key Redis
        memory is already self-bounding via the Lua script's ``EXPIRE`` TTL
        on the bucket's hash; :meth:`evict_idle_keyed_rate_limits` only
        bounds this Python-process-local registry dict, not Redis itself.
        These are two independent growth bounds.

        On materialization with a ``pg_pool`` available, the new bucket is
        also published to the ``rate_limit_buckets`` table (best-effort,
        idempotent) so the admin UI can surface keyed buckets discovered
        after worker startup, see the inline note at the publish site.
        """
        if isinstance(ref, str):
            return ref

        if payload is None:
            raise ValueError(
                f"rate limit {ref.base_name!r} is a KeyedRateLimitRef but no "
                "payload was provided to derive its key from"
            )
        key = self._derive_keyed_key(ref, payload)
        concrete_name = f"{ref.base_name}:{key}"
        # The cap bounds keyed-materialized GROWTH. It must not fire when
        # the concrete name already exists, neither for a tracked keyed
        # entry (recency refresh grows nothing) nor for a statically
        # pre-registered entry (reused as-is, never tracked, grows nothing).
        if (
            concrete_name not in self._rate_limits
            and concrete_name not in self._keyed_rate_limit_last_used
            and settings is not None
            and len(self._keyed_rate_limit_last_used) >= settings.max_keyed_rate_limits
        ):
            self._opportunistic_evict_rate_limits(settings)
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
                # The fleet-reclaimable mark, restricted to PG-state
                # backed buckets: only their acquire path touches the
                # rate_limit_buckets row, so only there is the row's
                # last_used_at a truthful liveness signal. A
                # redis-backend keyed bucket's PG row (admin metadata +
                # outage-fallback state) must never be swept, its
                # stamp cannot speak for Redis-side use.
                keyed=ref.backend == "postgres",
            )
            self.register(new_bucket)
            self._keyed_rate_limit_last_used[concrete_name] = monotonic()
            # Publish the freshly-materialized bucket to PG (best-effort)
            # so the admin UI's rate-limits page surfaces it in ANY
            # topology: statically registered buckets are published at
            # worker startup by sync_rate_limit_buckets, but a keyed bucket
            # materialized long after startup would otherwise be invisible
            # to a standalone admin process, whose registry singleton
            # never dispatches jobs, making an active per-tenant throttle
            # look like "no limiter configured". Unlike ensure_slots for
            # keyed reservations (a correctness precondition for acquire),
            # this row is observability metadata, so a publish failure
            # must NOT fail the acquisition, warn and continue.
            if pg_pool is not None:
                try:
                    await _upsert_rate_limit_bucket_row(
                        pg_pool,
                        schema,
                        concrete_name,
                        "token_bucket",
                        keyed=new_bucket.keyed,
                    )
                except Exception:
                    logger.warning(
                        "keyed-rate-limit-bucket-publish-failed",
                        bucket_name=concrete_name,
                        exc_info=True,
                    )
                # The reclaim capture rides the RESOLUTION, not the
                # publish's outcome. A row can exist without a successful
                # publish: the acquire path preseeds one for
                # backend="postgres" (and for the redis backend's PG
                # fallback) whatever the publish did, so keying the
                # capture on publish success orphans that row, its idle
                # eviction records nothing (the pending-reclaim set is the
                # only code path that can name it) and steady state is one
                # row per key whose first publish failed, unbounded in the
                # caller-controlled key space. A captured name with no row
                # drains as a one-statement no-op DELETE; an uncaptured
                # row is permanent.
                self._keyed_rate_limit_row_schemas[concrete_name] = schema
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
                    f"backend={ref.backend}), concrete-name collision between keyed "
                    f"refs; choose distinct base_names "
                    f"(':' in keys can make one ref's base_name a prefix of another's "
                    f"concrete name)"
                )
            self._keyed_rate_limit_last_used[concrete_name] = monotonic()
            if pg_pool is not None and concrete_name not in self._keyed_rate_limit_row_schemas:
                # First pool-bearing touch of a bucket materialized without
                # a pool: the materialisation arm above never ran its
                # publish, so the admin-UI row is missing AND the reclaim
                # capture is missing. Publish now (the same idempotent,
                # best-effort statement the materialisation arm uses) and
                # capture whatever the publish's outcome, the acquire
                # path can preseed the row regardless (see the capture
                # comment above). Self-limiting: once captured, this arm
                # is a dict membership check on the reuse hot path and
                # nothing else.
                schema = settings.schema_name if settings is not None else "taskq"
                try:
                    await _upsert_rate_limit_bucket_row(
                        pg_pool,
                        schema,
                        concrete_name,
                        "token_bucket",
                        keyed=existing.keyed,
                    )
                except Exception:
                    logger.warning(
                        "keyed-rate-limit-bucket-publish-failed",
                        bucket_name=concrete_name,
                        exc_info=True,
                    )
                self._keyed_rate_limit_row_schemas[concrete_name] = schema
        # else: the concrete name was STATICALLY pre-registered (not keyed-
        # materialized), reuse it as-is and never stamp the tracking dict,
        # so the sweep can never evict a user's static entry.
        return concrete_name

    async def acquire_for_actor(
        self,
        rate_limits: Sequence["str | KeyedRateLimitRef | TokenBucket | SlidingWindow"],
        reservations: Sequence["str | KeyedReservationRef | ConcurrencyReservation"],
        *,
        job_id: "UUID",
        worker_id: "UUID",
        payload: dict[str, object] | BaseModel | None = None,
        redis_client: "redis_async.Redis | None" = None,
        pg_pool: "asyncpg.Pool | None" = None,
        clock: "Clock | None" = None,
        settings: "WorkerSettings | None" = None,
    ) -> list[AcquiredResource]:
        """AND-composition: acquire reservations first, then rate limits.

        ``reservations`` entries may be plain names (resolved against
        statically pre-registered primitives), :class:`KeyedReservationRef`
        instances (resolved dynamically per job from ``payload``, see
        :meth:`_resolve_reservation_name`), or
        :class:`ConcurrencyReservation` instances (normalized to their
        ``.name`` up front, the instance must already be registered,
        e.g. by the worker bootstrap's actor-declaration collection
        pass; an unregistered instance raises ``KeyError`` exactly like
        an unknown name). ``rate_limits`` entries may likewise be plain
        names, :class:`KeyedRateLimitRef` instances, or
        :class:`TokenBucket` / :class:`SlidingWindow` instances.
        ``payload`` is required if any entry is a ``KeyedReservationRef``
        or ``KeyedRateLimitRef``. It may be a ``dict`` (validated via
        ``ref.payload_type.model_validate``) or a ``BaseModel`` (used
        directly if it matches ``ref.payload_type``, otherwise re-validated
        via ``model_dump()`` → ``model_validate()``).

        Returns the list of ``AcquiredResource`` handles on full success.
        Raises ``ReservationUnavailable`` on any denial, rollback is performed
        internally before re-raising (already-acquired resources released in
        reverse order, each failure logged at ERROR).
        """
        # Normalize primitive instances to their names BEFORE any use ,
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
                slot_index = await self._acquire_reservation_slot_with_heal(
                    reservation,
                    job_id=job_id,
                    worker_id=worker_id,
                    pg_pool=pg_pool,
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
            # bounded, self-healing leak, NOT an oversight: leaked
            # reservation slots are reclaimed by lease expiry (the
            # lock-expiry sweep, within ~30s), and consumed rate-limit
            # tokens are bounded by the bucket's Redis EXPIRE TTL. Rolling
            # back here would mean network I/O (handle.release()) while the
            # task is being torn down, delaying cancellation, with a
            # second cancel able to interrupt the release itself, which is
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

    async def _acquire_reservation_slot_with_heal(
        self,
        reservation: ConcurrencyReservation,
        *,
        job_id: "UUID",
        worker_id: "UUID",
        pg_pool: "asyncpg.Pool | None",
    ) -> SlotLease:
        """Acquire one reservation slot, healing a keyed bucket whose rows vanished.

        Wraps :meth:`ConcurrencyReservation.acquire` for every
        reservation in the composition. On denial, a KEYED-materialized
        bucket gets one gated existence probe (see
        :meth:`_heal_deleted_keyed_reservation_rows`): zero rows, its
        ``reservation_slots`` rows were deleted out from under a still-
        registered bucket, the cross-worker trap the reclamation drain
        creates, re-materialises the rows and retries the acquire
        exactly once. Every other denial re-raises unchanged: static
        reservations never reach the probe, and the busy case (rows
        present, all held) is ordinary contention.
        """
        try:
            return await reservation.acquire(job_id, worker_id, pg_pool)
        except ReservationUnavailable:
            if not await self._heal_deleted_keyed_reservation_rows(reservation, pg_pool):
                raise
            return await reservation.acquire(job_id, worker_id, pg_pool)

    async def _heal_deleted_keyed_reservation_rows(
        self,
        reservation: ConcurrencyReservation,
        pg_pool: "asyncpg.Pool | None",
    ) -> bool:
        """Re-materialise a keyed bucket whose slot rows no longer exist.

        Returns True when the heal re-materialised the rows and the
        caller should retry the acquire once; False when the denial
        stands. A heal FAILURE never raises, it is recorded (its own
        failure counter, an AVAILABILITY signal distinct from the drain's
        storage counter, plus a window-gated warning) and the original
        ``ReservationUnavailable`` propagates from the caller; a
        cancellation (``BaseException``) is not a heal outcome and
        propagates unchanged.

        False (deny) cases, in evaluation order:

        - the bucket is STATIC (untracked), no keyed lifecycle, no
          cross-worker row-deletion hazard, and the hot path pays only
          one dict lookup;
        - no PG pool, the in-memory backend's acquire already re-ensures
          its rows on every call;
        - inside ``_KEYED_RECLAIM_HEAL_WINDOW`` of the last attempt, a
          genuinely busy bucket denies constantly, and probing it per
          denial would put a PG round trip on the hottest path; the
          window bounds it to one probe per bucket per window. The
          stamp from a BUSY attempt also defers a later zero-rows heal
          that lands inside the same window, a bounded availability
          blip (the first denial after the window heals) accepted
          deliberately: un-stamping on busy would reintroduce the
          probe-per-denial stampede the window exists to prevent;
        - the probe found rows, ordinary contention; the window stamp
          STANDS (this is the cost the window exists to bound);
        - the probe or ``ensure_slots`` raised, the stamp is rolled
          back so the next denial retries the heal, the failure is
          counted (every attempt, metrics aggregate), and the denial
          propagates. The failure WARNING is gated to one per bucket per
          heal window by its own stamp: a busy bucket with a broken
          probe denies on every acquisition, and a warning line per
          denial is a log flood, not a signal;
        - the probe or ``ensure_slots`` was cancelled (a
          ``BaseException``, task teardown, not a heal outcome), the
          stamp is rolled back exactly like a failed attempt (the next
          denial probes immediately) and the cancellation propagates
          unchanged.

        Steady state: a contended keyed bucket costs at most one probe
        per window per worker; every denial inside the window pays only
        the in-process stamp check.
        """
        name = reservation.name
        if name not in self._keyed_reservation_last_used or pg_pool is None:
            return False
        now = monotonic()
        last_attempt = self._keyed_reservation_heal_attempted.get(name)
        if (
            last_attempt is not None
            and now - last_attempt < _KEYED_RECLAIM_HEAL_WINDOW.total_seconds()
        ):
            return False
        self._keyed_reservation_heal_attempted[name] = now
        try:
            if await reservation.slot_rows_exist(pg_pool):
                return False
            await reservation.ensure_slots(pg_pool)
        except Exception as exc:
            self._keyed_reservation_heal_attempted.pop(name, None)
            record_reservation_reclaim_heal_failure(type(exc).__name__)
            last_logged = self._keyed_reservation_heal_failure_logged.get(name)
            if (
                last_logged is None
                or now - last_logged >= _KEYED_RECLAIM_HEAL_WINDOW.total_seconds()
            ):
                self._keyed_reservation_heal_failure_logged[name] = now
                logger.warning(
                    "keyed-reservation-heal-failed",
                    bucket_name=name,
                    error=repr(exc),
                )
            return False
        except BaseException:
            self._keyed_reservation_heal_attempted.pop(name, None)
            raise
        logger.info(
            "keyed-reservation-healed",
            bucket_name=name,
        )
        return True

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
                f"name {name!r} is a ConcurrencyReservation, "
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
        timeout: "float | None" = None,  # noqa: ASYNC109  # Why: the bound is a per-call deadline the caller passes, not an enclosing asyncio.timeout scope; the registry's other bounded methods (drain_pending_reservation_reclaims) take the same shape.
    ) -> dict[str, RateLimitState]:
        """Peek all registered rate limits. Returns {name: RateLimitState}.

        Each bucket's read is a separate Redis/PG round trip, so a call
        costs O(buckets) round trips, and the registry can hold up to
        ``max_keyed_rate_limits`` keyed-materialised buckets. *timeout*
        bounds the WHOLE pass: a bucket whose store hangs (a black-holed
        broker answers no read) must not park the caller past it. Raises
        :class:`TimeoutError` when the bound fires, per-bucket failures
        are still caught and logged per bucket, but a read that never
        RETURNS is indistinguishable from a dead registry at page-render
        time, so the caller learns of the bound instead of rendering a
        half-empty live-state map as if it were current. ``None`` (the
        default) keeps the unbounded shape for callers that manage their
        own deadline.
        """
        if timeout is not None:
            return await asyncio.wait_for(
                self._peek_all(redis_client, pg_pool, clock, settings), timeout=timeout
            )
        return await self._peek_all(redis_client, pg_pool, clock, settings)

    async def _peek_all(
        self,
        redis_client: "redis_async.Redis | None",
        pg_pool: "asyncpg.Pool | None",
        clock: "Clock | None",
        settings: "WorkerSettings | None",
    ) -> dict[str, RateLimitState]:
        results: dict[str, RateLimitState] = {}
        for name, prim in list(self._rate_limits.items()):
            try:
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
        timeout: "float | None" = None,  # noqa: ASYNC109  # Why: the bound is a per-call deadline the caller passes, not an enclosing asyncio.timeout scope.
    ) -> None:
        """Reset a rate-limit bucket to full capacity.

        *timeout* bounds the reset's backend round trip (a Redis DEL or a
        PG upsert against a dead store can hang the caller forever);
        raises :class:`TimeoutError` when it fires. ``None`` (the default)
        keeps the unbounded shape for callers that manage their own
        deadline.
        """
        if name in self._reservations:
            raise TypeError(
                f"name {name!r} is a ConcurrencyReservation, "
                f"reset() on reservations is not supported"
            )
        if name not in self._rate_limits:
            raise KeyError(name)

        primitive = self._rate_limits[name]
        if timeout is not None:
            await asyncio.wait_for(
                primitive.reset(
                    redis_client=redis_client,
                    pg_pool=pg_pool,
                    clock=clock,
                    settings=settings,
                ),
                timeout=timeout,
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

        Why *pg_pool* is unused: each handle captured the pool it needs at
        acquisition time, so ``handle.release()`` is self-contained. The
        parameter mirrors :meth:`acquire_for_actor`, which does need it, so
        the acquire/release pair reads as one symmetric surface at the call
        site (``taskq.worker._consumer`` passes the same ``worker_pool`` to
        both). It is public API, so it is kept rather than removed.
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

    def _opportunistic_evict_reservations(self, settings: "WorkerSettings | None") -> None:
        """Idle-reservation scan, amortized to one per min-interval.

        Called on the acquisition path when the keyed-reservation cap is
        hit. The scan is O(tracked entries); without amortization a
        registry at cap under sustained denials would pay O(n) per denied
        request and reclaim nothing (see
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL``). Registry ENTRIES are
        still reclaimed within max(sweep cadence, min-interval) of
        becoming idle, the scan just can't be stampeded; the evicted
        buckets' ``reservation_slots`` ROWS are reclaimed separately, on
        the sweep cadence, by the pending-reclaim drain. Pending records
        carry the caller's settings-derived cap when settings are in
        scope (they are at the only call site, the cap-hit branch of
        :meth:`_resolve_reservation_name`), so the pending set is bounded
        by the same ceiling that bounds the tracked entries instead of
        the constant fallback.
        """
        now = monotonic()
        if (
            now - self._keyed_reservation_last_eviction_scan
            >= _OPPORTUNISTIC_EVICT_MIN_INTERVAL.total_seconds()
        ):
            self._keyed_reservation_last_eviction_scan = now
            self.evict_idle_keyed_reservations(
                idle_for=_KEYED_IDLE_THRESHOLD,
                max_pending_reclaims=(
                    None if settings is None else settings.max_keyed_reservations
                ),
            )

    def _opportunistic_evict_rate_limits(self, settings: "WorkerSettings | None") -> None:
        """Idle-rate-limit scan, amortized to one per min-interval.

        Rate-limit twin of :meth:`_opportunistic_evict_reservations`:
        the scan reclaims registry ENTRIES within max(sweep cadence,
        min-interval) of becoming idle, and evicted buckets' PUBLISHED
        ``rate_limit_buckets`` rows are recorded for the reclaim drain
        under the settings-derived cap when settings are in scope (they
        are at the only call site, the cap-hit branch of
        :meth:`_resolve_rate_limit_name`).
        """
        now = monotonic()
        if (
            now - self._keyed_rate_limit_last_eviction_scan
            >= _OPPORTUNISTIC_EVICT_MIN_INTERVAL.total_seconds()
        ):
            self._keyed_rate_limit_last_eviction_scan = now
            self.evict_idle_keyed_rate_limits(
                idle_for=_KEYED_IDLE_THRESHOLD,
                max_pending_reclaims=(None if settings is None else settings.max_keyed_rate_limits),
            )

    def _evict_idle_keyed(
        self,
        tracking_dict: dict[str, float],
        primitive_dict: dict[str, _P],
        idle_for: timedelta,
        event_name: str,
        *,
        preserve: Callable[[_P], bool] | None = None,
        admit: Callable[[str, _P], bool] | None = None,
    ) -> list[str]:
        """Evict stale entries from *tracking_dict* and *primitive_dict*.

        Removes entries whose ``last_used`` timestamp is older than
        ``monotonic() - idle_for`` from both dicts, logs *event_name*
        with the evicted count, and returns the evicted names.

        *preserve*, when given, exempts an entry from eviction when the
        predicate returns True for its primitive, the entry keeps its
        tracking timestamp and is re-scanned on the next sweep. Use for
        primitives whose in-instance state eviction would destroy
        irrecoverably (see :meth:`evict_idle_keyed_rate_limits`).

        *admit*, when given, is consulted immediately before an entry is
        removed: it records the eviction's downstream work and may veto
        it by returning False, a vetoed entry keeps its primitive, its
        tracking timestamp, and is re-scanned on the next sweep (the
        pending-reclaim cap works this way; see
        :meth:`evict_idle_keyed_reservations`). Both the scan and the
        pops are synchronous with no await points, so veto, record and
        remove are atomic within the event loop.
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
        evicted: list[str] = []
        for name in stale:
            prim = primitive_dict.get(name)
            if prim is not None and admit is not None and not admit(name, prim):
                continue
            primitive_dict.pop(name, None)
            del tracking_dict[name]
            evicted.append(name)
        if evicted:
            logger.debug(event_name, count=len(evicted))
        return evicted

    def evict_idle_keyed_reservations(
        self,
        idle_for: "timedelta",
        *,
        max_pending_reclaims: "int | None" = None,
    ) -> int:
        """Drop registry entries for keyed reservations idle at least ``idle_for``.

        Reservations derived from a :class:`KeyedReservationRef` are
        registered lazily and never removed automatically, under high key
        cardinality (e.g. one reservation per import session over a long
        worker lifetime) this dict grows without bound. Each worker's
        30-second sweep calls this automatically against its own registry
        (not leader-gated) with a 1-hour idle threshold; call directly for
        custom eviction windows.

        Removes the in-memory registry entry and its acquire-recency
        tracking, and RECORDS the bucket in the pending-reclaim set so
        its ``reservation_slots`` rows are deleted by
        :meth:`drain_pending_reservation_reclaims` on the same sweep
        cadence. The lock-expiry sweep is an ``UPDATE ... SET job_id =
        NULL``, it clears a row's holder but never deletes the row, and
        ``sync_slots`` iterates only currently-registered reservations,
        so without the pending-reclaim drain an evicted bucket's rows
        would be orphaned permanently (steady-state cardinality: slots x
        every key ever seen, unbounded in the caller-controlled key
        space). A slot still held by a live lease survives the drain's
        idle-guarded DELETE and stays pending until its lease expires.

        *max_pending_reclaims* caps the pending-reclaim set (default:
        :data:`taskq.constants.DEFAULT_MAX_KEYED_RESERVATIONS`; both
        production call sites, the per-worker sweep and the acquisition
        path's opportunistic eviction, pass the settings-derived
        ``WorkerSettings.max_keyed_reservations``, so the pending set is
        bounded by the same ceiling that bounds the tracked entries;
        direct callers without a settings object get the constant). At
        the cap the eviction is VETOED, the entry stays registered and
        re-scanned on the next sweep, so no structure grows unbounded
        and no rows are orphaned by an eviction that could not be
        recorded. The visible signals are the pending-depth gauge
        (``taskq.ratelimit.reclaim_pending``, the steady signal), the
        ``registry-keyed-reclaim-pending-cap-veto`` warning (one
        aggregated line per eviction call that shed evictions, pending
        at cap means reclamation is falling behind), and, if the veto
        persists up to the registry's own entry cap, the existing
        ``registry-keyed-reservation-limit-exceeded`` soft-cap warning.

        A key that is acquired again after eviction is
        re-registered on next use (idempotent, see
        :meth:`_resolve_reservation_name`), and the drain drops its name
        from the pending set without deleting anything (a re-activated
        key owns its rows again), so eviction is always safe to call,
        including concurrently with in-flight acquisitions for other
        keys.

        Returns the number of entries evicted.
        """
        cap = (
            DEFAULT_MAX_KEYED_RESERVATIONS if max_pending_reclaims is None else max_pending_reclaims
        )
        vetoed = 0

        def _admit(name: str, prim: ConcurrencyReservation) -> bool:
            nonlocal vetoed
            if self._record_pending_reclaim(
                self._pending_reservation_reclaims, prim.schema, name, cap=cap
            ):
                return True
            vetoed += 1
            return False

        evicted = self._evict_idle_keyed(
            self._keyed_reservation_last_used,
            self._reservations,
            idle_for,
            "registry-evicted-idle-keyed-reservations",
            admit=_admit,
        )
        # The heal stamps ride the registration's lifecycle: an evicted
        # bucket's window must not survive into a future re-registration
        # of the same concrete name.
        for name in evicted:
            self._keyed_reservation_heal_attempted.pop(name, None)
            self._keyed_reservation_heal_failure_logged.pop(name, None)
        update_keyed_reclaim_pending(self._pending_reclaim_total())
        if vetoed:
            logger.warning(
                "registry-keyed-reclaim-pending-cap-veto",
                vetoed=vetoed,
                cap=cap,
                pending=self._pending_reclaim_total(),
            )
        return len(evicted)

    def _record_pending_reclaim(
        self,
        pending: dict[str, dict[str, None]],
        schema: str,
        name: str,
        *,
        cap: int,
    ) -> bool:
        """Record one evicted keyed bucket for row reclamation, under the cap.

        Shared by both eviction kinds, *pending* is the kind's own
        per-schema set (``_pending_reservation_reclaims`` for slot rows,
        ``_pending_rate_limit_reclaims`` for published bucket rows), so
        each kind's cap bounds its own set. Returns True (the caller's
        eviction proceeds) after recording *name* under *schema*, the
        drain's DELETE is a harmless no-op for a bucket that has no rows.
        A name still pending from an earlier eviction wave keeps its
        position (it has not been served a drain pass yet). At the cap,
        returns False: the eviction is vetoed and the entry stays
        registered (see the eviction methods).
        """
        if sum(len(names) for names in pending.values()) >= cap:
            return False
        pending.setdefault(schema, {})[name] = None
        return True

    def evict_idle_keyed_rate_limits(
        self,
        idle_for: "timedelta",
        *,
        max_pending_reclaims: "int | None" = None,
    ) -> int:
        """Drop registry entries for keyed rate limits idle at least ``idle_for``.

        Rate limits derived from a :class:`KeyedRateLimitRef` are registered
        lazily and never removed automatically, under high key cardinality
        (e.g. one token bucket per tenant over a long worker lifetime) this
        dict grows without bound. Each worker's 30-second sweep calls this
        automatically against its own registry (not leader-gated) with a
        1-hour idle threshold; call directly for custom eviction windows.

        Removes the in-memory registry entry and its acquire-recency
        tracking. The underlying Redis hash is deliberately NOT touched:
        per-key Redis memory is already self-bounding via the Lua script's
        ``EXPIRE`` TTL on the bucket's hash (see
        :meth:`_resolve_rate_limit_name`), that TTL governs Redis, not
        the PG ``rate_limit_buckets`` row the materialization path
        publishes, so a bucket resolved with a PG pool has its schema
        captured at resolution time (publish outcome irrelevant, the
        acquire path can preseed the row) and is recorded here for row
        reclamation: :meth:`drain_pending_reservation_reclaims` deletes
        the published row on the same sweep cadence, the exact shape of
        the reservation-side reclamation (steady state without it: one
        ``rate_limit_buckets`` row per key ever seen, unbounded in the
        caller-controlled key space). A key that is acquired again after
        eviction is re-registered on next use (idempotent, see
        :meth:`_resolve_rate_limit_name`), and the drain drops its name
        from the pending set without deleting anything (a re-activated
        key owns its row again), so eviction is always safe to call,
        including concurrently with in-flight acquisitions for other
        keys.

        *max_pending_reclaims* caps the rate-limit pending-reclaim set
        (default: :data:`taskq.constants.DEFAULT_MAX_KEYED_RESERVATIONS`,
        the shared keyed-pending ceiling; the acquisition path's
        opportunistic eviction passes the settings-derived
        ``WorkerSettings.max_keyed_rate_limits``, so the pending set is
        bounded by the same ceiling that bounds the tracked entries). At
        the cap the eviction is VETOED, the entry stays registered and
        re-scanned on the next sweep, so no structure grows unbounded
        and no published row is orphaned by an eviction that could not
        be recorded (the same fail-closed bound as the reservation
        side; the pending-depth gauge and the
        ``registry-keyed-reclaim-pending-cap-veto`` warning are the
        visible signals).

        **Exemption: memory fixed-quota buckets.** A ``backend="memory"``
        bucket with ``refill_per_second == 0`` that has consumed part of its
        quota is NOT evicted (see :meth:`TokenBucket.holds_consumed_quota`):
        its token state lives only on the in-process instance, so eviction
        would silently reset the drained quota to full, and the next acquire
        would over-admit against a budget the tenant already spent. The
        exemption applies to both callers of this method (the per-worker
        sweep and the cap-pressure opportunistic eviction). Trade-off,
        deliberately chosen: an exempt bucket counts against
        ``settings.max_keyed_rate_limits`` until its quota returns to full
        (refund/reset) or the process restarts, so under sustained
        high-cardinality memory fixed-quota keys the cardinality cap can
        fill and deny NEW keys, the cap fails CLOSED with a warning rather
        than silently resetting quotas, which is the correct failure
        direction for a limiter.

        PG fixed-quota buckets are NOT exempt: their quota state lives in
        the ``rate_limit_buckets`` row, and no reclamation path here can
        lose it, the drain's DELETE below and the maintenance leader's
        fleet sweep share the consumed-quota veto
        (``_no_consumed_quota_sql``) that keeps a partly-spent row, and a
        re-materialized bucket resumes from that row (the acquire path
        preseeds ``ON CONFLICT DO NOTHING`` and reads the surviving state).
        Evicting the registry entry is pure bookkeeping recycling, which is
        what keeps the ``max_keyed_rate_limits`` cap from filling with
        never-again-used fixed-quota keys and refusing every new key.

        Returns the number of entries evicted.
        """
        cap = (
            DEFAULT_MAX_KEYED_RESERVATIONS if max_pending_reclaims is None else max_pending_reclaims
        )
        vetoed = 0

        def _admit(name: str, prim: TokenBucket | SlidingWindow) -> bool:
            nonlocal vetoed
            schema = self._keyed_rate_limit_row_schemas.get(name)
            if schema is None:
                # Never resolved with a PG pool on this worker: no publish,
                # no preseed (a pool-less acquire cannot touch PG), so this
                # registry created no row, nothing to reclaim from it. A
                # pool-bearing sibling that resolved the same key holds its
                # own capture for the concrete name and reclaims the row
                # through its own eviction+drain.
                return True
            if self._record_pending_reclaim(
                self._pending_rate_limit_reclaims, schema, name, cap=cap
            ):
                return True
            vetoed += 1
            return False

        evicted = self._evict_idle_keyed(
            self._keyed_rate_limit_last_used,
            self._rate_limits,
            idle_for,
            "registry-evicted-idle-keyed-rate-limits",
            preserve=_preserves_fixed_quota_state,
            admit=_admit,
        )
        # The publish-schema capture rides the registration's lifecycle:
        # an evicted bucket's schema must not survive into a future
        # re-registration of the same concrete name (a vetoed entry keeps
        # its capture, it is still registered and still owns its row).
        for name in evicted:
            self._keyed_rate_limit_row_schemas.pop(name, None)
        update_keyed_reclaim_pending(self._pending_reclaim_total())
        if vetoed:
            logger.warning(
                "registry-keyed-reclaim-pending-cap-veto",
                vetoed=vetoed,
                cap=cap,
                pending=self._pending_reclaim_total(),
            )
        return len(evicted)

    async def drain_pending_reservation_reclaims(
        self,
        pool: "asyncpg.Pool",
        *,
        batch_names: int = _DEFAULT_RECLAIM_BATCH_NAMES,
        acquire_timeout: "float | None" = None,
    ) -> int:
        """Delete the PG rows of evicted keyed buckets.

        Two row kinds, one bounded pass per schema per call:
        ``reservation_slots`` rows of evicted keyed reservations, and the
        published ``rate_limit_buckets`` rows of evicted keyed rate
        limits. For each kind and schema, at most *batch_names* pending
        bucket names and ONE batched DELETE. The slice is the FRONT of
        the per-schema insertion-ordered pending set. Driven by the
        per-worker sweep loop on the sweep cadence, immediately after
        the keyed evictions that feed it; a no-op (no connection
        acquired) when nothing is pending on either side.

        Semantics, what stays pending and why:

        - A name that re-registered since eviction is dropped from its
          pending set WITHOUT a statement: a re-activated key owns its
          rows again.
        - The reservation DELETE removes only free or lease-expired rows
         , a slot still held by a live lease survives (the
          over-admission invariant). A HELD slot is therefore the only
          reason a reservation name stays pending after its slice ran:
          the existence probe names the buckets whose rows survived the
          DELETE, and those names rotate to the BACK of the queue, the
          lease expires, the holder's release or the lock-expiry sweep
          frees the row, and a later drain (at most one full rotation
          away) deletes it. A name with no rows left, fully deleted
          this tick, or never materialized at all, leaves the pending
          set.
        - The rate-limit DELETE needs no HOLDER guard and no survivor
          probe, a bucket row has no holder or lease, so nothing
          survives the statement on that axis and every sliced name
          leaves the pending set in one pass, the queue drains FIFO
          with no survivors to rotate. It DOES carry a guard, and that
          guard is critical: the consumed-quota veto
          (``_no_consumed_quota_sql``, the same predicate the fleet
          sweep applies) refuses to delete a row whose fixed quota is
          partly spent, so an evicted spent bucket's row survives the
          drain and a re-materialized key resumes its spent state
          instead of resetting to full capacity (pinned by
          tests/test_keyed_fixed_quota_eviction.py). A vetoed name
          still leaves the pending set, the row stays, and the
          fleet sweep is the backstop once the quota is no longer
          consumed. A live bucket never reaches the statement at all:
          only evicted keyed names are ever recorded, and the
          re-registered check above drops a re-activated key first.
        - A schema key whose pending set empties (every name
          re-registered, or every row reclaimed) is popped after the
          pass, so a later drain with nothing pending acquires no
          connection at all.

        Instrumentation, on the failure path as much as the success
        path: duration in a ``finally`` (a timed-out drain still leaves
        a duration sample), deleted rows only as RETURNING-confirmed
        (a timed-out statement cannot masquerade as an empty drain),
        the drain-failure counter on exception (a STORAGE signal, a
        failing drain strands rows; the pending-depth gauge shows the
        backlog forming), and the pending-depth gauge after each drain.
        Raises on failure, callers guard (the sweep loop warns and
        continues on the next tick).

        *acquire_timeout* bounds the pool wait, for callers on a loop
        that must never block indefinitely on an exhausted pool (the
        sweep loop passes its dispatcher command timeout); the default
        matches the registry's other pool-bearing methods (unbounded).

        Returns the number of rows deleted.
        """
        if not self._pending_reservation_reclaims and not self._pending_rate_limit_reclaims:
            return 0
        start = monotonic()
        total_deleted = 0
        try:
            async with pool.acquire(timeout=acquire_timeout) as conn:
                for schema in list(self._pending_reservation_reclaims):
                    if not _IDENT_RE.match(schema):
                        raise ValueError(f"invalid schema identifier: {schema!r}")
                    pending = self._pending_reservation_reclaims.get(schema)
                    if not pending:
                        self._drop_pending_schema_if_empty(
                            self._pending_reservation_reclaims, schema
                        )
                        continue
                    # Round-robin: the front batch (insertion order) is
                    # served first; survivors re-enter at the back below.
                    candidates = list(pending)[:batch_names]
                    slice_names: list[str] = []
                    for name in candidates:
                        if name in self._reservations or name in self._keyed_reservation_last_used:
                            # Re-registered since eviction: the bucket is
                            # live again and owns its rows, drop the
                            # pending entry, touch nothing in PG.
                            pending.pop(name, None)
                        else:
                            slice_names.append(name)
                    if not slice_names:
                        self._drop_pending_schema_if_empty(
                            self._pending_reservation_reclaims, schema
                        )
                        continue
                    deleted_rows = await conn.fetch(
                        _RECLAIM_SLICE_DELETE_SQL_TEMPLATE.format(schema=schema),
                        slice_names,
                    )
                    total_deleted += len(deleted_rows)
                    surviving: set[str] = {
                        row["bucket_name"]
                        for row in await conn.fetch(
                            _RECLAIM_SLICE_EXISTING_SQL_TEMPLATE.format(schema=schema),
                            slice_names,
                        )
                    }
                    # Tolerant pops: the DELETE/probe awaits can interleave
                    # with another drain pass on the same registry, a
                    # missing key means the name was already removed, and
                    # re-removing it must not turn into a spurious drain
                    # failure.
                    for name in slice_names:
                        pending.pop(name, None)
                    for name in surviving:
                        # A survivor (rows still held) re-enters at the
                        # BACK: its next chance comes after every other
                        # pending bucket's, never before them.
                        pending[name] = None
                    self._drop_pending_schema_if_empty(self._pending_reservation_reclaims, schema)
                for schema in list(self._pending_rate_limit_reclaims):
                    if not _IDENT_RE.match(schema):
                        raise ValueError(f"invalid schema identifier: {schema!r}")
                    pending = self._pending_rate_limit_reclaims.get(schema)
                    if not pending:
                        self._drop_pending_schema_if_empty(
                            self._pending_rate_limit_reclaims, schema
                        )
                        continue
                    # Front batch, same as the reservation pass; with no
                    # survivors there is nothing to rotate, the names
                    # that fit the slice are served in insertion order.
                    candidates = list(pending)[:batch_names]
                    slice_names: list[str] = []
                    for name in candidates:
                        if name in self._rate_limits or name in self._keyed_rate_limit_last_used:
                            # Re-registered since eviction: the bucket is
                            # live again and owns its published row, drop
                            # the pending entry, touch nothing in PG.
                            pending.pop(name, None)
                        else:
                            slice_names.append(name)
                    if not slice_names:
                        self._drop_pending_schema_if_empty(
                            self._pending_rate_limit_reclaims, schema
                        )
                        continue
                    deleted_rows = await conn.fetch(
                        _RECLAIM_RATE_LIMIT_SLICE_DELETE_SQL_TEMPLATE.format(schema=schema),
                        slice_names,
                    )
                    total_deleted += len(deleted_rows)
                    for name in slice_names:
                        pending.pop(name, None)
                    self._drop_pending_schema_if_empty(self._pending_rate_limit_reclaims, schema)
        except Exception as exc:
            record_reservation_reclaim_drain_failure(type(exc).__name__)
            raise
        finally:
            record_reservation_reclaim_drain_duration(monotonic() - start)
            record_reservation_reclaim_drain_rows(total_deleted)
            update_keyed_reclaim_pending(self._pending_reclaim_total())
        return total_deleted

    def _drop_pending_schema_if_empty(
        self, pending: dict[str, dict[str, None]], schema: str
    ) -> None:
        """Pop a schema key whose pending set is empty.

        An empty-set key is invisible to ``has_pending_reservation_reclaims``
        but keeps the drain's top-level truthiness gate passing, so every
        later drain would acquire a connection to discover nothing.
        """
        if not pending.get(schema):
            pending.pop(schema, None)

    def clear(self) -> None:
        """Reset ALL mutable registry state, a test aid, NOT safe while running.

        Clears the nine dicts (``_rate_limits``, ``_reservations``,
        ``_keyed_reservation_last_used``, ``_keyed_rate_limit_last_used``,
        ``_keyed_rate_limit_row_schemas``,
        ``_pending_reservation_reclaims``,
        ``_pending_rate_limit_reclaims``,
        ``_keyed_reservation_heal_attempted``,
        ``_keyed_reservation_heal_failure_logged``)
        AND resets the two opportunistic-eviction scan timestamps
        (``_keyed_reservation_last_eviction_scan`` /
        ``_keyed_rate_limit_last_eviction_scan``) to ``float("-inf")``.
        Omitting the timestamps would leave the opportunistic-eviction
        throttle stamped, silently suppressing scans for up to
        ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` (30 s) in the next test;
        omitting a pending-reclaim set, a publish-schema capture, or a
        heal stamp would leak one test's evictions into the next.

        **Not safe to call while a worker is running**, concurrent
        dispatch / sweep iteration over the dicts would observe
        inconsistent state. Use for per-test isolation only.

        Like the eviction methods, this resets IN-PROCESS bookkeeping
        only, it does NOT touch Redis bucket hashes or Postgres
        ``reservation_slots`` / ``rate_limit_buckets`` rows; backend
        state persists and will be observed on next acquire.
        """
        self._rate_limits.clear()
        self._reservations.clear()
        self._keyed_reservation_last_used.clear()
        self._keyed_rate_limit_last_used.clear()
        self._keyed_rate_limit_row_schemas.clear()
        self._pending_reservation_reclaims.clear()
        self._pending_rate_limit_reclaims.clear()
        self._keyed_reservation_heal_attempted.clear()
        self._keyed_reservation_heal_failure_logged.clear()
        self._keyed_reservation_last_eviction_scan = float("-inf")
        self._keyed_rate_limit_last_eviction_scan = float("-inf")
        update_keyed_reclaim_pending(0)


# The reclaim drain's rate-limit statement, beside the publish statement
# whose rows it reclaims (the same locality as the reservation reclaim
# templates in ratelimit/reservation.py). A rate_limit_buckets row has no
# holder, so there is no lease guard like the reservation twin's, but
# idleness is still not evidence the row is safe to delete. For a
# PG-backed bucket the row IS the state, so deleting one that still holds
# consumed fixed quota lets the next acquire re-preseed at full capacity
# and re-admit a budget the tenant already spent, the exact damage the
# memory backend's eviction exemption prevents on the instance side.
# Idle eviction exists to bound registry growth, not to reset quotas, so
# it must do neither. The guard is shared with the fleet sweep
# (_no_consumed_quota_sql) so the two cannot disagree about which rows
# are safe. A vetoed name still leaves the pending set, every sliced
# name does, and the fleet sweep is its backstop once the quota is no
# longer consumed.
_RECLAIM_RATE_LIMIT_SLICE_DELETE_SQL_TEMPLATE = f"""\
DELETE FROM "{{schema}}".rate_limit_buckets
WHERE bucket_name = ANY($1)
  AND {_no_consumed_quota_sql()}
RETURNING bucket_name"""  # noqa: S608  # Why: the only interpolation is this module's own constant predicate; schema is caller-formatted from the _IDENT_RE-validated setting and bucket_name is $1-bound


async def _upsert_rate_limit_bucket_row(
    pool: "asyncpg.Pool",
    schema: str,
    name: str,
    kind: str,
    *,
    keyed: bool = False,
) -> None:
    """Insert one ``rate_limit_buckets`` row (idempotent).

     Shared by :func:`sync_rate_limit_buckets` (startup bulk publish of
     statically registered primitives, ``keyed`` stays False, a static
     row is never fleet-reclaimable) and the keyed-materialization paths
     in :meth:`RateLimitRegistry._resolve_rate_limit_name` (publish on
     first acquisition and on the first pool-bearing reuse), so both
     write identical rows, the keyed paths pass the bucket's
     fleet-reclaimable mark, which is True only for PG-state-backed
     keyed buckets (see :class:`TokenBucket`'s ``keyed`` docstring).

     Uses ``ON CONFLICT DO NOTHING`` so concurrent workers and restarts
     are idempotent. The row is born with a fresh ``last_used_at`` (the
     horizon starts ticking at publish time), so a keyed bucket that is
     published but never acquired is still reclaimable after the horizon
    , the acquire path's own stamps take over from the first acquire.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    upsert_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at, keyed, last_used_at) '  # noqa: S608
        f"VALUES ($1, $2, '{{}}'::jsonb, clock_timestamp(), $3, clock_timestamp()) "
        f"ON CONFLICT (bucket_name) DO NOTHING"
    )
    async with pool.acquire() as conn:
        await conn.execute(upsert_sql, name, kind, keyed)


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
    acquisition path, see
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

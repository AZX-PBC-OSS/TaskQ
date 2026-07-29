"""TTL-bounded client-side snapshot of stored ``actor_config`` capacity.

``max_pending`` is operator-owned: once an ``actor_config`` row exists, a
non-NULL stored value is authoritative and the ``@actor(max_pending=...)``
literal is only the seed (see ``taskq/worker/startup.py``). The enqueue
path therefore needs the stored value — but ``enqueue()`` is a hot path
and must not pay a query per call (the ``max_pending`` count check
itself already costs one query when a limit is in play).

:class:`ActorCapacityCache` is the deliberate compromise: one small
whole-table read (``SELECT actor, max_pending FROM actor_config`` —
one row per actor) is cached in-process and reused for up to ``ttl``
seconds. Consequences, all by design:

* **Bounded staleness.** An operator change via
  ``taskq actor-config set --max-pending`` is invisible to a given
  client process for at most ``ttl`` seconds (default 5). Dispatch-side
  capacity (``max_concurrent``) has no such window — the dispatch query
  re-reads the table every cycle — which is why only ``max_pending``
  needs this cache.
* **Bounded cost.** At most one refresh query per ``ttl`` per process,
  regardless of enqueue rate; concurrent enqueues share a single
  refresh (single-flight lock).
* **Bounded wait.** The refresh read is wrapped in
  ``asyncio.wait_for(..., read_timeout)`` (default 2s). Without it, an
  exhausted backend pool would block the read — and, because the read
  runs under the single-flight lock, every other enqueue in the process
  behind it — indefinitely. A timeout becomes an ordinary refresh
  failure and takes the fail-open path below.
* **Fail-open to the code literal.** A failed refresh logs a warning
  and the resolver falls back to the last good snapshot (or the
  ``@actor`` literal if none), retrying no sooner than ``ttl`` — a sick
  database never turns into a per-enqueue failing query storm.
* **Explicit invalidation.** :meth:`invalidate` drops the snapshot so
  the next read refreshes immediately (tests, and operator tooling that
  knows it just changed the table). An epoch counter makes this safe
  against a refresh already in flight: a read that started before the
  invalidation is discarded on completion instead of re-stamping the
  pre-invalidation snapshot for another full TTL.
* **Fail-fast on contract drift.** The first capacity resolution
  requires the backend to implement ``get_actor_max_pending``
  (``BACKEND_PROTOCOL_VERSION`` 3) and raises ``TypeError`` if it does
  not. A backend built against an older protocol would otherwise hit
  ``AttributeError`` inside the fail-open handler on every refresh and
  silently enforce code literals forever — exactly the silent drift the
  protocol version exists to prevent. The check fires once at first
  *use* (cached), not at construction, so partial backend doubles that
  never exercise the enqueue path are unaffected. A backend that sets
  ``get_actor_max_pending = None`` is also caught — the guard verifies
  the attribute is callable, not merely present.

Resolution rule (shared by every enqueue path), given the stored value,
the ``@actor`` literal, and an optional per-call ``max_pending=``
argument:

1. A non-NULL **stored** value wins over the literal — the operator's
   cap is authoritative and can both loosen and tighten it ("no row"
   and "row with NULL" both fall through to the literal; clearing an
   override reverts to the code default, exactly like ``result_ttl``).
2. An explicit **per-call** argument is always honored in the tightening
   direction and never weakened: with a stored cap the effective limit is
   ``min(stored, per_call)`` — a caller shedding load must not be
   widened by an operator override. Against the *literal* (no stored
   value) the per-call argument wins outright, in both directions —
   actor code may loosen its own declaration; that is the historical
   behavior. Against the *stored* value it may not: the operator cap is
   a fleet ceiling no code path can raise.

This deliberately differs from ``max_concurrent``, where a stored NULL
means *unlimited*: the dispatch SQL cannot see the code literal, while
this resolver can.
"""

import asyncio
import math
import time

import structlog

from taskq.backend._protocol import BACKEND_PROTOCOL_VERSION, Backend

__all__ = ["DEFAULT_CAPACITY_CACHE_TTL", "DEFAULT_CAPACITY_READ_TIMEOUT", "ActorCapacityCache"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

DEFAULT_CAPACITY_CACHE_TTL: float = 5.0
"""Seconds a snapshot is reused before the next read refreshes it."""

DEFAULT_CAPACITY_READ_TIMEOUT: float = 2.0
"""Seconds a single refresh read may take before it fails open.

Aligned with the 2s ``command_timeout`` the worker uses for heartbeat
writes (``taskq/worker/deps.py``): long enough for a healthy small
whole-table read, short enough that an exhausted pool fails open before
enqueue latency becomes an outage.
"""


class ActorCapacityCache:
    """TTL-bounded snapshot of ``actor_config.max_pending`` for one backend.

    One instance per client process (``JobsClient`` owns one;
    ``SubJobEnqueuer`` accepts one or builds its own). Not shared across
    threads or event loops — the owning client's loop drives it.
    """

    def __init__(
        self,
        backend: Backend,
        *,
        ttl: float = DEFAULT_CAPACITY_CACHE_TTL,
        read_timeout: float = DEFAULT_CAPACITY_READ_TIMEOUT,
    ) -> None:
        if ttl < 0:
            raise ValueError(f"capacity cache ttl must be >= 0, got {ttl!r}")
        if read_timeout <= 0 or not math.isfinite(read_timeout):
            raise ValueError(
                f"capacity cache read_timeout must be a positive finite number, got {read_timeout!r}"
            )
        self._backend = backend
        self._ttl = ttl
        self._read_timeout = read_timeout
        self._rows: dict[str, int | None] = {}
        self._refreshed_at: float | None = None
        self._epoch = 0
        self._lock = asyncio.Lock()
        self._backend_checked = False

    def _stale(self) -> bool:
        return self._refreshed_at is None or (time.monotonic() - self._refreshed_at) >= self._ttl

    async def _refresh(self) -> None:
        if not self._stale():
            return
        async with self._lock:
            if not self._stale():
                return  # another task refreshed while we waited
            epoch = self._epoch
            try:
                # wait_for bounds the whole read — pool acquisition
                # included. Without it an exhausted pool blocks here
                # (asyncpg acquire has no default timeout) while the lock
                # is held, stacking up every enqueue in the process.
                rows = await asyncio.wait_for(
                    self._backend.get_actor_max_pending(), timeout=self._read_timeout
                )
            except Exception as exc:
                # Fail-open: keep the last good snapshot (or empty → the
                # caller's literal). Stamping refreshed_at bounds the
                # retry rate to 1/ttl so a sick backend is not queried
                # on every enqueue.
                logger.warning(
                    "actor-capacity-cache-refresh-failed",
                    error_class=type(exc).__name__,
                    error=str(exc),
                )
            else:
                if epoch == self._epoch:
                    self._rows = rows
            if epoch != self._epoch:
                # invalidate() fired while the read was in flight: the
                # result may predate the change the caller wanted
                # re-read. Do not stamp — the next read refreshes.
                return
            self._refreshed_at = time.monotonic()

    async def effective_max_pending(
        self, actor: str, literal: int | None, *, per_call: int | None = None
    ) -> int | None:
        """Return the enforced ``max_pending`` for *actor*.

        *literal* is the ``@actor(max_pending=...)`` declaration; it
        applies whenever no non-NULL stored value exists. *per_call* is
        an explicit ``max_pending=`` argument from the enqueue call
        itself: honored in the tightening direction against a stored cap
        (``min(stored, per_call)`` — load-shedding callers are never
        widened), and winning outright against the literal (the
        historical behavior — actor code may loosen its own declaration,
        but never an operator's cap). See the module docstring for the
        full rule. Refreshes lazily when stale.

        Raises :class:`TypeError` if the backend does not implement
        ``get_actor_max_pending`` — contract drift fails fast here, at
        first use, rather than degrading silently through the fail-open
        path (see the module docstring).
        """
        # Check at first use (cached) — not per-call.  Using
        # callable(getattr(..., None)) rather than hasattr catches both
        # an absent method (getattr returns None) and a backend that
        # sets ``get_actor_max_pending = None`` (callable(None) is
        # False).  MagicMock test doubles still pass because
        # getattr(vivifies a callable child.
        if not self._backend_checked:
            method = getattr(self._backend, "get_actor_max_pending", None)
            if not callable(method):
                raise TypeError(
                    f"{type(self._backend).__name__} does not implement get_actor_max_pending "
                    f"(BACKEND_PROTOCOL_VERSION {BACKEND_PROTOCOL_VERSION}). A backend built "
                    "against an older protocol would silently enforce @actor literals "
                    "forever through this cache's fail-open path — failing fast at "
                    "first use instead. Implement the method or upgrade the backend."
                )
            self._backend_checked = True
        await self._refresh()
        stored = self._rows.get(actor)
        if stored is not None:
            return min(stored, per_call) if per_call is not None else stored
        return per_call if per_call is not None else literal

    def invalidate(self) -> None:
        """Drop the snapshot; the next read refreshes from the backend.

        Bumps the refresh epoch so a read already in flight is discarded
        on completion rather than re-stamping the pre-invalidation
        snapshot for another full TTL.
        """
        self._refreshed_at = None
        self._epoch += 1

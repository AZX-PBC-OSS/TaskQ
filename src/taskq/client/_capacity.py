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
* **Fail-open to the code literal.** A failed refresh logs a warning
  and the resolver falls back to the last good snapshot (or the
  ``@actor`` literal if none), retrying no sooner than ``ttl`` — a sick
  database never turns into a per-enqueue failing query storm.
* **Explicit invalidation.** :meth:`invalidate` drops the snapshot so
  the next read refreshes immediately (tests, and operator tooling that
  knows it just changed the table).

Resolution rule (shared by every enqueue path):
**a non-NULL stored value wins; otherwise the literal applies.** "No
row" and "row with NULL" both fall through to the literal — clearing an
override (``--clear-max-pending``) reverts to the code default, exactly
like ``result_ttl``. This deliberately differs from ``max_concurrent``,
where a stored NULL means *unlimited*: the dispatch SQL cannot see the
code literal, while this resolver can.
"""

import asyncio
import time

import structlog

from taskq.backend._protocol import Backend

__all__ = ["DEFAULT_CAPACITY_CACHE_TTL", "ActorCapacityCache"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

DEFAULT_CAPACITY_CACHE_TTL: float = 5.0
"""Seconds a snapshot is reused before the next read refreshes it."""


class ActorCapacityCache:
    """TTL-bounded snapshot of ``actor_config.max_pending`` for one backend.

    One instance per client process (``JobsClient`` owns one;
    ``SubJobEnqueuer`` accepts one or builds its own). Not shared across
    threads or event loops — the owning client's loop drives it.
    """

    def __init__(self, backend: Backend, *, ttl: float = DEFAULT_CAPACITY_CACHE_TTL) -> None:
        if ttl < 0:
            raise ValueError(f"capacity cache ttl must be >= 0, got {ttl!r}")
        self._backend = backend
        self._ttl = ttl
        self._rows: dict[str, int | None] = {}
        self._refreshed_at: float | None = None
        self._lock = asyncio.Lock()

    def _stale(self) -> bool:
        return self._refreshed_at is None or (time.monotonic() - self._refreshed_at) >= self._ttl

    async def _refresh(self) -> None:
        if not self._stale():
            return
        async with self._lock:
            if not self._stale():
                return  # another task refreshed while we waited
            try:
                self._rows = await self._backend.get_actor_max_pending()
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
            self._refreshed_at = time.monotonic()

    async def effective_max_pending(self, actor: str, fallback: int | None) -> int | None:
        """Return the enforced ``max_pending`` for *actor*.

        A non-NULL stored value wins; otherwise *fallback* (the
        ``@actor(...)`` literal, or a per-call override the caller
        resolved) applies. Refreshing lazily when stale — see the module
        docstring for the staleness/cost bounds.
        """
        await self._refresh()
        stored = self._rows.get(actor)
        return stored if stored is not None else fallback

    def invalidate(self) -> None:
        """Drop the snapshot; the next read refreshes from the backend."""
        self._refreshed_at = None

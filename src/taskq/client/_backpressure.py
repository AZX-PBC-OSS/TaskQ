"""The backpressure read's orchestration (LIB-2, issue #670).

A read-only typed read on the submit path, mirroring the reader-method
precedent (``get_row`` / ``list``): one call, one typed
:class:`~taskq.types.BackpressureSnapshot`. NOT an enqueue kwarg —
``enqueue`` is generic ``JobHandle[R]`` and no flag may flip the reply
type.

The design, stated once here:

* DEPTH — one indexed aggregate per call
  (``count_pending_jobs_by_queue``, grouped by queue over pending +
  scheduled) — the queue-local OPS view, a REPORTED field. It is NOT
  the verdict's basis: the admission cap is PER-ACTOR and governs the
  actor's count across ALL queues (``enqueue_max_pending_count``, WHERE
  actor = $1 — _sql_templates.py), so a queue-local count against an
  actor cap would false-OK exactly when an actor's traffic splits
  across queues (the F1 review finding: 9 pending here + 9 there reads
  ok while the next enqueue is refused at 18 >= 10).
* VERDICT — the routing actors' OWN counts (``count_pending_jobs``, the
  actor-grouped read the cap actually governs): ``over`` when ANY
  routing actor with a resolvable stored cap is at/over its own
  boundary; ``ok`` only when EVERY routing actor resolves and none is
  over (an unresolvable actor may be over — claiming ok would be the
  false-ok again). The binding actor (smallest headroom), its load, and
  its cap are carried on the entry.
* CAP — the actor's effective ``max_pending`` from the
  ``ActorCapacityCache`` TTL snapshot (zero I/O beyond the one warm-up
  refresh): the data model has NO per-queue depth threshold, so "over"
  is defined against the queue's ACTOR caps. A routing actor whose cap
  lives only in the ``@actor`` literal reads ``unknown``: the literal
  is code-side, invisible to this process — set a stored override
  (``taskq actor-config set --max-pending``) to make the read exact.
* FAN-OUT — the parent's pending children, EXACT: the ``parent_id``
  ledger stamp (01.00.23_01), no tag approximation. REPORTED, not
  verdict-bearing: every pending child already counts toward its OWN
  actor's cap (wherever it sits), which is the number the admission
  check enforces — the fan-out pressure reaches the verdict through the
  routing actors' admission loads, and the ledger here is the
  parent-attributed view of the same rows.
* FAIL-OPEN — every unavailable half reads as the explicit ``unknown``
  state with a reason (sick database, capability-less backend, no
  capacity snapshot). NEVER a fabricated verdict, the
  ``maybe_warn_unserved_queue`` posture. The ONE exception: a missing
  schema (asyncpg ``UndefinedTableError``) propagates, so the client's
  ``SchemaNotMigratedError`` translation names the remedy — a setup
  defect is not degraded data.

The staged-read pattern (the ``get_actor_queues`` precedent): both
backend reads are OPTIONAL capabilities detected by attribute, so
backends built before they existed keep working with the affected
verdicts reading ``unknown`` instead of raising.
"""

import asyncio
import time
from collections.abc import Coroutine
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

import structlog

from taskq.types import BackpressureSnapshot, BackpressureState, QueueBackpressure

if TYPE_CHECKING:
    from taskq.backend._protocol import Backend, JobId
    from taskq.client._capacity import ActorCapacityCache

__all__ = ["read_backpressure"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class _StagedReader(Protocol):
    """The OPTIONAL staged reads (the ``get_actor_queues`` pattern:
    detected by attribute, so older backends and test doubles simply do
    not carry them and the affected verdicts read ``unknown``).
    Deliberately loose — a staged capability is older than the protocol
    that would type it."""

    def __call__(self, *args: Any) -> Any: ...


_GroupedResult = "tuple[dict[str, int] | None, str | None]"


async def _resolve_now(
    result: dict[str, int] | None, reason: str | None
) -> "tuple[dict[str, int] | None, str | None]":
    return result, reason


async def _run_grouped(
    reader: object,
    *,
    cap_name: str,
    read_timeout: float,
    call: "tuple[Any, ...]",
) -> "tuple[dict[str, int] | None, str | None]":
    try:
        result: object = await asyncio.wait_for(
            cast("_StagedReader", reader)(*call), timeout=read_timeout
        )
    except Exception as exc:
        if _is_missing_schema(exc):
            # Missing schema: a SETUP defect, not degraded data — propagates
            # so the client's SchemaNotMigratedError translation names the
            # remedy.
            raise
        logger.warning(
            "backpressure-read-failed",
            kind="backpressure_read_failed",
            read=cap_name,
            error_class=type(exc).__name__,
            error=str(exc),
        )
        return None, f"{cap_name} read failed: {type(exc).__name__}"
    if not isinstance(result, dict):
        # Contract-drift guard, the get_actor_queues discipline: a mock
        # auto-vivification or backend bug must not turn the verdict into
        # a TypeError downstream.
        return None, f"{cap_name} returned {type(result).__name__}, expected dict[str, int]"
    grouped = cast("dict[object, object]", result)
    return {str(k): int(str(v)) for k, v in grouped.items()}, None


def _is_missing_schema(exc: BaseException) -> bool:
    """True for asyncpg's ``UndefinedTableError`` — the lazy-import
    discipline (asyncpg may be absent; then no schema errors exist)."""
    try:
        import asyncpg
    except ImportError:  # pragma: no cover - the postgres extra absent
        return False
    return isinstance(exc, asyncpg.exceptions.UndefinedTableError)


def _read_grouped(
    reader: object | None,
    *,
    cap_name: str,
    read_timeout: float,
    call: "tuple[Any, ...]",
) -> "Coroutine[Any, Any, tuple[dict[str, int] | None, str | None]]":
    """One staged aggregate round trip, bounded and fail-open.

    ``(None, reason)`` when the backend lacks the read or the read
    fails/times out — the caller's ``unknown`` state, never a raise
    (except the missing-schema case, which propagates). ``(mapping,
    None)`` on success.
    """
    if reader is None:
        return _resolve_now(
            None, f"backend does not implement {cap_name} (staged read; upgrade the backend)"
        )
    return _run_grouped(reader, cap_name=cap_name, read_timeout=read_timeout, call=call)


async def read_backpressure(
    backend: "Backend",
    cache: "ActorCapacityCache",
    queues: "list[str]",
    *,
    parent_id: "JobId | None",
    read_timeout: float,
) -> BackpressureSnapshot:
    """Build the snapshot: one depth aggregate, one optional children
    aggregate, caps from the live capacity snapshot, one verdict per queue.

    The verdict is a pure composition of the three halves; every
    unavailable half forces ``unknown`` with the reason named.
    """
    started_at = time.monotonic()

    depth_map: dict[str, int] | None = None
    if queues:
        depth_map, _ = await _read_grouped(
            getattr(backend, "count_pending_jobs_by_queue", None),
            cap_name="count_pending_jobs_by_queue",
            read_timeout=read_timeout,
            call=(queues,),
        )

    children_map: dict[str, int] | None = None
    if parent_id is not None:
        children_map, _ = await _read_grouped(
            getattr(backend, "count_pending_children_by_queue", None),
            cap_name="count_pending_children_by_queue",
            read_timeout=read_timeout,
            call=(parent_id,),
        )

    children_total = sum(children_map.values()) if children_map is not None else None

    entries: dict[str, QueueBackpressure] = {}
    queue_routing: dict[str, dict[str, int | None] | None] = {}
    wanted_actors: set[str] = set()
    for queue in queues:
        depth = depth_map.get(queue, 0) if depth_map is not None else None
        # The fan-out ledger attribution, exact and double-count-free:
        # this queue's own children are IN its depth, so the reported
        # children_depth is the parent's pending children OUTSIDE it.
        children_depth: int | None = None
        if children_total is not None:
            children_depth = children_total - (children_map or {}).get(queue, 0)

        routing = cache.peek_queue_caps(queue)
        # None (no snapshot) and {} (unserved) are DIFFERENT unknown
        # reasons; the map keeps the distinction, the verdict below
        # renders it.
        queue_routing[queue] = routing
        if routing is not None:
            wanted_actors.update(a for a, c in routing.items() if c is not None)

        entries[queue] = QueueBackpressure(
            queue=queue,
            depth=depth,
            children_depth=children_depth,
            state="unknown",  # resolved below, after the actor counts land
        )

    # The VERDICT's basis: the routing actors' OWN pending+scheduled
    # counts, ALL queues (enqueue_max_pending_count's grouping — the cap
    # governs the ACTOR, not the queue; a queue-local count against an
    # actor cap would false-OK exactly when an actor's traffic splits
    # across queues, the F1 review finding). One round trip for the
    # union of every queried queue's resolvable routing actors.
    actor_counts: dict[str, int] | None = None
    actor_reason: str | None = None
    if wanted_actors:
        actor_counts, actor_reason = await _read_grouped(
            getattr(backend, "count_pending_jobs", None),
            cap_name="count_pending_jobs",
            read_timeout=read_timeout,
            call=(sorted(wanted_actors),),
        )

    for queue in queues:
        entry = entries[queue]
        routing = queue_routing[queue]

        # The verdict: the routing actors' own admission states, the
        # 3-valent OR. Every compared half must be present; any absent
        # half is unknown with the reasons named — never a fabricated
        # verdict.
        reasons: list[str] = []
        if routing is None:
            reasons.append(
                "actor capacity snapshot unavailable (no successful refresh yet, or the "
                "backend lacks the optional get_actor_queues read / its read failed)"
            )
        elif not routing:
            reasons.append(
                "no stored actor_config assignment routes this queue; unless a worker "
                "consumes this queue via --queues/TASKQ_QUEUES the jobs are unserved "
                "(snapshot is TTL-bounded)"
            )
        if actor_counts is None and actor_reason is not None:
            reasons.append(actor_reason)

        resolvable = {a: c for a, c in (routing or {}).items() if c is not None}
        if routing and not resolvable:
            names = ", ".join(sorted(routing))
            reasons.append(
                f"no stored max_pending override for the routing actor(s) [{names}]; the "
                "effective cap is the @actor literal, which this process cannot see — "
                "set a stored override (taskq actor-config set --max-pending) to make "
                "this read exact"
            )

        if reasons:
            entries[queue] = QueueBackpressure(
                queue=queue,
                depth=entry.depth,
                children_depth=entry.children_depth,
                state="unknown",
                reason="; ".join(reasons),
            )
            continue

        assert resolvable and actor_counts is not None
        headrooms = sorted(
            (cap - actor_counts.get(actor, 0), actor, cap) for actor, cap in resolvable.items()
        )
        min_headroom, binding_actor, binding_cap = headrooms[0]
        binding_load = binding_cap - min_headroom
        if min_headroom <= 0:
            state: BackpressureState = "over"
        elif len(resolvable) < len(routing or {}):
            # A resolvable actor is below its boundary, but another
            # routing actor's cap is the @actor literal, unresolvable
            # here: it may be over. Claiming ok would be the F1-class
            # false-ok again.
            state = "unknown"
            reasons.append(
                "some routing actor(s) enforce only the @actor literal (unresolvable "
                "here) and may be at or over their boundary"
            )
        else:
            state = "ok"

        entries[queue] = QueueBackpressure(
            queue=queue,
            depth=entry.depth,
            children_depth=entry.children_depth,
            binding_actor=binding_actor,
            effective_max_pending=binding_cap,
            admission_load=binding_load,
            state=state,
            reason="; ".join(reasons) if reasons else None,
        )

    logger.debug(
        "backpressure_read",
        kind="backpressure_read",
        queues=len(queues),
        parent_id=str(parent_id) if parent_id is not None else None,
        elapsed_ms=round((time.monotonic() - started_at) * 1000, 3),
    )
    return BackpressureSnapshot(
        queues=entries,
        parent_id=parent_id,
        # The as-of story, carried explicitly (the prefect lesson): a
        # typed snapshot that does not say how old it is would be a lie
        # by omission. The depth half was exact at this instant; the cap
        # half was already cap_age_seconds old.
        as_of=datetime.now(UTC),
        cap_age_seconds=cache.snapshot_age(),
    )

"""The flow-run reducer resolution (T04's tx1→tx2 crash window, hardened
across processes).

THE WINDOW: ``finalize_node`` = tx1 (the fenced terminal) then tx2 (the
decrement + fire + REDUCER BODY + outbox). A worker killed after tx1 and
before tx2 leaves the decrement undone; the sweep's healing pass
(``sweep_join_rederive``) reconciles the cache and FIRES the join — but a
fire without its reducer body dispatches consumers off an UN-REDUCED join:
the join's output never exists, "at-least-once body execution" degraded to
NEVER.

THE CURE — THE REGISTRY IS THE TRUTH, THE MEMO IS A CACHE: the reducer
body resolves OUTSIDE the finalizing process's memory. ``insert_flow_run``
stamps the flow root's metadata with its workflow's registered name
(``metadata.workflow`` — schema-level, every process reads it); the
sweep's fire arm resolves the join's body FROM THE REGISTERED DEFINITION
via that name (:func:`taskq.workflows.definitions.resolve_step_body`, D1's
BODY-FROM-DEFINITION discipline) and runs it INSIDE the sweep's
transaction. The definition registry is populated by the definitions'
import — every worker process in the fleet carries the SAME registry
content, so a flow finalized in process A heals with its real body in
process B: the exactly-once boundary is the FIRE's boundary, never the
body's — a raising body rolls the sweep tx back (the fire row and the
outbox rows with it), the re-derive re-fires, and the body RE-RUNS — the
same at-least-once doctrine the finalize's tx2 states.

The process-local memo (:func:`register_flow_reducers`) is a CACHE, never
the source of truth: it serves only a flow the registry cannot resolve (a
root stamped before the workflow-name stamp existed, an anonymous flow),
in the process whose finalize warmed it. It NEVER shadows the definition:
when the stamped name resolves, the definition's body wins. The memo is
bounded — the phantom reaper drops a terminal flow's entry
(:func:`forget_flow_reducers` at the reap; a terminal flow's joins can
never fire, the fire's own flow-status leg refuses them).

D1 (BODY-FROM-DEFINITION) is preserved: the public dispatch resolves step
bodies from the registered definition only. The reducer step inherits the
run-key arbiter + the attempt fence the same way its outbox-consumer
siblings do: the fired join's consumer rows (and the reducer's own row,
keyed ``wf:{flow}:{join_step_key}`` on the composite arbiter) are deduped
by the arbiter, and the claim that grants the body's re-run is fenced by
the ledger.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from taskq.backend._protocol import JobId

__all__ = ["forget_flow_reducers", "register_flow_reducers", "resolve_flow_reducer"]

#: The flow-run reducer CACHE: flow_id → (join step_key → body). Warmed by
#: the finalizing process only — never the source of truth (the module
#: docstring states the resolution order).
_flow_reducers: dict[JobId, dict[str, Callable[[], Awaitable[None]]]] = {}


def register_flow_reducers(
    flow_id: JobId,
    reducers: dict[str, Callable[[], Awaitable[None]]],
) -> None:
    """Warm the flow run's reducer cache (the finalize declares the bodies
    its joins re-derive with). Idempotent per (flow, step): a re-finalize
    of the same run re-registers the same keys. CACHE ONLY — the heal
    resolves from the registered definition first; this memo answers only
    for flows the definition registry cannot resolve, in this process."""
    _flow_reducers.setdefault(flow_id, {}).update(reducers)


def resolve_flow_reducer(
    flow_id: JobId,
    step_key: str,
    *,
    workflow_name: str | None = None,
) -> Callable[[], Awaitable[None]] | None:
    """The body for a fired join, DURABLY: the REGISTERED DEFINITION of the
    workflow named on the flow root's metadata (D1 — the registry every
    process carries) first; the process-local cache second (a flow the
    registry cannot resolve, warmed by this process's own finalize).
    ``None`` = no resolvable body anywhere — the fire delivers the
    declared consumers; nothing else runs."""
    if workflow_name:
        from taskq.workflows.definitions import resolve_step_body

        try:
            definition_body = resolve_step_body(workflow_name, step_key)
        except KeyError:
            definition_body = None
        if definition_body is not None:

            async def _definition_adapter() -> None:
                await definition_body(None)

            return _definition_adapter
    # The cache answers ONLY when the registry could not: it never
    # shadows the definition (D1).
    return _flow_reducers.get(flow_id, {}).get(step_key)


def forget_flow_reducers(flow_id: JobId) -> None:
    """Drop a flow run's cache entry (the terminal flow's entry — the
    phantom reaper calls this at the reap, so the map is bounded by THIS
    process's live flow runs)."""
    _flow_reducers.pop(flow_id, None)

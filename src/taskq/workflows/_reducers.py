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
from dataclasses import dataclass

from taskq.backend._protocol import JobId

__all__ = [
    "FlowReducerResolution",
    "forget_flow_reducers",
    "register_flow_reducers",
    "resolve_flow_reducer",
]

#: The flow-run reducer CACHE: flow_id → (join step_key → body). Warmed by
#: the finalizing process only — never the source of truth (the module
#: docstring states the resolution order).
_flow_reducers: dict[JobId, dict[str, Callable[[], Awaitable[None]]]] = {}


@dataclass(frozen=True, slots=True)
class FlowReducerResolution:
    """The fired join's body resolution's VERDICT (the three faces the
    sweep's fire arm must distinguish — the wedged-hold cure's own
    contract):

    * ``body`` set: the engine-level reducer (the tx2's own dict, warmed
      into the memo) — the healer re-runs it INSIDE the fire's tx (the
      B3 window's at-least-once body execution).
    * ``body`` None, ``loud`` False: the join HAS no body BY DESIGN —
      the identity packer (the compiled node's own body is None: the
      join/gather kinds) or a STEP body (whose execution is the CLAIM's,
      the wired args — never the fire's). The row fires, the consumers
      deliver, the row's own claim executes it. NO stamp: the record is
      healthy — ``body_unavailable`` over an identity join is the stamp
      that lies.
    * ``body`` None, ``loud`` True: the stamped workflow name resolves to
      NO compiled graph in this process (the R2-2 deployment-defect
      class: the definitions not imported here) — the delivery continues
      and the record goes LOUD (the stamp + the warning).

    THE CONVICTED VARIANT this contract replaces: the registry leg that
    adapted ANY registered body to the reducer convention
    (``definition_body(None)``) — the registry's bodies are STEP bodies
    (ctx + params), so the adapter was the TypeError machine: a crash-
    window heal of a parented STEP row fired it, resolved the step's
    body, called it with the reducer's arity, rolled the fire's tx back,
    and re-fired FOREVER — the sweep arm wedged, the drive max_ticks
    (the two-drivers pin's conviction, 2026-10-09)."""

    body: Callable[[], Awaitable[None]] | None
    loud: bool


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
) -> FlowReducerResolution:
    """The fired join's body resolution's VERDICT (see
    :class:`FlowReducerResolution` for the three faces).

    The memo (the tx2's warmed cache) is the only body source: the
    registry's bodies are STEP bodies — a step's execution is the
    CLAIM's (the wired args), a join's the identity packer's — neither
    is ever a zero-arg reducer, and the adapter that called one as the
    other was the wedged-hold TypeError machine. The compiled graph's
    own truth decides the LOUD face: a stamped workflow name that
    resolves to NO compiled graph in this process is the R2-2
    deployment-defect class (the definitions not imported here) — the
    delivery continues and the record goes loud; a graph that resolves
    is the healthy shape, whatever the node's kind."""
    memo = _flow_reducers.get(flow_id, {}).get(step_key)
    if memo is not None:
        return FlowReducerResolution(body=memo, loud=False)
    if workflow_name:
        from taskq.workflows._worker_execution import get_compiled_workflow

        try:
            get_compiled_workflow(workflow_name)
        except KeyError:
            return FlowReducerResolution(body=None, loud=True)
        # The graph resolves in THIS process — the process knows the
        # workflow: a node WITH a body is a step (the row's own claim
        # executes it, the wired args); a node WITHOUT one is a join (the
        # identity packer's); a fork-spawned key (no compiled decl — the
        # '<src>.item' shape) is the definition's own step body (the
        # claim's again). EVERY face is the claim's, never the fire's:
        # the fire delivers, the row fires, the record is healthy.
        return FlowReducerResolution(body=None, loud=False)
    # No stamp and no memo: the pre-stamp/anonymous root — the R2-2 loud
    # face (the record must not look healthy while the resolution could
    # not even name the workflow to ask).
    return FlowReducerResolution(body=None, loud=True)


def forget_flow_reducers(flow_id: JobId) -> None:
    """Drop a flow run's cache entry (the terminal flow's entry — the
    phantom reaper calls this at the reap, so the map is bounded by THIS
    process's live flow runs)."""
    _flow_reducers.pop(flow_id, None)

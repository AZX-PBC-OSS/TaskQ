"""The flow-run reducer registry (T04's tx1→tx2 crash window, attack-hardened).

THE WINDOW: ``finalize_node`` = tx1 (the fenced terminal) then tx2 (the
decrement + fire + REDUCER BODY + outbox). A worker killed after tx1 and
before tx2 leaves the decrement undone; the sweep's healing pass
(``sweep_join_rederive``) reconciles the cache and FIRES the join — but a
fire without its reducer body dispatches consumers off an UN-REDUCED join:
the join's output never exists, "at-least-once body execution" degraded to
NEVER.

THE CURE: the reducer body is resolvable OUTSIDE the dying process's
stack. The finalize REGISTERS its reducers against the flow run (this
module — the same process that ran the step owns the healing pass, so the
closure survives the crash window in-process); the sweep's fire arm
resolves the join's body from here (falling back to the registered
DEFINITION via :func:`taskq.workflows.definitions.resolve_step_body` when
the flow root's metadata names its workflow) and runs it INSIDE the
sweep's transaction — the exactly-once boundary is the FIRE's boundary,
never the body's: a raising body rolls the sweep tx back (the fire row and
the outbox rows with it), the re-derive re-fires, and the body RE-RUNS —
the same at-least-once doctrine the finalize's tx2 states.

D1 (BODY-FROM-DEFINITION) is preserved: the public dispatch resolves step
bodies from the registered definition only; this registry is the ENGINE's
own memo of the reducers a flow run's finalize declared — it never
shadows the definition, it carries the bodies across the engine's own
crash window. The reducer step inherits the run-key arbiter + the attempt
fence the same way its outbox-consumer siblings do: the fired join's
consumer rows (and the reducer's own row, keyed
``wf:{flow}:{join_step_key}`` on the composite arbiter) are deduped by
the arbiter, and the claim that grants the body's re-run is fenced by the
ledger.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from taskq.backend._protocol import JobId

__all__ = ["forget_flow_reducers", "register_flow_reducers", "resolve_flow_reducer"]

#: The flow-run reducer memo: flow_id → (join step_key → body).
_flow_reducers: dict[JobId, dict[str, Callable[[], Awaitable[None]]]] = {}


def register_flow_reducers(
    flow_id: JobId,
    reducers: dict[str, Callable[[], Awaitable[None]]],
) -> None:
    """Record a finalize's reducers for the flow run (the sweep's healing
    pass resolves from here). Idempotent per (flow, step): a re-finalize of
    the same run re-registers the same keys."""
    _flow_reducers.setdefault(flow_id, {}).update(reducers)


def resolve_flow_reducer(flow_id: JobId, step_key: str) -> Callable[[], Awaitable[None]] | None:
    """The body for a fired join: the flow run's own memo first, then the
    registered definition (D1) when the memo holds nothing. ``None`` = the
    join declares no body in this process — the fire delivers the declared
    consumers; nothing else runs."""
    body = _flow_reducers.get(flow_id, {}).get(step_key)
    if body is not None:
        return body
    from taskq.workflows.definitions import resolve_step_body

    try:
        definition_body = resolve_step_body(f"flow:{flow_id}", step_key)
    except (KeyError, TypeError):
        return None

    async def _definition_adapter() -> None:
        await definition_body(None)

    return _definition_adapter


def forget_flow_reducers(flow_id: JobId) -> None:
    """Drop a flow run's memo (the terminal flow's entry — hygiene, the
    map is bounded by the live runs of THIS process)."""
    _flow_reducers.pop(flow_id, None)

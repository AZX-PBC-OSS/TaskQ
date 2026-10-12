# T31 — The nested flow spawn/join: sub-investigations with their own run identity

**Status:** LANDED — CLOSED (was: this lane's DRAFT, ranked 2nd; the child-flows lane landed the surface) · **Landed as:** `ctx.spawn_flow(name, input=…, result=G, key=…, max_depth=…)` → `ChildHandle[G].wait(timeout_s=…) → ChildOk[G] | ChildFailed | ChildCancelled | Expired` — the child is a REAL flow (its own run rows; the run explorer, the admin, the CLI all see it), the spawn's run key is the TWO-CLAIMS LAW at the flow level (the retry ADOPTS the same child — the per-attempt spawn cursor is stable across retries; an explicit `key=` overrides), the parent's node carries the child's id (`metadata.child_flows`, the ensure-append), the child's root carries the parent's (the create's ONE transaction — never a post-create UPDATE), the join rides the hold machinery (the typed outcome union, the fail-close `Expired` arm), the depth bound (default 32) bounds the recursion, and the child's holds are the CHILD's own. The design-first shape's shared-state channel stays deliberately-not-v1. Pins: `tests/test_wf_childflow_pins.py`, `tests/test_wf_childflow_rv5_pins.py`, `tests/test_wf_childflow_cures_pins.py` — green ×2 at the merged estate. The guide's §Pattern 3 teaches the landed face.

## The named pattern

The SUB-AGENT SPAWN/JOIN across run identities: a parent workflow spawns a
CHILD workflow that is itself a first-class run — its own id, its own run key,
its own holds, its own cancellation — the parent awaits (or doesn't) and
joins the child's result. The frameworks' faces:

- **LangGraph**: the subgraph as a node (a compiled graph mounted inside the
  parent's state space — one checkpointer thread, shared state channel).
- **Temporal**: `workflow.execute_child(...)` → the handle (`.result()`, the
  cancellation cascade, the child's own history).
- **Inngest**: `step.invoke()` (the sync delegation) and `step.sendEvent()`
  (the async/scheduled spawn) — the docs' own agent-pattern table.
- **LlamaIndex**: sub-workflows (a workflow mounted as a step's tool).
- **Trigger.dev**: `batch.triggerByTask` / `tasks.triggerAndPoll`.

## The gap in TaskQflow's landed surface

The landed sub-investigation is the ONE-FLOW fan-out: `map_source(plan,
investigate_one)` + the gather join. Each child is a queue ROW (its own
claim, retry ladder, rate-limit terms, progress channel) but NOT a run: it
shares the parent's flow identity, and critically it cannot have its own
HOLD. The moment a sub-investigation needs its own human gate (the sub-agent
that asks ITS reviewer, not the parent's), the one-flow shape leaks: the
parent's wait site consumes the answer-queue cursor per attempt, so a second
concurrent hold inside a map child has no addressable identity distinct from
the parent's gate. Same for the idempotent re-fire: the run-key arbiter keys
RUNS — a sub-investigation cannot be re-fired idempotently on its own key
while the parent lives.

The adoption cost is the product story: the cowork-agent that "spawns three
investigations, each of which may need a human sign-off" — the maintainer's
own example — stalls at the one-flow boundary. Today's answer is "flatten the
approvals into the parent" — workable until the sub-agents are dynamic
(the count unknown until the orchestrator's LLM decides), at which point the
map's fork IS the right shape and the holds are the missing piece.

## The design-first shape

- `spawn_flow(compiled, input, *, key=...)` → `ChildOutcome[R]` — the child
  flow is a REAL run (its own `create_flow` with the run-key arbiter's typed
  verdict — the idempotent re-fire comes free), the parent's node holds on
  the child's terminal row, the result decodes through the child's terminal
  node's declared type.
- The cancel cascade: cancelling the parent's node cancels the child RUN
  (the three cancel legs' third leg extended one hop — the child's own
  cancellation is its own row-level op, already landed).
- The child's holds are the child's own (the multi-workflow property already
  holds: "the filter is BY RUN, resolves never cross") — the sub-agent's
  gate is addressable in `HitlClient.list(run=child_id)` TODAY, the moment
  the child is a run.
- The honest v1 boundary: no shared-state channel between parent and child
  (the payload + the result are the contract — LangGraph's shared-state
  subgraph is a DIFFERENT pattern, deliberately not v1).

## The verification bar

- The pin: a spawned child's row IS a run row (the arbiter's verdict
  applies — a duplicate spawn key re-fires LOUDLY, never silently).
- The pin: the parent's cancel cancels the child's held node (the cascade's
  pin — an orphaned child is the convicted state).
- The fence in `docs/guides/agent-loops.md` §Pattern 3 grows the spawn
  variant (the sub-agent with its own gate, resolved separately from the
  parent's).

## The receipts

Temporal child workflows + the AI samples' child-agent patterns; Inngest's
agent pattern table (`step.invoke` / `sendEvent`); LlamaIndex sub-workflows;
LangGraph subgraphs; the lane's mapping table row 3.

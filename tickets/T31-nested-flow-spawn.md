# T31 — The nested flow spawn/join: sub-investigations with their own run identity

**Status:** DRAFT (the agent-loop parity lane's research) · **Lane:** agentloop-parity · **Ranked:** 2nd of the five gaps (T30–T34)

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

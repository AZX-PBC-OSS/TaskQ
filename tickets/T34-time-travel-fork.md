# T34 — The time travel / fork-from-history: replay from an arbitrary node with new input

**Status:** DRAFT (the agent-loop parity lane's research) · **Lane:** agentloop-parity · **Ranked:** 5th of the five gaps (T30–T34)

## The named pattern

The TIME-TRAVEL face: inspect a run's state AT ANY PAST STEP, replay from
that step, or FORK — re-run from an arbitrary node with corrected input —
without starting the run from zero. The frameworks' faces:

- **LangGraph**: `graph.get_state(config, subgraphs=True)` /
  `get_state_history(config)` — every superstep's checkpoint retained; the
  replay is `graph.invoke(None, config={checkpoint_id: ...})` and the FORK
  is the same call with the state update applied. The docs' own framing:
  "time travel" is a listed feature, not a hack.
- **CrewAI**: the checkpointing pillar (crewai.com/open-source, 2026) —
  "replay from specific steps, fork workflows, and run tasks with different
  inputs without starting from scratch".
- **Temporal**: the deterministic REPLAY is the foundation (the same event
  history re-executes the code), and the reset is the operator's `reset`
  CLI — the event-history form of the same power.

## The gap in TaskQflow's landed surface

The landed durability IS replay-shaped already — the rows are the truth, the
step ledger remembers every attempt, the re-drive of a terminal flow re-runs
NOTHING (the ledger's memo returns the recorded results — the
no-re-execution pin), and the retry ladder re-executes a FAILED node from
the top with the ledger fencing its side effects. What is missing is the
AUTHORING/DEBUGGING face on top: there is no declared API to (a) READ a
run's state at a past node (the decoded payloads are on the edges — the
data is THERE, but the access is the operator's SQL, not an API), (b)
RE-EXECUTE from an arbitrary node with the downstream ledger entries
invalidated (the "fix the prompt and re-run from step 3" loop), or (c) FORK
a completed run into a new run whose prefix is the recorded results and
whose suffix re-runs with corrected input.

The adoption cost is the debugging loop: every framework user's demo includes
"the agent wrote a bad plan; I fixed the plan and re-ran from there" — a
30-second loop in LangGraph Studio. Ours is "re-run from zero with the fix",
which for a run that is 40 minutes of LLM calls and a human approval deep is
the difference between adopting and admiring.

## The design-first shape

- READ first (the cheap half, mostly a face over landed rows):
  `FlowRunner.state_at(flow_id)` / `state_at(flow_id, node_key=...)` — the
  ledger + the edge payloads composed into the run's state at that node,
  the typed view the run explorer already renders. No new writes; the rows
  are the history.
- FORK second (the design's real seam): `fork_flow(flow_id, from_node,
  input_override=None) -> RunClaim` — a NEW run whose completed prefix
  COPIES the recorded results (the ledger rows re-parented or the cache
  pre-seeded — the cross-run cache T25 is the likely mechanism: the fork
  pre-seeds each prefix node's cache address), and whose suffix re-runs.
  The run-key arbiter's verdict types the fork (a fork is a NEW run — never
  a silent mutation of the old one).
- The human-gate interaction named up front: forking past a RESOLVED hold
  re-uses the recorded answer (the operator never re-answers a question the
  human answered); forking TO a hold re-asks (the new run's hold is a new
  row, the old answer's audit intact). This boundary is the design's
  hardest question and the ticket exists to force it to be answered BEFORE
  any code.
- The honesty row: Temporal's reset and LangGraph's fork both lean on
  "the state is one thing" (the event history / the checkpointer). Ours is
  explicit steps — the fork's prefix-copy is MECHANICAL but its interaction
  with the cross-run cache's determinism hazard (same body + input ≠ same
  world) must be documented, not inherited.

## The verification bar

- The pin: a fork's prefix nodes do NOT re-execute their bodies (the
  cache/ledger proof — the fork's run shows the recorded results and zero
  body calls), and the suffix's re-runs are fresh attempts.
- The pin: the fork is a NEW run (the original's rows are immutable — the
  fork cannot corrupt the audited history).
- The fence in `docs/guides/agent-loops.md` grows the debugging-loop
  variant (the bad-plan fix, re-run from step 3).

## The receipts

LangGraph's time-travel/persistence docs (get_state_history, checkpoint_id
replay); CrewAI's checkpointing pillar; Temporal's reset + the deterministic
replay framing (temporal.io blog, "deterministic in execution, not
predetermined"); the lane's mapping table row 6.

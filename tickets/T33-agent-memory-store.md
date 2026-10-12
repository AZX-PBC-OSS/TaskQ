# T33 — The cross-run agent memory: a namespaced store the runs read and write

**Status:** DRAFT (the agent-loop parity lane's research) · **Lane:** agentloop-parity · **Ranked:** 4th of the five gaps (T30–T34)

## The named pattern

The AGENT MEMORY / the store: a durable, namespaced key-value (or
document) store that OUTLIVES runs — the agent writes what it learned this
run and reads it back next run ("this customer's tier", "the last incident's
root cause", the working set that is not a payload and not a cache). The
frameworks' faces:

- **LangGraph**: the BaseStore — `store.put(namespace, key, value)` /
  `store.get` / `store.search`, namespaced per thread/user/agent, backed by
  Postgres in production; the memory patterns' third tier (the long-term
  store beside the thread-scoped checkpointer).
- **Inngest**: AgentKit's shared state (the network's agents coordinating
  through a durable state layer).
- **CrewAI**: the Memory pillar — "remembers, resolves contradictions,
  forgets intentionally, knows when it lacks context" (crewai.com/open-source,
  2026) — the most product-shaped of the faces.
- **LlamaIndex**: the Context's state (run-scoped) + the memory tiers
  (short/working/long) as the author's composition.

## The gap in TaskQflow's landed surface

The landed cross-run face is the STEP CACHE (T25): a content-addressed
dedup — "same body, same input → the same stored answer" — with the receipt
stamped on the node row. It is a cache, not a memory: the address is the
body+input hash, the writer is the engine (terminal-SUCCEEDED only), and
there is no namespace an agent addresses BY MEANING. An agent that wants to
remember the customer's context for the run after this one has: the payload
(and the input — re-supplied every time), the cache (the wrong contract),
or its own table (outside the estate — unredacted, unaudited, invisible to
the operator's run explorer, a second shadow database).

The adoption cost compounds: agents that "run long and know things" are the
product story, and memory is where ad-hoc agent stacks accrete their worst
state (the unversioned blob column nobody can query). The frameworks that
HAVE the store get the memory-tier products built on it; we would be
sending the agent-builder to invent their own.

## The design-first shape

- `taskq.workflows.Memory` (or `app.store`): a namespaced KV over the
  estate's own PG — `(namespace, key) → jsonb payload + schema version +
  redact hook + writer provenance (flow_id, node_key, when)`. The
  namespace is the address the run receives in its input (the tenant/user/
  agent dimension is the CALLER's composition, not the engine's guess).
- The body's API: `ctx.memory.get/put/delete/search_prefix` — transactional
  with the node's finalize ONLY at the `substep` boundary (a memory write
  that must be exactly-once rides the ledger; a plain put is its own tx).
- The redact law extends: the chain-then-hook pipeline runs BEFORE persist —
  the store never carries what the hold row cannot (the same attack surface,
  one law).
- The cache-vs-memory boundary stated in the docs: the cache dedups
  COMPUTATION; the memory remembers CONTEXT. Different retention policies
  (the cache's TTL vs the memory's explicit-forget), different readers
  (the engine vs the body), different guarantees.
- The LATER rows: the semantic search (an embedding column + the pgvector
  face) and the contradiction resolution (CrewAI's most-product face) —
  v1 is the honest KV with provenance; the recall quality is not v1's fight.

## The verification bar

- The pin: a memory put is visible to a DIFFERENT process's run (the
  cross-process truth), and the redact hook runs before persist (the canary
  never lands).
- The pin: the store's rows are swept by the retention coupling (T18's arm
  extended — the memory that outlives every run forever is the operator's
  configured choice, never the default).
- The fence in `docs/guides/agent-loops.md` grows the memory variant (the
  agent that remembers the customer across runs).

## The receipts

LangGraph BaseStore + the memory patterns; CrewAI's Memory pillar;
Inngest AgentKit's shared state; LlamaIndex's memory-tier discussion
(run-llama/llama_index #20381); the lane's mapping table row 6 (the
checkpointing row's memory boundary).

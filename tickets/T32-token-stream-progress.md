# T32 — The token-stream progress class: rendering the answer as it thinks

**Status:** DRAFT (the agent-loop parity lane's research) · **Lane:** agentloop-parity · **Ranked:** 3rd of the five gaps (T30–T34)

## The named pattern

The STREAMING / token-progress face: the agent's answer rendered AS IT IS
PRODUCED — token deltas (or part deltas: text, tool calls, reasoning) pushed
to the product UI over a resumable stream. The frameworks' faces:

- **LangGraph**: `stream_mode="messages"` — token-level deltas per LLM call;
  `stream_mode="custom"` for the author's own emissions; `astream_events`
  for the whole lifecycle.
- **Trigger.dev**: v4.5.0's durable chat — the stream reconnects from the
  LAST CHUNK after a crash/redeploy/idle (the run survives, the stream
  resumes); `realtime` streams runs to the browser.
- **Temporal**: Workflow Streams (public preview, May 2026) — a workflow's
  live output streamed to a client, the durability underneath.
- **LlamaIndex**: `handler.stream_events()` — the author's steps emit typed
  events the client consumes.
- **Inngest**: `useAgent` / AgentKit's parts-based streaming (the UI composes
  text/tool/data parts, each updating independently).

## The gap in TaskQflow's landed surface

The landed progress face is coarser BY DESIGN: `ctx.progress` updates an
in-memory latest-wins buffer flushed at a ~50 ms cadence (≈20 writes/s), and
the STREAM channel is a per-node 64-row drop-oldest ring. For the operator's
"where is this node" gauge and the replayable "what did it report" stream,
the shape is right and honestly bounded. For the chat-shaped product — the
cowork-agent whose answer streams into a conversation — it shows the shape
of the work, not the words: a token delta stream at 50–200 events/s cannot
ride a 64-row ring (the ring would prune the answer mid-sentence) and cannot
ride a ~20 writes/s upsert (the coalescing law destroys the deltas by
construction).

The adoption cost is product-visible: the demo comparison against any
chat-native stack is lost at the first rendering ("ours shows a progress
bar, theirs shows the agent thinking"). An agent-builder who needs the
token face today must BYPASS the engine (stream from the body to their own
SSE channel) — the engine goes blind to exactly the traffic the product
cares about, and the run explorer shows nothing.

## The design-first shape

- A THIRD channel class — the token stream (`class='token'` in the closed
  kind vocabulary, or its own table): append rows, PER-NODE bound sized for
  prose (the ring's law kept: a bound, the dropped count ON THE RECORD —
  but the bound measured in KB, not rows, and the cadence law RELAXED for
  this class: token deltas are the payload, not observability).
- The resumable replay face: the SSE contract's `id:`/`resync` frames
  EXTENDED to the token class (reconnect from the last chunk — the
  Trigger.dev v4.5 semantics), the state channel carrying the answer's
  committed prefix so a reconnect converges.
- The durability boundary stated honestly: token deltas are
  OBSERVABILITY-FIRST (best-effort, degrades first) — the ANSWER's truth
  stays the node's terminal row (the progress-lie fence extended: a failed
  node never shows a half-answer as its result).
- The body's API: `ctx.stream_delta(text)` (the fire-and-forget twin of
  `ctx.progress`) and the LLM-client adapters as a contrib (the OpenAI /
  Anthropic stream → delta calls, the contrib lane's own shape).

## The verification bar

- The pin: a token-emitting node's deltas replay in order from ANY cursor,
  and the reconnect's resync names the gap (no silent hole).
- The pin: the token channel's write path NEVER touches the finalize path
  (the observability-degrades-first law holds for the new class — the
  zero-finalize-changes probe extended).
- The fence in `docs/guides/agent-loops.md` §Pattern 4 grows the token
  variant.

## The receipts

LangGraph stream modes; Trigger.dev v4.5.0 changelog (the durable chat + the
resume-from-last-chunk claim); Temporal Workflow Streams preview (May 2026);
Inngest AgentKit's `useAgent`; the lane's mapping table row 4.
